#!/usr/bin/env python3
"""Flag migration-time optimization opportunities in a consolidated analyzer report.

This is a second, independent scoring pass over the same JSON that ``build-hierarchy``
consumes (``consolidated_all_jobs.json`` from ``analyze``/``analyze-parallel`` +
``merge-reports``, or the official analyzer's own output). It does not change any
generated notebook or converted job; ``run-dbxconv``/``dbxconv`` stays a literal
lift-and-shift. This pass is about what is worth re-architecting *after* the lift-and-shift
lands, scored the same way the Lakebridge Analyzer scores complexity (HIGH/MEDIUM/LOW),
so the two reports can be read side by side.

Nine heuristics, each looking for a known DataStage-to-Databricks or general SQL-tuning win:

1. **Truncate-and-reload writes** (``truncateReloadWrites``): a target stage whose write
   mode is a full overwrite/truncate/replace, not an incremental append or merge. On
   Databricks this is the ``MERGE``/Auto CDC opportunity -- see the retail pipeline's own
   SCD2 job for a worked example (``migration/README.md`` §7).
2. **Hand-rolled change-detection jobs** (``scd2Candidates``): a job that joins a snapshot
   against the current state and branches into expire/carry/new-version logic by hand.
   This is exactly the shape ``create_auto_cdc_from_snapshot_flow`` replaces.
3. **Duplicate queries** (``duplicateQueries``): the same embedded SQL statement, filter
   predicate, or derivation expression appearing in more than one place. Each group that
   spans two or more jobs is also surfaced as a merge candidate.
4. **Filters pushed too late** (``queriesWithoutFilters``): a join/lookup/aggregation with
   no filter stage anywhere in the job, or embedded SQL with no ``WHERE``, i.e. a full
   table scan feeding an expensive stage.
5. **Non-sargable predicates** (``nonSargablePredicates``): a filter or ``WHERE`` wraps the
   compared column in a function (``Upcase(x) = 'Y'``, ``UPPER(x) = 'Y'``, a ``CAST``, a date
   truncation, ...). That defeats partition pruning / data skipping: the engine must
   evaluate the function on every row before it can tell which rows qualify.
6. **OR-heavy predicates** (``orHeavyPredicates``): several ORs in one predicate, usually
   across different columns, which is harder to push down than one predicate per column.
7. **Cartesian-risk joins** (``cartesianRiskJoins``): a join stage with no declared key, or
   embedded SQL with a ``JOIN`` and no ``ON``/``USING`` -- a silent row-count explosion
   instead of an error, usually discovered far downstream of where it happened.
8. **Embedded-SQL set-operation issues** (``embeddedSqlSetIssues``): ``SELECT *`` (reads
   every column), a wide ``SELECT DISTINCT`` (forces a full shuffle when only a narrower
   key needs to be unique), ``UNION`` instead of ``UNION ALL`` (an unneeded extra
   distinct/sort pass), and a subquery inside ``WHERE`` (likely rewritable as a join).
9. **Repeated derivations within one job** (``repeatedExpressionsWithinJob``): the same
   non-trivial expression computed more than once in the same job -- excluding DataStage's
   row-context macros (``@INROWNUM``/``@PARTITIONNUM``/``@NUMPARTITIONS``), since two
   columns using the same row-numbering formula are two independent counters, not the same
   value computed twice.

Per-job findings are weighted into an **optimization index** (HIGH/MEDIUM/LOW, mirroring
the Analyzer's complexity index) so the busiest jobs to re-architect surface first, and
into a **merge candidates** list naming the specific jobs that share duplicated logic.

Example::

    lakebridge-workflow optimize --input-json consolidated_all_jobs.json \\
        --out-prefix optimization_report
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .common import atomic_json, now_utc, write_xlsx

# --- Heuristic patterns -----------------------------------------------------------------
# Property names/values that spell out a full overwrite. Covers both this repository's
# Sequential File idiom (``append\overwrite = overwrite``) and the generic DB-connector
# idiom (``Write mode`` / ``Table action`` = Truncate/Replace).
#
# Note: DataStage identifiers concatenate words (``PxJoin``, ``CJoin``) or join them with
# underscores (``tgt_dim_customer``), and underscore counts as a word character to Python's
# regex engine. So these patterns match as plain substrings, never with ``\b`` word
# boundaries, which would silently fail to match at a ``_`` (or not match at all).
WRITE_MODE_NAME_RE = re.compile(r"overwrite|write.?mode|table.?action|load.?mode", re.IGNORECASE)
WRITE_MODE_VALUE_RE = re.compile(r"overwrite|truncate|replace|recreate", re.IGNORECASE)
AUDIT_STAGE_RE = re.compile(r"audit|reject|_rej|dq", re.IGNORECASE)
DIMFACT_STAGE_RE = re.compile(r"dim|fact|mart", re.IGNORECASE)

# A hand-rolled SCD2/CDC job: a Join (snapshot vs current) plus the vocabulary used to
# expire old rows, carry unchanged ones forward, and version new ones.
JOIN_TYPE_RE = re.compile(r"join", re.IGNORECASE)
CHANGE_WORD_RE = re.compile(
    r"expire|carry.*unchanged|new.*version|detect.*change|scd.?2|change.*captur|change.*apply|cdc",
    re.IGNORECASE,
)
MIN_CHANGE_WORD_HITS = 2

# Expensive stages that deserve an upstream filter; a job with any of these and zero
# filter-type stages is doing the filtering too late, or not at all.
EXPENSIVE_TYPE_RE = re.compile(r"(?i)join|lookup|merge|aggregat")
FILTER_TYPE_RE = re.compile(r"(?i)filter")

WHERE_PROP_NAME_RE = re.compile(r"(?i)^where$")
TRIVIAL_PREDICATE_RE = re.compile(r"(?i)^\s*(1\s*=\s*1|true)\s*$")
SQL_SELECT_RE = re.compile(r"(?i)\bselect\b")
SQL_WHERE_RE = re.compile(r"(?i)\bwhere\b")
TRIVIAL_EXPRESSION_RE = re.compile(r"^[\w.]+$")  # a bare column/link reference, no logic

# DataStage's own function vocabulary (ds2spark.py's expression translator, migration/README.md
# §2.3) plus the generic SQL a DB-connector stage's embedded query would use. Wrapping the
# compared column in any of these breaks partition pruning / data skipping: the engine can't
# tell which rows/files qualify without evaluating the function on every row first.
NONSARGABLE_FUNC_RE = re.compile(
    r"(?i)\b(trim|upcase|downcase|upper|lower|left|right|substring|substr|ereplace|replace|"
    r"cast|convert|stringtodate|stringtotimestamp|datetostring|timestamptostring|"
    r"year|month|day|date_trunc|to_date|to_timestamp)\s*\([^()]*\)\s*(=|<>|!=|<=|>=|<|>)"
)

OR_COUNT_RE = re.compile(r"(?i)\bor\b")
MIN_OR_COUNT = 2  # at least this many ORs in one predicate before it's worth flagging

# A join/lookup property that declares the join key, however it's spelled.
JOIN_KEY_PROP_RE = re.compile(r"key|condition|^on$", re.IGNORECASE)
SQL_JOIN_NO_ON_RE = re.compile(r"(?i)\bjoin\b(?![^;]*\b(on|using)\b)")

SQL_UNION_NOT_ALL_RE = re.compile(r"(?i)\bunion\b(?!\s+all\b)")
SQL_SELECT_STAR_RE = re.compile(r"(?i)select\s+(\w+\.)?\*")
SQL_SELECT_DISTINCT_RE = re.compile(r"(?i)select\s+distinct\b")
SQL_SUBQUERY_IN_WHERE_RE = re.compile(r"(?i)where\b[^;]*\(\s*select\b", re.DOTALL)

# DataStage's per-row partitioning macros. Two columns that both use this formula are two
# independent row-number generators (one per upstream link/sort order), not the same
# computation twice -- unlike a pure function of input columns, identical *text* here does
# not mean identical *value*, so these must never be treated as a duplicate/common
# subexpression (see find_repeated_expressions_within_job).
ROW_CONTEXT_MACRO_RE = re.compile(r"@INROWNUM|@PARTITIONNUM|@NUMPARTITIONS", re.IGNORECASE)

# dsx_condense.py renders some condensed properties (``where``, ``key``, ...) verbatim from
# the DSX export's raw Value field, which for these is DataStage's own BASIC/UniVerse
# multi-value list encoding (``\(1)``/``\(2)``/``\(3)`` are its field/value/subvalue marks)
# rather than plain text -- see migration/README.md §8a.7 on how little is documented about
# this format. Unwrap the common single-value shape so a human reading a report sees the
# real predicate; a multi-value shape (several marks in the same property, e.g. a
# multi-column join key) is left exactly as condensed, since mangling that silently would be
# worse than showing the raw text.
DSX_PROPERTY_WRAPPER_RE = re.compile(
    r"^\\\(2\)\\\(2\)0\\\(1\)\\\(3\)\w+\\\(2\)(?P<value>(?:(?!\\\(1\)).)*)\\\(2\)0$",
    re.IGNORECASE | re.DOTALL,
)


def _unwrap_dsx_value(value: str) -> str:
    match = DSX_PROPERTY_WRAPPER_RE.match(value.strip())
    return match.group("value") if match else value


BANDS = (("HIGH", 8), ("MEDIUM", 4), ("LOW", 1))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input-json", type=Path, required=True, help="consolidated analyzer JSON report")
    p.add_argument("--out-prefix", type=Path, default=Path("optimization_report"), help="output path prefix")
    return p.parse_args(argv)


def _normalize(text: str) -> str:
    return " ".join(text.split()).lower()


def _band(score: int) -> str:
    for name, threshold in BANDS:
        if score >= threshold:
            return name
    return "NONE"


def find_truncate_reload_writes(attributes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Target stages whose write mode is a full overwrite rather than an incremental one."""
    out: list[dict[str, Any]] = []
    for row in attributes:
        name, value = str(row.get("name") or ""), str(row.get("value") or "")
        if not WRITE_MODE_NAME_RE.search(name) or not WRITE_MODE_VALUE_RE.search(value):
            continue
        stage = str(row.get("stage") or "")
        audit_like = bool(AUDIT_STAGE_RE.search(stage))
        dimfact_like = bool(DIMFACT_STAGE_RE.search(stage))
        out.append({
            "job": row.get("job"), "stage": stage, "property": name, "value": value,
            "auditLike": audit_like, "dimFactLike": dimfact_like,
            "note": (
                "Dimension/fact/mart-style target; a strong Auto CDC / MERGE candidate "
                "if the source can be read incrementally." if dimfact_like else
                "Audit/reject/DQ-style target; reload is likely intentional, lower priority."
                if audit_like else
                "Full overwrite on every run; review whether the source is a full "
                "snapshot (expected) or could be read incrementally."
            ),
        })
    return out


def find_scd2_candidates(inventory: list[dict[str, Any]],
                         item_nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Jobs that look like a hand-rolled SCD2/change-detection implementation.

    These are exactly the shape ``create_auto_cdc_from_snapshot_flow`` replaces (see
    ``migration/README.md`` §7): a Join between a new snapshot and the current state,
    branching by hand into expire/carry/new-version logic.
    """
    by_job: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for node in item_nodes:
        by_job[str(node.get("job") or "")].append(node)
    names_by_complexity = {str(x.get("name")): x for x in inventory}

    out: list[dict[str, Any]] = []
    for job, nodes in sorted(by_job.items()):
        joins = [n for n in nodes if JOIN_TYPE_RE.search(str(n.get("nodeType") or ""))]
        change_hits = [n for n in nodes if CHANGE_WORD_RE.search(str(n.get("node") or ""))]
        if joins and len(change_hits) >= MIN_CHANGE_WORD_HITS:
            out.append({
                "job": job,
                "complexityLevel": str(names_by_complexity.get(job, {}).get("complexityLevel", "")),
                "joinStages": sorted({str(n.get("node")) for n in joins}),
                "changeDetectionStages": sorted({str(n.get("node")) for n in change_hits}),
                "note": "Hand-rolled change detection; a candidate for "
                        "create_auto_cdc_from_snapshot_flow from a staged snapshot.",
            })
    return out


def _dedupe_occurrences(occurrences: list[dict[str, str]], kind: str,
                        trivial: re.Pattern | None = None, min_len: int = 0) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for occ in occurrences:
        text = str(occ.get("text") or "").strip()
        if len(text) < min_len:
            continue
        if trivial and trivial.match(text):
            continue
        groups[_normalize(text)].append(occ)
    out: list[dict[str, Any]] = []
    for norm, occs in groups.items():
        jobs = sorted({occ["job"] for occ in occs})
        if len(occs) < 2:
            continue
        out.append({
            "kind": kind, "normalizedText": norm, "sampleText": occs[0]["text"],
            "jobs": jobs, "occurrences": occs,
        })
    return out


def find_duplicate_queries(embedded_sql: list[dict[str, Any]], attributes: list[dict[str, Any]],
                           expressions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Identical embedded SQL, filter predicates, or derivation expressions in >1 place."""
    sql_occurrences = [{"job": str(r.get("job") or ""), "stage": str(r.get("stage") or ""),
                        "text": str(r.get("sql") or "")} for r in embedded_sql]
    filter_occurrences = [{"job": str(r.get("job") or ""), "stage": str(r.get("stage") or ""),
                           "text": _unwrap_dsx_value(str(r.get("value") or ""))}
                          for r in attributes if WHERE_PROP_NAME_RE.match(str(r.get("name") or ""))]
    expression_occurrences = [{"job": str(r.get("job") or ""), "stage": str(r.get("transformation") or ""),
                               "text": str(r.get("expression") or "")} for r in expressions]

    groups = (
        _dedupe_occurrences(sql_occurrences, "embedded_sql")
        + _dedupe_occurrences(filter_occurrences, "filter_predicate", trivial=TRIVIAL_PREDICATE_RE, min_len=3)
        + _dedupe_occurrences(expression_occurrences, "derivation_expression",
                              trivial=TRIVIAL_EXPRESSION_RE, min_len=6)
    )
    groups.sort(key=lambda g: (g["kind"], g["normalizedText"]))
    for i, group in enumerate(groups, 1):
        group["groupId"] = f"DUP-{i:03d}"
    return groups


def find_queries_without_filters(inventory: list[dict[str, Any]], item_nodes: list[dict[str, Any]],
                                 embedded_sql: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Expensive stages (or embedded SQL) reached with no filtering anywhere upstream."""
    by_job: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for node in item_nodes:
        by_job[str(node.get("job") or "")].append(node)

    out: list[dict[str, Any]] = []
    for job, nodes in sorted(by_job.items()):
        expensive = [n for n in nodes if EXPENSIVE_TYPE_RE.search(str(n.get("nodeType") or ""))]
        has_filter = any(FILTER_TYPE_RE.search(str(n.get("nodeType") or "")) for n in nodes)
        if expensive and not has_filter:
            out.append({
                "job": job, "kind": "join_without_filter",
                "stages": sorted({str(n.get("node")) for n in expensive}),
                "note": "Join/lookup/aggregation with no filter stage anywhere in the job; "
                        "a full-table read reaches the expensive stage.",
            })
    for row in embedded_sql:
        sql = str(row.get("sql") or "")
        if SQL_SELECT_RE.search(sql) and not SQL_WHERE_RE.search(sql):
            out.append({
                "job": row.get("job"), "kind": "embedded_sql_without_where",
                "stages": [str(row.get("stage") or "")], "detail": sql,
                "note": "SELECT with no WHERE clause; review whether a predicate was dropped "
                        "in translation or the source table really needs a full scan.",
            })
    return out


def find_nonsargable_predicates(attributes: list[dict[str, Any]],
                                embedded_sql: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A filter predicate or embedded-SQL WHERE wraps the compared column in a function.

    ``WHERE Upcase(status) = 'ACTIVE'`` (or SQL's ``UPPER``/``TRIM``/a ``CAST``/...) forces a
    per-row function evaluation and defeats partition pruning / data skipping: the engine
    cannot tell which files qualify without transforming every row first.
    """
    out: list[dict[str, Any]] = []
    for row in attributes:
        if not WHERE_PROP_NAME_RE.match(str(row.get("name") or "")):
            continue
        predicate = _unwrap_dsx_value(str(row.get("value") or ""))
        if NONSARGABLE_FUNC_RE.search(predicate):
            out.append({
                "job": row.get("job"), "stage": row.get("stage"), "kind": "filter_predicate",
                "predicate": predicate,
                "note": "Filter wraps a column in a function before comparing it; compare the "
                        "raw column directly (precompute the derived value upstream if one is "
                        "needed), or pruning/skipping cannot use this filter.",
            })
    for row in embedded_sql:
        sql = str(row.get("sql") or "")
        if SQL_WHERE_RE.search(sql) and NONSARGABLE_FUNC_RE.search(sql):
            out.append({
                "job": row.get("job"), "stage": row.get("stage"), "kind": "embedded_sql",
                "predicate": sql,
                "note": "WHERE wraps a column in a function before comparing it; same fix as "
                        "the native-stage case.",
            })
    return out


def find_or_heavy_predicates(attributes: list[dict[str, Any]],
                             embedded_sql: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A filter/WHERE with several ORs, usually across different columns.

    Harder for the engine to push down than one predicate per column; splitting it into
    separate reads unioned together (or an IN-list on a single column) lets each branch
    prune independently.
    """
    out: list[dict[str, Any]] = []
    for row in attributes:
        if not WHERE_PROP_NAME_RE.match(str(row.get("name") or "")):
            continue
        predicate = _unwrap_dsx_value(str(row.get("value") or ""))
        count = len(OR_COUNT_RE.findall(predicate))
        if count >= MIN_OR_COUNT:
            out.append({
                "job": row.get("job"), "stage": row.get("stage"), "kind": "filter_predicate",
                "orCount": count, "predicate": predicate,
                "note": f"{count} ORs in one predicate; consider separate reads unioned "
                        "together, or an IN-list, so each branch can prune independently.",
            })
    for row in embedded_sql:
        sql = str(row.get("sql") or "")
        count = len(OR_COUNT_RE.findall(sql))
        if count >= MIN_OR_COUNT:
            out.append({
                "job": row.get("job"), "stage": row.get("stage"), "kind": "embedded_sql",
                "orCount": count, "predicate": sql,
                "note": f"{count} ORs in one WHERE; same fix as the native-stage case.",
            })
    return out


def find_cartesian_risk_joins(item_nodes: list[dict[str, Any]], attributes: list[dict[str, Any]],
                              embedded_sql: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A join with no declared key, or embedded SQL with a JOIN and no ON/USING.

    Both produce a cartesian (or near-cartesian) product silently -- the row count explodes
    instead of raising an error, so this is usually noticed far downstream of where it
    actually happened.
    """
    keyed_stages: set[tuple[str, str]] = set()
    for row in attributes:
        if JOIN_KEY_PROP_RE.search(str(row.get("name") or "")):
            keyed_stages.add((str(row.get("job") or ""), str(row.get("stage") or "")))

    out: list[dict[str, Any]] = []
    for node in item_nodes:
        if not JOIN_TYPE_RE.search(str(node.get("nodeType") or "")):
            continue
        job, stage = str(node.get("job") or ""), str(node.get("node") or "")
        if (job, stage) not in keyed_stages:
            out.append({
                "job": job, "stage": stage, "kind": "join_without_key",
                "note": "Join stage with no declared key property found; verify this isn't an "
                        "unintended cartesian product before it lands as a join/MERGE on "
                        "Databricks.",
            })
    for row in embedded_sql:
        sql = str(row.get("sql") or "")
        if SQL_JOIN_NO_ON_RE.search(sql):
            out.append({
                "job": row.get("job"), "stage": row.get("stage"), "kind": "embedded_sql_join_no_on",
                "note": "JOIN with no ON/USING clause in the same statement; likely an "
                        "implicit cartesian join (or an old-style comma join whose condition "
                        "lives in WHERE -- check before assuming this one is fine).",
            })
    return out


def find_embedded_sql_set_issues(embedded_sql: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """SELECT *, a wide SELECT DISTINCT, UNION without ALL, or a subquery inside WHERE."""
    out: list[dict[str, Any]] = []
    for row in embedded_sql:
        sql = str(row.get("sql") or "")
        job, stage = row.get("job"), row.get("stage")
        if SQL_SELECT_STAR_RE.search(sql):
            out.append({"job": job, "stage": stage, "kind": "select_star",
                        "note": "SELECT * reads every column; prune to only what's used "
                                "downstream so column pruning/data skipping can help."})
        if SQL_SELECT_DISTINCT_RE.search(sql):
            out.append({"job": job, "stage": stage, "kind": "wide_distinct",
                        "note": "SELECT DISTINCT across many columns forces a full shuffle; "
                                "if only uniqueness on a subset of them is required, dedupe "
                                "with row_number() over that narrower key instead."})
        if SQL_UNION_NOT_ALL_RE.search(sql):
            out.append({"job": job, "stage": stage, "kind": "union_not_all",
                        "note": "UNION forces an extra distinct/sort pass; use UNION ALL if "
                                "the branches can't produce duplicate rows."})
        if SQL_SUBQUERY_IN_WHERE_RE.search(sql):
            out.append({"job": job, "stage": stage, "kind": "subquery_in_where",
                        "note": "Subquery inside WHERE; if it's correlated to the outer query, "
                                "rewrite as a join so the engine plans one query instead of one "
                                "subquery execution per outer row."})
    return out


def find_repeated_expressions_within_job(expressions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The same non-trivial derivation computed more than once inside one job.

    Excludes DataStage's row-context macros (``@INROWNUM``/``@PARTITIONNUM``/
    ``@NUMPARTITIONS``): two columns using the same row-numbering formula are two
    independent counters (one per upstream link/sort order), not the same value computed
    twice, so identical text there does not imply redundant work. (A real instance of this
    was found and ruled out while validating this detector: this repository's J08 computes
    ``@PARTITIONNUM + (@NUMPARTITIONS * (@INROWNUM - 1)) + 1`` for both ``store_sk`` and
    ``employee_sk`` -- two unrelated surrogate keys on two different links, not one value
    reused.)
    """
    by_job: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in expressions:
        expr = str(row.get("expression") or "").strip()
        if len(expr) < 6 or TRIVIAL_EXPRESSION_RE.match(expr) or ROW_CONTEXT_MACRO_RE.search(expr):
            continue
        by_job[str(row.get("job") or "")][_normalize(expr)].append(row)

    out: list[dict[str, Any]] = []
    for job, groups in sorted(by_job.items()):
        for occs in groups.values():
            if len(occs) < 2:
                continue
            fields = sorted({str(o.get("field")) for o in occs})
            out.append({
                "job": job, "fields": fields, "occurrenceCount": len(occs),
                "sampleText": occs[0].get("expression"),
                "note": f"The same derivation is computed more than once in this job "
                        f"(columns {', '.join(fields)}); compute it once and reuse it.",
            })
    return out


def build(input_json: Path) -> dict[str, Any]:
    report = json.loads(input_json.read_text(encoding="utf-8"))
    inventory = report.get("inventory", [])
    if not inventory:
        raise RuntimeError(f"no inventory records in {input_json}")
    attributes = report.get("miscJobAttributes", [])
    item_nodes = report.get("itemNodeInfo", [])
    expressions = report.get("transformationExpressions", [])
    embedded_sql = report.get("embeddedSQLPrograms", [])

    truncate_reload = find_truncate_reload_writes(attributes)
    scd2_candidates = find_scd2_candidates(inventory, item_nodes)
    duplicate_queries = find_duplicate_queries(embedded_sql, attributes, expressions)
    no_filter = find_queries_without_filters(inventory, item_nodes, embedded_sql)
    nonsargable = find_nonsargable_predicates(attributes, embedded_sql)
    or_heavy = find_or_heavy_predicates(attributes, embedded_sql)
    cartesian_risk = find_cartesian_risk_joins(item_nodes, attributes, embedded_sql)
    sql_set_issues = find_embedded_sql_set_issues(embedded_sql)
    repeated_within_job = find_repeated_expressions_within_job(expressions)

    def _group_by_job(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[str(row["job"])].append(row)
        return grouped

    truncate_by_job = _group_by_job(truncate_reload)
    scd2_jobs = {str(row["job"]) for row in scd2_candidates}
    no_filter_by_job = _group_by_job(no_filter)
    duplicate_groups_by_job: dict[str, set[str]] = defaultdict(set)
    for group in duplicate_queries:
        for job in group["jobs"]:
            duplicate_groups_by_job[job].add(group["groupId"])
    nonsargable_by_job = _group_by_job(nonsargable)
    or_heavy_by_job = _group_by_job(or_heavy)
    cartesian_jobs = {str(row["job"]) for row in cartesian_risk}
    sql_set_by_job = _group_by_job(sql_set_issues)
    repeated_within_job_by_job = _group_by_job(repeated_within_job)

    optimization_index: list[dict[str, Any]] = []
    for item in inventory:
        job = str(item.get("name") or "")
        truncates = truncate_by_job.get(job, [])
        is_scd2 = job in scd2_jobs
        no_filter_hits = no_filter_by_job.get(job, [])
        dup_groups = duplicate_groups_by_job.get(job, set())
        nonsargable_hits = nonsargable_by_job.get(job, [])
        or_heavy_hits = or_heavy_by_job.get(job, [])
        has_cartesian_risk = job in cartesian_jobs
        sql_set_hits = sql_set_by_job.get(job, [])
        repeated_within_hits = repeated_within_job_by_job.get(job, [])

        score = (
            (4 if is_scd2 else 0)
            + min(len(truncates), 3) * 2
            + min(len(no_filter_hits), 3) * 2
            + len(dup_groups) * 2
            + (5 if has_cartesian_risk else 0)
            + min(len(nonsargable_hits), 3) * 1
            + min(len(or_heavy_hits), 2) * 1
            + min(len(sql_set_hits), 3) * 1
            + min(len(repeated_within_hits), 2) * 2
        )
        if score <= 0:
            continue
        findings = []
        if is_scd2:
            findings.append("hand-rolled SCD2/change-detection pattern")
        if truncates:
            findings.append(f"{len(truncates)} truncate-and-reload write(s)")
        if no_filter_hits:
            findings.append(f"{len(no_filter_hits)} join/query without an upstream filter")
        if dup_groups:
            findings.append(f"shares duplicate logic in {len(dup_groups)} group(s)")
        if has_cartesian_risk:
            findings.append("join with no declared key (cartesian risk)")
        if nonsargable_hits:
            findings.append(f"{len(nonsargable_hits)} non-sargable predicate(s)")
        if or_heavy_hits:
            findings.append(f"{len(or_heavy_hits)} OR-heavy predicate(s)")
        if sql_set_hits:
            findings.append(f"{len(sql_set_hits)} embedded-SQL set-operation issue(s)")
        if repeated_within_hits:
            findings.append(f"recomputes the same expression {len(repeated_within_hits)} time(s) in-job")
        optimization_index.append({
            "job": job, "complexityLevel": str(item.get("complexityLevel", "")),
            "score": score, "band": _band(score), "findings": findings,
            "duplicateGroupIds": sorted(dup_groups),
        })
    optimization_index.sort(key=lambda x: (-x["score"], x["job"]))

    merge_candidates: list[dict[str, Any]] = []
    for group in duplicate_queries:
        if len(group["jobs"]) < 2:
            continue
        suggestion = {
            "embedded_sql": "Identical embedded SQL across jobs; consolidate into one shared "
                            "query/view or a single parameterized job.",
            "filter_predicate": "Identical filter predicate reused across jobs; factor it into "
                                "a shared view or macro instead of repeating it per job.",
            "derivation_expression": "Identical derivation logic reused across jobs; factor it "
                                     "into a shared UDF or notebook function.",
        }[group["kind"]]
        merge_candidates.append({
            "groupId": group["groupId"], "kind": group["kind"], "jobs": group["jobs"],
            "occurrenceCount": len(group["occurrences"]), "sampleText": group["sampleText"],
            "suggestion": suggestion,
        })

    band_counts = Counter(x["band"] for x in optimization_index)
    summary = {
        "totalJobsScanned": len(inventory),
        "jobsWithFindings": len(optimization_index),
        "byBand": {name: band_counts.get(name, 0) for name, _ in BANDS},
        "truncateReloadWrites": len(truncate_reload),
        "scd2Candidates": len(scd2_candidates),
        "duplicateQueryGroups": len(duplicate_queries),
        "queriesWithoutFilters": len(no_filter),
        "nonSargablePredicates": len(nonsargable),
        "orHeavyPredicates": len(or_heavy),
        "cartesianRiskJoins": len(cartesian_risk),
        "embeddedSqlSetIssues": len(sql_set_issues),
        "repeatedExpressionsWithinJob": len(repeated_within_job),
        "mergeCandidateGroups": len(merge_candidates),
    }
    return {
        "runInfo": {
            "sourceReport": str(input_json), "builtAt": now_utc(),
            "sourceTechnology": report.get("runInfo", {}).get("sourceTechnology", "Datastage"),
        },
        "summary": summary,
        "optimizationIndex": optimization_index,
        "truncateReloadWrites": truncate_reload,
        "scd2Candidates": scd2_candidates,
        "duplicateQueries": duplicate_queries,
        "queriesWithoutFilters": no_filter,
        "nonSargablePredicates": nonsargable,
        "orHeavyPredicates": or_heavy,
        "cartesianRiskJoins": cartesian_risk,
        "embeddedSqlSetIssues": sql_set_issues,
        "repeatedExpressionsWithinJob": repeated_within_job,
        "mergeCandidates": merge_candidates,
    }


def workbook(result: dict[str, Any]) -> list[tuple[str, list[list[Any]]]]:
    summary = result["summary"]
    summary_rows = [["Metric", "Value"]]
    for key, value in summary.items():
        if isinstance(value, dict):
            summary_rows += [[f"{key}: {k}", v] for k, v in value.items()]
        else:
            summary_rows.append([key, value])

    index_rows = [["Job", "Band", "Score", "Complexity", "Findings", "Duplicate Group Ids"]]
    for item in result["optimizationIndex"]:
        index_rows.append([item["job"], item["band"], item["score"], item["complexityLevel"],
                           "; ".join(item["findings"]), ", ".join(item["duplicateGroupIds"])])

    truncate_rows = [["Job", "Stage", "Property", "Value", "Dim/Fact-like", "Audit-like", "Note"]]
    for row in result["truncateReloadWrites"]:
        truncate_rows.append([row["job"], row["stage"], row["property"], row["value"],
                              row["dimFactLike"], row["auditLike"], row["note"]])

    scd2_rows = [["Job", "Complexity", "Join Stages", "Change-Detection Stages", "Note"]]
    for row in result["scd2Candidates"]:
        scd2_rows.append([row["job"], row["complexityLevel"], ", ".join(row["joinStages"]),
                          ", ".join(row["changeDetectionStages"]), row["note"]])

    dup_rows = [["Group Id", "Kind", "Jobs", "Occurrence Count", "Sample Text"]]
    for row in result["duplicateQueries"]:
        dup_rows.append([row["groupId"], row["kind"], ", ".join(row["jobs"]),
                         len(row["occurrences"]), row["sampleText"]])

    no_filter_rows = [["Job", "Kind", "Stages", "Note"]]
    for row in result["queriesWithoutFilters"]:
        no_filter_rows.append([row["job"], row["kind"], ", ".join(row["stages"]), row["note"]])

    nonsargable_rows = [["Job", "Stage", "Kind", "Predicate", "Note"]]
    for row in result["nonSargablePredicates"]:
        nonsargable_rows.append([row["job"], row["stage"], row["kind"], row["predicate"], row["note"]])

    or_heavy_rows = [["Job", "Stage", "Kind", "OR Count", "Predicate", "Note"]]
    for row in result["orHeavyPredicates"]:
        or_heavy_rows.append([row["job"], row["stage"], row["kind"], row["orCount"],
                              row["predicate"], row["note"]])

    cartesian_rows = [["Job", "Stage", "Kind", "Note"]]
    for row in result["cartesianRiskJoins"]:
        cartesian_rows.append([row["job"], row["stage"], row["kind"], row["note"]])

    sql_set_rows = [["Job", "Stage", "Kind", "Note"]]
    for row in result["embeddedSqlSetIssues"]:
        sql_set_rows.append([row["job"], row["stage"], row["kind"], row["note"]])

    repeated_within_rows = [["Job", "Fields", "Occurrence Count", "Sample Text", "Note"]]
    for row in result["repeatedExpressionsWithinJob"]:
        repeated_within_rows.append([row["job"], ", ".join(row["fields"]), row["occurrenceCount"],
                                     row["sampleText"], row["note"]])

    merge_rows = [["Group Id", "Kind", "Jobs", "Occurrence Count", "Suggestion"]]
    for row in result["mergeCandidates"]:
        merge_rows.append([row["groupId"], row["kind"], ", ".join(row["jobs"]),
                           row["occurrenceCount"], row["suggestion"]])

    return [
        ("Summary", summary_rows),
        ("Optimization Index", index_rows),
        ("Truncate-Reload Writes", truncate_rows),
        ("SCD2 Candidates", scd2_rows),
        ("Duplicate Queries", dup_rows),
        ("Queries Without Filters", no_filter_rows),
        ("Non-Sargable Predicates", nonsargable_rows),
        ("OR-Heavy Predicates", or_heavy_rows),
        ("Cartesian-Risk Joins", cartesian_rows),
        ("Embedded SQL Set Issues", sql_set_rows),
        ("Repeated Expr. Within Job", repeated_within_rows),
        ("Merge Candidates", merge_rows),
    ]


def run(args: argparse.Namespace) -> int:
    result = build(args.input_json)
    json_path = args.out_prefix.with_suffix(".json")
    xlsx_path = args.out_prefix.with_suffix(".xlsx")
    atomic_json(json_path, result)
    write_xlsx(xlsx_path, workbook(result))
    print("Optimization summary:")
    for key, value in result["summary"].items():
        if not isinstance(value, dict):
            print(f"  {key}: {value}")
        else:
            print(f"  {key}: {dict(value)}")
    print(f"  JSON:  {json_path}")
    print(f"  Excel: {xlsx_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
