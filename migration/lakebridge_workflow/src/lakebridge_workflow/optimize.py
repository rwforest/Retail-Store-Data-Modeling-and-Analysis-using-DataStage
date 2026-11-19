#!/usr/bin/env python3
"""Flag migration-time optimization opportunities in a consolidated analyzer report.

This is a second, independent scoring pass over the same JSON that ``build-hierarchy``
consumes (``consolidated_all_jobs.json`` from ``analyze``/``analyze-parallel`` +
``merge-reports``, or the official analyzer's own output). It does not change any
generated notebook or converted job; ``run-dbxconv``/``dbxconv`` stays a literal
lift-and-shift. This pass is about what is worth re-architecting *after* the lift-and-shift
lands, scored the same way the Lakebridge Analyzer scores complexity (HIGH/MEDIUM/LOW),
so the two reports can be read side by side.

Four heuristics, each looking for a known DataStage-to-Databricks win:

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
                           "text": str(r.get("value") or "")}
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

    truncate_by_job: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in truncate_reload:
        truncate_by_job[str(row["job"])].append(row)
    scd2_jobs = {str(row["job"]) for row in scd2_candidates}
    no_filter_by_job: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in no_filter:
        no_filter_by_job[str(row["job"])].append(row)
    duplicate_groups_by_job: dict[str, set[str]] = defaultdict(set)
    for group in duplicate_queries:
        for job in group["jobs"]:
            duplicate_groups_by_job[job].add(group["groupId"])

    optimization_index: list[dict[str, Any]] = []
    for item in inventory:
        job = str(item.get("name") or "")
        truncates = truncate_by_job.get(job, [])
        is_scd2 = job in scd2_jobs
        no_filter_hits = no_filter_by_job.get(job, [])
        dup_groups = duplicate_groups_by_job.get(job, set())

        score = (
            (4 if is_scd2 else 0)
            + min(len(truncates), 3) * 2
            + min(len(no_filter_hits), 3) * 2
            + len(dup_groups) * 2
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
