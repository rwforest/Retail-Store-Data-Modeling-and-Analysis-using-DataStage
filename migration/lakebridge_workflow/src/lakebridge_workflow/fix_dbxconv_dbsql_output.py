#!/usr/bin/env python3
"""Patch known defects in ``dbxconv``'s generated DBSQL-target output.

``dbxconv``'s ``DBSQL`` target (§8a.2) is BladeBridge's plain-SQL-script conversion,
meant to run on a Databricks SQL Warehouse rather than a cluster or serverless Python
compute -- unlike the ``SPARKSQL`` target (``fix_dbxconv_output.py``), which generates a
Python notebook and needs Spark/Python compute no matter what else is fixed. Running
``dbxconv`` with ``--target DBSQL`` against this repository's own 23 jobs found several
systematic, generalizable defects in the generated SQL text itself -- the same kind of
gap ``fix_dbxconv_output.py`` already fixes for the SPARKSQL target, just in a different
function/type-translation table. This module fixes only those: textual, per-expression
defects that read the same condensed job model the rest of this package already parses,
not anything specific to this pipeline's own columns or logic.

Eleven fixes, all independently callable and independently reported, applied in this order
(``fix_job``):

1. **``Ereplace(x, old, new)`` is left untranslated** (``fix_ereplace_calls``):
   DataStage's BASIC string-replace function has no Databricks SQL equivalent by that
   name; the call survives verbatim into the generated SQL, where it would fail with an
   unresolved-routine error. Renamed to ``REPLACE(x, old, new)``, which has the same
   3-argument signature and semantics -- found reading the generated code (an SCD2 job's
   ``eff_end_key``/``eff_start_key`` derivations, a date-dimension job's ``YearMonth``
   reformat).
2. **``Len(x)`` is left untranslated** (``fix_len_calls``): the same gap as ``Ereplace``,
   for DataStage's string-length function. Renamed to ``LENGTH(x)``.
3. **``NullToValue(x, default)`` is left untranslated** (``fix_null_to_value_calls``):
   the same gap as ``Ereplace``/``Len`` above, for DataStage's null-coalesce function --
   found not by reading the generated code (a plausible-looking 2-argument call, nothing
   about it looks incomplete) but by actually deploying a job that uses it
   (``J03_Land_Products``) and getting back ``UNRESOLVED_ROUTINE``. Renamed to
   ``COALESCE(x, default)``, the same 2-argument signature and semantics.
4. **A run-parameter widget reference is left as a bare placeholder**
   (``fix_widget_defaults``): BladeBridge's DBSQL target emits a job parameter
   (``pEffDate``, ``pRunDate``) as a literal ``{widgetName}`` token -- the same token the
   SPARKSQL target's notebooks resolve via ``dbutils.widgets.get(...)``, but a plain SQL
   script run as a Databricks Jobs SQL-file task has no such mechanism, so the
   placeholder survives verbatim into the deployed script. Casting that literal string to
   ``date``/``int`` doesn't raise an error, it returns ``NULL`` -- invisible until the
   result feeds a downstream join: ``J06_Dim_Customer_SCD2``'s own ``eff_start_key`` for
   every *changed* customer version came out ``NULL``, failing the same point-in-time
   join range filter ``fix_timestamp_to_string_format`` (below) documents, from the
   dimension side this time. Resolved from the job's own declared widget default
   (``job.parameters``) -- the same one-time, non-reusable scope ``--src-dir`` already
   has for ``pSrcDir``.
5. **DataStage's ``:`` string-concatenation operator becomes a literal ``+``**
   (``fix_string_concat_plus``): ``A : "|" : B`` (DataStage BASIC's concatenation
   operator) is correctly recognized as concatenation but mistranslated as numeric
   addition (``A + '|' + B``) instead of ``A || '|' || B`` -- wrong for any Databricks SQL
   engine running in ANSI mode, since ``+`` between a string and a non-numeric-coercible
   value raises a cast error rather than concatenating. Only rewrites a generated line
   whose target column's DataStage derivation actually contains the ``:`` operator (and
   whose ``+`` count matches the derivation's ``:`` count, as a safety check), so a
   genuine numeric ``+`` elsewhere -- `old_version_no + 1`, for example -- is never touched.
6. **``StringToDecimal(x)`` casts with no precision** (``fix_bare_numeric_cast``): the
   same defect ``fix_dbxconv_output.py``'s own version fixes for SPARKSQL -- a bare
   ``CAST(x AS numeric)`` defaults to Spark/Databricks SQL's ``DECIMAL(10, 0)``, silently
   rounding away every decimal place. Fixed the same way: per generated *line*, using the
   declared type of the column the line's expression is assigned to.
7. **``date_format()`` patterns are all upper-case** (``fix_date_format_pattern_case``):
   the same defect ``fix_dbxconv_output.py``'s own version fixes for SPARKSQL, reproduced
   identically here -- ``DateToString`` correctly becomes ``date_format(...)``, but the
   pattern string comes out all-upper-case, and Spark/Databricks SQL's strict parser
   rejects mixing capital ``Y`` (week-based-year) with a bare month.
8. **``TimestampToString`` loses its format** (``fix_timestamp_to_string_format``): the
   same defect ``fix_dbxconv_output.py``'s own version fixes for SPARKSQL, reproduced
   identically here -- ``TimestampToString(ts, "%yyyy%mm%dd")`` becomes a bare
   ``CAST(x AS STRING)`` (Spark's own default timestamp text, e.g.
   ``"2010-12-01 08:26:00"``) instead of the requested ``"20101201"``. Invisible until the
   column feeds a date-range comparison: a point-in-time join's own filter
   (``customer_sk is null or (eff_start_key <= invoice_date_key and eff_end_key >=
   invoice_date_key)``) compares a real ``yyyyMMdd`` boundary against a never-reformatted
   timestamp string, which is ``NULL``, not ``true`` or ``false`` -- so the filter's ``OR``
   never takes its second branch for a row whose join genuinely matched, and
   ``fact_sales`` collapsed from 32575 to 9106 rows with every surviving row's
   ``customer_sk`` defaulted to ``-1``. Found the same way as every other defect here: by
   deploying and reconciling, not by reading the generated code -- a bare ``CAST(... AS
   STRING)`` looks exactly as complete as a correctly-formatted one.
9. **Numeric columns never cast** (``fix_numeric_casts``): the same defect
   ``fix_dbxconv_output.py``'s own version fixes for SPARKSQL (see its docstring, point
   12) -- a bare passthrough or a bare comparison against a literal leaves a numeric
   source column as the literal CSV string it was read as. Must run *after*
   ``reconstruct_job``, not before: a fused source's qualifier (``lnk_x.``) doesn't exist
   in the raw text at all until reconstruction's own ``qualify_bare_dot_fields`` adds it.
10. **``StringToDate()`` loses its format too** (``fix_string_to_date_cast``): the same
   defect ``fix_dbxconv_output.py``'s own version fixes for SPARKSQL -- ``StringToDate(x,
   "%yyyy%mm%dd")`` becomes a bare ``cast(x AS date)``, which happens to work whenever
   ``x`` is already an ISO ``yyyy-MM-dd`` string (most occurrences in this repository) but
   silently returns ``NULL`` on a ``yyyyMMdd``-format date key
   (``J17_Mart_Customer_360``'s ``recency_days``/``r_score``, from a dimension's own
   ``last_date_key`` column). Scoped by the derivation's own fully-qualified value
   expression, so -- like ``fix_numeric_casts`` -- this also has to run after
   reconstruction.
11. **DataStage's empty-string-vs-NULL semantics** (``fix_nonnullable_string_coalesce``):
   the same defect ``fix_dbxconv_output.py``'s own version fixes for SPARKSQL (see its
   docstring, point 9) -- a non-nullable source string column reads as ``""`` in
   DataStage but ``NULL`` in Databricks SQL's CSV reader, which silently breaks any
   ``Len(Trim(x)) = 0``-style check. Found by reconciling ``stg_orders`` itself, the most
   upstream table in the pipeline: a guest checkout's empty ``CustomerID`` produced
   ``customer_id = NULL``/``is_guest = 0`` instead of ``'0'``/``1``, cascading into every
   downstream join keyed on ``customer_id``. Every qualified reference to such a column
   is wrapped in ``coalesce(..., '')`` -- except a reference to the non-preserving side
   of an outer join (``outer_join_nullable_links``), for the same reason as the SPARKSQL
   version. Scoped by the link's own qualified name, so this also has to run after
   reconstruction, last of all, so it can never interfere with the exact-text matches
   ``fix_numeric_casts``/``fix_string_to_date_cast`` look for.

**Reconstruction: the DBSQL output is also made executable, not just textually
corrected.** Earlier work on this module concluded that making the output runnable at
all (no ``;`` between statements, nothing names any intermediate result, every source
read has a completely blank ``FROM`` and no column qualifier) required real knowledge of
dbxconv's own statement-emission rules, not a text-pattern-matching guess -- and left it
unfixed rather than risk a silently-wrong auto-wrapping. That knowledge was then actually
built: a clean test case with no ``PxCopy``/``PxFunnel`` ambiguity
(``J05_Dim_Customer_Initial``, 4 stages) revealed three interacting rules (a source stage
is fused into its consumer rather than getting its own statement; a target stage becomes
a ``SELECT CONCAT_WS(...)`` row-dump; ``PxHead`` has no DBSQL handler and no statement at
all), validated against every job in this repository until zero statements were left
unmatched on either side. ``reconstruct_job`` (and everything it calls -- see the
module-level comment above that function) uses this to turn the fragmented output into
one executable script per job: every statement wrapped in a named ``CREATE OR REPLACE
TEMP VIEW``, every target rewritten into a real ``CREATE OR REPLACE TABLE`` (the same
CSV-dump-to-Delta-table re-targeting ``fix_dbxconv_output.py``'s own
``fix_csv_writes_to_delta`` already does for SPARKSQL), every blank source reference
resolved via ``read_files(...)`` (external reads) or a plain table reference
(intermediate reads, written by an earlier job in the pipeline) using this project's own
``pSrcDir``/``pStgDir``/``pDwhDir``/``pMartDir`` widget conventions -- the same ones
``fix_source_read_paths`` already uses for SPARKSQL. The same validated matching also
made two previously-deferred, block-anchored fixes solvable for the first time:
``Funnel`` reading only its first input link, and Aggregator output always being
``MIN(...)`` (the identical SPARKSQL defects ``fix_funnel_union_all`` and
``fix_aggregator_functions`` already fix -- see their docstrings).

Requires ``--catalog``/``--schema`` (where intermediate and target tables live) and
``--src-dir`` (the resolved value of the ``pSrcDir`` widget, baked in directly -- this is
a one-time validation fixer, not a reusable parameterized template, the same scope
``dbxconv_output_fixed/job_spec.json`` already has for the SPARKSQL deployment).

Example::

    lakebridge-workflow fix-dbxconv-dbsql --dbxconv-dir migrated/dbxconv_dbsql_out \\
        --condensed-dir switch_input --catalog unitygo --schema retail_dw_dbxconv_dbsql \\
        --src-dir /Volumes/unitygo/retail_dw/landing \\
        --out-dir migrated/dbxconv_dbsql_fixed
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

from .common import (
    AGGREGATOR_KIND_RE, BALANCED_EXPR_RE_SRC, DATE_FORMAT_TOKENS, FUNNEL_KIND_RE, HEAD_KIND_RE,
    JOIN_KIND_RE, SORT_KEY_NAME_RE, STRING_SQL_TYPES, CondensedJob, Link, Stage, atomic_json,
    csv_escape, link_order, outer_join_nullable_links, read_condensed_job, spark_numeric_type,
    unwrap_dsx_value,
)

# The same DSX encoding fix_dbxconv_output.py's own fix_aggregator_functions parses for an
# Aggregator's "reduce" property (duplicated rather than cross-imported, matching this
# package's existing per-target-module convention).
AGGREGATOR_REDUCE_SOURCE_RE = re.compile(r"reduce\\\(2\)(\w+)\\\(2\)0((?:\\\(1\)\\\(3\)\\\(3\)\w+\\\(2\)\w+\\\(2\)0)+)")
AGGREGATOR_REDUCE_FUNC_RE = re.compile(r"\\\(3\)\\\(3\)(\w+)\\\(2\)(\w+)\\\(2\)0")

# BladeBridge's DBSQL target renders both of these with a space before the opening
# paren (``Ereplace ( x , old , new )``), unlike SPARKSQL's ``Ereplace(x, old, new)`` --
# tolerate either.
EREPLACE_CALL_RE = re.compile(r"\bEreplace\s*\(")
LEN_CALL_RE = re.compile(r"\bLen\s*\(")
NULL_TO_VALUE_CALL_RE = re.compile(r"\bNullToValue\s*\(")

# DataStage's ``:`` concatenation operator, as it appears in the condensed job's own
# derivation text (``A : "|" : B``, whitespace on both sides) -- the same operator
# ``fix_string_concat_plus`` below looks for to decide whether a generated line's ``+``
# is really concatenation, not DataStage source syntax shared with any other fixer here.
STRING_CONCAT_COLON_RE = re.compile(r"\s:\s")

# Same duplicated-nested-paren concern as fix_dbxconv_output.py's own date-format fix
# (BALANCED_EXPR_RE_SRC, from common.py): a date_format(...) call's first argument can
# itself contain other function calls.
DATE_FORMAT_CALL_RE = re.compile(rf"date_format\(({BALANCED_EXPR_RE_SRC}),\s*'([^']*)'\)")

# The derivation text for a TimestampToString(..., "<format>") column -- same DSX encoding
# and same token table (DATE_FORMAT_TOKENS, from common.py) as fix_dbxconv_output.py's own
# TIMESTAMP_TO_STRING_DERIVATION_RE, duplicated here per this package's existing
# per-target-module convention.
TIMESTAMP_TO_STRING_DERIVATION_RE = re.compile(
    rf'TimestampToString\(({BALANCED_EXPR_RE_SRC}),\s*"([^"]+)"\)'
)

# StringToDate(value, "<format>") -- same format-loss defect as TimestampToString, becoming
# a bare cast(value AS date) instead of a formatted parse. See fix_dbxconv_output.py's own
# STRING_TO_DATE_DERIVATION_RE.
STRING_TO_DATE_DERIVATION_RE = re.compile(rf'StringToDate\(({BALANCED_EXPR_RE_SRC}),\s*"([^"]+)"\)')

# A DataStage run-parameter widget reference, left as a bare brace placeholder in the
# generated SQL text (`'{pEffDate}'`) -- the only `{...}` shape dbxconv's DBSQL target
# ever emits (confirmed against every job in this repository), so this never needs to
# tell a widget reference apart from anything else.
WIDGET_PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dbxconv-dir", type=Path, required=True,
                   help="run-dbxconv's --output directory for a DBSQL conversion (one "
                        "subfolder per job, each containing out/**/<job>.sql)")
    p.add_argument("--condensed-dir", type=Path, required=True,
                   help="directory of the same jobs' condensed .txt specs")
    p.add_argument("--out-dir", type=Path, required=True, help="where to write patched SQL files")
    p.add_argument("--catalog", required=True, help="Unity Catalog catalog for intermediate/target tables")
    p.add_argument("--schema", required=True, help="Unity Catalog schema for intermediate/target tables")
    p.add_argument("--src-dir", required=True,
                   help="resolved value of the pSrcDir widget (a Volume path), baked into "
                        "each read_files(...) call -- this is a one-time validation fixer, "
                        "not a reusable parameterized template")
    return p.parse_args(argv)


def fix_ereplace_calls(code: str) -> tuple[str, int]:
    """Rename every ``Ereplace(x, old, new)`` call to Databricks SQL's ``REPLACE``."""
    return EREPLACE_CALL_RE.subn("REPLACE(", code)


def fix_len_calls(code: str) -> tuple[str, int]:
    """Rename every ``Len(x)`` call to Databricks SQL's ``LENGTH``."""
    return LEN_CALL_RE.subn("LENGTH(", code)


def fix_null_to_value_calls(code: str) -> tuple[str, int]:
    """Rename every ``NullToValue(x, default)`` call to Databricks SQL's ``COALESCE``.

    DataStage's BASIC null-coalesce function, same gap as ``Ereplace``/``Len`` above and
    found the same way -- not by reading the generated code (it looks complete, a
    plausible-sounding function call with the right argument count), but by actually
    deploying a job that uses it (``J03_Land_Products``) and getting
    ``UNRESOLVED_ROUTINE`` back. Same 2-argument signature and semantics as ``COALESCE``,
    so a straight rename is correct regardless of whether the default is a string or a
    number.
    """
    return NULL_TO_VALUE_CALL_RE.subn("COALESCE(", code)


def fix_string_concat_plus(code: str, job: CondensedJob) -> tuple[str, int]:
    """Turn a DataStage ``:``-concatenation mistranslated as ``+`` into ``||``.

    ``A : "|" : B`` (DataStage's string-concatenation operator) comes out as
    ``A + '|' + B`` in the generated SQL -- correctly recognized as an operator, but
    mapped to numeric addition instead of string concatenation. Under ANSI mode, ``+``
    between a string and a value that isn't numeric-coercible raises a cast error rather
    than concatenating, so this is wrong rather than merely different-looking.

    Scoped per generated line, by the target column's own DataStage derivation: only a
    line assigned to a column whose derivation actually contains ``:`` is touched, and
    only if that line's ``+`` count matches the derivation's ``:`` count exactly (DataStage
    syntax requires one ``:`` between every pair of concatenated terms, so N terms need
    N-1 operators -- an exact count match is strong evidence every ``+`` on the line came
    from this same chain, not from some other, genuinely numeric expression folded onto
    the same line).
    """
    concat_cols: set[str] = set()
    for stage in job.stages:
        for lk in stage.links:
            for col in lk.columns:
                if STRING_CONCAT_COLON_RE.search(col.get("derivation") or ""):
                    concat_cols.add(col["name"])
    if not concat_cols:
        return code, 0

    count = 0
    lines = code.split("\n")
    for i, line in enumerate(lines):
        if " + " not in line:
            continue
        m = re.search(r"as\s+(\w+)\s*,?\s*$", line.strip(), re.IGNORECASE)
        if not m or m.group(1) not in concat_cols:
            continue
        n = line.count(" + ")
        lines[i] = line.replace(" + ", " || ")
        count += n
    return "\n".join(lines), count


def fix_bare_numeric_cast(code: str, job: CondensedJob) -> tuple[str, int]:
    """Give a bare ``CAST(x AS numeric)`` the real precision/scale it always drops.

    Same defect, same fix, as ``fix_dbxconv_output.py``'s own ``fix_bare_numeric_cast``
    (see its docstring) -- ``StringToDecimal(x)`` carries no precision in DataStage's own
    syntax, so the generated cast defaults to Databricks SQL's ``DECIMAL(10, 0)``,
    silently rounding away every decimal place of a real money value. Scoped per
    generated line (not per occurrence) using the declared type of the column the line's
    expression is assigned to, the same way, for the same reason: an expression with two
    bare casts belonging to the same target column.
    """
    type_by_col: dict[str, str] = {}
    for stage in job.stages:
        for lk in stage.links:
            for col in lk.columns:
                spark_type = spark_numeric_type(col.get("sqlType"), col.get("precision"), col.get("scale"))
                if spark_type and col["name"] not in type_by_col:
                    type_by_col[col["name"]] = spark_type

    count = 0
    lines = code.split("\n")
    for i, line in enumerate(lines):
        if "as numeric)" not in line.lower():
            continue
        m = re.search(r"as\s+(\w+)\s*,?\s*$", line.strip(), re.IGNORECASE)
        if not m:
            continue
        target_type = type_by_col.get(m.group(1))
        if target_type is None:
            continue
        new_line, n = re.subn(r"as numeric\)", f"as {target_type})", line, flags=re.IGNORECASE)
        if n:
            lines[i] = new_line
            count += n
    return "\n".join(lines), count


def fix_date_format_pattern_case(code: str) -> tuple[str, int]:
    """Fix the letter-casing BladeBridge gets wrong in every ``date_format()`` pattern.

    Same defect, same fix, as ``fix_dbxconv_output.py``'s own
    ``fix_date_format_pattern_case`` (see its docstring) -- reproduced identically in the
    DBSQL target: ``'YYYYMMDD'`` instead of ``'yyyyMMdd'``, which Spark/Databricks SQL's
    strict parser rejects for mixing week-based-year with a bare month. Duplicated here
    rather than imported, matching this package's existing per-target-module convention
    (``fix_dbxconv_output.py`` similarly duplicates small, purely-textual helpers rather
    than cross-importing between the SPARKSQL- and DBSQL-specific modules).
    """
    count = 0

    def repl(m: re.Match) -> str:
        nonlocal count
        pattern = m.group(2)
        fixed = re.sub(r"Y+", lambda mm: mm.group(0).lower(), pattern)
        fixed = re.sub(r"D+", lambda mm: mm.group(0).lower(), fixed)
        fixed = re.sub(r"N+", "mm", fixed)
        fixed = re.sub(r"S+", lambda mm: mm.group(0).lower(), fixed)
        if fixed == pattern:
            return m.group(0)
        count += 1
        return f"date_format({m.group(1)}, '{fixed}')"

    return DATE_FORMAT_CALL_RE.sub(repl, code), count


def fix_timestamp_to_string_format(code: str, job: CondensedJob) -> tuple[str, int]:
    """Restore the DataStage-specified output format for ``TimestampToString`` derivations.

    Same defect, same fix, as ``fix_dbxconv_output.py``'s own
    ``fix_timestamp_to_string_format`` (see its docstring) -- BladeBridge's DBSQL target
    drops the explicit output format entirely too, emitting a bare ``CAST(x AS STRING)``
    -- Spark/Databricks SQL's own default timestamp representation
    (``"2010-12-01 08:26:00"``) -- instead of the format the derivation actually specified
    (``TimestampToString(ts, "%yyyy%mm%dd")`` -> ``"20101201"``). Found by deploying and
    reconciling: a date key derived this way is a point-in-time join's own range-filter
    column (``eff_start_key <= invoice_date_key and eff_end_key >= invoice_date_key``), and
    comparing a real date boundary against a never-reformatted full timestamp string is
    ``NULL``, not ``true`` or ``false`` -- so the filter's own ``customer_sk is null OR
    (...)`` never takes the second branch for a row whose join genuinely matched.
    ``fact_sales`` collapsed from 32575 to 9106 rows, with every surviving row's
    ``customer_sk`` defaulted to ``-1`` even though the dimension row it should have joined
    to genuinely existed. Matched directly against the generated text's own expression
    (whatever its qualifier -- blank for a fused source, same as everywhere else in this
    module), not reconstructed from the qualified name in the derivation, so this runs
    safely before reconstruction like the other textual fixes above, unlike
    ``fix_numeric_casts``.
    """
    count = 0
    for stage in job.stages:
        for lk in stage.links:
            for col in lk.columns:
                m = TIMESTAMP_TO_STRING_DERIVATION_RE.search(col.get("derivation") or "")
                if not m:
                    continue
                spark_fmt = m.group(2)
                for ds_token, spark_token in DATE_FORMAT_TOKENS:
                    spark_fmt = spark_fmt.replace(ds_token, spark_token)
                pattern = re.compile(
                    rf"CAST\(({BALANCED_EXPR_RE_SRC})\s+AS\s+STRING\)\s+as\s+{re.escape(col['name'])}\b",
                    re.IGNORECASE,
                )
                code, n = pattern.subn(
                    rf"date_format(\1, '{spark_fmt}') as {col['name']}", code)
                count += n
    return code, count


def fix_string_to_date_cast(code: str, job: CondensedJob) -> tuple[str, int]:
    """Restore the format on a ``StringToDate(value, "<format>")`` BladeBridge dropped.

    Same defect, same fix, as ``fix_dbxconv_output.py``'s own ``fix_string_to_date_cast``
    (see its docstring) -- ``StringToDate(value, "%yyyy%mm%dd")`` becomes a bare
    ``cast(value AS date)``, which happens to work whenever ``value`` is already an ISO
    ``yyyy-MM-dd`` string (most occurrences in this repository's own widgets and source
    dates) but fails outright -- returning ``NULL``, not an error -- on a
    ``yyyyMMdd``-format date key: ``J17_Mart_Customer_360``'s ``recency_days``/``r_score``
    derivations, from a dimension's own ``last_date_key`` column. Scoped by the derivation's
    own fully-qualified value expression, so (like ``fix_numeric_casts``) this has to run
    after reconstruction: a fused source's qualifier doesn't exist in the raw text at all
    until reconstruction's own ``qualify_bare_dot_fields`` adds it.
    """
    count = 0
    seen: set[tuple[str, str]] = set()
    for stage in job.stages:
        for lk in stage.links:
            for col in lk.columns:
                derivation = col.get("derivation") or ""
                for value_expr, ds_fmt in STRING_TO_DATE_DERIVATION_RE.findall(derivation):
                    value_expr = value_expr.strip()
                    if "." not in value_expr or (value_expr, col["name"]) in seen:
                        continue
                    seen.add((value_expr, col["name"]))
                    spark_fmt = ds_fmt
                    for ds_token, spark_token in DATE_FORMAT_TOKENS:
                        spark_fmt = spark_fmt.replace(ds_token, spark_token)
                    pattern = re.compile(rf"cast\(\s*{re.escape(value_expr)}\s+as\s+date\)", re.IGNORECASE)
                    code, n = pattern.subn(
                        f"to_date(cast({value_expr} as string), '{spark_fmt}')", code)
                    count += n
    return code, count


def fix_widget_defaults(code: str, job: CondensedJob) -> tuple[str, int]:
    """Substitute a run-parameter widget's literal declared default value.

    BladeBridge's DBSQL target emits a job parameter reference (``pEffDate``,
    ``pRunDate``, ...) as a bare ``{widgetName}`` placeholder -- the same token the
    SPARKSQL target's Python notebooks resolve via an f-string and
    ``dbutils.widgets.get(...)``, but a plain SQL script run as a Databricks Jobs
    SQL-file task has no such mechanism: nothing ever supplies a value, so the
    placeholder survives into the deployed script as the literal string ``{pEffDate}``.
    Casting that to ``date``/``int`` doesn't raise an error -- it returns ``NULL`` --
    so this is invisible until a column built from the result feeds a downstream join or
    comparison, found the same way as every other defect here (deploying and
    reconciling, not reading the generated code): ``J06_Dim_Customer_SCD2``'s own
    ``eff_start_key`` for every *changed* customer version (``old_version_no != 0``, i.e.
    every version after a customer's first) came out ``NULL`` instead of the real
    effective date, failing the exact same point-in-time join range filter
    ``fix_timestamp_to_string_format`` documents, just from the dimension side this time
    instead of the fact side. Resolved from the job's own declared widget default
    (``job.parameters``, parsed from the condensed spec's own parameter-declaration
    header) -- the same one-time, non-reusable scope ``--src-dir`` already has for
    ``pSrcDir`` (there's no job-level ``base_parameters``-style override for a SQL-file
    task the way the SPARKSQL deployment's ``job_spec.json`` has for a notebook task).
    """
    defaults = {p["name"]: p["default"] for p in job.parameters}
    count = 0

    def repl(m: re.Match) -> str:
        nonlocal count
        value = defaults.get(m.group(1))
        if value is None:
            return m.group(0)
        count += 1
        return value

    return WIDGET_PLACEHOLDER_RE.sub(repl, code), count


def fix_nonnullable_string_coalesce(code: str, job: CondensedJob) -> tuple[str, int]:
    """Wrap every reference to a non-nullable source string column in ``coalesce(x, '')``.

    Same defect, same fix, as ``fix_dbxconv_output.py``'s own
    ``fix_nonnullable_string_coalesce`` (see its docstring) -- DataStage reads an empty
    field of such a column as ``""``; Databricks SQL's CSV reader (``read_files``, used by
    ``synthesize_source_view`` below, same underlying reader as ``spark.read.csv``) reads
    it as ``NULL`` instead, by the exact same default (its own ``nullValue`` option
    defaults to ``""``). Found by reconciling ``stg_orders`` itself -- the most upstream
    table in the whole pipeline, one line removed from the raw CSV -- not by reading the
    generated code: a guest checkout's empty ``CustomerID`` field produced
    ``customer_id = NULL`` / ``is_guest = 0`` instead of the DataStage derivation's own
    intended ``'0'`` / ``1`` (``If Len(Trim(x)) = 0 Then ... Else ...`` silently takes the
    wrong branch once ``Len(Trim(NULL))`` is ``NULL``, not ``0``), cascading into every
    downstream join keyed on ``customer_id``. Unlike ``fix_dbxconv_output.py``'s version,
    the DBSQL target's generated text is always plain SQL -- there's no PySpark
    DataFrame-API call here to tell apart from ``spark.sql()`` string content, so every
    occurrence takes the same plain ``coalesce(x, '')`` form.

    Skips any link that's the non-preserving side of an outer join elsewhere in this job
    (``outer_join_nullable_links``) -- see ``fix_dbxconv_output.py``'s docstring for why.
    Scoped by the link's own qualified name, so -- like ``fix_numeric_casts`` and
    ``fix_string_to_date_cast`` -- this also has to run after reconstruction.
    """
    nullable_join_links = outer_join_nullable_links(job)
    count = 0
    for stage in job.stages:
        for lk in stage.links:
            if lk.direction != "out" or lk.link_name in nullable_join_links:
                continue
            for col in lk.columns:
                if col.get("nullable") or col.get("sqlType") not in STRING_SQL_TYPES:
                    continue
                pattern = re.compile(rf"\b{re.escape(lk.link_name)}\.{re.escape(col['name'])}\b")
                code, n = pattern.subn(rf"coalesce({lk.link_name}.{col['name']}, '')", code)
                count += n
    return code, count


def fix_numeric_casts(code: str, job: CondensedJob) -> tuple[str, int]:
    """Cast every numeric source-column reference to its declared SQL type.

    Same defect, same fix, as ``fix_dbxconv_output.py``'s own ``fix_numeric_casts`` (see
    its docstring, point 11) -- BladeBridge only casts a column when the DataStage
    expression already contained a function call; a bare passthrough (no ``:=``
    derivation at all, or one that's just another column's name) leaves it as whatever
    string ``read_files`` produced. Found by reconciling: a plain-passthrough
    ``unit_cost`` (``sql=3 prec=18,2``, no derivation) carried the source CSV's full
    floating-point text (``0.21450000000000002``) straight through to the final table
    instead of rounding to its declared 2 decimal places (SDP's own ``0.21``) -- not a
    SQL error, just silently wrong precision, since nothing ever cast it.
    """
    count = 0
    for stage in job.stages:
        for lk in stage.links:
            if lk.direction != "out":
                continue
            for col in lk.columns:
                spark_type = spark_numeric_type(col.get("sqlType"), col.get("precision"), col.get("scale"))
                if spark_type is None:
                    continue
                pattern = re.compile(rf"\b{re.escape(lk.link_name)}\.{re.escape(col['name'])}\b")

                def repl(m: re.Match, _code=code) -> str:
                    before = _code[max(0, m.start() - 6):m.start()].rstrip().upper()
                    if before.endswith("CAST(") or before.endswith(".CAST("):
                        return m.group(0)  # already cast immediately before this reference
                    return f"CAST({m.group(0)} AS {spark_type})"

                code, n = pattern.subn(repl, code)
                count += n
    return code, count


# --- Statement reconstruction (makes the script actually executable) -------------------
#
# dbxconv's DBSQL output has no semicolons and names no intermediate result (module
# docstring, point 1) because of three interacting, but fully model-derivable, rules --
# found by constructing a clean test case with no PxCopy/PxFunnel ambiguity
# (J05_Dim_Customer_Initial, 4 stages) and then validating the rule against every
# increasingly complex job in this repository until every statement in every file (23
# parallel jobs) matched its declared stage with zero left over on either side:
#
# 1. A stage with no in-links (a plain external/intermediate-table read) never gets its
#    own statement -- it's fused directly into each of its consumer(s)' own statement(s),
#    which is why a blank ``FROM`` and dangling ``.field`` references aren't confined to
#    a source's own (nonexistent) statement; they show up whatever statement reads it.
# 2. A stage with no out-links (a file/audit target) gets exactly one statement, always
#    shaped ``SELECT CONCAT_WS(',', <link>.<col>, ...) FROM <link>`` -- a CSV-row-dump,
#    DataStage's own file-target semantics, never referenced by anything later.
# 3. ``PxHead`` (Top-N) has no handler in the DBSQL config either (the same gap
#    ``fix_dbxconv_output.py``'s ``fix_head_stages`` found in SPARKSQL) -- no statement of
#    any kind, not even a broken one.
# Every other stage gets exactly one statement per output link, matchable to its stage
# unambiguously by its generated column-alias list plus its ``FROM`` target (tolerating a
# blank target exactly when that stage's in-link traces to a rule-1 fused source) --
# *regardless* of emission order, which turns out not to be simple file-declaration
# order (a stage's statement is emitted as soon as its own inputs are ready, not in the
# order DataStage happened to declare it).
#
# A join with *both* sides fused (``J09_Dim_Date_Promo_FX``'s ``jn_Country_FX``, joining
# two plain external reads) renders with no table name on either side of ``JOIN`` at all,
# and its own join condition and SELECT list reference both sides with the same bare,
# unqualified ``.field`` -- genuinely ambiguous from the text alone when (as here) both
# sides share a field name (``currency``). Resolved the same way every other defect in
# this package is: from the condensed job model, never by guessing from the generated
# text -- each output column's own derivation (``col.derivation``, e.g. ``lnk_fx.currency``)
# states which side it came from even when the generated text doesn't, and the join
# stage's own ``key``/``operator`` properties state the join condition directly.

SQL_RESERVED_AFTER_FROM = {"WHERE", "GROUP", "ORDER", "LEFT", "RIGHT", "INNER", "FULL", "ON", "AND"}
# PxHead's own "no handler" marker (rule 3: no statement of any kind for a Head stage) is
# a free-floating comment between two real statements in the raw text, belonging to
# neither -- split_statements() has no reason to treat it as a boundary (it isn't one),
# so left alone it gets silently absorbed into whichever statement happens to precede it,
# a syntax error once that statement is wrapped as a view. Stripped before splitting;
# synthesize_head_statement (rule 3) is what actually implements the stage, independently,
# from the condensed model.
HEAD_COMMENT_RE = re.compile(r"^// \*+\s*No handler defined for type HEAD,.*\*+[ \t]*\n?", re.MULTILINE)
TYPE_CAST_KEYWORDS = {"date", "numeric", "int", "bigint", "string", "double", "decimal", "timestamp"}
JOIN_OPERATOR_SQL = {
    "innerjoin": "INNER JOIN", "leftouterjoin": "LEFT JOIN",
    "rightouterjoin": "RIGHT JOIN", "fullouterjoin": "FULL JOIN",
}
SOURCE_FILE_RE = re.compile(r"#(\w+)#(/.*)$")
FROM_TARGET_RE = re.compile(r"\bFROM\s*\n?\s*(\w+)")
BARE_DOT_FIELD_RE = re.compile(r"(?<!\w)\.(\w+)")
TRAILING_ALIAS_RE = re.compile(r"\bas\s+(\w+)\s*,?\s*$", re.IGNORECASE)
DERIVATION_LEADING_QUALIFIER_RE = re.compile(r"(\w+)\.\w+")


def split_statements(code: str) -> list[str]:
    """Split dbxconv's semicolon-less DBSQL output into top-level statements.

    Splits before a line that starts a new ``SELECT`` -- but only at paren depth 0, so a
    ``RemoveDuplicates`` stage's own ``SELECT ... FROM (SELECT ... ) alias WHERE ...``
    dedup subquery (whose *inner* SELECT also starts a line) stays part of the same
    statement as its outer wrapper.
    """
    lines = code.split("\n")
    statements: list[str] = []
    current: list[str] = []
    depth = 0
    for line in lines:
        if line.lstrip().startswith("SELECT") and depth == 0 and any(l.strip() for l in current):
            statements.append("\n".join(current))
            current = []
        current.append(line)
        depth += line.count("(") - line.count(")")
    if any(l.strip() for l in current):
        statements.append("\n".join(current))
    return [s for s in statements if s.strip()]


def source_out_links(job: CondensedJob) -> set[str]:
    """Out-link names of every stage with no in-links (rule 1: fused into consumers)."""
    links: set[str] = set()
    for stage in job.stages:
        if any(lk.direction == "in" for lk in stage.links):
            continue
        links.update(lk.link_name for lk in stage.links if lk.direction == "out")
    return links


def statement_signature(statement: str) -> tuple[str, str | None, list[str]]:
    is_dump = "CONCAT_WS" in statement
    frm_m = FROM_TARGET_RE.search(statement)
    frm = frm_m.group(1) if frm_m else None
    if frm in SQL_RESERVED_AFTER_FROM:
        frm = None
    if is_dump:
        cols = re.findall(r"(\w+)\.(\w+)", statement.split("FROM")[0])
        return "DUMP", frm, [c[1] for c in cols]
    aliases = [a for a in re.findall(r"\bas\s+(\w+)", statement, re.IGNORECASE)
               if a.lower() not in TYPE_CAST_KEYWORDS]
    return "STMT", frm, aliases


def match_statements_to_stages(
    statements: list[str], job: CondensedJob, fused: set[str],
) -> dict[tuple[str, str | None], int]:
    """Map (stage name, out-link-or-None-for-a-target) -> the one matching statement index.

    Validated (see the module-level comment above) against every job in this repository:
    zero statements left unmatched, zero expected stages left without a statement, for
    every job with no PxHead stage (PxHead's own absence is rule 3, handled separately).
    """
    sigs = [statement_signature(s) for s in statements]
    used: set[int] = set()
    matches: dict[tuple[str, str | None], int] = {}
    for stage in job.stages:
        in_links = [lk.link_name for lk in stage.links if lk.direction == "in"]
        out_links = [lk for lk in stage.links if lk.direction == "out"]
        if not in_links or HEAD_KIND_RE.search(stage.kind):
            continue
        stage_fused = any(l in fused for l in in_links)
        if not out_links:
            targets = [i for i, (kind, frm, _) in enumerate(sigs)
                       if i not in used and kind == "DUMP" and (frm in in_links or (stage_fused and frm is None))]
            if targets:
                used.add(targets[0])
                matches[(stage.name, None)] = targets[0]
            continue
        for ol in out_links:
            cols = [c["name"] for c in ol.columns]
            targets = [i for i, (kind, frm, aliases) in enumerate(sigs)
                       if i not in used and kind == "STMT" and aliases == cols
                       and (frm in in_links or (stage_fused and frm is None))]
            if targets:
                used.add(targets[0])
                matches[(stage.name, ol.link_name)] = targets[0]
    return matches


def resolve_file_widget(stage: Stage) -> tuple[str, str] | None:
    """(widget, relative-path) from a stage's own ``file`` link property.

    A source stage's ``file`` lives on its *out*-link (the one real link it has); a
    target stage has no out-link at all, so the identical property lives on its *in*-link
    instead -- checking out-links first (a source's own, more common case) and falling
    back to in-links covers both without needing to know which kind of stage this is.
    """
    for lk in stage.links:
        if lk.direction != "out":
            continue
        raw = dict(lk.properties).get("file")
        if raw is None:
            continue
        m = SOURCE_FILE_RE.match(unwrap_dsx_value(raw).strip())
        if m:
            return m.group(1), m.group(2)
    for lk in stage.links:
        if lk.direction != "in":
            continue
        raw = dict(lk.properties).get("file")
        if raw is None:
            continue
        m = SOURCE_FILE_RE.match(unwrap_dsx_value(raw).strip())
        if m:
            return m.group(1), m.group(2)
    return None


def synthesize_source_view(stage: Stage, out_link: Link, catalog: str, schema: str, src_dir: str) -> str:
    """A fused source stage never gets a statement of its own (rule 1) -- synthesize one
    directly from the model, something the raw text never has to begin with, not patch
    anything dbxconv generated.

    A ``pSrcDir`` read is a genuine external file: ``read_files`` with an explicit
    positional ``schema`` (``_c0``, ``_c1``, ... -- DataStage maps columns by position,
    not by header text, the same reasoning ``fix_dbxconv_output.py``'s
    ``fix_source_read_paths`` already documents) renamed to this stage's own declared
    field names in declaration order. ``header => true`` is still needed *despite* the
    explicit schema -- it's what makes ``read_files`` skip the file's own physical header
    row rather than read it as an extra data row (confirmed only by reconciling: every
    source-read table came out with exactly one extra row versus SDP, every single-column
    numeric/date dimension off by exactly one for the same reason -- `header => false`
    was tried first and is wrong for a source that genuinely has a header line, the exact
    defect ``fix_dbxconv_output.py``'s own ``fix_source_read_paths`` already documents
    finding for the SPARKSQL target; this target needed the identical fix a second time).
    Any other widget (``pStgDir``/``pDwhDir``/``pMartDir``/``pAuditDir``) reads what an
    *earlier* job in the pipeline wrote -- a plain Delta table reference, the same
    widget-to-table mapping ``fix_source_read_paths`` uses.
    """
    widget_info = resolve_file_widget(stage)
    cols = out_link.columns
    if widget_info is None:
        select_list = ", ".join(c["name"] for c in cols) or "*"
        return (f"CREATE OR REPLACE TEMP VIEW {out_link.link_name} AS (\n"
                f"  SELECT {select_list} FROM {{{{ unresolved source for {stage.name} }}}}\n);")
    widget, relative = widget_info
    if widget == "pSrcDir":
        # Give each positional column its declared type right here -- fix_numeric_casts
        # (run earlier in fix_job) only sees text dbxconv itself generated; a stage's own
        # synthesized source view is generated *after* that pass, bypassing it entirely,
        # the same gap for the same reason the module docstring's header-row fix needed a
        # second pass of something fix_dbxconv_output.py already fixed once for SPARKSQL.
        parts = []
        for i, c in enumerate(cols):
            spark_type = spark_numeric_type(c.get("sqlType"), c.get("precision"), c.get("scale"))
            ref = f"CAST(_c{i} AS {spark_type})" if spark_type else f"_c{i}"
            parts.append(f"{ref} AS {c['name']}")
        renamed = ", ".join(parts)
        positional_schema = ", ".join(f"_c{i} string" for i in range(len(cols)))
        return (
            f"CREATE OR REPLACE TEMP VIEW {out_link.link_name} AS (\n"
            f"  SELECT {renamed}\n"
            f"  FROM read_files('{src_dir}{relative}', format => 'csv', header => true,\n"
            f"    schema => '{positional_schema}', escape => '\"')\n"
            f");"
        )
    table = relative.lstrip("/").rsplit(".", 1)[0]
    return (f"CREATE OR REPLACE TEMP VIEW {out_link.link_name} AS (\n"
            f"  SELECT * FROM {catalog}.{schema}.{table}\n);")


def fill_blank_from_targets(statement: str, names: list[str]) -> str:
    """Insert each fused in-link's real name where dbxconv left the slot after ``FROM``
    (or a join's own target slot) completely blank -- never touches a slot that already
    has a real name in it, and never touches a ``FROM (`` that opens a subquery (a
    ``RemoveDuplicates`` stage's own dedup wrapper, e.g. ``FROM (SELECT ... FROM <blank>)
    alias WHERE ...`` -- the *outer* ``FROM (`` is never blank, only the *inner* one can
    be, and both look identical -- no word character immediately follows either -- unless
    the immediately-following character is checked for ``(``).
    """
    remaining = list(names)

    # Walk every "FROM"/"JOIN" keyword in textual order, filling the next blank slot (no
    # real name before the next keyword/end) with the next still-unfilled fused name.
    pattern = re.compile(r"\b(FROM|JOIN)\b(\s*\n?\s*)(\(?)(\w*)")

    def filler(m: re.Match) -> str:
        keyword, gap, paren, token = m.group(1), m.group(2), m.group(3), m.group(4)
        if paren:
            return m.group(0)  # opens a subquery -- never a blank slot to fill
        if token and token.upper() not in SQL_RESERVED_AFTER_FROM:
            return m.group(0)  # already has a real name
        if not remaining:
            return m.group(0)
        name = remaining.pop(0)
        return f"{keyword}{gap if gap.strip() else ' '}{name}{(' ' + token) if token else ''}"

    return pattern.sub(filler, statement)


def qualify_bare_dot_fields(statement: str, job: CondensedJob, stage: Stage, fused_links: list[str]) -> str:
    """Give every dangling ``.field`` (dbxconv's blank qualifier for a fused source) the
    real link name it was always meant to have.

    A single fused in-link is unambiguous -- every bare dot in the whole statement
    belongs to it. Two (``J09_Dim_Date_Promo_FX``'s ``jn_Country_FX``, joining two plain
    reads) is genuinely ambiguous from the generated text alone when, as there, both
    sides share a field name -- resolved per output column from the condensed model's own
    derivation text (``col.derivation``, e.g. ``lnk_fx.currency``), which states which
    fused side a column came from even though the generated text doesn't.
    """
    if len(fused_links) == 1:
        return BARE_DOT_FIELD_RE.sub(rf"{fused_links[0]}.\1", statement)

    out_links = [lk for lk in stage.links if lk.direction == "out"]
    deriv_by_col: dict[str, str] = {}
    for ol in out_links:
        for col in ol.columns:
            m = DERIVATION_LEADING_QUALIFIER_RE.search(col.get("derivation") or "")
            if m and m.group(1) in fused_links:
                deriv_by_col[col["name"]] = m.group(1)

    lines = statement.split("\n")
    for i, line in enumerate(lines):
        if "." not in line:
            continue
        m = TRAILING_ALIAS_RE.search(line.strip())
        qualifier = deriv_by_col.get(m.group(1)) if m else None
        if qualifier:
            lines[i] = BARE_DOT_FIELD_RE.sub(rf"{qualifier}.\1", line)
    statement = "\n".join(lines)

    # The join's own ON condition has no "as <col>" anchor -- rebuilt directly from the
    # stage's key/operator properties instead of guessing which bare dot is which side.
    if JOIN_KIND_RE.search(stage.kind):
        keys = SORT_KEY_NAME_RE.findall(stage.prop_map.get("key", ""))
        if keys and BARE_DOT_FIELD_RE.search(statement):
            in_links = [lk.link_name for lk in stage.links if lk.direction == "in"]
            if len(in_links) == 2:
                left, right = in_links
                on_clause = " AND ".join(f"{left}.{k} = {right}.{k}" for k in keys)
                statement = re.sub(r"\bON\b.*$", f"ON {on_clause}", statement, count=1, flags=re.MULTILINE | re.DOTALL)
    return statement


def fix_funnel_statement(statement: str, stage: Stage) -> str:
    """``Funnel`` only ever reads its first input link in the generated DBSQL, silently
    dropping the rest -- the identical defect ``fix_dbxconv_output.py``'s own
    ``fix_funnel_union_all`` finds and fixes for SPARKSQL (see its docstring), reproduced
    here. Deferred in this module's first pass (module docstring, point 1) because there
    was no reliable way to anchor a fix on *this specific* statement without first solving
    the statement-to-stage matching problem -- now solved, so this is no longer deferred.
    Same fix: reuse the one matched branch's SELECT list for every other declared
    in-link, swapping only the source reference, since a Funnel's own semantics require
    every input to already share the output schema.
    """
    in_links = [lk.link_name for lk in stage.links if lk.direction == "in"]
    if len(in_links) < 2:
        return statement
    first_in = in_links[0]
    m = re.search(rf"(SELECT\n.*?)\nFROM\s*\n?\s*{re.escape(first_in)}\b", statement, re.DOTALL)
    if not m:
        return statement
    select_block = m.group(1)
    alias_re = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(first_in)}\.")
    branches = [f"{select_block}\nFROM {first_in}"]
    for other in in_links[1:]:
        branches.append(f"{alias_re.sub(f'{other}.', select_block)}\nFROM {other}")
    return statement[:m.start()] + "\nUNION ALL\n".join(branches) + statement[m.end():]


def fix_aggregator_statement(statement: str, stage: Stage) -> str:
    """Aggregator output is always ``MIN(...)`` in the generated DBSQL regardless of the
    stage's actual ``selection``/``reduce`` property -- the identical defect
    ``fix_dbxconv_output.py``'s own ``fix_aggregator_functions`` finds and fixes for
    SPARKSQL (see its docstring), reproduced here and deferred for the same
    statement-matching reason as the Funnel fix above, now solved the same way.
    """
    out_links = [lk for lk in stage.links if lk.direction == "out"]
    if not out_links:
        return statement
    correct: dict[str, str] = {}
    if stage.prop_map.get("selection", "").strip() == "count":
        if out_links[0].columns:
            correct[out_links[0].columns[-1]["name"]] = "COUNT(*)"
    else:
        raw = stage.prop_map.get("reduce", "")
        for src_col, rest in AGGREGATOR_REDUCE_SOURCE_RE.findall(raw):
            for fn, out_col in AGGREGATOR_REDUCE_FUNC_RE.findall(rest):
                correct[out_col] = f"{fn.upper()}({src_col})"
    for out_col, replacement in correct.items():
        statement = re.sub(rf"MIN\(\s*(?:\w+\.)?{re.escape(out_col)}\s*\) as {re.escape(out_col)}",
                            f"{replacement} as {out_col}", statement)
    return statement


def rebuild_regular_statement(statement: str, job: CondensedJob, stage: Stage, fused: set[str]) -> str:
    in_links = [lk.link_name for lk in stage.links if lk.direction == "in"]
    fused_links = [l for l in in_links if l in fused]
    if fused_links:
        statement = fill_blank_from_targets(statement, fused_links)
        statement = qualify_bare_dot_fields(statement, job, stage, fused_links)
    if FUNNEL_KIND_RE.search(stage.kind):
        statement = fix_funnel_statement(statement, stage)
    if AGGREGATOR_KIND_RE.search(stage.kind):
        statement = fix_aggregator_statement(statement, stage)
    return statement


def rebuild_target_statement(statement: str, stage: Stage, catalog: str, schema: str) -> str:
    """A target's ``SELECT CONCAT_WS(...) FROM <link>`` row-dump (rule 2, DataStage's own
    file-target semantics) rewritten into a real Delta table write -- the same
    CSV-dump-to-Delta-table re-targeting ``fix_dbxconv_output.py``'s own
    ``fix_csv_writes_to_delta`` already does for the SPARKSQL target, using the target's
    own ``file`` property for the table name the same way.
    """
    in_links = [lk for lk in stage.links if lk.direction == "in"]
    cols = in_links[0].columns if in_links else []
    widget_info = resolve_file_widget(stage)
    table = widget_info[1].lstrip("/").rsplit(".", 1)[0] if widget_info else stage.name
    in_link = in_links[0].link_name if in_links else "?"
    select_list = ",\n  ".join(f"{in_link}.{c['name']} AS {c['name']}" for c in cols) or "*"
    return (f"CREATE OR REPLACE TABLE {catalog}.{schema}.{table} AS\n"
            f"SELECT\n  {select_list}\nFROM {in_link};")


def synthesize_head_statement(stage: Stage, order: dict[str, str | None]) -> str:
    """``PxHead`` (Top-N) has no DBSQL handler at all (rule 3) -- no statement of any
    kind, not even a broken one. Same translation as ``fix_dbxconv_output.py``'s own
    ``fix_head_stages`` for SPARKSQL: ``SELECT * FROM <input> [ORDER BY <upstream sort>]
    LIMIT <nrecs>``.
    """
    in_links = [lk for lk in stage.links if lk.direction == "in"]
    out_links = [lk for lk in stage.links if lk.direction == "out"]
    in_link = in_links[0].link_name
    out_link = out_links[0].link_name
    nrecs = stage.prop_map.get("nrecs", "").strip() or "1"
    order_clause = order.get(in_link)
    order_sql = f"\n  ORDER BY {order_clause}" if order_clause else ""
    return f"CREATE OR REPLACE TEMP VIEW {out_link} AS (\n  SELECT * FROM {in_link}{order_sql}\n  LIMIT {nrecs}\n);"


def stage_topological_order(job: CondensedJob) -> list[Stage]:
    """Stages ordered so every in-link is defined by an earlier stage's statement --
    *not* assumed to be dbxconv's own file order, which turns out to be a
    dependency-ready order, not DataStage's declaration order (see the module-level
    comment above).
    """
    producer_of: dict[str, int] = {}
    for i, stage in enumerate(job.stages):
        for lk in stage.links:
            if lk.direction == "out":
                producer_of[lk.link_name] = i
    order: list[int] = []
    visited: set[int] = set()

    def visit(i: int) -> None:
        if i in visited:
            return
        visited.add(i)
        for lk in job.stages[i].links:
            if lk.direction == "in" and lk.link_name in producer_of:
                visit(producer_of[lk.link_name])
        order.append(i)

    for i in range(len(job.stages)):
        visit(i)
    return [job.stages[i] for i in order]


def reconstruct_job(job: CondensedJob, code: str, catalog: str, schema: str, src_dir: str) -> tuple[str, int]:
    """Turn dbxconv's fragmented, semicolon-less DBSQL output into one executable script:
    one ``CREATE OR REPLACE TEMP VIEW``/``TABLE`` statement per stage, in dependency
    order, every blank source reference and dangling ``.field`` resolved. See the
    module-level comment above for the three rules this is built on and how they were
    found and validated.
    """
    fused = source_out_links(job)
    statements = split_statements(HEAD_COMMENT_RE.sub("", code))
    matches = match_statements_to_stages(statements, job, fused)
    order = link_order(job)

    out: list[str] = []
    unresolved = 0
    for stage in stage_topological_order(job):
        in_links = [lk.link_name for lk in stage.links if lk.direction == "in"]
        out_links = [lk for lk in stage.links if lk.direction == "out"]
        if not in_links:
            for ol in out_links:
                out.append(synthesize_source_view(stage, ol, catalog, schema, src_dir))
            continue
        if HEAD_KIND_RE.search(stage.kind):
            out.append(synthesize_head_statement(stage, order))
            continue
        if not out_links:
            idx = matches.get((stage.name, None))
            if idx is None:
                unresolved += 1
                continue
            out.append(rebuild_target_statement(statements[idx], stage, catalog, schema))
            continue
        for ol in out_links:
            idx = matches.get((stage.name, ol.link_name))
            if idx is None:
                unresolved += 1
                continue
            rebuilt = rebuild_regular_statement(statements[idx], job, stage, fused)
            out.append(f"CREATE OR REPLACE TEMP VIEW {ol.link_name} AS (\n{rebuilt}\n);")
    return "\n\n".join(out), unresolved


def find_job_sql(dbxconv_dir: Path, job_name: str) -> Path | None:
    job_dir = dbxconv_dir / job_name / "out"
    if not job_dir.is_dir():
        return None
    candidates = list(job_dir.rglob("*.sql"))
    return candidates[0] if len(candidates) == 1 else None


def fix_job(job_name: str, sql_path: Path, job: CondensedJob, catalog: str, schema: str, src_dir: str) -> dict[str, Any]:
    code = sql_path.read_text(encoding="utf-8")

    code, ereplace_calls = fix_ereplace_calls(code)
    code, len_calls = fix_len_calls(code)
    code, null_to_value_calls = fix_null_to_value_calls(code)
    code, widget_defaults = fix_widget_defaults(code, job)
    code, string_concats = fix_string_concat_plus(code, job)
    code, bare_numeric_casts = fix_bare_numeric_cast(code, job)
    code, date_format_cases = fix_date_format_pattern_case(code)
    # Safe before reconstruction: matched against the generated text's own expression
    # (BALANCED_EXPR_RE_SRC captures it verbatim, qualifier and all), not reconstructed
    # from the derivation's qualified name, so a fused source's still-blank qualifier
    # doesn't matter here the way it does for fix_numeric_casts/fix_string_to_date_cast
    # below.
    code, timestamp_to_string_formats = fix_timestamp_to_string_format(code, job)
    # Must run before reconstruction: reconstruction matches each dbxconv-generated
    # statement to its stage by its *already-fixed* column-alias list (unaffected by a
    # CAST, which never changes an alias name) and wraps it as-is, so these need to have
    # already landed.
    code, unresolved = reconstruct_job(job, code, catalog, schema, src_dir)
    # Must run *after* reconstruction, not before: a fused source's qualifier (`lnk_x.`)
    # doesn't exist in the raw text at all until reconstruction's own qualify_bare_dot_fields
    # adds it -- the `\blnk_x\.col\b`-style pattern these need can't match a still-bare `.col`.
    code, numeric_casts = fix_numeric_casts(code, job)
    code, string_to_date_casts = fix_string_to_date_cast(code, job)
    # Runs last: wraps a column reference in coalesce(..., ''), which would otherwise
    # break the exact-text matches fix_string_to_date_cast/fix_numeric_casts look for if
    # it ran before them (in this repository's own jobs the two never target the same
    # column -- string vs numeric declared types -- but nothing guarantees that for every
    # DataStage source, so this runs after both regardless).
    code, nonnullable_string_coalesces = fix_nonnullable_string_coalesce(code, job)

    return {
        "job": job_name, "patchedCode": code, "unresolvedStatements": unresolved,
        "fixes": {
            "ereplaceCalls": ereplace_calls, "lenCalls": len_calls,
            "nullToValueCalls": null_to_value_calls, "widgetDefaults": widget_defaults,
            "stringConcats": string_concats, "bareNumericCasts": bare_numeric_casts,
            "dateFormatCases": date_format_cases,
            "timestampToStringFormats": timestamp_to_string_formats,
            "numericCasts": numeric_casts, "stringToDateCasts": string_to_date_casts,
            "nonnullableStringCoalesces": nonnullable_string_coalesces,
        },
    }


def run(args: argparse.Namespace) -> int:
    if not args.dbxconv_dir.is_dir():
        raise SystemExit(f"error: --dbxconv-dir not found: {args.dbxconv_dir}")
    if not args.condensed_dir.is_dir():
        raise SystemExit(f"error: --condensed-dir not found: {args.condensed_dir}")
    args.out_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    for txt in sorted(args.condensed_dir.glob("*.txt")):
        try:
            job = read_condensed_job(txt)
        except ValueError:
            continue
        if job.kind == "SEQUENCE":
            # A sequence has no ETL stages of its own -- CJSExecCmdActivity, CJSJobActivity,
            # CJSSequencer, ... are orchestration steps, not Px* stages that read or write
            # rows. dbxconv's DBSQL target emits an empty .sql file for one (confirmed: a
            # sequence's own output is a Databricks Workflow JSON instead, §8a.2), so this
            # reconstruction -- built entirely around per-stage SELECT statements -- doesn't
            # apply and has nothing to resolve.
            continue
        sql_path = find_job_sql(args.dbxconv_dir, job.name)
        if sql_path is None:
            continue
        result = fix_job(job.name, sql_path, job, args.catalog, args.schema, args.src_dir)
        (args.out_dir / f"{job.name}.sql").write_text(result["patchedCode"], encoding="utf-8")
        results.append(result)

    fix_keys = ["ereplaceCalls", "lenCalls", "nullToValueCalls", "widgetDefaults",
                "stringConcats", "bareNumericCasts", "dateFormatCases",
                "timestampToStringFormats", "numericCasts", "stringToDateCasts",
                "nonnullableStringCoalesces"]
    report = {
        "summary": {
            "jobsPatched": len(results),
            "jobsWithUnresolvedStatements": sum(1 for r in results if r["unresolvedStatements"]),
            **{f"total{key[0].upper()}{key[1:]}": sum(r["fixes"][key] for r in results) for key in fix_keys},
        },
        "jobs": [{"job": r["job"], "unresolvedStatements": r["unresolvedStatements"], **r["fixes"]}
                 for r in results],
    }
    report_path = args.out_dir.parent / f"{args.out_dir.name}_report.json"
    atomic_json(report_path, report)
    csv_path = args.out_dir.parent / f"{args.out_dir.name}_report.csv"
    header = ["job", "unresolvedStatements", *fix_keys]
    rows = [header] + [[r["job"], str(r["unresolvedStatements"]), *(r["fixes"][key] for key in fix_keys)]
                        for r in results]
    csv_path.write_text("\n".join(",".join(csv_escape(v) for v in row) for row in rows) + "\n", encoding="utf-8")

    print(f"Reconstructed {len(results)} job(s) into one executable script each.")
    for key, value in report["summary"].items():
        print(f"  {key}: {value}")
    print(f"  SQL files: {args.out_dir}")
    print(f"  Report JSON: {report_path}")
    print(f"  Report CSV:  {csv_path}")
    return 1 if report["summary"]["jobsWithUnresolvedStatements"] else 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
