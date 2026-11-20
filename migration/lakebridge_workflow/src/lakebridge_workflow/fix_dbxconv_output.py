#!/usr/bin/env python3
"""Patch known defects in ``dbxconv``'s generated SPARKSQL-target notebooks.

``dbxconv`` (BladeBridge's offline DataStage converter, §8a) is a literal lift-and-shift:
every job becomes a notebook with no re-architecture. Running it against this repository's
own 23 jobs (never done before this script existed -- see ``migration/README.md`` §8a.5)
found that its raw output does not run at all: 11 of 23 jobs fail to even parse as Python,
and all 23 share defects that would silently produce wrong data or the wrong storage shape
if deployed as-is. None of this is pipeline-specific -- every defect traces to a gap in how
BladeBridge's ``SPARKSQL`` config handles a DataStage stage type, not to anything about this
job's particular columns or logic -- so every fix here reads the same condensed job
specification (``common.CondensedJob``) that the rest of this package already parses, and
works the same way for any DataStage source converted with the same target.

Eleven fixes, applied in order, each independently callable and independently reported:

1. **Blank attribute references** (``fix_blank_attribute_refs``): a target stage reading
   from an Aggregator emits one extra ``lnk_x..alias('lnk_x')`` line per aggregated column
   -- a syntax error (an attribute access with no name between the dots). In every observed
   case this is a pure duplicate: the same columns, correctly named, already follow in the
   same ``.select(...)`` call. Safe to delete outright.
2. **Unresolved source read paths, a wrong header option, a missing quote-escape option,
   and intermediate reads left as CSV** (``fix_source_read_paths``): four defects on the
   one generated line. The path is never substituted (literal ``load(f'''%PATH%''')``) --
   the parameter substitution that correctly fires on the *write* side of every target
   never fires on the *read* side; resolved from the source stage's own ``file`` property.
   The same line also hardcodes ``option('header', 'false')`` even when the source
   genuinely has a header row (confirmed against this repository's own landing volume) --
   backwards from what "DataStage maps columns by position" (§4 #1) actually means: it
   skips the header row, it doesn't imply there isn't one; flipped to `true`. It never sets
   an ``escape`` option either, so a field with an RFC 4180-escaped quote (``""`` for a
   literal ``"``) comes out with the escaping characters still in it, because Spark's CSV
   reader defaults to backslash escaping, not doubled-quote; added. And a stage reading
   what an *earlier job in the same pipeline* wrote -- through the intermediate-file
   widgets (``pStgDir``/``pDwhDir``/``pMartDir``/``pAuditDir``) rather than the one genuine
   external source (``pSrcDir``) -- is left reading a CSV file at that widget's (useless,
   Windows-path) default, instead of the Delta table fix #9 below turns the *matching
   write* into. Unlike every other defect here, that one doesn't raise a clean Python
   exception; it fails with an opaque, non-retriable Databricks platform error that
   reproduces identically whether the job runs alone or concurrently with others -- found
   by actually deploying and running this, and only pinned down by isolating it to one job
   running by itself to rule out a concurrency/resource explanation first.
3. **Unhandled ``PxHead`` (Top-N) stages** (``fix_head_stages``): BladeBridge's SPARKSQL
   config has no handler for DataStage's Head stage at all -- it drops a literal
   ``// ******** No handler defined for type HEAD ... *******`` line into the Python file
   (a `//` comment, which Python doesn't have -- another syntax error) and never defines the
   stage's output. Replaced with ``SELECT * FROM <input> [ORDER BY <upstream sort>] LIMIT
   <nrecs>``, the same translation ``ds2spark.py``'s own ``st_head`` uses (migration/
   ds2spark.py), with the ORDER BY computed by walking the job's stage graph the same way
   that compiler's ``self.order`` dict does (Sort defines it, Join/Aggregator/Funnel break
   it, everything else passes it through) -- see ``link_order``.
4. **``TimestampToString`` loses its format** (``fix_timestamp_to_string_format``):
   BladeBridge drops the explicit output format pattern entirely, emitting
   ``CAST(to_timestamp(x) AS STRING)`` -- Spark's own default timestamp representation
   (``"2011-09-15 10:35:00"``) -- instead of the format the derivation actually specified
   (``TimestampToString(ts, "%yyyy%mm%dd")`` -> ``"20110915"``). Harmless until the column
   is declared numeric downstream and fix #7 below correctly tries to cast it, which then
   fails on a value that was never reformatted into a cast-able digit string. Restored with
   ``date_format(...)`` using the same token table ``ds2spark.py``'s own ``fmt()`` uses.
5. **``date_format()`` patterns are all upper-case** (``fix_date_format_pattern_case``):
   ``DateToString(date, "%yyyy%mm%dd")`` correctly becomes a ``date_format(...)`` call,
   but with every letter upper-cased (``'YYYYMMDD'``) regardless of what it means. Capital
   `Y` is week-based-year, not year, and Spark 3.0+ rejects mixing it with a bare month.
   Purely textual: re-lowercases year/day/second runs and DataStage's minutes token
   (``NN`` -> ``mm``), leaves month/hour upper-case, the same case ``ds2spark.py``'s own
   token table uses.
6. **``StringToDate()`` loses its format too** (``fix_string_to_date_cast``):
   ``StringToDate(value, "%yyyy%mm%dd")`` becomes a bare ``CAST(value AS date)``, which
   happens to work whenever ``value`` is already an ISO ``yyyy-MM-dd`` string -- which is
   how this project always supplies its date widgets, so most occurrences are
   coincidentally fine -- but fails outright on a ``yyyyMMdd``-format date key. Only
   rewrites a call whose value is a genuine qualified column reference, never a bare
   widget name, using ``to_date(cast(value AS string), '<format>')``.
7. **``StringToDecimal()`` casts with no precision** (``fix_bare_numeric_cast``):
   ``StringToDecimal(x)`` carries no precision/scale in DataStage's own syntax -- the real
   precision only exists on the declared target column -- so BladeBridge emits a bare
   ``CAST(x AS numeric)``. Spark's ``numeric`` with nothing specified defaults to
   ``DECIMAL(10, 0)``: zero decimal places, silently rounding away every cent of a real
   money value instead of raising an error. Fixed per *generated line* rather than per
   occurrence, using the declared type of the column the line's expression is assigned
   to, since an expression like ``CAST(a AS numeric) * CAST(b AS numeric) as line_amount``
   has two bare casts that both belong to the one target column.
8. **DataStage's empty-string-vs-NULL semantics** (``fix_nonnullable_string_coalesce``):
   reproduces the exact defect this repository's own migration found and fixed during
   validation (``migration/README.md`` §4 #2) -- a non-nullable source string column reads
   as `""` in DataStage but `NULL` in Spark, which silently breaks any `Len(Trim(x)) = 0`
   check. Every qualified reference to such a column is wrapped in ``coalesce(..., '')``.
9. **CSV files instead of Delta tables** (``fix_csv_writes_to_delta``): every target write
   is a raw ``.csv()`` file write to a widget-driven directory, matching DataStage's own
   file-based intermediate storage rather than this migration's chosen Delta-table mapping
   (§2.1). Rewritten to ``saveAsTable(f'{catalog}.{schema}.<file stem>')``, with ``catalog``/
   ``schema`` widgets added if not already present, and `overwriteSchema=true` so
   re-running the same job against a target from an earlier attempt doesn't fail if a
   column's inferred type has since changed.
10. **Aggregator functions all become MIN()** (``fix_aggregator_functions``): BladeBridge's
   Aggregator handling ignores the DataStage stage's actual `selection`/`reduce` property
   and always emits `MIN(...)`. For a `count` selection this references a column that was
   never there in the first place (`MIN(rows_loaded)` where `rows_loaded` is the *name of
   the count itself*, not an existing column -- an unresolved-column error at runtime,
   found by actually deploying this), and for `sum`/other reduce functions it silently
   computes the wrong aggregate with no error at all. Replaced with the correct
   `COUNT(*)`/`SUM(...)`/etc. by parsing the same `selection`/`countField`/`reduce`
   properties `ds2spark.py`'s own `st_agg` does (same DSX encoding, same regex).
11. **Numeric columns never cast** (``fix_numeric_casts``): BladeBridge only casts a column
   when the DataStage expression already contained a function call; a bare passthrough or
   a bare comparison against a literal (`ListPrice < 1`) leaves the column as the literal
   CSV string Spark's schema-less read produced. That's invisible until the column holds a
   genuinely non-integer value and gets compared against an integer literal, which Spark's
   implicit coercion resolves to `BIGINT`: a value like `"1.25"` then fails to cast under
   ANSI mode instead of being read (found the same way, by actually running it). Every
   numeric source column is now cast to its declared SQL type right where it's read, the
   same way `ds2spark.py`'s own `read_col` always casts regardless of whether the specific
   derivation needed it syntactically.

Example::

    lakebridge-workflow fix-dbxconv --dbxconv-dir migrated/dbxconv_out \\
        --condensed-dir switch_input --catalog unitygo --schema retail_dw_dbxconv \\
        --out-dir migrated/dbxconv_fixed
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

from .common import (
    CondensedJob, Stage, atomic_json, csv_escape, read_condensed_job, unwrap_dsx_value,
)

# --- Stage-kind classification -----------------------------------------------------------
# Same non-\b-boundary caveat as optimize.py: DataStage stage kinds concatenate words
# (PxSort, PxJoin) or join them with underscores, so these match as plain substrings.
SORT_KIND_RE = re.compile(r"sort", re.IGNORECASE)
ORDER_BREAKING_KIND_RE = re.compile(r"join|aggregat|funnel", re.IGNORECASE)
HEAD_KIND_RE = re.compile(r"head", re.IGNORECASE)
AGGREGATOR_KIND_RE = re.compile(r"aggregat", re.IGNORECASE)

# The exact pattern ds2spark.py's st_sort uses to pull key name + direction out of a Sort
# stage's DSX-encoded "key" property (DataStage's \(1)/\(2)/\(3) field/value/subvalue marks;
# see common.unwrap_dsx_value for the single-value case this is the multi-value cousin of).
SORT_KEY_NAME_RE = re.compile(r"\\\(1\)\\\(3\)key\\\(2\)(\w+)")

# JDBC/ODBC SQL type codes for character data (CHAR, VARCHAR, LONGVARCHAR, and their
# national-character variants) -- the condensed job's "sql=" column attribute uses these
# directly (e.g. "col region sql=12 ... nullable=0"), so this is source-agnostic, not tied
# to this pipeline's own columns.
STRING_SQL_TYPES = {1, 12, -1, -9, -15, -16}

# JDBC/ODBC SQL type codes for numeric data, mapped to a Spark type/cast expression. DECIMAL
# and NUMERIC (2, 3) use the column's own declared precision/scale; everything else maps to
# a fixed Spark type.
NUMERIC_SQL_TYPE_NAMES = {4: "int", -5: "bigint", 5: "smallint", -6: "tinyint",
                          6: "double", 7: "double", 8: "double"}
DECIMAL_SQL_TYPES = {2, 3}

# The same DSX encoding ds2spark.py's st_agg already parses for an Aggregator's "reduce"
# property: a source column name, followed by one or more (function, output column) pairs.
AGGREGATOR_REDUCE_SOURCE_RE = re.compile(r"reduce\\\(2\)(\w+)\\\(2\)0((?:\\\(1\)\\\(3\)\\\(3\)\w+\\\(2\)\w+\\\(2\)0)+)")
AGGREGATOR_REDUCE_FUNC_RE = re.compile(r"\\\(3\)\\\(3\)(\w+)\\\(2\)(\w+)\\\(2\)0")

# An expression that may contain parens nested up to two levels deep (an argument like
# `date_add(cast('{pEffDate}' as date),1)` already needs two: the outer date_add(...) call,
# and the cast(...) nested inside it). A bare `[^,]+`/`[^()]+` for "the first argument"
# stops at the first comma or paren *anywhere*, including ones that belong to a nested
# call, not to the top-level argument list -- wrong for any date/timestamp expression this
# generated code wraps in more than one function call.
_PARENS_0 = r"[^()]*"
_PARENS_1 = rf"(?:[^()]|\({_PARENS_0}\))*"
BALANCED_EXPR_RE_SRC = rf"(?:[^()]|\({_PARENS_1}\))*"

# The derivation text for a TimestampToString(..., "<format>") column; the same token table
# ds2spark.py's own fmt() helper uses (migration/README.md §2.3) converts DataStage's format
# to Spark's date_format() syntax. The format argument is TimestampToString's *own* (outer)
# second argument, not the nested StringToTimestamp(ts, "<input format>")'s one.
TIMESTAMP_TO_STRING_DERIVATION_RE = re.compile(
    rf'TimestampToString\(({BALANCED_EXPR_RE_SRC}),\s*"([^"]+)"\)'
)
DATE_FORMAT_TOKENS = [("%yyyy", "yyyy"), ("%mm", "MM"), ("%dd", "dd"), ("%hh", "HH"), ("%nn", "mm"), ("%ss", "ss")]

# Any date_format(expr, '<pattern>') call, wherever it appears and however deeply it's
# nested inside other wrapping calls (REGEXP_REPLACE, CAST, ...) -- a DateToString(date,
# "%yyyy%mm%dd") derivation becomes one of these, correctly, but with every letter
# upper-cased regardless of what it means.
DATE_FORMAT_CALL_RE = re.compile(rf"date_format\(({BALANCED_EXPR_RE_SRC}),\s*'([^']*)'\)")

# StringToDate(value, "<format>") -- like TimestampToString, loses its format pattern
# entirely, becoming a bare CAST(value AS date). Only a problem when value isn't already
# an ISO "yyyy-MM-dd" string (which a bare cast parses correctly by coincidence) -- e.g. a
# yyyyMMdd-format date key, which a bare cast can't parse at all.
STRING_TO_DATE_DERIVATION_RE = re.compile(rf'StringToDate\(({BALANCED_EXPR_RE_SRC}),\s*"([^"]+)"\)')

# --- Generated-code patterns (SPARKSQL target only; see module docstring) ----------------
BLANK_ATTR_RE = re.compile(r"^[ \t]*(\w+)\.\.alias\('\1'\)[^\n]*\n", re.MULTILINE)
SOURCE_READ_RE = re.compile(
    r"(\w+) = spark\.read\.format\('csv'\)\.option\('header', 'false'\)\.load\(f'''%PATH%'''\)"
)
# The rename-by-position block every source read gets (needed for a header-less CSV read,
# where Spark's own column names are just `_c0`, `_c1`, ...) is actively wrong once that
# read has become a Delta table read instead: the table's columns are already correctly
# named, and renaming them by position again is a latent bug waiting for the declared
# column list and the table's real column order to ever drift apart. Matched only
# immediately after a converted `spark.read.table(...)` line, so a source that's still a
# genuine CSV read keeps its rename-by-position block.
STRIP_CONFORM_AFTER_TABLE_READ_RE = re.compile(
    r"(\w+ = spark\.read\.table\(f'\{catalog\}\.\{schema\}\.\w+'\)\n)"
    r"# Conforming fields names to the component layout\n"
    r"\w+_conformed_cols = \[[^\]]*\]\n"
    r"\w+ = DatabricksConversionSupplements\.conform_df_columns\(\w+,\w+_conformed_cols\)\n"
)
HEAD_COMMENT_RE = re.compile(
    r"^// \*+\s*No handler defined for type HEAD, node (\w+)\s*\*+[ \t]*\n", re.MULTILINE
)
CSV_WRITE_RE = re.compile(
    r"(\w+)\.write\.format\('csv'\)\.option\('header','true'\)\.mode\('overwrite'\)"
    r"\.option\('sep',''\)\.csv\(f'''\{(\w+)\}/([A-Za-z0-9_]+)\.csv'''\)"
)
LAST_WIDGET_GET_RE = re.compile(r"^(\w+) = dbutils\.widgets\.get\(\"\1\"\)[ \t]*\n", re.MULTILINE)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dbxconv-dir", type=Path, required=True,
                   help="run-dbxconv's --output directory (one subfolder per job, each "
                        "containing out/**/<job>.py, matching run-dbxconv's own layout)")
    p.add_argument("--condensed-dir", type=Path, required=True,
                   help="directory of the same jobs' condensed .txt specs")
    p.add_argument("--catalog", required=True, help="Unity Catalog catalog for saveAsTable")
    p.add_argument("--schema", required=True, help="Unity Catalog schema for saveAsTable")
    p.add_argument("--out-dir", type=Path, required=True, help="where to write patched notebooks")
    return p.parse_args(argv)


def link_order(job: CondensedJob) -> dict[str, str | None]:
    """For every output link in the job, the SQL ORDER BY clause its rows carry (or None).

    Mirrors ds2spark.py's own ``self.order`` tracking: a Sort stage defines a new order from
    its key property; Join/Aggregator/Funnel break it; every other stage (Filter, Copy,
    Transformer, RemoveDuplicates, Head, ...) just passes its input's order through.
    """
    order: dict[str, str | None] = {}
    for stage in job.stages:
        in_links = [lk for lk in stage.links if lk.direction == "in"]
        out_links = [lk for lk in stage.links if lk.direction == "out"]
        if not out_links:
            continue
        inherited = order.get(in_links[0].link_name) if in_links else None
        if SORT_KIND_RE.search(stage.kind):
            result = sort_order_clause(stage) or inherited
        elif ORDER_BREAKING_KIND_RE.search(stage.kind):
            result = None
        else:
            result = inherited
        for lk in out_links:
            order[lk.link_name] = result
    return order


def sort_order_clause(stage: Stage) -> str:
    """"col1 DIR1, col2 DIR2, ..." from a Sort stage's DSX-encoded key property."""
    raw = stage.prop_map.get("key", "")
    keys = SORT_KEY_NAME_RE.findall(raw)
    parts = []
    for key in keys:
        m = re.search(rf"key\\\(2\){re.escape(key)}\\\(2\)0(\\\(1\)\\\(3\)\\\(3\)asc\\\\desc\\\(2\)(\w+))?", raw)
        direction = "DESC" if m and m.group(2) == "desc" else "ASC"
        parts.append(f"{key} {direction}")
    return ", ".join(parts)


def find_stage_by_out_link(job: CondensedJob, kind_re: re.Pattern, out_link: str) -> Stage | None:
    for stage in job.stages:
        if not kind_re.search(stage.kind):
            continue
        if any(lk.direction == "out" and lk.link_name == out_link for lk in stage.links):
            return stage
    return None


def fix_blank_attribute_refs(code: str) -> tuple[str, int]:
    """Delete `ident..alias('ident')` lines -- always a duplicate of a correctly named one."""
    return BLANK_ATTR_RE.subn("", code)


def fix_source_read_paths(code: str, job: CondensedJob, catalog: str, schema: str) -> tuple[str, int]:
    """Resolve `load(f'''%PATH%''')`, fix header and quote handling, and read intermediate
    files as the Delta tables fix #9 turns their *writes* into, instead of CSV files.

    Four independent defects live on the same generated line:

    1. The path is never substituted -- resolved from the source link's own ``file``
       property. DataStage attaches ``file`` as a property of the *link*, not the stage
       (visible in the condensed text nested under ``out link <name> <-> ...``), so this
       reads ``lk.properties`` directly rather than ``stage.prop_map``.
    2. The read is hardcoded to ``option('header', 'false')`` even when the source file
       genuinely has a header row (confirmed against this repository's own landing volume).
       That is backwards from what DataStage itself does: DataStage maps columns *by
       position* and ignores the header text (``migration/README.md`` §4 #1), which means
       it skips row 1 as a header, not that no header exists. ``conform_df_columns``
       already renames every column by position regardless of what Spark calls it, so
       flipping this to ``'true'`` only changes which row gets skipped as metadata instead
       of silently read as a data row (the latter breaks any numeric cast or comparison on
       that first row).
    3. A stage reading what an *earlier job in the same pipeline* wrote (the DataStage
       intermediate-file widgets: ``pStgDir``/``pDwhDir``/``pMartDir``/``pAuditDir``, as
       opposed to ``pSrcDir``, the one genuine external source) is left reading a CSV file
       at that widget's path. ``fix_csv_writes_to_delta`` turns every *write* through those
       same widgets into a Delta table instead -- so the matching read has to become
       ``spark.read.table(...)`` too, or it's reading a file that was never written,
       usually at that widget's useless literal Windows-path default. Found by actually
       deploying and running this: unlike every other defect here, this one doesn't raise
       a clean Python exception -- it fails with an opaque, non-retriable Databricks
       platform error, reproduced the same way whether the job runs alone or concurrently
       with others, which took isolating to a single job with nothing else running at all
       to confirm it wasn't a concurrency/resource issue before finding the real cause.
    4. No ``escape`` option is ever set, so a field containing an RFC 4180-escaped quote
       (``"RECORD FRAME 7"" SINGLE SIZE"`` in the raw CSV, i.e. a literal ``"`` written as
       ``""`` inside a quoted field) comes out with the escaping characters still in it
       instead of being unescaped, because Spark's CSV reader's own default escape
       character is a backslash, not a second quote. Found the same way as defect #3 --
       by reconciling this output against an independent implementation and tracing a
       wrong value back to its source -- not by reading the generated code, since nothing
       about a bare ``.option('header', 'true')`` call looks incomplete on its own.
    """
    file_by_link: dict[str, tuple[str, str]] = {}
    for stage in job.stages:
        for lk in stage.links:
            if lk.direction != "out":
                continue
            raw = dict(lk.properties).get("file")
            if raw is None:
                continue
            value = unwrap_dsx_value(raw)
            m = re.match(r"#(\w+)#(/.*)$", value.strip())
            if m:
                file_by_link[lk.link_name] = (m.group(1), m.group(2))

    count = 0

    def repl(m: re.Match) -> str:
        nonlocal count
        info = file_by_link.get(m.group(1))
        if info is None:
            return m.group(0)
        widget, relative = info
        count += 1
        if widget != "pSrcDir":
            table = relative.lstrip("/").rsplit(".", 1)[0]
            return f"{m.group(1)} = spark.read.table(f'{{catalog}}.{{schema}}.{table}')"
        return (f"{m.group(1)} = spark.read.format('csv').option('header', 'true')"
                f".option('escape', '\"')"
                f".load(f'''{{{widget}}}{relative}''')")

    code = SOURCE_READ_RE.sub(repl, code)
    code = STRIP_CONFORM_AFTER_TABLE_READ_RE.sub(r"\1", code)
    return code, count


def fix_head_stages(code: str, job: CondensedJob, order: dict[str, str | None]) -> tuple[str, int]:
    """Implement DataStage's Head (Top-N) stage, which BladeBridge's SPARKSQL config skips."""
    count = 0

    def repl(m: re.Match) -> str:
        nonlocal count
        out_link = m.group(1)
        stage = find_stage_by_out_link(job, HEAD_KIND_RE, out_link)
        if stage is None:
            return m.group(0)
        in_links = [lk for lk in stage.links if lk.direction == "in"]
        if not in_links:
            return m.group(0)
        in_link = in_links[0].link_name
        nrecs = stage.prop_map.get("nrecs", "").strip() or "1"
        order_clause = order.get(in_link)
        order_sql = f"\nORDER BY {order_clause}" if order_clause else ""
        count += 1
        return (
            f'{out_link} = spark.sql(f"""\nSELECT * FROM {in_link}{order_sql}\n'
            f'LIMIT {nrecs}\n""")\n{out_link}.createOrReplaceTempView("{out_link}")\n'
        )

    return HEAD_COMMENT_RE.sub(repl, code), count


def fix_timestamp_to_string_format(code: str, job: CondensedJob) -> tuple[str, int]:
    """Restore the DataStage-specified output format for TimestampToString derivations.

    BladeBridge's conversion drops the explicit output format entirely, emitting a bare
    ``CAST(x AS STRING)`` -- Spark's own default timestamp representation
    (``"2011-09-15 10:35:00"``) -- instead of the format DataStage's derivation actually
    asked for (``TimestampToString(ts, "%yyyy%mm%dd")``, e.g. ``"20110915"``). Found by
    deploying and running this: a date key derived this way is declared as a numeric
    column downstream, so `fix_numeric_casts` correctly tries to cast it to `int` -- which
    then fails, because the value was never reformatted into a cast-able digit string in
    the first place.

    ``x`` is wrapped in ``to_timestamp(...)`` when the DataStage expression's input needs
    parsing from a raw string first (``TimestampToString(StringToTimestamp(...), fmt)``,
    e.g. a raw CSV field), but is a bare qualified column reference when the input is
    already timestamp-typed (``TimestampToString(lnk_x.some_ts, fmt)``, e.g. a value
    computed earlier in the same job) -- found as a second, separate case by the exact
    same method as every other date-format defect here: deploying and running it, then
    tracing a value that was wrong downstream back to where it first went wrong, which
    this time was a FACT table's join to a currency-rate dimension silently never
    matching, because the two sides' join keys were in different formats. ``date_format``
    accepts either shape directly, so both are matched and fixed without needing to tell
    them apart; restored using the same token mapping ``ds2spark.py``'s own ``fmt()``
    helper uses (§2.3).
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


def fix_date_format_pattern_case(code: str) -> tuple[str, int]:
    """Fix the letter-casing BladeBridge gets wrong in every date_format() pattern.

    ``DateToString(date, "%yyyy%mm%dd")`` correctly becomes a ``date_format(...)`` call
    (unlike ``TimestampToString``, which loses its format entirely -- see
    ``fix_timestamp_to_string_format``), but the pattern string itself comes out all
    upper-case (``'YYYYMMDD'``) regardless of what each letter means. Capital `Y` is
    week-based-year, not year, and Spark 3.0+'s strict parser rejects mixing it with a
    bare month -- found by actually running this
    (``INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION``). This is a
    purely textual fix -- no need to re-derive the format from the original DataStage
    expression, since the pattern survived, just mis-cased -- re-lowercasing year/day/
    second runs and DataStage's minutes token (``NN`` -> ``mm``), leaving month (`MM`) and
    hour (`HH`) upper-case, exactly as both are in DataStage's own format syntax and in
    ``ds2spark.py``'s token table (§2.3). Only ever sees DataStage-sourced patterns in this
    tool's own output, so an upper-case `D` run is always "day of month" (DataStage has no
    "day of year" token), never Spark's own different meaning for capital `D`.
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


def fix_string_to_date_cast(code: str, job: CondensedJob) -> tuple[str, int]:
    """Restore the format on a StringToDate(value, "<format>") BladeBridge dropped.

    Becomes a bare ``CAST(value AS date)``, which happens to work whenever ``value`` is
    already an ISO ``yyyy-MM-dd`` string -- which is how this project always supplies its
    date widgets (``pRunDate``/``pEffDate``), so those occurrences are left alone -- but
    fails outright on anything else, such as a ``yyyyMMdd``-format date key (found the
    same way as every other date-format defect here: by deploying and running this, not
    by reading the generated code). Only ever rewrites a call whose value is a genuine
    qualified column reference (contains a ``.``), identified from the derivation text,
    never a bare widget name.
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


def fix_nonnullable_string_coalesce(code: str, job: CondensedJob) -> tuple[str, int]:
    """Wrap every reference to a non-nullable source string column in coalesce(x, '').

    DataStage reads an empty field of such a column as `""`; Spark reads it as `NULL`,
    which silently breaks any `Len(Trim(x)) = 0`-style check downstream (the exact defect
    migration/README.md §4 #2 documents finding and fixing in the other two implementations).

    The generated code mixes two completely different languages that happen to look
    similar: SQL text inside a ``spark.sql(f'''...''')`` call, where a bare ``''`` is
    correctly a string literal, and the PySpark DataFrame API's own `.select(col.alias(...))`
    calls,
    where passing a bare Python string to `coalesce()` is a well-known trap -- PySpark
    resolves it as a *column name* to look up, not a literal value, so `coalesce(x, '')`
    there fails at runtime with "a column ... with name `` cannot be resolved" (found by
    actually deploying and running this, not by reading the generated code). Every
    DataFrame-API reference in this family of generated code is immediately followed by
    `.alias(`, so that's used to tell the two contexts apart: `lit('')` in the one case,
    plain `''` in the other.
    """
    count = 0
    for stage in job.stages:
        for lk in stage.links:
            if lk.direction != "out":
                continue
            for col in lk.columns:
                if col.get("nullable") or col.get("sqlType") not in STRING_SQL_TYPES:
                    continue
                pattern = re.compile(rf"\b{re.escape(lk.link_name)}\.{re.escape(col['name'])}\b")

                def repl(m: re.Match, _code=code) -> str:
                    # Decide per occurrence, from the ORIGINAL text each match came from --
                    # never from text another replacement in this same pass already
                    # produced, which is what caused a double-wrap the first time this was
                    # tried as two separate subn() passes (lookahead/lookbehind, run second,
                    # matched the first pass's own output).
                    follows_alias = _code[m.end():m.end() + 10].lstrip().startswith(".alias(")
                    tail = ", lit(''))" if follows_alias else ", '')"
                    return f"coalesce({m.group(0)}{tail}"

                code, n = pattern.subn(repl, code)
                count += n
    return code, count


def spark_numeric_type(sql_type: Any, precision: Any, scale: Any) -> str | None:
    """A Spark cast-target type string for a JDBC numeric SQL type code, or None."""
    if sql_type in DECIMAL_SQL_TYPES:
        return f"decimal({int(precision) if precision else 18},{int(scale) if scale else 2})"
    return NUMERIC_SQL_TYPE_NAMES.get(sql_type)


def fix_bare_numeric_cast(code: str, job: CondensedJob) -> tuple[str, int]:
    """Give BladeBridge's bare `CAST(x AS numeric)` the precision/scale it always drops.

    ``StringToDecimal(x)`` (no precision argument -- DataStage's own syntax doesn't carry
    one; the real precision only exists on the *declared target column*) becomes a bare
    ``CAST(x AS numeric)``. Spark's ``numeric`` with no precision/scale given defaults to
    ``DECIMAL(10, 0)`` -- zero decimal places -- which silently rounds away every cent of
    a real money value instead of raising an error (found the same way as the other
    precision-losing defects here: a value was subtly wrong three stages downstream, not
    by reading the generated code). The same bare cast is also how BladeBridge handles a
    `StringToDecimal` used only to turn a cleaned-up digit string into an *integer* date
    key, so this looks up the actual declared type (decimal or int) from the column the
    expression is assigned to, not just decimal.

    Scoped per *generated line*, not per occurrence: an expression like
    ``CAST(a AS numeric) * CAST(b AS numeric) as line_amount`` has two bare casts that
    both belong to the same target column, and anchoring on "immediately followed by
    ``as <name>``" (as the other fixes here do) would only ever reach the second one.
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


def fix_aggregator_functions(code: str, job: CondensedJob) -> tuple[str, int]:
    """Replace BladeBridge's always-`MIN()` Aggregator output with the real function.

    Parses the same `selection`/`countField`/`reduce` DSX-encoded properties
    `ds2spark.py`'s own `st_agg` does, then finds that stage's generated
    ``<out_link> = spark.sql(f\"\"\"...\"\"\")`` block (identified by the assignment target,
    which is always the out link's name in this generated-code family) and replaces
    `MIN((optionally qualified) <output column>) as <output column>` within *that block
    only*, so a different aggregator elsewhere in the same job that happens to produce a
    column with the same name is never touched.
    """
    count = 0
    for stage in job.stages:
        if not AGGREGATOR_KIND_RE.search(stage.kind):
            continue
        out_links = [lk for lk in stage.links if lk.direction == "out"]
        if not out_links:
            continue
        out_link = out_links[0]
        correct: dict[str, str] = {}
        if stage.prop_map.get("selection", "").strip() == "count":
            if out_link.columns:
                correct[out_link.columns[-1]["name"]] = "COUNT(*)"
        else:
            raw = stage.prop_map.get("reduce", "")
            for src_col, rest in AGGREGATOR_REDUCE_SOURCE_RE.findall(raw):
                for fn, out_col in AGGREGATOR_REDUCE_FUNC_RE.findall(rest):
                    correct[out_col] = f"{fn.upper()}({src_col})"
        if not correct:
            continue

        block_re = re.compile(rf"^{re.escape(out_link.link_name)} = spark\.sql\(f\"\"\".*?\"\"\"\)", re.MULTILINE | re.DOTALL)

        def repl_block(m: re.Match) -> str:
            nonlocal count
            block = m.group(0)
            for out_col, replacement in correct.items():
                pattern = re.compile(rf"MIN\((?:\w+\.)?{re.escape(out_col)}\) as {re.escape(out_col)}")
                block, n = pattern.subn(f"{replacement} as {out_col}", block)
                count += n
            return block

        code = block_re.sub(repl_block, code)
    return code, count


def fix_numeric_casts(code: str, job: CondensedJob) -> tuple[str, int]:
    """Cast every numeric source-column reference to its declared SQL type.

    See the module docstring (point 7): BladeBridge only casts when the DataStage
    expression already contained a function call, so a bare passthrough or comparison
    against a literal leaves the column as the raw CSV string, which can fail an implicit
    cast later on real (non-integer) data.
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

                def repl(m: re.Match, _code=code, _type=spark_type) -> str:
                    before = _code[max(0, m.start() - 6):m.start()].rstrip().upper()
                    if before.endswith("CAST(") or before.endswith(".CAST("):
                        return m.group(0)  # already cast immediately before this reference
                    follows_alias = _code[m.end():m.end() + 10].lstrip().startswith(".alias(")
                    if follows_alias:
                        return f"{m.group(0)}.cast('{_type}')"
                    return f"CAST({m.group(0)} AS {_type})"

                code, n = pattern.subn(repl, code)
                count += n
    return code, count


def fix_csv_writes_to_delta(code: str, catalog: str, schema: str) -> tuple[str, int, list[str]]:
    """Rewrite every raw `.csv()` target write into a `saveAsTable` Delta write.

    Includes `overwriteSchema=true`: re-running this same job against a target that
    already has a table from an earlier attempt -- exactly what validating a fix like this
    one involves -- otherwise fails with `DELTA_FAILED_TO_MERGE_FIELDS` the moment a
    column's inferred type differs from whatever got written before (found by doing
    exactly that: an early, not-yet-numeric-cast run left `quantity` as a string column,
    and the next run's correctly-cast numeric write then couldn't merge into it).
    """
    tables: list[str] = []

    def repl(m: re.Match) -> str:
        table = m.group(3)
        tables.append(table)
        return (f"{m.group(1)}.write.format('delta').mode('overwrite')"
                f".option('overwriteSchema', 'true').saveAsTable(f'{{catalog}}.{{schema}}.{table}')")

    patched, count = CSV_WRITE_RE.subn(repl, code)
    if tables:
        patched = add_catalog_schema_widgets(patched, catalog, schema)
    return patched, count, tables


def add_catalog_schema_widgets(code: str, catalog_default: str, schema_default: str) -> str:
    if "name = 'catalog'" in code:
        return code
    insertion = (
        f"\ndbutils.widgets.text(name = 'catalog', defaultValue = '{catalog_default}')\n"
        f'catalog = dbutils.widgets.get("catalog")\n\n'
        f"dbutils.widgets.text(name = 'schema', defaultValue = '{schema_default}')\n"
        f'schema = dbutils.widgets.get("schema")\n'
    )
    matches = list(LAST_WIDGET_GET_RE.finditer(code))
    if not matches:
        return insertion + "\n" + code
    pos = matches[-1].end()
    return code[:pos] + insertion + code[pos:]


def find_job_notebook(dbxconv_dir: Path, job_name: str) -> Path | None:
    job_dir = dbxconv_dir / job_name / "out"
    if not job_dir.is_dir():
        return None
    candidates = [p for p in job_dir.rglob("*.py") if p.name != "databricks_conversion_supplements.py"]
    return candidates[0] if len(candidates) == 1 else None


def fix_job(job_name: str, notebook_path: Path, job: CondensedJob, catalog: str, schema: str) -> dict[str, Any]:
    code = notebook_path.read_text(encoding="utf-8")
    order = link_order(job)

    # Added unconditionally and early: fix_source_read_paths may need catalog/schema for an
    # intermediate-file read even in a job with no target write of its own to trigger them.
    code = add_catalog_schema_widgets(code, catalog, schema)
    code, blank_attrs = fix_blank_attribute_refs(code)
    code, resolved_paths = fix_source_read_paths(code, job, catalog, schema)
    code, head_stages = fix_head_stages(code, job, order)
    # Must run before fix_numeric_casts: a date key reformatted here is what makes the cast
    # fix_numeric_casts adds further down actually succeed, instead of trying to cast
    # Spark's default ("2011-09-15 10:35:00") timestamp string to an int.
    code, timestamp_formats = fix_timestamp_to_string_format(code, job)
    code, date_format_cases = fix_date_format_pattern_case(code)
    code, string_to_date_casts = fix_string_to_date_cast(code, job)
    code, bare_numeric_casts = fix_bare_numeric_cast(code, job)
    # Aggregator functions must be fixed before numeric casts are added: the MIN(...)
    # pattern it looks for only matches a bare (optionally qualified) column reference, and
    # fix_numeric_casts would otherwise wrap that same reference in CAST(...) first.
    code, aggregator_fixes = fix_aggregator_functions(code, job)
    code, numeric_casts = fix_numeric_casts(code, job)
    code, coalesce_wraps = fix_nonnullable_string_coalesce(code, job)
    code, delta_writes, tables = fix_csv_writes_to_delta(code, catalog, schema)

    try:
        compile(code, job_name, "exec")
        syntax_error = ""
    except SyntaxError as exc:
        syntax_error = str(exc)

    return {
        "job": job_name, "patchedCode": code, "tables": tables,
        "fixes": {
            "blankAttributeRefs": blank_attrs, "resolvedSourcePaths": resolved_paths,
            "headStages": head_stages, "timestampFormats": timestamp_formats,
            "dateFormatCases": date_format_cases, "stringToDateCasts": string_to_date_casts,
            "bareNumericCasts": bare_numeric_casts,
            "aggregatorFunctions": aggregator_fixes, "numericCasts": numeric_casts,
            "coalesceWraps": coalesce_wraps, "deltaWrites": delta_writes,
        },
        "syntaxError": syntax_error,
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
        notebook = find_job_notebook(args.dbxconv_dir, job.name)
        if notebook is None:
            continue
        result = fix_job(job.name, notebook, job, args.catalog, args.schema)
        (args.out_dir / f"{job.name}.py").write_text(result["patchedCode"], encoding="utf-8")
        results.append(result)

    fix_keys = ["blankAttributeRefs", "resolvedSourcePaths", "headStages", "timestampFormats",
                "dateFormatCases", "stringToDateCasts", "bareNumericCasts", "aggregatorFunctions",
                "numericCasts", "coalesceWraps", "deltaWrites"]
    report = {
        "summary": {
            "jobsPatched": len(results),
            "jobsStillBroken": sum(1 for r in results if r["syntaxError"]),
            **{f"total{key[0].upper()}{key[1:]}": sum(r["fixes"][key] for r in results) for key in fix_keys},
        },
        "jobs": [{"job": r["job"], **r["fixes"], "tables": r["tables"], "syntaxError": r["syntaxError"]}
                for r in results],
    }
    report_path = args.out_dir.parent / f"{args.out_dir.name}_report.json"
    atomic_json(report_path, report)
    csv_path = args.out_dir.parent / f"{args.out_dir.name}_report.csv"
    header = ["job", *fix_keys, "tables", "syntaxError"]
    rows = [header] + [[r["job"], *(r["fixes"][key] for key in fix_keys),
                        "; ".join(r["tables"]), r["syntaxError"]] for r in results]
    csv_path.write_text("\n".join(",".join(csv_escape(v) for v in row) for row in rows) + "\n", encoding="utf-8")

    print(f"Patched {len(results)} job(s); {report['summary']['jobsStillBroken']} still fail to parse.")
    for key, value in report["summary"].items():
        print(f"  {key}: {value}")
    print(f"  Notebooks: {args.out_dir}")
    print(f"  Report JSON: {report_path}")
    print(f"  Report CSV:  {csv_path}")
    return 1 if report["summary"]["jobsStillBroken"] else 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
