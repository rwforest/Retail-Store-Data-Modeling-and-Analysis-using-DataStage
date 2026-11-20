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

Five fixes, all independently callable and independently reported:

1. **``Ereplace(x, old, new)`` is left untranslated** (``fix_ereplace_calls``):
   DataStage's BASIC string-replace function has no Databricks SQL equivalent by that
   name; the call survives verbatim into the generated SQL, where it would fail with an
   unresolved-routine error. Renamed to ``REPLACE(x, old, new)``, which has the same
   3-argument signature and semantics -- found reading the generated code (an SCD2 job's
   ``eff_end_key``/``eff_start_key`` derivations, a date-dimension job's ``YearMonth``
   reformat).
2. **``Len(x)`` is left untranslated** (``fix_len_calls``): the same gap as ``Ereplace``,
   for DataStage's string-length function. Renamed to ``LENGTH(x)``.
3. **DataStage's ``:`` string-concatenation operator becomes a literal ``+``**
   (``fix_string_concat_plus``): ``A : "|" : B`` (DataStage BASIC's concatenation
   operator) is correctly recognized as concatenation but mistranslated as numeric
   addition (``A + '|' + B``) instead of ``A || '|' || B`` -- wrong for any Databricks SQL
   engine running in ANSI mode, since ``+`` between a string and a non-numeric-coercible
   value raises a cast error rather than concatenating. Only rewrites a generated line
   whose target column's DataStage derivation actually contains the ``:`` operator (and
   whose ``+`` count matches the derivation's ``:`` count, as a safety check), so a
   genuine numeric ``+`` elsewhere -- `old_version_no + 1`, for example -- is never touched.
4. **``StringToDecimal(x)`` casts with no precision** (``fix_bare_numeric_cast``): the
   same defect ``fix_dbxconv_output.py``'s own version fixes for SPARKSQL -- a bare
   ``CAST(x AS numeric)`` defaults to Spark/Databricks SQL's ``DECIMAL(10, 0)``, silently
   rounding away every decimal place. Fixed the same way: per generated *line*, using the
   declared type of the column the line's expression is assigned to.
5. **``date_format()`` patterns are all upper-case** (``fix_date_format_pattern_case``):
   the same defect ``fix_dbxconv_output.py``'s own version fixes for SPARKSQL, reproduced
   identically here -- ``DateToString`` correctly becomes ``date_format(...)``, but the
   pattern string comes out all-upper-case, and Spark/Databricks SQL's strict parser
   rejects mixing capital ``Y`` (week-based-year) with a bare month.

**What this module does not fix, and why -- the DBSQL target is not runnable after these
five fixes either way:**

- **No ``;`` between statements, and no ``CREATE VIEW``/CTE naming any intermediate
  result** (already documented in ``migration/README.md`` §8a.4 from the external sample
  jobs; confirmed unchanged for this repository's own 23 jobs). A later stage's ``FROM
  <link>`` references a name no earlier statement ever defines. Investigated at length
  for this repository's own SCD2 job (``J06_Dim_Customer_SCD2``) trying to recover a
  reliable statement-to-link correspondence (by file position, by column-alias-list
  matching, by cross-referencing each statement's own ``FROM`` target against the
  condensed job model's declared in-links): no single heuristic held across the whole
  file. A ``PxCopy`` (Replicate) stage with two output links emits what looks like two
  near-identical statements, but at least one declared link (``lnk_snap``, a plain
  external-source read) has **no** statement anywhere that matches its own declared
  column list, and the file also contains ``SELECT CONCAT_WS(...)`` statements that are
  neither out-link producers nor consumers -- they correspond to a file/audit *target*
  stage's own CSV-row-dump, a third statement shape not accounted for by either of the
  first two. Attempting a best-guess auto-wrapping here, without being able to deploy and
  reconcile it the way every other fix in this package has been validated, risks silently
  mis-naming a view and producing wrong data rather than a clean failure -- exactly the
  failure mode this package's own README (§7.2) already documents and warns against.
  Making DBSQL output actually executable needs a real investigation of dbxconv's own
  statement-emission rules (which DataStage stage kinds get their own named statement,
  which get fused into a consumer, and how a file/audit target stage's dump statement is
  meant to be handled), not a textual fix at this level.
- **Source reads (``PxSequentialFile`` stages with no in-links) have a completely blank
  ``FROM`` clause and no column qualifier at all** -- not a placeholder like SPARKSQL's
  ``load(f'''%PATH%''')``, just nothing (a bare ``.CustomerID`` with a dangling leading
  dot, read directly off the SQL Warehouse via ``read_files(...)``, confirmed working
  syntax -- ``SELECT * FROM read_files('<path>', format => 'csv', header => false)``
  exposes positional columns as ``_c0``, ``_c1``, ... exactly like Spark's own CSV
  reader). **Deliberately not auto-fixed here, or in a separate module, because doing so
  safely needs knowing which external source each blank reference belongs to, and that
  turns out to be genuinely ambiguous, not just unexplored**: checking raw DataStage
  field names for collisions across every job's distinct external sources found that
  *most* multi-source jobs in this repository reuse the same field name
  (``customer_id``, ``country``, ``stock_code``, ...) across two or more unrelated
  sources. Matching a blank reference to a source by column/field name, the only signal
  available from the text itself, would silently resolve to the *wrong* source for a
  large fraction of occurrences -- a wrong-source read is a wrong-source read regardless
  of why a reference was blank, and this package already treats "produces a confident,
  wrong-looking result" as worse than a clean failure (README §7.2). Resolving this
  safely needs either a real understanding of dbxconv's own emission rules for which
  in-link each blank reference traces back to (not recoverable from the output text
  alone, per the view-wrapping finding above) or carefully isolated single-stage test
  jobs built specifically to observe that behavior without this repository's own
  field-name collisions obscuring it -- not a text-pattern-matching guess against this
  pipeline's actual, ambiguous output.

Example::

    lakebridge-workflow fix-dbxconv-dbsql --dbxconv-dir migrated/dbxconv_dbsql_out \\
        --condensed-dir switch_input --out-dir migrated/dbxconv_dbsql_fixed
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

from .common import (
    BALANCED_EXPR_RE_SRC, DATE_FORMAT_TOKENS, CondensedJob, atomic_json, csv_escape,
    read_condensed_job, spark_numeric_type,
)

# BladeBridge's DBSQL target renders both of these with a space before the opening
# paren (``Ereplace ( x , old , new )``), unlike SPARKSQL's ``Ereplace(x, old, new)`` --
# tolerate either.
EREPLACE_CALL_RE = re.compile(r"\bEreplace\s*\(")
LEN_CALL_RE = re.compile(r"\bLen\s*\(")

# DataStage's ``:`` concatenation operator, as it appears in the condensed job's own
# derivation text (``A : "|" : B``, whitespace on both sides) -- the same operator
# ``fix_string_concat_plus`` below looks for to decide whether a generated line's ``+``
# is really concatenation, not DataStage source syntax shared with any other fixer here.
STRING_CONCAT_COLON_RE = re.compile(r"\s:\s")

# Same duplicated-nested-paren concern as fix_dbxconv_output.py's own date-format fix
# (BALANCED_EXPR_RE_SRC, from common.py): a date_format(...) call's first argument can
# itself contain other function calls.
DATE_FORMAT_CALL_RE = re.compile(rf"date_format\(({BALANCED_EXPR_RE_SRC}),\s*'([^']*)'\)")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dbxconv-dir", type=Path, required=True,
                   help="run-dbxconv's --output directory for a DBSQL conversion (one "
                        "subfolder per job, each containing out/**/<job>.sql)")
    p.add_argument("--condensed-dir", type=Path, required=True,
                   help="directory of the same jobs' condensed .txt specs")
    p.add_argument("--out-dir", type=Path, required=True, help="where to write patched SQL files")
    return p.parse_args(argv)


def fix_ereplace_calls(code: str) -> tuple[str, int]:
    """Rename every ``Ereplace(x, old, new)`` call to Databricks SQL's ``REPLACE``."""
    return EREPLACE_CALL_RE.subn("REPLACE(", code)


def fix_len_calls(code: str) -> tuple[str, int]:
    """Rename every ``Len(x)`` call to Databricks SQL's ``LENGTH``."""
    return LEN_CALL_RE.subn("LENGTH(", code)


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


def find_job_sql(dbxconv_dir: Path, job_name: str) -> Path | None:
    job_dir = dbxconv_dir / job_name / "out"
    if not job_dir.is_dir():
        return None
    candidates = list(job_dir.rglob("*.sql"))
    return candidates[0] if len(candidates) == 1 else None


def fix_job(job_name: str, sql_path: Path, job: CondensedJob) -> dict[str, Any]:
    code = sql_path.read_text(encoding="utf-8")

    code, ereplace_calls = fix_ereplace_calls(code)
    code, len_calls = fix_len_calls(code)
    code, string_concats = fix_string_concat_plus(code, job)
    code, bare_numeric_casts = fix_bare_numeric_cast(code, job)
    code, date_format_cases = fix_date_format_pattern_case(code)

    return {
        "job": job_name, "patchedCode": code,
        "fixes": {
            "ereplaceCalls": ereplace_calls, "lenCalls": len_calls,
            "stringConcats": string_concats, "bareNumericCasts": bare_numeric_casts,
            "dateFormatCases": date_format_cases,
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
        sql_path = find_job_sql(args.dbxconv_dir, job.name)
        if sql_path is None:
            continue
        result = fix_job(job.name, sql_path, job)
        (args.out_dir / f"{job.name}.sql").write_text(result["patchedCode"], encoding="utf-8")
        results.append(result)

    fix_keys = ["ereplaceCalls", "lenCalls", "stringConcats", "bareNumericCasts", "dateFormatCases"]
    report = {
        "summary": {
            "jobsPatched": len(results),
            **{f"total{key[0].upper()}{key[1:]}": sum(r["fixes"][key] for r in results) for key in fix_keys},
        },
        "jobs": [{"job": r["job"], **r["fixes"]} for r in results],
    }
    report_path = args.out_dir.parent / f"{args.out_dir.name}_report.json"
    atomic_json(report_path, report)
    csv_path = args.out_dir.parent / f"{args.out_dir.name}_report.csv"
    header = ["job", *fix_keys]
    rows = [header] + [[r["job"], *(r["fixes"][key] for key in fix_keys)] for r in results]
    csv_path.write_text("\n".join(",".join(csv_escape(v) for v in row) for row in rows) + "\n", encoding="utf-8")

    print(f"Patched {len(results)} job(s). Not yet runnable (see module docstring: "
          f"missing semicolons/view-naming, and blank source references, are separate, unsolved issues).")
    for key, value in report["summary"].items():
        print(f"  {key}: {value}")
    print(f"  SQL files: {args.out_dir}")
    print(f"  Report JSON: {report_path}")
    print(f"  Report CSV:  {csv_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
