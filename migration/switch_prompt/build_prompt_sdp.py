#!/usr/bin/env python3
"""Build the Switch conversion prompt for target_type: sdp (DataStage job -> Spark Declarative Pipeline source file).

    python3 migration/switch_prompt/build_prompt_sdp.py   # writes migration/switch_prompt/datastage_to_sdp.yml

The stage / function / DataStage-semantics rules are the same as the notebook prompt (build_prompt.py); what changes is
the output shape (temporary views + materialized views in one pipeline) and an SCD2 rule (Auto CDC from snapshots).
Few-shots pair condensed jobs with the SDP files that `ds2spark.py --sdp` compiles. Switch's built-in SDP guidance
(${common_sdp_instructions_and_guidelines}) is deliberately NOT referenced: it uses legacy APIs (dp.view, dp.read) and
forbids spark.sql, which this translation relies on.
"""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
SHOTS = ["J04_Land_Stores_Employees", "J15_Analysis_CustomerType_Profit"]  # positional CSV read; Sort -> Copy -> Head

SYSTEM = r"""
Convert a DataStage job description into ONE Python source file of a Lakeflow Spark Declarative Pipeline (SDP)
according to these rules.

# Input and Output
- Input: a text rendering of ONE DataStage parallel job (parameters, stages with properties, links between stages,
  each link's columns with derivations ":="), or a DIMENSION UNIT: several jobs plus orchestration notes (see the
  SCD Type 2 section).
- Output: Python code with Python comments (in {comment_lang}) only. The file is one of many source files of a single
  pipeline: every job of the warehouse is in the same pipeline, so datasets defined in other files are read by name.
- Use the modern API only: `from pyspark import pipelines as dp`, `@dp.temporary_view(name=...)`,
  `@dp.materialized_view(name=...)`, `dp.create_streaming_table(...)`, `dp.create_auto_cdc_from_snapshot_flow(...)`,
  `spark.read.table("<name>")`. Do NOT use `dlt`, `@dp.view`, `@dp.table`, `dp.read`, `dbutils`, writes
  (`saveAsTable`, `.write`), actions (`count`, `collect`, `show`), `USE`, or catalog/schema-qualified names: the
  pipeline publishes everything to its own target catalog and schema.
- Convert EVERYTHING in the job, one dataset per link, in data-flow order. Never invent logic: use only what the input
  states. If something cannot be converted, say so in a comment.
- DO NOT output cell separators or notebook-level descriptions; another program adds them.

# Parameters
Start with exactly:
    from pyspark import pipelines as dp

    src_dir = spark.conf.get("src_dir")
    pRunDate = spark.conf.get("pRunDate", "2011-12-31")
    pEffDate = spark.conf.get("pEffDate", "2011-07-01")
DataStage job parameters #pSrcDir# -> `src_dir`; #pStgDir#, #pDwhDir#, #pMartDir#, #pAuditDir# have no equivalent:
those files are pipeline datasets. #pRunDate# / #pEffDate# stay as pRunDate / pEffDate.

# Datasets (names are pipeline-wide, so they must not collide across jobs)
- Every LINK becomes a `@dp.temporary_view` named `<job code>_<link>`, where the job code is the first part of the job
  name in lower case (J04_Land_Stores_Employees -> `j04`). Use that same name for the Python function. In SQL, refer to
  other links of the job by their prefixed names.
- Every Sequential File TARGET becomes a `@dp.materialized_view` named EXACTLY like the target file stem (same case),
  returning `spark.read.table("<job code>_<input link>").select(<the link's columns in order>)`.
- Sequential File source with path #pSrcDir#/<file>.csv: a temporary view that reads the CSV with
  spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/<file>.csv").
  DataStage maps file columns BY POSITION, never by header text: assert the column count equals the link's column
  count, rename with .toDF(<link column names in order>), then .selectExpr(...) every column with a CAST to its
  declared type. EMPTY FIELDS: Spark reads an empty CSV field as NULL, DataStage reads it as "" for a non-nullable
  string column - wrap every non-nullable string source column in coalesce(`col`, '').
- Sequential File source with any other path (a file written by another job): a temporary view returning
  `spark.read.table("<file stem>")`.
- Transformations return `spark.sql(<f-string with the SQL>)` inside the temporary view function, as in the examples.

# Stages -> SQL
- Transformer: SELECT with one expression per output column, each wrapped in CAST(... AS <column type>).
  Input-link prefixes (link.col) are dropped. A column derived as just `link.col` is passed through.
- Filter: SELECT * ... WHERE <expression>. Join: SELECT the output link's columns FROM left l <type> JOIN right r ON the
  key columns. Key columns come from the left side. innerjoin/leftouterjoin/rightouterjoin/fullouterjoin.
- Aggregator: GROUP BY the keys; "count" -> CAST(COUNT(*) AS int); "reduce" sum/min/max -> CAST(SUM|MIN|MAX(col) AS type).
- Sort: ORDER BY keys (asc/desc). Copy: one view per output link. Funnel: UNION ALL of the inputs' columns.
- ROW ORDER (critical): datasets do NOT keep row order. A DataStage Sort's order flows to every downstream stage
  until a Join, Aggregator or Funnel breaks it (it passes through Copy, Filter and Transformer). Track it, and:
  - Head: emit `ORDER BY <the upstream sort keys> LIMIT n` - the first n rows in sort order. Only use a bare LIMIT
    when no Sort is upstream.
  - Remove Duplicates: keep first via row_number() OVER (PARTITION BY keys ORDER BY <upstream sort keys>, else
    monotonically_increasing_id()) = 1.
- The surrogate-key idiom `@PARTITIONNUM + (@NUMPARTITIONS * (@INROWNUM - 1)) + 1` -> row_number() OVER (ORDER BY <upstream sort>
  or monotonically_increasing_id()).
- Column types: int->int, bigint->bigint, string->string, decimal(18,2) (precision 18 scale 2), decimal(18,6) for scale 6,
  timestamp, date. SqlType codes: 4=int, -5=bigint, 12=string, 3=decimal, 11=timestamp, 9=date.

# DataStage functions -> Spark SQL
Trim->trim, Upcase->upper, Downcase->lower, Len->length, Abs->abs, Left(x,n)->left(x,n), NullToValue(x,d)->coalesce(x,d),
IsNull(x)->(x IS NULL), Ereplace->replace, CRC32->crc32, StringToDecimal(x)->CAST(x AS decimal(18,4)),
StringToDate(x,fmt)->to_date(x,fmt), StringToTimestamp(x,fmt)->to_timestamp(x,fmt),
DateToString / TimestampToString(x,fmt)->date_format(x,fmt), DaysSinceFromDate(a,b)->datediff(a,b),
DateFromDaysSince(n,d)->date_add(d,n), `:` (concatenate)->concat(CAST(x AS string), ...),
If c Then a Else b->CASE WHEN c THEN a ELSE b END (nested Else If -> nested CASE), And/Or->AND/OR.
Format strings: %yyyy->yyyy, %mm->MM (month), %dd->dd, %hh->HH, %nn->mm (minutes), %ss->ss.
Parameters inside expressions become f-string substitutions, e.g. '{pRunDate}'.

# SCD Type 2 dimension units (Auto CDC from snapshots)
When the input is a DIMENSION UNIT (jobs that together maintain a slowly changing dimension from successive full
snapshots, plus orchestration notes such as "load version 1 only if the dimension does not exist" and "promote the
next file"), do NOT translate the expire / carry / insert / funnel / promote stages one by one. The orchestration
disappears; implement the dimension declaratively. Use the job code `cust` as the prefix for its temporary views:
1. Staging: keep the staging job's targets as materialized views (same names, columns and derivations). Build each
   staged snapshot in a plain module-level Python function (e.g. `def staged_snapshot(year): ...`) that reads the
   source CSV directly with the staging derivations; the staging materialized views return it.
2. History: `dp.create_streaming_table(name="<dimension>_history")` and
   `dp.create_auto_cdc_from_snapshot_flow(target="<dimension>_history", source=next_snapshot_and_version,
   keys=[<business key>], stored_as_scd_type=2, track_history_column_list=[<the column(s) the job compares to detect
   a change, e.g. the attribute hash>])`. `next_snapshot_and_version(latest_version)` returns
   (snapshot DataFrame, version) for the next snapshot in the order the notes give - None -> version 1, 1 -> version 2,
   then None. It must build the snapshot with the staging function, never by reading a pipeline dataset.
3. Outputs: materialized views with EXACTLY the names and columns of the dimension's target files (the dimension, its
   "next" file, any audit file). Downstream jobs read the dimension AFTER the orchestration has run, i.e. after every
   snapshot has been applied and the "next" file promoted - so the dimension and its "next" file have the SAME
   content (all versions after the last snapshot), never just the initial load. Derive them from the history table: version_no = row_number() OVER (PARTITION BY
   <business key> ORDER BY __START_AT); the current row is __END_AT IS NULL; compute the surrogate key, effective
   start / end keys and current flag exactly as the jobs' derivations define them for an initial version, a new
   version and an expired version; do not output __START_AT / __END_AT.
4. Audit outputs (e.g. change counts): compute them from the staged snapshots exactly as the job defines the flags.

# Style
- A short comment per dataset: `# <stage name>: <stage type>`.

# Self-check before answering
1. Every link in the input has exactly one temporary view (or materialized view for targets), prefixed with the job code.
2. Every target file has a materialized view with exactly its name and columns; a promoted dimension holds the
   state after the last snapshot.
3. Every `LIMIT` that has a Sort upstream (through Copy/Filter/Transformer) carries that Sort's `ORDER BY`.
4. Every CSV source is renamed by position with .toDF(...), has the column-count assert, and its non-nullable
   string columns are wrapped in coalesce(`col`, '').
5. No dbutils, writes, actions, legacy APIs or qualified names; no logic, column or table the input does not contain.
"""


def strip(code):
    code = code.replace("# Databricks notebook source\n", "")
    lines = code.split("\n")
    while lines and lines[0].startswith("# ") and "SDP source file" not in lines[0] and not lines[0].startswith("# COMMAND"):
        lines.pop(0)  # job title line
    code = "\n".join(l for l in lines if not l.startswith("# Layer:"))
    return code.replace("# COMMAND ----------\n", "").strip() + "\n"


def main():
    shots = []
    for job in SHOTS:
        shots.append({"role": "user", "content": (ROOT / "migration/switch_input" / f"{job}.txt").read_text()})
        shots.append({"role": "assistant", "content": strip((ROOT / "migration/sdp_reference" / f"{job}.py").read_text())})
    out = ROOT / "migration/switch_prompt/datastage_to_sdp.yml"
    out.write_text(yaml.safe_dump({"system_message": SYSTEM.strip() + "\n", "few_shots": shots}, sort_keys=False,
                                  width=1000, allow_unicode=True))
    print("wrote", out, f"{out.stat().st_size:,} bytes")


if __name__ == "__main__":
    main()
