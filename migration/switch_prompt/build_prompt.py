#!/usr/bin/env python3
"""Build the Switch conversion prompt (DataStage job description -> Databricks Python notebook).

    python3 migration/switch_prompt/build_prompt.py   # writes migration/switch_prompt/datastage_to_notebook.yml

Few-shots pair condensed DataStage jobs (migration/switch_input, from dsx_condense.py) with the notebooks that
migration/ds2spark.py compiles, so Switch follows the same conventions: widgets, one temp view per link, Delta tables
instead of files, Spark SQL for derivations.
"""
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
SHOTS = ["J04_Land_Stores_Employees", "J15_Analysis_CustomerType_Profit"]  # positional CSV read; Sort -> Copy -> Head

SYSTEM = r"""
Convert a DataStage job description into a Databricks Python notebook (PySpark + Spark SQL) according to these rules.

# Input and Output
- Input: a text rendering of ONE DataStage parallel job: parameters, stages (with properties), links between stages and
  each link's columns with their derivations (":=").
- Output: Python code with Python comments (in {comment_lang}) only. Do NOT output a Declarative Pipeline / `dp.table`
  / `pyspark.pipelines` code: this is a plain batch notebook that a Databricks Job runs.
- Convert EVERYTHING in the job, one section per stage, in data-flow order. Never invent logic: use only what the input
  states. If something cannot be converted, say so in a comment.
- DO NOT output cell separators or notebook-level descriptions; another program adds them.

# Parameters
Start with exactly these widgets, then read them (they are the only parameters, whatever the job declares):
    dbutils.widgets.text("catalog", ""); dbutils.widgets.text("schema", "retail_dw"); dbutils.widgets.text("src_dir", "")
    dbutils.widgets.text("pRunDate", "2011-12-31"); dbutils.widgets.text("pEffDate", "2011-07-01")
    catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))
    pRunDate, pEffDate = dbutils.widgets.get("pRunDate"), dbutils.widgets.get("pEffDate")
    spark.sql(f"USE {catalog}.{schema}")
DataStage job parameters #pSrcDir# -> `src_dir`; #pStgDir#, #pDwhDir#, #pMartDir#, #pAuditDir# have no equivalent:
those files become Delta tables in {catalog}.{schema}. #pRunDate# / #pEffDate# stay as pRunDate / pEffDate.

# Stages -> code (one temp view per link, named exactly like the link)
- Sequential File source with path #pSrcDir#/<file>.csv: read the CSV from f"{src_dir}/<file>.csv" with header=true.
  DataStage maps file columns BY POSITION, never by header text (headers in the file may differ from the job's column
  names). So: assert the column count equals the link's column count, rename with .toDF(<link column names in order>),
  register it as a view, then SELECT every column with a CAST to its declared type.
  EMPTY FIELDS: Spark reads an empty CSV field as NULL, DataStage reads it as "" for a non-nullable string column.
  Wrap every non-nullable string source column in coalesce(`col`, '') - otherwise checks such as
  `Len(Trim(x)) = 0` never fire (Len of NULL is NULL).
- Sequential File source with any other path: `spark.table(f"{catalog}.{schema}.<file stem>")`.
- Sequential File target: write a Delta table `{catalog}.{schema}.<file stem>` (mode overwrite, overwriteSchema true;
  append only if the stage says append), selecting the link's columns in order.
- Transformer: SELECT with one expression per output column, each wrapped in CAST(... AS <column type>).
  Input-link prefixes (link.col) are dropped. A column derived as just `link.col` is passed through.
- Filter: SELECT * ... WHERE <expression>. Join: SELECT the output link's columns FROM left l <type> JOIN right r ON the
  key columns. Key columns come from the left side. innerjoin/leftouterjoin/rightouterjoin/fullouterjoin.
- Aggregator: GROUP BY the keys; "count" -> CAST(COUNT(*) AS int); "reduce" sum/min/max -> CAST(SUM|MIN|MAX(col) AS type).
- Sort: ORDER BY keys (asc/desc). Copy: one view per output link. Funnel: UNION ALL of the inputs' columns.
- ROW ORDER (critical): Spark views do NOT keep row order. A DataStage Sort's order flows to every downstream stage
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
Run each SQL statement with spark.sql(f-string), assign the result to a Python variable named after the link, and call
createOrReplaceTempView("<link>").

# Style
- Use only `spark`, `dbutils`, `F` from pyspark.sql.functions if needed. DO NOT create a SparkSession.
- A short comment per stage: `# <stage name>: <stage type>`.
- Print the row count after each table write.

# Self-check before answering
1. Every link in the input has exactly one view (or table write), named like the link.
2. Every `LIMIT` that has a Sort upstream (through Copy/Filter/Transformer) carries that Sort's `ORDER BY`.
3. Every CSV source is renamed by position with .toDF(...), has the column-count assert, and its non-nullable
   string columns are wrapped in coalesce(`col`, '').
4. Every output view selects exactly the output link's columns, in the link's order, with the declared types.
5. No logic, column or table appears that the input does not contain.
"""


def strip(code):
    code = code.replace("# Databricks notebook source\n", "")
    return re.sub(r"^# COMMAND ----------\n", "", code, flags=re.M).strip() + "\n"


def main():
    shots = []
    for job in SHOTS:
        shots.append({"role": "user", "content": (ROOT / "migration/switch_input" / f"{job}.txt").read_text()})
        nb = (ROOT / "migration/bundle/src/notebooks" / f"{job}.py").read_text()
        # drop the header comment lines (title/layer) that are not part of the code body
        nb = nb.split("\n", 3)[3] if nb.startswith("# Databricks notebook source") else nb
        shots.append({"role": "assistant", "content": strip(nb)})
    out = ROOT / "migration/switch_prompt/datastage_to_notebook.yml"
    out.write_text(yaml.safe_dump({"system_message": SYSTEM.strip() + "\n", "few_shots": shots}, sort_keys=False,
                                  width=1000, allow_unicode=True))
    print("wrote", out, f"{out.stat().st_size:,} bytes")


if __name__ == "__main__":
    main()
