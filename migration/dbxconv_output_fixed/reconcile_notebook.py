# Databricks notebook source
import json

dbutils.widgets.text(name='catalog', defaultValue='unitygo')
dbutils.widgets.text(name='schema_a', defaultValue='retail_dw_dbxconv')
dbutils.widgets.text(name='schema_b', defaultValue='retail_dw_sdp')
catalog = dbutils.widgets.get("catalog")
schema_a = dbutils.widgets.get("schema_a")
schema_b = dbutils.widgets.get("schema_b")

# COMMAND ----------
tables_a = {r.tableName for r in spark.sql(f"SHOW TABLES IN {catalog}.{schema_a}").collect()}
tables_b = {r.tableName for r in spark.sql(f"SHOW TABLES IN {catalog}.{schema_b}").collect()}
common = sorted(tables_a & tables_b)
only_a = sorted(tables_a - tables_b)
only_b = sorted(tables_b - tables_a)

# COMMAND ----------
results = []
for t in common:
    row = {"table": t}
    try:
        df_a = spark.table(f"{catalog}.{schema_a}.{t}")
        df_b = spark.table(f"{catalog}.{schema_b}.{t}")
        cols_a = set(df_a.columns)
        cols_b = set(df_b.columns)
        row["schemaMatch"] = (cols_a == cols_b)
        row["colsOnlyInA"] = sorted(cols_a - cols_b)
        row["colsOnlyInB"] = sorted(cols_b - cols_a)
        row["countA"] = df_a.count()
        row["countB"] = df_b.count()
        if row["schemaMatch"]:
            common_cols = sorted(cols_a)
            sel_a = df_a.select(*common_cols)
            sel_b = df_b.select(*common_cols)
            only_in_a = sel_a.exceptAll(sel_b).count()
            only_in_b = sel_b.exceptAll(sel_a).count()
            row["rowsOnlyInA"] = only_in_a
            row["rowsOnlyInB"] = only_in_b
            row["match"] = (only_in_a == 0 and only_in_b == 0)
        else:
            row["rowsOnlyInA"] = None
            row["rowsOnlyInB"] = None
            row["match"] = False
    except Exception as e:
        row["error"] = repr(e)
        row["match"] = False
    results.append(row)

# COMMAND ----------
summary = {
    "tablesOnlyInA": only_a,
    "tablesOnlyInB": only_b,
    "commonTables": len(common),
    "matchingTables": sum(1 for r in results if r.get("match")),
    "mismatchedTables": [r["table"] for r in results if not r.get("match")],
    "details": results,
}
dbutils.notebook.exit(json.dumps(summary, default=str))
