# Databricks notebook source
# act_Check_Dim_Customer: dim_customer.csv existed? -> EXISTS / MISSING (drives the initial-load branch).

# COMMAND ----------
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "retail_dw")
dbutils.widgets.text("src_dir", "")
catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))

state = "EXISTS" if spark.catalog.tableExists(f"{catalog}.{schema}.dim_customer") else "MISSING"
print("dim_state =", state)
dbutils.jobs.taskValues.set(key="dim_state", value=state)
