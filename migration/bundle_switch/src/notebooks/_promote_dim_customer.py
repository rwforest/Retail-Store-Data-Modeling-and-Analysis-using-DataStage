# Databricks notebook source
# act_Promote_Dim_Customer: mv dim_customer_next.csv dim_customer.csv -> replace the Delta table.

# COMMAND ----------
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "retail_dw")
dbutils.widgets.text("src_dir", "")
catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))

spark.sql(f"CREATE OR REPLACE TABLE {catalog}.{schema}.dim_customer AS SELECT * FROM {catalog}.{schema}.dim_customer_next")
print(spark.table(f"{catalog}.{schema}.dim_customer").count(), "rows promoted")
