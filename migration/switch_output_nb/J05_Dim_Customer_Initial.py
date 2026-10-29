# Databricks notebook source
# MAGIC %md
# MAGIC # J05_Dim_Customer_Initial
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028071346-rdb5/J05_Dim_Customer_Initial.txt`
# COMMAND ----------
# COMMAND ----------
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "retail_dw")
dbutils.widgets.text("src_dir", "")
dbutils.widgets.text("pRunDate", "2011-12-31")
dbutils.widgets.text("pEffDate", "2011-07-01")
catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))
pRunDate, pEffDate = dbutils.widgets.get("pRunDate"), dbutils.widgets.get("pEffDate")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------
# src_stg_customers_2010: Sequential File source (staging file -> Delta table stg_customers_2010)
lnk_stg = spark.table(f"{catalog}.{schema}.stg_customers_2010")
lnk_stg.createOrReplaceTempView("lnk_stg")

# COMMAND ----------
# xfm_Version1: Transformer
lnk_v1 = spark.sql(f"""
SELECT
  CAST(((customer_id * 1000) + 1) AS int) AS customer_sk,
  customer_id,
  cust_name,
  gender,
  age,
  spending_score,
  annual_income_k,
  country,
  customer_type,
  loyalty_tier,
  email,
  city,
  attr_hash,
  CAST(1 AS int) AS version_no,
  CAST(19000101 AS int) AS eff_start_key,
  CAST(99991231 AS int) AS eff_end_key,
  CAST(1 AS int) AS is_current
FROM lnk_stg
""")
lnk_v1.createOrReplaceTempView("lnk_v1")

# COMMAND ----------
# srt_Dim_Customer: Sort
lnk_sorted = spark.sql(f"""
SELECT * FROM lnk_v1
ORDER BY customer_id ASC, version_no ASC
""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")

# COMMAND ----------
# tgt_dim_customer: Sequential File target (written as Delta table dim_customer, overwrite)
(lnk_sorted.select('customer_sk', 'customer_id', 'cust_name', 'gender', 'age', 'spending_score', 'annual_income_k', 'country', 'customer_type', 'loyalty_tier', 'email', 'city', 'attr_hash', 'version_no', 'eff_start_key', 'eff_end_key', 'is_current').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_customer"))
print("dim_customer:", spark.table(f"{catalog}.{schema}.dim_customer").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
