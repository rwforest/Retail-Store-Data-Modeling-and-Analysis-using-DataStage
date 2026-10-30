# Databricks notebook source
# MAGIC %md
# MAGIC # J20_Mart_Store_Monthly
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261030063735-t49e/J20_Mart_Store_Monthly.txt`
# COMMAND ----------
# COMMAND ----------
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir", "")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# src_fact_sales: dataset fact_sales (written by another job)
@dp.temporary_view(name="j20_lnk_fact")
def j20_lnk_fact():
    return spark.read.table("fact_sales")

# agg_Store_Month: Aggregator
@dp.temporary_view(name="j20_lnk_sm")
def j20_lnk_sm():
    return spark.sql(f"""
SELECT store_sk, year_month, CAST(SUM(sales_amount) AS decimal(18,2)) AS sales, CAST(SUM(gross_profit) AS decimal(18,2)) AS profit, CAST(SUM(line_cnt) AS bigint) AS lines
FROM j20_lnk_fact
GROUP BY store_sk, year_month
""")

# src_dim_store: dataset dim_store (written by another job)
@dp.temporary_view(name="j20_lnk_dim_store")
def j20_lnk_dim_store():
    return spark.read.table("dim_store")

# jn_Store: Join (inner) on store_sk
@dp.temporary_view(name="j20_lnk_named")
def j20_lnk_named():
    return spark.sql(f"""
SELECT
  l.store_sk,
  l.year_month,
  l.sales,
  l.profit,
  l.lines,
  r.store_id,
  r.store_name,
  r.no_of_employees,
  r.region
FROM j20_lnk_sm l
INNER JOIN j20_lnk_dim_store r ON l.store_sk = r.store_sk
""")

# xfm_Store_KPIs: Transformer
@dp.temporary_view(name="j20_lnk_kpi")
def j20_lnk_kpi():
    return spark.sql(f"""
SELECT
  store_sk,
  year_month,
  sales,
  profit,
  lines,
  store_id,
  store_name,
  no_of_employees,
  region,
  CAST(CASE WHEN no_of_employees > 0 THEN sales / no_of_employees ELSE 0 END AS decimal(18,2)) AS sales_per_employee,
  CAST(CASE WHEN sales > 0 THEN profit / sales ELSE 0 END AS decimal(18,6)) AS margin_pct
FROM j20_lnk_named
""")

# srt_Store_Month: Sort
@dp.temporary_view(name="j20_lnk_sorted")
def j20_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j20_lnk_kpi
ORDER BY store_sk ASC, year_month ASC
""")

# tgt_mart_store_monthly: target file -> materialized view Store_Monthly
@dp.materialized_view(name="Store_Monthly")
def Store_Monthly():
    return spark.read.table("j20_lnk_sorted").select('store_sk', 'year_month', 'sales', 'profit', 'lines', 'store_id', 'store_name', 'no_of_employees', 'region', 'sales_per_employee', 'margin_pct')

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
