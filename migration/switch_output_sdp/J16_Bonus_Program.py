# Databricks notebook source
# MAGIC %md
# MAGIC # J16_Bonus_Program
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261030063735-t49e/J16_Bonus_Program.txt`
# COMMAND ----------
# COMMAND ----------
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir", "")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# src_mart: dataset RETAIL_DATA_MART
@dp.temporary_view(name="j16_lnk_mart")
def j16_lnk_mart():
    return spark.read.table("RETAIL_DATA_MART")

# agg_Customer_Profit: Aggregator
@dp.temporary_view(name="j16_lnk_cust_profit")
def j16_lnk_cust_profit():
    return spark.sql(f"""
SELECT customer_id, CAST(SUM(gross_profit) AS decimal(18,2)) AS profit_total
FROM j16_lnk_mart
GROUP BY customer_id
""")

# srt_Profit: Sort
@dp.temporary_view(name="j16_lnk_ranked")
def j16_lnk_ranked():
    return spark.sql(f"""
SELECT * FROM j16_lnk_cust_profit
ORDER BY profit_total DESC
""")

# hd_Top100: Head
@dp.temporary_view(name="j16_lnk_top100")
def j16_lnk_top100():
    return spark.sql(f"""
SELECT * FROM j16_lnk_ranked
ORDER BY profit_total DESC
LIMIT 100
""")

# src_dim_customer: source file dim_customer.csv written by another job -> read the pipeline dataset
@dp.temporary_view(name="j16_lnk_dim_customer")
def j16_lnk_dim_customer():
    return spark.read.table("dim_customer")

# flt_Current_Customer: Filter
@dp.temporary_view(name="j16_lnk_cur")
def j16_lnk_cur():
    return spark.sql(f"""
SELECT * FROM j16_lnk_dim_customer
WHERE is_current = 1
""")

# jn_Customer: Join (inner) on customer_id
@dp.temporary_view(name="j16_lnk_named")
def j16_lnk_named():
    return spark.sql(f"""
SELECT
  l.customer_id,
  l.profit_total,
  r.customer_sk,
  r.cust_name,
  r.customer_type,
  r.loyalty_tier
FROM j16_lnk_top100 l
INNER JOIN j16_lnk_cur r ON l.customer_id = r.customer_id
""")

# xfm_Bonus: Transformer
@dp.temporary_view(name="j16_lnk_bonus")
def j16_lnk_bonus():
    return spark.sql(f"""
SELECT
  customer_id,
  profit_total,
  customer_sk,
  cust_name,
  customer_type,
  loyalty_tier,
  CAST(CASE WHEN loyalty_tier = 'PLATINUM' THEN 0.05 ELSE CASE WHEN loyalty_tier = 'GOLD' THEN 0.04 ELSE CASE WHEN loyalty_tier = 'SILVER' THEN 0.03 ELSE 0.02 END END END AS decimal(18,6)) AS bonus_rate,
  CAST(CASE WHEN loyalty_tier = 'PLATINUM' THEN profit_total * 0.05 ELSE CASE WHEN loyalty_tier = 'GOLD' THEN profit_total * 0.04 ELSE CASE WHEN loyalty_tier = 'SILVER' THEN profit_total * 0.03 ELSE profit_total * 0.02 END END END AS decimal(18,2)) AS bonus_amount,
  CAST('BONUS_2011' AS string) AS program
FROM j16_lnk_named
""")

# cpy_Bonus: Copy
@dp.temporary_view(name="j16_lnk_out")
def j16_lnk_out():
    return spark.sql(f"""
SELECT * FROM j16_lnk_bonus
""")

# cpy_Bonus: Copy
@dp.temporary_view(name="j16_lnk_total_in")
def j16_lnk_total_in():
    return spark.sql(f"""
SELECT * FROM j16_lnk_bonus
""")

# tgt_bonus_program: target file -> materialized view Q3_Bonus_Program
@dp.materialized_view(name="Q3_Bonus_Program")
def Q3_Bonus_Program():
    return spark.read.table("j16_lnk_out").select('customer_id', 'profit_total', 'customer_sk', 'cust_name', 'customer_type', 'loyalty_tier', 'bonus_rate', 'bonus_amount', 'program')

# agg_Bonus_Total: Aggregator
@dp.temporary_view(name="j16_lnk_total")
def j16_lnk_total():
    return spark.sql(f"""
SELECT program, CAST(SUM(bonus_amount) AS decimal(18,2)) AS bonus_total
FROM j16_lnk_total_in
GROUP BY program
""")

# tgt_bonus_summary: target file -> materialized view Q3_Bonus_Summary
@dp.materialized_view(name="Q3_Bonus_Summary")
def Q3_Bonus_Summary():
    return spark.read.table("j16_lnk_total").select('program', 'bonus_total')

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
