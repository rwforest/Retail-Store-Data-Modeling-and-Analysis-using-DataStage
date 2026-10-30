# Databricks notebook source
# MAGIC %md
# MAGIC # J15_Analysis_CustomerType_Profit
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261030063735-t49e/J15_Analysis_CustomerType_Profit.txt`
# COMMAND ----------
# COMMAND ----------
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir", "")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# src_mart: dataset RETAIL_DATA_MART (file written by another job, read by name)
@dp.temporary_view(name="j15_lnk_mart")
def j15_lnk_mart():
    return spark.read.table("RETAIL_DATA_MART")

# agg_By_Type: Aggregator
@dp.temporary_view(name="j15_lnk_by_type")
def j15_lnk_by_type():
    return spark.sql(f"""
SELECT customer_type, CAST(SUM(gross_profit) AS decimal(18,2)) AS profit_total, CAST(SUM(sales_amount) AS decimal(18,2)) AS sales_total, CAST(SUM(line_cnt) AS bigint) AS lines
FROM j15_lnk_mart
GROUP BY customer_type
""")

# srt_Profit: Sort
@dp.temporary_view(name="j15_lnk_ranked")
def j15_lnk_ranked():
    return spark.sql(f"""
SELECT * FROM j15_lnk_by_type
ORDER BY profit_total DESC
""")

# cpy_Ranked: Copy
@dp.temporary_view(name="j15_lnk_all")
def j15_lnk_all():
    return spark.sql(f"""
SELECT * FROM j15_lnk_ranked
""")

# cpy_Ranked: Copy
@dp.temporary_view(name="j15_lnk_win_in")
def j15_lnk_win_in():
    return spark.sql(f"""
SELECT * FROM j15_lnk_ranked
""")

# tgt_customer_type_ranking: target file -> materialized view Q2_CustomerType_Profit
@dp.materialized_view(name="Q2_CustomerType_Profit")
def Q2_CustomerType_Profit():
    return spark.read.table("j15_lnk_all").select('customer_type', 'profit_total', 'sales_total', 'lines')

# hd_Winner: Head
@dp.temporary_view(name="j15_lnk_winner")
def j15_lnk_winner():
    return spark.sql(f"""
SELECT * FROM j15_lnk_win_in
ORDER BY profit_total DESC
LIMIT 1
""")

# tgt_customer_type_winner: target file -> materialized view Q2_Winning_CustomerType
@dp.materialized_view(name="Q2_Winning_CustomerType")
def Q2_Winning_CustomerType():
    return spark.read.table("j15_lnk_winner").select('customer_type', 'profit_total', 'sales_total', 'lines')
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
