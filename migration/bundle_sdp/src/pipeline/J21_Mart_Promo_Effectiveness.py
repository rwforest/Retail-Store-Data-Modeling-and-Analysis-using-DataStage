# Databricks notebook source
# MAGIC %md
# MAGIC # J21_Mart_Promo_Effectiveness
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261030063735-t49e/J21_Mart_Promo_Effectiveness.txt`
# COMMAND ----------
# COMMAND ----------
from pyspark import pipelines as dp

# src_dir is not used anywhere else in this pipeline, so provide a default to avoid SQL_CONF_NOT_FOUND
src_dir = spark.conf.get("src_dir", "")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# src_fact_sales: dataset fact_sales (written by another job)
@dp.temporary_view(name="j21_lnk_fact")
def j21_lnk_fact():
    return spark.read.table("fact_sales")

# src_dim_promotion: dataset dim_promotion (written by another job)
@dp.temporary_view(name="j21_lnk_dim_promo")
def j21_lnk_dim_promo():
    return spark.read.table("dim_promotion")

# jn_Promo_Category: Join (inner) on category
@dp.temporary_view(name="j21_lnk_fp")
def j21_lnk_fp():
    return spark.sql(f"""
SELECT
  l.invoice_date_key,
  l.category,
  l.sales_amount,
  l.gross_profit,
  l.line_cnt,
  r.promo_id,
  r.discount_pct,
  r.start_key,
  r.end_key
FROM j21_lnk_fact l
INNER JOIN j21_lnk_dim_promo r ON l.category = r.category
""")

# flt_In_Window: Filter
@dp.temporary_view(name="j21_lnk_window")
def j21_lnk_window():
    return spark.sql(f"""
SELECT * FROM j21_lnk_fp
WHERE invoice_date_key >= start_key AND invoice_date_key <= end_key
""")

# agg_By_Promo: Aggregator
@dp.temporary_view(name="j21_lnk_promo")
def j21_lnk_promo():
    return spark.sql(f"""
SELECT promo_id, category, discount_pct,
  CAST(SUM(sales_amount) AS decimal(18,2)) AS promo_sales,
  CAST(SUM(gross_profit) AS decimal(18,2)) AS promo_profit,
  CAST(SUM(line_cnt) AS bigint) AS promo_lines
FROM j21_lnk_window
GROUP BY promo_id, category, discount_pct
""")

# xfm_Discount_Cost: Transformer
@dp.temporary_view(name="j21_lnk_cost")
def j21_lnk_cost():
    return spark.sql(f"""
SELECT
  promo_id,
  category,
  discount_pct,
  promo_sales,
  promo_profit,
  promo_lines,
  CAST(promo_sales * discount_pct / 100 AS decimal(18,2)) AS est_discount_cost,
  CAST(promo_profit - (promo_sales * discount_pct / 100) AS decimal(18,2)) AS profit_after_discount
FROM j21_lnk_promo
""")

# srt_Promo: Sort
@dp.temporary_view(name="j21_lnk_sorted")
def j21_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j21_lnk_cost
ORDER BY profit_after_discount DESC
""")

# tgt_mart_promo_effectiveness: target file -> materialized view Promo_Effectiveness
@dp.materialized_view(name="Promo_Effectiveness")
def Promo_Effectiveness():
    return spark.read.table("j21_lnk_sorted").select('promo_id', 'category', 'discount_pct', 'promo_sales', 'promo_profit', 'promo_lines', 'est_discount_cost', 'profit_after_discount')
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
