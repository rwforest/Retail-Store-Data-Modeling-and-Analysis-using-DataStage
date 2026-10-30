# Databricks notebook source
# MAGIC %md
# MAGIC # J19_Mart_Basket_Affinity
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261030063735-t49e/J19_Mart_Basket_Affinity.txt`
# COMMAND ----------
# COMMAND ----------
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir", "")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# src_fact_sales: dataset fact_sales (written by another job)
@dp.temporary_view(name="j19_lnk_fact")
def j19_lnk_fact():
    return spark.read.table("fact_sales")

# flt_Positive_Lines: Filter
@dp.temporary_view(name="j19_lnk_pos")
def j19_lnk_pos():
    return spark.sql(f"""
SELECT * FROM j19_lnk_fact
WHERE quantity > 0
""")

# xfm_Pair_Prep: Transformer
@dp.temporary_view(name="j19_lnk_prep")
def j19_lnk_prep():
    return spark.sql(f"""
SELECT
  invoice_no,
  stock_code
FROM j19_lnk_pos
""")

# rmd_Invoice_Product: Remove Duplicates (keep first; no Sort upstream, so arbitrary but stable-by-id order)
@dp.temporary_view(name="j19_lnk_dedup")
def j19_lnk_dedup():
    return spark.sql(f"""
SELECT invoice_no, stock_code
FROM (
  SELECT invoice_no, stock_code,
         row_number() OVER (PARTITION BY invoice_no, stock_code ORDER BY monotonically_increasing_id()) AS rn
  FROM j19_lnk_prep
) t
WHERE rn = 1
""")

# cpy_Basket: Copy
@dp.temporary_view(name="j19_lnk_a")
def j19_lnk_a():
    return spark.sql(f"""
SELECT invoice_no, stock_code FROM j19_lnk_dedup
""")

# cpy_Basket: Copy
@dp.temporary_view(name="j19_lnk_b_in")
def j19_lnk_b_in():
    return spark.sql(f"""
SELECT invoice_no, stock_code FROM j19_lnk_dedup
""")

# xfm_Rename_B: Transformer
@dp.temporary_view(name="j19_lnk_b")
def j19_lnk_b():
    return spark.sql(f"""
SELECT
  invoice_no,
  stock_code AS stock_code_b
FROM j19_lnk_b_in
""")

# jn_Self: Join (inner) on invoice_no
@dp.temporary_view(name="j19_lnk_pairs_all")
def j19_lnk_pairs_all():
    return spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  r.stock_code_b
FROM j19_lnk_a l
INNER JOIN j19_lnk_b r ON l.invoice_no = r.invoice_no
""")

# flt_Ordered_Pairs: Filter
@dp.temporary_view(name="j19_lnk_pairs")
def j19_lnk_pairs():
    return spark.sql(f"""
SELECT * FROM j19_lnk_pairs_all
WHERE stock_code < stock_code_b
""")

# agg_Pair_Counts: Aggregator
@dp.temporary_view(name="j19_lnk_counts")
def j19_lnk_counts():
    return spark.sql(f"""
SELECT stock_code, stock_code_b, CAST(COUNT(*) AS int) AS pair_count
FROM j19_lnk_pairs
GROUP BY stock_code, stock_code_b
""")

# flt_Min_Support: Filter
@dp.temporary_view(name="j19_lnk_support")
def j19_lnk_support():
    return spark.sql(f"""
SELECT * FROM j19_lnk_counts
WHERE pair_count >= 5
""")

# srt_Pairs: Sort
@dp.temporary_view(name="j19_lnk_sorted")
def j19_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j19_lnk_support
ORDER BY pair_count DESC
""")

# hd_Top1000: Head (first 1000 rows in the upstream sort order)
@dp.temporary_view(name="j19_lnk_top")
def j19_lnk_top():
    return spark.sql(f"""
SELECT * FROM j19_lnk_sorted
ORDER BY pair_count DESC
LIMIT 1000
""")

# tgt_mart_basket_affinity: target file -> materialized view Basket_Affinity
@dp.materialized_view(name="Basket_Affinity")
def Basket_Affinity():
    return spark.read.table("j19_lnk_top").select('stock_code', 'stock_code_b', 'pair_count')
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
