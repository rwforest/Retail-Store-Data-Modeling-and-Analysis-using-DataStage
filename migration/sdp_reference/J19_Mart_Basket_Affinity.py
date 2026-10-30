# Databricks notebook source
# J19_Mart_Basket_Affinity - Market-basket: product pairs bought on the same invoice
# Layer: 4_Marts. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_fact_sales: dataset fact_sales
@dp.temporary_view(name="j19_lnk_fact")
def j19_lnk_fact():
    return spark.read.table("fact_sales")

# COMMAND ----------
# flt_Positive_Lines: Filter
@dp.temporary_view(name="j19_lnk_pos")
def j19_lnk_pos():
    return spark.sql(f"""
SELECT * FROM j19_lnk_fact
WHERE (quantity > 0)
""")

# COMMAND ----------
# xfm_Pair_Prep: Transformer
@dp.temporary_view(name="j19_lnk_prep")
def j19_lnk_prep():
    return spark.sql(f"""
SELECT
  invoice_no,
  stock_code
FROM j19_lnk_pos
""")

# COMMAND ----------
# rmd_Invoice_Product: Remove Duplicates
@dp.temporary_view(name="j19_lnk_dedup")
def j19_lnk_dedup():
    return spark.sql(f"""
SELECT invoice_no, stock_code FROM (
  SELECT *, row_number() OVER (PARTITION BY invoice_no, stock_code ORDER BY monotonically_increasing_id()) AS _rn FROM j19_lnk_prep
) WHERE _rn = 1
""")

# COMMAND ----------
# cpy_Basket: Copy
@dp.temporary_view(name="j19_lnk_a")
def j19_lnk_a():
    return spark.sql(f"""
SELECT * FROM j19_lnk_dedup
""")

# COMMAND ----------
# cpy_Basket: Copy
@dp.temporary_view(name="j19_lnk_b_in")
def j19_lnk_b_in():
    return spark.sql(f"""
SELECT * FROM j19_lnk_dedup
""")

# COMMAND ----------
# xfm_Rename_B: Transformer
@dp.temporary_view(name="j19_lnk_b")
def j19_lnk_b():
    return spark.sql(f"""
SELECT
  invoice_no,
  stock_code AS stock_code_b
FROM j19_lnk_b_in
""")

# COMMAND ----------
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

# COMMAND ----------
# flt_Ordered_Pairs: Filter
@dp.temporary_view(name="j19_lnk_pairs")
def j19_lnk_pairs():
    return spark.sql(f"""
SELECT * FROM j19_lnk_pairs_all
WHERE (stock_code < stock_code_b)
""")

# COMMAND ----------
# agg_Pair_Counts: Aggregator
@dp.temporary_view(name="j19_lnk_counts")
def j19_lnk_counts():
    return spark.sql(f"""
SELECT stock_code, stock_code_b, CAST(COUNT(*) AS int) AS pair_count
FROM j19_lnk_pairs
GROUP BY stock_code, stock_code_b
""")

# COMMAND ----------
# flt_Min_Support: Filter
@dp.temporary_view(name="j19_lnk_support")
def j19_lnk_support():
    return spark.sql(f"""
SELECT * FROM j19_lnk_counts
WHERE (pair_count >= 5)
""")

# COMMAND ----------
# srt_Pairs: Sort
@dp.temporary_view(name="j19_lnk_sorted")
def j19_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j19_lnk_support
ORDER BY pair_count DESC
""")

# COMMAND ----------
# hd_Top1000: Head
@dp.temporary_view(name="j19_lnk_top")
def j19_lnk_top():
    return spark.sql(f"""
SELECT * FROM j19_lnk_sorted
ORDER BY pair_count DESC
LIMIT 1000
""")

# COMMAND ----------
# tgt_mart_basket_affinity: target file -> materialized view Basket_Affinity
@dp.materialized_view(name="Basket_Affinity")
def Basket_Affinity():
    return spark.read.table("j19_lnk_top").select('stock_code', 'stock_code_b', 'pair_count')
