# Databricks notebook source
# MAGIC %md
# MAGIC # J07_Dim_Product
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261030063735-t49e/J07_Dim_Product.txt`
# COMMAND ----------
# COMMAND ----------
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir", "")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# src_stg_products: Sequential File (written by another job -> read the pipeline dataset stg_products)
@dp.temporary_view(name="j07_lnk_stg")
def j07_lnk_stg():
    return spark.read.table("stg_products")

# rmd_Product: Remove Duplicates (keep first per stock_code; no upstream Sort, so arbitrary but stable-by-id order)
@dp.temporary_view(name="j07_lnk_dedup")
def j07_lnk_dedup():
    return spark.sql(f"""
SELECT stock_code, product_name, category, list_price, supplier_id, status, unit_cost, price_band,
       supplier_name, supplier_country, lead_time_days, reliability_score
FROM (
  SELECT *, row_number() OVER (PARTITION BY stock_code ORDER BY monotonically_increasing_id()) AS rn
  FROM j07_lnk_stg
) t
WHERE rn = 1
""")

# srt_Product: Sort (by stock_code ascending)
@dp.temporary_view(name="j07_lnk_sorted")
def j07_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j07_lnk_dedup
ORDER BY stock_code ASC
""")

# xfm_Product_SK: Transformer (surrogate key follows the upstream sort order: stock_code)
@dp.temporary_view(name="j07_lnk_dim")
def j07_lnk_dim():
    return spark.sql(f"""
SELECT
  stock_code,
  product_name,
  category,
  list_price,
  supplier_id,
  status,
  unit_cost,
  price_band,
  supplier_name,
  supplier_country,
  lead_time_days,
  reliability_score,
  CAST(row_number() OVER (ORDER BY stock_code) AS int) AS product_sk,
  CAST(CASE WHEN category = 'Seasonal' THEN 'SEASONAL' ELSE CASE WHEN category = 'General Merchandise' THEN 'OTHER' ELSE 'CORE' END END AS string) AS category_group
FROM j07_lnk_sorted
""")

# xfm_Product_Cols: Transformer (all columns passed through)
@dp.temporary_view(name="j07_lnk_dim_cols")
def j07_lnk_dim_cols():
    return spark.sql(f"""
SELECT
  stock_code,
  product_name,
  category,
  list_price,
  supplier_id,
  status,
  unit_cost,
  price_band,
  supplier_name,
  supplier_country,
  lead_time_days,
  reliability_score,
  product_sk,
  category_group
FROM j07_lnk_dim
""")

# cpy_Dim_Product: Copy
@dp.temporary_view(name="j07_lnk_dim_out")
def j07_lnk_dim_out():
    return spark.sql(f"""
SELECT * FROM j07_lnk_dim_cols
""")

# cpy_Dim_Product: Copy
@dp.temporary_view(name="j07_lnk_audit_in")
def j07_lnk_audit_in():
    return spark.sql(f"""
SELECT * FROM j07_lnk_dim_cols
""")

# tgt_dim_product: target file -> materialized view dim_product
@dp.materialized_view(name="dim_product")
def dim_product():
    return spark.read.table("j07_lnk_dim_out").select('stock_code', 'product_name', 'category', 'list_price', 'supplier_id', 'status', 'unit_cost', 'price_band', 'supplier_name', 'supplier_country', 'lead_time_days', 'reliability_score', 'product_sk', 'category_group')

# agg_Products_By_Category: Aggregator
@dp.temporary_view(name="j07_lnk_cat_counts")
def j07_lnk_cat_counts():
    return spark.sql(f"""
SELECT category, CAST(COUNT(*) AS int) AS products
FROM j07_lnk_audit_in
GROUP BY category
""")

# tgt_audit_products_by_category: target file -> materialized view audit_products_by_category
@dp.materialized_view(name="audit_products_by_category")
def audit_products_by_category():
    return spark.read.table("j07_lnk_cat_counts").select('category', 'products')
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
