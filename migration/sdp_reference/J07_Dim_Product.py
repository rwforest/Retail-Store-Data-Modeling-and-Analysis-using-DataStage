# Databricks notebook source
# J07_Dim_Product - Build Dim_Product with surrogate key and category group
# Layer: 2_Dimensions. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_stg_products: dataset stg_products
@dp.temporary_view(name="j07_lnk_stg")
def j07_lnk_stg():
    return spark.read.table("stg_products")

# COMMAND ----------
# rmd_Product: Remove Duplicates
@dp.temporary_view(name="j07_lnk_dedup")
def j07_lnk_dedup():
    return spark.sql(f"""
SELECT stock_code, product_name, category, list_price, supplier_id, status, unit_cost, price_band, supplier_name, supplier_country, lead_time_days, reliability_score FROM (
  SELECT *, row_number() OVER (PARTITION BY stock_code ORDER BY monotonically_increasing_id()) AS _rn FROM j07_lnk_stg
) WHERE _rn = 1
""")

# COMMAND ----------
# srt_Product: Sort
@dp.temporary_view(name="j07_lnk_sorted")
def j07_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j07_lnk_dedup
ORDER BY stock_code ASC
""")

# COMMAND ----------
# xfm_Product_SK: Transformer
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
  CAST(row_number() OVER (ORDER BY stock_code ASC) AS int) AS product_sk,
  CAST(CASE WHEN (category = 'Seasonal') THEN 'SEASONAL' ELSE CASE WHEN (category = 'General Merchandise') THEN 'OTHER' ELSE 'CORE' END END AS string) AS category_group
FROM j07_lnk_sorted
""")

# COMMAND ----------
# xfm_Product_Cols: Transformer
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

# COMMAND ----------
# cpy_Dim_Product: Copy
@dp.temporary_view(name="j07_lnk_dim_out")
def j07_lnk_dim_out():
    return spark.sql(f"""
SELECT * FROM j07_lnk_dim_cols
""")

# COMMAND ----------
# cpy_Dim_Product: Copy
@dp.temporary_view(name="j07_lnk_audit_in")
def j07_lnk_audit_in():
    return spark.sql(f"""
SELECT * FROM j07_lnk_dim_cols
""")

# COMMAND ----------
# tgt_dim_product: target file -> materialized view dim_product
@dp.materialized_view(name="dim_product")
def dim_product():
    return spark.read.table("j07_lnk_dim_out").select('stock_code', 'product_name', 'category', 'list_price', 'supplier_id', 'status', 'unit_cost', 'price_band', 'supplier_name', 'supplier_country', 'lead_time_days', 'reliability_score', 'product_sk', 'category_group')

# COMMAND ----------
# agg_Products_By_Category: Aggregator
@dp.temporary_view(name="j07_lnk_cat_counts")
def j07_lnk_cat_counts():
    return spark.sql(f"""
SELECT category, CAST(COUNT(*) AS int) AS products
FROM j07_lnk_audit_in
GROUP BY category
""")

# COMMAND ----------
# tgt_audit_products_by_category: target file -> materialized view audit_products_by_category
@dp.materialized_view(name="audit_products_by_category")
def audit_products_by_category():
    return spark.read.table("j07_lnk_cat_counts").select('category', 'products')
