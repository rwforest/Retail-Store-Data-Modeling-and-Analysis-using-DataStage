# Databricks notebook source
# J07_Dim_Product - Build Dim_Product with surrogate key and category group
# Layer: 2_Dimensions. Compiled from the DataStage job by migration/ds2spark.py; do not edit by hand.

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
# src_stg_products: Delta table stg_products
lnk_stg = spark.table(f"{catalog}.{schema}.stg_products")
lnk_stg.createOrReplaceTempView("lnk_stg")

# COMMAND ----------
# rmd_Product: Remove Duplicates
lnk_dedup = spark.sql(f"""
SELECT stock_code, product_name, category, list_price, supplier_id, status, unit_cost, price_band, supplier_name, supplier_country, lead_time_days, reliability_score FROM (
  SELECT *, row_number() OVER (PARTITION BY stock_code ORDER BY monotonically_increasing_id()) AS _rn FROM lnk_stg
) WHERE _rn = 1
""")
lnk_dedup.createOrReplaceTempView("lnk_dedup")

# COMMAND ----------
# srt_Product: Sort
lnk_sorted = spark.sql(f"""
SELECT * FROM lnk_dedup
ORDER BY stock_code ASC
""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")

# COMMAND ----------
# xfm_Product_SK: Transformer
lnk_dim = spark.sql(f"""
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
FROM lnk_sorted
""")
lnk_dim.createOrReplaceTempView("lnk_dim")

# COMMAND ----------
# xfm_Product_Cols: Transformer
lnk_dim_cols = spark.sql(f"""
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
FROM lnk_dim
""")
lnk_dim_cols.createOrReplaceTempView("lnk_dim_cols")

# COMMAND ----------
# cpy_Dim_Product: Copy
lnk_dim_out = spark.sql(f"""
SELECT * FROM lnk_dim_cols
""")
lnk_dim_out.createOrReplaceTempView("lnk_dim_out")

# COMMAND ----------
# cpy_Dim_Product: Copy
lnk_audit_in = spark.sql(f"""
SELECT * FROM lnk_dim_cols
""")
lnk_audit_in.createOrReplaceTempView("lnk_audit_in")

# COMMAND ----------
# tgt_dim_product: write Delta table dim_product
(lnk_dim_out.select('stock_code', 'product_name', 'category', 'list_price', 'supplier_id', 'status', 'unit_cost', 'price_band', 'supplier_name', 'supplier_country', 'lead_time_days', 'reliability_score', 'product_sk', 'category_group').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_product"))
print("dim_product:", spark.table(f"{catalog}.{schema}.dim_product").count(), "rows")

# COMMAND ----------
# agg_Products_By_Category: Aggregator
lnk_cat_counts = spark.sql(f"""
SELECT category, CAST(COUNT(*) AS int) AS products
FROM lnk_audit_in
GROUP BY category
""")
lnk_cat_counts.createOrReplaceTempView("lnk_cat_counts")

# COMMAND ----------
# tgt_audit_products_by_category: write Delta table audit_products_by_category
(lnk_cat_counts.select('category', 'products').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.audit_products_by_category"))
print("audit_products_by_category:", spark.table(f"{catalog}.{schema}.audit_products_by_category").count(), "rows")
