# Databricks notebook source
# MAGIC %md
# MAGIC # J07_Dim_Product
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261029064410-juws/J07_Dim_Product.txt`
# COMMAND ----------
# COMMAND ----------
dbutils.widgets.text("catalog", ""); dbutils.widgets.text("schema", "retail_dw"); dbutils.widgets.text("src_dir", "")
dbutils.widgets.text("pRunDate", "2011-12-31"); dbutils.widgets.text("pEffDate", "2011-07-01")
catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))
pRunDate, pEffDate = dbutils.widgets.get("pRunDate"), dbutils.widgets.get("pEffDate")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------
# src_stg_products: Sequential File source (#pStgDir#/stg_products.csv -> Delta table stg_products)
lnk_stg = spark.table(f"{catalog}.{schema}.stg_products")
lnk_stg.createOrReplaceTempView("lnk_stg")

# COMMAND ----------
# rmd_Product: Remove Duplicates (key stock_code, keep first)
# No Sort upstream, so "first" is defined by monotonically_increasing_id()
lnk_dedup = spark.sql(f"""
SELECT stock_code, product_name, category, list_price, supplier_id, status, unit_cost,
       price_band, supplier_name, supplier_country, lead_time_days, reliability_score
FROM (
  SELECT *, row_number() OVER (PARTITION BY stock_code ORDER BY _mid) AS _rn
  FROM (SELECT *, monotonically_increasing_id() AS _mid FROM lnk_stg)
)
WHERE _rn = 1
""")
lnk_dedup.createOrReplaceTempView("lnk_dedup")

# COMMAND ----------
# srt_Product: Sort (stock_code asc)
lnk_sorted = spark.sql(f"""
SELECT * FROM lnk_dedup
ORDER BY stock_code
""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")

# COMMAND ----------
# xfm_Product_SK: Transformer
# product_sk = surrogate key idiom -> row_number() over the upstream sort order (stock_code)
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
  CAST(row_number() OVER (ORDER BY stock_code) AS int) AS product_sk,
  CAST(CASE WHEN (category = 'Seasonal') THEN 'SEASONAL' ELSE CASE WHEN (category = 'General Merchandise') THEN 'OTHER' ELSE 'CORE' END END AS string) AS category_group
FROM lnk_sorted
""")
lnk_dim.createOrReplaceTempView("lnk_dim")

# COMMAND ----------
# xfm_Product_Cols: Transformer (all columns passed through)
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
(lnk_dim_out.select('stock_code', 'product_name', 'category', 'list_price', 'supplier_id', 'status',
                    'unit_cost', 'price_band', 'supplier_name', 'supplier_country', 'lead_time_days',
                    'reliability_score', 'product_sk', 'category_group').write.mode("overwrite")
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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
