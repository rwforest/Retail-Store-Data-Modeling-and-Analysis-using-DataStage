# Databricks notebook source
# MAGIC %md
# MAGIC # J03_Land_Products
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028235854-szp7/J03_Land_Products.txt`
# COMMAND ----------
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
# src_products: Sequential File source products_catalog.csv (columns mapped by position, as DataStage does)
_lnk_products_raw = (spark.read.option("header", "true").option("escape", '"')
    .csv(f"{src_dir}/products_catalog.csv"))
assert len(_lnk_products_raw.columns) == 6, f"products_catalog.csv: expected 6 columns, got {len(_lnk_products_raw.columns)}"
_lnk_products_raw = _lnk_products_raw.toDF('StockCode', 'Description', 'Category', 'ListPrice', 'SupplierID', 'Status')
_lnk_products_raw.createOrReplaceTempView("_lnk_products_raw")
lnk_products = spark.sql(f"""
SELECT
  CAST(`StockCode` AS string) AS StockCode,
  CAST(`Description` AS string) AS Description,
  CAST(`Category` AS string) AS Category,
  CAST(`ListPrice` AS decimal(18,2)) AS ListPrice,
  CAST(`SupplierID` AS string) AS SupplierID,
  CAST(`Status` AS string) AS Status
FROM _lnk_products_raw
""")
lnk_products.createOrReplaceTempView("lnk_products")

# COMMAND ----------
# src_suppliers: Sequential File source suppliers.csv (columns mapped by position, as DataStage does)
_lnk_suppliers_raw = (spark.read.option("header", "true").option("escape", '"')
    .csv(f"{src_dir}/suppliers.csv"))
assert len(_lnk_suppliers_raw.columns) == 5, f"suppliers.csv: expected 5 columns, got {len(_lnk_suppliers_raw.columns)}"
_lnk_suppliers_raw = _lnk_suppliers_raw.toDF('SupplierID', 'SupplierName', 'SupplierCountry', 'LeadTimeDays', 'ReliabilityScore')
_lnk_suppliers_raw.createOrReplaceTempView("_lnk_suppliers_raw")
lnk_suppliers = spark.sql(f"""
SELECT
  CAST(`SupplierID` AS string) AS SupplierID,
  CAST(`SupplierName` AS string) AS SupplierName,
  CAST(`SupplierCountry` AS string) AS SupplierCountry,
  CAST(`LeadTimeDays` AS int) AS LeadTimeDays,
  CAST(`ReliabilityScore` AS decimal(18,2)) AS ReliabilityScore
FROM _lnk_suppliers_raw
""")
lnk_suppliers.createOrReplaceTempView("lnk_suppliers")

# COMMAND ----------
# xfm_Std_Product: Transformer
lnk_std_prod = spark.sql(f"""
SELECT
  CAST(upper(trim(StockCode)) AS string) AS stock_code,
  CAST(CASE WHEN length(trim(Description)) = 0 THEN 'UNKNOWN' ELSE upper(trim(Description)) END AS string) AS product_name,
  CAST(trim(Category) AS string) AS category,
  ListPrice AS list_price,
  CAST(trim(SupplierID) AS string) AS supplier_id,
  CAST(trim(Status) AS string) AS status,
  CAST(ListPrice * 0.55 AS decimal(18,2)) AS unit_cost,
  CAST(CASE WHEN ListPrice < 1 THEN 'LOW' ELSE CASE WHEN ListPrice < 5 THEN 'MID' ELSE 'HIGH' END END AS string) AS price_band
FROM lnk_products
""")
lnk_std_prod.createOrReplaceTempView("lnk_std_prod")

# COMMAND ----------
# xfm_Std_Supplier: Transformer
lnk_std_sup = spark.sql(f"""
SELECT
  CAST(trim(SupplierID) AS string) AS supplier_id,
  CAST(trim(SupplierName) AS string) AS supplier_name,
  CAST(trim(SupplierCountry) AS string) AS supplier_country,
  LeadTimeDays AS lead_time_days,
  ReliabilityScore AS reliability_score
FROM lnk_suppliers
""")
lnk_std_sup.createOrReplaceTempView("lnk_std_sup")

# COMMAND ----------
# jn_Product_Supplier: Join (left outer) on supplier_id
lnk_prod_sup = spark.sql(f"""
SELECT
  l.stock_code,
  l.product_name,
  l.category,
  l.list_price,
  l.supplier_id,
  l.status,
  l.unit_cost,
  l.price_band,
  r.supplier_name,
  r.supplier_country,
  r.lead_time_days,
  r.reliability_score
FROM lnk_std_prod l
LEFT OUTER JOIN lnk_std_sup r ON l.supplier_id = r.supplier_id
""")
lnk_prod_sup.createOrReplaceTempView("lnk_prod_sup")

# COMMAND ----------
# xfm_Supplier_Defaults: Transformer
lnk_prod_sup_nn = spark.sql(f"""
SELECT
  stock_code,
  product_name,
  category,
  list_price,
  supplier_id,
  status,
  unit_cost,
  price_band,
  CAST(coalesce(supplier_name, 'UNKNOWN') AS string) AS supplier_name,
  CAST(coalesce(supplier_country, 'UNKNOWN') AS string) AS supplier_country,
  CAST(coalesce(lead_time_days, 0) AS int) AS lead_time_days,
  CAST(coalesce(reliability_score, 0) AS decimal(18,2)) AS reliability_score
FROM lnk_prod_sup
""")
lnk_prod_sup_nn.createOrReplaceTempView("lnk_prod_sup_nn")

# COMMAND ----------
# rmd_Products: Remove Duplicates (keep first, key stock_code; no Sort upstream -> monotonically_increasing_id order)
lnk_dedup = spark.sql(f"""
SELECT
  stock_code, product_name, category, list_price, supplier_id, status, unit_cost, price_band,
  supplier_name, supplier_country, lead_time_days, reliability_score
FROM (
  SELECT *, row_number() OVER (PARTITION BY stock_code ORDER BY monotonically_increasing_id()) AS _rn
  FROM lnk_prod_sup_nn
) t
WHERE _rn = 1
""")
lnk_dedup.createOrReplaceTempView("lnk_dedup")

# COMMAND ----------
# cpy_Products: Copy (output lnk_all)
lnk_all = spark.sql(f"""
SELECT * FROM lnk_dedup
""")
lnk_all.createOrReplaceTempView("lnk_all")

# COMMAND ----------
# cpy_Products: Copy (output lnk_for_active)
lnk_for_active = spark.sql(f"""
SELECT * FROM lnk_dedup
""")
lnk_for_active.createOrReplaceTempView("lnk_for_active")

# COMMAND ----------
# tgt_stg_products: write Delta table stg_products
(lnk_all.select('stock_code', 'product_name', 'category', 'list_price', 'supplier_id', 'status', 'unit_cost', 'price_band',
                'supplier_name', 'supplier_country', 'lead_time_days', 'reliability_score').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.stg_products"))
print("stg_products:", spark.table(f"{catalog}.{schema}.stg_products").count(), "rows")

# COMMAND ----------
# flt_Active_Products: Filter
lnk_active = spark.sql(f"""
SELECT * FROM lnk_for_active
WHERE status = 'ACTIVE'
""")
lnk_active.createOrReplaceTempView("lnk_active")

# COMMAND ----------
# tgt_stg_active_products: write Delta table stg_active_products
(lnk_active.select('stock_code', 'product_name', 'category', 'list_price', 'supplier_id', 'status', 'unit_cost', 'price_band',
                   'supplier_name', 'supplier_country', 'lead_time_days', 'reliability_score').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.stg_active_products"))
print("stg_active_products:", spark.table(f"{catalog}.{schema}.stg_active_products").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
