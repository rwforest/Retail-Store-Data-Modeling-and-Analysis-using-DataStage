# Databricks notebook source
# J03_Land_Products - Land product catalogue + suppliers, derive cost and price band
# Layer: 1_Land. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_products: source file products_catalog.csv (columns mapped by position, as DataStage does)
@dp.temporary_view(name="j03_lnk_products")
def j03_lnk_products():
    raw = spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/products_catalog.csv")
    assert len(raw.columns) == 6, f"products_catalog.csv: expected 6 columns, got {len(raw.columns)}"
    return raw.toDF('StockCode', 'Description', 'Category', 'ListPrice', 'SupplierID', 'Status').selectExpr(
        "coalesce(`StockCode`, '') AS StockCode",
        "coalesce(`Description`, '') AS Description",
        "coalesce(`Category`, '') AS Category",
        'CAST(`ListPrice` AS decimal(18,2)) AS ListPrice',
        "coalesce(`SupplierID`, '') AS SupplierID",
        "coalesce(`Status`, '') AS Status")

# COMMAND ----------
# src_suppliers: source file suppliers.csv (columns mapped by position, as DataStage does)
@dp.temporary_view(name="j03_lnk_suppliers")
def j03_lnk_suppliers():
    raw = spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/suppliers.csv")
    assert len(raw.columns) == 5, f"suppliers.csv: expected 5 columns, got {len(raw.columns)}"
    return raw.toDF('SupplierID', 'SupplierName', 'SupplierCountry', 'LeadTimeDays', 'ReliabilityScore').selectExpr(
        "coalesce(`SupplierID`, '') AS SupplierID",
        "coalesce(`SupplierName`, '') AS SupplierName",
        "coalesce(`SupplierCountry`, '') AS SupplierCountry",
        'CAST(`LeadTimeDays` AS int) AS LeadTimeDays',
        'CAST(`ReliabilityScore` AS decimal(18,2)) AS ReliabilityScore')

# COMMAND ----------
# xfm_Std_Product: Transformer
@dp.temporary_view(name="j03_lnk_std_prod")
def j03_lnk_std_prod():
    return spark.sql(f"""
SELECT
  CAST(upper(trim(StockCode)) AS string) AS stock_code,
  CAST(CASE WHEN (length(trim(Description)) = 0) THEN 'UNKNOWN' ELSE upper(trim(Description)) END AS string) AS product_name,
  CAST(trim(Category) AS string) AS category,
  ListPrice AS list_price,
  CAST(trim(SupplierID) AS string) AS supplier_id,
  CAST(trim(Status) AS string) AS status,
  CAST((ListPrice * 0.55) AS decimal(18,2)) AS unit_cost,
  CAST(CASE WHEN (ListPrice < 1) THEN 'LOW' ELSE CASE WHEN (ListPrice < 5) THEN 'MID' ELSE 'HIGH' END END AS string) AS price_band
FROM j03_lnk_products
""")

# COMMAND ----------
# xfm_Std_Supplier: Transformer
@dp.temporary_view(name="j03_lnk_std_sup")
def j03_lnk_std_sup():
    return spark.sql(f"""
SELECT
  CAST(trim(SupplierID) AS string) AS supplier_id,
  CAST(trim(SupplierName) AS string) AS supplier_name,
  CAST(trim(SupplierCountry) AS string) AS supplier_country,
  LeadTimeDays AS lead_time_days,
  ReliabilityScore AS reliability_score
FROM j03_lnk_suppliers
""")

# COMMAND ----------
# jn_Product_Supplier: Join (left outer) on supplier_id
@dp.temporary_view(name="j03_lnk_prod_sup")
def j03_lnk_prod_sup():
    return spark.sql(f"""
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
FROM j03_lnk_std_prod l
LEFT OUTER JOIN j03_lnk_std_sup r ON l.supplier_id = r.supplier_id
""")

# COMMAND ----------
# xfm_Supplier_Defaults: Transformer
@dp.temporary_view(name="j03_lnk_prod_sup_nn")
def j03_lnk_prod_sup_nn():
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
  CAST(coalesce(supplier_name, 'UNKNOWN') AS string) AS supplier_name,
  CAST(coalesce(supplier_country, 'UNKNOWN') AS string) AS supplier_country,
  CAST(coalesce(lead_time_days, 0) AS int) AS lead_time_days,
  CAST(coalesce(reliability_score, 0) AS decimal(18,2)) AS reliability_score
FROM j03_lnk_prod_sup
""")

# COMMAND ----------
# rmd_Products: Remove Duplicates
@dp.temporary_view(name="j03_lnk_dedup")
def j03_lnk_dedup():
    return spark.sql(f"""
SELECT stock_code, product_name, category, list_price, supplier_id, status, unit_cost, price_band, supplier_name, supplier_country, lead_time_days, reliability_score FROM (
  SELECT *, row_number() OVER (PARTITION BY stock_code ORDER BY monotonically_increasing_id()) AS _rn FROM j03_lnk_prod_sup_nn
) WHERE _rn = 1
""")

# COMMAND ----------
# cpy_Products: Copy
@dp.temporary_view(name="j03_lnk_all")
def j03_lnk_all():
    return spark.sql(f"""
SELECT * FROM j03_lnk_dedup
""")

# COMMAND ----------
# cpy_Products: Copy
@dp.temporary_view(name="j03_lnk_for_active")
def j03_lnk_for_active():
    return spark.sql(f"""
SELECT * FROM j03_lnk_dedup
""")

# COMMAND ----------
# tgt_stg_products: target file -> materialized view stg_products
@dp.materialized_view(name="stg_products")
def stg_products():
    return spark.read.table("j03_lnk_all").select('stock_code', 'product_name', 'category', 'list_price', 'supplier_id', 'status', 'unit_cost', 'price_band', 'supplier_name', 'supplier_country', 'lead_time_days', 'reliability_score')

# COMMAND ----------
# flt_Active_Products: Filter
@dp.temporary_view(name="j03_lnk_active")
def j03_lnk_active():
    return spark.sql(f"""
SELECT * FROM j03_lnk_for_active
WHERE (status = 'ACTIVE')
""")

# COMMAND ----------
# tgt_stg_active_products: target file -> materialized view stg_active_products
@dp.materialized_view(name="stg_active_products")
def stg_active_products():
    return spark.read.table("j03_lnk_active").select('stock_code', 'product_name', 'category', 'list_price', 'supplier_id', 'status', 'unit_cost', 'price_band', 'supplier_name', 'supplier_country', 'lead_time_days', 'reliability_score')
