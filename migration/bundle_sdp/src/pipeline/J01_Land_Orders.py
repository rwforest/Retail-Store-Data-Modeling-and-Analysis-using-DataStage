# Databricks notebook source
# MAGIC %md
# MAGIC # J01_Land_Orders
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261030063735-t49e/J01_Land_Orders.txt`
# COMMAND ----------
# COMMAND ----------
from pyspark import pipelines as dp

# Provide a default so the pipeline does not fail when the "src_dir" config is not set
src_dir = spark.conf.get("src_dir", "/Volumes/main/default/source_data")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# src_orders_raw: source file orders_raw.csv (columns mapped by position, as DataStage does)
@dp.temporary_view(name="j01_lnk_raw")
def j01_lnk_raw():
    raw = spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/orders_raw.csv")
    assert len(raw.columns) == 10, f"orders_raw.csv: expected 10 columns, got {len(raw.columns)}"
    return raw.toDF('InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country', 'StoreID', 'EmployeeID').selectExpr(
        "coalesce(`InvoiceNo`, '') AS InvoiceNo",
        "coalesce(`StockCode`, '') AS StockCode",
        "coalesce(`Description`, '') AS Description",
        "coalesce(`Quantity`, '') AS Quantity",
        "coalesce(`InvoiceDate`, '') AS InvoiceDate",
        "coalesce(`UnitPrice`, '') AS UnitPrice",
        "coalesce(`CustomerID`, '') AS CustomerID",
        "coalesce(`Country`, '') AS Country",
        "coalesce(`StoreID`, '') AS StoreID",
        "coalesce(`EmployeeID`, '') AS EmployeeID")

# xfm_Type_Orders: Transformer
@dp.temporary_view(name="j01_lnk_typed")
def j01_lnk_typed():
    return spark.sql(f"""
SELECT
  CAST(trim(InvoiceNo) AS string) AS invoice_no,
  CAST(upper(trim(StockCode)) AS string) AS stock_code,
  CAST(trim(Description) AS string) AS description,
  CAST(trim(Quantity) AS int) AS quantity,
  CAST(to_timestamp(InvoiceDate, 'yyyy-MM-dd HH:mm:ss') AS timestamp) AS invoice_ts,
  CAST(CAST(trim(UnitPrice) AS decimal(18,4)) AS decimal(18,2)) AS unit_price,
  CAST(CASE WHEN length(trim(CustomerID)) = 0 THEN '0' ELSE trim(CustomerID) END AS int) AS customer_id,
  CAST(trim(Country) AS string) AS country,
  CAST(trim(StoreID) AS string) AS store_id,
  CAST(trim(EmployeeID) AS string) AS employee_id,
  CAST(date_format(to_timestamp(InvoiceDate, 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd') AS int) AS invoice_date_key,
  CAST(CAST(trim(Quantity) AS decimal(18,4)) * CAST(trim(UnitPrice) AS decimal(18,4)) AS decimal(18,2)) AS line_amount,
  CAST(CASE WHEN left(trim(InvoiceNo), 1) = 'C' THEN 1 ELSE 0 END AS int) AS is_cancel,
  CAST(CASE WHEN upper(trim(StockCode)) = 'POST' OR upper(trim(StockCode)) = 'D' OR upper(trim(StockCode)) = 'M' OR upper(trim(StockCode)) = 'DOT' OR upper(trim(StockCode)) = 'CRUK' OR upper(trim(StockCode)) = 'BANK CHARGES' OR upper(trim(StockCode)) = 'AMAZONFEE' OR upper(trim(StockCode)) = 'S' OR upper(trim(StockCode)) = 'B' OR upper(trim(StockCode)) = 'C2' OR upper(trim(StockCode)) = 'PADS' OR upper(trim(StockCode)) = 'M' THEN 1 ELSE 0 END AS int) AS is_nonproduct,
  CAST(CASE WHEN length(trim(CustomerID)) = 0 THEN 1 ELSE 0 END AS int) AS is_guest
FROM j01_lnk_raw
""")

# cpy_Typed: Copy
@dp.temporary_view(name="j01_lnk_typed_ok")
def j01_lnk_typed_ok():
    return spark.sql(f"""
SELECT * FROM j01_lnk_typed
""")

# cpy_Typed: Copy
@dp.temporary_view(name="j01_lnk_typed_bad")
def j01_lnk_typed_bad():
    return spark.sql(f"""
SELECT * FROM j01_lnk_typed
""")

# flt_Valid_Rows: Filter
@dp.temporary_view(name="j01_lnk_valid")
def j01_lnk_valid():
    return spark.sql(f"""
SELECT * FROM j01_lnk_typed_ok
WHERE quantity <> 0 AND unit_price >= 0
""")

# rmd_Dedup_Lines: Remove Duplicates (keep first; no Sort upstream, so arbitrary but deterministic-by-id order)
@dp.temporary_view(name="j01_lnk_dedup")
def j01_lnk_dedup():
    return spark.sql(f"""
SELECT invoice_no, stock_code, description, quantity, invoice_ts, unit_price, customer_id, country, store_id, employee_id,
       invoice_date_key, line_amount, is_cancel, is_nonproduct, is_guest
FROM (
  SELECT *, row_number() OVER (
    PARTITION BY invoice_no, stock_code, invoice_ts, customer_id, quantity, unit_price
    ORDER BY monotonically_increasing_id()) AS rn
  FROM j01_lnk_valid
) t
WHERE rn = 1
""")

# cpy_Orders: Copy
@dp.temporary_view(name="j01_lnk_stg")
def j01_lnk_stg():
    return spark.sql(f"""
SELECT * FROM j01_lnk_dedup
""")

# cpy_Orders: Copy
@dp.temporary_view(name="j01_lnk_anom_in")
def j01_lnk_anom_in():
    return spark.sql(f"""
SELECT * FROM j01_lnk_dedup
""")

# cpy_Orders: Copy
@dp.temporary_view(name="j01_lnk_cnt_in")
def j01_lnk_cnt_in():
    return spark.sql(f"""
SELECT * FROM j01_lnk_dedup
""")

# tgt_stg_orders: target file -> materialized view stg_orders
@dp.materialized_view(name="stg_orders")
def stg_orders():
    return spark.read.table("j01_lnk_stg").select('invoice_no', 'stock_code', 'description', 'quantity', 'invoice_ts', 'unit_price', 'customer_id', 'country', 'store_id', 'employee_id', 'invoice_date_key', 'line_amount', 'is_cancel', 'is_nonproduct', 'is_guest')

# flt_Invalid_Rows: Filter
@dp.temporary_view(name="j01_lnk_invalid")
def j01_lnk_invalid():
    return spark.sql(f"""
SELECT * FROM j01_lnk_typed_bad
WHERE quantity = 0 OR unit_price < 0
""")

# tgt_rej_orders: target file -> materialized view rej_orders
@dp.materialized_view(name="rej_orders")
def rej_orders():
    return spark.read.table("j01_lnk_invalid").select('invoice_no', 'stock_code', 'description', 'quantity', 'invoice_ts', 'unit_price', 'customer_id', 'country', 'store_id', 'employee_id', 'invoice_date_key', 'line_amount', 'is_cancel', 'is_nonproduct', 'is_guest')

# flt_Anomalies: Filter
@dp.temporary_view(name="j01_lnk_anom")
def j01_lnk_anom():
    return spark.sql(f"""
SELECT * FROM j01_lnk_anom_in
WHERE unit_price = 0 OR quantity > 5000 OR quantity < -5000
""")

# tgt_dq_order_anomalies: target file -> materialized view dq_order_anomalies
@dp.materialized_view(name="dq_order_anomalies")
def dq_order_anomalies():
    return spark.read.table("j01_lnk_anom").select('invoice_no', 'stock_code', 'description', 'quantity', 'invoice_ts', 'unit_price', 'customer_id', 'country', 'store_id', 'employee_id', 'invoice_date_key', 'line_amount', 'is_cancel', 'is_nonproduct', 'is_guest')

# agg_Rows_By_Country: Aggregator
@dp.temporary_view(name="j01_lnk_rows_by_country")
def j01_lnk_rows_by_country():
    return spark.sql(f"""
SELECT country, CAST(COUNT(*) AS int) AS rows_loaded
FROM j01_lnk_cnt_in
GROUP BY country
""")

# tgt_audit_land_orders: target file -> materialized view audit_land_orders
@dp.materialized_view(name="audit_land_orders")
def audit_land_orders():
    return spark.read.table("j01_lnk_rows_by_country").select('country', 'rows_loaded')

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
