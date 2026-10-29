# Databricks notebook source
# MAGIC %md
# MAGIC # J01_Land_Orders
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028235854-szp7/J01_Land_Orders.txt`
# COMMAND ----------
# COMMAND ----------
dbutils.widgets.text("catalog", ""); dbutils.widgets.text("schema", "retail_dw"); dbutils.widgets.text("src_dir", "")
dbutils.widgets.text("pRunDate", "2011-12-31"); dbutils.widgets.text("pEffDate", "2011-07-01")
catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))
pRunDate, pEffDate = dbutils.widgets.get("pRunDate"), dbutils.widgets.get("pEffDate")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------
# Output columns shared by the typed order links (same 15 columns, same order)
order_cols = ['invoice_no', 'stock_code', 'description', 'quantity', 'invoice_ts', 'unit_price',
              'customer_id', 'country', 'store_id', 'employee_id', 'invoice_date_key',
              'line_amount', 'is_cancel', 'is_nonproduct', 'is_guest']

# COMMAND ----------
# src_orders_raw: Sequential File source orders_raw.csv (columns mapped by position, as DataStage does)
_lnk_raw_raw = (spark.read.option("header", "true").option("escape", '"')
    .csv(f"{src_dir}/orders_raw.csv"))
assert len(_lnk_raw_raw.columns) == 10, f"orders_raw.csv: expected 10 columns, got {len(_lnk_raw_raw.columns)}"
_lnk_raw_raw = _lnk_raw_raw.toDF('InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate',
                                 'UnitPrice', 'CustomerID', 'Country', 'StoreID', 'EmployeeID')
_lnk_raw_raw.createOrReplaceTempView("_lnk_raw_raw")
lnk_raw = spark.sql(f"""
SELECT
  CAST(`InvoiceNo` AS string) AS InvoiceNo,
  CAST(`StockCode` AS string) AS StockCode,
  CAST(`Description` AS string) AS Description,
  CAST(`Quantity` AS string) AS Quantity,
  CAST(`InvoiceDate` AS string) AS InvoiceDate,
  CAST(`UnitPrice` AS string) AS UnitPrice,
  CAST(`CustomerID` AS string) AS CustomerID,
  CAST(`Country` AS string) AS Country,
  CAST(`StoreID` AS string) AS StoreID,
  CAST(`EmployeeID` AS string) AS EmployeeID
FROM _lnk_raw_raw
""")
lnk_raw.createOrReplaceTempView("lnk_raw")

# COMMAND ----------
# xfm_Type_Orders: Transformer
lnk_typed = spark.sql(f"""
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
  CAST((CAST(trim(Quantity) AS decimal(18,4)) * CAST(trim(UnitPrice) AS decimal(18,4))) AS decimal(18,2)) AS line_amount,
  CAST(CASE WHEN left(trim(InvoiceNo), 1) = 'C' THEN 1 ELSE 0 END AS int) AS is_cancel,
  CAST(CASE WHEN upper(trim(StockCode)) = 'POST' OR upper(trim(StockCode)) = 'D' OR upper(trim(StockCode)) = 'M' OR upper(trim(StockCode)) = 'DOT' OR upper(trim(StockCode)) = 'CRUK' OR upper(trim(StockCode)) = 'BANK CHARGES' OR upper(trim(StockCode)) = 'AMAZONFEE' OR upper(trim(StockCode)) = 'S' OR upper(trim(StockCode)) = 'B' OR upper(trim(StockCode)) = 'C2' OR upper(trim(StockCode)) = 'PADS' OR upper(trim(StockCode)) = 'M' THEN 1 ELSE 0 END AS int) AS is_nonproduct,
  CAST(CASE WHEN length(trim(CustomerID)) = 0 THEN 1 ELSE 0 END AS int) AS is_guest
FROM lnk_raw
""")
lnk_typed.createOrReplaceTempView("lnk_typed")

# COMMAND ----------
# cpy_Typed: Copy (output link lnk_typed_ok)
lnk_typed_ok = spark.sql(f"""
SELECT invoice_no, stock_code, description, quantity, invoice_ts, unit_price, customer_id, country,
       store_id, employee_id, invoice_date_key, line_amount, is_cancel, is_nonproduct, is_guest
FROM lnk_typed
""")
lnk_typed_ok.createOrReplaceTempView("lnk_typed_ok")

# COMMAND ----------
# cpy_Typed: Copy (output link lnk_typed_bad)
lnk_typed_bad = spark.sql(f"""
SELECT invoice_no, stock_code, description, quantity, invoice_ts, unit_price, customer_id, country,
       store_id, employee_id, invoice_date_key, line_amount, is_cancel, is_nonproduct, is_guest
FROM lnk_typed
""")
lnk_typed_bad.createOrReplaceTempView("lnk_typed_bad")

# COMMAND ----------
# flt_Valid_Rows: Filter
lnk_valid = spark.sql(f"""
SELECT * FROM lnk_typed_ok
WHERE quantity <> 0 AND unit_price >= 0
""")
lnk_valid.createOrReplaceTempView("lnk_valid")

# COMMAND ----------
# rmd_Dedup_Lines: Remove Duplicates (keep first; no Sort upstream, so monotonically_increasing_id() defines "first")
lnk_dedup = spark.sql(f"""
SELECT invoice_no, stock_code, description, quantity, invoice_ts, unit_price, customer_id, country,
       store_id, employee_id, invoice_date_key, line_amount, is_cancel, is_nonproduct, is_guest
FROM (
  SELECT *,
         row_number() OVER (
           PARTITION BY invoice_no, stock_code, invoice_ts, customer_id, quantity, unit_price
           ORDER BY monotonically_increasing_id()
         ) AS _rn
  FROM lnk_valid
) t
WHERE _rn = 1
""")
lnk_dedup.createOrReplaceTempView("lnk_dedup")

# COMMAND ----------
# cpy_Orders: Copy (output link lnk_stg)
lnk_stg = spark.sql(f"""
SELECT invoice_no, stock_code, description, quantity, invoice_ts, unit_price, customer_id, country,
       store_id, employee_id, invoice_date_key, line_amount, is_cancel, is_nonproduct, is_guest
FROM lnk_dedup
""")
lnk_stg.createOrReplaceTempView("lnk_stg")

# COMMAND ----------
# cpy_Orders: Copy (output link lnk_anom_in)
lnk_anom_in = spark.sql(f"""
SELECT invoice_no, stock_code, description, quantity, invoice_ts, unit_price, customer_id, country,
       store_id, employee_id, invoice_date_key, line_amount, is_cancel, is_nonproduct, is_guest
FROM lnk_dedup
""")
lnk_anom_in.createOrReplaceTempView("lnk_anom_in")

# COMMAND ----------
# cpy_Orders: Copy (output link lnk_cnt_in)
lnk_cnt_in = spark.sql(f"""
SELECT invoice_no, stock_code, description, quantity, invoice_ts, unit_price, customer_id, country,
       store_id, employee_id, invoice_date_key, line_amount, is_cancel, is_nonproduct, is_guest
FROM lnk_dedup
""")
lnk_cnt_in.createOrReplaceTempView("lnk_cnt_in")

# COMMAND ----------
# tgt_stg_orders: Sequential File target -> Delta table stg_orders
(lnk_stg.select(*order_cols).write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.stg_orders"))
print("stg_orders:", spark.table(f"{catalog}.{schema}.stg_orders").count(), "rows")

# COMMAND ----------
# flt_Invalid_Rows: Filter
lnk_invalid = spark.sql(f"""
SELECT * FROM lnk_typed_bad
WHERE quantity = 0 OR unit_price < 0
""")
lnk_invalid.createOrReplaceTempView("lnk_invalid")

# COMMAND ----------
# tgt_rej_orders: Sequential File target -> Delta table rej_orders
(lnk_invalid.select(*order_cols).write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.rej_orders"))
print("rej_orders:", spark.table(f"{catalog}.{schema}.rej_orders").count(), "rows")

# COMMAND ----------
# flt_Anomalies: Filter
lnk_anom = spark.sql(f"""
SELECT * FROM lnk_anom_in
WHERE unit_price = 0 OR quantity > 5000 OR quantity < -5000
""")
lnk_anom.createOrReplaceTempView("lnk_anom")

# COMMAND ----------
# tgt_dq_order_anomalies: Sequential File target -> Delta table dq_order_anomalies
(lnk_anom.select(*order_cols).write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dq_order_anomalies"))
print("dq_order_anomalies:", spark.table(f"{catalog}.{schema}.dq_order_anomalies").count(), "rows")

# COMMAND ----------
# agg_Rows_By_Country: Aggregator
lnk_rows_by_country = spark.sql(f"""
SELECT country, CAST(COUNT(*) AS int) AS rows_loaded
FROM lnk_cnt_in
GROUP BY country
""")
lnk_rows_by_country.createOrReplaceTempView("lnk_rows_by_country")

# COMMAND ----------
# tgt_audit_land_orders: Sequential File target -> Delta table audit_land_orders
(lnk_rows_by_country.select('country', 'rows_loaded').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.audit_land_orders"))
print("audit_land_orders:", spark.table(f"{catalog}.{schema}.audit_land_orders").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
