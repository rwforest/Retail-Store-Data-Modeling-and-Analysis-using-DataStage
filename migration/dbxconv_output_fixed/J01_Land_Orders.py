# Databricks notebook source
# Code converted on 2026-11-19 16:17:47
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.functions import lit, to_timestamp, when, expr, col, explode
from databricks_conversion_supplements import DatabricksConversionSupplements


# COMMAND ----------

dbutils.widgets.text(name = 'pSrcDir', defaultValue = 'C:/case_study/Source_Data')
pSrcDir = dbutils.widgets.get("pSrcDir")

dbutils.widgets.text(name = 'pStgDir', defaultValue = 'C:/case_study/Staging')
pStgDir = dbutils.widgets.get("pStgDir")

dbutils.widgets.text(name = 'pDwhDir', defaultValue = 'C:/case_study/Warehouse')
pDwhDir = dbutils.widgets.get("pDwhDir")

dbutils.widgets.text(name = 'pMartDir', defaultValue = 'C:/case_study/Marts')
pMartDir = dbutils.widgets.get("pMartDir")

dbutils.widgets.text(name = 'pAuditDir', defaultValue = 'C:/case_study/Audit')
pAuditDir = dbutils.widgets.get("pAuditDir")

dbutils.widgets.text(name = 'pRunDate', defaultValue = '2011-12-31')
pRunDate = dbutils.widgets.get("pRunDate")

dbutils.widgets.text(name = 'pEffDate', defaultValue = '2011-07-01')
pEffDate = dbutils.widgets.get("pEffDate")

dbutils.widgets.text(name = 'catalog', defaultValue = 'unitygo')
catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text(name = 'schema', defaultValue = 'retail_dw_dbxconv')
schema = dbutils.widgets.get("schema")


# COMMAND ----------
# Component lnk_raw, Type SOURCE Original node name src_orders_raw, link lnk_raw
lnk_raw = spark.read.format('csv').option('header', 'true').option('escape', '"').load(f'''{pSrcDir}/orders_raw.csv''')
# Conforming fields names to the component layout
lnk_raw_conformed_cols = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country", "StoreID", "EmployeeID"]
lnk_raw = DatabricksConversionSupplements.conform_df_columns(lnk_raw,lnk_raw_conformed_cols)
lnk_raw.createOrReplaceTempView("lnk_raw")


# COMMAND ----------
# Component lnk_typed, Type TRANSFORMATION Original node name xfm_Type_Orders, link lnk_typed
lnk_typed = spark.sql(f"""
SELECT
trim(coalesce(lnk_raw.InvoiceNo, '')) as invoice_no,
UPPER(trim(coalesce(lnk_raw.StockCode, ''))) as stock_code,
trim(coalesce(lnk_raw.Description, '')) as description,
trim(coalesce(lnk_raw.Quantity, '')) as quantity,
to_timestamp(coalesce(lnk_raw.InvoiceDate, '')) as invoice_ts,
CAST(trim(coalesce(lnk_raw.UnitPrice, '')) as decimal(18,2)) as unit_price,
IF ( Len ( trim(coalesce(lnk_raw.CustomerID, '')) ) = 0 , '0' , trim(coalesce(lnk_raw.CustomerID, '')) ) as customer_id,
trim(coalesce(lnk_raw.Country, '')) as country,
trim(coalesce(lnk_raw.StoreID, '')) as store_id,
trim(coalesce(lnk_raw.EmployeeID, '')) as employee_id,
date_format(to_timestamp(coalesce(lnk_raw.InvoiceDate, '')), 'yyyyMMdd') as invoice_date_key,
CAST(trim(coalesce(lnk_raw.Quantity, '')) as decimal(18,2)) * CAST(trim(coalesce(lnk_raw.UnitPrice, '')) as decimal(18,2)) as line_amount,
IF ( Left ( trim(coalesce(lnk_raw.InvoiceNo, '')) , 1 ) = 'C' , 1 , 0 ) as is_cancel,
IF ( UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'POST' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'D' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'M' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'DOT' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'CRUK' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'BANK CHARGES' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'AMAZONFEE' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'S' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'B' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'C2' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'PADS' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'M' , 1 , 0 ) as is_nonproduct,
IF ( Len ( trim(coalesce(lnk_raw.CustomerID, '')) ) = 0 , 1 , 0 ) as is_guest
FROM
lnk_raw""")
lnk_typed.createOrReplaceTempView("lnk_typed")


# COMMAND ----------
# Component lnk_typed_bad, Type REPLICATE Original node name cpy_Typed, link lnk_typed_bad
lnk_typed_bad = spark.sql(f"""
SELECT
coalesce(lnk_typed.invoice_no, '') as invoice_no,
coalesce(lnk_typed.stock_code, '') as stock_code,
coalesce(lnk_typed.description, '') as description,
CAST(lnk_typed.quantity AS int) as quantity,
lnk_typed.invoice_ts as invoice_ts,
CAST(lnk_typed.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_typed.customer_id AS int) as customer_id,
coalesce(lnk_typed.country, '') as country,
coalesce(lnk_typed.store_id, '') as store_id,
coalesce(lnk_typed.employee_id, '') as employee_id,
CAST(lnk_typed.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_typed.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_typed.is_cancel AS int) as is_cancel,
CAST(lnk_typed.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_typed.is_guest AS int) as is_guest
FROM
lnk_typed""")
lnk_typed_bad.createOrReplaceTempView("lnk_typed_bad")


# COMMAND ----------
# Component lnk_typed_ok, Type REPLICATE Original node name cpy_Typed, link lnk_typed_ok
lnk_typed_ok = spark.sql(f"""
SELECT
coalesce(lnk_typed.invoice_no, '') as invoice_no,
coalesce(lnk_typed.stock_code, '') as stock_code,
coalesce(lnk_typed.description, '') as description,
CAST(lnk_typed.quantity AS int) as quantity,
lnk_typed.invoice_ts as invoice_ts,
CAST(lnk_typed.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_typed.customer_id AS int) as customer_id,
coalesce(lnk_typed.country, '') as country,
coalesce(lnk_typed.store_id, '') as store_id,
coalesce(lnk_typed.employee_id, '') as employee_id,
CAST(lnk_typed.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_typed.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_typed.is_cancel AS int) as is_cancel,
CAST(lnk_typed.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_typed.is_guest AS int) as is_guest
FROM
lnk_typed""")
lnk_typed_ok.createOrReplaceTempView("lnk_typed_ok")


# COMMAND ----------
# Component lnk_invalid, Type TRANSFORMATION Original node name flt_Invalid_Rows, link lnk_invalid
lnk_invalid = spark.sql(f"""
SELECT
coalesce(lnk_typed_bad.invoice_no, '') as invoice_no,
coalesce(lnk_typed_bad.stock_code, '') as stock_code,
coalesce(lnk_typed_bad.description, '') as description,
CAST(lnk_typed_bad.quantity AS int) as quantity,
lnk_typed_bad.invoice_ts as invoice_ts,
CAST(lnk_typed_bad.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_typed_bad.customer_id AS int) as customer_id,
coalesce(lnk_typed_bad.country, '') as country,
coalesce(lnk_typed_bad.store_id, '') as store_id,
coalesce(lnk_typed_bad.employee_id, '') as employee_id,
CAST(lnk_typed_bad.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_typed_bad.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_typed_bad.is_cancel AS int) as is_cancel,
CAST(lnk_typed_bad.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_typed_bad.is_guest AS int) as is_guest
FROM
lnk_typed_bad
WHERE quantity = 0 or unit_price < 0""")
lnk_invalid.createOrReplaceTempView("lnk_invalid")


# COMMAND ----------
# Component lnk_valid, Type TRANSFORMATION Original node name flt_Valid_Rows, link lnk_valid
lnk_valid = spark.sql(f"""
SELECT
coalesce(lnk_typed_ok.invoice_no, '') as invoice_no,
coalesce(lnk_typed_ok.stock_code, '') as stock_code,
coalesce(lnk_typed_ok.description, '') as description,
CAST(lnk_typed_ok.quantity AS int) as quantity,
lnk_typed_ok.invoice_ts as invoice_ts,
CAST(lnk_typed_ok.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_typed_ok.customer_id AS int) as customer_id,
coalesce(lnk_typed_ok.country, '') as country,
coalesce(lnk_typed_ok.store_id, '') as store_id,
coalesce(lnk_typed_ok.employee_id, '') as employee_id,
CAST(lnk_typed_ok.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_typed_ok.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_typed_ok.is_cancel AS int) as is_cancel,
CAST(lnk_typed_ok.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_typed_ok.is_guest AS int) as is_guest
FROM
lnk_typed_ok
WHERE quantity <> 0 and unit_price >= 0""")
lnk_valid.createOrReplaceTempView("lnk_valid")


# COMMAND ----------
# Component lnk_dedup, Type UNIQUE_ROW Original node name rmd_Dedup_Lines, link lnk_dedup
lnk_dedup = spark.sql(f"""
SELECT
invoice_no,
stock_code,
description,
quantity,
invoice_ts,
unit_price,
customer_id,
country,
store_id,
employee_id,
invoice_date_key,
line_amount,
is_cancel,
is_nonproduct,
is_guest
FROM (
SELECT
coalesce(lnk_valid.invoice_no, '') as invoice_no,
coalesce(lnk_valid.stock_code, '') as stock_code,
coalesce(lnk_valid.description, '') as description,
CAST(lnk_valid.quantity AS int) as quantity,
lnk_valid.invoice_ts as invoice_ts,
CAST(lnk_valid.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_valid.customer_id AS int) as customer_id,
coalesce(lnk_valid.country, '') as country,
coalesce(lnk_valid.store_id, '') as store_id,
coalesce(lnk_valid.employee_id, '') as employee_id,
CAST(lnk_valid.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_valid.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_valid.is_cancel AS int) as is_cancel,
CAST(lnk_valid.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_valid.is_guest AS int) as is_guest,
ROW_NUMBER() OVER(PARTITION BY coalesce(lnk_valid.invoice_no, ''), coalesce(lnk_valid.stock_code, ''), CAST(lnk_valid.quantity AS int), lnk_valid.invoice_ts, CAST(lnk_valid.unit_price AS decimal(18,2)), CAST(lnk_valid.customer_id AS int) ORDER BY coalesce(lnk_valid.description, ''), coalesce(lnk_valid.country, ''), coalesce(lnk_valid.store_id, ''), coalesce(lnk_valid.employee_id, ''), CAST(lnk_valid.invoice_date_key AS int), CAST(lnk_valid.line_amount AS decimal(18,2)), CAST(lnk_valid.is_cancel AS int), CAST(lnk_valid.is_nonproduct AS int), CAST(lnk_valid.is_guest AS int)) dedup_rnk
FROM lnk_valid) dedup_lnk_dedup WHERE dedup_rnk = 1""")
lnk_dedup.createOrReplaceTempView("lnk_dedup")


# COMMAND ----------
# Component tgt_rej_orders, Type TARGET 

tgt_rej_orders = lnk_invalid.select( \
	coalesce(lnk_invalid.invoice_no, lit('')).alias('invoice_no'), \
	coalesce(lnk_invalid.stock_code, lit('')).alias('stock_code'), \
	coalesce(lnk_invalid.description, lit('')).alias('description'), \
	lnk_invalid.quantity.cast('int').alias('quantity'), \
	lnk_invalid.invoice_ts.alias('invoice_ts'), \
	lnk_invalid.unit_price.cast('decimal(18,2)').alias('unit_price'), \
	lnk_invalid.customer_id.cast('int').alias('customer_id'), \
	coalesce(lnk_invalid.country, lit('')).alias('country'), \
	coalesce(lnk_invalid.store_id, lit('')).alias('store_id'), \
	coalesce(lnk_invalid.employee_id, lit('')).alias('employee_id'), \
	lnk_invalid.invoice_date_key.cast('int').alias('invoice_date_key'), \
	lnk_invalid.line_amount.cast('decimal(18,2)').alias('line_amount'), \
	lnk_invalid.is_cancel.cast('int').alias('is_cancel'), \
	lnk_invalid.is_nonproduct.cast('int').alias('is_nonproduct'), \
	lnk_invalid.is_guest.cast('int').alias('is_guest') \
)
tgt_rej_orders.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.rej_orders')


# COMMAND ----------
# Component lnk_cnt_in, Type REPLICATE Original node name cpy_Orders, link lnk_cnt_in
lnk_cnt_in = spark.sql(f"""
SELECT
coalesce(lnk_dedup.invoice_no, '') as invoice_no,
coalesce(lnk_dedup.stock_code, '') as stock_code,
coalesce(lnk_dedup.description, '') as description,
CAST(lnk_dedup.quantity AS int) as quantity,
lnk_dedup.invoice_ts as invoice_ts,
CAST(lnk_dedup.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_dedup.customer_id AS int) as customer_id,
coalesce(lnk_dedup.country, '') as country,
coalesce(lnk_dedup.store_id, '') as store_id,
coalesce(lnk_dedup.employee_id, '') as employee_id,
CAST(lnk_dedup.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_dedup.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_dedup.is_cancel AS int) as is_cancel,
CAST(lnk_dedup.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_dedup.is_guest AS int) as is_guest
FROM
lnk_dedup""")
lnk_cnt_in.createOrReplaceTempView("lnk_cnt_in")


# COMMAND ----------
# Component lnk_anom_in, Type REPLICATE Original node name cpy_Orders, link lnk_anom_in
lnk_anom_in = spark.sql(f"""
SELECT
coalesce(lnk_dedup.invoice_no, '') as invoice_no,
coalesce(lnk_dedup.stock_code, '') as stock_code,
coalesce(lnk_dedup.description, '') as description,
CAST(lnk_dedup.quantity AS int) as quantity,
lnk_dedup.invoice_ts as invoice_ts,
CAST(lnk_dedup.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_dedup.customer_id AS int) as customer_id,
coalesce(lnk_dedup.country, '') as country,
coalesce(lnk_dedup.store_id, '') as store_id,
coalesce(lnk_dedup.employee_id, '') as employee_id,
CAST(lnk_dedup.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_dedup.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_dedup.is_cancel AS int) as is_cancel,
CAST(lnk_dedup.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_dedup.is_guest AS int) as is_guest
FROM
lnk_dedup""")
lnk_anom_in.createOrReplaceTempView("lnk_anom_in")


# COMMAND ----------
# Component lnk_stg, Type REPLICATE Original node name cpy_Orders, link lnk_stg
lnk_stg = spark.sql(f"""
SELECT
coalesce(lnk_dedup.invoice_no, '') as invoice_no,
coalesce(lnk_dedup.stock_code, '') as stock_code,
coalesce(lnk_dedup.description, '') as description,
CAST(lnk_dedup.quantity AS int) as quantity,
lnk_dedup.invoice_ts as invoice_ts,
CAST(lnk_dedup.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_dedup.customer_id AS int) as customer_id,
coalesce(lnk_dedup.country, '') as country,
coalesce(lnk_dedup.store_id, '') as store_id,
coalesce(lnk_dedup.employee_id, '') as employee_id,
CAST(lnk_dedup.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_dedup.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_dedup.is_cancel AS int) as is_cancel,
CAST(lnk_dedup.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_dedup.is_guest AS int) as is_guest
FROM
lnk_dedup""")
lnk_stg.createOrReplaceTempView("lnk_stg")


# COMMAND ----------
# Component lnk_anom, Type TRANSFORMATION Original node name flt_Anomalies, link lnk_anom
lnk_anom = spark.sql(f"""
SELECT
coalesce(lnk_anom_in.invoice_no, '') as invoice_no,
coalesce(lnk_anom_in.stock_code, '') as stock_code,
coalesce(lnk_anom_in.description, '') as description,
CAST(lnk_anom_in.quantity AS int) as quantity,
lnk_anom_in.invoice_ts as invoice_ts,
CAST(lnk_anom_in.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_anom_in.customer_id AS int) as customer_id,
coalesce(lnk_anom_in.country, '') as country,
coalesce(lnk_anom_in.store_id, '') as store_id,
coalesce(lnk_anom_in.employee_id, '') as employee_id,
CAST(lnk_anom_in.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_anom_in.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_anom_in.is_cancel AS int) as is_cancel,
CAST(lnk_anom_in.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_anom_in.is_guest AS int) as is_guest
FROM
lnk_anom_in
WHERE unit_price = 0 or quantity > 5000 or quantity < -5000""")
lnk_anom.createOrReplaceTempView("lnk_anom")


# COMMAND ----------
# Component tgt_dq_order_anomalies, Type TARGET 

tgt_dq_order_anomalies = lnk_anom.select( \
	coalesce(lnk_anom.invoice_no, lit('')).alias('invoice_no'), \
	coalesce(lnk_anom.stock_code, lit('')).alias('stock_code'), \
	coalesce(lnk_anom.description, lit('')).alias('description'), \
	lnk_anom.quantity.cast('int').alias('quantity'), \
	lnk_anom.invoice_ts.alias('invoice_ts'), \
	lnk_anom.unit_price.cast('decimal(18,2)').alias('unit_price'), \
	lnk_anom.customer_id.cast('int').alias('customer_id'), \
	coalesce(lnk_anom.country, lit('')).alias('country'), \
	coalesce(lnk_anom.store_id, lit('')).alias('store_id'), \
	coalesce(lnk_anom.employee_id, lit('')).alias('employee_id'), \
	lnk_anom.invoice_date_key.cast('int').alias('invoice_date_key'), \
	lnk_anom.line_amount.cast('decimal(18,2)').alias('line_amount'), \
	lnk_anom.is_cancel.cast('int').alias('is_cancel'), \
	lnk_anom.is_nonproduct.cast('int').alias('is_nonproduct'), \
	lnk_anom.is_guest.cast('int').alias('is_guest') \
)
tgt_dq_order_anomalies.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.dq_order_anomalies')


# COMMAND ----------
# Component tgt_stg_orders, Type TARGET 

tgt_stg_orders = lnk_stg.select( \
	coalesce(lnk_stg.invoice_no, lit('')).alias('invoice_no'), \
	coalesce(lnk_stg.stock_code, lit('')).alias('stock_code'), \
	coalesce(lnk_stg.description, lit('')).alias('description'), \
	lnk_stg.quantity.cast('int').alias('quantity'), \
	lnk_stg.invoice_ts.alias('invoice_ts'), \
	lnk_stg.unit_price.cast('decimal(18,2)').alias('unit_price'), \
	lnk_stg.customer_id.cast('int').alias('customer_id'), \
	coalesce(lnk_stg.country, lit('')).alias('country'), \
	coalesce(lnk_stg.store_id, lit('')).alias('store_id'), \
	coalesce(lnk_stg.employee_id, lit('')).alias('employee_id'), \
	lnk_stg.invoice_date_key.cast('int').alias('invoice_date_key'), \
	lnk_stg.line_amount.cast('decimal(18,2)').alias('line_amount'), \
	lnk_stg.is_cancel.cast('int').alias('is_cancel'), \
	lnk_stg.is_nonproduct.cast('int').alias('is_nonproduct'), \
	lnk_stg.is_guest.cast('int').alias('is_guest') \
)
tgt_stg_orders.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.stg_orders')


# COMMAND ----------
# Component lnk_rows_by_country, Type AGGREGATOR Original node name agg_Rows_By_Country, link lnk_rows_by_country
lnk_rows_by_country = spark.sql(f"""
SELECT
coalesce(lnk_cnt_in.country, '') as country,
COUNT(*) as rows_loaded
FROM
lnk_cnt_in
GROUP BY
coalesce(lnk_cnt_in.country, '')""")
lnk_rows_by_country.createOrReplaceTempView("lnk_rows_by_country")


# COMMAND ----------
# Component tgt_audit_land_orders, Type TARGET 

tgt_audit_land_orders = lnk_rows_by_country.select( \
	coalesce(lnk_rows_by_country.country, lit('')).alias('country'), \
	lnk_rows_by_country.rows_loaded.cast('int').alias('rows_loaded') \
)
tgt_audit_land_orders.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.audit_land_orders')
