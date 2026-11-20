# Databricks notebook source
# Code converted on 2026-11-19 16:21:06
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
# Component lnk_dim_customer, Type SOURCE Original node name src_dim_customer, link lnk_dim_customer
lnk_dim_customer = spark.read.table(f'{catalog}.{schema}.dim_customer')
lnk_dim_customer.createOrReplaceTempView("lnk_dim_customer")


# COMMAND ----------
# Component lnk_dim_product, Type SOURCE Original node name src_dim_product, link lnk_dim_product
lnk_dim_product = spark.read.table(f'{catalog}.{schema}.dim_product')
lnk_dim_product.createOrReplaceTempView("lnk_dim_product")


# COMMAND ----------
# Component lnk_orders, Type SOURCE Original node name src_stg_orders, link lnk_orders
lnk_orders = spark.read.table(f'{catalog}.{schema}.stg_orders')
lnk_orders.createOrReplaceTempView("lnk_orders")


# COMMAND ----------
# Component lnk_cancels, Type TRANSFORMATION Original node name flt_Cancels, link lnk_cancels
lnk_cancels = spark.sql(f"""
SELECT
coalesce(lnk_orders.invoice_no, '') as invoice_no,
coalesce(lnk_orders.stock_code, '') as stock_code,
coalesce(lnk_orders.description, '') as description,
CAST(lnk_orders.quantity AS int) as quantity,
lnk_orders.invoice_ts as invoice_ts,
CAST(lnk_orders.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_orders.customer_id AS int) as customer_id,
coalesce(lnk_orders.country, '') as country,
coalesce(lnk_orders.store_id, '') as store_id,
coalesce(lnk_orders.employee_id, '') as employee_id,
CAST(lnk_orders.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_orders.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_orders.is_cancel AS int) as is_cancel,
CAST(lnk_orders.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_orders.is_guest AS int) as is_guest
FROM
lnk_orders
WHERE is_cancel = 1 and is_nonproduct = 0""")
lnk_cancels.createOrReplaceTempView("lnk_cancels")


# COMMAND ----------
# Component lnk_cur, Type TRANSFORMATION Original node name flt_Current_Customer, link lnk_cur
lnk_cur = spark.sql(f"""
SELECT
CAST(lnk_dim_customer.customer_sk AS int) as customer_sk,
CAST(lnk_dim_customer.customer_id AS int) as customer_id,
coalesce(lnk_dim_customer.cust_name, '') as cust_name,
coalesce(lnk_dim_customer.gender, '') as gender,
CAST(lnk_dim_customer.age AS int) as age,
CAST(lnk_dim_customer.spending_score AS int) as spending_score,
CAST(lnk_dim_customer.annual_income_k AS int) as annual_income_k,
coalesce(lnk_dim_customer.country, '') as country,
coalesce(lnk_dim_customer.customer_type, '') as customer_type,
coalesce(lnk_dim_customer.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_dim_customer.email, '') as email,
coalesce(lnk_dim_customer.city, '') as city,
CAST(lnk_dim_customer.attr_hash AS bigint) as attr_hash,
CAST(lnk_dim_customer.version_no AS int) as version_no,
CAST(lnk_dim_customer.eff_start_key AS int) as eff_start_key,
CAST(lnk_dim_customer.eff_end_key AS int) as eff_end_key,
CAST(lnk_dim_customer.is_current AS int) as is_current
FROM
lnk_dim_customer
WHERE is_current = 1""")
lnk_cur.createOrReplaceTempView("lnk_cur")


# COMMAND ----------
# Component lnk_ret, Type TRANSFORMATION Original node name xfm_Return_Measures, link lnk_ret
lnk_ret = spark.sql(f"""
SELECT
coalesce(lnk_cancels.invoice_no, '') as invoice_no,
coalesce(lnk_cancels.stock_code, '') as stock_code,
CAST(lnk_cancels.customer_id AS int) as customer_id,
coalesce(lnk_cancels.country, '') as country,
CAST(lnk_cancels.invoice_date_key AS int) as invoice_date_key,
date_format(lnk_cancels.invoice_ts, 'yyyyMM') as year_month,
Abs ( CAST(lnk_cancels.quantity AS int) ) as returned_qty,
Abs ( CAST(lnk_cancels.line_amount AS decimal(18,2)) ) as returned_amount
FROM
lnk_cancels""")
lnk_ret.createOrReplaceTempView("lnk_ret")


# COMMAND ----------
# Component lnk_p, Type JOINER Original node name jn_Product, link lnk_p
lnk_p = spark.sql(f"""
SELECT
coalesce(lnk_ret.invoice_no, '') as invoice_no,
coalesce(lnk_ret.stock_code, '') as stock_code,
CAST(lnk_ret.customer_id AS int) as customer_id,
coalesce(lnk_ret.country, '') as country,
CAST(lnk_ret.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_ret.year_month, '') as year_month,
CAST(lnk_ret.returned_qty AS int) as returned_qty,
CAST(lnk_ret.returned_amount AS decimal(18,2)) as returned_amount,
lnk_dim_product.category as category,
CAST(lnk_dim_product.product_sk AS int) as product_sk
FROM
lnk_ret
LEFT JOIN lnk_dim_product ON coalesce(lnk_ret.stock_code, '') = lnk_dim_product.stock_code""")
lnk_p.createOrReplaceTempView("lnk_p")


# COMMAND ----------
# Component lnk_p_nn, Type TRANSFORMATION Original node name xfm_Product_Defaults, link lnk_p_nn
lnk_p_nn = spark.sql(f"""
SELECT
coalesce(lnk_p.invoice_no, '') as invoice_no,
coalesce(lnk_p.stock_code, '') as stock_code,
CAST(lnk_p.customer_id AS int) as customer_id,
coalesce(lnk_p.country, '') as country,
CAST(lnk_p.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_p.year_month, '') as year_month,
CAST(lnk_p.returned_qty AS int) as returned_qty,
CAST(lnk_p.returned_amount AS decimal(18,2)) as returned_amount,
 COALESCE(lnk_p.category, 'Unknown') as category,
 COALESCE(CAST(lnk_p.product_sk AS int), - 1) as product_sk
FROM
lnk_p""")
lnk_p_nn.createOrReplaceTempView("lnk_p_nn")


# COMMAND ----------
# Component lnk_c, Type JOINER Original node name jn_Customer, link lnk_c
lnk_c = spark.sql(f"""
SELECT
coalesce(lnk_p_nn.invoice_no, '') as invoice_no,
coalesce(lnk_p_nn.stock_code, '') as stock_code,
CAST(lnk_p_nn.customer_id AS int) as customer_id,
coalesce(lnk_p_nn.country, '') as country,
CAST(lnk_p_nn.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_p_nn.year_month, '') as year_month,
CAST(lnk_p_nn.returned_qty AS int) as returned_qty,
CAST(lnk_p_nn.returned_amount AS decimal(18,2)) as returned_amount,
coalesce(lnk_p_nn.category, '') as category,
CAST(lnk_p_nn.product_sk AS int) as product_sk,
CAST(lnk_cur.customer_sk AS int) as customer_sk
FROM
lnk_p_nn
LEFT JOIN lnk_cur ON CAST(lnk_p_nn.customer_id AS int) = CAST(lnk_cur.customer_id AS int)""")
lnk_c.createOrReplaceTempView("lnk_c")


# COMMAND ----------
# Component lnk_c_nn, Type TRANSFORMATION Original node name xfm_Customer_Defaults, link lnk_c_nn
lnk_c_nn = spark.sql(f"""
SELECT
coalesce(lnk_c.invoice_no, '') as invoice_no,
coalesce(lnk_c.stock_code, '') as stock_code,
CAST(lnk_c.customer_id AS int) as customer_id,
coalesce(lnk_c.country, '') as country,
CAST(lnk_c.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_c.year_month, '') as year_month,
CAST(lnk_c.returned_qty AS int) as returned_qty,
CAST(lnk_c.returned_amount AS decimal(18,2)) as returned_amount,
coalesce(lnk_c.category, '') as category,
CAST(lnk_c.product_sk AS int) as product_sk,
 COALESCE(CAST(lnk_c.customer_sk AS int), - 1) as customer_sk
FROM
lnk_c""")
lnk_c_nn.createOrReplaceTempView("lnk_c_nn")


# COMMAND ----------
# Component lnk_sorted, Type SORTER Original node name srt_Fact_Returns, link lnk_sorted
lnk_sorted = spark.sql(f"""
SELECT
coalesce(lnk_c_nn.invoice_no, '') as invoice_no,
coalesce(lnk_c_nn.stock_code, '') as stock_code,
CAST(lnk_c_nn.customer_id AS int) as customer_id,
coalesce(lnk_c_nn.country, '') as country,
CAST(lnk_c_nn.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_c_nn.year_month, '') as year_month,
CAST(lnk_c_nn.returned_qty AS int) as returned_qty,
CAST(lnk_c_nn.returned_amount AS decimal(18,2)) as returned_amount,
coalesce(lnk_c_nn.category, '') as category,
CAST(lnk_c_nn.product_sk AS int) as product_sk,
CAST(lnk_c_nn.customer_sk AS int) as customer_sk
FROM
lnk_c_nn
ORDER BY invoice_no asc, stock_code asc""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")


# COMMAND ----------
# Component lnk_audit_in, Type REPLICATE Original node name cpy_Returns, link lnk_audit_in
lnk_audit_in = spark.sql(f"""
SELECT
coalesce(lnk_sorted.invoice_no, '') as invoice_no,
coalesce(lnk_sorted.stock_code, '') as stock_code,
CAST(lnk_sorted.customer_id AS int) as customer_id,
coalesce(lnk_sorted.country, '') as country,
CAST(lnk_sorted.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_sorted.year_month, '') as year_month,
CAST(lnk_sorted.returned_qty AS int) as returned_qty,
CAST(lnk_sorted.returned_amount AS decimal(18,2)) as returned_amount,
coalesce(lnk_sorted.category, '') as category,
CAST(lnk_sorted.product_sk AS int) as product_sk,
CAST(lnk_sorted.customer_sk AS int) as customer_sk
FROM
lnk_sorted""")
lnk_audit_in.createOrReplaceTempView("lnk_audit_in")


# COMMAND ----------
# Component lnk_out, Type REPLICATE Original node name cpy_Returns, link lnk_out
lnk_out = spark.sql(f"""
SELECT
coalesce(lnk_sorted.invoice_no, '') as invoice_no,
coalesce(lnk_sorted.stock_code, '') as stock_code,
CAST(lnk_sorted.customer_id AS int) as customer_id,
coalesce(lnk_sorted.country, '') as country,
CAST(lnk_sorted.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_sorted.year_month, '') as year_month,
CAST(lnk_sorted.returned_qty AS int) as returned_qty,
CAST(lnk_sorted.returned_amount AS decimal(18,2)) as returned_amount,
coalesce(lnk_sorted.category, '') as category,
CAST(lnk_sorted.product_sk AS int) as product_sk,
CAST(lnk_sorted.customer_sk AS int) as customer_sk
FROM
lnk_sorted""")
lnk_out.createOrReplaceTempView("lnk_out")


# COMMAND ----------
# Component tgt_fact_returns, Type TARGET 

tgt_fact_returns = lnk_out.select( \
	coalesce(lnk_out.invoice_no, lit('')).alias('invoice_no'), \
	coalesce(lnk_out.stock_code, lit('')).alias('stock_code'), \
	lnk_out.customer_id.cast('int').alias('customer_id'), \
	coalesce(lnk_out.country, lit('')).alias('country'), \
	lnk_out.invoice_date_key.cast('int').alias('invoice_date_key'), \
	coalesce(lnk_out.year_month, lit('')).alias('year_month'), \
	lnk_out.returned_qty.cast('int').alias('returned_qty'), \
	lnk_out.returned_amount.cast('decimal(18,2)').alias('returned_amount'), \
	coalesce(lnk_out.category, lit('')).alias('category'), \
	lnk_out.product_sk.cast('int').alias('product_sk'), \
	lnk_out.customer_sk.cast('int').alias('customer_sk') \
)
tgt_fact_returns.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.fact_returns')


# COMMAND ----------
# Component lnk_by_cat, Type AGGREGATOR Original node name agg_Returns_By_Category, link lnk_by_cat
lnk_by_cat = spark.sql(f"""
SELECT
coalesce(lnk_audit_in.category, '') as category,
SUM(returned_amount) as returned_amount
FROM
lnk_audit_in
GROUP BY
coalesce(lnk_audit_in.category, '')""")
lnk_by_cat.createOrReplaceTempView("lnk_by_cat")


# COMMAND ----------
# Component tgt_audit_returns_by_category, Type TARGET 

tgt_audit_returns_by_category = lnk_by_cat.select( \
	coalesce(lnk_by_cat.category, lit('')).alias('category'), \
	lnk_by_cat.returned_amount.cast('decimal(18,2)').alias('returned_amount') \
)
tgt_audit_returns_by_category.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.audit_returns_by_category')
