# Databricks notebook source
# Code converted on 2026-11-19 16:21:42
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
# Component lnk_dim_store, Type SOURCE Original node name src_dim_store, link lnk_dim_store
lnk_dim_store = spark.read.table(f'{catalog}.{schema}.dim_store')
lnk_dim_store.createOrReplaceTempView("lnk_dim_store")


# COMMAND ----------
# Component lnk_fact, Type SOURCE Original node name src_fact_sales, link lnk_fact
lnk_fact = spark.read.table(f'{catalog}.{schema}.fact_sales')
lnk_fact.createOrReplaceTempView("lnk_fact")


# COMMAND ----------
# Component lnk_c, Type JOINER Original node name jn_Customer, link lnk_c
lnk_c = spark.sql(f"""
SELECT
coalesce(lnk_fact.invoice_no, '') as invoice_no,
coalesce(lnk_fact.stock_code, '') as stock_code,
CAST(lnk_fact.quantity AS int) as quantity,
lnk_fact.invoice_ts as invoice_ts,
CAST(lnk_fact.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_fact.customer_id AS int) as customer_id,
coalesce(lnk_fact.country, '') as country,
coalesce(lnk_fact.store_id, '') as store_id,
CAST(lnk_fact.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_fact.year_month, '') as year_month,
CAST(lnk_fact.customer_sk AS int) as customer_sk,
coalesce(lnk_fact.customer_type, '') as customer_type,
coalesce(lnk_fact.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_fact.category, '') as category,
CAST(lnk_fact.product_sk AS int) as product_sk,
coalesce(lnk_fact.region, '') as region,
CAST(lnk_fact.store_sk AS int) as store_sk,
CAST(lnk_fact.employee_sk AS int) as employee_sk,
coalesce(lnk_fact.currency, '') as currency,
CAST(lnk_fact.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_fact.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_fact.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_fact.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_fact.line_cnt AS int) as line_cnt,
coalesce(lnk_dim_customer.cust_name, '') as cust_name,
coalesce(lnk_dim_customer.gender, '') as gender,
CAST(lnk_dim_customer.age AS int) as age,
CAST(lnk_dim_customer.spending_score AS int) as spending_score,
CAST(lnk_dim_customer.annual_income_k AS int) as annual_income_k
FROM
lnk_fact
INNER JOIN lnk_dim_customer ON CAST(lnk_fact.customer_sk AS int) = CAST(lnk_dim_customer.customer_sk AS int)""")
lnk_c.createOrReplaceTempView("lnk_c")


# COMMAND ----------
# Component lnk_p, Type JOINER Original node name jn_Product, link lnk_p
lnk_p = spark.sql(f"""
SELECT
coalesce(lnk_c.invoice_no, '') as invoice_no,
coalesce(lnk_c.stock_code, '') as stock_code,
CAST(lnk_c.quantity AS int) as quantity,
lnk_c.invoice_ts as invoice_ts,
CAST(lnk_c.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_c.customer_id AS int) as customer_id,
coalesce(lnk_c.country, '') as country,
coalesce(lnk_c.store_id, '') as store_id,
CAST(lnk_c.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_c.year_month, '') as year_month,
CAST(lnk_c.customer_sk AS int) as customer_sk,
coalesce(lnk_c.customer_type, '') as customer_type,
coalesce(lnk_c.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_c.category, '') as category,
CAST(lnk_c.product_sk AS int) as product_sk,
coalesce(lnk_c.region, '') as region,
CAST(lnk_c.store_sk AS int) as store_sk,
CAST(lnk_c.employee_sk AS int) as employee_sk,
coalesce(lnk_c.currency, '') as currency,
CAST(lnk_c.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_c.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_c.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_c.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_c.line_cnt AS int) as line_cnt,
coalesce(lnk_c.cust_name, '') as cust_name,
coalesce(lnk_c.gender, '') as gender,
CAST(lnk_c.age AS int) as age,
CAST(lnk_c.spending_score AS int) as spending_score,
CAST(lnk_c.annual_income_k AS int) as annual_income_k,
coalesce(lnk_dim_product.product_name, '') as product_name,
CAST(lnk_dim_product.list_price AS decimal(18,2)) as list_price
FROM
lnk_c
INNER JOIN lnk_dim_product ON CAST(lnk_c.product_sk AS int) = CAST(lnk_dim_product.product_sk AS int)""")
lnk_p.createOrReplaceTempView("lnk_p")


# COMMAND ----------
# Component lnk_s, Type JOINER Original node name jn_Store, link lnk_s
lnk_s = spark.sql(f"""
SELECT
coalesce(lnk_p.invoice_no, '') as invoice_no,
coalesce(lnk_p.stock_code, '') as stock_code,
CAST(lnk_p.quantity AS int) as quantity,
lnk_p.invoice_ts as invoice_ts,
CAST(lnk_p.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_p.customer_id AS int) as customer_id,
coalesce(lnk_p.country, '') as country,
coalesce(lnk_p.store_id, '') as store_id,
CAST(lnk_p.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_p.year_month, '') as year_month,
CAST(lnk_p.customer_sk AS int) as customer_sk,
coalesce(lnk_p.customer_type, '') as customer_type,
coalesce(lnk_p.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_p.category, '') as category,
CAST(lnk_p.product_sk AS int) as product_sk,
coalesce(lnk_p.region, '') as region,
CAST(lnk_p.store_sk AS int) as store_sk,
CAST(lnk_p.employee_sk AS int) as employee_sk,
coalesce(lnk_p.currency, '') as currency,
CAST(lnk_p.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_p.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_p.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_p.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_p.line_cnt AS int) as line_cnt,
coalesce(lnk_p.cust_name, '') as cust_name,
coalesce(lnk_p.gender, '') as gender,
CAST(lnk_p.age AS int) as age,
CAST(lnk_p.spending_score AS int) as spending_score,
CAST(lnk_p.annual_income_k AS int) as annual_income_k,
coalesce(lnk_p.product_name, '') as product_name,
CAST(lnk_p.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_dim_store.store_name, '') as store_name,
CAST(lnk_dim_store.rating AS int) as rating,
CAST(lnk_dim_store.no_of_employees AS int) as no_of_employees
FROM
lnk_p
INNER JOIN lnk_dim_store ON CAST(lnk_p.store_sk AS int) = CAST(lnk_dim_store.store_sk AS int)""")
lnk_s.createOrReplaceTempView("lnk_s")


# COMMAND ----------
# Component lnk_types, Type TRANSFORMATION Original node name flt_Customer_Types, link lnk_types
lnk_types = spark.sql(f"""
SELECT
coalesce(lnk_s.invoice_no, '') as invoice_no,
coalesce(lnk_s.stock_code, '') as stock_code,
CAST(lnk_s.quantity AS int) as quantity,
lnk_s.invoice_ts as invoice_ts,
CAST(lnk_s.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_s.customer_id AS int) as customer_id,
coalesce(lnk_s.country, '') as country,
coalesce(lnk_s.store_id, '') as store_id,
CAST(lnk_s.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_s.year_month, '') as year_month,
CAST(lnk_s.customer_sk AS int) as customer_sk,
coalesce(lnk_s.customer_type, '') as customer_type,
coalesce(lnk_s.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_s.category, '') as category,
CAST(lnk_s.product_sk AS int) as product_sk,
coalesce(lnk_s.region, '') as region,
CAST(lnk_s.store_sk AS int) as store_sk,
CAST(lnk_s.employee_sk AS int) as employee_sk,
coalesce(lnk_s.currency, '') as currency,
CAST(lnk_s.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_s.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_s.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_s.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_s.line_cnt AS int) as line_cnt,
coalesce(lnk_s.cust_name, '') as cust_name,
coalesce(lnk_s.gender, '') as gender,
CAST(lnk_s.age AS int) as age,
CAST(lnk_s.spending_score AS int) as spending_score,
CAST(lnk_s.annual_income_k AS int) as annual_income_k,
coalesce(lnk_s.product_name, '') as product_name,
CAST(lnk_s.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_s.store_name, '') as store_name,
CAST(lnk_s.rating AS int) as rating,
CAST(lnk_s.no_of_employees AS int) as no_of_employees
FROM
lnk_s
WHERE customer_type = 'citizen' or customer_type = 'foreign'""")
lnk_types.createOrReplaceTempView("lnk_types")


# COMMAND ----------
# Component lnk_upper, Type TRANSFORMATION Original node name xfm_Upper_Names, link lnk_upper
lnk_upper = spark.sql(f"""
SELECT
coalesce(lnk_types.invoice_no, '') as invoice_no,
coalesce(lnk_types.stock_code, '') as stock_code,
CAST(lnk_types.quantity AS int) as quantity,
lnk_types.invoice_ts as invoice_ts,
CAST(lnk_types.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_types.customer_id AS int) as customer_id,
coalesce(lnk_types.country, '') as country,
coalesce(lnk_types.store_id, '') as store_id,
CAST(lnk_types.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_types.year_month, '') as year_month,
CAST(lnk_types.customer_sk AS int) as customer_sk,
coalesce(lnk_types.customer_type, '') as customer_type,
coalesce(lnk_types.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_types.category, '') as category,
CAST(lnk_types.product_sk AS int) as product_sk,
coalesce(lnk_types.region, '') as region,
CAST(lnk_types.store_sk AS int) as store_sk,
CAST(lnk_types.employee_sk AS int) as employee_sk,
coalesce(lnk_types.currency, '') as currency,
CAST(lnk_types.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_types.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_types.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_types.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_types.line_cnt AS int) as line_cnt,
UPPER(coalesce(lnk_types.cust_name, '')) as cust_name,
coalesce(lnk_types.gender, '') as gender,
CAST(lnk_types.age AS int) as age,
CAST(lnk_types.spending_score AS int) as spending_score,
CAST(lnk_types.annual_income_k AS int) as annual_income_k,
UPPER(coalesce(lnk_types.product_name, '')) as product_name,
CAST(lnk_types.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_types.store_name, '') as store_name,
CAST(lnk_types.rating AS int) as rating,
CAST(lnk_types.no_of_employees AS int) as no_of_employees
FROM
lnk_types""")
lnk_upper.createOrReplaceTempView("lnk_upper")


# COMMAND ----------
# Component lnk_sorted, Type SORTER Original node name srt_Mart, link lnk_sorted
lnk_sorted = spark.sql(f"""
SELECT
coalesce(lnk_upper.invoice_no, '') as invoice_no,
coalesce(lnk_upper.stock_code, '') as stock_code,
CAST(lnk_upper.quantity AS int) as quantity,
lnk_upper.invoice_ts as invoice_ts,
CAST(lnk_upper.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_upper.customer_id AS int) as customer_id,
coalesce(lnk_upper.country, '') as country,
coalesce(lnk_upper.store_id, '') as store_id,
CAST(lnk_upper.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_upper.year_month, '') as year_month,
CAST(lnk_upper.customer_sk AS int) as customer_sk,
coalesce(lnk_upper.customer_type, '') as customer_type,
coalesce(lnk_upper.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_upper.category, '') as category,
CAST(lnk_upper.product_sk AS int) as product_sk,
coalesce(lnk_upper.region, '') as region,
CAST(lnk_upper.store_sk AS int) as store_sk,
CAST(lnk_upper.employee_sk AS int) as employee_sk,
coalesce(lnk_upper.currency, '') as currency,
CAST(lnk_upper.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_upper.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_upper.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_upper.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_upper.line_cnt AS int) as line_cnt,
coalesce(lnk_upper.cust_name, '') as cust_name,
coalesce(lnk_upper.gender, '') as gender,
CAST(lnk_upper.age AS int) as age,
CAST(lnk_upper.spending_score AS int) as spending_score,
CAST(lnk_upper.annual_income_k AS int) as annual_income_k,
coalesce(lnk_upper.product_name, '') as product_name,
CAST(lnk_upper.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_upper.store_name, '') as store_name,
CAST(lnk_upper.rating AS int) as rating,
CAST(lnk_upper.no_of_employees AS int) as no_of_employees
FROM
lnk_upper
ORDER BY invoice_no asc, stock_code asc""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")


# COMMAND ----------
# Component tgt_mart_retail_data_mart, Type TARGET 

tgt_mart_retail_data_mart = lnk_sorted.select( \
	coalesce(lnk_sorted.invoice_no, lit('')).alias('invoice_no'), \
	coalesce(lnk_sorted.stock_code, lit('')).alias('stock_code'), \
	lnk_sorted.quantity.cast('int').alias('quantity'), \
	lnk_sorted.invoice_ts.alias('invoice_ts'), \
	lnk_sorted.unit_price.cast('decimal(18,2)').alias('unit_price'), \
	lnk_sorted.customer_id.cast('int').alias('customer_id'), \
	coalesce(lnk_sorted.country, lit('')).alias('country'), \
	coalesce(lnk_sorted.store_id, lit('')).alias('store_id'), \
	lnk_sorted.invoice_date_key.cast('int').alias('invoice_date_key'), \
	coalesce(lnk_sorted.year_month, lit('')).alias('year_month'), \
	lnk_sorted.customer_sk.cast('int').alias('customer_sk'), \
	coalesce(lnk_sorted.customer_type, lit('')).alias('customer_type'), \
	coalesce(lnk_sorted.loyalty_tier, lit('')).alias('loyalty_tier'), \
	coalesce(lnk_sorted.category, lit('')).alias('category'), \
	lnk_sorted.product_sk.cast('int').alias('product_sk'), \
	coalesce(lnk_sorted.region, lit('')).alias('region'), \
	lnk_sorted.store_sk.cast('int').alias('store_sk'), \
	lnk_sorted.employee_sk.cast('int').alias('employee_sk'), \
	coalesce(lnk_sorted.currency, lit('')).alias('currency'), \
	lnk_sorted.sales_amount.cast('decimal(18,2)').alias('sales_amount'), \
	lnk_sorted.cost_amount.cast('decimal(18,2)').alias('cost_amount'), \
	lnk_sorted.gross_profit.cast('decimal(18,2)').alias('gross_profit'), \
	lnk_sorted.local_amount.cast('decimal(18,2)').alias('local_amount'), \
	lnk_sorted.line_cnt.cast('int').alias('line_cnt'), \
	coalesce(lnk_sorted.cust_name, lit('')).alias('cust_name'), \
	coalesce(lnk_sorted.gender, lit('')).alias('gender'), \
	lnk_sorted.age.cast('int').alias('age'), \
	lnk_sorted.spending_score.cast('int').alias('spending_score'), \
	lnk_sorted.annual_income_k.cast('int').alias('annual_income_k'), \
	coalesce(lnk_sorted.product_name, lit('')).alias('product_name'), \
	lnk_sorted.list_price.cast('decimal(18,2)').alias('list_price'), \
	coalesce(lnk_sorted.store_name, lit('')).alias('store_name'), \
	lnk_sorted.rating.cast('int').alias('rating'), \
	lnk_sorted.no_of_employees.cast('int').alias('no_of_employees') \
)
tgt_mart_retail_data_mart.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.RETAIL_DATA_MART')
