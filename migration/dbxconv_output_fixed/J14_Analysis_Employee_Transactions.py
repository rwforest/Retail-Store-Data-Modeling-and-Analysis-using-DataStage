# Databricks notebook source
# Code converted on 2026-11-20 14:14:24
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
# Component lnk_dim_emp, Type SOURCE Original node name src_dim_employee, link lnk_dim_emp
lnk_dim_emp = spark.read.table(f'{catalog}.{schema}.dim_employee')
lnk_dim_emp.createOrReplaceTempView("lnk_dim_emp")


# COMMAND ----------
# Component lnk_mart, Type SOURCE Original node name src_mart, link lnk_mart
lnk_mart = spark.read.table(f'{catalog}.{schema}.RETAIL_DATA_MART')
lnk_mart.createOrReplaceTempView("lnk_mart")


# COMMAND ----------
# Component lnk_invoices, Type UNIQUE_ROW Original node name rmd_Invoices, link lnk_invoices
lnk_invoices = spark.sql(f"""
SELECT
invoice_no,
stock_code,
quantity,
invoice_ts,
unit_price,
customer_id,
country,
store_id,
invoice_date_key,
year_month,
customer_sk,
customer_type,
loyalty_tier,
category,
product_sk,
region,
store_sk,
employee_sk,
currency,
sales_amount,
cost_amount,
gross_profit,
local_amount,
line_cnt,
cust_name,
gender,
age,
spending_score,
annual_income_k,
product_name,
list_price,
store_name,
rating,
no_of_employees
FROM (
SELECT
coalesce(lnk_mart.invoice_no, '') as invoice_no,
coalesce(lnk_mart.stock_code, '') as stock_code,
CAST(lnk_mart.quantity AS int) as quantity,
lnk_mart.invoice_ts as invoice_ts,
CAST(lnk_mart.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_mart.customer_id AS int) as customer_id,
coalesce(lnk_mart.country, '') as country,
coalesce(lnk_mart.store_id, '') as store_id,
CAST(lnk_mart.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_mart.year_month, '') as year_month,
CAST(lnk_mart.customer_sk AS int) as customer_sk,
coalesce(lnk_mart.customer_type, '') as customer_type,
coalesce(lnk_mart.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_mart.category, '') as category,
CAST(lnk_mart.product_sk AS int) as product_sk,
coalesce(lnk_mart.region, '') as region,
CAST(lnk_mart.store_sk AS int) as store_sk,
CAST(lnk_mart.employee_sk AS int) as employee_sk,
coalesce(lnk_mart.currency, '') as currency,
CAST(lnk_mart.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_mart.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_mart.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_mart.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_mart.line_cnt AS int) as line_cnt,
coalesce(lnk_mart.cust_name, '') as cust_name,
coalesce(lnk_mart.gender, '') as gender,
CAST(lnk_mart.age AS int) as age,
CAST(lnk_mart.spending_score AS int) as spending_score,
CAST(lnk_mart.annual_income_k AS int) as annual_income_k,
coalesce(lnk_mart.product_name, '') as product_name,
CAST(lnk_mart.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_mart.store_name, '') as store_name,
CAST(lnk_mart.rating AS int) as rating,
CAST(lnk_mart.no_of_employees AS int) as no_of_employees,
ROW_NUMBER() OVER(PARTITION BY coalesce(lnk_mart.invoice_no, ''), CAST(lnk_mart.store_sk AS int), CAST(lnk_mart.employee_sk AS int) ORDER BY coalesce(lnk_mart.stock_code, ''), CAST(lnk_mart.quantity AS int), lnk_mart.invoice_ts, CAST(lnk_mart.unit_price AS decimal(18,2)), CAST(lnk_mart.customer_id AS int), coalesce(lnk_mart.country, ''), coalesce(lnk_mart.store_id, ''), CAST(lnk_mart.invoice_date_key AS int), coalesce(lnk_mart.year_month, ''), CAST(lnk_mart.customer_sk AS int), coalesce(lnk_mart.customer_type, ''), coalesce(lnk_mart.loyalty_tier, ''), coalesce(lnk_mart.category, ''), CAST(lnk_mart.product_sk AS int), coalesce(lnk_mart.region, ''), coalesce(lnk_mart.currency, ''), CAST(lnk_mart.sales_amount AS decimal(18,2)), CAST(lnk_mart.cost_amount AS decimal(18,2)), CAST(lnk_mart.gross_profit AS decimal(18,2)), CAST(lnk_mart.local_amount AS decimal(18,2)), CAST(lnk_mart.line_cnt AS int), coalesce(lnk_mart.cust_name, ''), coalesce(lnk_mart.gender, ''), CAST(lnk_mart.age AS int), CAST(lnk_mart.spending_score AS int), CAST(lnk_mart.annual_income_k AS int), coalesce(lnk_mart.product_name, ''), CAST(lnk_mart.list_price AS decimal(18,2)), coalesce(lnk_mart.store_name, ''), CAST(lnk_mart.rating AS int), CAST(lnk_mart.no_of_employees AS int)) dedup_rnk
FROM lnk_mart) dedup_lnk_invoices WHERE dedup_rnk = 1""")
lnk_invoices.createOrReplaceTempView("lnk_invoices")


# COMMAND ----------
# Component lnk_counts, Type AGGREGATOR Original node name agg_Transactions, link lnk_counts
lnk_counts = spark.sql(f"""
SELECT
CAST(lnk_invoices.store_sk AS int) as store_sk,
CAST(lnk_invoices.employee_sk AS int) as employee_sk,
COUNT(*) as total_transactions
FROM
lnk_invoices
GROUP BY
CAST(lnk_invoices.store_sk AS int),
CAST(lnk_invoices.employee_sk AS int)""")
lnk_counts.createOrReplaceTempView("lnk_counts")


# COMMAND ----------
# Component lnk_named, Type JOINER Original node name jn_Employee, link lnk_named
lnk_named = spark.sql(f"""
SELECT
CAST(lnk_counts.store_sk AS int) as store_sk,
CAST(lnk_counts.employee_sk AS int) as employee_sk,
CAST(lnk_counts.total_transactions AS int) as total_transactions,
coalesce(lnk_dim_emp.employee_id, '') as employee_id,
coalesce(lnk_dim_emp.employee_name, '') as employee_name,
coalesce(lnk_dim_emp.role, '') as role
FROM
lnk_counts
INNER JOIN lnk_dim_emp ON CAST(lnk_counts.employee_sk AS int) = CAST(lnk_dim_emp.employee_sk AS int)""")
lnk_named.createOrReplaceTempView("lnk_named")


# COMMAND ----------
# Component lnk_ranked, Type SORTER Original node name srt_Store_Rank, link lnk_ranked
lnk_ranked = spark.sql(f"""
SELECT
CAST(lnk_named.store_sk AS int) as store_sk,
CAST(lnk_named.employee_sk AS int) as employee_sk,
CAST(lnk_named.total_transactions AS int) as total_transactions,
coalesce(lnk_named.employee_id, '') as employee_id,
coalesce(lnk_named.employee_name, '') as employee_name,
coalesce(lnk_named.role, '') as role
FROM
lnk_named
ORDER BY store_sk asc, total_transactions desc""")
lnk_ranked.createOrReplaceTempView("lnk_ranked")


# COMMAND ----------
# Component lnk_top_in, Type REPLICATE Original node name cpy_Ranked, link lnk_top_in
lnk_top_in = spark.sql(f"""
SELECT
CAST(lnk_ranked.store_sk AS int) as store_sk,
CAST(lnk_ranked.employee_sk AS int) as employee_sk,
CAST(lnk_ranked.total_transactions AS int) as total_transactions,
coalesce(lnk_ranked.employee_id, '') as employee_id,
coalesce(lnk_ranked.employee_name, '') as employee_name,
coalesce(lnk_ranked.role, '') as role
FROM
lnk_ranked""")
lnk_top_in.createOrReplaceTempView("lnk_top_in")


# COMMAND ----------
# Component lnk_all, Type REPLICATE Original node name cpy_Ranked, link lnk_all
lnk_all = spark.sql(f"""
SELECT
CAST(lnk_ranked.store_sk AS int) as store_sk,
CAST(lnk_ranked.employee_sk AS int) as employee_sk,
CAST(lnk_ranked.total_transactions AS int) as total_transactions,
coalesce(lnk_ranked.employee_id, '') as employee_id,
coalesce(lnk_ranked.employee_name, '') as employee_name,
coalesce(lnk_ranked.role, '') as role
FROM
lnk_ranked""")
lnk_all.createOrReplaceTempView("lnk_all")


lnk_top10 = spark.sql(f"""
SELECT * FROM lnk_top_in
ORDER BY store_sk ASC, total_transactions DESC, employee_sk ASC, employee_id ASC, employee_name ASC, role ASC
LIMIT 10
""")
lnk_top10.createOrReplaceTempView("lnk_top10")


# COMMAND ----------
# Component tgt_employee_transactions, Type TARGET 

tgt_employee_transactions = lnk_all.select( \
	lnk_all.store_sk.cast('int').alias('store_sk'), \
	lnk_all.employee_sk.cast('int').alias('employee_sk'), \
	lnk_all.total_transactions.cast('int').alias('total_transactions'), \
	coalesce(lnk_all.employee_id, lit('')).alias('employee_id'), \
	coalesce(lnk_all.employee_name, lit('')).alias('employee_name'), \
	coalesce(lnk_all.role, lit('')).alias('role') \
)
tgt_employee_transactions.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.Q1_Employee_Transactions')


# COMMAND ----------
# Component tgt_top10_employees, Type TARGET 

tgt_top10_employees = lnk_top10.select( \
	lnk_top10.store_sk.cast('int').alias('store_sk'), \
	lnk_top10.employee_sk.cast('int').alias('employee_sk'), \
	lnk_top10.total_transactions.cast('int').alias('total_transactions'), \
	coalesce(lnk_top10.employee_id, lit('')).alias('employee_id'), \
	coalesce(lnk_top10.employee_name, lit('')).alias('employee_name'), \
	coalesce(lnk_top10.role, lit('')).alias('role') \
)
tgt_top10_employees.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.Q1_Top10_Employees')
