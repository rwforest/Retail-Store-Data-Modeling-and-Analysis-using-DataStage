# Databricks notebook source
# Code converted on 2026-11-19 16:21:23
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
# Component lnk_fact, Type SOURCE Original node name src_fact_sales, link lnk_fact
lnk_fact = spark.read.table(f'{catalog}.{schema}.fact_sales')
lnk_fact.createOrReplaceTempView("lnk_fact")


# COMMAND ----------
# Component lnk_inv, Type AGGREGATOR Original node name agg_Invoice, link lnk_inv
lnk_inv = spark.sql(f"""
SELECT
coalesce(lnk_fact.invoice_no, '') as invoice_no,
CAST(lnk_fact.customer_id AS int) as customer_id,
CAST(lnk_fact.customer_sk AS int) as customer_sk,
CAST(lnk_fact.store_sk AS int) as store_sk,
CAST(lnk_fact.employee_sk AS int) as employee_sk,
CAST(lnk_fact.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_fact.year_month, '') as year_month,
SUM(sales_amount) as invoice_sales,
SUM(gross_profit) as invoice_profit,
SUM(quantity) as item_qty,
SUM(line_cnt) as line_total
FROM
lnk_fact
GROUP BY
coalesce(lnk_fact.invoice_no, ''),
CAST(lnk_fact.customer_id AS int),
CAST(lnk_fact.customer_sk AS int),
CAST(lnk_fact.store_sk AS int),
CAST(lnk_fact.employee_sk AS int),
CAST(lnk_fact.invoice_date_key AS int),
coalesce(lnk_fact.year_month, '')""")
lnk_inv.createOrReplaceTempView("lnk_inv")


# COMMAND ----------
# Component lnk_basket, Type TRANSFORMATION Original node name xfm_Basket_Metrics, link lnk_basket
lnk_basket = spark.sql(f"""
SELECT
coalesce(lnk_inv.invoice_no, '') as invoice_no,
CAST(lnk_inv.customer_id AS int) as customer_id,
CAST(lnk_inv.customer_sk AS int) as customer_sk,
CAST(lnk_inv.store_sk AS int) as store_sk,
CAST(lnk_inv.employee_sk AS int) as employee_sk,
CAST(lnk_inv.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_inv.year_month, '') as year_month,
CAST(lnk_inv.invoice_sales AS decimal(18,2)) as invoice_sales,
CAST(lnk_inv.invoice_profit AS decimal(18,2)) as invoice_profit,
CAST(lnk_inv.item_qty AS bigint) as item_qty,
CAST(lnk_inv.line_total AS bigint) as line_total,
IF ( CAST(lnk_inv.line_total AS bigint) > 0 , CAST(lnk_inv.invoice_sales AS decimal(18,2)) / CAST(lnk_inv.line_total AS bigint) , 0 ) as avg_line_value,
1 as invoice_cnt
FROM
lnk_inv""")
lnk_basket.createOrReplaceTempView("lnk_basket")


# COMMAND ----------
# Component lnk_sorted, Type SORTER Original node name srt_Fact_Invoice, link lnk_sorted
lnk_sorted = spark.sql(f"""
SELECT
coalesce(lnk_basket.invoice_no, '') as invoice_no,
CAST(lnk_basket.customer_id AS int) as customer_id,
CAST(lnk_basket.customer_sk AS int) as customer_sk,
CAST(lnk_basket.store_sk AS int) as store_sk,
CAST(lnk_basket.employee_sk AS int) as employee_sk,
CAST(lnk_basket.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_basket.year_month, '') as year_month,
CAST(lnk_basket.invoice_sales AS decimal(18,2)) as invoice_sales,
CAST(lnk_basket.invoice_profit AS decimal(18,2)) as invoice_profit,
CAST(lnk_basket.item_qty AS bigint) as item_qty,
CAST(lnk_basket.line_total AS bigint) as line_total,
CAST(lnk_basket.avg_line_value AS decimal(18,2)) as avg_line_value,
CAST(lnk_basket.invoice_cnt AS int) as invoice_cnt
FROM
lnk_basket
ORDER BY invoice_no asc""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")


# COMMAND ----------
# Component tgt_fact_invoice, Type TARGET 

tgt_fact_invoice = lnk_sorted.select( \
	coalesce(lnk_sorted.invoice_no, lit('')).alias('invoice_no'), \
	lnk_sorted.customer_id.cast('int').alias('customer_id'), \
	lnk_sorted.customer_sk.cast('int').alias('customer_sk'), \
	lnk_sorted.store_sk.cast('int').alias('store_sk'), \
	lnk_sorted.employee_sk.cast('int').alias('employee_sk'), \
	lnk_sorted.invoice_date_key.cast('int').alias('invoice_date_key'), \
	coalesce(lnk_sorted.year_month, lit('')).alias('year_month'), \
	lnk_sorted.invoice_sales.cast('decimal(18,2)').alias('invoice_sales'), \
	lnk_sorted.invoice_profit.cast('decimal(18,2)').alias('invoice_profit'), \
	lnk_sorted.item_qty.cast('bigint').alias('item_qty'), \
	lnk_sorted.line_total.cast('bigint').alias('line_total'), \
	lnk_sorted.avg_line_value.cast('decimal(18,2)').alias('avg_line_value'), \
	lnk_sorted.invoice_cnt.cast('int').alias('invoice_cnt') \
)
tgt_fact_invoice.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.fact_invoice')
