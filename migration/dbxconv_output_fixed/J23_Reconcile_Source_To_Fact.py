# Databricks notebook source
# Code converted on 2026-11-19 16:24:51
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
# Component lnk_orders, Type SOURCE Original node name src_stg_orders, link lnk_orders
lnk_orders = spark.read.table(f'{catalog}.{schema}.stg_orders')
lnk_orders.createOrReplaceTempView("lnk_orders")


# COMMAND ----------
# Component lnk_k2, Type TRANSFORMATION Original node name xfm_Key_Fact, link lnk_k2
lnk_k2 = spark.sql(f"""
SELECT
CAST(lnk_fact.sales_amount AS decimal(18,2)) as sales_amount,
'ALL' as rec_key
FROM
lnk_fact""")
lnk_k2.createOrReplaceTempView("lnk_k2")


# COMMAND ----------
# Component lnk_a2, Type AGGREGATOR Original node name agg_Fact_Amount, link lnk_a2
lnk_a2 = spark.sql(f"""
SELECT
coalesce(lnk_k2.rec_key, '') as rec_key,
SUM(sales_amount) as fact_amount
FROM
lnk_k2
GROUP BY
coalesce(lnk_k2.rec_key, '')""")
lnk_a2.createOrReplaceTempView("lnk_a2")


# COMMAND ----------
# Component lnk_lines, Type TRANSFORMATION Original node name flt_Sales_Lines, link lnk_lines
lnk_lines = spark.sql(f"""
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
WHERE is_cancel = 0 and is_nonproduct = 0 and quantity > 0""")
lnk_lines.createOrReplaceTempView("lnk_lines")


# COMMAND ----------
# Component lnk_k1, Type TRANSFORMATION Original node name xfm_Key_Stage, link lnk_k1
lnk_k1 = spark.sql(f"""
SELECT
CAST(lnk_lines.line_amount AS decimal(18,2)) as line_amount,
'ALL' as rec_key
FROM
lnk_lines""")
lnk_k1.createOrReplaceTempView("lnk_k1")


# COMMAND ----------
# Component lnk_a1, Type AGGREGATOR Original node name agg_Stage_Amount, link lnk_a1
lnk_a1 = spark.sql(f"""
SELECT
coalesce(lnk_k1.rec_key, '') as rec_key,
SUM(line_amount) as stage_amount
FROM
lnk_k1
GROUP BY
coalesce(lnk_k1.rec_key, '')""")
lnk_a1.createOrReplaceTempView("lnk_a1")


# COMMAND ----------
# Component lnk_rec, Type JOINER Original node name jn_Reconcile, link lnk_rec
lnk_rec = spark.sql(f"""
SELECT
coalesce(lnk_a1.rec_key, '') as rec_key,
CAST(lnk_a1.stage_amount AS decimal(18,2)) as stage_amount,
CAST(lnk_a2.fact_amount AS decimal(18,2)) as fact_amount
FROM
lnk_a1
INNER JOIN lnk_a2 ON coalesce(lnk_a1.rec_key, '') = coalesce(lnk_a2.rec_key, '')""")
lnk_rec.createOrReplaceTempView("lnk_rec")


# COMMAND ----------
# Component lnk_diff, Type TRANSFORMATION Original node name xfm_Diff, link lnk_diff
lnk_diff = spark.sql(f"""
SELECT
coalesce(lnk_rec.rec_key, '') as rec_key,
CAST(lnk_rec.stage_amount AS decimal(18,2)) as stage_amount,
CAST(lnk_rec.fact_amount AS decimal(18,2)) as fact_amount,
CAST(lnk_rec.stage_amount AS decimal(18,2)) - CAST(lnk_rec.fact_amount AS decimal(18,2)) as diff_amount,
IF ( Abs ( CAST(lnk_rec.stage_amount AS decimal(18,2)) - CAST(lnk_rec.fact_amount AS decimal(18,2)) ) < 0.01 , 'PASS' , 'FAIL' ) as status
FROM
lnk_rec""")
lnk_diff.createOrReplaceTempView("lnk_diff")


# COMMAND ----------
# Component tgt_audit_reconciliation, Type TARGET 

tgt_audit_reconciliation = lnk_diff.select( \
	coalesce(lnk_diff.rec_key, lit('')).alias('rec_key'), \
	lnk_diff.stage_amount.cast('decimal(18,2)').alias('stage_amount'), \
	lnk_diff.fact_amount.cast('decimal(18,2)').alias('fact_amount'), \
	lnk_diff.diff_amount.cast('decimal(18,2)').alias('diff_amount'), \
	coalesce(lnk_diff.status, lit('')).alias('status') \
)
tgt_audit_reconciliation.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.audit_reconciliation')
