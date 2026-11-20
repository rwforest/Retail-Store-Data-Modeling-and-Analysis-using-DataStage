# Databricks notebook source
# Code converted on 2026-11-19 16:23:59
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
# Component lnk_dim_store, Type SOURCE Original node name src_dim_store, link lnk_dim_store
lnk_dim_store = spark.read.table(f'{catalog}.{schema}.dim_store')
lnk_dim_store.createOrReplaceTempView("lnk_dim_store")


# COMMAND ----------
# Component lnk_fact, Type SOURCE Original node name src_fact_sales, link lnk_fact
lnk_fact = spark.read.table(f'{catalog}.{schema}.fact_sales')
lnk_fact.createOrReplaceTempView("lnk_fact")


# COMMAND ----------
# Component lnk_sm, Type AGGREGATOR Original node name agg_Store_Month, link lnk_sm
lnk_sm = spark.sql(f"""
SELECT
CAST(lnk_fact.store_sk AS int) as store_sk,
coalesce(lnk_fact.year_month, '') as year_month,
SUM(sales_amount) as sales,
SUM(gross_profit) as profit,
SUM(line_cnt) as lines
FROM
lnk_fact
GROUP BY
CAST(lnk_fact.store_sk AS int),
coalesce(lnk_fact.year_month, '')""")
lnk_sm.createOrReplaceTempView("lnk_sm")


# COMMAND ----------
# Component lnk_named, Type JOINER Original node name jn_Store, link lnk_named
lnk_named = spark.sql(f"""
SELECT
CAST(lnk_sm.store_sk AS int) as store_sk,
coalesce(lnk_sm.year_month, '') as year_month,
CAST(lnk_sm.sales AS decimal(18,2)) as sales,
CAST(lnk_sm.profit AS decimal(18,2)) as profit,
CAST(lnk_sm.lines AS bigint) as lines,
coalesce(lnk_dim_store.store_id, '') as store_id,
coalesce(lnk_dim_store.store_name, '') as store_name,
CAST(lnk_dim_store.no_of_employees AS int) as no_of_employees,
coalesce(lnk_dim_store.region, '') as region
FROM
lnk_sm
INNER JOIN lnk_dim_store ON CAST(lnk_sm.store_sk AS int) = CAST(lnk_dim_store.store_sk AS int)""")
lnk_named.createOrReplaceTempView("lnk_named")


# COMMAND ----------
# Component lnk_kpi, Type TRANSFORMATION Original node name xfm_Store_KPIs, link lnk_kpi
lnk_kpi = spark.sql(f"""
SELECT
CAST(lnk_named.store_sk AS int) as store_sk,
coalesce(lnk_named.year_month, '') as year_month,
CAST(lnk_named.sales AS decimal(18,2)) as sales,
CAST(lnk_named.profit AS decimal(18,2)) as profit,
CAST(lnk_named.lines AS bigint) as lines,
coalesce(lnk_named.store_id, '') as store_id,
coalesce(lnk_named.store_name, '') as store_name,
CAST(lnk_named.no_of_employees AS int) as no_of_employees,
coalesce(lnk_named.region, '') as region,
IF ( CAST(lnk_named.no_of_employees AS int) > 0 , CAST(lnk_named.sales AS decimal(18,2)) / CAST(lnk_named.no_of_employees AS int) , 0 ) as sales_per_employee,
IF ( CAST(lnk_named.sales AS decimal(18,2)) > 0 , CAST(lnk_named.profit AS decimal(18,2)) / CAST(lnk_named.sales AS decimal(18,2)) , 0 ) as margin_pct
FROM
lnk_named""")
lnk_kpi.createOrReplaceTempView("lnk_kpi")


# COMMAND ----------
# Component lnk_sorted, Type SORTER Original node name srt_Store_Month, link lnk_sorted
lnk_sorted = spark.sql(f"""
SELECT
CAST(lnk_kpi.store_sk AS int) as store_sk,
coalesce(lnk_kpi.year_month, '') as year_month,
CAST(lnk_kpi.sales AS decimal(18,2)) as sales,
CAST(lnk_kpi.profit AS decimal(18,2)) as profit,
CAST(lnk_kpi.lines AS bigint) as lines,
coalesce(lnk_kpi.store_id, '') as store_id,
coalesce(lnk_kpi.store_name, '') as store_name,
CAST(lnk_kpi.no_of_employees AS int) as no_of_employees,
coalesce(lnk_kpi.region, '') as region,
CAST(lnk_kpi.sales_per_employee AS decimal(18,2)) as sales_per_employee,
CAST(lnk_kpi.margin_pct AS decimal(18,6)) as margin_pct
FROM
lnk_kpi
ORDER BY store_sk asc, year_month asc""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")


# COMMAND ----------
# Component tgt_mart_store_monthly, Type TARGET 

tgt_mart_store_monthly = lnk_sorted.select( \
	lnk_sorted.store_sk.cast('int').alias('store_sk'), \
	coalesce(lnk_sorted.year_month, lit('')).alias('year_month'), \
	lnk_sorted.sales.cast('decimal(18,2)').alias('sales'), \
	lnk_sorted.profit.cast('decimal(18,2)').alias('profit'), \
	lnk_sorted.lines.cast('bigint').alias('lines'), \
	coalesce(lnk_sorted.store_id, lit('')).alias('store_id'), \
	coalesce(lnk_sorted.store_name, lit('')).alias('store_name'), \
	lnk_sorted.no_of_employees.cast('int').alias('no_of_employees'), \
	coalesce(lnk_sorted.region, lit('')).alias('region'), \
	lnk_sorted.sales_per_employee.cast('decimal(18,2)').alias('sales_per_employee'), \
	lnk_sorted.margin_pct.cast('decimal(18,6)').alias('margin_pct') \
)
tgt_mart_store_monthly.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.Store_Monthly')
