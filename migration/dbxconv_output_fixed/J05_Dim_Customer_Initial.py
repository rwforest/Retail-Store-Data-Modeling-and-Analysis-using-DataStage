# Databricks notebook source
# Code converted on 2026-11-19 16:18:58
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
# Component lnk_stg, Type SOURCE Original node name src_stg_customers_2010, link lnk_stg
lnk_stg = spark.read.table(f'{catalog}.{schema}.stg_customers_2010')
lnk_stg.createOrReplaceTempView("lnk_stg")


# COMMAND ----------
# Component lnk_v1, Type TRANSFORMATION Original node name xfm_Version1, link lnk_v1
lnk_v1 = spark.sql(f"""
SELECT
CAST(lnk_stg.customer_id AS int) * 1000 + 1 as customer_sk,
CAST(lnk_stg.customer_id AS int) as customer_id,
coalesce(lnk_stg.cust_name, '') as cust_name,
coalesce(lnk_stg.gender, '') as gender,
CAST(lnk_stg.age AS int) as age,
CAST(lnk_stg.spending_score AS int) as spending_score,
CAST(lnk_stg.annual_income_k AS int) as annual_income_k,
coalesce(lnk_stg.country, '') as country,
coalesce(lnk_stg.customer_type, '') as customer_type,
coalesce(lnk_stg.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_stg.email, '') as email,
coalesce(lnk_stg.city, '') as city,
CAST(lnk_stg.attr_hash AS bigint) as attr_hash,
1 as version_no,
19000101 as eff_start_key,
99991231 as eff_end_key,
1 as is_current
FROM
lnk_stg""")
lnk_v1.createOrReplaceTempView("lnk_v1")


# COMMAND ----------
# Component lnk_sorted, Type SORTER Original node name srt_Dim_Customer, link lnk_sorted
lnk_sorted = spark.sql(f"""
SELECT
CAST(lnk_v1.customer_sk AS int) as customer_sk,
CAST(lnk_v1.customer_id AS int) as customer_id,
coalesce(lnk_v1.cust_name, '') as cust_name,
coalesce(lnk_v1.gender, '') as gender,
CAST(lnk_v1.age AS int) as age,
CAST(lnk_v1.spending_score AS int) as spending_score,
CAST(lnk_v1.annual_income_k AS int) as annual_income_k,
coalesce(lnk_v1.country, '') as country,
coalesce(lnk_v1.customer_type, '') as customer_type,
coalesce(lnk_v1.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_v1.email, '') as email,
coalesce(lnk_v1.city, '') as city,
CAST(lnk_v1.attr_hash AS bigint) as attr_hash,
CAST(lnk_v1.version_no AS int) as version_no,
CAST(lnk_v1.eff_start_key AS int) as eff_start_key,
CAST(lnk_v1.eff_end_key AS int) as eff_end_key,
CAST(lnk_v1.is_current AS int) as is_current
FROM
lnk_v1
ORDER BY customer_id asc, version_no asc""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")


# COMMAND ----------
# Component tgt_dim_customer, Type TARGET 

tgt_dim_customer = lnk_sorted.select( \
	lnk_sorted.customer_sk.cast('int').alias('customer_sk'), \
	lnk_sorted.customer_id.cast('int').alias('customer_id'), \
	coalesce(lnk_sorted.cust_name, lit('')).alias('cust_name'), \
	coalesce(lnk_sorted.gender, lit('')).alias('gender'), \
	lnk_sorted.age.cast('int').alias('age'), \
	lnk_sorted.spending_score.cast('int').alias('spending_score'), \
	lnk_sorted.annual_income_k.cast('int').alias('annual_income_k'), \
	coalesce(lnk_sorted.country, lit('')).alias('country'), \
	coalesce(lnk_sorted.customer_type, lit('')).alias('customer_type'), \
	coalesce(lnk_sorted.loyalty_tier, lit('')).alias('loyalty_tier'), \
	coalesce(lnk_sorted.email, lit('')).alias('email'), \
	coalesce(lnk_sorted.city, lit('')).alias('city'), \
	lnk_sorted.attr_hash.cast('bigint').alias('attr_hash'), \
	lnk_sorted.version_no.cast('int').alias('version_no'), \
	lnk_sorted.eff_start_key.cast('int').alias('eff_start_key'), \
	lnk_sorted.eff_end_key.cast('int').alias('eff_end_key'), \
	lnk_sorted.is_current.cast('int').alias('is_current') \
)
tgt_dim_customer.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.dim_customer')
