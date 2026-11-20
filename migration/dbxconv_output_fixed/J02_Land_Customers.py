# Databricks notebook source
# Code converted on 2026-11-19 16:18:08
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
# Component lnk_raw_2010, Type SOURCE Original node name src_cust_2010, link lnk_raw_2010
lnk_raw_2010 = spark.read.format('csv').option('header', 'true').option('escape', '"').load(f'''{pSrcDir}/customers_snapshot_2010.csv''')
# Conforming fields names to the component layout
lnk_raw_2010_conformed_cols = ["CustomerID", "CustomerName", "Gender", "Age", "SpendingScore", "AnnualIncomeK", "Country", "CustomerType", "LoyaltyTier", "Email", "City", "SnapshotDate"]
lnk_raw_2010 = DatabricksConversionSupplements.conform_df_columns(lnk_raw_2010,lnk_raw_2010_conformed_cols)
lnk_raw_2010.createOrReplaceTempView("lnk_raw_2010")


# COMMAND ----------
# Component lnk_raw_2011, Type SOURCE Original node name src_cust_2011, link lnk_raw_2011
lnk_raw_2011 = spark.read.format('csv').option('header', 'true').option('escape', '"').load(f'''{pSrcDir}/customers_snapshot_2011.csv''')
# Conforming fields names to the component layout
lnk_raw_2011_conformed_cols = ["CustomerID", "CustomerName", "Gender", "Age", "SpendingScore", "AnnualIncomeK", "Country", "CustomerType", "LoyaltyTier", "Email", "City", "SnapshotDate"]
lnk_raw_2011 = DatabricksConversionSupplements.conform_df_columns(lnk_raw_2011,lnk_raw_2011_conformed_cols)
lnk_raw_2011.createOrReplaceTempView("lnk_raw_2011")


# COMMAND ----------
# Component lnk_std_2010, Type TRANSFORMATION Original node name xfm_Std_Cust_2010, link lnk_std_2010
lnk_std_2010 = spark.sql(f"""
SELECT
CAST(lnk_raw_2010.CustomerID AS int) as customer_id,
UPPER(trim(coalesce(lnk_raw_2010.CustomerName, ''))) as cust_name,
trim(coalesce(lnk_raw_2010.Gender, '')) as gender,
CAST(lnk_raw_2010.Age AS int) as age,
CAST(lnk_raw_2010.SpendingScore AS int) as spending_score,
CAST(lnk_raw_2010.AnnualIncomeK AS int) as annual_income_k,
trim(coalesce(lnk_raw_2010.Country, '')) as country,
trim(coalesce(lnk_raw_2010.CustomerType, '')) as customer_type,
UPPER(trim(coalesce(lnk_raw_2010.LoyaltyTier, ''))) as loyalty_tier,
LOWER(trim(coalesce(lnk_raw_2010.Email, ''))) as email,
trim(coalesce(lnk_raw_2010.City, '')) as city,
to_date(cast(coalesce(lnk_raw_2010.SnapshotDate, '') as string), 'yyyy-MM-dd') as snapshot_date,
CRC32 ( UPPER(trim(coalesce(lnk_raw_2010.CustomerName, ''))) || '|' || CAST(lnk_raw_2010.Age AS int) || '|' || CAST(lnk_raw_2010.SpendingScore AS int) || '|' || CAST(lnk_raw_2010.AnnualIncomeK AS int) || '|' || trim(coalesce(lnk_raw_2010.Country, '')) || '|' || UPPER(trim(coalesce(lnk_raw_2010.LoyaltyTier, ''))) || '|' || trim(coalesce(lnk_raw_2010.City, '')) || '|' || LOWER(trim(coalesce(lnk_raw_2010.Email, ''))) ) as attr_hash
FROM
lnk_raw_2010""")
lnk_std_2010.createOrReplaceTempView("lnk_std_2010")


# COMMAND ----------
# Component lnk_std_2011, Type TRANSFORMATION Original node name xfm_Std_Cust_2011, link lnk_std_2011
lnk_std_2011 = spark.sql(f"""
SELECT
CAST(lnk_raw_2011.CustomerID AS int) as customer_id,
UPPER(trim(coalesce(lnk_raw_2011.CustomerName, ''))) as cust_name,
trim(coalesce(lnk_raw_2011.Gender, '')) as gender,
CAST(lnk_raw_2011.Age AS int) as age,
CAST(lnk_raw_2011.SpendingScore AS int) as spending_score,
CAST(lnk_raw_2011.AnnualIncomeK AS int) as annual_income_k,
trim(coalesce(lnk_raw_2011.Country, '')) as country,
trim(coalesce(lnk_raw_2011.CustomerType, '')) as customer_type,
UPPER(trim(coalesce(lnk_raw_2011.LoyaltyTier, ''))) as loyalty_tier,
LOWER(trim(coalesce(lnk_raw_2011.Email, ''))) as email,
trim(coalesce(lnk_raw_2011.City, '')) as city,
to_date(cast(coalesce(lnk_raw_2011.SnapshotDate, '') as string), 'yyyy-MM-dd') as snapshot_date,
CRC32 ( UPPER(trim(coalesce(lnk_raw_2011.CustomerName, ''))) || '|' || CAST(lnk_raw_2011.Age AS int) || '|' || CAST(lnk_raw_2011.SpendingScore AS int) || '|' || CAST(lnk_raw_2011.AnnualIncomeK AS int) || '|' || trim(coalesce(lnk_raw_2011.Country, '')) || '|' || UPPER(trim(coalesce(lnk_raw_2011.LoyaltyTier, ''))) || '|' || trim(coalesce(lnk_raw_2011.City, '')) || '|' || LOWER(trim(coalesce(lnk_raw_2011.Email, ''))) ) as attr_hash
FROM
lnk_raw_2011""")
lnk_std_2011.createOrReplaceTempView("lnk_std_2011")


# COMMAND ----------
# Component lnk_dedup_2010, Type UNIQUE_ROW Original node name rmd_Cust_2010, link lnk_dedup_2010
lnk_dedup_2010 = spark.sql(f"""
SELECT
customer_id,
cust_name,
gender,
age,
spending_score,
annual_income_k,
country,
customer_type,
loyalty_tier,
email,
city,
snapshot_date,
attr_hash
FROM (
SELECT
CAST(lnk_std_2010.customer_id AS int) as customer_id,
coalesce(lnk_std_2010.cust_name, '') as cust_name,
coalesce(lnk_std_2010.gender, '') as gender,
CAST(lnk_std_2010.age AS int) as age,
CAST(lnk_std_2010.spending_score AS int) as spending_score,
CAST(lnk_std_2010.annual_income_k AS int) as annual_income_k,
coalesce(lnk_std_2010.country, '') as country,
coalesce(lnk_std_2010.customer_type, '') as customer_type,
coalesce(lnk_std_2010.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_std_2010.email, '') as email,
coalesce(lnk_std_2010.city, '') as city,
lnk_std_2010.snapshot_date as snapshot_date,
CAST(lnk_std_2010.attr_hash AS bigint) as attr_hash,
ROW_NUMBER() OVER(PARTITION BY CAST(lnk_std_2010.customer_id AS int) ORDER BY coalesce(lnk_std_2010.cust_name, ''), coalesce(lnk_std_2010.gender, ''), CAST(lnk_std_2010.age AS int), CAST(lnk_std_2010.spending_score AS int), CAST(lnk_std_2010.annual_income_k AS int), coalesce(lnk_std_2010.country, ''), coalesce(lnk_std_2010.customer_type, ''), coalesce(lnk_std_2010.loyalty_tier, ''), coalesce(lnk_std_2010.email, ''), coalesce(lnk_std_2010.city, ''), lnk_std_2010.snapshot_date, CAST(lnk_std_2010.attr_hash AS bigint)) dedup_rnk
FROM lnk_std_2010) dedup_lnk_dedup_2010 WHERE dedup_rnk = 1""")
lnk_dedup_2010.createOrReplaceTempView("lnk_dedup_2010")


# COMMAND ----------
# Component lnk_dedup_2011, Type UNIQUE_ROW Original node name rmd_Cust_2011, link lnk_dedup_2011
lnk_dedup_2011 = spark.sql(f"""
SELECT
customer_id,
cust_name,
gender,
age,
spending_score,
annual_income_k,
country,
customer_type,
loyalty_tier,
email,
city,
snapshot_date,
attr_hash
FROM (
SELECT
CAST(lnk_std_2011.customer_id AS int) as customer_id,
coalesce(lnk_std_2011.cust_name, '') as cust_name,
coalesce(lnk_std_2011.gender, '') as gender,
CAST(lnk_std_2011.age AS int) as age,
CAST(lnk_std_2011.spending_score AS int) as spending_score,
CAST(lnk_std_2011.annual_income_k AS int) as annual_income_k,
coalesce(lnk_std_2011.country, '') as country,
coalesce(lnk_std_2011.customer_type, '') as customer_type,
coalesce(lnk_std_2011.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_std_2011.email, '') as email,
coalesce(lnk_std_2011.city, '') as city,
lnk_std_2011.snapshot_date as snapshot_date,
CAST(lnk_std_2011.attr_hash AS bigint) as attr_hash,
ROW_NUMBER() OVER(PARTITION BY CAST(lnk_std_2011.customer_id AS int) ORDER BY coalesce(lnk_std_2011.cust_name, ''), coalesce(lnk_std_2011.gender, ''), CAST(lnk_std_2011.age AS int), CAST(lnk_std_2011.spending_score AS int), CAST(lnk_std_2011.annual_income_k AS int), coalesce(lnk_std_2011.country, ''), coalesce(lnk_std_2011.customer_type, ''), coalesce(lnk_std_2011.loyalty_tier, ''), coalesce(lnk_std_2011.email, ''), coalesce(lnk_std_2011.city, ''), lnk_std_2011.snapshot_date, CAST(lnk_std_2011.attr_hash AS bigint)) dedup_rnk
FROM lnk_std_2011) dedup_lnk_dedup_2011 WHERE dedup_rnk = 1""")
lnk_dedup_2011.createOrReplaceTempView("lnk_dedup_2011")


# COMMAND ----------
# Component tgt_stg_customers_2010, Type TARGET 

tgt_stg_customers_2010 = lnk_dedup_2010.select( \
	lnk_dedup_2010.customer_id.cast('int').alias('customer_id'), \
	coalesce(lnk_dedup_2010.cust_name, lit('')).alias('cust_name'), \
	coalesce(lnk_dedup_2010.gender, lit('')).alias('gender'), \
	lnk_dedup_2010.age.cast('int').alias('age'), \
	lnk_dedup_2010.spending_score.cast('int').alias('spending_score'), \
	lnk_dedup_2010.annual_income_k.cast('int').alias('annual_income_k'), \
	coalesce(lnk_dedup_2010.country, lit('')).alias('country'), \
	coalesce(lnk_dedup_2010.customer_type, lit('')).alias('customer_type'), \
	coalesce(lnk_dedup_2010.loyalty_tier, lit('')).alias('loyalty_tier'), \
	coalesce(lnk_dedup_2010.email, lit('')).alias('email'), \
	coalesce(lnk_dedup_2010.city, lit('')).alias('city'), \
	lnk_dedup_2010.snapshot_date.alias('snapshot_date'), \
	lnk_dedup_2010.attr_hash.cast('bigint').alias('attr_hash') \
)
tgt_stg_customers_2010.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.stg_customers_2010')


# COMMAND ----------
# Component tgt_stg_customers_2011, Type TARGET 

tgt_stg_customers_2011 = lnk_dedup_2011.select( \
	lnk_dedup_2011.customer_id.cast('int').alias('customer_id'), \
	coalesce(lnk_dedup_2011.cust_name, lit('')).alias('cust_name'), \
	coalesce(lnk_dedup_2011.gender, lit('')).alias('gender'), \
	lnk_dedup_2011.age.cast('int').alias('age'), \
	lnk_dedup_2011.spending_score.cast('int').alias('spending_score'), \
	lnk_dedup_2011.annual_income_k.cast('int').alias('annual_income_k'), \
	coalesce(lnk_dedup_2011.country, lit('')).alias('country'), \
	coalesce(lnk_dedup_2011.customer_type, lit('')).alias('customer_type'), \
	coalesce(lnk_dedup_2011.loyalty_tier, lit('')).alias('loyalty_tier'), \
	coalesce(lnk_dedup_2011.email, lit('')).alias('email'), \
	coalesce(lnk_dedup_2011.city, lit('')).alias('city'), \
	lnk_dedup_2011.snapshot_date.alias('snapshot_date'), \
	lnk_dedup_2011.attr_hash.cast('bigint').alias('attr_hash') \
)
tgt_stg_customers_2011.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.stg_customers_2011')
