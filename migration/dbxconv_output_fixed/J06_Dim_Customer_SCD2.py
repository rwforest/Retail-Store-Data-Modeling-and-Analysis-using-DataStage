# Databricks notebook source
# Code converted on 2026-11-19 16:19:38
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
# Component lnk_dim, Type SOURCE Original node name src_dim_customer, link lnk_dim
lnk_dim = spark.read.table(f'{catalog}.{schema}.dim_customer')
lnk_dim.createOrReplaceTempView("lnk_dim")


# COMMAND ----------
# Component lnk_snap, Type SOURCE Original node name src_stg_customers_2011, link lnk_snap
lnk_snap = spark.read.table(f'{catalog}.{schema}.stg_customers_2011')
lnk_snap.createOrReplaceTempView("lnk_snap")


# COMMAND ----------
# Component lnk_dim_a, Type REPLICATE Original node name cpy_Dim, link lnk_dim_a
lnk_dim_a = spark.sql(f"""
SELECT
CAST(lnk_dim.customer_sk AS int) as customer_sk,
CAST(lnk_dim.customer_id AS int) as customer_id,
coalesce(lnk_dim.cust_name, '') as cust_name,
coalesce(lnk_dim.gender, '') as gender,
CAST(lnk_dim.age AS int) as age,
CAST(lnk_dim.spending_score AS int) as spending_score,
CAST(lnk_dim.annual_income_k AS int) as annual_income_k,
coalesce(lnk_dim.country, '') as country,
coalesce(lnk_dim.customer_type, '') as customer_type,
coalesce(lnk_dim.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_dim.email, '') as email,
coalesce(lnk_dim.city, '') as city,
CAST(lnk_dim.attr_hash AS bigint) as attr_hash,
CAST(lnk_dim.version_no AS int) as version_no,
CAST(lnk_dim.eff_start_key AS int) as eff_start_key,
CAST(lnk_dim.eff_end_key AS int) as eff_end_key,
CAST(lnk_dim.is_current AS int) as is_current
FROM
lnk_dim""")
lnk_dim_a.createOrReplaceTempView("lnk_dim_a")


# COMMAND ----------
# Component lnk_dim_b, Type REPLICATE Original node name cpy_Dim, link lnk_dim_b
lnk_dim_b = spark.sql(f"""
SELECT
CAST(lnk_dim.customer_sk AS int) as customer_sk,
CAST(lnk_dim.customer_id AS int) as customer_id,
coalesce(lnk_dim.cust_name, '') as cust_name,
coalesce(lnk_dim.gender, '') as gender,
CAST(lnk_dim.age AS int) as age,
CAST(lnk_dim.spending_score AS int) as spending_score,
CAST(lnk_dim.annual_income_k AS int) as annual_income_k,
coalesce(lnk_dim.country, '') as country,
coalesce(lnk_dim.customer_type, '') as customer_type,
coalesce(lnk_dim.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_dim.email, '') as email,
coalesce(lnk_dim.city, '') as city,
CAST(lnk_dim.attr_hash AS bigint) as attr_hash,
CAST(lnk_dim.version_no AS int) as version_no,
CAST(lnk_dim.eff_start_key AS int) as eff_start_key,
CAST(lnk_dim.eff_end_key AS int) as eff_end_key,
CAST(lnk_dim.is_current AS int) as is_current
FROM
lnk_dim""")
lnk_dim_b.createOrReplaceTempView("lnk_dim_b")


# COMMAND ----------
# Component lnk_current, Type TRANSFORMATION Original node name flt_Current_Rows, link lnk_current
lnk_current = spark.sql(f"""
SELECT
CAST(lnk_dim_a.customer_sk AS int) as customer_sk,
CAST(lnk_dim_a.customer_id AS int) as customer_id,
coalesce(lnk_dim_a.cust_name, '') as cust_name,
coalesce(lnk_dim_a.gender, '') as gender,
CAST(lnk_dim_a.age AS int) as age,
CAST(lnk_dim_a.spending_score AS int) as spending_score,
CAST(lnk_dim_a.annual_income_k AS int) as annual_income_k,
coalesce(lnk_dim_a.country, '') as country,
coalesce(lnk_dim_a.customer_type, '') as customer_type,
coalesce(lnk_dim_a.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_dim_a.email, '') as email,
coalesce(lnk_dim_a.city, '') as city,
CAST(lnk_dim_a.attr_hash AS bigint) as attr_hash,
CAST(lnk_dim_a.version_no AS int) as version_no,
CAST(lnk_dim_a.eff_start_key AS int) as eff_start_key,
CAST(lnk_dim_a.eff_end_key AS int) as eff_end_key,
CAST(lnk_dim_a.is_current AS int) as is_current
FROM
lnk_dim_a
WHERE is_current = 1""")
lnk_current.createOrReplaceTempView("lnk_current")


# COMMAND ----------
# Component lnk_history, Type TRANSFORMATION Original node name flt_History_Rows, link lnk_history
lnk_history = spark.sql(f"""
SELECT
CAST(lnk_dim_b.customer_sk AS int) as customer_sk,
CAST(lnk_dim_b.customer_id AS int) as customer_id,
coalesce(lnk_dim_b.cust_name, '') as cust_name,
coalesce(lnk_dim_b.gender, '') as gender,
CAST(lnk_dim_b.age AS int) as age,
CAST(lnk_dim_b.spending_score AS int) as spending_score,
CAST(lnk_dim_b.annual_income_k AS int) as annual_income_k,
coalesce(lnk_dim_b.country, '') as country,
coalesce(lnk_dim_b.customer_type, '') as customer_type,
coalesce(lnk_dim_b.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_dim_b.email, '') as email,
coalesce(lnk_dim_b.city, '') as city,
CAST(lnk_dim_b.attr_hash AS bigint) as attr_hash,
CAST(lnk_dim_b.version_no AS int) as version_no,
CAST(lnk_dim_b.eff_start_key AS int) as eff_start_key,
CAST(lnk_dim_b.eff_end_key AS int) as eff_end_key,
CAST(lnk_dim_b.is_current AS int) as is_current
FROM
lnk_dim_b
WHERE is_current = 0""")
lnk_history.createOrReplaceTempView("lnk_history")


# COMMAND ----------
# Component lnk_old, Type TRANSFORMATION Original node name xfm_Prefix_Old, link lnk_old
lnk_old = spark.sql(f"""
SELECT
CAST(lnk_current.customer_id AS int) as customer_id,
CAST(lnk_current.customer_sk AS int) as old_customer_sk,
coalesce(lnk_current.cust_name, '') as old_cust_name,
coalesce(lnk_current.gender, '') as old_gender,
CAST(lnk_current.age AS int) as old_age,
CAST(lnk_current.spending_score AS int) as old_spending_score,
CAST(lnk_current.annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_current.country, '') as old_country,
coalesce(lnk_current.customer_type, '') as old_customer_type,
coalesce(lnk_current.loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_current.email, '') as old_email,
coalesce(lnk_current.city, '') as old_city,
CAST(lnk_current.attr_hash AS bigint) as old_attr_hash,
CAST(lnk_current.version_no AS int) as old_version_no,
CAST(lnk_current.eff_start_key AS int) as old_eff_start_key,
CAST(lnk_current.eff_end_key AS int) as old_eff_end_key,
CAST(lnk_current.is_current AS int) as old_is_current
FROM
lnk_current""")
lnk_old.createOrReplaceTempView("lnk_old")


# COMMAND ----------
# Component lnk_cmp, Type JOINER Original node name jn_Snapshot_vs_Current, link lnk_cmp
lnk_cmp = spark.sql(f"""
SELECT
CAST(lnk_snap.customer_id AS int) as customer_id,
coalesce(lnk_snap.cust_name, '') as cust_name,
coalesce(lnk_snap.gender, '') as gender,
CAST(lnk_snap.age AS int) as age,
CAST(lnk_snap.spending_score AS int) as spending_score,
CAST(lnk_snap.annual_income_k AS int) as annual_income_k,
coalesce(lnk_snap.country, '') as country,
coalesce(lnk_snap.customer_type, '') as customer_type,
coalesce(lnk_snap.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_snap.email, '') as email,
coalesce(lnk_snap.city, '') as city,
lnk_snap.snapshot_date as snapshot_date,
CAST(lnk_snap.attr_hash AS bigint) as attr_hash,
CAST(lnk_old.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_old.old_cust_name, '') as old_cust_name,
coalesce(lnk_old.old_gender, '') as old_gender,
CAST(lnk_old.old_age AS int) as old_age,
CAST(lnk_old.old_spending_score AS int) as old_spending_score,
CAST(lnk_old.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_old.old_country, '') as old_country,
coalesce(lnk_old.old_customer_type, '') as old_customer_type,
coalesce(lnk_old.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_old.old_email, '') as old_email,
coalesce(lnk_old.old_city, '') as old_city,
CAST(lnk_old.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_old.old_version_no AS int) as old_version_no,
CAST(lnk_old.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_old.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_old.old_is_current AS int) as old_is_current
FROM
lnk_snap
LEFT JOIN lnk_old ON CAST(lnk_snap.customer_id AS int) = CAST(lnk_old.customer_id AS int)""")
lnk_cmp.createOrReplaceTempView("lnk_cmp")


# COMMAND ----------
# Component lnk_detected, Type TRANSFORMATION Original node name xfm_Detect_Change, link lnk_detected
lnk_detected = spark.sql(f"""
SELECT
CAST(lnk_cmp.customer_id AS int) as customer_id,
coalesce(lnk_cmp.cust_name, '') as cust_name,
coalesce(lnk_cmp.gender, '') as gender,
CAST(lnk_cmp.age AS int) as age,
CAST(lnk_cmp.spending_score AS int) as spending_score,
CAST(lnk_cmp.annual_income_k AS int) as annual_income_k,
coalesce(lnk_cmp.country, '') as country,
coalesce(lnk_cmp.customer_type, '') as customer_type,
coalesce(lnk_cmp.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_cmp.email, '') as email,
coalesce(lnk_cmp.city, '') as city,
lnk_cmp.snapshot_date as snapshot_date,
CAST(lnk_cmp.attr_hash AS bigint) as attr_hash,
 COALESCE(CAST(lnk_cmp.old_customer_sk AS int), 0) as old_customer_sk,
 COALESCE(lnk_cmp.old_cust_name, '') as old_cust_name,
 COALESCE(lnk_cmp.old_gender, '') as old_gender,
 COALESCE(CAST(lnk_cmp.old_age AS int), 0) as old_age,
 COALESCE(CAST(lnk_cmp.old_spending_score AS int), 0) as old_spending_score,
 COALESCE(CAST(lnk_cmp.old_annual_income_k AS int), 0) as old_annual_income_k,
 COALESCE(lnk_cmp.old_country, '') as old_country,
 COALESCE(lnk_cmp.old_customer_type, '') as old_customer_type,
 COALESCE(lnk_cmp.old_loyalty_tier, '') as old_loyalty_tier,
 COALESCE(lnk_cmp.old_email, '') as old_email,
 COALESCE(lnk_cmp.old_city, '') as old_city,
 COALESCE(CAST(lnk_cmp.old_attr_hash AS bigint), 0) as old_attr_hash,
 COALESCE(CAST(lnk_cmp.old_version_no AS int), 0) as old_version_no,
 COALESCE(CAST(lnk_cmp.old_eff_start_key AS int), 0) as old_eff_start_key,
 COALESCE(CAST(lnk_cmp.old_eff_end_key AS int), 0) as old_eff_end_key,
 COALESCE(CAST(lnk_cmp.old_is_current AS int), 0) as old_is_current,
IF ( CAST(lnk_cmp.old_attr_hash AS bigint) IS NULL , 'N' , IF ( CAST(lnk_cmp.old_attr_hash AS bigint) <> CAST(lnk_cmp.attr_hash AS bigint) , 'C' , 'U' ) ) as change_flag
FROM
lnk_cmp""")
lnk_detected.createOrReplaceTempView("lnk_detected")


# COMMAND ----------
# Component lnk_audit, Type REPLICATE Original node name cpy_Detect, link lnk_audit
lnk_audit = spark.sql(f"""
SELECT
CAST(lnk_detected.customer_id AS int) as customer_id,
coalesce(lnk_detected.cust_name, '') as cust_name,
coalesce(lnk_detected.gender, '') as gender,
CAST(lnk_detected.age AS int) as age,
CAST(lnk_detected.spending_score AS int) as spending_score,
CAST(lnk_detected.annual_income_k AS int) as annual_income_k,
coalesce(lnk_detected.country, '') as country,
coalesce(lnk_detected.customer_type, '') as customer_type,
coalesce(lnk_detected.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_detected.email, '') as email,
coalesce(lnk_detected.city, '') as city,
lnk_detected.snapshot_date as snapshot_date,
CAST(lnk_detected.attr_hash AS bigint) as attr_hash,
CAST(lnk_detected.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_detected.old_cust_name, '') as old_cust_name,
coalesce(lnk_detected.old_gender, '') as old_gender,
CAST(lnk_detected.old_age AS int) as old_age,
CAST(lnk_detected.old_spending_score AS int) as old_spending_score,
CAST(lnk_detected.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_detected.old_country, '') as old_country,
coalesce(lnk_detected.old_customer_type, '') as old_customer_type,
coalesce(lnk_detected.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_detected.old_email, '') as old_email,
coalesce(lnk_detected.old_city, '') as old_city,
CAST(lnk_detected.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_detected.old_version_no AS int) as old_version_no,
CAST(lnk_detected.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_detected.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_detected.old_is_current AS int) as old_is_current,
coalesce(lnk_detected.change_flag, '') as change_flag
FROM
lnk_detected""")
lnk_audit.createOrReplaceTempView("lnk_audit")


# COMMAND ----------
# Component lnk_c, Type REPLICATE Original node name cpy_Detect, link lnk_c
lnk_c = spark.sql(f"""
SELECT
CAST(lnk_detected.customer_id AS int) as customer_id,
coalesce(lnk_detected.cust_name, '') as cust_name,
coalesce(lnk_detected.gender, '') as gender,
CAST(lnk_detected.age AS int) as age,
CAST(lnk_detected.spending_score AS int) as spending_score,
CAST(lnk_detected.annual_income_k AS int) as annual_income_k,
coalesce(lnk_detected.country, '') as country,
coalesce(lnk_detected.customer_type, '') as customer_type,
coalesce(lnk_detected.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_detected.email, '') as email,
coalesce(lnk_detected.city, '') as city,
lnk_detected.snapshot_date as snapshot_date,
CAST(lnk_detected.attr_hash AS bigint) as attr_hash,
CAST(lnk_detected.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_detected.old_cust_name, '') as old_cust_name,
coalesce(lnk_detected.old_gender, '') as old_gender,
CAST(lnk_detected.old_age AS int) as old_age,
CAST(lnk_detected.old_spending_score AS int) as old_spending_score,
CAST(lnk_detected.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_detected.old_country, '') as old_country,
coalesce(lnk_detected.old_customer_type, '') as old_customer_type,
coalesce(lnk_detected.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_detected.old_email, '') as old_email,
coalesce(lnk_detected.old_city, '') as old_city,
CAST(lnk_detected.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_detected.old_version_no AS int) as old_version_no,
CAST(lnk_detected.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_detected.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_detected.old_is_current AS int) as old_is_current,
coalesce(lnk_detected.change_flag, '') as change_flag
FROM
lnk_detected""")
lnk_c.createOrReplaceTempView("lnk_c")


# COMMAND ----------
# Component lnk_nc, Type REPLICATE Original node name cpy_Detect, link lnk_nc
lnk_nc = spark.sql(f"""
SELECT
CAST(lnk_detected.customer_id AS int) as customer_id,
coalesce(lnk_detected.cust_name, '') as cust_name,
coalesce(lnk_detected.gender, '') as gender,
CAST(lnk_detected.age AS int) as age,
CAST(lnk_detected.spending_score AS int) as spending_score,
CAST(lnk_detected.annual_income_k AS int) as annual_income_k,
coalesce(lnk_detected.country, '') as country,
coalesce(lnk_detected.customer_type, '') as customer_type,
coalesce(lnk_detected.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_detected.email, '') as email,
coalesce(lnk_detected.city, '') as city,
lnk_detected.snapshot_date as snapshot_date,
CAST(lnk_detected.attr_hash AS bigint) as attr_hash,
CAST(lnk_detected.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_detected.old_cust_name, '') as old_cust_name,
coalesce(lnk_detected.old_gender, '') as old_gender,
CAST(lnk_detected.old_age AS int) as old_age,
CAST(lnk_detected.old_spending_score AS int) as old_spending_score,
CAST(lnk_detected.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_detected.old_country, '') as old_country,
coalesce(lnk_detected.old_customer_type, '') as old_customer_type,
coalesce(lnk_detected.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_detected.old_email, '') as old_email,
coalesce(lnk_detected.old_city, '') as old_city,
CAST(lnk_detected.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_detected.old_version_no AS int) as old_version_no,
CAST(lnk_detected.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_detected.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_detected.old_is_current AS int) as old_is_current,
coalesce(lnk_detected.change_flag, '') as change_flag
FROM
lnk_detected""")
lnk_nc.createOrReplaceTempView("lnk_nc")


# COMMAND ----------
# Component lnk_u, Type REPLICATE Original node name cpy_Detect, link lnk_u
lnk_u = spark.sql(f"""
SELECT
CAST(lnk_detected.customer_id AS int) as customer_id,
coalesce(lnk_detected.cust_name, '') as cust_name,
coalesce(lnk_detected.gender, '') as gender,
CAST(lnk_detected.age AS int) as age,
CAST(lnk_detected.spending_score AS int) as spending_score,
CAST(lnk_detected.annual_income_k AS int) as annual_income_k,
coalesce(lnk_detected.country, '') as country,
coalesce(lnk_detected.customer_type, '') as customer_type,
coalesce(lnk_detected.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_detected.email, '') as email,
coalesce(lnk_detected.city, '') as city,
lnk_detected.snapshot_date as snapshot_date,
CAST(lnk_detected.attr_hash AS bigint) as attr_hash,
CAST(lnk_detected.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_detected.old_cust_name, '') as old_cust_name,
coalesce(lnk_detected.old_gender, '') as old_gender,
CAST(lnk_detected.old_age AS int) as old_age,
CAST(lnk_detected.old_spending_score AS int) as old_spending_score,
CAST(lnk_detected.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_detected.old_country, '') as old_country,
coalesce(lnk_detected.old_customer_type, '') as old_customer_type,
coalesce(lnk_detected.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_detected.old_email, '') as old_email,
coalesce(lnk_detected.old_city, '') as old_city,
CAST(lnk_detected.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_detected.old_version_no AS int) as old_version_no,
CAST(lnk_detected.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_detected.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_detected.old_is_current AS int) as old_is_current,
coalesce(lnk_detected.change_flag, '') as change_flag
FROM
lnk_detected""")
lnk_u.createOrReplaceTempView("lnk_u")


# COMMAND ----------
# Component lnk_changed, Type TRANSFORMATION Original node name flt_Changed, link lnk_changed
lnk_changed = spark.sql(f"""
SELECT
CAST(lnk_c.customer_id AS int) as customer_id,
coalesce(lnk_c.cust_name, '') as cust_name,
coalesce(lnk_c.gender, '') as gender,
CAST(lnk_c.age AS int) as age,
CAST(lnk_c.spending_score AS int) as spending_score,
CAST(lnk_c.annual_income_k AS int) as annual_income_k,
coalesce(lnk_c.country, '') as country,
coalesce(lnk_c.customer_type, '') as customer_type,
coalesce(lnk_c.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_c.email, '') as email,
coalesce(lnk_c.city, '') as city,
lnk_c.snapshot_date as snapshot_date,
CAST(lnk_c.attr_hash AS bigint) as attr_hash,
CAST(lnk_c.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_c.old_cust_name, '') as old_cust_name,
coalesce(lnk_c.old_gender, '') as old_gender,
CAST(lnk_c.old_age AS int) as old_age,
CAST(lnk_c.old_spending_score AS int) as old_spending_score,
CAST(lnk_c.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_c.old_country, '') as old_country,
coalesce(lnk_c.old_customer_type, '') as old_customer_type,
coalesce(lnk_c.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_c.old_email, '') as old_email,
coalesce(lnk_c.old_city, '') as old_city,
CAST(lnk_c.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_c.old_version_no AS int) as old_version_no,
CAST(lnk_c.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_c.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_c.old_is_current AS int) as old_is_current,
coalesce(lnk_c.change_flag, '') as change_flag
FROM
lnk_c
WHERE change_flag = 'C'""")
lnk_changed.createOrReplaceTempView("lnk_changed")


# COMMAND ----------
# Component lnk_new_or_changed, Type TRANSFORMATION Original node name flt_New_Or_Changed, link lnk_new_or_changed
lnk_new_or_changed = spark.sql(f"""
SELECT
CAST(lnk_nc.customer_id AS int) as customer_id,
coalesce(lnk_nc.cust_name, '') as cust_name,
coalesce(lnk_nc.gender, '') as gender,
CAST(lnk_nc.age AS int) as age,
CAST(lnk_nc.spending_score AS int) as spending_score,
CAST(lnk_nc.annual_income_k AS int) as annual_income_k,
coalesce(lnk_nc.country, '') as country,
coalesce(lnk_nc.customer_type, '') as customer_type,
coalesce(lnk_nc.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_nc.email, '') as email,
coalesce(lnk_nc.city, '') as city,
lnk_nc.snapshot_date as snapshot_date,
CAST(lnk_nc.attr_hash AS bigint) as attr_hash,
CAST(lnk_nc.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_nc.old_cust_name, '') as old_cust_name,
coalesce(lnk_nc.old_gender, '') as old_gender,
CAST(lnk_nc.old_age AS int) as old_age,
CAST(lnk_nc.old_spending_score AS int) as old_spending_score,
CAST(lnk_nc.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_nc.old_country, '') as old_country,
coalesce(lnk_nc.old_customer_type, '') as old_customer_type,
coalesce(lnk_nc.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_nc.old_email, '') as old_email,
coalesce(lnk_nc.old_city, '') as old_city,
CAST(lnk_nc.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_nc.old_version_no AS int) as old_version_no,
CAST(lnk_nc.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_nc.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_nc.old_is_current AS int) as old_is_current,
coalesce(lnk_nc.change_flag, '') as change_flag
FROM
lnk_nc
WHERE change_flag <> 'U'""")
lnk_new_or_changed.createOrReplaceTempView("lnk_new_or_changed")


# COMMAND ----------
# Component lnk_unchanged, Type TRANSFORMATION Original node name flt_Unchanged, link lnk_unchanged
lnk_unchanged = spark.sql(f"""
SELECT
CAST(lnk_u.customer_id AS int) as customer_id,
coalesce(lnk_u.cust_name, '') as cust_name,
coalesce(lnk_u.gender, '') as gender,
CAST(lnk_u.age AS int) as age,
CAST(lnk_u.spending_score AS int) as spending_score,
CAST(lnk_u.annual_income_k AS int) as annual_income_k,
coalesce(lnk_u.country, '') as country,
coalesce(lnk_u.customer_type, '') as customer_type,
coalesce(lnk_u.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_u.email, '') as email,
coalesce(lnk_u.city, '') as city,
lnk_u.snapshot_date as snapshot_date,
CAST(lnk_u.attr_hash AS bigint) as attr_hash,
CAST(lnk_u.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_u.old_cust_name, '') as old_cust_name,
coalesce(lnk_u.old_gender, '') as old_gender,
CAST(lnk_u.old_age AS int) as old_age,
CAST(lnk_u.old_spending_score AS int) as old_spending_score,
CAST(lnk_u.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_u.old_country, '') as old_country,
coalesce(lnk_u.old_customer_type, '') as old_customer_type,
coalesce(lnk_u.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_u.old_email, '') as old_email,
coalesce(lnk_u.old_city, '') as old_city,
CAST(lnk_u.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_u.old_version_no AS int) as old_version_no,
CAST(lnk_u.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_u.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_u.old_is_current AS int) as old_is_current,
coalesce(lnk_u.change_flag, '') as change_flag
FROM
lnk_u
WHERE change_flag = 'U'""")
lnk_unchanged.createOrReplaceTempView("lnk_unchanged")


# COMMAND ----------
# Component lnk_carry, Type TRANSFORMATION Original node name xfm_Carry_Unchanged, link lnk_carry
lnk_carry = spark.sql(f"""
SELECT
CAST(lnk_unchanged.old_customer_sk AS int) as customer_sk,
CAST(lnk_unchanged.customer_id AS int) as customer_id,
coalesce(lnk_unchanged.old_cust_name, '') as cust_name,
coalesce(lnk_unchanged.old_gender, '') as gender,
CAST(lnk_unchanged.old_age AS int) as age,
CAST(lnk_unchanged.old_spending_score AS int) as spending_score,
CAST(lnk_unchanged.old_annual_income_k AS int) as annual_income_k,
coalesce(lnk_unchanged.old_country, '') as country,
coalesce(lnk_unchanged.old_customer_type, '') as customer_type,
coalesce(lnk_unchanged.old_loyalty_tier, '') as loyalty_tier,
coalesce(lnk_unchanged.old_email, '') as email,
coalesce(lnk_unchanged.old_city, '') as city,
CAST(lnk_unchanged.old_attr_hash AS bigint) as attr_hash,
CAST(lnk_unchanged.old_version_no AS int) as version_no,
CAST(lnk_unchanged.old_eff_start_key AS int) as eff_start_key,
CAST(lnk_unchanged.old_eff_end_key AS int) as eff_end_key,
CAST(lnk_unchanged.old_is_current AS int) as is_current
FROM
lnk_unchanged""")
lnk_carry.createOrReplaceTempView("lnk_carry")


# COMMAND ----------
# Component lnk_expired, Type TRANSFORMATION Original node name xfm_Expire_Old, link lnk_expired
lnk_expired = spark.sql(f"""
SELECT
CAST(lnk_changed.old_customer_sk AS int) as customer_sk,
CAST(lnk_changed.customer_id AS int) as customer_id,
coalesce(lnk_changed.old_cust_name, '') as cust_name,
coalesce(lnk_changed.old_gender, '') as gender,
CAST(lnk_changed.old_age AS int) as age,
CAST(lnk_changed.old_spending_score AS int) as spending_score,
CAST(lnk_changed.old_annual_income_k AS int) as annual_income_k,
coalesce(lnk_changed.old_country, '') as country,
coalesce(lnk_changed.old_customer_type, '') as customer_type,
coalesce(lnk_changed.old_loyalty_tier, '') as loyalty_tier,
coalesce(lnk_changed.old_email, '') as email,
coalesce(lnk_changed.old_city, '') as city,
CAST(lnk_changed.old_attr_hash AS bigint) as attr_hash,
CAST(lnk_changed.old_version_no AS int) as version_no,
CAST(lnk_changed.old_eff_start_key AS int) as eff_start_key,
CAST(REGEXP_REPLACE(date_format(date_add(cast('{pEffDate}' as date),1), 'yyyy-MM-dd'), '-', '') as int) as eff_end_key,
0 as is_current
FROM
lnk_changed""")
lnk_expired.createOrReplaceTempView("lnk_expired")


# COMMAND ----------
# Component lnk_newver, Type TRANSFORMATION Original node name xfm_New_Version, link lnk_newver
lnk_newver = spark.sql(f"""
SELECT
CAST(lnk_new_or_changed.customer_id AS int) * 1000 + CAST(lnk_new_or_changed.old_version_no AS int) + 1 as customer_sk,
CAST(lnk_new_or_changed.customer_id AS int) as customer_id,
coalesce(lnk_new_or_changed.cust_name, '') as cust_name,
coalesce(lnk_new_or_changed.gender, '') as gender,
CAST(lnk_new_or_changed.age AS int) as age,
CAST(lnk_new_or_changed.spending_score AS int) as spending_score,
CAST(lnk_new_or_changed.annual_income_k AS int) as annual_income_k,
coalesce(lnk_new_or_changed.country, '') as country,
coalesce(lnk_new_or_changed.customer_type, '') as customer_type,
coalesce(lnk_new_or_changed.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_new_or_changed.email, '') as email,
coalesce(lnk_new_or_changed.city, '') as city,
CAST(lnk_new_or_changed.attr_hash AS bigint) as attr_hash,
CAST(lnk_new_or_changed.old_version_no AS int) + 1 as version_no,
IF ( CAST(lnk_new_or_changed.old_version_no AS int) = 0 , 19000101 , CAST(REGEXP_REPLACE('{pEffDate}', '-', '') as int) ) as eff_start_key,
99991231 as eff_end_key,
1 as is_current
FROM
lnk_new_or_changed""")
lnk_newver.createOrReplaceTempView("lnk_newver")


# COMMAND ----------
# Component lnk_change_counts, Type AGGREGATOR Original node name agg_Change_Counts, link lnk_change_counts
lnk_change_counts = spark.sql(f"""
SELECT
coalesce(lnk_audit.change_flag, '') as change_flag,
COUNT(*) as customers
FROM
lnk_audit
GROUP BY
coalesce(lnk_audit.change_flag, '')""")
lnk_change_counts.createOrReplaceTempView("lnk_change_counts")


# COMMAND ----------
# Component lnk_all_versions, Type UNION Original node name fnl_Dim_Next, link lnk_all_versions
lnk_all_versions = spark.sql(f"""
SELECT
CAST(lnk_history.customer_sk AS int) as customer_sk,
CAST(lnk_history.customer_id AS int) as customer_id,
coalesce(lnk_history.cust_name, '') as cust_name,
coalesce(lnk_history.gender, '') as gender,
CAST(lnk_history.age AS int) as age,
CAST(lnk_history.spending_score AS int) as spending_score,
CAST(lnk_history.annual_income_k AS int) as annual_income_k,
coalesce(lnk_history.country, '') as country,
coalesce(lnk_history.customer_type, '') as customer_type,
coalesce(lnk_history.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_history.email, '') as email,
coalesce(lnk_history.city, '') as city,
CAST(lnk_history.attr_hash AS bigint) as attr_hash,
CAST(lnk_history.version_no AS int) as version_no,
CAST(lnk_history.eff_start_key AS int) as eff_start_key,
CAST(lnk_history.eff_end_key AS int) as eff_end_key,
CAST(lnk_history.is_current AS int) as is_current
FROM lnk_history""")
lnk_all_versions.createOrReplaceTempView("lnk_all_versions")


# COMMAND ----------
# Component lnk_sorted, Type SORTER Original node name srt_Dim_Next, link lnk_sorted
lnk_sorted = spark.sql(f"""
SELECT
CAST(lnk_all_versions.customer_sk AS int) as customer_sk,
CAST(lnk_all_versions.customer_id AS int) as customer_id,
coalesce(lnk_all_versions.cust_name, '') as cust_name,
coalesce(lnk_all_versions.gender, '') as gender,
CAST(lnk_all_versions.age AS int) as age,
CAST(lnk_all_versions.spending_score AS int) as spending_score,
CAST(lnk_all_versions.annual_income_k AS int) as annual_income_k,
coalesce(lnk_all_versions.country, '') as country,
coalesce(lnk_all_versions.customer_type, '') as customer_type,
coalesce(lnk_all_versions.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_all_versions.email, '') as email,
coalesce(lnk_all_versions.city, '') as city,
CAST(lnk_all_versions.attr_hash AS bigint) as attr_hash,
CAST(lnk_all_versions.version_no AS int) as version_no,
CAST(lnk_all_versions.eff_start_key AS int) as eff_start_key,
CAST(lnk_all_versions.eff_end_key AS int) as eff_end_key,
CAST(lnk_all_versions.is_current AS int) as is_current
FROM
lnk_all_versions
ORDER BY customer_id asc, version_no asc""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")


# COMMAND ----------
# Component tgt_audit_scd2_customer, Type TARGET 

tgt_audit_scd2_customer = lnk_change_counts.select( \
	coalesce(lnk_change_counts.change_flag, lit('')).alias('change_flag'), \
	lnk_change_counts.customers.cast('int').alias('customers') \
)
tgt_audit_scd2_customer.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.audit_scd2_customer')


# COMMAND ----------
# Component tgt_dim_customer_next, Type TARGET 

tgt_dim_customer_next = lnk_sorted.select( \
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
tgt_dim_customer_next.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.dim_customer_next')
