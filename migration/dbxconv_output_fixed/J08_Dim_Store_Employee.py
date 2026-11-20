# Databricks notebook source
# Code converted on 2026-11-19 16:20:11
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
# Component lnk_stg_emp, Type SOURCE Original node name src_stg_employees, link lnk_stg_emp
lnk_stg_emp = spark.read.table(f'{catalog}.{schema}.stg_employees')
lnk_stg_emp.createOrReplaceTempView("lnk_stg_emp")


# COMMAND ----------
# Component lnk_stg_store, Type SOURCE Original node name src_stg_stores, link lnk_stg_store
lnk_stg_store = spark.read.table(f'{catalog}.{schema}.stg_stores')
lnk_stg_store.createOrReplaceTempView("lnk_stg_store")


# COMMAND ----------
# Component lnk_dim_store, Type TRANSFORMATION Original node name xfm_Store_SK, link lnk_dim_store
lnk_dim_store = spark.sql(f"""
SELECT
coalesce(lnk_stg_store.store_id, '') as store_id,
coalesce(lnk_stg_store.store_name, '') as store_name,
coalesce(lnk_stg_store.country, '') as country,
coalesce(lnk_stg_store.channel, '') as channel,
CAST(lnk_stg_store.rating AS int) as rating,
CAST(lnk_stg_store.no_of_employees AS int) as no_of_employees,
coalesce(lnk_stg_store.region, '') as region,
CAST(lnk_stg_store.emp_cnt AS int) as emp_cnt,
row_number() over (order by 1) as store_sk
FROM
lnk_stg_store""")
lnk_dim_store.createOrReplaceTempView("lnk_dim_store")


# COMMAND ----------
# Component lnk_store_out, Type REPLICATE Original node name cpy_Store, link lnk_store_out
lnk_store_out = spark.sql(f"""
SELECT
coalesce(lnk_dim_store.store_id, '') as store_id,
coalesce(lnk_dim_store.store_name, '') as store_name,
coalesce(lnk_dim_store.country, '') as country,
coalesce(lnk_dim_store.channel, '') as channel,
CAST(lnk_dim_store.rating AS int) as rating,
CAST(lnk_dim_store.no_of_employees AS int) as no_of_employees,
coalesce(lnk_dim_store.region, '') as region,
CAST(lnk_dim_store.emp_cnt AS int) as emp_cnt,
CAST(lnk_dim_store.store_sk AS int) as store_sk
FROM
lnk_dim_store""")
lnk_store_out.createOrReplaceTempView("lnk_store_out")


# COMMAND ----------
# Component lnk_store_keys_in, Type REPLICATE Original node name cpy_Store, link lnk_store_keys_in
lnk_store_keys_in = spark.sql(f"""
SELECT
coalesce(lnk_dim_store.store_id, '') as store_id,
coalesce(lnk_dim_store.store_name, '') as store_name,
coalesce(lnk_dim_store.country, '') as country,
coalesce(lnk_dim_store.channel, '') as channel,
CAST(lnk_dim_store.rating AS int) as rating,
CAST(lnk_dim_store.no_of_employees AS int) as no_of_employees,
coalesce(lnk_dim_store.region, '') as region,
CAST(lnk_dim_store.emp_cnt AS int) as emp_cnt,
CAST(lnk_dim_store.store_sk AS int) as store_sk
FROM
lnk_dim_store""")
lnk_store_keys_in.createOrReplaceTempView("lnk_store_keys_in")


# COMMAND ----------
# Component tgt_dim_store, Type TARGET 

tgt_dim_store = lnk_store_out.select( \
	coalesce(lnk_store_out.store_id, lit('')).alias('store_id'), \
	coalesce(lnk_store_out.store_name, lit('')).alias('store_name'), \
	coalesce(lnk_store_out.country, lit('')).alias('country'), \
	coalesce(lnk_store_out.channel, lit('')).alias('channel'), \
	lnk_store_out.rating.cast('int').alias('rating'), \
	lnk_store_out.no_of_employees.cast('int').alias('no_of_employees'), \
	coalesce(lnk_store_out.region, lit('')).alias('region'), \
	lnk_store_out.emp_cnt.cast('int').alias('emp_cnt'), \
	lnk_store_out.store_sk.cast('int').alias('store_sk') \
)
tgt_dim_store.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.dim_store')


# COMMAND ----------
# Component lnk_store_keys, Type TRANSFORMATION Original node name xfm_Store_Keys, link lnk_store_keys
lnk_store_keys = spark.sql(f"""
SELECT
coalesce(lnk_store_keys_in.store_id, '') as store_id,
CAST(lnk_store_keys_in.store_sk AS int) as store_sk
FROM
lnk_store_keys_in""")
lnk_store_keys.createOrReplaceTempView("lnk_store_keys")


# COMMAND ----------
# Component lnk_emp_store, Type JOINER Original node name jn_Emp_Store, link lnk_emp_store
lnk_emp_store = spark.sql(f"""
SELECT
coalesce(lnk_stg_emp.employee_id, '') as employee_id,
coalesce(lnk_stg_emp.store_id, '') as store_id,
coalesce(lnk_stg_emp.employee_name, '') as employee_name,
coalesce(lnk_stg_emp.role, '') as role,
lnk_stg_emp.hire_date as hire_date,
CAST(lnk_stg_emp.tenure_years AS int) as tenure_years,
CAST(lnk_store_keys.store_sk AS int) as store_sk
FROM
lnk_stg_emp
INNER JOIN lnk_store_keys ON coalesce(lnk_stg_emp.store_id, '') = coalesce(lnk_store_keys.store_id, '')""")
lnk_emp_store.createOrReplaceTempView("lnk_emp_store")


# COMMAND ----------
# Component lnk_dim_emp, Type TRANSFORMATION Original node name xfm_Emp_SK, link lnk_dim_emp
lnk_dim_emp = spark.sql(f"""
SELECT
coalesce(lnk_emp_store.employee_id, '') as employee_id,
coalesce(lnk_emp_store.store_id, '') as store_id,
coalesce(lnk_emp_store.employee_name, '') as employee_name,
coalesce(lnk_emp_store.role, '') as role,
lnk_emp_store.hire_date as hire_date,
CAST(lnk_emp_store.tenure_years AS int) as tenure_years,
CAST(lnk_emp_store.store_sk AS int) as store_sk,
row_number() over (order by 1) as employee_sk
FROM
lnk_emp_store""")
lnk_dim_emp.createOrReplaceTempView("lnk_dim_emp")


# COMMAND ----------
# Component tgt_dim_employee, Type TARGET 

tgt_dim_employee = lnk_dim_emp.select( \
	coalesce(lnk_dim_emp.employee_id, lit('')).alias('employee_id'), \
	coalesce(lnk_dim_emp.store_id, lit('')).alias('store_id'), \
	coalesce(lnk_dim_emp.employee_name, lit('')).alias('employee_name'), \
	coalesce(lnk_dim_emp.role, lit('')).alias('role'), \
	lnk_dim_emp.hire_date.alias('hire_date'), \
	lnk_dim_emp.tenure_years.cast('int').alias('tenure_years'), \
	lnk_dim_emp.store_sk.cast('int').alias('store_sk'), \
	lnk_dim_emp.employee_sk.cast('int').alias('employee_sk') \
)
tgt_dim_employee.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.dim_employee')
