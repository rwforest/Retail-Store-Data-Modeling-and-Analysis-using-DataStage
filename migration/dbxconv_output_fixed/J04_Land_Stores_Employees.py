# Databricks notebook source
# Code converted on 2026-11-19 16:18:41
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
# Component lnk_employees, Type SOURCE Original node name src_employees, link lnk_employees
lnk_employees = spark.read.format('csv').option('header', 'true').option('escape', '"').load(f'''{pSrcDir}/employees.csv''')
# Conforming fields names to the component layout
lnk_employees_conformed_cols = ["EmployeeID", "StoreID", "EmployeeName", "Role", "HireDate"]
lnk_employees = DatabricksConversionSupplements.conform_df_columns(lnk_employees,lnk_employees_conformed_cols)
lnk_employees.createOrReplaceTempView("lnk_employees")


# COMMAND ----------
# Component lnk_stores, Type SOURCE Original node name src_stores, link lnk_stores
lnk_stores = spark.read.format('csv').option('header', 'true').option('escape', '"').load(f'''{pSrcDir}/stores.csv''')
# Conforming fields names to the component layout
lnk_stores_conformed_cols = ["StoreID", "StoreName", "Country", "Channel", "Rating", "NoOfEmployees"]
lnk_stores = DatabricksConversionSupplements.conform_df_columns(lnk_stores,lnk_stores_conformed_cols)
lnk_stores.createOrReplaceTempView("lnk_stores")


# COMMAND ----------
# Component lnk_std_emp, Type TRANSFORMATION Original node name xfm_Emp_Std, link lnk_std_emp
lnk_std_emp = spark.sql(f"""
SELECT
trim(coalesce(lnk_employees.EmployeeID, '')) as employee_id,
trim(coalesce(lnk_employees.StoreID, '')) as store_id,
UPPER(trim(coalesce(lnk_employees.EmployeeName, ''))) as employee_name,
UPPER(trim(coalesce(lnk_employees.Role, ''))) as role,
to_date(cast(coalesce(lnk_employees.HireDate, '') as string), 'yyyy-MM-dd') as hire_date,
DATEDIFF(cast('{pRunDate}' as date), to_date(cast(coalesce(lnk_employees.HireDate, '') as string), 'yyyy-MM-dd')) / 365 as tenure_years
FROM
lnk_employees""")
lnk_std_emp.createOrReplaceTempView("lnk_std_emp")


# COMMAND ----------
# Component lnk_std_store, Type TRANSFORMATION Original node name xfm_Store_Region, link lnk_std_store
lnk_std_store = spark.sql(f"""
SELECT
trim(coalesce(lnk_stores.StoreID, '')) as store_id,
UPPER(trim(coalesce(lnk_stores.StoreName, ''))) as store_name,
trim(coalesce(lnk_stores.Country, '')) as country,
trim(coalesce(lnk_stores.Channel, '')) as channel,
CAST(lnk_stores.Rating AS int) as rating,
CAST(lnk_stores.NoOfEmployees AS int) as no_of_employees,
IF ( trim(coalesce(lnk_stores.Country, '')) = 'United Kingdom' , 'UK' , IF ( trim(coalesce(lnk_stores.Country, '')) = 'EIRE' , 'UK&I' , IF ( trim(coalesce(lnk_stores.Channel, '')) = 'ONLINE' , 'ONLINE' , 'EUROPE_ROW' ) ) ) as region
FROM
lnk_stores""")
lnk_std_store.createOrReplaceTempView("lnk_std_store")


# COMMAND ----------
# Component lnk_emp_cnt, Type REPLICATE Original node name cpy_Emp, link lnk_emp_cnt
lnk_emp_cnt = spark.sql(f"""
SELECT
coalesce(lnk_std_emp.employee_id, '') as employee_id,
coalesce(lnk_std_emp.store_id, '') as store_id,
coalesce(lnk_std_emp.employee_name, '') as employee_name,
coalesce(lnk_std_emp.role, '') as role,
lnk_std_emp.hire_date as hire_date,
CAST(lnk_std_emp.tenure_years AS int) as tenure_years
FROM
lnk_std_emp""")
lnk_emp_cnt.createOrReplaceTempView("lnk_emp_cnt")


# COMMAND ----------
# Component lnk_emp_out, Type REPLICATE Original node name cpy_Emp, link lnk_emp_out
lnk_emp_out = spark.sql(f"""
SELECT
coalesce(lnk_std_emp.employee_id, '') as employee_id,
coalesce(lnk_std_emp.store_id, '') as store_id,
coalesce(lnk_std_emp.employee_name, '') as employee_name,
coalesce(lnk_std_emp.role, '') as role,
lnk_std_emp.hire_date as hire_date,
CAST(lnk_std_emp.tenure_years AS int) as tenure_years
FROM
lnk_std_emp""")
lnk_emp_out.createOrReplaceTempView("lnk_emp_out")


# COMMAND ----------
# Component tgt_stg_employees, Type TARGET 

tgt_stg_employees = lnk_emp_out.select( \
	coalesce(lnk_emp_out.employee_id, lit('')).alias('employee_id'), \
	coalesce(lnk_emp_out.store_id, lit('')).alias('store_id'), \
	coalesce(lnk_emp_out.employee_name, lit('')).alias('employee_name'), \
	coalesce(lnk_emp_out.role, lit('')).alias('role'), \
	lnk_emp_out.hire_date.alias('hire_date'), \
	lnk_emp_out.tenure_years.cast('int').alias('tenure_years') \
)
tgt_stg_employees.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.stg_employees')


# COMMAND ----------
# Component lnk_emp_by_store, Type AGGREGATOR Original node name agg_Emp_By_Store, link lnk_emp_by_store
lnk_emp_by_store = spark.sql(f"""
SELECT
coalesce(lnk_emp_cnt.store_id, '') as store_id,
COUNT(*) as emp_cnt
FROM
lnk_emp_cnt
GROUP BY
coalesce(lnk_emp_cnt.store_id, '')""")
lnk_emp_by_store.createOrReplaceTempView("lnk_emp_by_store")


# COMMAND ----------
# Component lnk_store_final, Type JOINER Original node name jn_Store_EmpCount, link lnk_store_final
lnk_store_final = spark.sql(f"""
SELECT
coalesce(lnk_std_store.store_id, '') as store_id,
coalesce(lnk_std_store.store_name, '') as store_name,
coalesce(lnk_std_store.country, '') as country,
coalesce(lnk_std_store.channel, '') as channel,
CAST(lnk_std_store.rating AS int) as rating,
CAST(lnk_std_store.no_of_employees AS int) as no_of_employees,
coalesce(lnk_std_store.region, '') as region,
CAST(lnk_emp_by_store.emp_cnt AS int) as emp_cnt
FROM
lnk_std_store
INNER JOIN lnk_emp_by_store ON coalesce(lnk_std_store.store_id, '') = coalesce(lnk_emp_by_store.store_id, '')""")
lnk_store_final.createOrReplaceTempView("lnk_store_final")


# COMMAND ----------
# Component tgt_stg_stores, Type TARGET 

tgt_stg_stores = lnk_store_final.select( \
	coalesce(lnk_store_final.store_id, lit('')).alias('store_id'), \
	coalesce(lnk_store_final.store_name, lit('')).alias('store_name'), \
	coalesce(lnk_store_final.country, lit('')).alias('country'), \
	coalesce(lnk_store_final.channel, lit('')).alias('channel'), \
	lnk_store_final.rating.cast('int').alias('rating'), \
	lnk_store_final.no_of_employees.cast('int').alias('no_of_employees'), \
	coalesce(lnk_store_final.region, lit('')).alias('region'), \
	lnk_store_final.emp_cnt.cast('int').alias('emp_cnt') \
)
tgt_stg_stores.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.stg_stores')
