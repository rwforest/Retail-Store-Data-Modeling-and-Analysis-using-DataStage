# Databricks notebook source
# J08_Dim_Store_Employee - Build Dim_Store and Dim_Employee with surrogate keys
# Layer: 2_Dimensions. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_stg_stores: dataset stg_stores
@dp.temporary_view(name="j08_lnk_stg_store")
def j08_lnk_stg_store():
    return spark.read.table("stg_stores")

# COMMAND ----------
# xfm_Store_SK: Transformer
@dp.temporary_view(name="j08_lnk_dim_store")
def j08_lnk_dim_store():
    return spark.sql(f"""
SELECT
  store_id,
  store_name,
  country,
  channel,
  rating,
  no_of_employees,
  region,
  emp_cnt,
  CAST(row_number() OVER (ORDER BY monotonically_increasing_id()) AS int) AS store_sk
FROM j08_lnk_stg_store
""")

# COMMAND ----------
# cpy_Store: Copy
@dp.temporary_view(name="j08_lnk_store_out")
def j08_lnk_store_out():
    return spark.sql(f"""
SELECT * FROM j08_lnk_dim_store
""")

# COMMAND ----------
# cpy_Store: Copy
@dp.temporary_view(name="j08_lnk_store_keys_in")
def j08_lnk_store_keys_in():
    return spark.sql(f"""
SELECT * FROM j08_lnk_dim_store
""")

# COMMAND ----------
# tgt_dim_store: target file -> materialized view dim_store
@dp.materialized_view(name="dim_store")
def dim_store():
    return spark.read.table("j08_lnk_store_out").select('store_id', 'store_name', 'country', 'channel', 'rating', 'no_of_employees', 'region', 'emp_cnt', 'store_sk')

# COMMAND ----------
# xfm_Store_Keys: Transformer
@dp.temporary_view(name="j08_lnk_store_keys")
def j08_lnk_store_keys():
    return spark.sql(f"""
SELECT
  store_id,
  store_sk
FROM j08_lnk_store_keys_in
""")

# COMMAND ----------
# src_stg_employees: dataset stg_employees
@dp.temporary_view(name="j08_lnk_stg_emp")
def j08_lnk_stg_emp():
    return spark.read.table("stg_employees")

# COMMAND ----------
# jn_Emp_Store: Join (inner) on store_id
@dp.temporary_view(name="j08_lnk_emp_store")
def j08_lnk_emp_store():
    return spark.sql(f"""
SELECT
  l.employee_id,
  l.store_id,
  l.employee_name,
  l.role,
  l.hire_date,
  l.tenure_years,
  r.store_sk
FROM j08_lnk_stg_emp l
INNER JOIN j08_lnk_store_keys r ON l.store_id = r.store_id
""")

# COMMAND ----------
# xfm_Emp_SK: Transformer
@dp.temporary_view(name="j08_lnk_dim_emp")
def j08_lnk_dim_emp():
    return spark.sql(f"""
SELECT
  employee_id,
  store_id,
  employee_name,
  role,
  hire_date,
  tenure_years,
  store_sk,
  CAST(row_number() OVER (ORDER BY monotonically_increasing_id()) AS int) AS employee_sk
FROM j08_lnk_emp_store
""")

# COMMAND ----------
# tgt_dim_employee: target file -> materialized view dim_employee
@dp.materialized_view(name="dim_employee")
def dim_employee():
    return spark.read.table("j08_lnk_dim_emp").select('employee_id', 'store_id', 'employee_name', 'role', 'hire_date', 'tenure_years', 'store_sk', 'employee_sk')
