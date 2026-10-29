# Databricks notebook source
# MAGIC %md
# MAGIC # J08_Dim_Store_Employee
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261029064410-juws/J08_Dim_Store_Employee.txt`
# COMMAND ----------
# COMMAND ----------
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "retail_dw")
dbutils.widgets.text("src_dir", "")
dbutils.widgets.text("pRunDate", "2011-12-31")
dbutils.widgets.text("pEffDate", "2011-07-01")
catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))
pRunDate, pEffDate = dbutils.widgets.get("pRunDate"), dbutils.widgets.get("pEffDate")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------
# src_stg_stores: Sequential File source (Delta table stg_stores)
lnk_stg_store = spark.table(f"{catalog}.{schema}.stg_stores")
lnk_stg_store.createOrReplaceTempView("lnk_stg_store")

# COMMAND ----------
# xfm_Store_SK: Transformer (surrogate key via row_number; no upstream Sort, so monotonically_increasing_id())
lnk_dim_store = spark.sql(f"""
SELECT
  CAST(store_id AS string) AS store_id,
  CAST(store_name AS string) AS store_name,
  CAST(country AS string) AS country,
  CAST(channel AS string) AS channel,
  CAST(rating AS int) AS rating,
  CAST(no_of_employees AS int) AS no_of_employees,
  CAST(region AS string) AS region,
  CAST(emp_cnt AS int) AS emp_cnt,
  CAST(row_number() OVER (ORDER BY monotonically_increasing_id()) AS int) AS store_sk
FROM lnk_stg_store
""")
lnk_dim_store.createOrReplaceTempView("lnk_dim_store")

# COMMAND ----------
# cpy_Store: Copy (output link lnk_store_out)
lnk_store_out = spark.sql(f"""
SELECT store_id, store_name, country, channel, rating, no_of_employees, region, emp_cnt, store_sk
FROM lnk_dim_store
""")
lnk_store_out.createOrReplaceTempView("lnk_store_out")

# COMMAND ----------
# cpy_Store: Copy (output link lnk_store_keys_in)
lnk_store_keys_in = spark.sql(f"""
SELECT store_id, store_name, country, channel, rating, no_of_employees, region, emp_cnt, store_sk
FROM lnk_dim_store
""")
lnk_store_keys_in.createOrReplaceTempView("lnk_store_keys_in")

# COMMAND ----------
# tgt_dim_store: Sequential File target (Delta table dim_store)
(lnk_store_out.select('store_id', 'store_name', 'country', 'channel', 'rating', 'no_of_employees', 'region', 'emp_cnt', 'store_sk').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_store"))
print("dim_store:", spark.table(f"{catalog}.{schema}.dim_store").count(), "rows")

# COMMAND ----------
# xfm_Store_Keys: Transformer
lnk_store_keys = spark.sql(f"""
SELECT
  CAST(store_id AS string) AS store_id,
  CAST(store_sk AS int) AS store_sk
FROM lnk_store_keys_in
""")
lnk_store_keys.createOrReplaceTempView("lnk_store_keys")

# COMMAND ----------
# src_stg_employees: Sequential File source (Delta table stg_employees)
lnk_stg_emp = spark.table(f"{catalog}.{schema}.stg_employees")
lnk_stg_emp.createOrReplaceTempView("lnk_stg_emp")

# COMMAND ----------
# jn_Store_Emp / jn_Emp_Store: Join (inner) on store_id
lnk_emp_store = spark.sql(f"""
SELECT
  l.employee_id,
  l.store_id,
  l.employee_name,
  l.role,
  l.hire_date,
  l.tenure_years,
  r.store_sk
FROM lnk_stg_emp l
INNER JOIN lnk_store_keys r ON l.store_id = r.store_id
""")
lnk_emp_store.createOrReplaceTempView("lnk_emp_store")

# COMMAND ----------
# xfm_Emp_SK: Transformer (surrogate key via row_number; the Join breaks any order, so monotonically_increasing_id())
lnk_dim_emp = spark.sql(f"""
SELECT
  CAST(employee_id AS string) AS employee_id,
  CAST(store_id AS string) AS store_id,
  CAST(employee_name AS string) AS employee_name,
  CAST(role AS string) AS role,
  CAST(hire_date AS date) AS hire_date,
  CAST(tenure_years AS int) AS tenure_years,
  CAST(store_sk AS int) AS store_sk,
  CAST(row_number() OVER (ORDER BY monotonically_increasing_id()) AS int) AS employee_sk
FROM lnk_emp_store
""")
lnk_dim_emp.createOrReplaceTempView("lnk_dim_emp")

# COMMAND ----------
# tgt_dim_employee: Sequential File target (Delta table dim_employee)
(lnk_dim_emp.select('employee_id', 'store_id', 'employee_name', 'role', 'hire_date', 'tenure_years', 'store_sk', 'employee_sk').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_employee"))
print("dim_employee:", spark.table(f"{catalog}.{schema}.dim_employee").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
