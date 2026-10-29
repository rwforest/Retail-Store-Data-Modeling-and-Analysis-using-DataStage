# Databricks notebook source
# MAGIC %md
# MAGIC # J04_Land_Stores_Employees
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028071346-rdb5/J04_Land_Stores_Employees.txt`
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
# src_stores: Sequential File (source stores.csv)
_lnk_stores_raw = spark.read.option("header", "true").csv(f"{src_dir}/stores.csv")
_lnk_stores_raw.createOrReplaceTempView("_lnk_stores_raw")
lnk_stores = spark.sql(f"""
SELECT
  CAST(`StoreID` AS string) AS StoreID,
  CAST(`StoreName` AS string) AS StoreName,
  CAST(`Country` AS string) AS Country,
  CAST(`Channel` AS string) AS Channel,
  CAST(`Rating` AS int) AS Rating,
  CAST(`NoOfEmployees` AS int) AS NoOfEmployees
FROM _lnk_stores_raw
""")
lnk_stores.createOrReplaceTempView("lnk_stores")

# COMMAND ----------
# src_employees: Sequential File (source employees.csv)
_lnk_employees_raw = spark.read.option("header", "true").csv(f"{src_dir}/employees.csv")
_lnk_employees_raw.createOrReplaceTempView("_lnk_employees_raw")
lnk_employees = spark.sql(f"""
SELECT
  CAST(`EmployeeID` AS string) AS EmployeeID,
  CAST(`StoreID` AS string) AS StoreID,
  CAST(`EmployeeName` AS string) AS EmployeeName,
  CAST(`Role` AS string) AS Role,
  CAST(`HireDate` AS string) AS HireDate
FROM _lnk_employees_raw
""")
lnk_employees.createOrReplaceTempView("lnk_employees")

# COMMAND ----------
# xfm_Store_Region: Transformer
lnk_std_store = spark.sql(f"""
SELECT
  CAST(trim(StoreID) AS string) AS store_id,
  CAST(upper(trim(StoreName)) AS string) AS store_name,
  CAST(trim(Country) AS string) AS country,
  CAST(trim(Channel) AS string) AS channel,
  Rating AS rating,
  NoOfEmployees AS no_of_employees,
  CAST(CASE WHEN trim(Country) = 'United Kingdom' THEN 'UK'
            ELSE CASE WHEN trim(Country) = 'EIRE' THEN 'UK&I'
                      ELSE CASE WHEN trim(Channel) = 'ONLINE' THEN 'ONLINE'
                                ELSE 'EUROPE_ROW' END END END AS string) AS region
FROM lnk_stores
""")
lnk_std_store.createOrReplaceTempView("lnk_std_store")

# COMMAND ----------
# xfm_Emp_Std: Transformer
lnk_std_emp = spark.sql(f"""
SELECT
  CAST(trim(EmployeeID) AS string) AS employee_id,
  CAST(trim(StoreID) AS string) AS store_id,
  CAST(upper(trim(EmployeeName)) AS string) AS employee_name,
  CAST(upper(trim(Role)) AS string) AS role,
  CAST(to_date(HireDate, 'yyyy-MM-dd') AS date) AS hire_date,
  CAST(datediff(to_date('{pRunDate}', 'yyyy-MM-dd'), to_date(HireDate, 'yyyy-MM-dd')) / 365 AS int) AS tenure_years
FROM lnk_employees
""")
lnk_std_emp.createOrReplaceTempView("lnk_std_emp")

# COMMAND ----------
# cpy_Emp: Copy (output link lnk_emp_out)
lnk_emp_out = spark.sql(f"""
SELECT employee_id, store_id, employee_name, role, hire_date, tenure_years
FROM lnk_std_emp
""")
lnk_emp_out.createOrReplaceTempView("lnk_emp_out")

# COMMAND ----------
# cpy_Emp: Copy (output link lnk_emp_cnt)
lnk_emp_cnt = spark.sql(f"""
SELECT employee_id, store_id, employee_name, role, hire_date, tenure_years
FROM lnk_std_emp
""")
lnk_emp_cnt.createOrReplaceTempView("lnk_emp_cnt")

# COMMAND ----------
# agg_Emp_By_Store: Aggregator (count rows per store_id into emp_cnt)
lnk_emp_by_store = spark.sql(f"""
SELECT
  store_id,
  CAST(COUNT(*) AS int) AS emp_cnt
FROM lnk_emp_cnt
GROUP BY store_id
""")
lnk_emp_by_store.createOrReplaceTempView("lnk_emp_by_store")

# COMMAND ----------
# jn_Store_EmpCount: Join (innerjoin on store_id)
lnk_store_final = spark.sql(f"""
SELECT
  l.store_id,
  l.store_name,
  l.country,
  l.channel,
  l.rating,
  l.no_of_employees,
  l.region,
  r.emp_cnt
FROM lnk_std_store l
INNER JOIN lnk_emp_by_store r ON l.store_id = r.store_id
""")
lnk_store_final.createOrReplaceTempView("lnk_store_final")

# COMMAND ----------
# tgt_stg_stores: Sequential File (target -> Delta table stg_stores, overwrite)
(lnk_store_final
    .select("store_id", "store_name", "country", "channel", "rating", "no_of_employees", "region", "emp_cnt")
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.stg_stores"))
print("stg_stores:", spark.table(f"{catalog}.{schema}.stg_stores").count(), "rows")

# COMMAND ----------
# tgt_stg_employees: Sequential File (target -> Delta table stg_employees, overwrite)
(lnk_emp_out
    .select("employee_id", "store_id", "employee_name", "role", "hire_date", "tenure_years")
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.stg_employees"))
print("stg_employees:", spark.table(f"{catalog}.{schema}.stg_employees").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
