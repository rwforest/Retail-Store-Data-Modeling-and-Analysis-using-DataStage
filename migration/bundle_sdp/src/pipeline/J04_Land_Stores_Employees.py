# Databricks notebook source
# MAGIC %md
# MAGIC # J04_Land_Stores_Employees
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261030063735-t49e/J04_Land_Stores_Employees.txt`
# COMMAND ----------
# COMMAND ----------
from pyspark import pipelines as dp

# Provide a default value so the pipeline does not fail when the "src_dir" config is not set
src_dir = spark.conf.get("src_dir", "/Volumes/main/default/source_data")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# src_stores: Sequential File (columns mapped by position, as DataStage does)
@dp.temporary_view(name="j04_lnk_stores")
def j04_lnk_stores():
    raw = spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/stores.csv")
    assert len(raw.columns) == 6, f"stores.csv: expected 6 columns, got {len(raw.columns)}"
    return raw.toDF('StoreID', 'StoreName', 'Country', 'Channel', 'Rating', 'NoOfEmployees').selectExpr(
        "coalesce(`StoreID`, '') AS StoreID",
        "coalesce(`StoreName`, '') AS StoreName",
        "coalesce(`Country`, '') AS Country",
        "coalesce(`Channel`, '') AS Channel",
        "CAST(`Rating` AS int) AS Rating",
        "CAST(`NoOfEmployees` AS int) AS NoOfEmployees")

# src_employees: Sequential File (columns mapped by position, as DataStage does)
@dp.temporary_view(name="j04_lnk_employees")
def j04_lnk_employees():
    raw = spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/employees.csv")
    assert len(raw.columns) == 5, f"employees.csv: expected 5 columns, got {len(raw.columns)}"
    return raw.toDF('EmployeeID', 'StoreID', 'EmployeeName', 'Role', 'HireDate').selectExpr(
        "coalesce(`EmployeeID`, '') AS EmployeeID",
        "coalesce(`StoreID`, '') AS StoreID",
        "coalesce(`EmployeeName`, '') AS EmployeeName",
        "coalesce(`Role`, '') AS Role",
        "coalesce(`HireDate`, '') AS HireDate")

# xfm_Store_Region: Transformer
@dp.temporary_view(name="j04_lnk_std_store")
def j04_lnk_std_store():
    return spark.sql(f"""
SELECT
  CAST(trim(StoreID) AS string) AS store_id,
  CAST(upper(trim(StoreName)) AS string) AS store_name,
  CAST(trim(Country) AS string) AS country,
  CAST(trim(Channel) AS string) AS channel,
  CAST(Rating AS int) AS rating,
  CAST(NoOfEmployees AS int) AS no_of_employees,
  CAST(CASE WHEN trim(Country) = 'United Kingdom' THEN 'UK' ELSE CASE WHEN trim(Country) = 'EIRE' THEN 'UK&I' ELSE CASE WHEN trim(Channel) = 'ONLINE' THEN 'ONLINE' ELSE 'EUROPE_ROW' END END END AS string) AS region
FROM j04_lnk_stores
""")

# xfm_Emp_Std: Transformer
@dp.temporary_view(name="j04_lnk_std_emp")
def j04_lnk_std_emp():
    return spark.sql(f"""
SELECT
  CAST(trim(EmployeeID) AS string) AS employee_id,
  CAST(trim(StoreID) AS string) AS store_id,
  CAST(upper(trim(EmployeeName)) AS string) AS employee_name,
  CAST(upper(trim(Role)) AS string) AS role,
  CAST(to_date(HireDate, 'yyyy-MM-dd') AS date) AS hire_date,
  CAST(datediff(to_date('{pRunDate}', 'yyyy-MM-dd'), to_date(HireDate, 'yyyy-MM-dd')) / 365 AS int) AS tenure_years
FROM j04_lnk_employees
""")

# cpy_Emp: Copy (output link lnk_emp_out)
@dp.temporary_view(name="j04_lnk_emp_out")
def j04_lnk_emp_out():
    return spark.sql(f"""
SELECT employee_id, store_id, employee_name, role, hire_date, tenure_years
FROM j04_lnk_std_emp
""")

# cpy_Emp: Copy (output link lnk_emp_cnt)
@dp.temporary_view(name="j04_lnk_emp_cnt")
def j04_lnk_emp_cnt():
    return spark.sql(f"""
SELECT employee_id, store_id, employee_name, role, hire_date, tenure_years
FROM j04_lnk_std_emp
""")

# agg_Emp_By_Store: Aggregator
@dp.temporary_view(name="j04_lnk_emp_by_store")
def j04_lnk_emp_by_store():
    return spark.sql(f"""
SELECT store_id, CAST(COUNT(*) AS int) AS emp_cnt
FROM j04_lnk_emp_cnt
GROUP BY store_id
""")

# jn_Store_EmpCount: Join (inner) on store_id
@dp.temporary_view(name="j04_lnk_store_final")
def j04_lnk_store_final():
    return spark.sql(f"""
SELECT
  l.store_id,
  l.store_name,
  l.country,
  l.channel,
  l.rating,
  l.no_of_employees,
  l.region,
  r.emp_cnt
FROM j04_lnk_std_store l
INNER JOIN j04_lnk_emp_by_store r ON l.store_id = r.store_id
""")

# tgt_stg_stores: Sequential File target -> materialized view stg_stores
@dp.materialized_view(name="stg_stores")
def stg_stores():
    return spark.read.table("j04_lnk_store_final").select('store_id', 'store_name', 'country', 'channel', 'rating', 'no_of_employees', 'region', 'emp_cnt')

# tgt_stg_employees: Sequential File target -> materialized view stg_employees
@dp.materialized_view(name="stg_employees")
def stg_employees():
    return spark.read.table("j04_lnk_emp_out").select('employee_id', 'store_id', 'employee_name', 'role', 'hire_date', 'tenure_years')

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
