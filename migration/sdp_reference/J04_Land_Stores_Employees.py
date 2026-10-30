# Databricks notebook source
# J04_Land_Stores_Employees - Land stores + employees, derive region and headcount
# Layer: 1_Land. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_stores: source file stores.csv (columns mapped by position, as DataStage does)
@dp.temporary_view(name="j04_lnk_stores")
def j04_lnk_stores():
    raw = spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/stores.csv")
    assert len(raw.columns) == 6, f"stores.csv: expected 6 columns, got {len(raw.columns)}"
    return raw.toDF('StoreID', 'StoreName', 'Country', 'Channel', 'Rating', 'NoOfEmployees').selectExpr(
        "coalesce(`StoreID`, '') AS StoreID",
        "coalesce(`StoreName`, '') AS StoreName",
        "coalesce(`Country`, '') AS Country",
        "coalesce(`Channel`, '') AS Channel",
        'CAST(`Rating` AS int) AS Rating',
        'CAST(`NoOfEmployees` AS int) AS NoOfEmployees')

# COMMAND ----------
# src_employees: source file employees.csv (columns mapped by position, as DataStage does)
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

# COMMAND ----------
# xfm_Store_Region: Transformer
@dp.temporary_view(name="j04_lnk_std_store")
def j04_lnk_std_store():
    return spark.sql(f"""
SELECT
  CAST(trim(StoreID) AS string) AS store_id,
  CAST(upper(trim(StoreName)) AS string) AS store_name,
  CAST(trim(Country) AS string) AS country,
  CAST(trim(Channel) AS string) AS channel,
  Rating AS rating,
  NoOfEmployees AS no_of_employees,
  CAST(CASE WHEN (trim(Country) = 'United Kingdom') THEN 'UK' ELSE CASE WHEN (trim(Country) = 'EIRE') THEN 'UK&I' ELSE CASE WHEN (trim(Channel) = 'ONLINE') THEN 'ONLINE' ELSE 'EUROPE_ROW' END END END AS string) AS region
FROM j04_lnk_stores
""")

# COMMAND ----------
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
  CAST((datediff(to_date('{pRunDate}', 'yyyy-MM-dd'), to_date(HireDate, 'yyyy-MM-dd')) / 365) AS int) AS tenure_years
FROM j04_lnk_employees
""")

# COMMAND ----------
# cpy_Emp: Copy
@dp.temporary_view(name="j04_lnk_emp_out")
def j04_lnk_emp_out():
    return spark.sql(f"""
SELECT * FROM j04_lnk_std_emp
""")

# COMMAND ----------
# cpy_Emp: Copy
@dp.temporary_view(name="j04_lnk_emp_cnt")
def j04_lnk_emp_cnt():
    return spark.sql(f"""
SELECT * FROM j04_lnk_std_emp
""")

# COMMAND ----------
# agg_Emp_By_Store: Aggregator
@dp.temporary_view(name="j04_lnk_emp_by_store")
def j04_lnk_emp_by_store():
    return spark.sql(f"""
SELECT store_id, CAST(COUNT(*) AS int) AS emp_cnt
FROM j04_lnk_emp_cnt
GROUP BY store_id
""")

# COMMAND ----------
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

# COMMAND ----------
# tgt_stg_stores: target file -> materialized view stg_stores
@dp.materialized_view(name="stg_stores")
def stg_stores():
    return spark.read.table("j04_lnk_store_final").select('store_id', 'store_name', 'country', 'channel', 'rating', 'no_of_employees', 'region', 'emp_cnt')

# COMMAND ----------
# tgt_stg_employees: target file -> materialized view stg_employees
@dp.materialized_view(name="stg_employees")
def stg_employees():
    return spark.read.table("j04_lnk_emp_out").select('employee_id', 'store_id', 'employee_name', 'role', 'hire_date', 'tenure_years')
