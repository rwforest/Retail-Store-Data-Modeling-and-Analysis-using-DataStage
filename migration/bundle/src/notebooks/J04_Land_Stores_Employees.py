# Databricks notebook source
# J04_Land_Stores_Employees - Land stores + employees, derive region and headcount
# Layer: 1_Land. Compiled from the DataStage job by migration/ds2spark.py; do not edit by hand.

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
# src_stores: source file stores.csv (columns mapped by position, as DataStage does)
_lnk_stores_raw = (spark.read.option("header", "true").option("escape", '"')
    .csv(f"{src_dir}/stores.csv"))
assert len(_lnk_stores_raw.columns) == 6, f"stores.csv: expected 6 columns, got {len(_lnk_stores_raw.columns)}"
_lnk_stores_raw = _lnk_stores_raw.toDF('StoreID', 'StoreName', 'Country', 'Channel', 'Rating', 'NoOfEmployees')
_lnk_stores_raw.createOrReplaceTempView("_lnk_stores_raw")
lnk_stores = spark.sql(f"""SELECT
  coalesce(`StoreID`, '') AS StoreID,
  coalesce(`StoreName`, '') AS StoreName,
  coalesce(`Country`, '') AS Country,
  coalesce(`Channel`, '') AS Channel,
  CAST(`Rating` AS int) AS Rating,
  CAST(`NoOfEmployees` AS int) AS NoOfEmployees
FROM _lnk_stores_raw""")
lnk_stores.createOrReplaceTempView("lnk_stores")

# COMMAND ----------
# src_employees: source file employees.csv (columns mapped by position, as DataStage does)
_lnk_employees_raw = (spark.read.option("header", "true").option("escape", '"')
    .csv(f"{src_dir}/employees.csv"))
assert len(_lnk_employees_raw.columns) == 5, f"employees.csv: expected 5 columns, got {len(_lnk_employees_raw.columns)}"
_lnk_employees_raw = _lnk_employees_raw.toDF('EmployeeID', 'StoreID', 'EmployeeName', 'Role', 'HireDate')
_lnk_employees_raw.createOrReplaceTempView("_lnk_employees_raw")
lnk_employees = spark.sql(f"""SELECT
  coalesce(`EmployeeID`, '') AS EmployeeID,
  coalesce(`StoreID`, '') AS StoreID,
  coalesce(`EmployeeName`, '') AS EmployeeName,
  coalesce(`Role`, '') AS Role,
  coalesce(`HireDate`, '') AS HireDate
FROM _lnk_employees_raw""")
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
  CAST(CASE WHEN (trim(Country) = 'United Kingdom') THEN 'UK' ELSE CASE WHEN (trim(Country) = 'EIRE') THEN 'UK&I' ELSE CASE WHEN (trim(Channel) = 'ONLINE') THEN 'ONLINE' ELSE 'EUROPE_ROW' END END END AS string) AS region
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
  CAST((datediff(to_date('{pRunDate}', 'yyyy-MM-dd'), to_date(HireDate, 'yyyy-MM-dd')) / 365) AS int) AS tenure_years
FROM lnk_employees
""")
lnk_std_emp.createOrReplaceTempView("lnk_std_emp")

# COMMAND ----------
# cpy_Emp: Copy
lnk_emp_out = spark.sql(f"""
SELECT * FROM lnk_std_emp
""")
lnk_emp_out.createOrReplaceTempView("lnk_emp_out")

# COMMAND ----------
# cpy_Emp: Copy
lnk_emp_cnt = spark.sql(f"""
SELECT * FROM lnk_std_emp
""")
lnk_emp_cnt.createOrReplaceTempView("lnk_emp_cnt")

# COMMAND ----------
# agg_Emp_By_Store: Aggregator
lnk_emp_by_store = spark.sql(f"""
SELECT store_id, CAST(COUNT(*) AS int) AS emp_cnt
FROM lnk_emp_cnt
GROUP BY store_id
""")
lnk_emp_by_store.createOrReplaceTempView("lnk_emp_by_store")

# COMMAND ----------
# jn_Store_EmpCount: Join (inner) on store_id
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
# tgt_stg_stores: write Delta table stg_stores
(lnk_store_final.select('store_id', 'store_name', 'country', 'channel', 'rating', 'no_of_employees', 'region', 'emp_cnt').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.stg_stores"))
print("stg_stores:", spark.table(f"{catalog}.{schema}.stg_stores").count(), "rows")

# COMMAND ----------
# tgt_stg_employees: write Delta table stg_employees
(lnk_emp_out.select('employee_id', 'store_id', 'employee_name', 'role', 'hire_date', 'tenure_years').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.stg_employees"))
print("stg_employees:", spark.table(f"{catalog}.{schema}.stg_employees").count(), "rows")
