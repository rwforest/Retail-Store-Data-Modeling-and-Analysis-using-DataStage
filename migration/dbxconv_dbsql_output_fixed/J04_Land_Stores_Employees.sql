CREATE OR REPLACE TEMP VIEW lnk_stores AS (
  SELECT _c0 AS StoreID, _c1 AS StoreName, _c2 AS Country, _c3 AS Channel, CAST(_c4 AS int) AS Rating, CAST(_c5 AS int) AS NoOfEmployees
  FROM read_files('/Volumes/unitygo/retail_dw/landing/stores.csv', format => 'csv', header => true,
    schema => '_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string', escape => '"')
);

CREATE OR REPLACE TEMP VIEW lnk_employees AS (
  SELECT _c0 AS EmployeeID, _c1 AS StoreID, _c2 AS EmployeeName, _c3 AS Role, _c4 AS HireDate
  FROM read_files('/Volumes/unitygo/retail_dw/landing/employees.csv', format => 'csv', header => true,
    schema => '_c0 string, _c1 string, _c2 string, _c3 string, _c4 string', escape => '"')
);

CREATE OR REPLACE TEMP VIEW lnk_std_store AS (
SELECT
trim(coalesce(lnk_stores.StoreID, '')) as store_id,
UPPER(trim(coalesce(lnk_stores.StoreName, ''))) as store_name,
trim(coalesce(lnk_stores.Country, '')) as country,
trim(coalesce(lnk_stores.Channel, '')) as channel,
CAST(lnk_stores.Rating AS int) as rating,
CAST(lnk_stores.NoOfEmployees AS int) as no_of_employees,
IF ( trim(coalesce(lnk_stores.Country, '')) = 'United Kingdom' , 'UK' , IF ( trim(coalesce(lnk_stores.Country, '')) = 'EIRE' , 'UK&I' , IF ( trim(coalesce(lnk_stores.Channel, '')) = 'ONLINE' , 'ONLINE' , 'EUROPE_ROW' ) ) ) as region
FROM lnk_stores
);

CREATE OR REPLACE TEMP VIEW lnk_std_emp AS (
SELECT
trim(coalesce(lnk_employees.EmployeeID, '')) as employee_id,
trim(coalesce(lnk_employees.StoreID, '')) as store_id,
UPPER(trim(coalesce(lnk_employees.EmployeeName, ''))) as employee_name,
UPPER(trim(coalesce(lnk_employees.Role, ''))) as role,
to_date(cast(coalesce(lnk_employees.HireDate, '') as string), 'yyyy-MM-dd') as hire_date,
DATEDIFF(cast('2011-12-31' as date), to_date(cast(coalesce(lnk_employees.HireDate, '') as string), 'yyyy-MM-dd')) / 365 as tenure_years
FROM lnk_employees
);

CREATE OR REPLACE TEMP VIEW lnk_emp_out AS (
SELECT
coalesce(lnk_std_emp.employee_id, '') as employee_id,
coalesce(lnk_std_emp.store_id, '') as store_id,
coalesce(lnk_std_emp.employee_name, '') as employee_name,
coalesce(lnk_std_emp.role, '') as role,
lnk_std_emp.hire_date as hire_date,
CAST(lnk_std_emp.tenure_years AS int) as tenure_years
FROM
lnk_std_emp

);

CREATE OR REPLACE TEMP VIEW lnk_emp_cnt AS (
SELECT
coalesce(lnk_std_emp.employee_id, '') as employee_id,
coalesce(lnk_std_emp.store_id, '') as store_id,
coalesce(lnk_std_emp.employee_name, '') as employee_name,
coalesce(lnk_std_emp.role, '') as role,
lnk_std_emp.hire_date as hire_date,
CAST(lnk_std_emp.tenure_years AS int) as tenure_years
FROM
lnk_std_emp

);

CREATE OR REPLACE TEMP VIEW lnk_emp_by_store AS (
SELECT
coalesce(lnk_emp_cnt.store_id, '') as store_id,
COUNT(*) as emp_cnt
FROM
lnk_emp_cnt
GROUP BY
coalesce(lnk_emp_cnt.store_id, '')

);

CREATE OR REPLACE TEMP VIEW lnk_store_final AS (
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
INNER JOIN lnk_emp_by_store ON coalesce(lnk_std_store.store_id, '') = coalesce(lnk_emp_by_store.store_id, '')

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.stg_stores AS
SELECT
  *
FROM lnk_store_final;

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.stg_employees AS
SELECT
  *
FROM lnk_emp_out;