CREATE OR REPLACE TEMP VIEW lnk_stg_store AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.stg_stores
);

CREATE OR REPLACE TEMP VIEW lnk_dim_store AS (
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
FROM lnk_stg_store
);

CREATE OR REPLACE TEMP VIEW lnk_store_out AS (
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
lnk_dim_store

);

CREATE OR REPLACE TEMP VIEW lnk_store_keys_in AS (
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
lnk_dim_store

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.dim_store AS
SELECT
  *
FROM lnk_store_out;

CREATE OR REPLACE TEMP VIEW lnk_store_keys AS (
SELECT
coalesce(lnk_store_keys_in.store_id, '') as store_id,
CAST(lnk_store_keys_in.store_sk AS int) as store_sk
FROM
lnk_store_keys_in

);

CREATE OR REPLACE TEMP VIEW lnk_stg_emp AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.stg_employees
);

CREATE OR REPLACE TEMP VIEW lnk_emp_store AS (
SELECT
coalesce(lnk_stg_emp.employee_id, '') as employee_id,
coalesce(lnk_stg_emp.store_id, '') as store_id,
coalesce(lnk_stg_emp.employee_name, '') as employee_name,
coalesce(lnk_stg_emp.role, '') as role,
lnk_stg_emp.hire_date as hire_date,
CAST(lnk_stg_emp.tenure_years AS int) as tenure_years,
CAST(lnk_store_keys.store_sk AS int) as store_sk
FROM lnk_stg_emp INNER JOIN lnk_store_keys ON coalesce(lnk_stg_emp.store_id, '') = coalesce(lnk_store_keys.store_id, '')

);

CREATE OR REPLACE TEMP VIEW lnk_dim_emp AS (
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
lnk_emp_store

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.dim_employee AS
SELECT
  *
FROM lnk_dim_emp;