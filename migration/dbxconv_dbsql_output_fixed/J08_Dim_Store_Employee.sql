SELECT
.store_id as store_id,
.store_name as store_name,
.country as country,
.channel as channel,
.rating as rating,
.no_of_employees as no_of_employees,
.region as region,
.emp_cnt as emp_cnt,
row_number() over (order by 1) as store_sk
FROM


SELECT
lnk_dim_store.store_id as store_id,
lnk_dim_store.store_name as store_name,
lnk_dim_store.country as country,
lnk_dim_store.channel as channel,
lnk_dim_store.rating as rating,
lnk_dim_store.no_of_employees as no_of_employees,
lnk_dim_store.region as region,
lnk_dim_store.emp_cnt as emp_cnt,
lnk_dim_store.store_sk as store_sk
FROM
lnk_dim_store

SELECT
lnk_dim_store.store_id as store_id,
lnk_dim_store.store_name as store_name,
lnk_dim_store.country as country,
lnk_dim_store.channel as channel,
lnk_dim_store.rating as rating,
lnk_dim_store.no_of_employees as no_of_employees,
lnk_dim_store.region as region,
lnk_dim_store.emp_cnt as emp_cnt,
lnk_dim_store.store_sk as store_sk
FROM
lnk_dim_store

SELECT CONCAT_WS(',',
lnk_store_out.store_id,
lnk_store_out.store_name,
lnk_store_out.country,
lnk_store_out.channel,
lnk_store_out.rating,
lnk_store_out.no_of_employees,
lnk_store_out.region,
lnk_store_out.emp_cnt,
lnk_store_out.store_sk)
FROM
lnk_store_out



SELECT
lnk_store_keys_in.store_id as store_id,
lnk_store_keys_in.store_sk as store_sk
FROM
lnk_store_keys_in

SELECT
.employee_id as employee_id,
.store_id as store_id,
.employee_name as employee_name,
.role as role,
.hire_date as hire_date,
.tenure_years as tenure_years,
lnk_store_keys.store_sk as store_sk
FROM

INNER JOIN lnk_store_keys ON .store_id = lnk_store_keys.store_id

SELECT
lnk_emp_store.employee_id as employee_id,
lnk_emp_store.store_id as store_id,
lnk_emp_store.employee_name as employee_name,
lnk_emp_store.role as role,
lnk_emp_store.hire_date as hire_date,
lnk_emp_store.tenure_years as tenure_years,
lnk_emp_store.store_sk as store_sk,
row_number() over (order by 1) as employee_sk
FROM
lnk_emp_store

SELECT CONCAT_WS(',',
lnk_dim_emp.employee_id,
lnk_dim_emp.store_id,
lnk_dim_emp.employee_name,
lnk_dim_emp.role,
lnk_dim_emp.hire_date,
lnk_dim_emp.tenure_years,
lnk_dim_emp.store_sk,
lnk_dim_emp.employee_sk)
FROM
lnk_dim_emp

