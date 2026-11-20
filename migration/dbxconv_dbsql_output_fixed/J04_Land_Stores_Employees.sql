SELECT
trim(.EmployeeID) as employee_id,
trim(.StoreID) as store_id,
UPPER(trim(.EmployeeName)) as employee_name,
UPPER(trim(.Role)) as role,
cast(.HireDate as date) as hire_date,
DATEDIFF(cast('{pRunDate}' as date), cast(.HireDate as date)) / 365 as tenure_years
FROM


SELECT
trim(.StoreID) as store_id,
UPPER(trim(.StoreName)) as store_name,
trim(.Country) as country,
trim(.Channel) as channel,
.Rating as rating,
.NoOfEmployees as no_of_employees,
IF ( trim(.Country) = 'United Kingdom' , 'UK' , IF ( trim(.Country) = 'EIRE' , 'UK&I' , IF ( trim(.Channel) = 'ONLINE' , 'ONLINE' , 'EUROPE_ROW' ) ) ) as region
FROM


SELECT
lnk_std_emp.employee_id as employee_id,
lnk_std_emp.store_id as store_id,
lnk_std_emp.employee_name as employee_name,
lnk_std_emp.role as role,
lnk_std_emp.hire_date as hire_date,
lnk_std_emp.tenure_years as tenure_years
FROM
lnk_std_emp

SELECT
lnk_std_emp.employee_id as employee_id,
lnk_std_emp.store_id as store_id,
lnk_std_emp.employee_name as employee_name,
lnk_std_emp.role as role,
lnk_std_emp.hire_date as hire_date,
lnk_std_emp.tenure_years as tenure_years
FROM
lnk_std_emp

SELECT CONCAT_WS(',',
lnk_emp_out.employee_id,
lnk_emp_out.store_id,
lnk_emp_out.employee_name,
lnk_emp_out.role,
lnk_emp_out.hire_date,
lnk_emp_out.tenure_years)
FROM
lnk_emp_out



SELECT
lnk_emp_cnt.store_id as store_id,
MIN(emp_cnt) as emp_cnt
FROM
lnk_emp_cnt
GROUP BY
lnk_emp_cnt.store_id

SELECT
lnk_std_store.store_id as store_id,
lnk_std_store.store_name as store_name,
lnk_std_store.country as country,
lnk_std_store.channel as channel,
lnk_std_store.rating as rating,
lnk_std_store.no_of_employees as no_of_employees,
lnk_std_store.region as region,
lnk_emp_by_store.emp_cnt as emp_cnt
FROM
lnk_std_store
INNER JOIN lnk_emp_by_store ON lnk_std_store.store_id = lnk_emp_by_store.store_id

SELECT CONCAT_WS(',',
lnk_store_final.store_id,
lnk_store_final.store_name,
lnk_store_final.country,
lnk_store_final.channel,
lnk_store_final.rating,
lnk_store_final.no_of_employees,
lnk_store_final.region,
lnk_store_final.emp_cnt)
FROM
lnk_store_final

