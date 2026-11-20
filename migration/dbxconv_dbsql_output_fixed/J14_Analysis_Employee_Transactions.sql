CREATE OR REPLACE TEMP VIEW lnk_mart AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.RETAIL_DATA_MART
);

CREATE OR REPLACE TEMP VIEW lnk_invoices AS (
SELECT
invoice_no,
stock_code,
quantity,
invoice_ts,
unit_price,
customer_id,
country,
store_id,
invoice_date_key,
year_month,
customer_sk,
customer_type,
loyalty_tier,
category,
product_sk,
region,
store_sk,
employee_sk,
currency,
sales_amount,
cost_amount,
gross_profit,
local_amount,
line_cnt,
cust_name,
gender,
age,
spending_score,
annual_income_k,
product_name,
list_price,
store_name,
rating,
no_of_employees
FROM (
SELECT
coalesce(lnk_mart.invoice_no, '') as invoice_no,
coalesce(lnk_mart.stock_code, '') as stock_code,
CAST(lnk_mart.quantity AS int) as quantity,
lnk_mart.invoice_ts as invoice_ts,
CAST(lnk_mart.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_mart.customer_id AS int) as customer_id,
coalesce(lnk_mart.country, '') as country,
coalesce(lnk_mart.store_id, '') as store_id,
CAST(lnk_mart.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_mart.year_month, '') as year_month,
CAST(lnk_mart.customer_sk AS int) as customer_sk,
coalesce(lnk_mart.customer_type, '') as customer_type,
coalesce(lnk_mart.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_mart.category, '') as category,
CAST(lnk_mart.product_sk AS int) as product_sk,
coalesce(lnk_mart.region, '') as region,
CAST(lnk_mart.store_sk AS int) as store_sk,
CAST(lnk_mart.employee_sk AS int) as employee_sk,
coalesce(lnk_mart.currency, '') as currency,
CAST(lnk_mart.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_mart.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_mart.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_mart.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_mart.line_cnt AS int) as line_cnt,
coalesce(lnk_mart.cust_name, '') as cust_name,
coalesce(lnk_mart.gender, '') as gender,
CAST(lnk_mart.age AS int) as age,
CAST(lnk_mart.spending_score AS int) as spending_score,
CAST(lnk_mart.annual_income_k AS int) as annual_income_k,
coalesce(lnk_mart.product_name, '') as product_name,
CAST(lnk_mart.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_mart.store_name, '') as store_name,
CAST(lnk_mart.rating AS int) as rating,
CAST(lnk_mart.no_of_employees AS int) as no_of_employees,
ROW_NUMBER() OVER(PARTITION BY coalesce(lnk_mart.invoice_no, ''), CAST(lnk_mart.store_sk AS int), CAST(lnk_mart.employee_sk AS int) ORDER BY coalesce(lnk_mart.stock_code, ''), CAST(lnk_mart.quantity AS int), lnk_mart.invoice_ts, CAST(lnk_mart.unit_price AS decimal(18,2)), CAST(lnk_mart.customer_id AS int), coalesce(lnk_mart.country, ''), coalesce(lnk_mart.store_id, ''), CAST(lnk_mart.invoice_date_key AS int), coalesce(lnk_mart.year_month, ''), CAST(lnk_mart.customer_sk AS int), coalesce(lnk_mart.customer_type, ''), coalesce(lnk_mart.loyalty_tier, ''), coalesce(lnk_mart.category, ''), CAST(lnk_mart.product_sk AS int), coalesce(lnk_mart.region, ''), coalesce(lnk_mart.currency, ''), CAST(lnk_mart.sales_amount AS decimal(18,2)), CAST(lnk_mart.cost_amount AS decimal(18,2)), CAST(lnk_mart.gross_profit AS decimal(18,2)), CAST(lnk_mart.local_amount AS decimal(18,2)), CAST(lnk_mart.line_cnt AS int), coalesce(lnk_mart.cust_name, ''), coalesce(lnk_mart.gender, ''), CAST(lnk_mart.age AS int), CAST(lnk_mart.spending_score AS int), CAST(lnk_mart.annual_income_k AS int), coalesce(lnk_mart.product_name, ''), CAST(lnk_mart.list_price AS decimal(18,2)), coalesce(lnk_mart.store_name, ''), CAST(lnk_mart.rating AS int), CAST(lnk_mart.no_of_employees AS int)) dedup_rnk
FROM lnk_mart) dedup_lnk_invoices WHERE dedup_rnk = 1

);

CREATE OR REPLACE TEMP VIEW lnk_counts AS (
SELECT
CAST(lnk_invoices.store_sk AS int) as store_sk,
CAST(lnk_invoices.employee_sk AS int) as employee_sk,
COUNT(*) as total_transactions
FROM
lnk_invoices
GROUP BY
CAST(lnk_invoices.store_sk AS int),
CAST(lnk_invoices.employee_sk AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_dim_emp AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_employee
);

CREATE OR REPLACE TEMP VIEW lnk_named AS (
SELECT
CAST(lnk_counts.store_sk AS int) as store_sk,
CAST(lnk_counts.employee_sk AS int) as employee_sk,
CAST(lnk_counts.total_transactions AS int) as total_transactions,
coalesce(lnk_dim_emp.employee_id, '') as employee_id,
coalesce(lnk_dim_emp.employee_name, '') as employee_name,
coalesce(lnk_dim_emp.role, '') as role
FROM
lnk_counts
INNER JOIN lnk_dim_emp ON CAST(lnk_counts.employee_sk AS int) = CAST(lnk_dim_emp.employee_sk AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_ranked AS (
SELECT
CAST(lnk_named.store_sk AS int) as store_sk,
CAST(lnk_named.employee_sk AS int) as employee_sk,
CAST(lnk_named.total_transactions AS int) as total_transactions,
coalesce(lnk_named.employee_id, '') as employee_id,
coalesce(lnk_named.employee_name, '') as employee_name,
coalesce(lnk_named.role, '') as role
FROM
lnk_named
ORDER BY store_sk asc, total_transactions desc

);

CREATE OR REPLACE TEMP VIEW lnk_all AS (
SELECT
CAST(lnk_ranked.store_sk AS int) as store_sk,
CAST(lnk_ranked.employee_sk AS int) as employee_sk,
CAST(lnk_ranked.total_transactions AS int) as total_transactions,
coalesce(lnk_ranked.employee_id, '') as employee_id,
coalesce(lnk_ranked.employee_name, '') as employee_name,
coalesce(lnk_ranked.role, '') as role
FROM
lnk_ranked

);

CREATE OR REPLACE TEMP VIEW lnk_top_in AS (
SELECT
CAST(lnk_ranked.store_sk AS int) as store_sk,
CAST(lnk_ranked.employee_sk AS int) as employee_sk,
CAST(lnk_ranked.total_transactions AS int) as total_transactions,
coalesce(lnk_ranked.employee_id, '') as employee_id,
coalesce(lnk_ranked.employee_name, '') as employee_name,
coalesce(lnk_ranked.role, '') as role
FROM
lnk_ranked



);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.Q1_Employee_Transactions AS
SELECT
  *
FROM lnk_all;

CREATE OR REPLACE TEMP VIEW lnk_top10 AS (
  SELECT * FROM lnk_top_in
  ORDER BY store_sk ASC, total_transactions DESC
  LIMIT 10
);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.Q1_Top10_Employees AS
SELECT
  *
FROM lnk_top10;