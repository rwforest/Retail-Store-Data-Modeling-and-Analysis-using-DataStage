CREATE OR REPLACE TEMP VIEW lnk_fact AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.fact_sales
);

CREATE OR REPLACE TEMP VIEW lnk_dim_customer AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_customer
);

CREATE OR REPLACE TEMP VIEW lnk_c AS (
SELECT
coalesce(lnk_fact.invoice_no, '') as invoice_no,
coalesce(lnk_fact.stock_code, '') as stock_code,
CAST(lnk_fact.quantity AS int) as quantity,
lnk_fact.invoice_ts as invoice_ts,
CAST(lnk_fact.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_fact.customer_id AS int) as customer_id,
coalesce(lnk_fact.country, '') as country,
coalesce(lnk_fact.store_id, '') as store_id,
CAST(lnk_fact.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_fact.year_month, '') as year_month,
CAST(lnk_fact.customer_sk AS int) as customer_sk,
coalesce(lnk_fact.customer_type, '') as customer_type,
coalesce(lnk_fact.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_fact.category, '') as category,
CAST(lnk_fact.product_sk AS int) as product_sk,
coalesce(lnk_fact.region, '') as region,
CAST(lnk_fact.store_sk AS int) as store_sk,
CAST(lnk_fact.employee_sk AS int) as employee_sk,
coalesce(lnk_fact.currency, '') as currency,
CAST(lnk_fact.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_fact.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_fact.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_fact.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_fact.line_cnt AS int) as line_cnt,
coalesce(lnk_dim_customer.cust_name, '') as cust_name,
coalesce(lnk_dim_customer.gender, '') as gender,
CAST(lnk_dim_customer.age AS int) as age,
CAST(lnk_dim_customer.spending_score AS int) as spending_score,
CAST(lnk_dim_customer.annual_income_k AS int) as annual_income_k
FROM lnk_fact INNER JOIN lnk_dim_customer ON CAST(lnk_fact.customer_sk AS int) = CAST(lnk_dim_customer.customer_sk AS int)
);

CREATE OR REPLACE TEMP VIEW lnk_dim_product AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_product
);

CREATE OR REPLACE TEMP VIEW lnk_p AS (
SELECT
coalesce(lnk_c.invoice_no, '') as invoice_no,
coalesce(lnk_c.stock_code, '') as stock_code,
CAST(lnk_c.quantity AS int) as quantity,
lnk_c.invoice_ts as invoice_ts,
CAST(lnk_c.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_c.customer_id AS int) as customer_id,
coalesce(lnk_c.country, '') as country,
coalesce(lnk_c.store_id, '') as store_id,
CAST(lnk_c.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_c.year_month, '') as year_month,
CAST(lnk_c.customer_sk AS int) as customer_sk,
coalesce(lnk_c.customer_type, '') as customer_type,
coalesce(lnk_c.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_c.category, '') as category,
CAST(lnk_c.product_sk AS int) as product_sk,
coalesce(lnk_c.region, '') as region,
CAST(lnk_c.store_sk AS int) as store_sk,
CAST(lnk_c.employee_sk AS int) as employee_sk,
coalesce(lnk_c.currency, '') as currency,
CAST(lnk_c.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_c.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_c.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_c.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_c.line_cnt AS int) as line_cnt,
coalesce(lnk_c.cust_name, '') as cust_name,
coalesce(lnk_c.gender, '') as gender,
CAST(lnk_c.age AS int) as age,
CAST(lnk_c.spending_score AS int) as spending_score,
CAST(lnk_c.annual_income_k AS int) as annual_income_k,
coalesce(lnk_dim_product.product_name, '') as product_name,
CAST(lnk_dim_product.list_price AS decimal(18,2)) as list_price
FROM
lnk_c
INNER JOIN lnk_dim_product ON CAST(lnk_c.product_sk AS int) = CAST(lnk_dim_product.product_sk AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_dim_store AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_store
);

CREATE OR REPLACE TEMP VIEW lnk_s AS (
SELECT
coalesce(lnk_p.invoice_no, '') as invoice_no,
coalesce(lnk_p.stock_code, '') as stock_code,
CAST(lnk_p.quantity AS int) as quantity,
lnk_p.invoice_ts as invoice_ts,
CAST(lnk_p.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_p.customer_id AS int) as customer_id,
coalesce(lnk_p.country, '') as country,
coalesce(lnk_p.store_id, '') as store_id,
CAST(lnk_p.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_p.year_month, '') as year_month,
CAST(lnk_p.customer_sk AS int) as customer_sk,
coalesce(lnk_p.customer_type, '') as customer_type,
coalesce(lnk_p.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_p.category, '') as category,
CAST(lnk_p.product_sk AS int) as product_sk,
coalesce(lnk_p.region, '') as region,
CAST(lnk_p.store_sk AS int) as store_sk,
CAST(lnk_p.employee_sk AS int) as employee_sk,
coalesce(lnk_p.currency, '') as currency,
CAST(lnk_p.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_p.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_p.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_p.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_p.line_cnt AS int) as line_cnt,
coalesce(lnk_p.cust_name, '') as cust_name,
coalesce(lnk_p.gender, '') as gender,
CAST(lnk_p.age AS int) as age,
CAST(lnk_p.spending_score AS int) as spending_score,
CAST(lnk_p.annual_income_k AS int) as annual_income_k,
coalesce(lnk_p.product_name, '') as product_name,
CAST(lnk_p.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_dim_store.store_name, '') as store_name,
CAST(lnk_dim_store.rating AS int) as rating,
CAST(lnk_dim_store.no_of_employees AS int) as no_of_employees
FROM
lnk_p
INNER JOIN lnk_dim_store ON CAST(lnk_p.store_sk AS int) = CAST(lnk_dim_store.store_sk AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_types AS (
SELECT
coalesce(lnk_s.invoice_no, '') as invoice_no,
coalesce(lnk_s.stock_code, '') as stock_code,
CAST(lnk_s.quantity AS int) as quantity,
lnk_s.invoice_ts as invoice_ts,
CAST(lnk_s.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_s.customer_id AS int) as customer_id,
coalesce(lnk_s.country, '') as country,
coalesce(lnk_s.store_id, '') as store_id,
CAST(lnk_s.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_s.year_month, '') as year_month,
CAST(lnk_s.customer_sk AS int) as customer_sk,
coalesce(lnk_s.customer_type, '') as customer_type,
coalesce(lnk_s.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_s.category, '') as category,
CAST(lnk_s.product_sk AS int) as product_sk,
coalesce(lnk_s.region, '') as region,
CAST(lnk_s.store_sk AS int) as store_sk,
CAST(lnk_s.employee_sk AS int) as employee_sk,
coalesce(lnk_s.currency, '') as currency,
CAST(lnk_s.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_s.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_s.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_s.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_s.line_cnt AS int) as line_cnt,
coalesce(lnk_s.cust_name, '') as cust_name,
coalesce(lnk_s.gender, '') as gender,
CAST(lnk_s.age AS int) as age,
CAST(lnk_s.spending_score AS int) as spending_score,
CAST(lnk_s.annual_income_k AS int) as annual_income_k,
coalesce(lnk_s.product_name, '') as product_name,
CAST(lnk_s.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_s.store_name, '') as store_name,
CAST(lnk_s.rating AS int) as rating,
CAST(lnk_s.no_of_employees AS int) as no_of_employees
FROM
lnk_s
WHERE customer_type = 'citizen' or customer_type = 'foreign'

);

CREATE OR REPLACE TEMP VIEW lnk_upper AS (
SELECT
coalesce(lnk_types.invoice_no, '') as invoice_no,
coalesce(lnk_types.stock_code, '') as stock_code,
CAST(lnk_types.quantity AS int) as quantity,
lnk_types.invoice_ts as invoice_ts,
CAST(lnk_types.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_types.customer_id AS int) as customer_id,
coalesce(lnk_types.country, '') as country,
coalesce(lnk_types.store_id, '') as store_id,
CAST(lnk_types.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_types.year_month, '') as year_month,
CAST(lnk_types.customer_sk AS int) as customer_sk,
coalesce(lnk_types.customer_type, '') as customer_type,
coalesce(lnk_types.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_types.category, '') as category,
CAST(lnk_types.product_sk AS int) as product_sk,
coalesce(lnk_types.region, '') as region,
CAST(lnk_types.store_sk AS int) as store_sk,
CAST(lnk_types.employee_sk AS int) as employee_sk,
coalesce(lnk_types.currency, '') as currency,
CAST(lnk_types.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_types.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_types.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_types.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_types.line_cnt AS int) as line_cnt,
UPPER(coalesce(lnk_types.cust_name, '')) as cust_name,
coalesce(lnk_types.gender, '') as gender,
CAST(lnk_types.age AS int) as age,
CAST(lnk_types.spending_score AS int) as spending_score,
CAST(lnk_types.annual_income_k AS int) as annual_income_k,
UPPER(coalesce(lnk_types.product_name, '')) as product_name,
CAST(lnk_types.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_types.store_name, '') as store_name,
CAST(lnk_types.rating AS int) as rating,
CAST(lnk_types.no_of_employees AS int) as no_of_employees
FROM
lnk_types

);

CREATE OR REPLACE TEMP VIEW lnk_sorted AS (
SELECT
coalesce(lnk_upper.invoice_no, '') as invoice_no,
coalesce(lnk_upper.stock_code, '') as stock_code,
CAST(lnk_upper.quantity AS int) as quantity,
lnk_upper.invoice_ts as invoice_ts,
CAST(lnk_upper.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_upper.customer_id AS int) as customer_id,
coalesce(lnk_upper.country, '') as country,
coalesce(lnk_upper.store_id, '') as store_id,
CAST(lnk_upper.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_upper.year_month, '') as year_month,
CAST(lnk_upper.customer_sk AS int) as customer_sk,
coalesce(lnk_upper.customer_type, '') as customer_type,
coalesce(lnk_upper.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_upper.category, '') as category,
CAST(lnk_upper.product_sk AS int) as product_sk,
coalesce(lnk_upper.region, '') as region,
CAST(lnk_upper.store_sk AS int) as store_sk,
CAST(lnk_upper.employee_sk AS int) as employee_sk,
coalesce(lnk_upper.currency, '') as currency,
CAST(lnk_upper.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_upper.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_upper.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_upper.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_upper.line_cnt AS int) as line_cnt,
coalesce(lnk_upper.cust_name, '') as cust_name,
coalesce(lnk_upper.gender, '') as gender,
CAST(lnk_upper.age AS int) as age,
CAST(lnk_upper.spending_score AS int) as spending_score,
CAST(lnk_upper.annual_income_k AS int) as annual_income_k,
coalesce(lnk_upper.product_name, '') as product_name,
CAST(lnk_upper.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_upper.store_name, '') as store_name,
CAST(lnk_upper.rating AS int) as rating,
CAST(lnk_upper.no_of_employees AS int) as no_of_employees
FROM
lnk_upper
ORDER BY invoice_no asc, stock_code asc

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.RETAIL_DATA_MART AS
SELECT
  *
FROM lnk_sorted;