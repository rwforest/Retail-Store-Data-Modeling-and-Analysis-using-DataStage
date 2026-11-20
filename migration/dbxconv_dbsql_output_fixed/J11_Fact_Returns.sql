CREATE OR REPLACE TEMP VIEW lnk_orders AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.stg_orders
);

CREATE OR REPLACE TEMP VIEW lnk_cancels AS (
SELECT
coalesce(lnk_orders.invoice_no, '') as invoice_no,
coalesce(lnk_orders.stock_code, '') as stock_code,
coalesce(lnk_orders.description, '') as description,
CAST(lnk_orders.quantity AS int) as quantity,
lnk_orders.invoice_ts as invoice_ts,
CAST(lnk_orders.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_orders.customer_id AS int) as customer_id,
coalesce(lnk_orders.country, '') as country,
coalesce(lnk_orders.store_id, '') as store_id,
coalesce(lnk_orders.employee_id, '') as employee_id,
CAST(lnk_orders.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_orders.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_orders.is_cancel AS int) as is_cancel,
CAST(lnk_orders.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_orders.is_guest AS int) as is_guest
FROM lnk_orders WHERE is_cancel = 1 and is_nonproduct = 0

);

CREATE OR REPLACE TEMP VIEW lnk_ret AS (
SELECT
coalesce(lnk_cancels.invoice_no, '') as invoice_no,
coalesce(lnk_cancels.stock_code, '') as stock_code,
CAST(lnk_cancels.customer_id AS int) as customer_id,
coalesce(lnk_cancels.country, '') as country,
CAST(lnk_cancels.invoice_date_key AS int) as invoice_date_key,
date_format(lnk_cancels.invoice_ts, 'yyyyMM') as year_month,
Abs ( CAST(lnk_cancels.quantity AS int) ) as returned_qty,
Abs ( CAST(lnk_cancels.line_amount AS decimal(18,2)) ) as returned_amount
FROM
lnk_cancels

);

CREATE OR REPLACE TEMP VIEW lnk_dim_product AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_product
);

CREATE OR REPLACE TEMP VIEW lnk_p AS (
SELECT
coalesce(lnk_ret.invoice_no, '') as invoice_no,
coalesce(lnk_ret.stock_code, '') as stock_code,
CAST(lnk_ret.customer_id AS int) as customer_id,
coalesce(lnk_ret.country, '') as country,
CAST(lnk_ret.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_ret.year_month, '') as year_month,
CAST(lnk_ret.returned_qty AS int) as returned_qty,
CAST(lnk_ret.returned_amount AS decimal(18,2)) as returned_amount,
lnk_dim_product.category as category,
CAST(lnk_dim_product.product_sk AS int) as product_sk
FROM
lnk_ret
LEFT JOIN lnk_dim_product ON coalesce(lnk_ret.stock_code, '') = lnk_dim_product.stock_code

);

CREATE OR REPLACE TEMP VIEW lnk_p_nn AS (
SELECT
coalesce(lnk_p.invoice_no, '') as invoice_no,
coalesce(lnk_p.stock_code, '') as stock_code,
CAST(lnk_p.customer_id AS int) as customer_id,
coalesce(lnk_p.country, '') as country,
CAST(lnk_p.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_p.year_month, '') as year_month,
CAST(lnk_p.returned_qty AS int) as returned_qty,
CAST(lnk_p.returned_amount AS decimal(18,2)) as returned_amount,
COALESCE( lnk_p.category , 'Unknown' ) as category,
COALESCE( CAST(lnk_p.product_sk AS int) , - 1 ) as product_sk
FROM
lnk_p

);

CREATE OR REPLACE TEMP VIEW lnk_dim_customer AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_customer
);

CREATE OR REPLACE TEMP VIEW lnk_cur AS (
SELECT
CAST(lnk_dim_customer.customer_sk AS int) as customer_sk,
CAST(lnk_dim_customer.customer_id AS int) as customer_id,
coalesce(lnk_dim_customer.cust_name, '') as cust_name,
coalesce(lnk_dim_customer.gender, '') as gender,
CAST(lnk_dim_customer.age AS int) as age,
CAST(lnk_dim_customer.spending_score AS int) as spending_score,
CAST(lnk_dim_customer.annual_income_k AS int) as annual_income_k,
coalesce(lnk_dim_customer.country, '') as country,
coalesce(lnk_dim_customer.customer_type, '') as customer_type,
coalesce(lnk_dim_customer.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_dim_customer.email, '') as email,
coalesce(lnk_dim_customer.city, '') as city,
CAST(lnk_dim_customer.attr_hash AS bigint) as attr_hash,
CAST(lnk_dim_customer.version_no AS int) as version_no,
CAST(lnk_dim_customer.eff_start_key AS int) as eff_start_key,
CAST(lnk_dim_customer.eff_end_key AS int) as eff_end_key,
CAST(lnk_dim_customer.is_current AS int) as is_current
FROM lnk_dim_customer WHERE is_current = 1

);

CREATE OR REPLACE TEMP VIEW lnk_c AS (
SELECT
coalesce(lnk_p_nn.invoice_no, '') as invoice_no,
coalesce(lnk_p_nn.stock_code, '') as stock_code,
CAST(lnk_p_nn.customer_id AS int) as customer_id,
coalesce(lnk_p_nn.country, '') as country,
CAST(lnk_p_nn.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_p_nn.year_month, '') as year_month,
CAST(lnk_p_nn.returned_qty AS int) as returned_qty,
CAST(lnk_p_nn.returned_amount AS decimal(18,2)) as returned_amount,
coalesce(lnk_p_nn.category, '') as category,
CAST(lnk_p_nn.product_sk AS int) as product_sk,
CAST(lnk_cur.customer_sk AS int) as customer_sk
FROM
lnk_p_nn
LEFT JOIN lnk_cur ON CAST(lnk_p_nn.customer_id AS int) = CAST(lnk_cur.customer_id AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_c_nn AS (
SELECT
coalesce(lnk_c.invoice_no, '') as invoice_no,
coalesce(lnk_c.stock_code, '') as stock_code,
CAST(lnk_c.customer_id AS int) as customer_id,
coalesce(lnk_c.country, '') as country,
CAST(lnk_c.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_c.year_month, '') as year_month,
CAST(lnk_c.returned_qty AS int) as returned_qty,
CAST(lnk_c.returned_amount AS decimal(18,2)) as returned_amount,
coalesce(lnk_c.category, '') as category,
CAST(lnk_c.product_sk AS int) as product_sk,
COALESCE( CAST(lnk_c.customer_sk AS int) , - 1 ) as customer_sk
FROM
lnk_c

);

CREATE OR REPLACE TEMP VIEW lnk_sorted AS (
SELECT
coalesce(lnk_c_nn.invoice_no, '') as invoice_no,
coalesce(lnk_c_nn.stock_code, '') as stock_code,
CAST(lnk_c_nn.customer_id AS int) as customer_id,
coalesce(lnk_c_nn.country, '') as country,
CAST(lnk_c_nn.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_c_nn.year_month, '') as year_month,
CAST(lnk_c_nn.returned_qty AS int) as returned_qty,
CAST(lnk_c_nn.returned_amount AS decimal(18,2)) as returned_amount,
coalesce(lnk_c_nn.category, '') as category,
CAST(lnk_c_nn.product_sk AS int) as product_sk,
CAST(lnk_c_nn.customer_sk AS int) as customer_sk
FROM
lnk_c_nn
ORDER BY invoice_no asc, stock_code asc

);

CREATE OR REPLACE TEMP VIEW lnk_out AS (
SELECT
coalesce(lnk_sorted.invoice_no, '') as invoice_no,
coalesce(lnk_sorted.stock_code, '') as stock_code,
CAST(lnk_sorted.customer_id AS int) as customer_id,
coalesce(lnk_sorted.country, '') as country,
CAST(lnk_sorted.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_sorted.year_month, '') as year_month,
CAST(lnk_sorted.returned_qty AS int) as returned_qty,
CAST(lnk_sorted.returned_amount AS decimal(18,2)) as returned_amount,
coalesce(lnk_sorted.category, '') as category,
CAST(lnk_sorted.product_sk AS int) as product_sk,
CAST(lnk_sorted.customer_sk AS int) as customer_sk
FROM
lnk_sorted

);

CREATE OR REPLACE TEMP VIEW lnk_audit_in AS (
SELECT
coalesce(lnk_sorted.invoice_no, '') as invoice_no,
coalesce(lnk_sorted.stock_code, '') as stock_code,
CAST(lnk_sorted.customer_id AS int) as customer_id,
coalesce(lnk_sorted.country, '') as country,
CAST(lnk_sorted.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_sorted.year_month, '') as year_month,
CAST(lnk_sorted.returned_qty AS int) as returned_qty,
CAST(lnk_sorted.returned_amount AS decimal(18,2)) as returned_amount,
coalesce(lnk_sorted.category, '') as category,
CAST(lnk_sorted.product_sk AS int) as product_sk,
CAST(lnk_sorted.customer_sk AS int) as customer_sk
FROM
lnk_sorted

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.fact_returns AS
SELECT
  *
FROM lnk_out;

CREATE OR REPLACE TEMP VIEW lnk_by_cat AS (
SELECT
coalesce(lnk_audit_in.category, '') as category,
SUM(returned_amount) as returned_amount
FROM
lnk_audit_in
GROUP BY
coalesce(lnk_audit_in.category, '')

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.audit_returns_by_category AS
SELECT
  *
FROM lnk_by_cat;