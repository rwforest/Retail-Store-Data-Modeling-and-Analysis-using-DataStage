CREATE OR REPLACE TEMP VIEW lnk_orders AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.stg_orders
);

CREATE OR REPLACE TEMP VIEW lnk_lines AS (
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
FROM lnk_orders WHERE is_cancel = 0 and is_nonproduct = 0 and quantity > 0

);

CREATE OR REPLACE TEMP VIEW lnk_k1 AS (
SELECT
CAST(lnk_lines.line_amount AS decimal(18,2)) as line_amount,
'ALL' as rec_key
FROM
lnk_lines

);

CREATE OR REPLACE TEMP VIEW lnk_a1 AS (
SELECT
coalesce(lnk_k1.rec_key, '') as rec_key,
SUM(line_amount) as stage_amount
FROM
lnk_k1
GROUP BY
coalesce(lnk_k1.rec_key, '')

);

CREATE OR REPLACE TEMP VIEW lnk_fact AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.fact_sales
);

CREATE OR REPLACE TEMP VIEW lnk_k2 AS (
SELECT
CAST(lnk_fact.sales_amount AS decimal(18,2)) as sales_amount,
'ALL' as rec_key
FROM lnk_fact
);

CREATE OR REPLACE TEMP VIEW lnk_a2 AS (
SELECT
coalesce(lnk_k2.rec_key, '') as rec_key,
SUM(sales_amount) as fact_amount
FROM
lnk_k2
GROUP BY
coalesce(lnk_k2.rec_key, '')

);

CREATE OR REPLACE TEMP VIEW lnk_rec AS (
SELECT
coalesce(lnk_a1.rec_key, '') as rec_key,
CAST(lnk_a1.stage_amount AS decimal(18,2)) as stage_amount,
CAST(lnk_a2.fact_amount AS decimal(18,2)) as fact_amount
FROM
lnk_a1
INNER JOIN lnk_a2 ON coalesce(lnk_a1.rec_key, '') = coalesce(lnk_a2.rec_key, '')

);

CREATE OR REPLACE TEMP VIEW lnk_diff AS (
SELECT
coalesce(lnk_rec.rec_key, '') as rec_key,
CAST(lnk_rec.stage_amount AS decimal(18,2)) as stage_amount,
CAST(lnk_rec.fact_amount AS decimal(18,2)) as fact_amount,
CAST(lnk_rec.stage_amount AS decimal(18,2)) - CAST(lnk_rec.fact_amount AS decimal(18,2)) as diff_amount,
IF ( Abs ( CAST(lnk_rec.stage_amount AS decimal(18,2)) - CAST(lnk_rec.fact_amount AS decimal(18,2)) ) < 0.01 , 'PASS' , 'FAIL' ) as status
FROM
lnk_rec

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.audit_reconciliation AS
SELECT
  *
FROM lnk_diff;