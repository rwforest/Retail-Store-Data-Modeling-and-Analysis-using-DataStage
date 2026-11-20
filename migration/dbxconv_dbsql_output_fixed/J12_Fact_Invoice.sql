CREATE OR REPLACE TEMP VIEW lnk_fact AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.fact_sales
);

CREATE OR REPLACE TEMP VIEW lnk_inv AS (
SELECT
coalesce(lnk_fact.invoice_no, '') as invoice_no,
CAST(lnk_fact.customer_id AS int) as customer_id,
CAST(lnk_fact.customer_sk AS int) as customer_sk,
CAST(lnk_fact.store_sk AS int) as store_sk,
CAST(lnk_fact.employee_sk AS int) as employee_sk,
CAST(lnk_fact.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_fact.year_month, '') as year_month,
SUM(sales_amount) as invoice_sales,
SUM(gross_profit) as invoice_profit,
SUM(quantity) as item_qty,
SUM(line_cnt) as line_total
FROM lnk_fact GROUP BY
coalesce(lnk_fact.invoice_no, ''),
CAST(lnk_fact.customer_id AS int),
CAST(lnk_fact.customer_sk AS int),
CAST(lnk_fact.store_sk AS int),
CAST(lnk_fact.employee_sk AS int),
CAST(lnk_fact.invoice_date_key AS int),
coalesce(lnk_fact.year_month, '')

);

CREATE OR REPLACE TEMP VIEW lnk_basket AS (
SELECT
coalesce(lnk_inv.invoice_no, '') as invoice_no,
CAST(lnk_inv.customer_id AS int) as customer_id,
CAST(lnk_inv.customer_sk AS int) as customer_sk,
CAST(lnk_inv.store_sk AS int) as store_sk,
CAST(lnk_inv.employee_sk AS int) as employee_sk,
CAST(lnk_inv.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_inv.year_month, '') as year_month,
CAST(lnk_inv.invoice_sales AS decimal(18,2)) as invoice_sales,
CAST(lnk_inv.invoice_profit AS decimal(18,2)) as invoice_profit,
CAST(lnk_inv.item_qty AS bigint) as item_qty,
CAST(lnk_inv.line_total AS bigint) as line_total,
IF ( CAST(lnk_inv.line_total AS bigint) > 0 , CAST(lnk_inv.invoice_sales AS decimal(18,2)) / CAST(lnk_inv.line_total AS bigint) , 0 ) as avg_line_value,
1 as invoice_cnt
FROM
lnk_inv

);

CREATE OR REPLACE TEMP VIEW lnk_sorted AS (
SELECT
coalesce(lnk_basket.invoice_no, '') as invoice_no,
CAST(lnk_basket.customer_id AS int) as customer_id,
CAST(lnk_basket.customer_sk AS int) as customer_sk,
CAST(lnk_basket.store_sk AS int) as store_sk,
CAST(lnk_basket.employee_sk AS int) as employee_sk,
CAST(lnk_basket.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_basket.year_month, '') as year_month,
CAST(lnk_basket.invoice_sales AS decimal(18,2)) as invoice_sales,
CAST(lnk_basket.invoice_profit AS decimal(18,2)) as invoice_profit,
CAST(lnk_basket.item_qty AS bigint) as item_qty,
CAST(lnk_basket.line_total AS bigint) as line_total,
CAST(lnk_basket.avg_line_value AS decimal(18,2)) as avg_line_value,
CAST(lnk_basket.invoice_cnt AS int) as invoice_cnt
FROM
lnk_basket
ORDER BY invoice_no asc

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.fact_invoice AS
SELECT
  *
FROM lnk_sorted;