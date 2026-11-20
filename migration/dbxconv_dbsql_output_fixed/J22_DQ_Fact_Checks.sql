CREATE OR REPLACE TEMP VIEW lnk_fact AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.fact_sales
);

CREATE OR REPLACE TEMP VIEW lnk_chk1 AS (
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
CAST(lnk_fact.line_cnt AS int) as line_cnt
FROM lnk_fact
);

CREATE OR REPLACE TEMP VIEW lnk_chk2 AS (
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
CAST(lnk_fact.line_cnt AS int) as line_cnt
FROM lnk_fact
);

CREATE OR REPLACE TEMP VIEW lnk_chk3 AS (
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
CAST(lnk_fact.line_cnt AS int) as line_cnt
FROM lnk_fact
);

CREATE OR REPLACE TEMP VIEW lnk_chk4 AS (
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
CAST(lnk_fact.line_cnt AS int) as line_cnt
FROM lnk_fact
);

CREATE OR REPLACE TEMP VIEW lnk_hit1 AS (
SELECT
coalesce(lnk_chk1.invoice_no, '') as invoice_no,
coalesce(lnk_chk1.stock_code, '') as stock_code,
CAST(lnk_chk1.quantity AS int) as quantity,
lnk_chk1.invoice_ts as invoice_ts,
CAST(lnk_chk1.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_chk1.customer_id AS int) as customer_id,
coalesce(lnk_chk1.country, '') as country,
coalesce(lnk_chk1.store_id, '') as store_id,
CAST(lnk_chk1.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_chk1.year_month, '') as year_month,
CAST(lnk_chk1.customer_sk AS int) as customer_sk,
coalesce(lnk_chk1.customer_type, '') as customer_type,
coalesce(lnk_chk1.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_chk1.category, '') as category,
CAST(lnk_chk1.product_sk AS int) as product_sk,
coalesce(lnk_chk1.region, '') as region,
CAST(lnk_chk1.store_sk AS int) as store_sk,
CAST(lnk_chk1.employee_sk AS int) as employee_sk,
coalesce(lnk_chk1.currency, '') as currency,
CAST(lnk_chk1.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_chk1.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_chk1.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_chk1.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_chk1.line_cnt AS int) as line_cnt
FROM
lnk_chk1
WHERE customer_sk = -1

);

CREATE OR REPLACE TEMP VIEW lnk_tag1 AS (
SELECT
'guest_customer_lines' as check_name
FROM
lnk_hit1

);

CREATE OR REPLACE TEMP VIEW lnk_cnt1 AS (
SELECT
coalesce(lnk_tag1.check_name, '') as check_name,
COUNT(*) as hits
FROM
lnk_tag1
GROUP BY
coalesce(lnk_tag1.check_name, '')

);

CREATE OR REPLACE TEMP VIEW lnk_hit2 AS (
SELECT
coalesce(lnk_chk2.invoice_no, '') as invoice_no,
coalesce(lnk_chk2.stock_code, '') as stock_code,
CAST(lnk_chk2.quantity AS int) as quantity,
lnk_chk2.invoice_ts as invoice_ts,
CAST(lnk_chk2.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_chk2.customer_id AS int) as customer_id,
coalesce(lnk_chk2.country, '') as country,
coalesce(lnk_chk2.store_id, '') as store_id,
CAST(lnk_chk2.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_chk2.year_month, '') as year_month,
CAST(lnk_chk2.customer_sk AS int) as customer_sk,
coalesce(lnk_chk2.customer_type, '') as customer_type,
coalesce(lnk_chk2.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_chk2.category, '') as category,
CAST(lnk_chk2.product_sk AS int) as product_sk,
coalesce(lnk_chk2.region, '') as region,
CAST(lnk_chk2.store_sk AS int) as store_sk,
CAST(lnk_chk2.employee_sk AS int) as employee_sk,
coalesce(lnk_chk2.currency, '') as currency,
CAST(lnk_chk2.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_chk2.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_chk2.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_chk2.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_chk2.line_cnt AS int) as line_cnt
FROM
lnk_chk2
WHERE product_sk = -1

);

CREATE OR REPLACE TEMP VIEW lnk_tag2 AS (
SELECT
'orphan_product_lines' as check_name
FROM
lnk_hit2

);

CREATE OR REPLACE TEMP VIEW lnk_cnt2 AS (
SELECT
coalesce(lnk_tag2.check_name, '') as check_name,
COUNT(*) as hits
FROM
lnk_tag2
GROUP BY
coalesce(lnk_tag2.check_name, '')

);

CREATE OR REPLACE TEMP VIEW lnk_hit3 AS (
SELECT
coalesce(lnk_chk3.invoice_no, '') as invoice_no,
coalesce(lnk_chk3.stock_code, '') as stock_code,
CAST(lnk_chk3.quantity AS int) as quantity,
lnk_chk3.invoice_ts as invoice_ts,
CAST(lnk_chk3.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_chk3.customer_id AS int) as customer_id,
coalesce(lnk_chk3.country, '') as country,
coalesce(lnk_chk3.store_id, '') as store_id,
CAST(lnk_chk3.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_chk3.year_month, '') as year_month,
CAST(lnk_chk3.customer_sk AS int) as customer_sk,
coalesce(lnk_chk3.customer_type, '') as customer_type,
coalesce(lnk_chk3.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_chk3.category, '') as category,
CAST(lnk_chk3.product_sk AS int) as product_sk,
coalesce(lnk_chk3.region, '') as region,
CAST(lnk_chk3.store_sk AS int) as store_sk,
CAST(lnk_chk3.employee_sk AS int) as employee_sk,
coalesce(lnk_chk3.currency, '') as currency,
CAST(lnk_chk3.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_chk3.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_chk3.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_chk3.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_chk3.line_cnt AS int) as line_cnt
FROM
lnk_chk3
WHERE sales_amount <= 0

);

CREATE OR REPLACE TEMP VIEW lnk_tag3 AS (
SELECT
'non_positive_sales' as check_name
FROM
lnk_hit3

);

CREATE OR REPLACE TEMP VIEW lnk_cnt3 AS (
SELECT
coalesce(lnk_tag3.check_name, '') as check_name,
COUNT(*) as hits
FROM
lnk_tag3
GROUP BY
coalesce(lnk_tag3.check_name, '')

);

CREATE OR REPLACE TEMP VIEW lnk_hit4 AS (
SELECT
coalesce(lnk_chk4.invoice_no, '') as invoice_no,
coalesce(lnk_chk4.stock_code, '') as stock_code,
CAST(lnk_chk4.quantity AS int) as quantity,
lnk_chk4.invoice_ts as invoice_ts,
CAST(lnk_chk4.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_chk4.customer_id AS int) as customer_id,
coalesce(lnk_chk4.country, '') as country,
coalesce(lnk_chk4.store_id, '') as store_id,
CAST(lnk_chk4.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_chk4.year_month, '') as year_month,
CAST(lnk_chk4.customer_sk AS int) as customer_sk,
coalesce(lnk_chk4.customer_type, '') as customer_type,
coalesce(lnk_chk4.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_chk4.category, '') as category,
CAST(lnk_chk4.product_sk AS int) as product_sk,
coalesce(lnk_chk4.region, '') as region,
CAST(lnk_chk4.store_sk AS int) as store_sk,
CAST(lnk_chk4.employee_sk AS int) as employee_sk,
coalesce(lnk_chk4.currency, '') as currency,
CAST(lnk_chk4.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_chk4.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_chk4.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_chk4.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_chk4.line_cnt AS int) as line_cnt
FROM
lnk_chk4
WHERE gross_profit < 0

);

CREATE OR REPLACE TEMP VIEW lnk_tag4 AS (
SELECT
'negative_gross_profit' as check_name
FROM
lnk_hit4

);

CREATE OR REPLACE TEMP VIEW lnk_cnt4 AS (
SELECT
coalesce(lnk_tag4.check_name, '') as check_name,
COUNT(*) as hits
FROM
lnk_tag4
GROUP BY
coalesce(lnk_tag4.check_name, '')

);

CREATE OR REPLACE TEMP VIEW lnk_all_checks AS (
SELECT
coalesce(lnk_cnt1.check_name, '') as check_name,
CAST(lnk_cnt1.hits AS int) as hits
FROM lnk_cnt1
UNION ALL
SELECT
coalesce(lnk_cnt2.check_name, '') as check_name,
CAST(lnk_cnt2.hits AS int) as hits
FROM lnk_cnt2
UNION ALL
SELECT
coalesce(lnk_cnt3.check_name, '') as check_name,
CAST(lnk_cnt3.hits AS int) as hits
FROM lnk_cnt3
UNION ALL
SELECT
coalesce(lnk_cnt4.check_name, '') as check_name,
CAST(lnk_cnt4.hits AS int) as hits
FROM lnk_cnt4

);

CREATE OR REPLACE TEMP VIEW lnk_sorted AS (
SELECT
coalesce(lnk_all_checks.check_name, '') as check_name,
CAST(lnk_all_checks.hits AS int) as hits
FROM
lnk_all_checks
ORDER BY check_name asc

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.dq_fact_checks AS
SELECT
  *
FROM lnk_sorted;