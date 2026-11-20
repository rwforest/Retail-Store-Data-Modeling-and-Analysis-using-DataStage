SELECT
.invoice_no as invoice_no,
.stock_code as stock_code,
.quantity as quantity,
.invoice_ts as invoice_ts,
.unit_price as unit_price,
.customer_id as customer_id,
.country as country,
.store_id as store_id,
.invoice_date_key as invoice_date_key,
.year_month as year_month,
.customer_sk as customer_sk,
.customer_type as customer_type,
.loyalty_tier as loyalty_tier,
.category as category,
.product_sk as product_sk,
.region as region,
.store_sk as store_sk,
.employee_sk as employee_sk,
.currency as currency,
.sales_amount as sales_amount,
.cost_amount as cost_amount,
.gross_profit as gross_profit,
.local_amount as local_amount,
.line_cnt as line_cnt
FROM


SELECT
.invoice_no as invoice_no,
.stock_code as stock_code,
.quantity as quantity,
.invoice_ts as invoice_ts,
.unit_price as unit_price,
.customer_id as customer_id,
.country as country,
.store_id as store_id,
.invoice_date_key as invoice_date_key,
.year_month as year_month,
.customer_sk as customer_sk,
.customer_type as customer_type,
.loyalty_tier as loyalty_tier,
.category as category,
.product_sk as product_sk,
.region as region,
.store_sk as store_sk,
.employee_sk as employee_sk,
.currency as currency,
.sales_amount as sales_amount,
.cost_amount as cost_amount,
.gross_profit as gross_profit,
.local_amount as local_amount,
.line_cnt as line_cnt
FROM


SELECT
.invoice_no as invoice_no,
.stock_code as stock_code,
.quantity as quantity,
.invoice_ts as invoice_ts,
.unit_price as unit_price,
.customer_id as customer_id,
.country as country,
.store_id as store_id,
.invoice_date_key as invoice_date_key,
.year_month as year_month,
.customer_sk as customer_sk,
.customer_type as customer_type,
.loyalty_tier as loyalty_tier,
.category as category,
.product_sk as product_sk,
.region as region,
.store_sk as store_sk,
.employee_sk as employee_sk,
.currency as currency,
.sales_amount as sales_amount,
.cost_amount as cost_amount,
.gross_profit as gross_profit,
.local_amount as local_amount,
.line_cnt as line_cnt
FROM


SELECT
.invoice_no as invoice_no,
.stock_code as stock_code,
.quantity as quantity,
.invoice_ts as invoice_ts,
.unit_price as unit_price,
.customer_id as customer_id,
.country as country,
.store_id as store_id,
.invoice_date_key as invoice_date_key,
.year_month as year_month,
.customer_sk as customer_sk,
.customer_type as customer_type,
.loyalty_tier as loyalty_tier,
.category as category,
.product_sk as product_sk,
.region as region,
.store_sk as store_sk,
.employee_sk as employee_sk,
.currency as currency,
.sales_amount as sales_amount,
.cost_amount as cost_amount,
.gross_profit as gross_profit,
.local_amount as local_amount,
.line_cnt as line_cnt
FROM


SELECT
lnk_chk1.invoice_no as invoice_no,
lnk_chk1.stock_code as stock_code,
lnk_chk1.quantity as quantity,
lnk_chk1.invoice_ts as invoice_ts,
lnk_chk1.unit_price as unit_price,
lnk_chk1.customer_id as customer_id,
lnk_chk1.country as country,
lnk_chk1.store_id as store_id,
lnk_chk1.invoice_date_key as invoice_date_key,
lnk_chk1.year_month as year_month,
lnk_chk1.customer_sk as customer_sk,
lnk_chk1.customer_type as customer_type,
lnk_chk1.loyalty_tier as loyalty_tier,
lnk_chk1.category as category,
lnk_chk1.product_sk as product_sk,
lnk_chk1.region as region,
lnk_chk1.store_sk as store_sk,
lnk_chk1.employee_sk as employee_sk,
lnk_chk1.currency as currency,
lnk_chk1.sales_amount as sales_amount,
lnk_chk1.cost_amount as cost_amount,
lnk_chk1.gross_profit as gross_profit,
lnk_chk1.local_amount as local_amount,
lnk_chk1.line_cnt as line_cnt
FROM
lnk_chk1
WHERE customer_sk = -1

SELECT
lnk_chk4.invoice_no as invoice_no,
lnk_chk4.stock_code as stock_code,
lnk_chk4.quantity as quantity,
lnk_chk4.invoice_ts as invoice_ts,
lnk_chk4.unit_price as unit_price,
lnk_chk4.customer_id as customer_id,
lnk_chk4.country as country,
lnk_chk4.store_id as store_id,
lnk_chk4.invoice_date_key as invoice_date_key,
lnk_chk4.year_month as year_month,
lnk_chk4.customer_sk as customer_sk,
lnk_chk4.customer_type as customer_type,
lnk_chk4.loyalty_tier as loyalty_tier,
lnk_chk4.category as category,
lnk_chk4.product_sk as product_sk,
lnk_chk4.region as region,
lnk_chk4.store_sk as store_sk,
lnk_chk4.employee_sk as employee_sk,
lnk_chk4.currency as currency,
lnk_chk4.sales_amount as sales_amount,
lnk_chk4.cost_amount as cost_amount,
lnk_chk4.gross_profit as gross_profit,
lnk_chk4.local_amount as local_amount,
lnk_chk4.line_cnt as line_cnt
FROM
lnk_chk4
WHERE gross_profit < 0

SELECT
lnk_chk3.invoice_no as invoice_no,
lnk_chk3.stock_code as stock_code,
lnk_chk3.quantity as quantity,
lnk_chk3.invoice_ts as invoice_ts,
lnk_chk3.unit_price as unit_price,
lnk_chk3.customer_id as customer_id,
lnk_chk3.country as country,
lnk_chk3.store_id as store_id,
lnk_chk3.invoice_date_key as invoice_date_key,
lnk_chk3.year_month as year_month,
lnk_chk3.customer_sk as customer_sk,
lnk_chk3.customer_type as customer_type,
lnk_chk3.loyalty_tier as loyalty_tier,
lnk_chk3.category as category,
lnk_chk3.product_sk as product_sk,
lnk_chk3.region as region,
lnk_chk3.store_sk as store_sk,
lnk_chk3.employee_sk as employee_sk,
lnk_chk3.currency as currency,
lnk_chk3.sales_amount as sales_amount,
lnk_chk3.cost_amount as cost_amount,
lnk_chk3.gross_profit as gross_profit,
lnk_chk3.local_amount as local_amount,
lnk_chk3.line_cnt as line_cnt
FROM
lnk_chk3
WHERE sales_amount <= 0

SELECT
lnk_chk2.invoice_no as invoice_no,
lnk_chk2.stock_code as stock_code,
lnk_chk2.quantity as quantity,
lnk_chk2.invoice_ts as invoice_ts,
lnk_chk2.unit_price as unit_price,
lnk_chk2.customer_id as customer_id,
lnk_chk2.country as country,
lnk_chk2.store_id as store_id,
lnk_chk2.invoice_date_key as invoice_date_key,
lnk_chk2.year_month as year_month,
lnk_chk2.customer_sk as customer_sk,
lnk_chk2.customer_type as customer_type,
lnk_chk2.loyalty_tier as loyalty_tier,
lnk_chk2.category as category,
lnk_chk2.product_sk as product_sk,
lnk_chk2.region as region,
lnk_chk2.store_sk as store_sk,
lnk_chk2.employee_sk as employee_sk,
lnk_chk2.currency as currency,
lnk_chk2.sales_amount as sales_amount,
lnk_chk2.cost_amount as cost_amount,
lnk_chk2.gross_profit as gross_profit,
lnk_chk2.local_amount as local_amount,
lnk_chk2.line_cnt as line_cnt
FROM
lnk_chk2
WHERE product_sk = -1

SELECT
'guest_customer_lines' as check_name
FROM
lnk_hit1

SELECT
'negative_gross_profit' as check_name
FROM
lnk_hit4

SELECT
'non_positive_sales' as check_name
FROM
lnk_hit3

SELECT
'orphan_product_lines' as check_name
FROM
lnk_hit2

SELECT
lnk_tag1.check_name as check_name,
MIN(hits) as hits
FROM
lnk_tag1
GROUP BY
lnk_tag1.check_name

SELECT
lnk_tag4.check_name as check_name,
MIN(hits) as hits
FROM
lnk_tag4
GROUP BY
lnk_tag4.check_name

SELECT
lnk_tag3.check_name as check_name,
MIN(hits) as hits
FROM
lnk_tag3
GROUP BY
lnk_tag3.check_name

SELECT
lnk_tag2.check_name as check_name,
MIN(hits) as hits
FROM
lnk_tag2
GROUP BY
lnk_tag2.check_name

SELECT
lnk_cnt1.check_name as check_name,
lnk_cnt1.hits as hits
FROM lnk_cnt1

SELECT
lnk_all_checks.check_name as check_name,
lnk_all_checks.hits as hits
FROM
lnk_all_checks
ORDER BY check_name asc

SELECT CONCAT_WS(',',
lnk_sorted.check_name,
lnk_sorted.hits)
FROM
lnk_sorted

