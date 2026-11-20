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

WHERE quantity > 0

SELECT
lnk_pos.invoice_no as invoice_no,
lnk_pos.stock_code as stock_code
FROM
lnk_pos

SELECT
invoice_no,
stock_code
FROM (
SELECT
lnk_prep.invoice_no as invoice_no,
lnk_prep.stock_code as stock_code,
ROW_NUMBER() OVER(PARTITION BY lnk_prep.invoice_no, lnk_prep.stock_code ORDER BY 1) dedup_rnk
FROM lnk_prep) dedup_lnk_dedup WHERE dedup_rnk = 1

SELECT
lnk_dedup.invoice_no as invoice_no,
lnk_dedup.stock_code as stock_code
FROM
lnk_dedup

SELECT
lnk_dedup.invoice_no as invoice_no,
lnk_dedup.stock_code as stock_code
FROM
lnk_dedup

SELECT
lnk_b_in.invoice_no as invoice_no,
lnk_b_in.stock_code as stock_code_b
FROM
lnk_b_in

SELECT
lnk_a.invoice_no as invoice_no,
lnk_a.stock_code as stock_code,
lnk_b.stock_code_b as stock_code_b
FROM
lnk_a
INNER JOIN lnk_b ON lnk_a.invoice_no = lnk_b.invoice_no

SELECT
lnk_pairs_all.invoice_no as invoice_no,
lnk_pairs_all.stock_code as stock_code,
lnk_pairs_all.stock_code_b as stock_code_b
FROM
lnk_pairs_all
WHERE stock_code < stock_code_b

SELECT
lnk_pairs.stock_code as stock_code,
lnk_pairs.stock_code_b as stock_code_b,
MIN(pair_count) as pair_count
FROM
lnk_pairs
GROUP BY
lnk_pairs.stock_code,
lnk_pairs.stock_code_b

SELECT
lnk_counts.stock_code as stock_code,
lnk_counts.stock_code_b as stock_code_b,
lnk_counts.pair_count as pair_count
FROM
lnk_counts
WHERE pair_count >= 5

SELECT
lnk_support.stock_code as stock_code,
lnk_support.stock_code_b as stock_code_b,
lnk_support.pair_count as pair_count
FROM
lnk_support
ORDER BY pair_count desc

// ******** No handler defined for type HEAD, node lnk_top *******


SELECT CONCAT_WS(',',
lnk_top.stock_code,
lnk_top.stock_code_b,
lnk_top.pair_count)
FROM
lnk_top

