CREATE OR REPLACE TEMP VIEW lnk_fact AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.fact_sales
);

CREATE OR REPLACE TEMP VIEW lnk_pos AS (
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
FROM lnk_fact WHERE quantity > 0

);

CREATE OR REPLACE TEMP VIEW lnk_prep AS (
SELECT
coalesce(lnk_pos.invoice_no, '') as invoice_no,
coalesce(lnk_pos.stock_code, '') as stock_code
FROM
lnk_pos

);

CREATE OR REPLACE TEMP VIEW lnk_dedup AS (
SELECT
invoice_no,
stock_code
FROM (
SELECT
coalesce(lnk_prep.invoice_no, '') as invoice_no,
coalesce(lnk_prep.stock_code, '') as stock_code,
ROW_NUMBER() OVER(PARTITION BY coalesce(lnk_prep.invoice_no, ''), coalesce(lnk_prep.stock_code, '') ORDER BY 1) dedup_rnk
FROM lnk_prep) dedup_lnk_dedup WHERE dedup_rnk = 1

);

CREATE OR REPLACE TEMP VIEW lnk_a AS (
SELECT
coalesce(lnk_dedup.invoice_no, '') as invoice_no,
coalesce(lnk_dedup.stock_code, '') as stock_code
FROM
lnk_dedup

);

CREATE OR REPLACE TEMP VIEW lnk_b_in AS (
SELECT
coalesce(lnk_dedup.invoice_no, '') as invoice_no,
coalesce(lnk_dedup.stock_code, '') as stock_code
FROM
lnk_dedup

);

CREATE OR REPLACE TEMP VIEW lnk_b AS (
SELECT
coalesce(lnk_b_in.invoice_no, '') as invoice_no,
coalesce(lnk_b_in.stock_code, '') as stock_code_b
FROM
lnk_b_in

);

CREATE OR REPLACE TEMP VIEW lnk_pairs_all AS (
SELECT
coalesce(lnk_a.invoice_no, '') as invoice_no,
coalesce(lnk_a.stock_code, '') as stock_code,
coalesce(lnk_b.stock_code_b, '') as stock_code_b
FROM
lnk_a
INNER JOIN lnk_b ON coalesce(lnk_a.invoice_no, '') = coalesce(lnk_b.invoice_no, '')

);

CREATE OR REPLACE TEMP VIEW lnk_pairs AS (
SELECT
coalesce(lnk_pairs_all.invoice_no, '') as invoice_no,
coalesce(lnk_pairs_all.stock_code, '') as stock_code,
coalesce(lnk_pairs_all.stock_code_b, '') as stock_code_b
FROM
lnk_pairs_all
WHERE stock_code < stock_code_b

);

CREATE OR REPLACE TEMP VIEW lnk_counts AS (
SELECT
coalesce(lnk_pairs.stock_code, '') as stock_code,
coalesce(lnk_pairs.stock_code_b, '') as stock_code_b,
COUNT(*) as pair_count
FROM
lnk_pairs
GROUP BY
coalesce(lnk_pairs.stock_code, ''),
coalesce(lnk_pairs.stock_code_b, '')

);

CREATE OR REPLACE TEMP VIEW lnk_support AS (
SELECT
coalesce(lnk_counts.stock_code, '') as stock_code,
coalesce(lnk_counts.stock_code_b, '') as stock_code_b,
CAST(lnk_counts.pair_count AS int) as pair_count
FROM
lnk_counts
WHERE pair_count >= 5

);

CREATE OR REPLACE TEMP VIEW lnk_sorted AS (
SELECT
coalesce(lnk_support.stock_code, '') as stock_code,
coalesce(lnk_support.stock_code_b, '') as stock_code_b,
CAST(lnk_support.pair_count AS int) as pair_count
FROM
lnk_support
ORDER BY pair_count desc



);

CREATE OR REPLACE TEMP VIEW lnk_top AS (
  SELECT * FROM lnk_sorted
  ORDER BY pair_count DESC
  LIMIT 1000
);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.Basket_Affinity AS
SELECT
  *
FROM lnk_top;