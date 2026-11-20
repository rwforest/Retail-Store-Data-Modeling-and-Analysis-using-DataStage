CREATE OR REPLACE TEMP VIEW lnk_fact AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.fact_sales
);

CREATE OR REPLACE TEMP VIEW lnk_dim_promo AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_promotion
);

CREATE OR REPLACE TEMP VIEW lnk_fp AS (
SELECT
CAST(lnk_fact.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_fact.category, '') as category,
CAST(lnk_fact.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_fact.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_fact.line_cnt AS int) as line_cnt,
coalesce(lnk_dim_promo.promo_id, '') as promo_id,
CAST(lnk_dim_promo.discount_pct AS int) as discount_pct,
CAST(lnk_dim_promo.start_key AS int) as start_key,
CAST(lnk_dim_promo.end_key AS int) as end_key
FROM lnk_fact INNER JOIN lnk_dim_promo ON coalesce(lnk_fact.category, '') = coalesce(lnk_dim_promo.category, '')
);

CREATE OR REPLACE TEMP VIEW lnk_window AS (
SELECT
CAST(lnk_fp.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_fp.category, '') as category,
CAST(lnk_fp.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_fp.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_fp.line_cnt AS int) as line_cnt,
coalesce(lnk_fp.promo_id, '') as promo_id,
CAST(lnk_fp.discount_pct AS int) as discount_pct,
CAST(lnk_fp.start_key AS int) as start_key,
CAST(lnk_fp.end_key AS int) as end_key
FROM
lnk_fp
WHERE invoice_date_key >= start_key and invoice_date_key <= end_key

);

CREATE OR REPLACE TEMP VIEW lnk_promo AS (
SELECT
coalesce(lnk_window.promo_id, '') as promo_id,
coalesce(lnk_window.category, '') as category,
CAST(lnk_window.discount_pct AS int) as discount_pct,
SUM(sales_amount) as promo_sales,
SUM(gross_profit) as promo_profit,
SUM(line_cnt) as promo_lines
FROM
lnk_window
GROUP BY
coalesce(lnk_window.promo_id, ''),
coalesce(lnk_window.category, ''),
CAST(lnk_window.discount_pct AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_cost AS (
SELECT
coalesce(lnk_promo.promo_id, '') as promo_id,
coalesce(lnk_promo.category, '') as category,
CAST(lnk_promo.discount_pct AS int) as discount_pct,
CAST(lnk_promo.promo_sales AS decimal(18,2)) as promo_sales,
CAST(lnk_promo.promo_profit AS decimal(18,2)) as promo_profit,
CAST(lnk_promo.promo_lines AS bigint) as promo_lines,
CAST(lnk_promo.promo_sales AS decimal(18,2)) * CAST(lnk_promo.discount_pct AS int) / 100 as est_discount_cost,
CAST(lnk_promo.promo_profit AS decimal(18,2)) - ( CAST(lnk_promo.promo_sales AS decimal(18,2)) * CAST(lnk_promo.discount_pct AS int) / 100 ) as profit_after_discount
FROM
lnk_promo

);

CREATE OR REPLACE TEMP VIEW lnk_sorted AS (
SELECT
coalesce(lnk_cost.promo_id, '') as promo_id,
coalesce(lnk_cost.category, '') as category,
CAST(lnk_cost.discount_pct AS int) as discount_pct,
CAST(lnk_cost.promo_sales AS decimal(18,2)) as promo_sales,
CAST(lnk_cost.promo_profit AS decimal(18,2)) as promo_profit,
CAST(lnk_cost.promo_lines AS bigint) as promo_lines,
CAST(lnk_cost.est_discount_cost AS decimal(18,2)) as est_discount_cost,
CAST(lnk_cost.profit_after_discount AS decimal(18,2)) as profit_after_discount
FROM
lnk_cost
ORDER BY profit_after_discount desc

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.Promo_Effectiveness AS
SELECT
  *
FROM lnk_sorted;