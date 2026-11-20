SELECT
.invoice_date_key as invoice_date_key,
.category as category,
.sales_amount as sales_amount,
.gross_profit as gross_profit,
.line_cnt as line_cnt,
.promo_id as promo_id,
.discount_pct as discount_pct,
.start_key as start_key,
.end_key as end_key
FROM

INNER JOIN  ON .category = .category

SELECT
lnk_fp.invoice_date_key as invoice_date_key,
lnk_fp.category as category,
lnk_fp.sales_amount as sales_amount,
lnk_fp.gross_profit as gross_profit,
lnk_fp.line_cnt as line_cnt,
lnk_fp.promo_id as promo_id,
lnk_fp.discount_pct as discount_pct,
lnk_fp.start_key as start_key,
lnk_fp.end_key as end_key
FROM
lnk_fp
WHERE invoice_date_key >= start_key and invoice_date_key <= end_key

SELECT
lnk_window.promo_id as promo_id,
lnk_window.category as category,
lnk_window.discount_pct as discount_pct,
MIN(promo_sales) as promo_sales,
MIN(promo_profit) as promo_profit,
MIN(promo_lines) as promo_lines
FROM
lnk_window
GROUP BY
lnk_window.promo_id,
lnk_window.category,
lnk_window.discount_pct

SELECT
lnk_promo.promo_id as promo_id,
lnk_promo.category as category,
lnk_promo.discount_pct as discount_pct,
lnk_promo.promo_sales as promo_sales,
lnk_promo.promo_profit as promo_profit,
lnk_promo.promo_lines as promo_lines,
lnk_promo.promo_sales * lnk_promo.discount_pct / 100 as est_discount_cost,
lnk_promo.promo_profit - ( lnk_promo.promo_sales * lnk_promo.discount_pct / 100 ) as profit_after_discount
FROM
lnk_promo

SELECT
lnk_cost.promo_id as promo_id,
lnk_cost.category as category,
lnk_cost.discount_pct as discount_pct,
lnk_cost.promo_sales as promo_sales,
lnk_cost.promo_profit as promo_profit,
lnk_cost.promo_lines as promo_lines,
lnk_cost.est_discount_cost as est_discount_cost,
lnk_cost.profit_after_discount as profit_after_discount
FROM
lnk_cost
ORDER BY profit_after_discount desc

SELECT CONCAT_WS(',',
lnk_sorted.promo_id,
lnk_sorted.category,
lnk_sorted.discount_pct,
lnk_sorted.promo_sales,
lnk_sorted.promo_profit,
lnk_sorted.promo_lines,
lnk_sorted.est_discount_cost,
lnk_sorted.profit_after_discount)
FROM
lnk_sorted

