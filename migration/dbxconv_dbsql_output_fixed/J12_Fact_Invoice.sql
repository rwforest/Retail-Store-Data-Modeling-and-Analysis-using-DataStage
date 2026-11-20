SELECT
.invoice_no as invoice_no,
.customer_id as customer_id,
.customer_sk as customer_sk,
.store_sk as store_sk,
.employee_sk as employee_sk,
.invoice_date_key as invoice_date_key,
.year_month as year_month,
MIN(invoice_sales) as invoice_sales,
MIN(invoice_profit) as invoice_profit,
MIN(item_qty) as item_qty,
MIN(line_total) as line_total
FROM

GROUP BY
.invoice_no,
.customer_id,
.customer_sk,
.store_sk,
.employee_sk,
.invoice_date_key,
.year_month

SELECT
lnk_inv.invoice_no as invoice_no,
lnk_inv.customer_id as customer_id,
lnk_inv.customer_sk as customer_sk,
lnk_inv.store_sk as store_sk,
lnk_inv.employee_sk as employee_sk,
lnk_inv.invoice_date_key as invoice_date_key,
lnk_inv.year_month as year_month,
lnk_inv.invoice_sales as invoice_sales,
lnk_inv.invoice_profit as invoice_profit,
lnk_inv.item_qty as item_qty,
lnk_inv.line_total as line_total,
IF ( lnk_inv.line_total > 0 , lnk_inv.invoice_sales / lnk_inv.line_total , 0 ) as avg_line_value,
1 as invoice_cnt
FROM
lnk_inv

SELECT
lnk_basket.invoice_no as invoice_no,
lnk_basket.customer_id as customer_id,
lnk_basket.customer_sk as customer_sk,
lnk_basket.store_sk as store_sk,
lnk_basket.employee_sk as employee_sk,
lnk_basket.invoice_date_key as invoice_date_key,
lnk_basket.year_month as year_month,
lnk_basket.invoice_sales as invoice_sales,
lnk_basket.invoice_profit as invoice_profit,
lnk_basket.item_qty as item_qty,
lnk_basket.line_total as line_total,
lnk_basket.avg_line_value as avg_line_value,
lnk_basket.invoice_cnt as invoice_cnt
FROM
lnk_basket
ORDER BY invoice_no asc

SELECT CONCAT_WS(',',
lnk_sorted.invoice_no,
lnk_sorted.customer_id,
lnk_sorted.customer_sk,
lnk_sorted.store_sk,
lnk_sorted.employee_sk,
lnk_sorted.invoice_date_key,
lnk_sorted.year_month,
lnk_sorted.invoice_sales,
lnk_sorted.invoice_profit,
lnk_sorted.item_qty,
lnk_sorted.line_total,
lnk_sorted.avg_line_value,
lnk_sorted.invoice_cnt)
FROM
lnk_sorted

