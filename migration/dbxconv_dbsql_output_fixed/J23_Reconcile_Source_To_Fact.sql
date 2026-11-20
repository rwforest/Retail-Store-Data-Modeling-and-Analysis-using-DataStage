SELECT
.sales_amount as sales_amount,
'ALL' as rec_key
FROM


SELECT
lnk_k2.rec_key as rec_key,
MIN(fact_amount) as fact_amount
FROM
lnk_k2
GROUP BY
lnk_k2.rec_key

SELECT
.invoice_no as invoice_no,
.stock_code as stock_code,
.description as description,
.quantity as quantity,
.invoice_ts as invoice_ts,
.unit_price as unit_price,
.customer_id as customer_id,
.country as country,
.store_id as store_id,
.employee_id as employee_id,
.invoice_date_key as invoice_date_key,
.line_amount as line_amount,
.is_cancel as is_cancel,
.is_nonproduct as is_nonproduct,
.is_guest as is_guest
FROM

WHERE is_cancel = 0 and is_nonproduct = 0 and quantity > 0

SELECT
lnk_lines.line_amount as line_amount,
'ALL' as rec_key
FROM
lnk_lines

SELECT
lnk_k1.rec_key as rec_key,
MIN(stage_amount) as stage_amount
FROM
lnk_k1
GROUP BY
lnk_k1.rec_key

SELECT
lnk_a1.rec_key as rec_key,
lnk_a1.stage_amount as stage_amount,
lnk_a2.fact_amount as fact_amount
FROM
lnk_a1
INNER JOIN lnk_a2 ON lnk_a1.rec_key = lnk_a2.rec_key

SELECT
lnk_rec.rec_key as rec_key,
lnk_rec.stage_amount as stage_amount,
lnk_rec.fact_amount as fact_amount,
lnk_rec.stage_amount - lnk_rec.fact_amount as diff_amount,
IF ( Abs ( lnk_rec.stage_amount - lnk_rec.fact_amount ) < 0.01 , 'PASS' , 'FAIL' ) as status
FROM
lnk_rec

SELECT CONCAT_WS(',',
lnk_diff.rec_key,
lnk_diff.stage_amount,
lnk_diff.fact_amount,
lnk_diff.diff_amount,
lnk_diff.status)
FROM
lnk_diff

