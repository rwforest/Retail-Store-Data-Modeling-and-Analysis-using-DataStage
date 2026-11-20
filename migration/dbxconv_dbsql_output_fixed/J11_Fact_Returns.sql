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

WHERE is_cancel = 1 and is_nonproduct = 0

SELECT
.customer_sk as customer_sk,
.customer_id as customer_id,
.cust_name as cust_name,
.gender as gender,
.age as age,
.spending_score as spending_score,
.annual_income_k as annual_income_k,
.country as country,
.customer_type as customer_type,
.loyalty_tier as loyalty_tier,
.email as email,
.city as city,
.attr_hash as attr_hash,
.version_no as version_no,
.eff_start_key as eff_start_key,
.eff_end_key as eff_end_key,
.is_current as is_current
FROM

WHERE is_current = 1

SELECT
lnk_cancels.invoice_no as invoice_no,
lnk_cancels.stock_code as stock_code,
lnk_cancels.customer_id as customer_id,
lnk_cancels.country as country,
lnk_cancels.invoice_date_key as invoice_date_key,
CAST(lnk_cancels.invoice_ts AS STRING) as year_month,
Abs ( lnk_cancels.quantity ) as returned_qty,
Abs ( lnk_cancels.line_amount ) as returned_amount
FROM
lnk_cancels

SELECT
lnk_ret.invoice_no as invoice_no,
lnk_ret.stock_code as stock_code,
lnk_ret.customer_id as customer_id,
lnk_ret.country as country,
lnk_ret.invoice_date_key as invoice_date_key,
lnk_ret.year_month as year_month,
lnk_ret.returned_qty as returned_qty,
lnk_ret.returned_amount as returned_amount,
.category as category,
.product_sk as product_sk
FROM
lnk_ret
LEFT JOIN  ON lnk_ret.stock_code = .stock_code

SELECT
lnk_p.invoice_no as invoice_no,
lnk_p.stock_code as stock_code,
lnk_p.customer_id as customer_id,
lnk_p.country as country,
lnk_p.invoice_date_key as invoice_date_key,
lnk_p.year_month as year_month,
lnk_p.returned_qty as returned_qty,
lnk_p.returned_amount as returned_amount,
NullToValue ( lnk_p.category , 'Unknown' ) as category,
NullToValue ( lnk_p.product_sk , - 1 ) as product_sk
FROM
lnk_p

SELECT
lnk_p_nn.invoice_no as invoice_no,
lnk_p_nn.stock_code as stock_code,
lnk_p_nn.customer_id as customer_id,
lnk_p_nn.country as country,
lnk_p_nn.invoice_date_key as invoice_date_key,
lnk_p_nn.year_month as year_month,
lnk_p_nn.returned_qty as returned_qty,
lnk_p_nn.returned_amount as returned_amount,
lnk_p_nn.category as category,
lnk_p_nn.product_sk as product_sk,
lnk_cur.customer_sk as customer_sk
FROM
lnk_p_nn
LEFT JOIN lnk_cur ON lnk_p_nn.customer_id = lnk_cur.customer_id

SELECT
lnk_c.invoice_no as invoice_no,
lnk_c.stock_code as stock_code,
lnk_c.customer_id as customer_id,
lnk_c.country as country,
lnk_c.invoice_date_key as invoice_date_key,
lnk_c.year_month as year_month,
lnk_c.returned_qty as returned_qty,
lnk_c.returned_amount as returned_amount,
lnk_c.category as category,
lnk_c.product_sk as product_sk,
NullToValue ( lnk_c.customer_sk , - 1 ) as customer_sk
FROM
lnk_c

SELECT
lnk_c_nn.invoice_no as invoice_no,
lnk_c_nn.stock_code as stock_code,
lnk_c_nn.customer_id as customer_id,
lnk_c_nn.country as country,
lnk_c_nn.invoice_date_key as invoice_date_key,
lnk_c_nn.year_month as year_month,
lnk_c_nn.returned_qty as returned_qty,
lnk_c_nn.returned_amount as returned_amount,
lnk_c_nn.category as category,
lnk_c_nn.product_sk as product_sk,
lnk_c_nn.customer_sk as customer_sk
FROM
lnk_c_nn
ORDER BY invoice_no asc, stock_code asc

SELECT
lnk_sorted.invoice_no as invoice_no,
lnk_sorted.stock_code as stock_code,
lnk_sorted.customer_id as customer_id,
lnk_sorted.country as country,
lnk_sorted.invoice_date_key as invoice_date_key,
lnk_sorted.year_month as year_month,
lnk_sorted.returned_qty as returned_qty,
lnk_sorted.returned_amount as returned_amount,
lnk_sorted.category as category,
lnk_sorted.product_sk as product_sk,
lnk_sorted.customer_sk as customer_sk
FROM
lnk_sorted

SELECT
lnk_sorted.invoice_no as invoice_no,
lnk_sorted.stock_code as stock_code,
lnk_sorted.customer_id as customer_id,
lnk_sorted.country as country,
lnk_sorted.invoice_date_key as invoice_date_key,
lnk_sorted.year_month as year_month,
lnk_sorted.returned_qty as returned_qty,
lnk_sorted.returned_amount as returned_amount,
lnk_sorted.category as category,
lnk_sorted.product_sk as product_sk,
lnk_sorted.customer_sk as customer_sk
FROM
lnk_sorted

SELECT CONCAT_WS(',',
lnk_out.invoice_no,
lnk_out.stock_code,
lnk_out.customer_id,
lnk_out.country,
lnk_out.invoice_date_key,
lnk_out.year_month,
lnk_out.returned_qty,
lnk_out.returned_amount,
lnk_out.category,
lnk_out.product_sk,
lnk_out.customer_sk)
FROM
lnk_out



SELECT
lnk_audit_in.category as category,
MIN(lnk_audit_in.returned_amount) as returned_amount
FROM
lnk_audit_in
GROUP BY
lnk_audit_in.category

SELECT CONCAT_WS(',',
lnk_by_cat.category,
lnk_by_cat.returned_amount)
FROM
lnk_by_cat

