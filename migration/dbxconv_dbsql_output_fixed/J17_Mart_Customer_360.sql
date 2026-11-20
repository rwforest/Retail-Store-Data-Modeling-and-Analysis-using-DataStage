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
.invoice_no as invoice_no,
.customer_id as customer_id,
.customer_sk as customer_sk,
.store_sk as store_sk,
.employee_sk as employee_sk,
.invoice_date_key as invoice_date_key,
.year_month as year_month,
.invoice_sales as invoice_sales,
.invoice_profit as invoice_profit,
.item_qty as item_qty,
.line_total as line_total,
.avg_line_value as avg_line_value,
.invoice_cnt as invoice_cnt
FROM

WHERE customer_id > 0

SELECT
.invoice_no as invoice_no,
.stock_code as stock_code,
.customer_id as customer_id,
.country as country,
.invoice_date_key as invoice_date_key,
.year_month as year_month,
.returned_qty as returned_qty,
.returned_amount as returned_amount,
.category as category,
.product_sk as product_sk,
.customer_sk as customer_sk
FROM

WHERE customer_id > 0

SELECT
lnk_ret_known.customer_id as customer_id,
MIN(total_returned) as total_returned
FROM
lnk_ret_known
GROUP BY
lnk_ret_known.customer_id

SELECT
lnk_known.customer_id as customer_id,
MIN(total_sales) as total_sales,
MIN(total_profit) as total_profit,
MIN(invoice_total) as invoice_total,
MIN(first_date_key) as first_date_key,
MIN(last_date_key) as last_date_key
FROM
lnk_known
GROUP BY
lnk_known.customer_id

SELECT
lnk_rev.customer_id as customer_id,
lnk_rev.total_sales as total_sales,
lnk_rev.total_profit as total_profit,
lnk_rev.invoice_total as invoice_total,
lnk_rev.first_date_key as first_date_key,
lnk_rev.last_date_key as last_date_key,
lnk_ret_agg.total_returned as total_returned
FROM
lnk_rev
LEFT JOIN lnk_ret_agg ON lnk_rev.customer_id = lnk_ret_agg.customer_id

SELECT
lnk_rr.customer_id as customer_id,
lnk_rr.total_sales as total_sales,
lnk_rr.total_profit as total_profit,
lnk_rr.invoice_total as invoice_total,
lnk_rr.first_date_key as first_date_key,
lnk_rr.last_date_key as last_date_key,
NullToValue ( lnk_rr.total_returned , 0 ) as total_returned
FROM
lnk_rr

SELECT
lnk_rr_nn.customer_id as customer_id,
lnk_rr_nn.total_sales as total_sales,
lnk_rr_nn.total_profit as total_profit,
lnk_rr_nn.invoice_total as invoice_total,
lnk_rr_nn.first_date_key as first_date_key,
lnk_rr_nn.last_date_key as last_date_key,
lnk_rr_nn.total_returned as total_returned,
lnk_cur.customer_sk as customer_sk,
lnk_cur.cust_name as cust_name,
lnk_cur.age as age,
lnk_cur.country as country,
lnk_cur.customer_type as customer_type,
lnk_cur.loyalty_tier as loyalty_tier
FROM
lnk_rr_nn
INNER JOIN lnk_cur ON lnk_rr_nn.customer_id = lnk_cur.customer_id

SELECT
lnk_attrs.customer_id as customer_id,
lnk_attrs.total_sales as total_sales,
lnk_attrs.total_profit as total_profit,
lnk_attrs.invoice_total as invoice_total,
lnk_attrs.first_date_key as first_date_key,
lnk_attrs.last_date_key as last_date_key,
lnk_attrs.total_returned as total_returned,
lnk_attrs.customer_sk as customer_sk,
lnk_attrs.cust_name as cust_name,
lnk_attrs.age as age,
lnk_attrs.country as country,
lnk_attrs.customer_type as customer_type,
lnk_attrs.loyalty_tier as loyalty_tier,
DATEDIFF(cast('{pRunDate}' as date), cast(lnk_attrs.last_date_key as date)) as recency_days,
IF ( DATEDIFF(cast('{pRunDate}' as date), cast(lnk_attrs.last_date_key as date)) <= 30 , 5 , IF ( DATEDIFF(cast('{pRunDate}' as date), cast(lnk_attrs.last_date_key as date)) <= 90 , 4 , IF ( DATEDIFF(cast('{pRunDate}' as date), cast(lnk_attrs.last_date_key as date)) <= 180 , 3 , IF ( DATEDIFF(cast('{pRunDate}' as date), cast(lnk_attrs.last_date_key as date)) <= 270 , 2 , 1 ) ) ) ) as r_score,
IF ( lnk_attrs.invoice_total >= 10 , 5 , IF ( lnk_attrs.invoice_total >= 6 , 4 , IF ( lnk_attrs.invoice_total >= 3 , 3 , IF ( lnk_attrs.invoice_total >= 2 , 2 , 1 ) ) ) ) as f_score,
IF ( lnk_attrs.total_sales >= 5000 , 5 , IF ( lnk_attrs.total_sales >= 2000 , 4 , IF ( lnk_attrs.total_sales >= 800 , 3 , IF ( lnk_attrs.total_sales >= 300 , 2 , 1 ) ) ) ) as m_score,
lnk_attrs.total_sales - lnk_attrs.total_returned as net_sales
FROM
lnk_attrs

SELECT
lnk_rfm.customer_id as customer_id,
lnk_rfm.total_sales as total_sales,
lnk_rfm.total_profit as total_profit,
lnk_rfm.invoice_total as invoice_total,
lnk_rfm.first_date_key as first_date_key,
lnk_rfm.last_date_key as last_date_key,
lnk_rfm.total_returned as total_returned,
lnk_rfm.customer_sk as customer_sk,
lnk_rfm.cust_name as cust_name,
lnk_rfm.age as age,
lnk_rfm.country as country,
lnk_rfm.customer_type as customer_type,
lnk_rfm.loyalty_tier as loyalty_tier,
lnk_rfm.recency_days as recency_days,
lnk_rfm.r_score as r_score,
lnk_rfm.f_score as f_score,
lnk_rfm.m_score as m_score,
lnk_rfm.net_sales as net_sales,
IF ( lnk_rfm.r_score >= 4 And lnk_rfm.f_score >= 4 , 'CHAMPION' , IF ( lnk_rfm.r_score >= 3 And lnk_rfm.f_score >= 3 , 'LOYAL' , IF ( lnk_rfm.r_score >= 4 , 'RECENT' , IF ( lnk_rfm.r_score <= 2 And lnk_rfm.f_score >= 3 , 'AT_RISK' , 'HIBERNATING' ) ) ) ) as segment
FROM
lnk_rfm

SELECT
lnk_seg.customer_id as customer_id,
lnk_seg.total_sales as total_sales,
lnk_seg.total_profit as total_profit,
lnk_seg.invoice_total as invoice_total,
lnk_seg.first_date_key as first_date_key,
lnk_seg.last_date_key as last_date_key,
lnk_seg.total_returned as total_returned,
lnk_seg.customer_sk as customer_sk,
lnk_seg.cust_name as cust_name,
lnk_seg.age as age,
lnk_seg.country as country,
lnk_seg.customer_type as customer_type,
lnk_seg.loyalty_tier as loyalty_tier,
lnk_seg.recency_days as recency_days,
lnk_seg.r_score as r_score,
lnk_seg.f_score as f_score,
lnk_seg.m_score as m_score,
lnk_seg.net_sales as net_sales,
lnk_seg.segment as segment
FROM
lnk_seg
ORDER BY net_sales desc

SELECT
lnk_sorted.customer_id as customer_id,
lnk_sorted.total_sales as total_sales,
lnk_sorted.total_profit as total_profit,
lnk_sorted.invoice_total as invoice_total,
lnk_sorted.first_date_key as first_date_key,
lnk_sorted.last_date_key as last_date_key,
lnk_sorted.total_returned as total_returned,
lnk_sorted.customer_sk as customer_sk,
lnk_sorted.cust_name as cust_name,
lnk_sorted.age as age,
lnk_sorted.country as country,
lnk_sorted.customer_type as customer_type,
lnk_sorted.loyalty_tier as loyalty_tier,
lnk_sorted.recency_days as recency_days,
lnk_sorted.r_score as r_score,
lnk_sorted.f_score as f_score,
lnk_sorted.m_score as m_score,
lnk_sorted.net_sales as net_sales,
lnk_sorted.segment as segment
FROM
lnk_sorted

SELECT
lnk_sorted.customer_id as customer_id,
lnk_sorted.total_sales as total_sales,
lnk_sorted.total_profit as total_profit,
lnk_sorted.invoice_total as invoice_total,
lnk_sorted.first_date_key as first_date_key,
lnk_sorted.last_date_key as last_date_key,
lnk_sorted.total_returned as total_returned,
lnk_sorted.customer_sk as customer_sk,
lnk_sorted.cust_name as cust_name,
lnk_sorted.age as age,
lnk_sorted.country as country,
lnk_sorted.customer_type as customer_type,
lnk_sorted.loyalty_tier as loyalty_tier,
lnk_sorted.recency_days as recency_days,
lnk_sorted.r_score as r_score,
lnk_sorted.f_score as f_score,
lnk_sorted.m_score as m_score,
lnk_sorted.net_sales as net_sales,
lnk_sorted.segment as segment
FROM
lnk_sorted

SELECT CONCAT_WS(',',
lnk_out.customer_id,
lnk_out.total_sales,
lnk_out.total_profit,
lnk_out.invoice_total,
lnk_out.first_date_key,
lnk_out.last_date_key,
lnk_out.total_returned,
lnk_out.customer_sk,
lnk_out.cust_name,
lnk_out.age,
lnk_out.country,
lnk_out.customer_type,
lnk_out.loyalty_tier,
lnk_out.recency_days,
lnk_out.r_score,
lnk_out.f_score,
lnk_out.m_score,
lnk_out.net_sales,
lnk_out.segment)
FROM
lnk_out



SELECT
lnk_seg_in.segment as segment,
MIN(customers) as customers
FROM
lnk_seg_in
GROUP BY
lnk_seg_in.segment

SELECT CONCAT_WS(',',
lnk_seg_counts.segment,
lnk_seg_counts.customers)
FROM
lnk_seg_counts

