SELECT
stock_code,
product_name,
category,
list_price,
supplier_id,
status,
unit_cost,
price_band,
supplier_name,
supplier_country,
lead_time_days,
reliability_score
FROM (
SELECT
.stock_code as stock_code,
.product_name as product_name,
.category as category,
.list_price as list_price,
.supplier_id as supplier_id,
.status as status,
.unit_cost as unit_cost,
.price_band as price_band,
.supplier_name as supplier_name,
.supplier_country as supplier_country,
.lead_time_days as lead_time_days,
.reliability_score as reliability_score,
ROW_NUMBER() OVER(PARTITION BY .stock_code ORDER BY .product_name, .category, .list_price, .supplier_id, .status, .unit_cost, .price_band, .supplier_name, .supplier_country, .lead_time_days, .reliability_score) dedup_rnk
FROM ) dedup_lnk_dedup WHERE dedup_rnk = 1

SELECT
lnk_dedup.stock_code as stock_code,
lnk_dedup.product_name as product_name,
lnk_dedup.category as category,
lnk_dedup.list_price as list_price,
lnk_dedup.supplier_id as supplier_id,
lnk_dedup.status as status,
lnk_dedup.unit_cost as unit_cost,
lnk_dedup.price_band as price_band,
lnk_dedup.supplier_name as supplier_name,
lnk_dedup.supplier_country as supplier_country,
lnk_dedup.lead_time_days as lead_time_days,
lnk_dedup.reliability_score as reliability_score
FROM
lnk_dedup
ORDER BY stock_code asc

SELECT
lnk_sorted.stock_code as stock_code,
lnk_sorted.product_name as product_name,
lnk_sorted.category as category,
lnk_sorted.list_price as list_price,
lnk_sorted.supplier_id as supplier_id,
lnk_sorted.status as status,
lnk_sorted.unit_cost as unit_cost,
lnk_sorted.price_band as price_band,
lnk_sorted.supplier_name as supplier_name,
lnk_sorted.supplier_country as supplier_country,
lnk_sorted.lead_time_days as lead_time_days,
lnk_sorted.reliability_score as reliability_score,
row_number() over (order by 1) as product_sk,
IF ( lnk_sorted.category = 'Seasonal' , 'SEASONAL' , IF ( lnk_sorted.category = 'General Merchandise' , 'OTHER' , 'CORE' ) ) as category_group
FROM
lnk_sorted

SELECT
lnk_dim.stock_code as stock_code,
lnk_dim.product_name as product_name,
lnk_dim.category as category,
lnk_dim.list_price as list_price,
lnk_dim.supplier_id as supplier_id,
lnk_dim.status as status,
lnk_dim.unit_cost as unit_cost,
lnk_dim.price_band as price_band,
lnk_dim.supplier_name as supplier_name,
lnk_dim.supplier_country as supplier_country,
lnk_dim.lead_time_days as lead_time_days,
lnk_dim.reliability_score as reliability_score,
lnk_dim.product_sk as product_sk,
lnk_dim.category_group as category_group
FROM
lnk_dim

SELECT
lnk_dim_cols.stock_code as stock_code,
lnk_dim_cols.product_name as product_name,
lnk_dim_cols.category as category,
lnk_dim_cols.list_price as list_price,
lnk_dim_cols.supplier_id as supplier_id,
lnk_dim_cols.status as status,
lnk_dim_cols.unit_cost as unit_cost,
lnk_dim_cols.price_band as price_band,
lnk_dim_cols.supplier_name as supplier_name,
lnk_dim_cols.supplier_country as supplier_country,
lnk_dim_cols.lead_time_days as lead_time_days,
lnk_dim_cols.reliability_score as reliability_score,
lnk_dim_cols.product_sk as product_sk,
lnk_dim_cols.category_group as category_group
FROM
lnk_dim_cols

SELECT
lnk_dim_cols.stock_code as stock_code,
lnk_dim_cols.product_name as product_name,
lnk_dim_cols.category as category,
lnk_dim_cols.list_price as list_price,
lnk_dim_cols.supplier_id as supplier_id,
lnk_dim_cols.status as status,
lnk_dim_cols.unit_cost as unit_cost,
lnk_dim_cols.price_band as price_band,
lnk_dim_cols.supplier_name as supplier_name,
lnk_dim_cols.supplier_country as supplier_country,
lnk_dim_cols.lead_time_days as lead_time_days,
lnk_dim_cols.reliability_score as reliability_score,
lnk_dim_cols.product_sk as product_sk,
lnk_dim_cols.category_group as category_group
FROM
lnk_dim_cols

SELECT CONCAT_WS(',',
lnk_dim_out.stock_code,
lnk_dim_out.product_name,
lnk_dim_out.category,
lnk_dim_out.list_price,
lnk_dim_out.supplier_id,
lnk_dim_out.status,
lnk_dim_out.unit_cost,
lnk_dim_out.price_band,
lnk_dim_out.supplier_name,
lnk_dim_out.supplier_country,
lnk_dim_out.lead_time_days,
lnk_dim_out.reliability_score,
lnk_dim_out.product_sk,
lnk_dim_out.category_group)
FROM
lnk_dim_out



SELECT
lnk_audit_in.category as category,
MIN(products) as products
FROM
lnk_audit_in
GROUP BY
lnk_audit_in.category

SELECT CONCAT_WS(',',
lnk_cat_counts.category,
lnk_cat_counts.products)
FROM
lnk_cat_counts

