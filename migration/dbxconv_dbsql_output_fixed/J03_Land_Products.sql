SELECT
UPPER(trim(.StockCode)) as stock_code,
IF ( LENGTH( trim(.Description) ) = 0 , 'UNKNOWN' , UPPER(trim(.Description)) ) as product_name,
trim(.Category) as category,
.ListPrice as list_price,
trim(.SupplierID) as supplier_id,
trim(.Status) as status,
.ListPrice * 0.55 as unit_cost,
IF ( .ListPrice < 1 , 'LOW' , IF ( .ListPrice < 5 , 'MID' , 'HIGH' ) ) as price_band
FROM


SELECT
trim(.SupplierID) as supplier_id,
trim(.SupplierName) as supplier_name,
trim(.SupplierCountry) as supplier_country,
.LeadTimeDays as lead_time_days,
.ReliabilityScore as reliability_score
FROM


SELECT
lnk_std_prod.stock_code as stock_code,
lnk_std_prod.product_name as product_name,
lnk_std_prod.category as category,
lnk_std_prod.list_price as list_price,
lnk_std_prod.supplier_id as supplier_id,
lnk_std_prod.status as status,
lnk_std_prod.unit_cost as unit_cost,
lnk_std_prod.price_band as price_band,
lnk_std_sup.supplier_name as supplier_name,
lnk_std_sup.supplier_country as supplier_country,
lnk_std_sup.lead_time_days as lead_time_days,
lnk_std_sup.reliability_score as reliability_score
FROM
lnk_std_prod
LEFT JOIN lnk_std_sup ON lnk_std_prod.supplier_id = lnk_std_sup.supplier_id

SELECT
lnk_prod_sup.stock_code as stock_code,
lnk_prod_sup.product_name as product_name,
lnk_prod_sup.category as category,
lnk_prod_sup.list_price as list_price,
lnk_prod_sup.supplier_id as supplier_id,
lnk_prod_sup.status as status,
lnk_prod_sup.unit_cost as unit_cost,
lnk_prod_sup.price_band as price_band,
NullToValue ( lnk_prod_sup.supplier_name , 'UNKNOWN' ) as supplier_name,
NullToValue ( lnk_prod_sup.supplier_country , 'UNKNOWN' ) as supplier_country,
NullToValue ( lnk_prod_sup.lead_time_days , 0 ) as lead_time_days,
NullToValue ( lnk_prod_sup.reliability_score , 0 ) as reliability_score
FROM
lnk_prod_sup

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
lnk_prod_sup_nn.stock_code as stock_code,
lnk_prod_sup_nn.product_name as product_name,
lnk_prod_sup_nn.category as category,
lnk_prod_sup_nn.list_price as list_price,
lnk_prod_sup_nn.supplier_id as supplier_id,
lnk_prod_sup_nn.status as status,
lnk_prod_sup_nn.unit_cost as unit_cost,
lnk_prod_sup_nn.price_band as price_band,
lnk_prod_sup_nn.supplier_name as supplier_name,
lnk_prod_sup_nn.supplier_country as supplier_country,
lnk_prod_sup_nn.lead_time_days as lead_time_days,
lnk_prod_sup_nn.reliability_score as reliability_score,
ROW_NUMBER() OVER(PARTITION BY lnk_prod_sup_nn.stock_code ORDER BY lnk_prod_sup_nn.product_name, lnk_prod_sup_nn.category, lnk_prod_sup_nn.list_price, lnk_prod_sup_nn.supplier_id, lnk_prod_sup_nn.status, lnk_prod_sup_nn.unit_cost, lnk_prod_sup_nn.price_band, lnk_prod_sup_nn.supplier_name, lnk_prod_sup_nn.supplier_country, lnk_prod_sup_nn.lead_time_days, lnk_prod_sup_nn.reliability_score) dedup_rnk
FROM lnk_prod_sup_nn) dedup_lnk_dedup WHERE dedup_rnk = 1

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

SELECT
lnk_for_active.stock_code as stock_code,
lnk_for_active.product_name as product_name,
lnk_for_active.category as category,
lnk_for_active.list_price as list_price,
lnk_for_active.supplier_id as supplier_id,
lnk_for_active.status as status,
lnk_for_active.unit_cost as unit_cost,
lnk_for_active.price_band as price_band,
lnk_for_active.supplier_name as supplier_name,
lnk_for_active.supplier_country as supplier_country,
lnk_for_active.lead_time_days as lead_time_days,
lnk_for_active.reliability_score as reliability_score
FROM
lnk_for_active
WHERE status = 'ACTIVE'

SELECT CONCAT_WS(',',
lnk_active.stock_code,
lnk_active.product_name,
lnk_active.category,
lnk_active.list_price,
lnk_active.supplier_id,
lnk_active.status,
lnk_active.unit_cost,
lnk_active.price_band,
lnk_active.supplier_name,
lnk_active.supplier_country,
lnk_active.lead_time_days,
lnk_active.reliability_score)
FROM
lnk_active



SELECT CONCAT_WS(',',
lnk_all.stock_code,
lnk_all.product_name,
lnk_all.category,
lnk_all.list_price,
lnk_all.supplier_id,
lnk_all.status,
lnk_all.unit_cost,
lnk_all.price_band,
lnk_all.supplier_name,
lnk_all.supplier_country,
lnk_all.lead_time_days,
lnk_all.reliability_score)
FROM
lnk_all

