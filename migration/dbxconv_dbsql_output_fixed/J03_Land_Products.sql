CREATE OR REPLACE TEMP VIEW lnk_products AS (
  SELECT _c0 AS StockCode, _c1 AS Description, _c2 AS Category, CAST(_c3 AS decimal(18,2)) AS ListPrice, _c4 AS SupplierID, _c5 AS Status
  FROM read_files('/Volumes/unitygo/retail_dw/landing/products_catalog.csv', format => 'csv', header => true,
    schema => '_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string', escape => '"')
);

CREATE OR REPLACE TEMP VIEW lnk_suppliers AS (
  SELECT _c0 AS SupplierID, _c1 AS SupplierName, _c2 AS SupplierCountry, CAST(_c3 AS int) AS LeadTimeDays, CAST(_c4 AS decimal(18,2)) AS ReliabilityScore
  FROM read_files('/Volumes/unitygo/retail_dw/landing/suppliers.csv', format => 'csv', header => true,
    schema => '_c0 string, _c1 string, _c2 string, _c3 string, _c4 string', escape => '"')
);

CREATE OR REPLACE TEMP VIEW lnk_std_prod AS (
SELECT
UPPER(trim(coalesce(lnk_products.StockCode, ''))) as stock_code,
IF ( LENGTH( trim(coalesce(lnk_products.Description, '')) ) = 0 , 'UNKNOWN' , UPPER(trim(coalesce(lnk_products.Description, ''))) ) as product_name,
trim(coalesce(lnk_products.Category, '')) as category,
CAST(lnk_products.ListPrice AS decimal(18,2)) as list_price,
trim(coalesce(lnk_products.SupplierID, '')) as supplier_id,
trim(coalesce(lnk_products.Status, '')) as status,
CAST(lnk_products.ListPrice AS decimal(18,2)) * 0.55 as unit_cost,
IF ( CAST(lnk_products.ListPrice AS decimal(18,2)) < 1 , 'LOW' , IF ( CAST(lnk_products.ListPrice AS decimal(18,2)) < 5 , 'MID' , 'HIGH' ) ) as price_band
FROM lnk_products
);

CREATE OR REPLACE TEMP VIEW lnk_std_sup AS (
SELECT
trim(coalesce(lnk_suppliers.SupplierID, '')) as supplier_id,
trim(coalesce(lnk_suppliers.SupplierName, '')) as supplier_name,
trim(coalesce(lnk_suppliers.SupplierCountry, '')) as supplier_country,
CAST(lnk_suppliers.LeadTimeDays AS int) as lead_time_days,
CAST(lnk_suppliers.ReliabilityScore AS decimal(18,2)) as reliability_score
FROM lnk_suppliers
);

CREATE OR REPLACE TEMP VIEW lnk_prod_sup AS (
SELECT
coalesce(lnk_std_prod.stock_code, '') as stock_code,
coalesce(lnk_std_prod.product_name, '') as product_name,
coalesce(lnk_std_prod.category, '') as category,
CAST(lnk_std_prod.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_std_prod.supplier_id, '') as supplier_id,
coalesce(lnk_std_prod.status, '') as status,
CAST(lnk_std_prod.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_std_prod.price_band, '') as price_band,
lnk_std_sup.supplier_name as supplier_name,
lnk_std_sup.supplier_country as supplier_country,
CAST(lnk_std_sup.lead_time_days AS int) as lead_time_days,
CAST(lnk_std_sup.reliability_score AS decimal(18,2)) as reliability_score
FROM
lnk_std_prod
LEFT JOIN lnk_std_sup ON coalesce(lnk_std_prod.supplier_id, '') = lnk_std_sup.supplier_id

);

CREATE OR REPLACE TEMP VIEW lnk_prod_sup_nn AS (
SELECT
coalesce(lnk_prod_sup.stock_code, '') as stock_code,
coalesce(lnk_prod_sup.product_name, '') as product_name,
coalesce(lnk_prod_sup.category, '') as category,
CAST(lnk_prod_sup.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_prod_sup.supplier_id, '') as supplier_id,
coalesce(lnk_prod_sup.status, '') as status,
CAST(lnk_prod_sup.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_prod_sup.price_band, '') as price_band,
COALESCE( lnk_prod_sup.supplier_name , 'UNKNOWN' ) as supplier_name,
COALESCE( lnk_prod_sup.supplier_country , 'UNKNOWN' ) as supplier_country,
COALESCE( CAST(lnk_prod_sup.lead_time_days AS int) , 0 ) as lead_time_days,
COALESCE( CAST(lnk_prod_sup.reliability_score AS decimal(18,2)) , 0 ) as reliability_score
FROM
lnk_prod_sup

);

CREATE OR REPLACE TEMP VIEW lnk_dedup AS (
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
coalesce(lnk_prod_sup_nn.stock_code, '') as stock_code,
coalesce(lnk_prod_sup_nn.product_name, '') as product_name,
coalesce(lnk_prod_sup_nn.category, '') as category,
CAST(lnk_prod_sup_nn.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_prod_sup_nn.supplier_id, '') as supplier_id,
coalesce(lnk_prod_sup_nn.status, '') as status,
CAST(lnk_prod_sup_nn.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_prod_sup_nn.price_band, '') as price_band,
coalesce(lnk_prod_sup_nn.supplier_name, '') as supplier_name,
coalesce(lnk_prod_sup_nn.supplier_country, '') as supplier_country,
CAST(lnk_prod_sup_nn.lead_time_days AS int) as lead_time_days,
CAST(lnk_prod_sup_nn.reliability_score AS decimal(18,2)) as reliability_score,
ROW_NUMBER() OVER(PARTITION BY coalesce(lnk_prod_sup_nn.stock_code, '') ORDER BY coalesce(lnk_prod_sup_nn.product_name, ''), coalesce(lnk_prod_sup_nn.category, ''), CAST(lnk_prod_sup_nn.list_price AS decimal(18,2)), coalesce(lnk_prod_sup_nn.supplier_id, ''), coalesce(lnk_prod_sup_nn.status, ''), CAST(lnk_prod_sup_nn.unit_cost AS decimal(18,2)), coalesce(lnk_prod_sup_nn.price_band, ''), coalesce(lnk_prod_sup_nn.supplier_name, ''), coalesce(lnk_prod_sup_nn.supplier_country, ''), CAST(lnk_prod_sup_nn.lead_time_days AS int), CAST(lnk_prod_sup_nn.reliability_score AS decimal(18,2))) dedup_rnk
FROM lnk_prod_sup_nn) dedup_lnk_dedup WHERE dedup_rnk = 1

);

CREATE OR REPLACE TEMP VIEW lnk_all AS (
SELECT
coalesce(lnk_dedup.stock_code, '') as stock_code,
coalesce(lnk_dedup.product_name, '') as product_name,
coalesce(lnk_dedup.category, '') as category,
CAST(lnk_dedup.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_dedup.supplier_id, '') as supplier_id,
coalesce(lnk_dedup.status, '') as status,
CAST(lnk_dedup.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_dedup.price_band, '') as price_band,
coalesce(lnk_dedup.supplier_name, '') as supplier_name,
coalesce(lnk_dedup.supplier_country, '') as supplier_country,
CAST(lnk_dedup.lead_time_days AS int) as lead_time_days,
CAST(lnk_dedup.reliability_score AS decimal(18,2)) as reliability_score
FROM
lnk_dedup

);

CREATE OR REPLACE TEMP VIEW lnk_for_active AS (
SELECT
coalesce(lnk_dedup.stock_code, '') as stock_code,
coalesce(lnk_dedup.product_name, '') as product_name,
coalesce(lnk_dedup.category, '') as category,
CAST(lnk_dedup.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_dedup.supplier_id, '') as supplier_id,
coalesce(lnk_dedup.status, '') as status,
CAST(lnk_dedup.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_dedup.price_band, '') as price_band,
coalesce(lnk_dedup.supplier_name, '') as supplier_name,
coalesce(lnk_dedup.supplier_country, '') as supplier_country,
CAST(lnk_dedup.lead_time_days AS int) as lead_time_days,
CAST(lnk_dedup.reliability_score AS decimal(18,2)) as reliability_score
FROM
lnk_dedup

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.stg_products AS
SELECT
  *
FROM lnk_all;

CREATE OR REPLACE TEMP VIEW lnk_active AS (
SELECT
coalesce(lnk_for_active.stock_code, '') as stock_code,
coalesce(lnk_for_active.product_name, '') as product_name,
coalesce(lnk_for_active.category, '') as category,
CAST(lnk_for_active.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_for_active.supplier_id, '') as supplier_id,
coalesce(lnk_for_active.status, '') as status,
CAST(lnk_for_active.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_for_active.price_band, '') as price_band,
coalesce(lnk_for_active.supplier_name, '') as supplier_name,
coalesce(lnk_for_active.supplier_country, '') as supplier_country,
CAST(lnk_for_active.lead_time_days AS int) as lead_time_days,
CAST(lnk_for_active.reliability_score AS decimal(18,2)) as reliability_score
FROM
lnk_for_active
WHERE status = 'ACTIVE'

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.stg_active_products AS
SELECT
  *
FROM lnk_active;