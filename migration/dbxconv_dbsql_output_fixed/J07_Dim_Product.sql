CREATE OR REPLACE TEMP VIEW lnk_stg AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.stg_products
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
coalesce(lnk_stg.stock_code, '') as stock_code,
coalesce(lnk_stg.product_name, '') as product_name,
coalesce(lnk_stg.category, '') as category,
CAST(lnk_stg.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_stg.supplier_id, '') as supplier_id,
coalesce(lnk_stg.status, '') as status,
CAST(lnk_stg.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_stg.price_band, '') as price_band,
coalesce(lnk_stg.supplier_name, '') as supplier_name,
coalesce(lnk_stg.supplier_country, '') as supplier_country,
CAST(lnk_stg.lead_time_days AS int) as lead_time_days,
CAST(lnk_stg.reliability_score AS decimal(18,2)) as reliability_score,
ROW_NUMBER() OVER(PARTITION BY coalesce(lnk_stg.stock_code, '') ORDER BY coalesce(lnk_stg.product_name, ''), coalesce(lnk_stg.category, ''), CAST(lnk_stg.list_price AS decimal(18,2)), coalesce(lnk_stg.supplier_id, ''), coalesce(lnk_stg.status, ''), CAST(lnk_stg.unit_cost AS decimal(18,2)), coalesce(lnk_stg.price_band, ''), coalesce(lnk_stg.supplier_name, ''), coalesce(lnk_stg.supplier_country, ''), CAST(lnk_stg.lead_time_days AS int), CAST(lnk_stg.reliability_score AS decimal(18,2))) dedup_rnk
FROM lnk_stg) dedup_lnk_dedup WHERE dedup_rnk = 1

);

CREATE OR REPLACE TEMP VIEW lnk_sorted AS (
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
ORDER BY stock_code asc

);

CREATE OR REPLACE TEMP VIEW lnk_dim AS (
SELECT
coalesce(lnk_sorted.stock_code, '') as stock_code,
coalesce(lnk_sorted.product_name, '') as product_name,
coalesce(lnk_sorted.category, '') as category,
CAST(lnk_sorted.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_sorted.supplier_id, '') as supplier_id,
coalesce(lnk_sorted.status, '') as status,
CAST(lnk_sorted.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_sorted.price_band, '') as price_band,
coalesce(lnk_sorted.supplier_name, '') as supplier_name,
coalesce(lnk_sorted.supplier_country, '') as supplier_country,
CAST(lnk_sorted.lead_time_days AS int) as lead_time_days,
CAST(lnk_sorted.reliability_score AS decimal(18,2)) as reliability_score,
row_number() over (order by 1) as product_sk,
IF ( coalesce(lnk_sorted.category, '') = 'Seasonal' , 'SEASONAL' , IF ( coalesce(lnk_sorted.category, '') = 'General Merchandise' , 'OTHER' , 'CORE' ) ) as category_group
FROM
lnk_sorted

);

CREATE OR REPLACE TEMP VIEW lnk_dim_cols AS (
SELECT
coalesce(lnk_dim.stock_code, '') as stock_code,
coalesce(lnk_dim.product_name, '') as product_name,
coalesce(lnk_dim.category, '') as category,
CAST(lnk_dim.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_dim.supplier_id, '') as supplier_id,
coalesce(lnk_dim.status, '') as status,
CAST(lnk_dim.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_dim.price_band, '') as price_band,
coalesce(lnk_dim.supplier_name, '') as supplier_name,
coalesce(lnk_dim.supplier_country, '') as supplier_country,
CAST(lnk_dim.lead_time_days AS int) as lead_time_days,
CAST(lnk_dim.reliability_score AS decimal(18,2)) as reliability_score,
CAST(lnk_dim.product_sk AS int) as product_sk,
coalesce(lnk_dim.category_group, '') as category_group
FROM
lnk_dim

);

CREATE OR REPLACE TEMP VIEW lnk_dim_out AS (
SELECT
coalesce(lnk_dim_cols.stock_code, '') as stock_code,
coalesce(lnk_dim_cols.product_name, '') as product_name,
coalesce(lnk_dim_cols.category, '') as category,
CAST(lnk_dim_cols.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_dim_cols.supplier_id, '') as supplier_id,
coalesce(lnk_dim_cols.status, '') as status,
CAST(lnk_dim_cols.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_dim_cols.price_band, '') as price_band,
coalesce(lnk_dim_cols.supplier_name, '') as supplier_name,
coalesce(lnk_dim_cols.supplier_country, '') as supplier_country,
CAST(lnk_dim_cols.lead_time_days AS int) as lead_time_days,
CAST(lnk_dim_cols.reliability_score AS decimal(18,2)) as reliability_score,
CAST(lnk_dim_cols.product_sk AS int) as product_sk,
coalesce(lnk_dim_cols.category_group, '') as category_group
FROM
lnk_dim_cols

);

CREATE OR REPLACE TEMP VIEW lnk_audit_in AS (
SELECT
coalesce(lnk_dim_cols.stock_code, '') as stock_code,
coalesce(lnk_dim_cols.product_name, '') as product_name,
coalesce(lnk_dim_cols.category, '') as category,
CAST(lnk_dim_cols.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_dim_cols.supplier_id, '') as supplier_id,
coalesce(lnk_dim_cols.status, '') as status,
CAST(lnk_dim_cols.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_dim_cols.price_band, '') as price_band,
coalesce(lnk_dim_cols.supplier_name, '') as supplier_name,
coalesce(lnk_dim_cols.supplier_country, '') as supplier_country,
CAST(lnk_dim_cols.lead_time_days AS int) as lead_time_days,
CAST(lnk_dim_cols.reliability_score AS decimal(18,2)) as reliability_score,
CAST(lnk_dim_cols.product_sk AS int) as product_sk,
coalesce(lnk_dim_cols.category_group, '') as category_group
FROM
lnk_dim_cols

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.dim_product AS
SELECT
  *
FROM lnk_dim_out;

CREATE OR REPLACE TEMP VIEW lnk_cat_counts AS (
SELECT
coalesce(lnk_audit_in.category, '') as category,
COUNT(*) as products
FROM
lnk_audit_in
GROUP BY
coalesce(lnk_audit_in.category, '')

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.audit_products_by_category AS
SELECT
  *
FROM lnk_cat_counts;