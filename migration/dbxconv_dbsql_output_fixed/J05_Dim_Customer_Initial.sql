CREATE OR REPLACE TEMP VIEW lnk_stg AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.stg_customers_2010
);

CREATE OR REPLACE TEMP VIEW lnk_v1 AS (
SELECT
CAST(lnk_stg.customer_id AS int) * 1000 + 1 as customer_sk,
CAST(lnk_stg.customer_id AS int) as customer_id,
coalesce(lnk_stg.cust_name, '') as cust_name,
coalesce(lnk_stg.gender, '') as gender,
CAST(lnk_stg.age AS int) as age,
CAST(lnk_stg.spending_score AS int) as spending_score,
CAST(lnk_stg.annual_income_k AS int) as annual_income_k,
coalesce(lnk_stg.country, '') as country,
coalesce(lnk_stg.customer_type, '') as customer_type,
coalesce(lnk_stg.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_stg.email, '') as email,
coalesce(lnk_stg.city, '') as city,
CAST(lnk_stg.attr_hash AS bigint) as attr_hash,
1 as version_no,
19000101 as eff_start_key,
99991231 as eff_end_key,
1 as is_current
FROM lnk_stg
);

CREATE OR REPLACE TEMP VIEW lnk_sorted AS (
SELECT
CAST(lnk_v1.customer_sk AS int) as customer_sk,
CAST(lnk_v1.customer_id AS int) as customer_id,
coalesce(lnk_v1.cust_name, '') as cust_name,
coalesce(lnk_v1.gender, '') as gender,
CAST(lnk_v1.age AS int) as age,
CAST(lnk_v1.spending_score AS int) as spending_score,
CAST(lnk_v1.annual_income_k AS int) as annual_income_k,
coalesce(lnk_v1.country, '') as country,
coalesce(lnk_v1.customer_type, '') as customer_type,
coalesce(lnk_v1.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_v1.email, '') as email,
coalesce(lnk_v1.city, '') as city,
CAST(lnk_v1.attr_hash AS bigint) as attr_hash,
CAST(lnk_v1.version_no AS int) as version_no,
CAST(lnk_v1.eff_start_key AS int) as eff_start_key,
CAST(lnk_v1.eff_end_key AS int) as eff_end_key,
CAST(lnk_v1.is_current AS int) as is_current
FROM
lnk_v1
ORDER BY customer_id asc, version_no asc

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.dim_customer AS
SELECT
  *
FROM lnk_sorted;