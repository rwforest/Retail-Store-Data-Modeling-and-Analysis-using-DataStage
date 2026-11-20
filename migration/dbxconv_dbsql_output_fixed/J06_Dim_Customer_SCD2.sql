CREATE OR REPLACE TEMP VIEW lnk_dim AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_customer
);

CREATE OR REPLACE TEMP VIEW lnk_dim_a AS (
SELECT
CAST(lnk_dim.customer_sk AS int) as customer_sk,
CAST(lnk_dim.customer_id AS int) as customer_id,
coalesce(lnk_dim.cust_name, '') as cust_name,
coalesce(lnk_dim.gender, '') as gender,
CAST(lnk_dim.age AS int) as age,
CAST(lnk_dim.spending_score AS int) as spending_score,
CAST(lnk_dim.annual_income_k AS int) as annual_income_k,
coalesce(lnk_dim.country, '') as country,
coalesce(lnk_dim.customer_type, '') as customer_type,
coalesce(lnk_dim.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_dim.email, '') as email,
coalesce(lnk_dim.city, '') as city,
CAST(lnk_dim.attr_hash AS bigint) as attr_hash,
CAST(lnk_dim.version_no AS int) as version_no,
CAST(lnk_dim.eff_start_key AS int) as eff_start_key,
CAST(lnk_dim.eff_end_key AS int) as eff_end_key,
CAST(lnk_dim.is_current AS int) as is_current
FROM lnk_dim
);

CREATE OR REPLACE TEMP VIEW lnk_dim_b AS (
SELECT
CAST(lnk_dim.customer_sk AS int) as customer_sk,
CAST(lnk_dim.customer_id AS int) as customer_id,
coalesce(lnk_dim.cust_name, '') as cust_name,
coalesce(lnk_dim.gender, '') as gender,
CAST(lnk_dim.age AS int) as age,
CAST(lnk_dim.spending_score AS int) as spending_score,
CAST(lnk_dim.annual_income_k AS int) as annual_income_k,
coalesce(lnk_dim.country, '') as country,
coalesce(lnk_dim.customer_type, '') as customer_type,
coalesce(lnk_dim.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_dim.email, '') as email,
coalesce(lnk_dim.city, '') as city,
CAST(lnk_dim.attr_hash AS bigint) as attr_hash,
CAST(lnk_dim.version_no AS int) as version_no,
CAST(lnk_dim.eff_start_key AS int) as eff_start_key,
CAST(lnk_dim.eff_end_key AS int) as eff_end_key,
CAST(lnk_dim.is_current AS int) as is_current
FROM lnk_dim
);

CREATE OR REPLACE TEMP VIEW lnk_current AS (
SELECT
CAST(lnk_dim_a.customer_sk AS int) as customer_sk,
CAST(lnk_dim_a.customer_id AS int) as customer_id,
coalesce(lnk_dim_a.cust_name, '') as cust_name,
coalesce(lnk_dim_a.gender, '') as gender,
CAST(lnk_dim_a.age AS int) as age,
CAST(lnk_dim_a.spending_score AS int) as spending_score,
CAST(lnk_dim_a.annual_income_k AS int) as annual_income_k,
coalesce(lnk_dim_a.country, '') as country,
coalesce(lnk_dim_a.customer_type, '') as customer_type,
coalesce(lnk_dim_a.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_dim_a.email, '') as email,
coalesce(lnk_dim_a.city, '') as city,
CAST(lnk_dim_a.attr_hash AS bigint) as attr_hash,
CAST(lnk_dim_a.version_no AS int) as version_no,
CAST(lnk_dim_a.eff_start_key AS int) as eff_start_key,
CAST(lnk_dim_a.eff_end_key AS int) as eff_end_key,
CAST(lnk_dim_a.is_current AS int) as is_current
FROM
lnk_dim_a
WHERE is_current = 1

);

CREATE OR REPLACE TEMP VIEW lnk_history AS (
SELECT
CAST(lnk_dim_b.customer_sk AS int) as customer_sk,
CAST(lnk_dim_b.customer_id AS int) as customer_id,
coalesce(lnk_dim_b.cust_name, '') as cust_name,
coalesce(lnk_dim_b.gender, '') as gender,
CAST(lnk_dim_b.age AS int) as age,
CAST(lnk_dim_b.spending_score AS int) as spending_score,
CAST(lnk_dim_b.annual_income_k AS int) as annual_income_k,
coalesce(lnk_dim_b.country, '') as country,
coalesce(lnk_dim_b.customer_type, '') as customer_type,
coalesce(lnk_dim_b.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_dim_b.email, '') as email,
coalesce(lnk_dim_b.city, '') as city,
CAST(lnk_dim_b.attr_hash AS bigint) as attr_hash,
CAST(lnk_dim_b.version_no AS int) as version_no,
CAST(lnk_dim_b.eff_start_key AS int) as eff_start_key,
CAST(lnk_dim_b.eff_end_key AS int) as eff_end_key,
CAST(lnk_dim_b.is_current AS int) as is_current
FROM
lnk_dim_b
WHERE is_current = 0

);

CREATE OR REPLACE TEMP VIEW lnk_old AS (
SELECT
CAST(lnk_current.customer_id AS int) as customer_id,
CAST(lnk_current.customer_sk AS int) as old_customer_sk,
coalesce(lnk_current.cust_name, '') as old_cust_name,
coalesce(lnk_current.gender, '') as old_gender,
CAST(lnk_current.age AS int) as old_age,
CAST(lnk_current.spending_score AS int) as old_spending_score,
CAST(lnk_current.annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_current.country, '') as old_country,
coalesce(lnk_current.customer_type, '') as old_customer_type,
coalesce(lnk_current.loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_current.email, '') as old_email,
coalesce(lnk_current.city, '') as old_city,
CAST(lnk_current.attr_hash AS bigint) as old_attr_hash,
CAST(lnk_current.version_no AS int) as old_version_no,
CAST(lnk_current.eff_start_key AS int) as old_eff_start_key,
CAST(lnk_current.eff_end_key AS int) as old_eff_end_key,
CAST(lnk_current.is_current AS int) as old_is_current
FROM
lnk_current

);

CREATE OR REPLACE TEMP VIEW lnk_snap AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.stg_customers_2011
);

CREATE OR REPLACE TEMP VIEW lnk_cmp AS (
SELECT
CAST(lnk_snap.customer_id AS int) as customer_id,
coalesce(lnk_snap.cust_name, '') as cust_name,
coalesce(lnk_snap.gender, '') as gender,
CAST(lnk_snap.age AS int) as age,
CAST(lnk_snap.spending_score AS int) as spending_score,
CAST(lnk_snap.annual_income_k AS int) as annual_income_k,
coalesce(lnk_snap.country, '') as country,
coalesce(lnk_snap.customer_type, '') as customer_type,
coalesce(lnk_snap.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_snap.email, '') as email,
coalesce(lnk_snap.city, '') as city,
lnk_snap.snapshot_date as snapshot_date,
CAST(lnk_snap.attr_hash AS bigint) as attr_hash,
CAST(lnk_old.old_customer_sk AS int) as old_customer_sk,
lnk_old.old_cust_name as old_cust_name,
lnk_old.old_gender as old_gender,
CAST(lnk_old.old_age AS int) as old_age,
CAST(lnk_old.old_spending_score AS int) as old_spending_score,
CAST(lnk_old.old_annual_income_k AS int) as old_annual_income_k,
lnk_old.old_country as old_country,
lnk_old.old_customer_type as old_customer_type,
lnk_old.old_loyalty_tier as old_loyalty_tier,
lnk_old.old_email as old_email,
lnk_old.old_city as old_city,
CAST(lnk_old.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_old.old_version_no AS int) as old_version_no,
CAST(lnk_old.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_old.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_old.old_is_current AS int) as old_is_current
FROM lnk_snap LEFT JOIN lnk_old ON CAST(lnk_snap.customer_id AS int) = CAST(lnk_old.customer_id AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_detected AS (
SELECT
CAST(lnk_cmp.customer_id AS int) as customer_id,
coalesce(lnk_cmp.cust_name, '') as cust_name,
coalesce(lnk_cmp.gender, '') as gender,
CAST(lnk_cmp.age AS int) as age,
CAST(lnk_cmp.spending_score AS int) as spending_score,
CAST(lnk_cmp.annual_income_k AS int) as annual_income_k,
coalesce(lnk_cmp.country, '') as country,
coalesce(lnk_cmp.customer_type, '') as customer_type,
coalesce(lnk_cmp.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_cmp.email, '') as email,
coalesce(lnk_cmp.city, '') as city,
lnk_cmp.snapshot_date as snapshot_date,
CAST(lnk_cmp.attr_hash AS bigint) as attr_hash,
COALESCE( CAST(lnk_cmp.old_customer_sk AS int) , 0 ) as old_customer_sk,
COALESCE( lnk_cmp.old_cust_name , '' ) as old_cust_name,
COALESCE( lnk_cmp.old_gender , '' ) as old_gender,
COALESCE( CAST(lnk_cmp.old_age AS int) , 0 ) as old_age,
COALESCE( CAST(lnk_cmp.old_spending_score AS int) , 0 ) as old_spending_score,
COALESCE( CAST(lnk_cmp.old_annual_income_k AS int) , 0 ) as old_annual_income_k,
COALESCE( lnk_cmp.old_country , '' ) as old_country,
COALESCE( lnk_cmp.old_customer_type , '' ) as old_customer_type,
COALESCE( lnk_cmp.old_loyalty_tier , '' ) as old_loyalty_tier,
COALESCE( lnk_cmp.old_email , '' ) as old_email,
COALESCE( lnk_cmp.old_city , '' ) as old_city,
COALESCE( CAST(lnk_cmp.old_attr_hash AS bigint) , 0 ) as old_attr_hash,
COALESCE( CAST(lnk_cmp.old_version_no AS int) , 0 ) as old_version_no,
COALESCE( CAST(lnk_cmp.old_eff_start_key AS int) , 0 ) as old_eff_start_key,
COALESCE( CAST(lnk_cmp.old_eff_end_key AS int) , 0 ) as old_eff_end_key,
COALESCE( CAST(lnk_cmp.old_is_current AS int) , 0 ) as old_is_current,
IF ( CAST(lnk_cmp.old_attr_hash AS bigint) IS NULL , 'N' , IF ( CAST(lnk_cmp.old_attr_hash AS bigint) <> CAST(lnk_cmp.attr_hash AS bigint) , 'C' , 'U' ) ) as change_flag
FROM
lnk_cmp

);

CREATE OR REPLACE TEMP VIEW lnk_u AS (
SELECT
CAST(lnk_detected.customer_id AS int) as customer_id,
coalesce(lnk_detected.cust_name, '') as cust_name,
coalesce(lnk_detected.gender, '') as gender,
CAST(lnk_detected.age AS int) as age,
CAST(lnk_detected.spending_score AS int) as spending_score,
CAST(lnk_detected.annual_income_k AS int) as annual_income_k,
coalesce(lnk_detected.country, '') as country,
coalesce(lnk_detected.customer_type, '') as customer_type,
coalesce(lnk_detected.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_detected.email, '') as email,
coalesce(lnk_detected.city, '') as city,
lnk_detected.snapshot_date as snapshot_date,
CAST(lnk_detected.attr_hash AS bigint) as attr_hash,
CAST(lnk_detected.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_detected.old_cust_name, '') as old_cust_name,
coalesce(lnk_detected.old_gender, '') as old_gender,
CAST(lnk_detected.old_age AS int) as old_age,
CAST(lnk_detected.old_spending_score AS int) as old_spending_score,
CAST(lnk_detected.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_detected.old_country, '') as old_country,
coalesce(lnk_detected.old_customer_type, '') as old_customer_type,
coalesce(lnk_detected.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_detected.old_email, '') as old_email,
coalesce(lnk_detected.old_city, '') as old_city,
CAST(lnk_detected.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_detected.old_version_no AS int) as old_version_no,
CAST(lnk_detected.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_detected.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_detected.old_is_current AS int) as old_is_current,
coalesce(lnk_detected.change_flag, '') as change_flag
FROM
lnk_detected

);

CREATE OR REPLACE TEMP VIEW lnk_nc AS (
SELECT
CAST(lnk_detected.customer_id AS int) as customer_id,
coalesce(lnk_detected.cust_name, '') as cust_name,
coalesce(lnk_detected.gender, '') as gender,
CAST(lnk_detected.age AS int) as age,
CAST(lnk_detected.spending_score AS int) as spending_score,
CAST(lnk_detected.annual_income_k AS int) as annual_income_k,
coalesce(lnk_detected.country, '') as country,
coalesce(lnk_detected.customer_type, '') as customer_type,
coalesce(lnk_detected.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_detected.email, '') as email,
coalesce(lnk_detected.city, '') as city,
lnk_detected.snapshot_date as snapshot_date,
CAST(lnk_detected.attr_hash AS bigint) as attr_hash,
CAST(lnk_detected.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_detected.old_cust_name, '') as old_cust_name,
coalesce(lnk_detected.old_gender, '') as old_gender,
CAST(lnk_detected.old_age AS int) as old_age,
CAST(lnk_detected.old_spending_score AS int) as old_spending_score,
CAST(lnk_detected.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_detected.old_country, '') as old_country,
coalesce(lnk_detected.old_customer_type, '') as old_customer_type,
coalesce(lnk_detected.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_detected.old_email, '') as old_email,
coalesce(lnk_detected.old_city, '') as old_city,
CAST(lnk_detected.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_detected.old_version_no AS int) as old_version_no,
CAST(lnk_detected.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_detected.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_detected.old_is_current AS int) as old_is_current,
coalesce(lnk_detected.change_flag, '') as change_flag
FROM
lnk_detected

);

CREATE OR REPLACE TEMP VIEW lnk_c AS (
SELECT
CAST(lnk_detected.customer_id AS int) as customer_id,
coalesce(lnk_detected.cust_name, '') as cust_name,
coalesce(lnk_detected.gender, '') as gender,
CAST(lnk_detected.age AS int) as age,
CAST(lnk_detected.spending_score AS int) as spending_score,
CAST(lnk_detected.annual_income_k AS int) as annual_income_k,
coalesce(lnk_detected.country, '') as country,
coalesce(lnk_detected.customer_type, '') as customer_type,
coalesce(lnk_detected.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_detected.email, '') as email,
coalesce(lnk_detected.city, '') as city,
lnk_detected.snapshot_date as snapshot_date,
CAST(lnk_detected.attr_hash AS bigint) as attr_hash,
CAST(lnk_detected.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_detected.old_cust_name, '') as old_cust_name,
coalesce(lnk_detected.old_gender, '') as old_gender,
CAST(lnk_detected.old_age AS int) as old_age,
CAST(lnk_detected.old_spending_score AS int) as old_spending_score,
CAST(lnk_detected.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_detected.old_country, '') as old_country,
coalesce(lnk_detected.old_customer_type, '') as old_customer_type,
coalesce(lnk_detected.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_detected.old_email, '') as old_email,
coalesce(lnk_detected.old_city, '') as old_city,
CAST(lnk_detected.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_detected.old_version_no AS int) as old_version_no,
CAST(lnk_detected.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_detected.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_detected.old_is_current AS int) as old_is_current,
coalesce(lnk_detected.change_flag, '') as change_flag
FROM
lnk_detected

);

CREATE OR REPLACE TEMP VIEW lnk_audit AS (
SELECT
CAST(lnk_detected.customer_id AS int) as customer_id,
coalesce(lnk_detected.cust_name, '') as cust_name,
coalesce(lnk_detected.gender, '') as gender,
CAST(lnk_detected.age AS int) as age,
CAST(lnk_detected.spending_score AS int) as spending_score,
CAST(lnk_detected.annual_income_k AS int) as annual_income_k,
coalesce(lnk_detected.country, '') as country,
coalesce(lnk_detected.customer_type, '') as customer_type,
coalesce(lnk_detected.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_detected.email, '') as email,
coalesce(lnk_detected.city, '') as city,
lnk_detected.snapshot_date as snapshot_date,
CAST(lnk_detected.attr_hash AS bigint) as attr_hash,
CAST(lnk_detected.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_detected.old_cust_name, '') as old_cust_name,
coalesce(lnk_detected.old_gender, '') as old_gender,
CAST(lnk_detected.old_age AS int) as old_age,
CAST(lnk_detected.old_spending_score AS int) as old_spending_score,
CAST(lnk_detected.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_detected.old_country, '') as old_country,
coalesce(lnk_detected.old_customer_type, '') as old_customer_type,
coalesce(lnk_detected.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_detected.old_email, '') as old_email,
coalesce(lnk_detected.old_city, '') as old_city,
CAST(lnk_detected.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_detected.old_version_no AS int) as old_version_no,
CAST(lnk_detected.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_detected.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_detected.old_is_current AS int) as old_is_current,
coalesce(lnk_detected.change_flag, '') as change_flag
FROM
lnk_detected

);

CREATE OR REPLACE TEMP VIEW lnk_unchanged AS (
SELECT
CAST(lnk_u.customer_id AS int) as customer_id,
coalesce(lnk_u.cust_name, '') as cust_name,
coalesce(lnk_u.gender, '') as gender,
CAST(lnk_u.age AS int) as age,
CAST(lnk_u.spending_score AS int) as spending_score,
CAST(lnk_u.annual_income_k AS int) as annual_income_k,
coalesce(lnk_u.country, '') as country,
coalesce(lnk_u.customer_type, '') as customer_type,
coalesce(lnk_u.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_u.email, '') as email,
coalesce(lnk_u.city, '') as city,
lnk_u.snapshot_date as snapshot_date,
CAST(lnk_u.attr_hash AS bigint) as attr_hash,
CAST(lnk_u.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_u.old_cust_name, '') as old_cust_name,
coalesce(lnk_u.old_gender, '') as old_gender,
CAST(lnk_u.old_age AS int) as old_age,
CAST(lnk_u.old_spending_score AS int) as old_spending_score,
CAST(lnk_u.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_u.old_country, '') as old_country,
coalesce(lnk_u.old_customer_type, '') as old_customer_type,
coalesce(lnk_u.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_u.old_email, '') as old_email,
coalesce(lnk_u.old_city, '') as old_city,
CAST(lnk_u.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_u.old_version_no AS int) as old_version_no,
CAST(lnk_u.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_u.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_u.old_is_current AS int) as old_is_current,
coalesce(lnk_u.change_flag, '') as change_flag
FROM
lnk_u
WHERE change_flag = 'U'

);

CREATE OR REPLACE TEMP VIEW lnk_new_or_changed AS (
SELECT
CAST(lnk_nc.customer_id AS int) as customer_id,
coalesce(lnk_nc.cust_name, '') as cust_name,
coalesce(lnk_nc.gender, '') as gender,
CAST(lnk_nc.age AS int) as age,
CAST(lnk_nc.spending_score AS int) as spending_score,
CAST(lnk_nc.annual_income_k AS int) as annual_income_k,
coalesce(lnk_nc.country, '') as country,
coalesce(lnk_nc.customer_type, '') as customer_type,
coalesce(lnk_nc.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_nc.email, '') as email,
coalesce(lnk_nc.city, '') as city,
lnk_nc.snapshot_date as snapshot_date,
CAST(lnk_nc.attr_hash AS bigint) as attr_hash,
CAST(lnk_nc.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_nc.old_cust_name, '') as old_cust_name,
coalesce(lnk_nc.old_gender, '') as old_gender,
CAST(lnk_nc.old_age AS int) as old_age,
CAST(lnk_nc.old_spending_score AS int) as old_spending_score,
CAST(lnk_nc.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_nc.old_country, '') as old_country,
coalesce(lnk_nc.old_customer_type, '') as old_customer_type,
coalesce(lnk_nc.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_nc.old_email, '') as old_email,
coalesce(lnk_nc.old_city, '') as old_city,
CAST(lnk_nc.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_nc.old_version_no AS int) as old_version_no,
CAST(lnk_nc.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_nc.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_nc.old_is_current AS int) as old_is_current,
coalesce(lnk_nc.change_flag, '') as change_flag
FROM
lnk_nc
WHERE change_flag <> 'U'

);

CREATE OR REPLACE TEMP VIEW lnk_changed AS (
SELECT
CAST(lnk_c.customer_id AS int) as customer_id,
coalesce(lnk_c.cust_name, '') as cust_name,
coalesce(lnk_c.gender, '') as gender,
CAST(lnk_c.age AS int) as age,
CAST(lnk_c.spending_score AS int) as spending_score,
CAST(lnk_c.annual_income_k AS int) as annual_income_k,
coalesce(lnk_c.country, '') as country,
coalesce(lnk_c.customer_type, '') as customer_type,
coalesce(lnk_c.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_c.email, '') as email,
coalesce(lnk_c.city, '') as city,
lnk_c.snapshot_date as snapshot_date,
CAST(lnk_c.attr_hash AS bigint) as attr_hash,
CAST(lnk_c.old_customer_sk AS int) as old_customer_sk,
coalesce(lnk_c.old_cust_name, '') as old_cust_name,
coalesce(lnk_c.old_gender, '') as old_gender,
CAST(lnk_c.old_age AS int) as old_age,
CAST(lnk_c.old_spending_score AS int) as old_spending_score,
CAST(lnk_c.old_annual_income_k AS int) as old_annual_income_k,
coalesce(lnk_c.old_country, '') as old_country,
coalesce(lnk_c.old_customer_type, '') as old_customer_type,
coalesce(lnk_c.old_loyalty_tier, '') as old_loyalty_tier,
coalesce(lnk_c.old_email, '') as old_email,
coalesce(lnk_c.old_city, '') as old_city,
CAST(lnk_c.old_attr_hash AS bigint) as old_attr_hash,
CAST(lnk_c.old_version_no AS int) as old_version_no,
CAST(lnk_c.old_eff_start_key AS int) as old_eff_start_key,
CAST(lnk_c.old_eff_end_key AS int) as old_eff_end_key,
CAST(lnk_c.old_is_current AS int) as old_is_current,
coalesce(lnk_c.change_flag, '') as change_flag
FROM
lnk_c
WHERE change_flag = 'C'

);

CREATE OR REPLACE TEMP VIEW lnk_carry AS (
SELECT
CAST(lnk_unchanged.old_customer_sk AS int) as customer_sk,
CAST(lnk_unchanged.customer_id AS int) as customer_id,
coalesce(lnk_unchanged.old_cust_name, '') as cust_name,
coalesce(lnk_unchanged.old_gender, '') as gender,
CAST(lnk_unchanged.old_age AS int) as age,
CAST(lnk_unchanged.old_spending_score AS int) as spending_score,
CAST(lnk_unchanged.old_annual_income_k AS int) as annual_income_k,
coalesce(lnk_unchanged.old_country, '') as country,
coalesce(lnk_unchanged.old_customer_type, '') as customer_type,
coalesce(lnk_unchanged.old_loyalty_tier, '') as loyalty_tier,
coalesce(lnk_unchanged.old_email, '') as email,
coalesce(lnk_unchanged.old_city, '') as city,
CAST(lnk_unchanged.old_attr_hash AS bigint) as attr_hash,
CAST(lnk_unchanged.old_version_no AS int) as version_no,
CAST(lnk_unchanged.old_eff_start_key AS int) as eff_start_key,
CAST(lnk_unchanged.old_eff_end_key AS int) as eff_end_key,
CAST(lnk_unchanged.old_is_current AS int) as is_current
FROM
lnk_unchanged

);

CREATE OR REPLACE TEMP VIEW lnk_expired AS (
SELECT
CAST(lnk_changed.old_customer_sk AS int) as customer_sk,
CAST(lnk_changed.customer_id AS int) as customer_id,
coalesce(lnk_changed.old_cust_name, '') as cust_name,
coalesce(lnk_changed.old_gender, '') as gender,
CAST(lnk_changed.old_age AS int) as age,
CAST(lnk_changed.old_spending_score AS int) as spending_score,
CAST(lnk_changed.old_annual_income_k AS int) as annual_income_k,
coalesce(lnk_changed.old_country, '') as country,
coalesce(lnk_changed.old_customer_type, '') as customer_type,
coalesce(lnk_changed.old_loyalty_tier, '') as loyalty_tier,
coalesce(lnk_changed.old_email, '') as email,
coalesce(lnk_changed.old_city, '') as city,
CAST(lnk_changed.old_attr_hash AS bigint) as attr_hash,
CAST(lnk_changed.old_version_no AS int) as version_no,
CAST(lnk_changed.old_eff_start_key AS int) as eff_start_key,
CAST(REPLACE( date_format(date_add(cast('2011-07-01' as date),- 1), 'yyyy-MM-dd') , '-' , '' ) as int) as eff_end_key,
0 as is_current
FROM
lnk_changed

);

CREATE OR REPLACE TEMP VIEW lnk_newver AS (
SELECT
CAST(lnk_new_or_changed.customer_id AS int) * 1000 + CAST(lnk_new_or_changed.old_version_no AS int) + 1 as customer_sk,
CAST(lnk_new_or_changed.customer_id AS int) as customer_id,
coalesce(lnk_new_or_changed.cust_name, '') as cust_name,
coalesce(lnk_new_or_changed.gender, '') as gender,
CAST(lnk_new_or_changed.age AS int) as age,
CAST(lnk_new_or_changed.spending_score AS int) as spending_score,
CAST(lnk_new_or_changed.annual_income_k AS int) as annual_income_k,
coalesce(lnk_new_or_changed.country, '') as country,
coalesce(lnk_new_or_changed.customer_type, '') as customer_type,
coalesce(lnk_new_or_changed.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_new_or_changed.email, '') as email,
coalesce(lnk_new_or_changed.city, '') as city,
CAST(lnk_new_or_changed.attr_hash AS bigint) as attr_hash,
CAST(lnk_new_or_changed.old_version_no AS int) + 1 as version_no,
IF ( CAST(lnk_new_or_changed.old_version_no AS int) = 0 , 19000101 , CAST(REPLACE( '2011-07-01' , '-' , '' ) as int) ) as eff_start_key,
99991231 as eff_end_key,
1 as is_current
FROM
lnk_new_or_changed

);

CREATE OR REPLACE TEMP VIEW lnk_all_versions AS (
SELECT
CAST(lnk_history.customer_sk AS int) as customer_sk,
CAST(lnk_history.customer_id AS int) as customer_id,
coalesce(lnk_history.cust_name, '') as cust_name,
coalesce(lnk_history.gender, '') as gender,
CAST(lnk_history.age AS int) as age,
CAST(lnk_history.spending_score AS int) as spending_score,
CAST(lnk_history.annual_income_k AS int) as annual_income_k,
coalesce(lnk_history.country, '') as country,
coalesce(lnk_history.customer_type, '') as customer_type,
coalesce(lnk_history.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_history.email, '') as email,
coalesce(lnk_history.city, '') as city,
CAST(lnk_history.attr_hash AS bigint) as attr_hash,
CAST(lnk_history.version_no AS int) as version_no,
CAST(lnk_history.eff_start_key AS int) as eff_start_key,
CAST(lnk_history.eff_end_key AS int) as eff_end_key,
CAST(lnk_history.is_current AS int) as is_current
FROM lnk_history
UNION ALL
SELECT
CAST(lnk_carry.customer_sk AS int) as customer_sk,
CAST(lnk_carry.customer_id AS int) as customer_id,
coalesce(lnk_carry.cust_name, '') as cust_name,
coalesce(lnk_carry.gender, '') as gender,
CAST(lnk_carry.age AS int) as age,
CAST(lnk_carry.spending_score AS int) as spending_score,
CAST(lnk_carry.annual_income_k AS int) as annual_income_k,
coalesce(lnk_carry.country, '') as country,
coalesce(lnk_carry.customer_type, '') as customer_type,
coalesce(lnk_carry.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_carry.email, '') as email,
coalesce(lnk_carry.city, '') as city,
CAST(lnk_carry.attr_hash AS bigint) as attr_hash,
CAST(lnk_carry.version_no AS int) as version_no,
CAST(lnk_carry.eff_start_key AS int) as eff_start_key,
CAST(lnk_carry.eff_end_key AS int) as eff_end_key,
CAST(lnk_carry.is_current AS int) as is_current
FROM lnk_carry
UNION ALL
SELECT
CAST(lnk_expired.customer_sk AS int) as customer_sk,
CAST(lnk_expired.customer_id AS int) as customer_id,
coalesce(lnk_expired.cust_name, '') as cust_name,
coalesce(lnk_expired.gender, '') as gender,
CAST(lnk_expired.age AS int) as age,
CAST(lnk_expired.spending_score AS int) as spending_score,
CAST(lnk_expired.annual_income_k AS int) as annual_income_k,
coalesce(lnk_expired.country, '') as country,
coalesce(lnk_expired.customer_type, '') as customer_type,
coalesce(lnk_expired.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_expired.email, '') as email,
coalesce(lnk_expired.city, '') as city,
CAST(lnk_expired.attr_hash AS bigint) as attr_hash,
CAST(lnk_expired.version_no AS int) as version_no,
CAST(lnk_expired.eff_start_key AS int) as eff_start_key,
CAST(lnk_expired.eff_end_key AS int) as eff_end_key,
CAST(lnk_expired.is_current AS int) as is_current
FROM lnk_expired
UNION ALL
SELECT
CAST(lnk_newver.customer_sk AS int) as customer_sk,
CAST(lnk_newver.customer_id AS int) as customer_id,
coalesce(lnk_newver.cust_name, '') as cust_name,
coalesce(lnk_newver.gender, '') as gender,
CAST(lnk_newver.age AS int) as age,
CAST(lnk_newver.spending_score AS int) as spending_score,
CAST(lnk_newver.annual_income_k AS int) as annual_income_k,
coalesce(lnk_newver.country, '') as country,
coalesce(lnk_newver.customer_type, '') as customer_type,
coalesce(lnk_newver.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_newver.email, '') as email,
coalesce(lnk_newver.city, '') as city,
CAST(lnk_newver.attr_hash AS bigint) as attr_hash,
CAST(lnk_newver.version_no AS int) as version_no,
CAST(lnk_newver.eff_start_key AS int) as eff_start_key,
CAST(lnk_newver.eff_end_key AS int) as eff_end_key,
CAST(lnk_newver.is_current AS int) as is_current
FROM lnk_newver

);

CREATE OR REPLACE TEMP VIEW lnk_sorted AS (
SELECT
CAST(lnk_all_versions.customer_sk AS int) as customer_sk,
CAST(lnk_all_versions.customer_id AS int) as customer_id,
coalesce(lnk_all_versions.cust_name, '') as cust_name,
coalesce(lnk_all_versions.gender, '') as gender,
CAST(lnk_all_versions.age AS int) as age,
CAST(lnk_all_versions.spending_score AS int) as spending_score,
CAST(lnk_all_versions.annual_income_k AS int) as annual_income_k,
coalesce(lnk_all_versions.country, '') as country,
coalesce(lnk_all_versions.customer_type, '') as customer_type,
coalesce(lnk_all_versions.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_all_versions.email, '') as email,
coalesce(lnk_all_versions.city, '') as city,
CAST(lnk_all_versions.attr_hash AS bigint) as attr_hash,
CAST(lnk_all_versions.version_no AS int) as version_no,
CAST(lnk_all_versions.eff_start_key AS int) as eff_start_key,
CAST(lnk_all_versions.eff_end_key AS int) as eff_end_key,
CAST(lnk_all_versions.is_current AS int) as is_current
FROM
lnk_all_versions
ORDER BY customer_id asc, version_no asc

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.dim_customer_next AS
SELECT
  *
FROM lnk_sorted;

CREATE OR REPLACE TEMP VIEW lnk_change_counts AS (
SELECT
coalesce(lnk_audit.change_flag, '') as change_flag,
COUNT(*) as customers
FROM
lnk_audit
GROUP BY
coalesce(lnk_audit.change_flag, '')

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.audit_scd2_customer AS
SELECT
  *
FROM lnk_change_counts;