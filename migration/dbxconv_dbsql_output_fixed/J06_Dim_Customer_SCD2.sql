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


SELECT
lnk_dim_a.customer_sk as customer_sk,
lnk_dim_a.customer_id as customer_id,
lnk_dim_a.cust_name as cust_name,
lnk_dim_a.gender as gender,
lnk_dim_a.age as age,
lnk_dim_a.spending_score as spending_score,
lnk_dim_a.annual_income_k as annual_income_k,
lnk_dim_a.country as country,
lnk_dim_a.customer_type as customer_type,
lnk_dim_a.loyalty_tier as loyalty_tier,
lnk_dim_a.email as email,
lnk_dim_a.city as city,
lnk_dim_a.attr_hash as attr_hash,
lnk_dim_a.version_no as version_no,
lnk_dim_a.eff_start_key as eff_start_key,
lnk_dim_a.eff_end_key as eff_end_key,
lnk_dim_a.is_current as is_current
FROM
lnk_dim_a
WHERE is_current = 1

SELECT
lnk_dim_b.customer_sk as customer_sk,
lnk_dim_b.customer_id as customer_id,
lnk_dim_b.cust_name as cust_name,
lnk_dim_b.gender as gender,
lnk_dim_b.age as age,
lnk_dim_b.spending_score as spending_score,
lnk_dim_b.annual_income_k as annual_income_k,
lnk_dim_b.country as country,
lnk_dim_b.customer_type as customer_type,
lnk_dim_b.loyalty_tier as loyalty_tier,
lnk_dim_b.email as email,
lnk_dim_b.city as city,
lnk_dim_b.attr_hash as attr_hash,
lnk_dim_b.version_no as version_no,
lnk_dim_b.eff_start_key as eff_start_key,
lnk_dim_b.eff_end_key as eff_end_key,
lnk_dim_b.is_current as is_current
FROM
lnk_dim_b
WHERE is_current = 0

SELECT
lnk_current.customer_id as customer_id,
lnk_current.customer_sk as old_customer_sk,
lnk_current.cust_name as old_cust_name,
lnk_current.gender as old_gender,
lnk_current.age as old_age,
lnk_current.spending_score as old_spending_score,
lnk_current.annual_income_k as old_annual_income_k,
lnk_current.country as old_country,
lnk_current.customer_type as old_customer_type,
lnk_current.loyalty_tier as old_loyalty_tier,
lnk_current.email as old_email,
lnk_current.city as old_city,
lnk_current.attr_hash as old_attr_hash,
lnk_current.version_no as old_version_no,
lnk_current.eff_start_key as old_eff_start_key,
lnk_current.eff_end_key as old_eff_end_key,
lnk_current.is_current as old_is_current
FROM
lnk_current

SELECT
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
.snapshot_date as snapshot_date,
.attr_hash as attr_hash,
lnk_old.old_customer_sk as old_customer_sk,
lnk_old.old_cust_name as old_cust_name,
lnk_old.old_gender as old_gender,
lnk_old.old_age as old_age,
lnk_old.old_spending_score as old_spending_score,
lnk_old.old_annual_income_k as old_annual_income_k,
lnk_old.old_country as old_country,
lnk_old.old_customer_type as old_customer_type,
lnk_old.old_loyalty_tier as old_loyalty_tier,
lnk_old.old_email as old_email,
lnk_old.old_city as old_city,
lnk_old.old_attr_hash as old_attr_hash,
lnk_old.old_version_no as old_version_no,
lnk_old.old_eff_start_key as old_eff_start_key,
lnk_old.old_eff_end_key as old_eff_end_key,
lnk_old.old_is_current as old_is_current
FROM

LEFT JOIN lnk_old ON .customer_id = lnk_old.customer_id

SELECT
lnk_cmp.customer_id as customer_id,
lnk_cmp.cust_name as cust_name,
lnk_cmp.gender as gender,
lnk_cmp.age as age,
lnk_cmp.spending_score as spending_score,
lnk_cmp.annual_income_k as annual_income_k,
lnk_cmp.country as country,
lnk_cmp.customer_type as customer_type,
lnk_cmp.loyalty_tier as loyalty_tier,
lnk_cmp.email as email,
lnk_cmp.city as city,
lnk_cmp.snapshot_date as snapshot_date,
lnk_cmp.attr_hash as attr_hash,
NullToValue ( lnk_cmp.old_customer_sk , 0 ) as old_customer_sk,
NullToValue ( lnk_cmp.old_cust_name , '' ) as old_cust_name,
NullToValue ( lnk_cmp.old_gender , '' ) as old_gender,
NullToValue ( lnk_cmp.old_age , 0 ) as old_age,
NullToValue ( lnk_cmp.old_spending_score , 0 ) as old_spending_score,
NullToValue ( lnk_cmp.old_annual_income_k , 0 ) as old_annual_income_k,
NullToValue ( lnk_cmp.old_country , '' ) as old_country,
NullToValue ( lnk_cmp.old_customer_type , '' ) as old_customer_type,
NullToValue ( lnk_cmp.old_loyalty_tier , '' ) as old_loyalty_tier,
NullToValue ( lnk_cmp.old_email , '' ) as old_email,
NullToValue ( lnk_cmp.old_city , '' ) as old_city,
NullToValue ( lnk_cmp.old_attr_hash , 0 ) as old_attr_hash,
NullToValue ( lnk_cmp.old_version_no , 0 ) as old_version_no,
NullToValue ( lnk_cmp.old_eff_start_key , 0 ) as old_eff_start_key,
NullToValue ( lnk_cmp.old_eff_end_key , 0 ) as old_eff_end_key,
NullToValue ( lnk_cmp.old_is_current , 0 ) as old_is_current,
IF ( lnk_cmp.old_attr_hash IS NULL , 'N' , IF ( lnk_cmp.old_attr_hash <> lnk_cmp.attr_hash , 'C' , 'U' ) ) as change_flag
FROM
lnk_cmp

SELECT
lnk_detected.customer_id as customer_id,
lnk_detected.cust_name as cust_name,
lnk_detected.gender as gender,
lnk_detected.age as age,
lnk_detected.spending_score as spending_score,
lnk_detected.annual_income_k as annual_income_k,
lnk_detected.country as country,
lnk_detected.customer_type as customer_type,
lnk_detected.loyalty_tier as loyalty_tier,
lnk_detected.email as email,
lnk_detected.city as city,
lnk_detected.snapshot_date as snapshot_date,
lnk_detected.attr_hash as attr_hash,
lnk_detected.old_customer_sk as old_customer_sk,
lnk_detected.old_cust_name as old_cust_name,
lnk_detected.old_gender as old_gender,
lnk_detected.old_age as old_age,
lnk_detected.old_spending_score as old_spending_score,
lnk_detected.old_annual_income_k as old_annual_income_k,
lnk_detected.old_country as old_country,
lnk_detected.old_customer_type as old_customer_type,
lnk_detected.old_loyalty_tier as old_loyalty_tier,
lnk_detected.old_email as old_email,
lnk_detected.old_city as old_city,
lnk_detected.old_attr_hash as old_attr_hash,
lnk_detected.old_version_no as old_version_no,
lnk_detected.old_eff_start_key as old_eff_start_key,
lnk_detected.old_eff_end_key as old_eff_end_key,
lnk_detected.old_is_current as old_is_current,
lnk_detected.change_flag as change_flag
FROM
lnk_detected

SELECT
lnk_detected.customer_id as customer_id,
lnk_detected.cust_name as cust_name,
lnk_detected.gender as gender,
lnk_detected.age as age,
lnk_detected.spending_score as spending_score,
lnk_detected.annual_income_k as annual_income_k,
lnk_detected.country as country,
lnk_detected.customer_type as customer_type,
lnk_detected.loyalty_tier as loyalty_tier,
lnk_detected.email as email,
lnk_detected.city as city,
lnk_detected.snapshot_date as snapshot_date,
lnk_detected.attr_hash as attr_hash,
lnk_detected.old_customer_sk as old_customer_sk,
lnk_detected.old_cust_name as old_cust_name,
lnk_detected.old_gender as old_gender,
lnk_detected.old_age as old_age,
lnk_detected.old_spending_score as old_spending_score,
lnk_detected.old_annual_income_k as old_annual_income_k,
lnk_detected.old_country as old_country,
lnk_detected.old_customer_type as old_customer_type,
lnk_detected.old_loyalty_tier as old_loyalty_tier,
lnk_detected.old_email as old_email,
lnk_detected.old_city as old_city,
lnk_detected.old_attr_hash as old_attr_hash,
lnk_detected.old_version_no as old_version_no,
lnk_detected.old_eff_start_key as old_eff_start_key,
lnk_detected.old_eff_end_key as old_eff_end_key,
lnk_detected.old_is_current as old_is_current,
lnk_detected.change_flag as change_flag
FROM
lnk_detected

SELECT
lnk_detected.customer_id as customer_id,
lnk_detected.cust_name as cust_name,
lnk_detected.gender as gender,
lnk_detected.age as age,
lnk_detected.spending_score as spending_score,
lnk_detected.annual_income_k as annual_income_k,
lnk_detected.country as country,
lnk_detected.customer_type as customer_type,
lnk_detected.loyalty_tier as loyalty_tier,
lnk_detected.email as email,
lnk_detected.city as city,
lnk_detected.snapshot_date as snapshot_date,
lnk_detected.attr_hash as attr_hash,
lnk_detected.old_customer_sk as old_customer_sk,
lnk_detected.old_cust_name as old_cust_name,
lnk_detected.old_gender as old_gender,
lnk_detected.old_age as old_age,
lnk_detected.old_spending_score as old_spending_score,
lnk_detected.old_annual_income_k as old_annual_income_k,
lnk_detected.old_country as old_country,
lnk_detected.old_customer_type as old_customer_type,
lnk_detected.old_loyalty_tier as old_loyalty_tier,
lnk_detected.old_email as old_email,
lnk_detected.old_city as old_city,
lnk_detected.old_attr_hash as old_attr_hash,
lnk_detected.old_version_no as old_version_no,
lnk_detected.old_eff_start_key as old_eff_start_key,
lnk_detected.old_eff_end_key as old_eff_end_key,
lnk_detected.old_is_current as old_is_current,
lnk_detected.change_flag as change_flag
FROM
lnk_detected

SELECT
lnk_detected.customer_id as customer_id,
lnk_detected.cust_name as cust_name,
lnk_detected.gender as gender,
lnk_detected.age as age,
lnk_detected.spending_score as spending_score,
lnk_detected.annual_income_k as annual_income_k,
lnk_detected.country as country,
lnk_detected.customer_type as customer_type,
lnk_detected.loyalty_tier as loyalty_tier,
lnk_detected.email as email,
lnk_detected.city as city,
lnk_detected.snapshot_date as snapshot_date,
lnk_detected.attr_hash as attr_hash,
lnk_detected.old_customer_sk as old_customer_sk,
lnk_detected.old_cust_name as old_cust_name,
lnk_detected.old_gender as old_gender,
lnk_detected.old_age as old_age,
lnk_detected.old_spending_score as old_spending_score,
lnk_detected.old_annual_income_k as old_annual_income_k,
lnk_detected.old_country as old_country,
lnk_detected.old_customer_type as old_customer_type,
lnk_detected.old_loyalty_tier as old_loyalty_tier,
lnk_detected.old_email as old_email,
lnk_detected.old_city as old_city,
lnk_detected.old_attr_hash as old_attr_hash,
lnk_detected.old_version_no as old_version_no,
lnk_detected.old_eff_start_key as old_eff_start_key,
lnk_detected.old_eff_end_key as old_eff_end_key,
lnk_detected.old_is_current as old_is_current,
lnk_detected.change_flag as change_flag
FROM
lnk_detected

SELECT
lnk_c.customer_id as customer_id,
lnk_c.cust_name as cust_name,
lnk_c.gender as gender,
lnk_c.age as age,
lnk_c.spending_score as spending_score,
lnk_c.annual_income_k as annual_income_k,
lnk_c.country as country,
lnk_c.customer_type as customer_type,
lnk_c.loyalty_tier as loyalty_tier,
lnk_c.email as email,
lnk_c.city as city,
lnk_c.snapshot_date as snapshot_date,
lnk_c.attr_hash as attr_hash,
lnk_c.old_customer_sk as old_customer_sk,
lnk_c.old_cust_name as old_cust_name,
lnk_c.old_gender as old_gender,
lnk_c.old_age as old_age,
lnk_c.old_spending_score as old_spending_score,
lnk_c.old_annual_income_k as old_annual_income_k,
lnk_c.old_country as old_country,
lnk_c.old_customer_type as old_customer_type,
lnk_c.old_loyalty_tier as old_loyalty_tier,
lnk_c.old_email as old_email,
lnk_c.old_city as old_city,
lnk_c.old_attr_hash as old_attr_hash,
lnk_c.old_version_no as old_version_no,
lnk_c.old_eff_start_key as old_eff_start_key,
lnk_c.old_eff_end_key as old_eff_end_key,
lnk_c.old_is_current as old_is_current,
lnk_c.change_flag as change_flag
FROM
lnk_c
WHERE change_flag = 'C'

SELECT
lnk_nc.customer_id as customer_id,
lnk_nc.cust_name as cust_name,
lnk_nc.gender as gender,
lnk_nc.age as age,
lnk_nc.spending_score as spending_score,
lnk_nc.annual_income_k as annual_income_k,
lnk_nc.country as country,
lnk_nc.customer_type as customer_type,
lnk_nc.loyalty_tier as loyalty_tier,
lnk_nc.email as email,
lnk_nc.city as city,
lnk_nc.snapshot_date as snapshot_date,
lnk_nc.attr_hash as attr_hash,
lnk_nc.old_customer_sk as old_customer_sk,
lnk_nc.old_cust_name as old_cust_name,
lnk_nc.old_gender as old_gender,
lnk_nc.old_age as old_age,
lnk_nc.old_spending_score as old_spending_score,
lnk_nc.old_annual_income_k as old_annual_income_k,
lnk_nc.old_country as old_country,
lnk_nc.old_customer_type as old_customer_type,
lnk_nc.old_loyalty_tier as old_loyalty_tier,
lnk_nc.old_email as old_email,
lnk_nc.old_city as old_city,
lnk_nc.old_attr_hash as old_attr_hash,
lnk_nc.old_version_no as old_version_no,
lnk_nc.old_eff_start_key as old_eff_start_key,
lnk_nc.old_eff_end_key as old_eff_end_key,
lnk_nc.old_is_current as old_is_current,
lnk_nc.change_flag as change_flag
FROM
lnk_nc
WHERE change_flag <> 'U'

SELECT
lnk_u.customer_id as customer_id,
lnk_u.cust_name as cust_name,
lnk_u.gender as gender,
lnk_u.age as age,
lnk_u.spending_score as spending_score,
lnk_u.annual_income_k as annual_income_k,
lnk_u.country as country,
lnk_u.customer_type as customer_type,
lnk_u.loyalty_tier as loyalty_tier,
lnk_u.email as email,
lnk_u.city as city,
lnk_u.snapshot_date as snapshot_date,
lnk_u.attr_hash as attr_hash,
lnk_u.old_customer_sk as old_customer_sk,
lnk_u.old_cust_name as old_cust_name,
lnk_u.old_gender as old_gender,
lnk_u.old_age as old_age,
lnk_u.old_spending_score as old_spending_score,
lnk_u.old_annual_income_k as old_annual_income_k,
lnk_u.old_country as old_country,
lnk_u.old_customer_type as old_customer_type,
lnk_u.old_loyalty_tier as old_loyalty_tier,
lnk_u.old_email as old_email,
lnk_u.old_city as old_city,
lnk_u.old_attr_hash as old_attr_hash,
lnk_u.old_version_no as old_version_no,
lnk_u.old_eff_start_key as old_eff_start_key,
lnk_u.old_eff_end_key as old_eff_end_key,
lnk_u.old_is_current as old_is_current,
lnk_u.change_flag as change_flag
FROM
lnk_u
WHERE change_flag = 'U'

SELECT
lnk_unchanged.old_customer_sk as customer_sk,
lnk_unchanged.customer_id as customer_id,
lnk_unchanged.old_cust_name as cust_name,
lnk_unchanged.old_gender as gender,
lnk_unchanged.old_age as age,
lnk_unchanged.old_spending_score as spending_score,
lnk_unchanged.old_annual_income_k as annual_income_k,
lnk_unchanged.old_country as country,
lnk_unchanged.old_customer_type as customer_type,
lnk_unchanged.old_loyalty_tier as loyalty_tier,
lnk_unchanged.old_email as email,
lnk_unchanged.old_city as city,
lnk_unchanged.old_attr_hash as attr_hash,
lnk_unchanged.old_version_no as version_no,
lnk_unchanged.old_eff_start_key as eff_start_key,
lnk_unchanged.old_eff_end_key as eff_end_key,
lnk_unchanged.old_is_current as is_current
FROM
lnk_unchanged

SELECT
lnk_changed.old_customer_sk as customer_sk,
lnk_changed.customer_id as customer_id,
lnk_changed.old_cust_name as cust_name,
lnk_changed.old_gender as gender,
lnk_changed.old_age as age,
lnk_changed.old_spending_score as spending_score,
lnk_changed.old_annual_income_k as annual_income_k,
lnk_changed.old_country as country,
lnk_changed.old_customer_type as customer_type,
lnk_changed.old_loyalty_tier as loyalty_tier,
lnk_changed.old_email as email,
lnk_changed.old_city as city,
lnk_changed.old_attr_hash as attr_hash,
lnk_changed.old_version_no as version_no,
lnk_changed.old_eff_start_key as eff_start_key,
CAST(REPLACE( date_format(date_add(cast('{pEffDate}' as date),- 1), 'yyyy-MM-dd') , '-' , '' ) as int) as eff_end_key,
0 as is_current
FROM
lnk_changed

SELECT
lnk_new_or_changed.customer_id * 1000 + lnk_new_or_changed.old_version_no + 1 as customer_sk,
lnk_new_or_changed.customer_id as customer_id,
lnk_new_or_changed.cust_name as cust_name,
lnk_new_or_changed.gender as gender,
lnk_new_or_changed.age as age,
lnk_new_or_changed.spending_score as spending_score,
lnk_new_or_changed.annual_income_k as annual_income_k,
lnk_new_or_changed.country as country,
lnk_new_or_changed.customer_type as customer_type,
lnk_new_or_changed.loyalty_tier as loyalty_tier,
lnk_new_or_changed.email as email,
lnk_new_or_changed.city as city,
lnk_new_or_changed.attr_hash as attr_hash,
lnk_new_or_changed.old_version_no + 1 as version_no,
IF ( lnk_new_or_changed.old_version_no = 0 , 19000101 , CAST(REPLACE( '{pEffDate}' , '-' , '' ) as int) ) as eff_start_key,
99991231 as eff_end_key,
1 as is_current
FROM
lnk_new_or_changed

SELECT
lnk_audit.change_flag as change_flag,
MIN(customers) as customers
FROM
lnk_audit
GROUP BY
lnk_audit.change_flag

SELECT
lnk_history.customer_sk as customer_sk,
lnk_history.customer_id as customer_id,
lnk_history.cust_name as cust_name,
lnk_history.gender as gender,
lnk_history.age as age,
lnk_history.spending_score as spending_score,
lnk_history.annual_income_k as annual_income_k,
lnk_history.country as country,
lnk_history.customer_type as customer_type,
lnk_history.loyalty_tier as loyalty_tier,
lnk_history.email as email,
lnk_history.city as city,
lnk_history.attr_hash as attr_hash,
lnk_history.version_no as version_no,
lnk_history.eff_start_key as eff_start_key,
lnk_history.eff_end_key as eff_end_key,
lnk_history.is_current as is_current
FROM lnk_history

SELECT
lnk_all_versions.customer_sk as customer_sk,
lnk_all_versions.customer_id as customer_id,
lnk_all_versions.cust_name as cust_name,
lnk_all_versions.gender as gender,
lnk_all_versions.age as age,
lnk_all_versions.spending_score as spending_score,
lnk_all_versions.annual_income_k as annual_income_k,
lnk_all_versions.country as country,
lnk_all_versions.customer_type as customer_type,
lnk_all_versions.loyalty_tier as loyalty_tier,
lnk_all_versions.email as email,
lnk_all_versions.city as city,
lnk_all_versions.attr_hash as attr_hash,
lnk_all_versions.version_no as version_no,
lnk_all_versions.eff_start_key as eff_start_key,
lnk_all_versions.eff_end_key as eff_end_key,
lnk_all_versions.is_current as is_current
FROM
lnk_all_versions
ORDER BY customer_id asc, version_no asc

SELECT CONCAT_WS(',',
lnk_change_counts.change_flag,
lnk_change_counts.customers)
FROM
lnk_change_counts



SELECT CONCAT_WS(',',
lnk_sorted.customer_sk,
lnk_sorted.customer_id,
lnk_sorted.cust_name,
lnk_sorted.gender,
lnk_sorted.age,
lnk_sorted.spending_score,
lnk_sorted.annual_income_k,
lnk_sorted.country,
lnk_sorted.customer_type,
lnk_sorted.loyalty_tier,
lnk_sorted.email,
lnk_sorted.city,
lnk_sorted.attr_hash,
lnk_sorted.version_no,
lnk_sorted.eff_start_key,
lnk_sorted.eff_end_key,
lnk_sorted.is_current)
FROM
lnk_sorted

