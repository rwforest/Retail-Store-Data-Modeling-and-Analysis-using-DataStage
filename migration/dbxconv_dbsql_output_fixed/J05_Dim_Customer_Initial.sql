SELECT
.customer_id * 1000 + 1 as customer_sk,
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
1 as version_no,
19000101 as eff_start_key,
99991231 as eff_end_key,
1 as is_current
FROM


SELECT
lnk_v1.customer_sk as customer_sk,
lnk_v1.customer_id as customer_id,
lnk_v1.cust_name as cust_name,
lnk_v1.gender as gender,
lnk_v1.age as age,
lnk_v1.spending_score as spending_score,
lnk_v1.annual_income_k as annual_income_k,
lnk_v1.country as country,
lnk_v1.customer_type as customer_type,
lnk_v1.loyalty_tier as loyalty_tier,
lnk_v1.email as email,
lnk_v1.city as city,
lnk_v1.attr_hash as attr_hash,
lnk_v1.version_no as version_no,
lnk_v1.eff_start_key as eff_start_key,
lnk_v1.eff_end_key as eff_end_key,
lnk_v1.is_current as is_current
FROM
lnk_v1
ORDER BY customer_id asc, version_no asc

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

