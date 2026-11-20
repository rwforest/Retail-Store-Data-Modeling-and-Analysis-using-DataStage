SELECT
.CustomerID as customer_id,
UPPER(trim(.CustomerName)) as cust_name,
trim(.Gender) as gender,
.Age as age,
.SpendingScore as spending_score,
.AnnualIncomeK as annual_income_k,
trim(.Country) as country,
trim(.CustomerType) as customer_type,
UPPER(trim(.LoyaltyTier)) as loyalty_tier,
LOWER(trim(.Email)) as email,
trim(.City) as city,
cast(.SnapshotDate as date) as snapshot_date,
CRC32 ( UPPER(trim(.CustomerName)) || '|' || .Age || '|' || .SpendingScore || '|' || .AnnualIncomeK || '|' || trim(.Country) || '|' || UPPER(trim(.LoyaltyTier)) || '|' || trim(.City) || '|' || LOWER(trim(.Email)) ) as attr_hash
FROM


SELECT
.CustomerID as customer_id,
UPPER(trim(.CustomerName)) as cust_name,
trim(.Gender) as gender,
.Age as age,
.SpendingScore as spending_score,
.AnnualIncomeK as annual_income_k,
trim(.Country) as country,
trim(.CustomerType) as customer_type,
UPPER(trim(.LoyaltyTier)) as loyalty_tier,
LOWER(trim(.Email)) as email,
trim(.City) as city,
cast(.SnapshotDate as date) as snapshot_date,
CRC32 ( UPPER(trim(.CustomerName)) || '|' || .Age || '|' || .SpendingScore || '|' || .AnnualIncomeK || '|' || trim(.Country) || '|' || UPPER(trim(.LoyaltyTier)) || '|' || trim(.City) || '|' || LOWER(trim(.Email)) ) as attr_hash
FROM


SELECT
customer_id,
cust_name,
gender,
age,
spending_score,
annual_income_k,
country,
customer_type,
loyalty_tier,
email,
city,
snapshot_date,
attr_hash
FROM (
SELECT
lnk_std_2010.customer_id as customer_id,
lnk_std_2010.cust_name as cust_name,
lnk_std_2010.gender as gender,
lnk_std_2010.age as age,
lnk_std_2010.spending_score as spending_score,
lnk_std_2010.annual_income_k as annual_income_k,
lnk_std_2010.country as country,
lnk_std_2010.customer_type as customer_type,
lnk_std_2010.loyalty_tier as loyalty_tier,
lnk_std_2010.email as email,
lnk_std_2010.city as city,
lnk_std_2010.snapshot_date as snapshot_date,
lnk_std_2010.attr_hash as attr_hash,
ROW_NUMBER() OVER(PARTITION BY lnk_std_2010.customer_id ORDER BY lnk_std_2010.cust_name, lnk_std_2010.gender, lnk_std_2010.age, lnk_std_2010.spending_score, lnk_std_2010.annual_income_k, lnk_std_2010.country, lnk_std_2010.customer_type, lnk_std_2010.loyalty_tier, lnk_std_2010.email, lnk_std_2010.city, lnk_std_2010.snapshot_date, lnk_std_2010.attr_hash) dedup_rnk
FROM lnk_std_2010) dedup_lnk_dedup_2010 WHERE dedup_rnk = 1

SELECT
customer_id,
cust_name,
gender,
age,
spending_score,
annual_income_k,
country,
customer_type,
loyalty_tier,
email,
city,
snapshot_date,
attr_hash
FROM (
SELECT
lnk_std_2011.customer_id as customer_id,
lnk_std_2011.cust_name as cust_name,
lnk_std_2011.gender as gender,
lnk_std_2011.age as age,
lnk_std_2011.spending_score as spending_score,
lnk_std_2011.annual_income_k as annual_income_k,
lnk_std_2011.country as country,
lnk_std_2011.customer_type as customer_type,
lnk_std_2011.loyalty_tier as loyalty_tier,
lnk_std_2011.email as email,
lnk_std_2011.city as city,
lnk_std_2011.snapshot_date as snapshot_date,
lnk_std_2011.attr_hash as attr_hash,
ROW_NUMBER() OVER(PARTITION BY lnk_std_2011.customer_id ORDER BY lnk_std_2011.cust_name, lnk_std_2011.gender, lnk_std_2011.age, lnk_std_2011.spending_score, lnk_std_2011.annual_income_k, lnk_std_2011.country, lnk_std_2011.customer_type, lnk_std_2011.loyalty_tier, lnk_std_2011.email, lnk_std_2011.city, lnk_std_2011.snapshot_date, lnk_std_2011.attr_hash) dedup_rnk
FROM lnk_std_2011) dedup_lnk_dedup_2011 WHERE dedup_rnk = 1

SELECT CONCAT_WS(',',
lnk_dedup_2010.customer_id,
lnk_dedup_2010.cust_name,
lnk_dedup_2010.gender,
lnk_dedup_2010.age,
lnk_dedup_2010.spending_score,
lnk_dedup_2010.annual_income_k,
lnk_dedup_2010.country,
lnk_dedup_2010.customer_type,
lnk_dedup_2010.loyalty_tier,
lnk_dedup_2010.email,
lnk_dedup_2010.city,
lnk_dedup_2010.snapshot_date,
lnk_dedup_2010.attr_hash)
FROM
lnk_dedup_2010



SELECT CONCAT_WS(',',
lnk_dedup_2011.customer_id,
lnk_dedup_2011.cust_name,
lnk_dedup_2011.gender,
lnk_dedup_2011.age,
lnk_dedup_2011.spending_score,
lnk_dedup_2011.annual_income_k,
lnk_dedup_2011.country,
lnk_dedup_2011.customer_type,
lnk_dedup_2011.loyalty_tier,
lnk_dedup_2011.email,
lnk_dedup_2011.city,
lnk_dedup_2011.snapshot_date,
lnk_dedup_2011.attr_hash)
FROM
lnk_dedup_2011

