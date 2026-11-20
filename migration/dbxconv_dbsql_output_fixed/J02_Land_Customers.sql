CREATE OR REPLACE TEMP VIEW lnk_raw_2010 AS (
  SELECT CAST(_c0 AS int) AS CustomerID, _c1 AS CustomerName, _c2 AS Gender, CAST(_c3 AS int) AS Age, CAST(_c4 AS int) AS SpendingScore, CAST(_c5 AS int) AS AnnualIncomeK, _c6 AS Country, _c7 AS CustomerType, _c8 AS LoyaltyTier, _c9 AS Email, _c10 AS City, _c11 AS SnapshotDate
  FROM read_files('/Volumes/unitygo/retail_dw/landing/customers_snapshot_2010.csv', format => 'csv', header => true,
    schema => '_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string', escape => '"')
);

CREATE OR REPLACE TEMP VIEW lnk_std_2010 AS (
SELECT
CAST(lnk_raw_2010.CustomerID AS int) as customer_id,
UPPER(trim(coalesce(lnk_raw_2010.CustomerName, ''))) as cust_name,
trim(coalesce(lnk_raw_2010.Gender, '')) as gender,
CAST(lnk_raw_2010.Age AS int) as age,
CAST(lnk_raw_2010.SpendingScore AS int) as spending_score,
CAST(lnk_raw_2010.AnnualIncomeK AS int) as annual_income_k,
trim(coalesce(lnk_raw_2010.Country, '')) as country,
trim(coalesce(lnk_raw_2010.CustomerType, '')) as customer_type,
UPPER(trim(coalesce(lnk_raw_2010.LoyaltyTier, ''))) as loyalty_tier,
LOWER(trim(coalesce(lnk_raw_2010.Email, ''))) as email,
trim(coalesce(lnk_raw_2010.City, '')) as city,
to_date(cast(coalesce(lnk_raw_2010.SnapshotDate, '') as string), 'yyyy-MM-dd') as snapshot_date,
CRC32 ( UPPER(trim(coalesce(lnk_raw_2010.CustomerName, ''))) || '|' || CAST(lnk_raw_2010.Age AS int) || '|' || CAST(lnk_raw_2010.SpendingScore AS int) || '|' || CAST(lnk_raw_2010.AnnualIncomeK AS int) || '|' || trim(coalesce(lnk_raw_2010.Country, '')) || '|' || UPPER(trim(coalesce(lnk_raw_2010.LoyaltyTier, ''))) || '|' || trim(coalesce(lnk_raw_2010.City, '')) || '|' || LOWER(trim(coalesce(lnk_raw_2010.Email, ''))) ) as attr_hash
FROM lnk_raw_2010
);

CREATE OR REPLACE TEMP VIEW lnk_dedup_2010 AS (
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
CAST(lnk_std_2010.customer_id AS int) as customer_id,
coalesce(lnk_std_2010.cust_name, '') as cust_name,
coalesce(lnk_std_2010.gender, '') as gender,
CAST(lnk_std_2010.age AS int) as age,
CAST(lnk_std_2010.spending_score AS int) as spending_score,
CAST(lnk_std_2010.annual_income_k AS int) as annual_income_k,
coalesce(lnk_std_2010.country, '') as country,
coalesce(lnk_std_2010.customer_type, '') as customer_type,
coalesce(lnk_std_2010.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_std_2010.email, '') as email,
coalesce(lnk_std_2010.city, '') as city,
lnk_std_2010.snapshot_date as snapshot_date,
CAST(lnk_std_2010.attr_hash AS bigint) as attr_hash,
ROW_NUMBER() OVER(PARTITION BY CAST(lnk_std_2010.customer_id AS int) ORDER BY coalesce(lnk_std_2010.cust_name, ''), coalesce(lnk_std_2010.gender, ''), CAST(lnk_std_2010.age AS int), CAST(lnk_std_2010.spending_score AS int), CAST(lnk_std_2010.annual_income_k AS int), coalesce(lnk_std_2010.country, ''), coalesce(lnk_std_2010.customer_type, ''), coalesce(lnk_std_2010.loyalty_tier, ''), coalesce(lnk_std_2010.email, ''), coalesce(lnk_std_2010.city, ''), lnk_std_2010.snapshot_date, CAST(lnk_std_2010.attr_hash AS bigint)) dedup_rnk
FROM lnk_std_2010) dedup_lnk_dedup_2010 WHERE dedup_rnk = 1

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.stg_customers_2010 AS
SELECT
  *
FROM lnk_dedup_2010;

CREATE OR REPLACE TEMP VIEW lnk_raw_2011 AS (
  SELECT CAST(_c0 AS int) AS CustomerID, _c1 AS CustomerName, _c2 AS Gender, CAST(_c3 AS int) AS Age, CAST(_c4 AS int) AS SpendingScore, CAST(_c5 AS int) AS AnnualIncomeK, _c6 AS Country, _c7 AS CustomerType, _c8 AS LoyaltyTier, _c9 AS Email, _c10 AS City, _c11 AS SnapshotDate
  FROM read_files('/Volumes/unitygo/retail_dw/landing/customers_snapshot_2011.csv', format => 'csv', header => true,
    schema => '_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string', escape => '"')
);

CREATE OR REPLACE TEMP VIEW lnk_std_2011 AS (
SELECT
CAST(lnk_raw_2011.CustomerID AS int) as customer_id,
UPPER(trim(coalesce(lnk_raw_2011.CustomerName, ''))) as cust_name,
trim(coalesce(lnk_raw_2011.Gender, '')) as gender,
CAST(lnk_raw_2011.Age AS int) as age,
CAST(lnk_raw_2011.SpendingScore AS int) as spending_score,
CAST(lnk_raw_2011.AnnualIncomeK AS int) as annual_income_k,
trim(coalesce(lnk_raw_2011.Country, '')) as country,
trim(coalesce(lnk_raw_2011.CustomerType, '')) as customer_type,
UPPER(trim(coalesce(lnk_raw_2011.LoyaltyTier, ''))) as loyalty_tier,
LOWER(trim(coalesce(lnk_raw_2011.Email, ''))) as email,
trim(coalesce(lnk_raw_2011.City, '')) as city,
to_date(cast(coalesce(lnk_raw_2011.SnapshotDate, '') as string), 'yyyy-MM-dd') as snapshot_date,
CRC32 ( UPPER(trim(coalesce(lnk_raw_2011.CustomerName, ''))) || '|' || CAST(lnk_raw_2011.Age AS int) || '|' || CAST(lnk_raw_2011.SpendingScore AS int) || '|' || CAST(lnk_raw_2011.AnnualIncomeK AS int) || '|' || trim(coalesce(lnk_raw_2011.Country, '')) || '|' || UPPER(trim(coalesce(lnk_raw_2011.LoyaltyTier, ''))) || '|' || trim(coalesce(lnk_raw_2011.City, '')) || '|' || LOWER(trim(coalesce(lnk_raw_2011.Email, ''))) ) as attr_hash
FROM lnk_raw_2011
);

CREATE OR REPLACE TEMP VIEW lnk_dedup_2011 AS (
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
CAST(lnk_std_2011.customer_id AS int) as customer_id,
coalesce(lnk_std_2011.cust_name, '') as cust_name,
coalesce(lnk_std_2011.gender, '') as gender,
CAST(lnk_std_2011.age AS int) as age,
CAST(lnk_std_2011.spending_score AS int) as spending_score,
CAST(lnk_std_2011.annual_income_k AS int) as annual_income_k,
coalesce(lnk_std_2011.country, '') as country,
coalesce(lnk_std_2011.customer_type, '') as customer_type,
coalesce(lnk_std_2011.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_std_2011.email, '') as email,
coalesce(lnk_std_2011.city, '') as city,
lnk_std_2011.snapshot_date as snapshot_date,
CAST(lnk_std_2011.attr_hash AS bigint) as attr_hash,
ROW_NUMBER() OVER(PARTITION BY CAST(lnk_std_2011.customer_id AS int) ORDER BY coalesce(lnk_std_2011.cust_name, ''), coalesce(lnk_std_2011.gender, ''), CAST(lnk_std_2011.age AS int), CAST(lnk_std_2011.spending_score AS int), CAST(lnk_std_2011.annual_income_k AS int), coalesce(lnk_std_2011.country, ''), coalesce(lnk_std_2011.customer_type, ''), coalesce(lnk_std_2011.loyalty_tier, ''), coalesce(lnk_std_2011.email, ''), coalesce(lnk_std_2011.city, ''), lnk_std_2011.snapshot_date, CAST(lnk_std_2011.attr_hash AS bigint)) dedup_rnk
FROM lnk_std_2011) dedup_lnk_dedup_2011 WHERE dedup_rnk = 1

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.stg_customers_2011 AS
SELECT
  *
FROM lnk_dedup_2011;