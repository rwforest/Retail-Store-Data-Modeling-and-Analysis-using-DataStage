CREATE OR REPLACE TEMP VIEW lnk_mart AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.RETAIL_DATA_MART
);

CREATE OR REPLACE TEMP VIEW lnk_cust_profit AS (
SELECT
CAST(lnk_mart.customer_id AS int) as customer_id,
SUM(gross_profit) as profit_total
FROM lnk_mart GROUP BY
CAST(lnk_mart.customer_id AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_ranked AS (
SELECT
CAST(lnk_cust_profit.customer_id AS int) as customer_id,
CAST(lnk_cust_profit.profit_total AS decimal(18,2)) as profit_total
FROM
lnk_cust_profit
ORDER BY profit_total desc



);

CREATE OR REPLACE TEMP VIEW lnk_top100 AS (
  SELECT * FROM lnk_ranked
  ORDER BY profit_total DESC, customer_id ASC
  LIMIT 100
);

CREATE OR REPLACE TEMP VIEW lnk_dim_customer AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_customer
);

CREATE OR REPLACE TEMP VIEW lnk_cur AS (
SELECT
CAST(lnk_dim_customer.customer_sk AS int) as customer_sk,
CAST(lnk_dim_customer.customer_id AS int) as customer_id,
coalesce(lnk_dim_customer.cust_name, '') as cust_name,
coalesce(lnk_dim_customer.gender, '') as gender,
CAST(lnk_dim_customer.age AS int) as age,
CAST(lnk_dim_customer.spending_score AS int) as spending_score,
CAST(lnk_dim_customer.annual_income_k AS int) as annual_income_k,
coalesce(lnk_dim_customer.country, '') as country,
coalesce(lnk_dim_customer.customer_type, '') as customer_type,
coalesce(lnk_dim_customer.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_dim_customer.email, '') as email,
coalesce(lnk_dim_customer.city, '') as city,
CAST(lnk_dim_customer.attr_hash AS bigint) as attr_hash,
CAST(lnk_dim_customer.version_no AS int) as version_no,
CAST(lnk_dim_customer.eff_start_key AS int) as eff_start_key,
CAST(lnk_dim_customer.eff_end_key AS int) as eff_end_key,
CAST(lnk_dim_customer.is_current AS int) as is_current
FROM lnk_dim_customer WHERE is_current = 1

);

CREATE OR REPLACE TEMP VIEW lnk_named AS (
SELECT
CAST(lnk_top100.customer_id AS int) as customer_id,
CAST(lnk_top100.profit_total AS decimal(18,2)) as profit_total,
CAST(lnk_cur.customer_sk AS int) as customer_sk,
coalesce(lnk_cur.cust_name, '') as cust_name,
coalesce(lnk_cur.customer_type, '') as customer_type,
coalesce(lnk_cur.loyalty_tier, '') as loyalty_tier
FROM
lnk_top100
INNER JOIN lnk_cur ON CAST(lnk_top100.customer_id AS int) = CAST(lnk_cur.customer_id AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_bonus AS (
SELECT
CAST(lnk_named.customer_id AS int) as customer_id,
CAST(lnk_named.profit_total AS decimal(18,2)) as profit_total,
CAST(lnk_named.customer_sk AS int) as customer_sk,
coalesce(lnk_named.cust_name, '') as cust_name,
coalesce(lnk_named.customer_type, '') as customer_type,
coalesce(lnk_named.loyalty_tier, '') as loyalty_tier,
IF ( coalesce(lnk_named.loyalty_tier, '') = 'PLATINUM' , 0.05 , IF ( coalesce(lnk_named.loyalty_tier, '') = 'GOLD' , 0.04 , IF ( coalesce(lnk_named.loyalty_tier, '') = 'SILVER' , 0.03 , 0.02 ) ) ) as bonus_rate,
IF ( coalesce(lnk_named.loyalty_tier, '') = 'PLATINUM' , CAST(lnk_named.profit_total AS decimal(18,2)) * 0.05 , IF ( coalesce(lnk_named.loyalty_tier, '') = 'GOLD' , CAST(lnk_named.profit_total AS decimal(18,2)) * 0.04 , IF ( coalesce(lnk_named.loyalty_tier, '') = 'SILVER' , CAST(lnk_named.profit_total AS decimal(18,2)) * 0.03 , CAST(lnk_named.profit_total AS decimal(18,2)) * 0.02 ) ) ) as bonus_amount,
'BONUS_2011' as program
FROM
lnk_named

);

CREATE OR REPLACE TEMP VIEW lnk_out AS (
SELECT
CAST(lnk_bonus.customer_id AS int) as customer_id,
CAST(lnk_bonus.profit_total AS decimal(18,2)) as profit_total,
CAST(lnk_bonus.customer_sk AS int) as customer_sk,
coalesce(lnk_bonus.cust_name, '') as cust_name,
coalesce(lnk_bonus.customer_type, '') as customer_type,
coalesce(lnk_bonus.loyalty_tier, '') as loyalty_tier,
CAST(lnk_bonus.bonus_rate AS decimal(18,6)) as bonus_rate,
CAST(lnk_bonus.bonus_amount AS decimal(18,2)) as bonus_amount,
coalesce(lnk_bonus.program, '') as program
FROM
lnk_bonus

);

CREATE OR REPLACE TEMP VIEW lnk_total_in AS (
SELECT
CAST(lnk_bonus.customer_id AS int) as customer_id,
CAST(lnk_bonus.profit_total AS decimal(18,2)) as profit_total,
CAST(lnk_bonus.customer_sk AS int) as customer_sk,
coalesce(lnk_bonus.cust_name, '') as cust_name,
coalesce(lnk_bonus.customer_type, '') as customer_type,
coalesce(lnk_bonus.loyalty_tier, '') as loyalty_tier,
CAST(lnk_bonus.bonus_rate AS decimal(18,6)) as bonus_rate,
CAST(lnk_bonus.bonus_amount AS decimal(18,2)) as bonus_amount,
coalesce(lnk_bonus.program, '') as program
FROM
lnk_bonus

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.Q3_Bonus_Program AS
SELECT
  *
FROM lnk_out;

CREATE OR REPLACE TEMP VIEW lnk_total AS (
SELECT
coalesce(lnk_total_in.program, '') as program,
SUM(bonus_amount) as bonus_total
FROM
lnk_total_in
GROUP BY
coalesce(lnk_total_in.program, '')

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.Q3_Bonus_Summary AS
SELECT
  *
FROM lnk_total;