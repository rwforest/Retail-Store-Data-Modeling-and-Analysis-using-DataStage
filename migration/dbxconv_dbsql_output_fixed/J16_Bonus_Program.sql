SELECT
.customer_id as customer_id,
MIN(profit_total) as profit_total
FROM

GROUP BY
.customer_id

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

WHERE is_current = 1

SELECT
lnk_cust_profit.customer_id as customer_id,
lnk_cust_profit.profit_total as profit_total
FROM
lnk_cust_profit
ORDER BY profit_total desc

// ******** No handler defined for type HEAD, node lnk_top100 *******


SELECT
lnk_top100.customer_id as customer_id,
lnk_top100.profit_total as profit_total,
lnk_cur.customer_sk as customer_sk,
lnk_cur.cust_name as cust_name,
lnk_cur.customer_type as customer_type,
lnk_cur.loyalty_tier as loyalty_tier
FROM
lnk_top100
INNER JOIN lnk_cur ON lnk_top100.customer_id = lnk_cur.customer_id

SELECT
lnk_named.customer_id as customer_id,
lnk_named.profit_total as profit_total,
lnk_named.customer_sk as customer_sk,
lnk_named.cust_name as cust_name,
lnk_named.customer_type as customer_type,
lnk_named.loyalty_tier as loyalty_tier,
IF ( lnk_named.loyalty_tier = 'PLATINUM' , 0.05 , IF ( lnk_named.loyalty_tier = 'GOLD' , 0.04 , IF ( lnk_named.loyalty_tier = 'SILVER' , 0.03 , 0.02 ) ) ) as bonus_rate,
IF ( lnk_named.loyalty_tier = 'PLATINUM' , lnk_named.profit_total * 0.05 , IF ( lnk_named.loyalty_tier = 'GOLD' , lnk_named.profit_total * 0.04 , IF ( lnk_named.loyalty_tier = 'SILVER' , lnk_named.profit_total * 0.03 , lnk_named.profit_total * 0.02 ) ) ) as bonus_amount,
'BONUS_2011' as program
FROM
lnk_named

SELECT
lnk_bonus.customer_id as customer_id,
lnk_bonus.profit_total as profit_total,
lnk_bonus.customer_sk as customer_sk,
lnk_bonus.cust_name as cust_name,
lnk_bonus.customer_type as customer_type,
lnk_bonus.loyalty_tier as loyalty_tier,
lnk_bonus.bonus_rate as bonus_rate,
lnk_bonus.bonus_amount as bonus_amount,
lnk_bonus.program as program
FROM
lnk_bonus

SELECT
lnk_bonus.customer_id as customer_id,
lnk_bonus.profit_total as profit_total,
lnk_bonus.customer_sk as customer_sk,
lnk_bonus.cust_name as cust_name,
lnk_bonus.customer_type as customer_type,
lnk_bonus.loyalty_tier as loyalty_tier,
lnk_bonus.bonus_rate as bonus_rate,
lnk_bonus.bonus_amount as bonus_amount,
lnk_bonus.program as program
FROM
lnk_bonus

SELECT CONCAT_WS(',',
lnk_out.customer_id,
lnk_out.profit_total,
lnk_out.customer_sk,
lnk_out.cust_name,
lnk_out.customer_type,
lnk_out.loyalty_tier,
lnk_out.bonus_rate,
lnk_out.bonus_amount,
lnk_out.program)
FROM
lnk_out



SELECT
lnk_total_in.program as program,
MIN(bonus_total) as bonus_total
FROM
lnk_total_in
GROUP BY
lnk_total_in.program

SELECT CONCAT_WS(',',
lnk_total.program,
lnk_total.bonus_total)
FROM
lnk_total

