SELECT
invoice_no,
stock_code,
quantity,
invoice_ts,
unit_price,
customer_id,
country,
store_id,
invoice_date_key,
year_month,
customer_sk,
customer_type,
loyalty_tier,
category,
product_sk,
region,
store_sk,
employee_sk,
currency,
sales_amount,
cost_amount,
gross_profit,
local_amount,
line_cnt,
cust_name,
gender,
age,
spending_score,
annual_income_k,
product_name,
list_price,
store_name,
rating,
no_of_employees
FROM (
SELECT
.invoice_no as invoice_no,
.stock_code as stock_code,
.quantity as quantity,
.invoice_ts as invoice_ts,
.unit_price as unit_price,
.customer_id as customer_id,
.country as country,
.store_id as store_id,
.invoice_date_key as invoice_date_key,
.year_month as year_month,
.customer_sk as customer_sk,
.customer_type as customer_type,
.loyalty_tier as loyalty_tier,
.category as category,
.product_sk as product_sk,
.region as region,
.store_sk as store_sk,
.employee_sk as employee_sk,
.currency as currency,
.sales_amount as sales_amount,
.cost_amount as cost_amount,
.gross_profit as gross_profit,
.local_amount as local_amount,
.line_cnt as line_cnt,
.cust_name as cust_name,
.gender as gender,
.age as age,
.spending_score as spending_score,
.annual_income_k as annual_income_k,
.product_name as product_name,
.list_price as list_price,
.store_name as store_name,
.rating as rating,
.no_of_employees as no_of_employees,
ROW_NUMBER() OVER(PARTITION BY .invoice_no, .store_sk, .employee_sk ORDER BY .stock_code, .quantity, .invoice_ts, .unit_price, .customer_id, .country, .store_id, .invoice_date_key, .year_month, .customer_sk, .customer_type, .loyalty_tier, .category, .product_sk, .region, .currency, .sales_amount, .cost_amount, .gross_profit, .local_amount, .line_cnt, .cust_name, .gender, .age, .spending_score, .annual_income_k, .product_name, .list_price, .store_name, .rating, .no_of_employees) dedup_rnk
FROM ) dedup_lnk_invoices WHERE dedup_rnk = 1

SELECT
lnk_invoices.store_sk as store_sk,
lnk_invoices.employee_sk as employee_sk,
MIN(total_transactions) as total_transactions
FROM
lnk_invoices
GROUP BY
lnk_invoices.store_sk,
lnk_invoices.employee_sk

SELECT
lnk_counts.store_sk as store_sk,
lnk_counts.employee_sk as employee_sk,
lnk_counts.total_transactions as total_transactions,
.employee_id as employee_id,
.employee_name as employee_name,
.role as role
FROM
lnk_counts
INNER JOIN  ON lnk_counts.employee_sk = .employee_sk

SELECT
lnk_named.store_sk as store_sk,
lnk_named.employee_sk as employee_sk,
lnk_named.total_transactions as total_transactions,
lnk_named.employee_id as employee_id,
lnk_named.employee_name as employee_name,
lnk_named.role as role
FROM
lnk_named
ORDER BY store_sk asc, total_transactions desc

SELECT
lnk_ranked.store_sk as store_sk,
lnk_ranked.employee_sk as employee_sk,
lnk_ranked.total_transactions as total_transactions,
lnk_ranked.employee_id as employee_id,
lnk_ranked.employee_name as employee_name,
lnk_ranked.role as role
FROM
lnk_ranked

SELECT
lnk_ranked.store_sk as store_sk,
lnk_ranked.employee_sk as employee_sk,
lnk_ranked.total_transactions as total_transactions,
lnk_ranked.employee_id as employee_id,
lnk_ranked.employee_name as employee_name,
lnk_ranked.role as role
FROM
lnk_ranked

// ******** No handler defined for type HEAD, node lnk_top10 *******


SELECT CONCAT_WS(',',
lnk_all.store_sk,
lnk_all.employee_sk,
lnk_all.total_transactions,
lnk_all.employee_id,
lnk_all.employee_name,
lnk_all.role)
FROM
lnk_all



SELECT CONCAT_WS(',',
lnk_top10.store_sk,
lnk_top10.employee_sk,
lnk_top10.total_transactions,
lnk_top10.employee_id,
lnk_top10.employee_name,
lnk_top10.role)
FROM
lnk_top10

