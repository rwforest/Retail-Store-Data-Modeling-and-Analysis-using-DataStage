SELECT
.store_sk as store_sk,
.year_month as year_month,
MIN(sales) as sales,
MIN(profit) as profit,
MIN(lines) as lines
FROM

GROUP BY
.store_sk,
.year_month

SELECT
lnk_sm.store_sk as store_sk,
lnk_sm.year_month as year_month,
lnk_sm.sales as sales,
lnk_sm.profit as profit,
lnk_sm.lines as lines,
.store_id as store_id,
.store_name as store_name,
.no_of_employees as no_of_employees,
.region as region
FROM
lnk_sm
INNER JOIN  ON lnk_sm.store_sk = .store_sk

SELECT
lnk_named.store_sk as store_sk,
lnk_named.year_month as year_month,
lnk_named.sales as sales,
lnk_named.profit as profit,
lnk_named.lines as lines,
lnk_named.store_id as store_id,
lnk_named.store_name as store_name,
lnk_named.no_of_employees as no_of_employees,
lnk_named.region as region,
IF ( lnk_named.no_of_employees > 0 , lnk_named.sales / lnk_named.no_of_employees , 0 ) as sales_per_employee,
IF ( lnk_named.sales > 0 , lnk_named.profit / lnk_named.sales , 0 ) as margin_pct
FROM
lnk_named

SELECT
lnk_kpi.store_sk as store_sk,
lnk_kpi.year_month as year_month,
lnk_kpi.sales as sales,
lnk_kpi.profit as profit,
lnk_kpi.lines as lines,
lnk_kpi.store_id as store_id,
lnk_kpi.store_name as store_name,
lnk_kpi.no_of_employees as no_of_employees,
lnk_kpi.region as region,
lnk_kpi.sales_per_employee as sales_per_employee,
lnk_kpi.margin_pct as margin_pct
FROM
lnk_kpi
ORDER BY store_sk asc, year_month asc

SELECT CONCAT_WS(',',
lnk_sorted.store_sk,
lnk_sorted.year_month,
lnk_sorted.sales,
lnk_sorted.profit,
lnk_sorted.lines,
lnk_sorted.store_id,
lnk_sorted.store_name,
lnk_sorted.no_of_employees,
lnk_sorted.region,
lnk_sorted.sales_per_employee,
lnk_sorted.margin_pct)
FROM
lnk_sorted

