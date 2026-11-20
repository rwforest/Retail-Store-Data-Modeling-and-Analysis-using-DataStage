CREATE OR REPLACE TEMP VIEW lnk_fact AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.fact_sales
);

CREATE OR REPLACE TEMP VIEW lnk_sm AS (
SELECT
CAST(lnk_fact.store_sk AS int) as store_sk,
coalesce(lnk_fact.year_month, '') as year_month,
SUM(sales_amount) as sales,
SUM(gross_profit) as profit,
SUM(line_cnt) as lines
FROM lnk_fact GROUP BY
CAST(lnk_fact.store_sk AS int),
coalesce(lnk_fact.year_month, '')

);

CREATE OR REPLACE TEMP VIEW lnk_dim_store AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_store
);

CREATE OR REPLACE TEMP VIEW lnk_named AS (
SELECT
CAST(lnk_sm.store_sk AS int) as store_sk,
coalesce(lnk_sm.year_month, '') as year_month,
CAST(lnk_sm.sales AS decimal(18,2)) as sales,
CAST(lnk_sm.profit AS decimal(18,2)) as profit,
CAST(lnk_sm.lines AS bigint) as lines,
coalesce(lnk_dim_store.store_id, '') as store_id,
coalesce(lnk_dim_store.store_name, '') as store_name,
CAST(lnk_dim_store.no_of_employees AS int) as no_of_employees,
coalesce(lnk_dim_store.region, '') as region
FROM
lnk_sm
INNER JOIN lnk_dim_store ON CAST(lnk_sm.store_sk AS int) = CAST(lnk_dim_store.store_sk AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_kpi AS (
SELECT
CAST(lnk_named.store_sk AS int) as store_sk,
coalesce(lnk_named.year_month, '') as year_month,
CAST(lnk_named.sales AS decimal(18,2)) as sales,
CAST(lnk_named.profit AS decimal(18,2)) as profit,
CAST(lnk_named.lines AS bigint) as lines,
coalesce(lnk_named.store_id, '') as store_id,
coalesce(lnk_named.store_name, '') as store_name,
CAST(lnk_named.no_of_employees AS int) as no_of_employees,
coalesce(lnk_named.region, '') as region,
IF ( CAST(lnk_named.no_of_employees AS int) > 0 , CAST(lnk_named.sales AS decimal(18,2)) / CAST(lnk_named.no_of_employees AS int) , 0 ) as sales_per_employee,
IF ( CAST(lnk_named.sales AS decimal(18,2)) > 0 , CAST(lnk_named.profit AS decimal(18,2)) / CAST(lnk_named.sales AS decimal(18,2)) , 0 ) as margin_pct
FROM
lnk_named

);

CREATE OR REPLACE TEMP VIEW lnk_sorted AS (
SELECT
CAST(lnk_kpi.store_sk AS int) as store_sk,
coalesce(lnk_kpi.year_month, '') as year_month,
CAST(lnk_kpi.sales AS decimal(18,2)) as sales,
CAST(lnk_kpi.profit AS decimal(18,2)) as profit,
CAST(lnk_kpi.lines AS bigint) as lines,
coalesce(lnk_kpi.store_id, '') as store_id,
coalesce(lnk_kpi.store_name, '') as store_name,
CAST(lnk_kpi.no_of_employees AS int) as no_of_employees,
coalesce(lnk_kpi.region, '') as region,
CAST(lnk_kpi.sales_per_employee AS decimal(18,2)) as sales_per_employee,
CAST(lnk_kpi.margin_pct AS decimal(18,6)) as margin_pct
FROM
lnk_kpi
ORDER BY store_sk asc, year_month asc

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.Store_Monthly AS
SELECT
  *
FROM lnk_sorted;