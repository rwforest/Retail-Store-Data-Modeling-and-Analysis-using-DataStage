CREATE OR REPLACE TEMP VIEW lnk_fact AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.fact_sales
);

CREATE OR REPLACE TEMP VIEW lnk_sales AS (
SELECT
CAST(lnk_fact.product_sk AS int) as product_sk,
SUM(quantity) as units_sold,
SUM(sales_amount) as revenue,
SUM(gross_profit) as profit
FROM lnk_fact GROUP BY
CAST(lnk_fact.product_sk AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_ret AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.fact_returns
);

CREATE OR REPLACE TEMP VIEW lnk_ret_agg AS (
SELECT
CAST(lnk_ret.product_sk AS int) as product_sk,
SUM(returned_amount) as returned
FROM lnk_ret GROUP BY
CAST(lnk_ret.product_sk AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_sr AS (
SELECT
CAST(lnk_sales.product_sk AS int) as product_sk,
CAST(lnk_sales.units_sold AS bigint) as units_sold,
CAST(lnk_sales.revenue AS decimal(18,2)) as revenue,
CAST(lnk_sales.profit AS decimal(18,2)) as profit,
CAST(lnk_ret_agg.returned AS decimal(18,2)) as returned
FROM
lnk_sales
LEFT JOIN lnk_ret_agg ON CAST(lnk_sales.product_sk AS int) = CAST(lnk_ret_agg.product_sk AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_sr_nn AS (
SELECT
CAST(lnk_sr.product_sk AS int) as product_sk,
CAST(lnk_sr.units_sold AS bigint) as units_sold,
CAST(lnk_sr.revenue AS decimal(18,2)) as revenue,
CAST(lnk_sr.profit AS decimal(18,2)) as profit,
COALESCE( CAST(lnk_sr.returned AS decimal(18,2)) , 0 ) as returned
FROM
lnk_sr

);

CREATE OR REPLACE TEMP VIEW lnk_kpi AS (
SELECT
CAST(lnk_sr_nn.product_sk AS int) as product_sk,
CAST(lnk_sr_nn.units_sold AS bigint) as units_sold,
CAST(lnk_sr_nn.revenue AS decimal(18,2)) as revenue,
CAST(lnk_sr_nn.profit AS decimal(18,2)) as profit,
CAST(lnk_sr_nn.returned AS decimal(18,2)) as returned,
IF ( CAST(lnk_sr_nn.revenue AS decimal(18,2)) > 0 , CAST(lnk_sr_nn.returned AS decimal(18,2)) / CAST(lnk_sr_nn.revenue AS decimal(18,2)) , 0 ) as return_rate,
IF ( CAST(lnk_sr_nn.revenue AS decimal(18,2)) > 0 , CAST(lnk_sr_nn.profit AS decimal(18,2)) / CAST(lnk_sr_nn.revenue AS decimal(18,2)) , 0 ) as margin_pct,
IF ( CAST(lnk_sr_nn.revenue AS decimal(18,2)) >= 5000 , 'A' , IF ( CAST(lnk_sr_nn.revenue AS decimal(18,2)) >= 1000 , 'B' , 'C' ) ) as abc_class
FROM
lnk_sr_nn

);

CREATE OR REPLACE TEMP VIEW lnk_dim_product AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_product
);

CREATE OR REPLACE TEMP VIEW lnk_named AS (
SELECT
CAST(lnk_kpi.product_sk AS int) as product_sk,
CAST(lnk_kpi.units_sold AS bigint) as units_sold,
CAST(lnk_kpi.revenue AS decimal(18,2)) as revenue,
CAST(lnk_kpi.profit AS decimal(18,2)) as profit,
CAST(lnk_kpi.returned AS decimal(18,2)) as returned,
CAST(lnk_kpi.return_rate AS decimal(18,6)) as return_rate,
CAST(lnk_kpi.margin_pct AS decimal(18,6)) as margin_pct,
coalesce(lnk_kpi.abc_class, '') as abc_class,
coalesce(lnk_dim_product.stock_code, '') as stock_code,
coalesce(lnk_dim_product.product_name, '') as product_name,
coalesce(lnk_dim_product.category, '') as category,
coalesce(lnk_dim_product.supplier_name, '') as supplier_name
FROM
lnk_kpi
INNER JOIN lnk_dim_product ON CAST(lnk_kpi.product_sk AS int) = CAST(lnk_dim_product.product_sk AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_sorted AS (
SELECT
CAST(lnk_named.product_sk AS int) as product_sk,
CAST(lnk_named.units_sold AS bigint) as units_sold,
CAST(lnk_named.revenue AS decimal(18,2)) as revenue,
CAST(lnk_named.profit AS decimal(18,2)) as profit,
CAST(lnk_named.returned AS decimal(18,2)) as returned,
CAST(lnk_named.return_rate AS decimal(18,6)) as return_rate,
CAST(lnk_named.margin_pct AS decimal(18,6)) as margin_pct,
coalesce(lnk_named.abc_class, '') as abc_class,
coalesce(lnk_named.stock_code, '') as stock_code,
coalesce(lnk_named.product_name, '') as product_name,
coalesce(lnk_named.category, '') as category,
coalesce(lnk_named.supplier_name, '') as supplier_name
FROM
lnk_named
ORDER BY revenue desc

);

CREATE OR REPLACE TEMP VIEW lnk_all AS (
SELECT
CAST(lnk_sorted.product_sk AS int) as product_sk,
CAST(lnk_sorted.units_sold AS bigint) as units_sold,
CAST(lnk_sorted.revenue AS decimal(18,2)) as revenue,
CAST(lnk_sorted.profit AS decimal(18,2)) as profit,
CAST(lnk_sorted.returned AS decimal(18,2)) as returned,
CAST(lnk_sorted.return_rate AS decimal(18,6)) as return_rate,
CAST(lnk_sorted.margin_pct AS decimal(18,6)) as margin_pct,
coalesce(lnk_sorted.abc_class, '') as abc_class,
coalesce(lnk_sorted.stock_code, '') as stock_code,
coalesce(lnk_sorted.product_name, '') as product_name,
coalesce(lnk_sorted.category, '') as category,
coalesce(lnk_sorted.supplier_name, '') as supplier_name
FROM
lnk_sorted

);

CREATE OR REPLACE TEMP VIEW lnk_top_in AS (
SELECT
CAST(lnk_sorted.product_sk AS int) as product_sk,
CAST(lnk_sorted.units_sold AS bigint) as units_sold,
CAST(lnk_sorted.revenue AS decimal(18,2)) as revenue,
CAST(lnk_sorted.profit AS decimal(18,2)) as profit,
CAST(lnk_sorted.returned AS decimal(18,2)) as returned,
CAST(lnk_sorted.return_rate AS decimal(18,6)) as return_rate,
CAST(lnk_sorted.margin_pct AS decimal(18,6)) as margin_pct,
coalesce(lnk_sorted.abc_class, '') as abc_class,
coalesce(lnk_sorted.stock_code, '') as stock_code,
coalesce(lnk_sorted.product_name, '') as product_name,
coalesce(lnk_sorted.category, '') as category,
coalesce(lnk_sorted.supplier_name, '') as supplier_name
FROM
lnk_sorted



);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.Product_Performance AS
SELECT
  *
FROM lnk_all;

CREATE OR REPLACE TEMP VIEW lnk_top50 AS (
  SELECT * FROM lnk_top_in
  ORDER BY revenue DESC, product_sk ASC, units_sold ASC, profit ASC, returned ASC, return_rate ASC, margin_pct ASC, abc_class ASC, stock_code ASC, product_name ASC, category ASC, supplier_name ASC
  LIMIT 50
);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.Product_Top50 AS
SELECT
  *
FROM lnk_top50;