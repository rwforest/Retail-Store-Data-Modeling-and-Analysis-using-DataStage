SELECT
.product_sk as product_sk,
MIN(returned) as returned
FROM

GROUP BY
.product_sk

SELECT
.product_sk as product_sk,
MIN(units_sold) as units_sold,
MIN(revenue) as revenue,
MIN(profit) as profit
FROM

GROUP BY
.product_sk

SELECT
lnk_sales.product_sk as product_sk,
lnk_sales.units_sold as units_sold,
lnk_sales.revenue as revenue,
lnk_sales.profit as profit,
lnk_ret_agg.returned as returned
FROM
lnk_sales
LEFT JOIN lnk_ret_agg ON lnk_sales.product_sk = lnk_ret_agg.product_sk

SELECT
lnk_sr.product_sk as product_sk,
lnk_sr.units_sold as units_sold,
lnk_sr.revenue as revenue,
lnk_sr.profit as profit,
NullToValue ( lnk_sr.returned , 0 ) as returned
FROM
lnk_sr

SELECT
lnk_sr_nn.product_sk as product_sk,
lnk_sr_nn.units_sold as units_sold,
lnk_sr_nn.revenue as revenue,
lnk_sr_nn.profit as profit,
lnk_sr_nn.returned as returned,
IF ( lnk_sr_nn.revenue > 0 , lnk_sr_nn.returned / lnk_sr_nn.revenue , 0 ) as return_rate,
IF ( lnk_sr_nn.revenue > 0 , lnk_sr_nn.profit / lnk_sr_nn.revenue , 0 ) as margin_pct,
IF ( lnk_sr_nn.revenue >= 5000 , 'A' , IF ( lnk_sr_nn.revenue >= 1000 , 'B' , 'C' ) ) as abc_class
FROM
lnk_sr_nn

SELECT
lnk_kpi.product_sk as product_sk,
lnk_kpi.units_sold as units_sold,
lnk_kpi.revenue as revenue,
lnk_kpi.profit as profit,
lnk_kpi.returned as returned,
lnk_kpi.return_rate as return_rate,
lnk_kpi.margin_pct as margin_pct,
lnk_kpi.abc_class as abc_class,
.stock_code as stock_code,
.product_name as product_name,
.category as category,
.supplier_name as supplier_name
FROM
lnk_kpi
INNER JOIN  ON lnk_kpi.product_sk = .product_sk

SELECT
lnk_named.product_sk as product_sk,
lnk_named.units_sold as units_sold,
lnk_named.revenue as revenue,
lnk_named.profit as profit,
lnk_named.returned as returned,
lnk_named.return_rate as return_rate,
lnk_named.margin_pct as margin_pct,
lnk_named.abc_class as abc_class,
lnk_named.stock_code as stock_code,
lnk_named.product_name as product_name,
lnk_named.category as category,
lnk_named.supplier_name as supplier_name
FROM
lnk_named
ORDER BY revenue desc

SELECT
lnk_sorted.product_sk as product_sk,
lnk_sorted.units_sold as units_sold,
lnk_sorted.revenue as revenue,
lnk_sorted.profit as profit,
lnk_sorted.returned as returned,
lnk_sorted.return_rate as return_rate,
lnk_sorted.margin_pct as margin_pct,
lnk_sorted.abc_class as abc_class,
lnk_sorted.stock_code as stock_code,
lnk_sorted.product_name as product_name,
lnk_sorted.category as category,
lnk_sorted.supplier_name as supplier_name
FROM
lnk_sorted

SELECT
lnk_sorted.product_sk as product_sk,
lnk_sorted.units_sold as units_sold,
lnk_sorted.revenue as revenue,
lnk_sorted.profit as profit,
lnk_sorted.returned as returned,
lnk_sorted.return_rate as return_rate,
lnk_sorted.margin_pct as margin_pct,
lnk_sorted.abc_class as abc_class,
lnk_sorted.stock_code as stock_code,
lnk_sorted.product_name as product_name,
lnk_sorted.category as category,
lnk_sorted.supplier_name as supplier_name
FROM
lnk_sorted

// ******** No handler defined for type HEAD, node lnk_top50 *******


SELECT CONCAT_WS(',',
lnk_all.product_sk,
lnk_all.units_sold,
lnk_all.revenue,
lnk_all.profit,
lnk_all.returned,
lnk_all.return_rate,
lnk_all.margin_pct,
lnk_all.abc_class,
lnk_all.stock_code,
lnk_all.product_name,
lnk_all.category,
lnk_all.supplier_name)
FROM
lnk_all



SELECT CONCAT_WS(',',
lnk_top50.product_sk,
lnk_top50.units_sold,
lnk_top50.revenue,
lnk_top50.profit,
lnk_top50.returned,
lnk_top50.return_rate,
lnk_top50.margin_pct,
lnk_top50.abc_class,
lnk_top50.stock_code,
lnk_top50.product_name,
lnk_top50.category,
lnk_top50.supplier_name)
FROM
lnk_top50

