SELECT
.customer_type as customer_type,
MIN(profit_total) as profit_total,
MIN(sales_total) as sales_total,
MIN(lines) as lines
FROM

GROUP BY
.customer_type

SELECT
lnk_by_type.customer_type as customer_type,
lnk_by_type.profit_total as profit_total,
lnk_by_type.sales_total as sales_total,
lnk_by_type.lines as lines
FROM
lnk_by_type
ORDER BY profit_total desc

SELECT
lnk_ranked.customer_type as customer_type,
lnk_ranked.profit_total as profit_total,
lnk_ranked.sales_total as sales_total,
lnk_ranked.lines as lines
FROM
lnk_ranked

SELECT
lnk_ranked.customer_type as customer_type,
lnk_ranked.profit_total as profit_total,
lnk_ranked.sales_total as sales_total,
lnk_ranked.lines as lines
FROM
lnk_ranked

// ******** No handler defined for type HEAD, node lnk_winner *******


SELECT CONCAT_WS(',',
lnk_all.customer_type,
lnk_all.profit_total,
lnk_all.sales_total,
lnk_all.lines)
FROM
lnk_all



SELECT CONCAT_WS(',',
lnk_winner.customer_type,
lnk_winner.profit_total,
lnk_winner.sales_total,
lnk_winner.lines)
FROM
lnk_winner

