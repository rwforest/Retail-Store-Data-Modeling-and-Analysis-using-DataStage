CREATE OR REPLACE TEMP VIEW lnk_mart AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.RETAIL_DATA_MART
);

CREATE OR REPLACE TEMP VIEW lnk_by_type AS (
SELECT
coalesce(lnk_mart.customer_type, '') as customer_type,
SUM(gross_profit) as profit_total,
SUM(sales_amount) as sales_total,
SUM(line_cnt) as lines
FROM lnk_mart GROUP BY
coalesce(lnk_mart.customer_type, '')

);

CREATE OR REPLACE TEMP VIEW lnk_ranked AS (
SELECT
coalesce(lnk_by_type.customer_type, '') as customer_type,
CAST(lnk_by_type.profit_total AS decimal(18,2)) as profit_total,
CAST(lnk_by_type.sales_total AS decimal(18,2)) as sales_total,
CAST(lnk_by_type.lines AS bigint) as lines
FROM
lnk_by_type
ORDER BY profit_total desc

);

CREATE OR REPLACE TEMP VIEW lnk_all AS (
SELECT
coalesce(lnk_ranked.customer_type, '') as customer_type,
CAST(lnk_ranked.profit_total AS decimal(18,2)) as profit_total,
CAST(lnk_ranked.sales_total AS decimal(18,2)) as sales_total,
CAST(lnk_ranked.lines AS bigint) as lines
FROM
lnk_ranked

);

CREATE OR REPLACE TEMP VIEW lnk_win_in AS (
SELECT
coalesce(lnk_ranked.customer_type, '') as customer_type,
CAST(lnk_ranked.profit_total AS decimal(18,2)) as profit_total,
CAST(lnk_ranked.sales_total AS decimal(18,2)) as sales_total,
CAST(lnk_ranked.lines AS bigint) as lines
FROM
lnk_ranked



);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.Q2_CustomerType_Profit AS
SELECT
  *
FROM lnk_all;

CREATE OR REPLACE TEMP VIEW lnk_winner AS (
  SELECT * FROM lnk_win_in
  ORDER BY profit_total DESC, customer_type ASC, sales_total ASC, lines ASC
  LIMIT 1
);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.Q2_Winning_CustomerType AS
SELECT
  *
FROM lnk_winner;