CREATE OR REPLACE TEMP VIEW lnk_raw AS (
  SELECT _c0 AS InvoiceNo, _c1 AS StockCode, _c2 AS Description, _c3 AS Quantity, _c4 AS InvoiceDate, _c5 AS UnitPrice, _c6 AS CustomerID, _c7 AS Country, _c8 AS StoreID, _c9 AS EmployeeID
  FROM read_files('/Volumes/unitygo/retail_dw/landing/orders_raw.csv', format => 'csv', header => true,
    schema => '_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string', escape => '"')
);

CREATE OR REPLACE TEMP VIEW lnk_typed AS (
SELECT
trim(coalesce(lnk_raw.InvoiceNo, '')) as invoice_no,
UPPER(trim(coalesce(lnk_raw.StockCode, ''))) as stock_code,
trim(coalesce(lnk_raw.Description, '')) as description,
trim(coalesce(lnk_raw.Quantity, '')) as quantity,
to_timestamp(coalesce(lnk_raw.InvoiceDate, '')) as invoice_ts,
CAST(trim(coalesce(lnk_raw.UnitPrice, '')) as decimal(18,2)) as unit_price,
IF ( LENGTH( trim(coalesce(lnk_raw.CustomerID, '')) ) = 0 , '0' , trim(coalesce(lnk_raw.CustomerID, '')) ) as customer_id,
trim(coalesce(lnk_raw.Country, '')) as country,
trim(coalesce(lnk_raw.StoreID, '')) as store_id,
trim(coalesce(lnk_raw.EmployeeID, '')) as employee_id,
date_format(to_timestamp(coalesce(lnk_raw.InvoiceDate, '')), 'yyyyMMdd') as invoice_date_key,
CAST(trim(coalesce(lnk_raw.Quantity, '')) as decimal(18,2)) * CAST(trim(coalesce(lnk_raw.UnitPrice, '')) as decimal(18,2)) as line_amount,
IF ( Left ( trim(coalesce(lnk_raw.InvoiceNo, '')) , 1 ) = 'C' , 1 , 0 ) as is_cancel,
IF ( UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'POST' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'D' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'M' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'DOT' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'CRUK' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'BANK CHARGES' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'AMAZONFEE' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'S' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'B' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'C2' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'PADS' Or UPPER(trim(coalesce(lnk_raw.StockCode, ''))) = 'M' , 1 , 0 ) as is_nonproduct,
IF ( LENGTH( trim(coalesce(lnk_raw.CustomerID, '')) ) = 0 , 1 , 0 ) as is_guest
FROM lnk_raw
);

CREATE OR REPLACE TEMP VIEW lnk_typed_ok AS (
SELECT
coalesce(lnk_typed.invoice_no, '') as invoice_no,
coalesce(lnk_typed.stock_code, '') as stock_code,
coalesce(lnk_typed.description, '') as description,
CAST(lnk_typed.quantity AS int) as quantity,
lnk_typed.invoice_ts as invoice_ts,
CAST(lnk_typed.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_typed.customer_id AS int) as customer_id,
coalesce(lnk_typed.country, '') as country,
coalesce(lnk_typed.store_id, '') as store_id,
coalesce(lnk_typed.employee_id, '') as employee_id,
CAST(lnk_typed.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_typed.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_typed.is_cancel AS int) as is_cancel,
CAST(lnk_typed.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_typed.is_guest AS int) as is_guest
FROM
lnk_typed

);

CREATE OR REPLACE TEMP VIEW lnk_typed_bad AS (
SELECT
coalesce(lnk_typed.invoice_no, '') as invoice_no,
coalesce(lnk_typed.stock_code, '') as stock_code,
coalesce(lnk_typed.description, '') as description,
CAST(lnk_typed.quantity AS int) as quantity,
lnk_typed.invoice_ts as invoice_ts,
CAST(lnk_typed.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_typed.customer_id AS int) as customer_id,
coalesce(lnk_typed.country, '') as country,
coalesce(lnk_typed.store_id, '') as store_id,
coalesce(lnk_typed.employee_id, '') as employee_id,
CAST(lnk_typed.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_typed.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_typed.is_cancel AS int) as is_cancel,
CAST(lnk_typed.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_typed.is_guest AS int) as is_guest
FROM
lnk_typed

);

CREATE OR REPLACE TEMP VIEW lnk_valid AS (
SELECT
coalesce(lnk_typed_ok.invoice_no, '') as invoice_no,
coalesce(lnk_typed_ok.stock_code, '') as stock_code,
coalesce(lnk_typed_ok.description, '') as description,
CAST(lnk_typed_ok.quantity AS int) as quantity,
lnk_typed_ok.invoice_ts as invoice_ts,
CAST(lnk_typed_ok.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_typed_ok.customer_id AS int) as customer_id,
coalesce(lnk_typed_ok.country, '') as country,
coalesce(lnk_typed_ok.store_id, '') as store_id,
coalesce(lnk_typed_ok.employee_id, '') as employee_id,
CAST(lnk_typed_ok.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_typed_ok.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_typed_ok.is_cancel AS int) as is_cancel,
CAST(lnk_typed_ok.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_typed_ok.is_guest AS int) as is_guest
FROM
lnk_typed_ok
WHERE quantity <> 0 and unit_price >= 0

);

CREATE OR REPLACE TEMP VIEW lnk_dedup AS (
SELECT
invoice_no,
stock_code,
description,
quantity,
invoice_ts,
unit_price,
customer_id,
country,
store_id,
employee_id,
invoice_date_key,
line_amount,
is_cancel,
is_nonproduct,
is_guest
FROM (
SELECT
coalesce(lnk_valid.invoice_no, '') as invoice_no,
coalesce(lnk_valid.stock_code, '') as stock_code,
coalesce(lnk_valid.description, '') as description,
CAST(lnk_valid.quantity AS int) as quantity,
lnk_valid.invoice_ts as invoice_ts,
CAST(lnk_valid.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_valid.customer_id AS int) as customer_id,
coalesce(lnk_valid.country, '') as country,
coalesce(lnk_valid.store_id, '') as store_id,
coalesce(lnk_valid.employee_id, '') as employee_id,
CAST(lnk_valid.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_valid.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_valid.is_cancel AS int) as is_cancel,
CAST(lnk_valid.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_valid.is_guest AS int) as is_guest,
ROW_NUMBER() OVER(PARTITION BY coalesce(lnk_valid.invoice_no, ''), coalesce(lnk_valid.stock_code, ''), CAST(lnk_valid.quantity AS int), lnk_valid.invoice_ts, CAST(lnk_valid.unit_price AS decimal(18,2)), CAST(lnk_valid.customer_id AS int) ORDER BY coalesce(lnk_valid.description, ''), coalesce(lnk_valid.country, ''), coalesce(lnk_valid.store_id, ''), coalesce(lnk_valid.employee_id, ''), CAST(lnk_valid.invoice_date_key AS int), CAST(lnk_valid.line_amount AS decimal(18,2)), CAST(lnk_valid.is_cancel AS int), CAST(lnk_valid.is_nonproduct AS int), CAST(lnk_valid.is_guest AS int)) dedup_rnk
FROM lnk_valid) dedup_lnk_dedup WHERE dedup_rnk = 1

);

CREATE OR REPLACE TEMP VIEW lnk_stg AS (
SELECT
coalesce(lnk_dedup.invoice_no, '') as invoice_no,
coalesce(lnk_dedup.stock_code, '') as stock_code,
coalesce(lnk_dedup.description, '') as description,
CAST(lnk_dedup.quantity AS int) as quantity,
lnk_dedup.invoice_ts as invoice_ts,
CAST(lnk_dedup.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_dedup.customer_id AS int) as customer_id,
coalesce(lnk_dedup.country, '') as country,
coalesce(lnk_dedup.store_id, '') as store_id,
coalesce(lnk_dedup.employee_id, '') as employee_id,
CAST(lnk_dedup.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_dedup.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_dedup.is_cancel AS int) as is_cancel,
CAST(lnk_dedup.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_dedup.is_guest AS int) as is_guest
FROM
lnk_dedup

);

CREATE OR REPLACE TEMP VIEW lnk_anom_in AS (
SELECT
coalesce(lnk_dedup.invoice_no, '') as invoice_no,
coalesce(lnk_dedup.stock_code, '') as stock_code,
coalesce(lnk_dedup.description, '') as description,
CAST(lnk_dedup.quantity AS int) as quantity,
lnk_dedup.invoice_ts as invoice_ts,
CAST(lnk_dedup.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_dedup.customer_id AS int) as customer_id,
coalesce(lnk_dedup.country, '') as country,
coalesce(lnk_dedup.store_id, '') as store_id,
coalesce(lnk_dedup.employee_id, '') as employee_id,
CAST(lnk_dedup.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_dedup.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_dedup.is_cancel AS int) as is_cancel,
CAST(lnk_dedup.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_dedup.is_guest AS int) as is_guest
FROM
lnk_dedup

);

CREATE OR REPLACE TEMP VIEW lnk_cnt_in AS (
SELECT
coalesce(lnk_dedup.invoice_no, '') as invoice_no,
coalesce(lnk_dedup.stock_code, '') as stock_code,
coalesce(lnk_dedup.description, '') as description,
CAST(lnk_dedup.quantity AS int) as quantity,
lnk_dedup.invoice_ts as invoice_ts,
CAST(lnk_dedup.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_dedup.customer_id AS int) as customer_id,
coalesce(lnk_dedup.country, '') as country,
coalesce(lnk_dedup.store_id, '') as store_id,
coalesce(lnk_dedup.employee_id, '') as employee_id,
CAST(lnk_dedup.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_dedup.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_dedup.is_cancel AS int) as is_cancel,
CAST(lnk_dedup.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_dedup.is_guest AS int) as is_guest
FROM
lnk_dedup

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.stg_orders AS
SELECT
  *
FROM lnk_stg;

CREATE OR REPLACE TEMP VIEW lnk_invalid AS (
SELECT
coalesce(lnk_typed_bad.invoice_no, '') as invoice_no,
coalesce(lnk_typed_bad.stock_code, '') as stock_code,
coalesce(lnk_typed_bad.description, '') as description,
CAST(lnk_typed_bad.quantity AS int) as quantity,
lnk_typed_bad.invoice_ts as invoice_ts,
CAST(lnk_typed_bad.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_typed_bad.customer_id AS int) as customer_id,
coalesce(lnk_typed_bad.country, '') as country,
coalesce(lnk_typed_bad.store_id, '') as store_id,
coalesce(lnk_typed_bad.employee_id, '') as employee_id,
CAST(lnk_typed_bad.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_typed_bad.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_typed_bad.is_cancel AS int) as is_cancel,
CAST(lnk_typed_bad.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_typed_bad.is_guest AS int) as is_guest
FROM
lnk_typed_bad
WHERE quantity = 0 or unit_price < 0

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.rej_orders AS
SELECT
  *
FROM lnk_invalid;

CREATE OR REPLACE TEMP VIEW lnk_anom AS (
SELECT
coalesce(lnk_anom_in.invoice_no, '') as invoice_no,
coalesce(lnk_anom_in.stock_code, '') as stock_code,
coalesce(lnk_anom_in.description, '') as description,
CAST(lnk_anom_in.quantity AS int) as quantity,
lnk_anom_in.invoice_ts as invoice_ts,
CAST(lnk_anom_in.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_anom_in.customer_id AS int) as customer_id,
coalesce(lnk_anom_in.country, '') as country,
coalesce(lnk_anom_in.store_id, '') as store_id,
coalesce(lnk_anom_in.employee_id, '') as employee_id,
CAST(lnk_anom_in.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_anom_in.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_anom_in.is_cancel AS int) as is_cancel,
CAST(lnk_anom_in.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_anom_in.is_guest AS int) as is_guest
FROM
lnk_anom_in
WHERE unit_price = 0 or quantity > 5000 or quantity < -5000

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.dq_order_anomalies AS
SELECT
  *
FROM lnk_anom;

CREATE OR REPLACE TEMP VIEW lnk_rows_by_country AS (
SELECT
coalesce(lnk_cnt_in.country, '') as country,
COUNT(*) as rows_loaded
FROM
lnk_cnt_in
GROUP BY
coalesce(lnk_cnt_in.country, '')

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.audit_land_orders AS
SELECT
  *
FROM lnk_rows_by_country;