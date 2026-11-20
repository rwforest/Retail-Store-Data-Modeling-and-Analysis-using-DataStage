SELECT
trim(.InvoiceNo) as invoice_no,
UPPER(trim(.StockCode)) as stock_code,
trim(.Description) as description,
trim(.Quantity) as quantity,
to_timestamp(.InvoiceDate) as invoice_ts,
CAST(trim(.UnitPrice) as decimal(18,2)) as unit_price,
IF ( LENGTH( trim(.CustomerID) ) = 0 , '0' , trim(.CustomerID) ) as customer_id,
trim(.Country) as country,
trim(.StoreID) as store_id,
trim(.EmployeeID) as employee_id,
CAST(to_timestamp(.InvoiceDate) AS STRING) as invoice_date_key,
CAST(trim(.Quantity) as decimal(18,2)) * CAST(trim(.UnitPrice) as decimal(18,2)) as line_amount,
IF ( Left ( trim(.InvoiceNo) , 1 ) = 'C' , 1 , 0 ) as is_cancel,
IF ( UPPER(trim(.StockCode)) = 'POST' Or UPPER(trim(.StockCode)) = 'D' Or UPPER(trim(.StockCode)) = 'M' Or UPPER(trim(.StockCode)) = 'DOT' Or UPPER(trim(.StockCode)) = 'CRUK' Or UPPER(trim(.StockCode)) = 'BANK CHARGES' Or UPPER(trim(.StockCode)) = 'AMAZONFEE' Or UPPER(trim(.StockCode)) = 'S' Or UPPER(trim(.StockCode)) = 'B' Or UPPER(trim(.StockCode)) = 'C2' Or UPPER(trim(.StockCode)) = 'PADS' Or UPPER(trim(.StockCode)) = 'M' , 1 , 0 ) as is_nonproduct,
IF ( LENGTH( trim(.CustomerID) ) = 0 , 1 , 0 ) as is_guest
FROM


SELECT
lnk_typed.invoice_no as invoice_no,
lnk_typed.stock_code as stock_code,
lnk_typed.description as description,
lnk_typed.quantity as quantity,
lnk_typed.invoice_ts as invoice_ts,
lnk_typed.unit_price as unit_price,
lnk_typed.customer_id as customer_id,
lnk_typed.country as country,
lnk_typed.store_id as store_id,
lnk_typed.employee_id as employee_id,
lnk_typed.invoice_date_key as invoice_date_key,
lnk_typed.line_amount as line_amount,
lnk_typed.is_cancel as is_cancel,
lnk_typed.is_nonproduct as is_nonproduct,
lnk_typed.is_guest as is_guest
FROM
lnk_typed

SELECT
lnk_typed.invoice_no as invoice_no,
lnk_typed.stock_code as stock_code,
lnk_typed.description as description,
lnk_typed.quantity as quantity,
lnk_typed.invoice_ts as invoice_ts,
lnk_typed.unit_price as unit_price,
lnk_typed.customer_id as customer_id,
lnk_typed.country as country,
lnk_typed.store_id as store_id,
lnk_typed.employee_id as employee_id,
lnk_typed.invoice_date_key as invoice_date_key,
lnk_typed.line_amount as line_amount,
lnk_typed.is_cancel as is_cancel,
lnk_typed.is_nonproduct as is_nonproduct,
lnk_typed.is_guest as is_guest
FROM
lnk_typed

SELECT
lnk_typed_bad.invoice_no as invoice_no,
lnk_typed_bad.stock_code as stock_code,
lnk_typed_bad.description as description,
lnk_typed_bad.quantity as quantity,
lnk_typed_bad.invoice_ts as invoice_ts,
lnk_typed_bad.unit_price as unit_price,
lnk_typed_bad.customer_id as customer_id,
lnk_typed_bad.country as country,
lnk_typed_bad.store_id as store_id,
lnk_typed_bad.employee_id as employee_id,
lnk_typed_bad.invoice_date_key as invoice_date_key,
lnk_typed_bad.line_amount as line_amount,
lnk_typed_bad.is_cancel as is_cancel,
lnk_typed_bad.is_nonproduct as is_nonproduct,
lnk_typed_bad.is_guest as is_guest
FROM
lnk_typed_bad
WHERE quantity = 0 or unit_price < 0

SELECT
lnk_typed_ok.invoice_no as invoice_no,
lnk_typed_ok.stock_code as stock_code,
lnk_typed_ok.description as description,
lnk_typed_ok.quantity as quantity,
lnk_typed_ok.invoice_ts as invoice_ts,
lnk_typed_ok.unit_price as unit_price,
lnk_typed_ok.customer_id as customer_id,
lnk_typed_ok.country as country,
lnk_typed_ok.store_id as store_id,
lnk_typed_ok.employee_id as employee_id,
lnk_typed_ok.invoice_date_key as invoice_date_key,
lnk_typed_ok.line_amount as line_amount,
lnk_typed_ok.is_cancel as is_cancel,
lnk_typed_ok.is_nonproduct as is_nonproduct,
lnk_typed_ok.is_guest as is_guest
FROM
lnk_typed_ok
WHERE quantity <> 0 and unit_price >= 0

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
lnk_valid.invoice_no as invoice_no,
lnk_valid.stock_code as stock_code,
lnk_valid.description as description,
lnk_valid.quantity as quantity,
lnk_valid.invoice_ts as invoice_ts,
lnk_valid.unit_price as unit_price,
lnk_valid.customer_id as customer_id,
lnk_valid.country as country,
lnk_valid.store_id as store_id,
lnk_valid.employee_id as employee_id,
lnk_valid.invoice_date_key as invoice_date_key,
lnk_valid.line_amount as line_amount,
lnk_valid.is_cancel as is_cancel,
lnk_valid.is_nonproduct as is_nonproduct,
lnk_valid.is_guest as is_guest,
ROW_NUMBER() OVER(PARTITION BY lnk_valid.invoice_no, lnk_valid.stock_code, lnk_valid.quantity, lnk_valid.invoice_ts, lnk_valid.unit_price, lnk_valid.customer_id ORDER BY lnk_valid.description, lnk_valid.country, lnk_valid.store_id, lnk_valid.employee_id, lnk_valid.invoice_date_key, lnk_valid.line_amount, lnk_valid.is_cancel, lnk_valid.is_nonproduct, lnk_valid.is_guest) dedup_rnk
FROM lnk_valid) dedup_lnk_dedup WHERE dedup_rnk = 1

SELECT CONCAT_WS(',',
lnk_invalid.invoice_no,
lnk_invalid.stock_code,
lnk_invalid.description,
lnk_invalid.quantity,
lnk_invalid.invoice_ts,
lnk_invalid.unit_price,
lnk_invalid.customer_id,
lnk_invalid.country,
lnk_invalid.store_id,
lnk_invalid.employee_id,
lnk_invalid.invoice_date_key,
lnk_invalid.line_amount,
lnk_invalid.is_cancel,
lnk_invalid.is_nonproduct,
lnk_invalid.is_guest)
FROM
lnk_invalid



SELECT
lnk_dedup.invoice_no as invoice_no,
lnk_dedup.stock_code as stock_code,
lnk_dedup.description as description,
lnk_dedup.quantity as quantity,
lnk_dedup.invoice_ts as invoice_ts,
lnk_dedup.unit_price as unit_price,
lnk_dedup.customer_id as customer_id,
lnk_dedup.country as country,
lnk_dedup.store_id as store_id,
lnk_dedup.employee_id as employee_id,
lnk_dedup.invoice_date_key as invoice_date_key,
lnk_dedup.line_amount as line_amount,
lnk_dedup.is_cancel as is_cancel,
lnk_dedup.is_nonproduct as is_nonproduct,
lnk_dedup.is_guest as is_guest
FROM
lnk_dedup

SELECT
lnk_dedup.invoice_no as invoice_no,
lnk_dedup.stock_code as stock_code,
lnk_dedup.description as description,
lnk_dedup.quantity as quantity,
lnk_dedup.invoice_ts as invoice_ts,
lnk_dedup.unit_price as unit_price,
lnk_dedup.customer_id as customer_id,
lnk_dedup.country as country,
lnk_dedup.store_id as store_id,
lnk_dedup.employee_id as employee_id,
lnk_dedup.invoice_date_key as invoice_date_key,
lnk_dedup.line_amount as line_amount,
lnk_dedup.is_cancel as is_cancel,
lnk_dedup.is_nonproduct as is_nonproduct,
lnk_dedup.is_guest as is_guest
FROM
lnk_dedup

SELECT
lnk_dedup.invoice_no as invoice_no,
lnk_dedup.stock_code as stock_code,
lnk_dedup.description as description,
lnk_dedup.quantity as quantity,
lnk_dedup.invoice_ts as invoice_ts,
lnk_dedup.unit_price as unit_price,
lnk_dedup.customer_id as customer_id,
lnk_dedup.country as country,
lnk_dedup.store_id as store_id,
lnk_dedup.employee_id as employee_id,
lnk_dedup.invoice_date_key as invoice_date_key,
lnk_dedup.line_amount as line_amount,
lnk_dedup.is_cancel as is_cancel,
lnk_dedup.is_nonproduct as is_nonproduct,
lnk_dedup.is_guest as is_guest
FROM
lnk_dedup

SELECT
lnk_anom_in.invoice_no as invoice_no,
lnk_anom_in.stock_code as stock_code,
lnk_anom_in.description as description,
lnk_anom_in.quantity as quantity,
lnk_anom_in.invoice_ts as invoice_ts,
lnk_anom_in.unit_price as unit_price,
lnk_anom_in.customer_id as customer_id,
lnk_anom_in.country as country,
lnk_anom_in.store_id as store_id,
lnk_anom_in.employee_id as employee_id,
lnk_anom_in.invoice_date_key as invoice_date_key,
lnk_anom_in.line_amount as line_amount,
lnk_anom_in.is_cancel as is_cancel,
lnk_anom_in.is_nonproduct as is_nonproduct,
lnk_anom_in.is_guest as is_guest
FROM
lnk_anom_in
WHERE unit_price = 0 or quantity > 5000 or quantity < -5000

SELECT CONCAT_WS(',',
lnk_anom.invoice_no,
lnk_anom.stock_code,
lnk_anom.description,
lnk_anom.quantity,
lnk_anom.invoice_ts,
lnk_anom.unit_price,
lnk_anom.customer_id,
lnk_anom.country,
lnk_anom.store_id,
lnk_anom.employee_id,
lnk_anom.invoice_date_key,
lnk_anom.line_amount,
lnk_anom.is_cancel,
lnk_anom.is_nonproduct,
lnk_anom.is_guest)
FROM
lnk_anom



SELECT CONCAT_WS(',',
lnk_stg.invoice_no,
lnk_stg.stock_code,
lnk_stg.description,
lnk_stg.quantity,
lnk_stg.invoice_ts,
lnk_stg.unit_price,
lnk_stg.customer_id,
lnk_stg.country,
lnk_stg.store_id,
lnk_stg.employee_id,
lnk_stg.invoice_date_key,
lnk_stg.line_amount,
lnk_stg.is_cancel,
lnk_stg.is_nonproduct,
lnk_stg.is_guest)
FROM
lnk_stg



SELECT
lnk_cnt_in.country as country,
MIN(rows_loaded) as rows_loaded
FROM
lnk_cnt_in
GROUP BY
lnk_cnt_in.country

SELECT CONCAT_WS(',',
lnk_rows_by_country.country,
lnk_rows_by_country.rows_loaded)
FROM
lnk_rows_by_country

