CREATE OR REPLACE TEMP VIEW lnk_orders AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.stg_orders
);

CREATE OR REPLACE TEMP VIEW lnk_lines AS (
SELECT
coalesce(lnk_orders.invoice_no, '') as invoice_no,
coalesce(lnk_orders.stock_code, '') as stock_code,
coalesce(lnk_orders.description, '') as description,
CAST(lnk_orders.quantity AS int) as quantity,
lnk_orders.invoice_ts as invoice_ts,
CAST(lnk_orders.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_orders.customer_id AS int) as customer_id,
coalesce(lnk_orders.country, '') as country,
coalesce(lnk_orders.store_id, '') as store_id,
coalesce(lnk_orders.employee_id, '') as employee_id,
CAST(lnk_orders.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_orders.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_orders.is_cancel AS int) as is_cancel,
CAST(lnk_orders.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_orders.is_guest AS int) as is_guest
FROM lnk_orders WHERE is_cancel = 0 and is_nonproduct = 0 and quantity > 0

);

CREATE OR REPLACE TEMP VIEW lnk_keyed AS (
SELECT
coalesce(lnk_lines.invoice_no, '') as invoice_no,
coalesce(lnk_lines.stock_code, '') as stock_code,
coalesce(lnk_lines.description, '') as description,
CAST(lnk_lines.quantity AS int) as quantity,
lnk_lines.invoice_ts as invoice_ts,
CAST(lnk_lines.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_lines.customer_id AS int) as customer_id,
coalesce(lnk_lines.country, '') as country,
coalesce(lnk_lines.store_id, '') as store_id,
coalesce(lnk_lines.employee_id, '') as employee_id,
CAST(lnk_lines.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_lines.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_lines.is_cancel AS int) as is_cancel,
CAST(lnk_lines.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_lines.is_guest AS int) as is_guest,
date_format(lnk_lines.invoice_ts, 'yyyyMM') as year_month
FROM
lnk_lines

);

CREATE OR REPLACE TEMP VIEW lnk_dim_customer AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_customer
);

CREATE OR REPLACE TEMP VIEW lnk_c AS (
SELECT
coalesce(lnk_keyed.invoice_no, '') as invoice_no,
coalesce(lnk_keyed.stock_code, '') as stock_code,
coalesce(lnk_keyed.description, '') as description,
CAST(lnk_keyed.quantity AS int) as quantity,
lnk_keyed.invoice_ts as invoice_ts,
CAST(lnk_keyed.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_keyed.customer_id AS int) as customer_id,
coalesce(lnk_keyed.country, '') as country,
coalesce(lnk_keyed.store_id, '') as store_id,
coalesce(lnk_keyed.employee_id, '') as employee_id,
CAST(lnk_keyed.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_keyed.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_keyed.is_cancel AS int) as is_cancel,
CAST(lnk_keyed.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_keyed.is_guest AS int) as is_guest,
coalesce(lnk_keyed.year_month, '') as year_month,
CAST(lnk_dim_customer.customer_sk AS int) as customer_sk,
lnk_dim_customer.customer_type as customer_type,
lnk_dim_customer.loyalty_tier as loyalty_tier,
CAST(lnk_dim_customer.eff_start_key AS int) as eff_start_key,
CAST(lnk_dim_customer.eff_end_key AS int) as eff_end_key
FROM
lnk_keyed
LEFT JOIN lnk_dim_customer ON CAST(lnk_keyed.customer_id AS int) = CAST(lnk_dim_customer.customer_id AS int)

);

CREATE OR REPLACE TEMP VIEW lnk_c_pit AS (
SELECT
coalesce(lnk_c.invoice_no, '') as invoice_no,
coalesce(lnk_c.stock_code, '') as stock_code,
coalesce(lnk_c.description, '') as description,
CAST(lnk_c.quantity AS int) as quantity,
lnk_c.invoice_ts as invoice_ts,
CAST(lnk_c.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_c.customer_id AS int) as customer_id,
coalesce(lnk_c.country, '') as country,
coalesce(lnk_c.store_id, '') as store_id,
coalesce(lnk_c.employee_id, '') as employee_id,
CAST(lnk_c.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_c.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_c.is_cancel AS int) as is_cancel,
CAST(lnk_c.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_c.is_guest AS int) as is_guest,
coalesce(lnk_c.year_month, '') as year_month,
CAST(lnk_c.customer_sk AS int) as customer_sk,
lnk_c.customer_type as customer_type,
lnk_c.loyalty_tier as loyalty_tier,
CAST(lnk_c.eff_start_key AS int) as eff_start_key,
CAST(lnk_c.eff_end_key AS int) as eff_end_key
FROM
lnk_c
WHERE customer_sk is null or (eff_start_key <= invoice_date_key and eff_end_key >= invoice_date_key)

);

CREATE OR REPLACE TEMP VIEW lnk_c_nn AS (
SELECT
coalesce(lnk_c_pit.invoice_no, '') as invoice_no,
coalesce(lnk_c_pit.stock_code, '') as stock_code,
coalesce(lnk_c_pit.description, '') as description,
CAST(lnk_c_pit.quantity AS int) as quantity,
lnk_c_pit.invoice_ts as invoice_ts,
CAST(lnk_c_pit.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_c_pit.customer_id AS int) as customer_id,
coalesce(lnk_c_pit.country, '') as country,
coalesce(lnk_c_pit.store_id, '') as store_id,
coalesce(lnk_c_pit.employee_id, '') as employee_id,
CAST(lnk_c_pit.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_c_pit.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_c_pit.is_cancel AS int) as is_cancel,
CAST(lnk_c_pit.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_c_pit.is_guest AS int) as is_guest,
coalesce(lnk_c_pit.year_month, '') as year_month,
COALESCE( CAST(lnk_c_pit.customer_sk AS int) , - 1 ) as customer_sk,
COALESCE( lnk_c_pit.customer_type , 'unknown' ) as customer_type,
COALESCE( lnk_c_pit.loyalty_tier , 'NONE' ) as loyalty_tier
FROM
lnk_c_pit

);

CREATE OR REPLACE TEMP VIEW lnk_dim_product AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_product
);

CREATE OR REPLACE TEMP VIEW lnk_p AS (
SELECT
coalesce(lnk_c_nn.invoice_no, '') as invoice_no,
coalesce(lnk_c_nn.stock_code, '') as stock_code,
coalesce(lnk_c_nn.description, '') as description,
CAST(lnk_c_nn.quantity AS int) as quantity,
lnk_c_nn.invoice_ts as invoice_ts,
CAST(lnk_c_nn.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_c_nn.customer_id AS int) as customer_id,
coalesce(lnk_c_nn.country, '') as country,
coalesce(lnk_c_nn.store_id, '') as store_id,
coalesce(lnk_c_nn.employee_id, '') as employee_id,
CAST(lnk_c_nn.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_c_nn.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_c_nn.is_cancel AS int) as is_cancel,
CAST(lnk_c_nn.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_c_nn.is_guest AS int) as is_guest,
coalesce(lnk_c_nn.year_month, '') as year_month,
CAST(lnk_c_nn.customer_sk AS int) as customer_sk,
coalesce(lnk_c_nn.customer_type, '') as customer_type,
coalesce(lnk_c_nn.loyalty_tier, '') as loyalty_tier,
lnk_dim_product.category as category,
lnk_dim_product.supplier_id as supplier_id,
CAST(lnk_dim_product.unit_cost AS decimal(18,2)) as unit_cost,
CAST(lnk_dim_product.product_sk AS int) as product_sk
FROM
lnk_c_nn
LEFT JOIN lnk_dim_product ON coalesce(lnk_c_nn.stock_code, '') = lnk_dim_product.stock_code

);

CREATE OR REPLACE TEMP VIEW lnk_p_nn AS (
SELECT
coalesce(lnk_p.invoice_no, '') as invoice_no,
coalesce(lnk_p.stock_code, '') as stock_code,
coalesce(lnk_p.description, '') as description,
CAST(lnk_p.quantity AS int) as quantity,
lnk_p.invoice_ts as invoice_ts,
CAST(lnk_p.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_p.customer_id AS int) as customer_id,
coalesce(lnk_p.country, '') as country,
coalesce(lnk_p.store_id, '') as store_id,
coalesce(lnk_p.employee_id, '') as employee_id,
CAST(lnk_p.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_p.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_p.is_cancel AS int) as is_cancel,
CAST(lnk_p.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_p.is_guest AS int) as is_guest,
coalesce(lnk_p.year_month, '') as year_month,
CAST(lnk_p.customer_sk AS int) as customer_sk,
coalesce(lnk_p.customer_type, '') as customer_type,
coalesce(lnk_p.loyalty_tier, '') as loyalty_tier,
COALESCE( lnk_p.category , 'Unknown' ) as category,
COALESCE( lnk_p.supplier_id , 'UNKNOWN' ) as supplier_id,
COALESCE( CAST(lnk_p.unit_cost AS decimal(18,2)) , 0 ) as unit_cost,
COALESCE( CAST(lnk_p.product_sk AS int) , - 1 ) as product_sk
FROM
lnk_p

);

CREATE OR REPLACE TEMP VIEW lnk_dim_store AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_store
);

CREATE OR REPLACE TEMP VIEW lnk_s AS (
SELECT
coalesce(lnk_p_nn.invoice_no, '') as invoice_no,
coalesce(lnk_p_nn.stock_code, '') as stock_code,
coalesce(lnk_p_nn.description, '') as description,
CAST(lnk_p_nn.quantity AS int) as quantity,
lnk_p_nn.invoice_ts as invoice_ts,
CAST(lnk_p_nn.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_p_nn.customer_id AS int) as customer_id,
coalesce(lnk_p_nn.country, '') as country,
coalesce(lnk_p_nn.store_id, '') as store_id,
coalesce(lnk_p_nn.employee_id, '') as employee_id,
CAST(lnk_p_nn.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_p_nn.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_p_nn.is_cancel AS int) as is_cancel,
CAST(lnk_p_nn.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_p_nn.is_guest AS int) as is_guest,
coalesce(lnk_p_nn.year_month, '') as year_month,
CAST(lnk_p_nn.customer_sk AS int) as customer_sk,
coalesce(lnk_p_nn.customer_type, '') as customer_type,
coalesce(lnk_p_nn.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_p_nn.category, '') as category,
coalesce(lnk_p_nn.supplier_id, '') as supplier_id,
CAST(lnk_p_nn.unit_cost AS decimal(18,2)) as unit_cost,
CAST(lnk_p_nn.product_sk AS int) as product_sk,
coalesce(lnk_dim_store.region, '') as region,
CAST(lnk_dim_store.store_sk AS int) as store_sk
FROM
lnk_p_nn
INNER JOIN lnk_dim_store ON coalesce(lnk_p_nn.store_id, '') = coalesce(lnk_dim_store.store_id, '')

);

CREATE OR REPLACE TEMP VIEW lnk_dim_emp AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_employee
);

CREATE OR REPLACE TEMP VIEW lnk_e AS (
SELECT
coalesce(lnk_s.invoice_no, '') as invoice_no,
coalesce(lnk_s.stock_code, '') as stock_code,
coalesce(lnk_s.description, '') as description,
CAST(lnk_s.quantity AS int) as quantity,
lnk_s.invoice_ts as invoice_ts,
CAST(lnk_s.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_s.customer_id AS int) as customer_id,
coalesce(lnk_s.country, '') as country,
coalesce(lnk_s.store_id, '') as store_id,
coalesce(lnk_s.employee_id, '') as employee_id,
CAST(lnk_s.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_s.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_s.is_cancel AS int) as is_cancel,
CAST(lnk_s.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_s.is_guest AS int) as is_guest,
coalesce(lnk_s.year_month, '') as year_month,
CAST(lnk_s.customer_sk AS int) as customer_sk,
coalesce(lnk_s.customer_type, '') as customer_type,
coalesce(lnk_s.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_s.category, '') as category,
coalesce(lnk_s.supplier_id, '') as supplier_id,
CAST(lnk_s.unit_cost AS decimal(18,2)) as unit_cost,
CAST(lnk_s.product_sk AS int) as product_sk,
coalesce(lnk_s.region, '') as region,
CAST(lnk_s.store_sk AS int) as store_sk,
CAST(lnk_dim_emp.employee_sk AS int) as employee_sk
FROM
lnk_s
INNER JOIN lnk_dim_emp ON coalesce(lnk_s.employee_id, '') = coalesce(lnk_dim_emp.employee_id, '')

);

CREATE OR REPLACE TEMP VIEW lnk_dim_fx AS (
  SELECT * FROM unitygo.retail_dw_dbxconv_dbsql.dim_fx
);

CREATE OR REPLACE TEMP VIEW lnk_f AS (
SELECT
coalesce(lnk_e.invoice_no, '') as invoice_no,
coalesce(lnk_e.stock_code, '') as stock_code,
coalesce(lnk_e.description, '') as description,
CAST(lnk_e.quantity AS int) as quantity,
lnk_e.invoice_ts as invoice_ts,
CAST(lnk_e.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_e.customer_id AS int) as customer_id,
coalesce(lnk_e.country, '') as country,
coalesce(lnk_e.store_id, '') as store_id,
coalesce(lnk_e.employee_id, '') as employee_id,
CAST(lnk_e.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_e.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_e.is_cancel AS int) as is_cancel,
CAST(lnk_e.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_e.is_guest AS int) as is_guest,
coalesce(lnk_e.year_month, '') as year_month,
CAST(lnk_e.customer_sk AS int) as customer_sk,
coalesce(lnk_e.customer_type, '') as customer_type,
coalesce(lnk_e.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_e.category, '') as category,
coalesce(lnk_e.supplier_id, '') as supplier_id,
CAST(lnk_e.unit_cost AS decimal(18,2)) as unit_cost,
CAST(lnk_e.product_sk AS int) as product_sk,
coalesce(lnk_e.region, '') as region,
CAST(lnk_e.store_sk AS int) as store_sk,
CAST(lnk_e.employee_sk AS int) as employee_sk,
lnk_dim_fx.currency as currency,
CAST(lnk_dim_fx.fx_rate AS decimal(18,6)) as fx_rate
FROM
lnk_e
LEFT JOIN lnk_dim_fx ON coalesce(lnk_e.country, '') = lnk_dim_fx.country AND coalesce(lnk_e.year_month, '') = lnk_dim_fx.year_month

);

CREATE OR REPLACE TEMP VIEW lnk_f_nn AS (
SELECT
coalesce(lnk_f.invoice_no, '') as invoice_no,
coalesce(lnk_f.stock_code, '') as stock_code,
coalesce(lnk_f.description, '') as description,
CAST(lnk_f.quantity AS int) as quantity,
lnk_f.invoice_ts as invoice_ts,
CAST(lnk_f.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_f.customer_id AS int) as customer_id,
coalesce(lnk_f.country, '') as country,
coalesce(lnk_f.store_id, '') as store_id,
coalesce(lnk_f.employee_id, '') as employee_id,
CAST(lnk_f.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_f.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_f.is_cancel AS int) as is_cancel,
CAST(lnk_f.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_f.is_guest AS int) as is_guest,
coalesce(lnk_f.year_month, '') as year_month,
CAST(lnk_f.customer_sk AS int) as customer_sk,
coalesce(lnk_f.customer_type, '') as customer_type,
coalesce(lnk_f.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_f.category, '') as category,
coalesce(lnk_f.supplier_id, '') as supplier_id,
CAST(lnk_f.unit_cost AS decimal(18,2)) as unit_cost,
CAST(lnk_f.product_sk AS int) as product_sk,
coalesce(lnk_f.region, '') as region,
CAST(lnk_f.store_sk AS int) as store_sk,
CAST(lnk_f.employee_sk AS int) as employee_sk,
COALESCE( lnk_f.currency , 'GBP' ) as currency,
COALESCE( CAST(lnk_f.fx_rate AS decimal(18,6)) , 1 ) as fx_rate
FROM
lnk_f

);

CREATE OR REPLACE TEMP VIEW lnk_fact AS (
SELECT
coalesce(lnk_f_nn.invoice_no, '') as invoice_no,
coalesce(lnk_f_nn.stock_code, '') as stock_code,
CAST(lnk_f_nn.quantity AS int) as quantity,
lnk_f_nn.invoice_ts as invoice_ts,
CAST(lnk_f_nn.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_f_nn.customer_id AS int) as customer_id,
coalesce(lnk_f_nn.country, '') as country,
coalesce(lnk_f_nn.store_id, '') as store_id,
CAST(lnk_f_nn.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_f_nn.year_month, '') as year_month,
CAST(lnk_f_nn.customer_sk AS int) as customer_sk,
coalesce(lnk_f_nn.customer_type, '') as customer_type,
coalesce(lnk_f_nn.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_f_nn.category, '') as category,
CAST(lnk_f_nn.product_sk AS int) as product_sk,
coalesce(lnk_f_nn.region, '') as region,
CAST(lnk_f_nn.store_sk AS int) as store_sk,
CAST(lnk_f_nn.employee_sk AS int) as employee_sk,
coalesce(lnk_f_nn.currency, '') as currency,
CAST(lnk_f_nn.line_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_f_nn.quantity AS int) * CAST(lnk_f_nn.unit_cost AS decimal(18,2)) as cost_amount,
CAST(lnk_f_nn.line_amount AS decimal(18,2)) - ( CAST(lnk_f_nn.quantity AS int) * CAST(lnk_f_nn.unit_cost AS decimal(18,2)) ) as gross_profit,
CAST(lnk_f_nn.line_amount AS decimal(18,2)) * CAST(lnk_f_nn.fx_rate AS decimal(18,6)) as local_amount,
1 as line_cnt
FROM
lnk_f_nn

);

CREATE OR REPLACE TEMP VIEW lnk_sorted AS (
SELECT
coalesce(lnk_fact.invoice_no, '') as invoice_no,
coalesce(lnk_fact.stock_code, '') as stock_code,
CAST(lnk_fact.quantity AS int) as quantity,
lnk_fact.invoice_ts as invoice_ts,
CAST(lnk_fact.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_fact.customer_id AS int) as customer_id,
coalesce(lnk_fact.country, '') as country,
coalesce(lnk_fact.store_id, '') as store_id,
CAST(lnk_fact.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_fact.year_month, '') as year_month,
CAST(lnk_fact.customer_sk AS int) as customer_sk,
coalesce(lnk_fact.customer_type, '') as customer_type,
coalesce(lnk_fact.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_fact.category, '') as category,
CAST(lnk_fact.product_sk AS int) as product_sk,
coalesce(lnk_fact.region, '') as region,
CAST(lnk_fact.store_sk AS int) as store_sk,
CAST(lnk_fact.employee_sk AS int) as employee_sk,
coalesce(lnk_fact.currency, '') as currency,
CAST(lnk_fact.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_fact.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_fact.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_fact.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_fact.line_cnt AS int) as line_cnt
FROM
lnk_fact
ORDER BY invoice_no asc, stock_code asc

);

CREATE OR REPLACE TEMP VIEW lnk_fact_out AS (
SELECT
coalesce(lnk_sorted.invoice_no, '') as invoice_no,
coalesce(lnk_sorted.stock_code, '') as stock_code,
CAST(lnk_sorted.quantity AS int) as quantity,
lnk_sorted.invoice_ts as invoice_ts,
CAST(lnk_sorted.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_sorted.customer_id AS int) as customer_id,
coalesce(lnk_sorted.country, '') as country,
coalesce(lnk_sorted.store_id, '') as store_id,
CAST(lnk_sorted.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_sorted.year_month, '') as year_month,
CAST(lnk_sorted.customer_sk AS int) as customer_sk,
coalesce(lnk_sorted.customer_type, '') as customer_type,
coalesce(lnk_sorted.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_sorted.category, '') as category,
CAST(lnk_sorted.product_sk AS int) as product_sk,
coalesce(lnk_sorted.region, '') as region,
CAST(lnk_sorted.store_sk AS int) as store_sk,
CAST(lnk_sorted.employee_sk AS int) as employee_sk,
coalesce(lnk_sorted.currency, '') as currency,
CAST(lnk_sorted.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_sorted.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_sorted.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_sorted.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_sorted.line_cnt AS int) as line_cnt
FROM
lnk_sorted

);

CREATE OR REPLACE TEMP VIEW lnk_orph_in AS (
SELECT
coalesce(lnk_sorted.invoice_no, '') as invoice_no,
coalesce(lnk_sorted.stock_code, '') as stock_code,
CAST(lnk_sorted.quantity AS int) as quantity,
lnk_sorted.invoice_ts as invoice_ts,
CAST(lnk_sorted.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_sorted.customer_id AS int) as customer_id,
coalesce(lnk_sorted.country, '') as country,
coalesce(lnk_sorted.store_id, '') as store_id,
CAST(lnk_sorted.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_sorted.year_month, '') as year_month,
CAST(lnk_sorted.customer_sk AS int) as customer_sk,
coalesce(lnk_sorted.customer_type, '') as customer_type,
coalesce(lnk_sorted.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_sorted.category, '') as category,
CAST(lnk_sorted.product_sk AS int) as product_sk,
coalesce(lnk_sorted.region, '') as region,
CAST(lnk_sorted.store_sk AS int) as store_sk,
CAST(lnk_sorted.employee_sk AS int) as employee_sk,
coalesce(lnk_sorted.currency, '') as currency,
CAST(lnk_sorted.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_sorted.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_sorted.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_sorted.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_sorted.line_cnt AS int) as line_cnt
FROM
lnk_sorted

);

CREATE OR REPLACE TEMP VIEW lnk_audit_in AS (
SELECT
coalesce(lnk_sorted.invoice_no, '') as invoice_no,
coalesce(lnk_sorted.stock_code, '') as stock_code,
CAST(lnk_sorted.quantity AS int) as quantity,
lnk_sorted.invoice_ts as invoice_ts,
CAST(lnk_sorted.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_sorted.customer_id AS int) as customer_id,
coalesce(lnk_sorted.country, '') as country,
coalesce(lnk_sorted.store_id, '') as store_id,
CAST(lnk_sorted.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_sorted.year_month, '') as year_month,
CAST(lnk_sorted.customer_sk AS int) as customer_sk,
coalesce(lnk_sorted.customer_type, '') as customer_type,
coalesce(lnk_sorted.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_sorted.category, '') as category,
CAST(lnk_sorted.product_sk AS int) as product_sk,
coalesce(lnk_sorted.region, '') as region,
CAST(lnk_sorted.store_sk AS int) as store_sk,
CAST(lnk_sorted.employee_sk AS int) as employee_sk,
coalesce(lnk_sorted.currency, '') as currency,
CAST(lnk_sorted.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_sorted.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_sorted.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_sorted.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_sorted.line_cnt AS int) as line_cnt
FROM
lnk_sorted

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.fact_sales AS
SELECT
  *
FROM lnk_fact_out;

CREATE OR REPLACE TEMP VIEW lnk_orphans AS (
SELECT
coalesce(lnk_orph_in.invoice_no, '') as invoice_no,
coalesce(lnk_orph_in.stock_code, '') as stock_code,
CAST(lnk_orph_in.quantity AS int) as quantity,
lnk_orph_in.invoice_ts as invoice_ts,
CAST(lnk_orph_in.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_orph_in.customer_id AS int) as customer_id,
coalesce(lnk_orph_in.country, '') as country,
coalesce(lnk_orph_in.store_id, '') as store_id,
CAST(lnk_orph_in.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_orph_in.year_month, '') as year_month,
CAST(lnk_orph_in.customer_sk AS int) as customer_sk,
coalesce(lnk_orph_in.customer_type, '') as customer_type,
coalesce(lnk_orph_in.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_orph_in.category, '') as category,
CAST(lnk_orph_in.product_sk AS int) as product_sk,
coalesce(lnk_orph_in.region, '') as region,
CAST(lnk_orph_in.store_sk AS int) as store_sk,
CAST(lnk_orph_in.employee_sk AS int) as employee_sk,
coalesce(lnk_orph_in.currency, '') as currency,
CAST(lnk_orph_in.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_orph_in.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_orph_in.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_orph_in.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_orph_in.line_cnt AS int) as line_cnt
FROM
lnk_orph_in
WHERE product_sk = -1

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.dq_orphan_products AS
SELECT
  *
FROM lnk_orphans;

CREATE OR REPLACE TEMP VIEW lnk_by_month AS (
SELECT
coalesce(lnk_audit_in.year_month, '') as year_month,
SUM(sales_amount) as sales_amount,
SUM(gross_profit) as gross_profit
FROM
lnk_audit_in
GROUP BY
coalesce(lnk_audit_in.year_month, '')

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.audit_fact_by_month AS
SELECT
  *
FROM lnk_by_month;