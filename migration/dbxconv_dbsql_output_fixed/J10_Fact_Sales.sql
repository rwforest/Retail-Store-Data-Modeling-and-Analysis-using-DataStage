SELECT
.invoice_no as invoice_no,
.stock_code as stock_code,
.description as description,
.quantity as quantity,
.invoice_ts as invoice_ts,
.unit_price as unit_price,
.customer_id as customer_id,
.country as country,
.store_id as store_id,
.employee_id as employee_id,
.invoice_date_key as invoice_date_key,
.line_amount as line_amount,
.is_cancel as is_cancel,
.is_nonproduct as is_nonproduct,
.is_guest as is_guest
FROM

WHERE is_cancel = 0 and is_nonproduct = 0 and quantity > 0

SELECT
lnk_lines.invoice_no as invoice_no,
lnk_lines.stock_code as stock_code,
lnk_lines.description as description,
lnk_lines.quantity as quantity,
lnk_lines.invoice_ts as invoice_ts,
lnk_lines.unit_price as unit_price,
lnk_lines.customer_id as customer_id,
lnk_lines.country as country,
lnk_lines.store_id as store_id,
lnk_lines.employee_id as employee_id,
lnk_lines.invoice_date_key as invoice_date_key,
lnk_lines.line_amount as line_amount,
lnk_lines.is_cancel as is_cancel,
lnk_lines.is_nonproduct as is_nonproduct,
lnk_lines.is_guest as is_guest,
CAST(lnk_lines.invoice_ts AS STRING) as year_month
FROM
lnk_lines

SELECT
lnk_keyed.invoice_no as invoice_no,
lnk_keyed.stock_code as stock_code,
lnk_keyed.description as description,
lnk_keyed.quantity as quantity,
lnk_keyed.invoice_ts as invoice_ts,
lnk_keyed.unit_price as unit_price,
lnk_keyed.customer_id as customer_id,
lnk_keyed.country as country,
lnk_keyed.store_id as store_id,
lnk_keyed.employee_id as employee_id,
lnk_keyed.invoice_date_key as invoice_date_key,
lnk_keyed.line_amount as line_amount,
lnk_keyed.is_cancel as is_cancel,
lnk_keyed.is_nonproduct as is_nonproduct,
lnk_keyed.is_guest as is_guest,
lnk_keyed.year_month as year_month,
.customer_sk as customer_sk,
.customer_type as customer_type,
.loyalty_tier as loyalty_tier,
.eff_start_key as eff_start_key,
.eff_end_key as eff_end_key
FROM
lnk_keyed
LEFT JOIN  ON lnk_keyed.customer_id = .customer_id

SELECT
lnk_c.invoice_no as invoice_no,
lnk_c.stock_code as stock_code,
lnk_c.description as description,
lnk_c.quantity as quantity,
lnk_c.invoice_ts as invoice_ts,
lnk_c.unit_price as unit_price,
lnk_c.customer_id as customer_id,
lnk_c.country as country,
lnk_c.store_id as store_id,
lnk_c.employee_id as employee_id,
lnk_c.invoice_date_key as invoice_date_key,
lnk_c.line_amount as line_amount,
lnk_c.is_cancel as is_cancel,
lnk_c.is_nonproduct as is_nonproduct,
lnk_c.is_guest as is_guest,
lnk_c.year_month as year_month,
lnk_c.customer_sk as customer_sk,
lnk_c.customer_type as customer_type,
lnk_c.loyalty_tier as loyalty_tier,
lnk_c.eff_start_key as eff_start_key,
lnk_c.eff_end_key as eff_end_key
FROM
lnk_c
WHERE customer_sk is null or (eff_start_key <= invoice_date_key and eff_end_key >= invoice_date_key)

SELECT
lnk_c_pit.invoice_no as invoice_no,
lnk_c_pit.stock_code as stock_code,
lnk_c_pit.description as description,
lnk_c_pit.quantity as quantity,
lnk_c_pit.invoice_ts as invoice_ts,
lnk_c_pit.unit_price as unit_price,
lnk_c_pit.customer_id as customer_id,
lnk_c_pit.country as country,
lnk_c_pit.store_id as store_id,
lnk_c_pit.employee_id as employee_id,
lnk_c_pit.invoice_date_key as invoice_date_key,
lnk_c_pit.line_amount as line_amount,
lnk_c_pit.is_cancel as is_cancel,
lnk_c_pit.is_nonproduct as is_nonproduct,
lnk_c_pit.is_guest as is_guest,
lnk_c_pit.year_month as year_month,
NullToValue ( lnk_c_pit.customer_sk , - 1 ) as customer_sk,
NullToValue ( lnk_c_pit.customer_type , 'unknown' ) as customer_type,
NullToValue ( lnk_c_pit.loyalty_tier , 'NONE' ) as loyalty_tier
FROM
lnk_c_pit

SELECT
lnk_c_nn.invoice_no as invoice_no,
lnk_c_nn.stock_code as stock_code,
lnk_c_nn.description as description,
lnk_c_nn.quantity as quantity,
lnk_c_nn.invoice_ts as invoice_ts,
lnk_c_nn.unit_price as unit_price,
lnk_c_nn.customer_id as customer_id,
lnk_c_nn.country as country,
lnk_c_nn.store_id as store_id,
lnk_c_nn.employee_id as employee_id,
lnk_c_nn.invoice_date_key as invoice_date_key,
lnk_c_nn.line_amount as line_amount,
lnk_c_nn.is_cancel as is_cancel,
lnk_c_nn.is_nonproduct as is_nonproduct,
lnk_c_nn.is_guest as is_guest,
lnk_c_nn.year_month as year_month,
lnk_c_nn.customer_sk as customer_sk,
lnk_c_nn.customer_type as customer_type,
lnk_c_nn.loyalty_tier as loyalty_tier,
.category as category,
.supplier_id as supplier_id,
.unit_cost as unit_cost,
.product_sk as product_sk
FROM
lnk_c_nn
LEFT JOIN  ON lnk_c_nn.stock_code = .stock_code

SELECT
lnk_p.invoice_no as invoice_no,
lnk_p.stock_code as stock_code,
lnk_p.description as description,
lnk_p.quantity as quantity,
lnk_p.invoice_ts as invoice_ts,
lnk_p.unit_price as unit_price,
lnk_p.customer_id as customer_id,
lnk_p.country as country,
lnk_p.store_id as store_id,
lnk_p.employee_id as employee_id,
lnk_p.invoice_date_key as invoice_date_key,
lnk_p.line_amount as line_amount,
lnk_p.is_cancel as is_cancel,
lnk_p.is_nonproduct as is_nonproduct,
lnk_p.is_guest as is_guest,
lnk_p.year_month as year_month,
lnk_p.customer_sk as customer_sk,
lnk_p.customer_type as customer_type,
lnk_p.loyalty_tier as loyalty_tier,
NullToValue ( lnk_p.category , 'Unknown' ) as category,
NullToValue ( lnk_p.supplier_id , 'UNKNOWN' ) as supplier_id,
NullToValue ( lnk_p.unit_cost , 0 ) as unit_cost,
NullToValue ( lnk_p.product_sk , - 1 ) as product_sk
FROM
lnk_p

SELECT
lnk_p_nn.invoice_no as invoice_no,
lnk_p_nn.stock_code as stock_code,
lnk_p_nn.description as description,
lnk_p_nn.quantity as quantity,
lnk_p_nn.invoice_ts as invoice_ts,
lnk_p_nn.unit_price as unit_price,
lnk_p_nn.customer_id as customer_id,
lnk_p_nn.country as country,
lnk_p_nn.store_id as store_id,
lnk_p_nn.employee_id as employee_id,
lnk_p_nn.invoice_date_key as invoice_date_key,
lnk_p_nn.line_amount as line_amount,
lnk_p_nn.is_cancel as is_cancel,
lnk_p_nn.is_nonproduct as is_nonproduct,
lnk_p_nn.is_guest as is_guest,
lnk_p_nn.year_month as year_month,
lnk_p_nn.customer_sk as customer_sk,
lnk_p_nn.customer_type as customer_type,
lnk_p_nn.loyalty_tier as loyalty_tier,
lnk_p_nn.category as category,
lnk_p_nn.supplier_id as supplier_id,
lnk_p_nn.unit_cost as unit_cost,
lnk_p_nn.product_sk as product_sk,
.region as region,
.store_sk as store_sk
FROM
lnk_p_nn
INNER JOIN  ON lnk_p_nn.store_id = .store_id

SELECT
lnk_s.invoice_no as invoice_no,
lnk_s.stock_code as stock_code,
lnk_s.description as description,
lnk_s.quantity as quantity,
lnk_s.invoice_ts as invoice_ts,
lnk_s.unit_price as unit_price,
lnk_s.customer_id as customer_id,
lnk_s.country as country,
lnk_s.store_id as store_id,
lnk_s.employee_id as employee_id,
lnk_s.invoice_date_key as invoice_date_key,
lnk_s.line_amount as line_amount,
lnk_s.is_cancel as is_cancel,
lnk_s.is_nonproduct as is_nonproduct,
lnk_s.is_guest as is_guest,
lnk_s.year_month as year_month,
lnk_s.customer_sk as customer_sk,
lnk_s.customer_type as customer_type,
lnk_s.loyalty_tier as loyalty_tier,
lnk_s.category as category,
lnk_s.supplier_id as supplier_id,
lnk_s.unit_cost as unit_cost,
lnk_s.product_sk as product_sk,
lnk_s.region as region,
lnk_s.store_sk as store_sk,
.employee_sk as employee_sk
FROM
lnk_s
INNER JOIN  ON lnk_s.employee_id = .employee_id

SELECT
lnk_e.invoice_no as invoice_no,
lnk_e.stock_code as stock_code,
lnk_e.description as description,
lnk_e.quantity as quantity,
lnk_e.invoice_ts as invoice_ts,
lnk_e.unit_price as unit_price,
lnk_e.customer_id as customer_id,
lnk_e.country as country,
lnk_e.store_id as store_id,
lnk_e.employee_id as employee_id,
lnk_e.invoice_date_key as invoice_date_key,
lnk_e.line_amount as line_amount,
lnk_e.is_cancel as is_cancel,
lnk_e.is_nonproduct as is_nonproduct,
lnk_e.is_guest as is_guest,
lnk_e.year_month as year_month,
lnk_e.customer_sk as customer_sk,
lnk_e.customer_type as customer_type,
lnk_e.loyalty_tier as loyalty_tier,
lnk_e.category as category,
lnk_e.supplier_id as supplier_id,
lnk_e.unit_cost as unit_cost,
lnk_e.product_sk as product_sk,
lnk_e.region as region,
lnk_e.store_sk as store_sk,
lnk_e.employee_sk as employee_sk,
.currency as currency,
.fx_rate as fx_rate
FROM
lnk_e
LEFT JOIN  ON lnk_e.country = .country AND lnk_e.year_month = .year_month

SELECT
lnk_f.invoice_no as invoice_no,
lnk_f.stock_code as stock_code,
lnk_f.description as description,
lnk_f.quantity as quantity,
lnk_f.invoice_ts as invoice_ts,
lnk_f.unit_price as unit_price,
lnk_f.customer_id as customer_id,
lnk_f.country as country,
lnk_f.store_id as store_id,
lnk_f.employee_id as employee_id,
lnk_f.invoice_date_key as invoice_date_key,
lnk_f.line_amount as line_amount,
lnk_f.is_cancel as is_cancel,
lnk_f.is_nonproduct as is_nonproduct,
lnk_f.is_guest as is_guest,
lnk_f.year_month as year_month,
lnk_f.customer_sk as customer_sk,
lnk_f.customer_type as customer_type,
lnk_f.loyalty_tier as loyalty_tier,
lnk_f.category as category,
lnk_f.supplier_id as supplier_id,
lnk_f.unit_cost as unit_cost,
lnk_f.product_sk as product_sk,
lnk_f.region as region,
lnk_f.store_sk as store_sk,
lnk_f.employee_sk as employee_sk,
NullToValue ( lnk_f.currency , 'GBP' ) as currency,
NullToValue ( lnk_f.fx_rate , 1 ) as fx_rate
FROM
lnk_f

SELECT
lnk_f_nn.invoice_no as invoice_no,
lnk_f_nn.stock_code as stock_code,
lnk_f_nn.quantity as quantity,
lnk_f_nn.invoice_ts as invoice_ts,
lnk_f_nn.unit_price as unit_price,
lnk_f_nn.customer_id as customer_id,
lnk_f_nn.country as country,
lnk_f_nn.store_id as store_id,
lnk_f_nn.invoice_date_key as invoice_date_key,
lnk_f_nn.year_month as year_month,
lnk_f_nn.customer_sk as customer_sk,
lnk_f_nn.customer_type as customer_type,
lnk_f_nn.loyalty_tier as loyalty_tier,
lnk_f_nn.category as category,
lnk_f_nn.product_sk as product_sk,
lnk_f_nn.region as region,
lnk_f_nn.store_sk as store_sk,
lnk_f_nn.employee_sk as employee_sk,
lnk_f_nn.currency as currency,
lnk_f_nn.line_amount as sales_amount,
lnk_f_nn.quantity * lnk_f_nn.unit_cost as cost_amount,
lnk_f_nn.line_amount - ( lnk_f_nn.quantity * lnk_f_nn.unit_cost ) as gross_profit,
lnk_f_nn.line_amount * lnk_f_nn.fx_rate as local_amount,
1 as line_cnt
FROM
lnk_f_nn

SELECT
lnk_fact.invoice_no as invoice_no,
lnk_fact.stock_code as stock_code,
lnk_fact.quantity as quantity,
lnk_fact.invoice_ts as invoice_ts,
lnk_fact.unit_price as unit_price,
lnk_fact.customer_id as customer_id,
lnk_fact.country as country,
lnk_fact.store_id as store_id,
lnk_fact.invoice_date_key as invoice_date_key,
lnk_fact.year_month as year_month,
lnk_fact.customer_sk as customer_sk,
lnk_fact.customer_type as customer_type,
lnk_fact.loyalty_tier as loyalty_tier,
lnk_fact.category as category,
lnk_fact.product_sk as product_sk,
lnk_fact.region as region,
lnk_fact.store_sk as store_sk,
lnk_fact.employee_sk as employee_sk,
lnk_fact.currency as currency,
lnk_fact.sales_amount as sales_amount,
lnk_fact.cost_amount as cost_amount,
lnk_fact.gross_profit as gross_profit,
lnk_fact.local_amount as local_amount,
lnk_fact.line_cnt as line_cnt
FROM
lnk_fact
ORDER BY invoice_no asc, stock_code asc

SELECT
lnk_sorted.invoice_no as invoice_no,
lnk_sorted.stock_code as stock_code,
lnk_sorted.quantity as quantity,
lnk_sorted.invoice_ts as invoice_ts,
lnk_sorted.unit_price as unit_price,
lnk_sorted.customer_id as customer_id,
lnk_sorted.country as country,
lnk_sorted.store_id as store_id,
lnk_sorted.invoice_date_key as invoice_date_key,
lnk_sorted.year_month as year_month,
lnk_sorted.customer_sk as customer_sk,
lnk_sorted.customer_type as customer_type,
lnk_sorted.loyalty_tier as loyalty_tier,
lnk_sorted.category as category,
lnk_sorted.product_sk as product_sk,
lnk_sorted.region as region,
lnk_sorted.store_sk as store_sk,
lnk_sorted.employee_sk as employee_sk,
lnk_sorted.currency as currency,
lnk_sorted.sales_amount as sales_amount,
lnk_sorted.cost_amount as cost_amount,
lnk_sorted.gross_profit as gross_profit,
lnk_sorted.local_amount as local_amount,
lnk_sorted.line_cnt as line_cnt
FROM
lnk_sorted

SELECT
lnk_sorted.invoice_no as invoice_no,
lnk_sorted.stock_code as stock_code,
lnk_sorted.quantity as quantity,
lnk_sorted.invoice_ts as invoice_ts,
lnk_sorted.unit_price as unit_price,
lnk_sorted.customer_id as customer_id,
lnk_sorted.country as country,
lnk_sorted.store_id as store_id,
lnk_sorted.invoice_date_key as invoice_date_key,
lnk_sorted.year_month as year_month,
lnk_sorted.customer_sk as customer_sk,
lnk_sorted.customer_type as customer_type,
lnk_sorted.loyalty_tier as loyalty_tier,
lnk_sorted.category as category,
lnk_sorted.product_sk as product_sk,
lnk_sorted.region as region,
lnk_sorted.store_sk as store_sk,
lnk_sorted.employee_sk as employee_sk,
lnk_sorted.currency as currency,
lnk_sorted.sales_amount as sales_amount,
lnk_sorted.cost_amount as cost_amount,
lnk_sorted.gross_profit as gross_profit,
lnk_sorted.local_amount as local_amount,
lnk_sorted.line_cnt as line_cnt
FROM
lnk_sorted

SELECT
lnk_sorted.invoice_no as invoice_no,
lnk_sorted.stock_code as stock_code,
lnk_sorted.quantity as quantity,
lnk_sorted.invoice_ts as invoice_ts,
lnk_sorted.unit_price as unit_price,
lnk_sorted.customer_id as customer_id,
lnk_sorted.country as country,
lnk_sorted.store_id as store_id,
lnk_sorted.invoice_date_key as invoice_date_key,
lnk_sorted.year_month as year_month,
lnk_sorted.customer_sk as customer_sk,
lnk_sorted.customer_type as customer_type,
lnk_sorted.loyalty_tier as loyalty_tier,
lnk_sorted.category as category,
lnk_sorted.product_sk as product_sk,
lnk_sorted.region as region,
lnk_sorted.store_sk as store_sk,
lnk_sorted.employee_sk as employee_sk,
lnk_sorted.currency as currency,
lnk_sorted.sales_amount as sales_amount,
lnk_sorted.cost_amount as cost_amount,
lnk_sorted.gross_profit as gross_profit,
lnk_sorted.local_amount as local_amount,
lnk_sorted.line_cnt as line_cnt
FROM
lnk_sorted

SELECT
lnk_orph_in.invoice_no as invoice_no,
lnk_orph_in.stock_code as stock_code,
lnk_orph_in.quantity as quantity,
lnk_orph_in.invoice_ts as invoice_ts,
lnk_orph_in.unit_price as unit_price,
lnk_orph_in.customer_id as customer_id,
lnk_orph_in.country as country,
lnk_orph_in.store_id as store_id,
lnk_orph_in.invoice_date_key as invoice_date_key,
lnk_orph_in.year_month as year_month,
lnk_orph_in.customer_sk as customer_sk,
lnk_orph_in.customer_type as customer_type,
lnk_orph_in.loyalty_tier as loyalty_tier,
lnk_orph_in.category as category,
lnk_orph_in.product_sk as product_sk,
lnk_orph_in.region as region,
lnk_orph_in.store_sk as store_sk,
lnk_orph_in.employee_sk as employee_sk,
lnk_orph_in.currency as currency,
lnk_orph_in.sales_amount as sales_amount,
lnk_orph_in.cost_amount as cost_amount,
lnk_orph_in.gross_profit as gross_profit,
lnk_orph_in.local_amount as local_amount,
lnk_orph_in.line_cnt as line_cnt
FROM
lnk_orph_in
WHERE product_sk = -1

SELECT CONCAT_WS(',',
lnk_orphans.invoice_no,
lnk_orphans.stock_code,
lnk_orphans.quantity,
lnk_orphans.invoice_ts,
lnk_orphans.unit_price,
lnk_orphans.customer_id,
lnk_orphans.country,
lnk_orphans.store_id,
lnk_orphans.invoice_date_key,
lnk_orphans.year_month,
lnk_orphans.customer_sk,
lnk_orphans.customer_type,
lnk_orphans.loyalty_tier,
lnk_orphans.category,
lnk_orphans.product_sk,
lnk_orphans.region,
lnk_orphans.store_sk,
lnk_orphans.employee_sk,
lnk_orphans.currency,
lnk_orphans.sales_amount,
lnk_orphans.cost_amount,
lnk_orphans.gross_profit,
lnk_orphans.local_amount,
lnk_orphans.line_cnt)
FROM
lnk_orphans



SELECT CONCAT_WS(',',
lnk_fact_out.invoice_no,
lnk_fact_out.stock_code,
lnk_fact_out.quantity,
lnk_fact_out.invoice_ts,
lnk_fact_out.unit_price,
lnk_fact_out.customer_id,
lnk_fact_out.country,
lnk_fact_out.store_id,
lnk_fact_out.invoice_date_key,
lnk_fact_out.year_month,
lnk_fact_out.customer_sk,
lnk_fact_out.customer_type,
lnk_fact_out.loyalty_tier,
lnk_fact_out.category,
lnk_fact_out.product_sk,
lnk_fact_out.region,
lnk_fact_out.store_sk,
lnk_fact_out.employee_sk,
lnk_fact_out.currency,
lnk_fact_out.sales_amount,
lnk_fact_out.cost_amount,
lnk_fact_out.gross_profit,
lnk_fact_out.local_amount,
lnk_fact_out.line_cnt)
FROM
lnk_fact_out



SELECT
lnk_audit_in.year_month as year_month,
MIN(lnk_audit_in.sales_amount) as sales_amount,
MIN(lnk_audit_in.gross_profit) as gross_profit
FROM
lnk_audit_in
GROUP BY
lnk_audit_in.year_month

SELECT CONCAT_WS(',',
lnk_by_month.year_month,
lnk_by_month.sales_amount,
lnk_by_month.gross_profit)
FROM
lnk_by_month

