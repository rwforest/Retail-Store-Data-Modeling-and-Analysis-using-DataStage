# Databricks notebook source
# J10_Fact_Sales - Fact_Sales at invoice-line grain with point-in-time customer SCD2 lookup
# Layer: 3_Facts. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_stg_orders: dataset stg_orders
@dp.temporary_view(name="j10_lnk_orders")
def j10_lnk_orders():
    return spark.read.table("stg_orders")

# COMMAND ----------
# flt_Sales_Lines: Filter
@dp.temporary_view(name="j10_lnk_lines")
def j10_lnk_lines():
    return spark.sql(f"""
SELECT * FROM j10_lnk_orders
WHERE (((is_cancel = 0) AND (is_nonproduct = 0)) AND (quantity > 0))
""")

# COMMAND ----------
# xfm_Fact_Keys: Transformer
@dp.temporary_view(name="j10_lnk_keyed")
def j10_lnk_keyed():
    return spark.sql(f"""
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
  is_guest,
  CAST(date_format(invoice_ts, 'yyyyMM') AS string) AS year_month
FROM j10_lnk_lines
""")

# COMMAND ----------
# src_dim_customer: dataset dim_customer
@dp.temporary_view(name="j10_lnk_dim_customer")
def j10_lnk_dim_customer():
    return spark.read.table("dim_customer")

# COMMAND ----------
# jn_Customer: Join (left outer) on customer_id
@dp.temporary_view(name="j10_lnk_c")
def j10_lnk_c():
    return spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.description,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.employee_id,
  l.invoice_date_key,
  l.line_amount,
  l.is_cancel,
  l.is_nonproduct,
  l.is_guest,
  l.year_month,
  r.customer_sk,
  r.customer_type,
  r.loyalty_tier,
  r.eff_start_key,
  r.eff_end_key
FROM j10_lnk_keyed l
LEFT OUTER JOIN j10_lnk_dim_customer r ON l.customer_id = r.customer_id
""")

# COMMAND ----------
# flt_PIT_Customer: Filter
@dp.temporary_view(name="j10_lnk_c_pit")
def j10_lnk_c_pit():
    return spark.sql(f"""
SELECT * FROM j10_lnk_c
WHERE ((customer_sk IS NULL) OR ((eff_start_key <= invoice_date_key) AND (eff_end_key >= invoice_date_key)))
""")

# COMMAND ----------
# xfm_Customer_Defaults: Transformer
@dp.temporary_view(name="j10_lnk_c_nn")
def j10_lnk_c_nn():
    return spark.sql(f"""
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
  is_guest,
  year_month,
  CAST(coalesce(customer_sk, (-1)) AS int) AS customer_sk,
  CAST(coalesce(customer_type, 'unknown') AS string) AS customer_type,
  CAST(coalesce(loyalty_tier, 'NONE') AS string) AS loyalty_tier
FROM j10_lnk_c_pit
""")

# COMMAND ----------
# src_dim_product: dataset dim_product
@dp.temporary_view(name="j10_lnk_dim_product")
def j10_lnk_dim_product():
    return spark.read.table("dim_product")

# COMMAND ----------
# jn_Product: Join (left outer) on stock_code
@dp.temporary_view(name="j10_lnk_p")
def j10_lnk_p():
    return spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.description,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.employee_id,
  l.invoice_date_key,
  l.line_amount,
  l.is_cancel,
  l.is_nonproduct,
  l.is_guest,
  l.year_month,
  l.customer_sk,
  l.customer_type,
  l.loyalty_tier,
  r.category,
  r.supplier_id,
  r.unit_cost,
  r.product_sk
FROM j10_lnk_c_nn l
LEFT OUTER JOIN j10_lnk_dim_product r ON l.stock_code = r.stock_code
""")

# COMMAND ----------
# xfm_Product_Defaults: Transformer
@dp.temporary_view(name="j10_lnk_p_nn")
def j10_lnk_p_nn():
    return spark.sql(f"""
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
  is_guest,
  year_month,
  customer_sk,
  customer_type,
  loyalty_tier,
  CAST(coalesce(category, 'Unknown') AS string) AS category,
  CAST(coalesce(supplier_id, 'UNKNOWN') AS string) AS supplier_id,
  CAST(coalesce(unit_cost, 0) AS decimal(18,2)) AS unit_cost,
  CAST(coalesce(product_sk, (-1)) AS int) AS product_sk
FROM j10_lnk_p
""")

# COMMAND ----------
# src_dim_store: dataset dim_store
@dp.temporary_view(name="j10_lnk_dim_store")
def j10_lnk_dim_store():
    return spark.read.table("dim_store")

# COMMAND ----------
# jn_Store: Join (inner) on store_id
@dp.temporary_view(name="j10_lnk_s")
def j10_lnk_s():
    return spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.description,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.employee_id,
  l.invoice_date_key,
  l.line_amount,
  l.is_cancel,
  l.is_nonproduct,
  l.is_guest,
  l.year_month,
  l.customer_sk,
  l.customer_type,
  l.loyalty_tier,
  l.category,
  l.supplier_id,
  l.unit_cost,
  l.product_sk,
  r.region,
  r.store_sk
FROM j10_lnk_p_nn l
INNER JOIN j10_lnk_dim_store r ON l.store_id = r.store_id
""")

# COMMAND ----------
# src_dim_employee: dataset dim_employee
@dp.temporary_view(name="j10_lnk_dim_emp")
def j10_lnk_dim_emp():
    return spark.read.table("dim_employee")

# COMMAND ----------
# jn_Employee: Join (inner) on employee_id
@dp.temporary_view(name="j10_lnk_e")
def j10_lnk_e():
    return spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.description,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.employee_id,
  l.invoice_date_key,
  l.line_amount,
  l.is_cancel,
  l.is_nonproduct,
  l.is_guest,
  l.year_month,
  l.customer_sk,
  l.customer_type,
  l.loyalty_tier,
  l.category,
  l.supplier_id,
  l.unit_cost,
  l.product_sk,
  l.region,
  l.store_sk,
  r.employee_sk
FROM j10_lnk_s l
INNER JOIN j10_lnk_dim_emp r ON l.employee_id = r.employee_id
""")

# COMMAND ----------
# src_dim_fx: dataset dim_fx
@dp.temporary_view(name="j10_lnk_dim_fx")
def j10_lnk_dim_fx():
    return spark.read.table("dim_fx")

# COMMAND ----------
# jn_FX: Join (left outer) on country, year_month
@dp.temporary_view(name="j10_lnk_f")
def j10_lnk_f():
    return spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.description,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.employee_id,
  l.invoice_date_key,
  l.line_amount,
  l.is_cancel,
  l.is_nonproduct,
  l.is_guest,
  l.year_month,
  l.customer_sk,
  l.customer_type,
  l.loyalty_tier,
  l.category,
  l.supplier_id,
  l.unit_cost,
  l.product_sk,
  l.region,
  l.store_sk,
  l.employee_sk,
  r.currency,
  r.fx_rate
FROM j10_lnk_e l
LEFT OUTER JOIN j10_lnk_dim_fx r ON l.country = r.country AND l.year_month = r.year_month
""")

# COMMAND ----------
# xfm_FX_Defaults: Transformer
@dp.temporary_view(name="j10_lnk_f_nn")
def j10_lnk_f_nn():
    return spark.sql(f"""
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
  is_guest,
  year_month,
  customer_sk,
  customer_type,
  loyalty_tier,
  category,
  supplier_id,
  unit_cost,
  product_sk,
  region,
  store_sk,
  employee_sk,
  CAST(coalesce(currency, 'GBP') AS string) AS currency,
  CAST(coalesce(fx_rate, 1) AS decimal(18,6)) AS fx_rate
FROM j10_lnk_f
""")

# COMMAND ----------
# xfm_Fact_Measures: Transformer
@dp.temporary_view(name="j10_lnk_fact")
def j10_lnk_fact():
    return spark.sql(f"""
SELECT
  invoice_no,
  stock_code,
  quantity,
  invoice_ts,
  unit_price,
  customer_id,
  country,
  store_id,
  invoice_date_key,
  year_month,
  customer_sk,
  customer_type,
  loyalty_tier,
  category,
  product_sk,
  region,
  store_sk,
  employee_sk,
  currency,
  line_amount AS sales_amount,
  CAST((quantity * unit_cost) AS decimal(18,2)) AS cost_amount,
  CAST((line_amount - (quantity * unit_cost)) AS decimal(18,2)) AS gross_profit,
  CAST((line_amount * fx_rate) AS decimal(18,2)) AS local_amount,
  CAST(1 AS int) AS line_cnt
FROM j10_lnk_f_nn
""")

# COMMAND ----------
# srt_Fact_Sales: Sort
@dp.temporary_view(name="j10_lnk_sorted")
def j10_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j10_lnk_fact
ORDER BY invoice_no ASC, stock_code ASC
""")

# COMMAND ----------
# cpy_Fact: Copy
@dp.temporary_view(name="j10_lnk_fact_out")
def j10_lnk_fact_out():
    return spark.sql(f"""
SELECT * FROM j10_lnk_sorted
""")

# COMMAND ----------
# cpy_Fact: Copy
@dp.temporary_view(name="j10_lnk_orph_in")
def j10_lnk_orph_in():
    return spark.sql(f"""
SELECT * FROM j10_lnk_sorted
""")

# COMMAND ----------
# cpy_Fact: Copy
@dp.temporary_view(name="j10_lnk_audit_in")
def j10_lnk_audit_in():
    return spark.sql(f"""
SELECT * FROM j10_lnk_sorted
""")

# COMMAND ----------
# tgt_fact_sales: target file -> materialized view fact_sales
@dp.materialized_view(name="fact_sales")
def fact_sales():
    return spark.read.table("j10_lnk_fact_out").select('invoice_no', 'stock_code', 'quantity', 'invoice_ts', 'unit_price', 'customer_id', 'country', 'store_id', 'invoice_date_key', 'year_month', 'customer_sk', 'customer_type', 'loyalty_tier', 'category', 'product_sk', 'region', 'store_sk', 'employee_sk', 'currency', 'sales_amount', 'cost_amount', 'gross_profit', 'local_amount', 'line_cnt')

# COMMAND ----------
# flt_Orphan_Product: Filter
@dp.temporary_view(name="j10_lnk_orphans")
def j10_lnk_orphans():
    return spark.sql(f"""
SELECT * FROM j10_lnk_orph_in
WHERE (product_sk = (-1))
""")

# COMMAND ----------
# tgt_dq_orphan_products: target file -> materialized view dq_orphan_products
@dp.materialized_view(name="dq_orphan_products")
def dq_orphan_products():
    return spark.read.table("j10_lnk_orphans").select('invoice_no', 'stock_code', 'quantity', 'invoice_ts', 'unit_price', 'customer_id', 'country', 'store_id', 'invoice_date_key', 'year_month', 'customer_sk', 'customer_type', 'loyalty_tier', 'category', 'product_sk', 'region', 'store_sk', 'employee_sk', 'currency', 'sales_amount', 'cost_amount', 'gross_profit', 'local_amount', 'line_cnt')

# COMMAND ----------
# agg_Fact_By_Month: Aggregator
@dp.temporary_view(name="j10_lnk_by_month")
def j10_lnk_by_month():
    return spark.sql(f"""
SELECT year_month, CAST(SUM(sales_amount) AS decimal(18,2)) AS sales_amount, CAST(SUM(gross_profit) AS decimal(18,2)) AS gross_profit
FROM j10_lnk_audit_in
GROUP BY year_month
""")

# COMMAND ----------
# tgt_audit_fact_by_month: target file -> materialized view audit_fact_by_month
@dp.materialized_view(name="audit_fact_by_month")
def audit_fact_by_month():
    return spark.read.table("j10_lnk_by_month").select('year_month', 'sales_amount', 'gross_profit')
