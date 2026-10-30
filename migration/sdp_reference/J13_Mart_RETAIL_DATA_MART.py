# Databricks notebook source
# J13_Mart_RETAIL_DATA_MART - RETAIL_DATA_MART: every identified-customer transaction, citizen/foreign, with product & store
# Layer: 4_Marts. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_fact_sales: dataset fact_sales
@dp.temporary_view(name="j13_lnk_fact")
def j13_lnk_fact():
    return spark.read.table("fact_sales")

# COMMAND ----------
# src_dim_customer: dataset dim_customer
@dp.temporary_view(name="j13_lnk_dim_customer")
def j13_lnk_dim_customer():
    return spark.read.table("dim_customer")

# COMMAND ----------
# jn_Customer: Join (inner) on customer_sk
@dp.temporary_view(name="j13_lnk_c")
def j13_lnk_c():
    return spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.invoice_date_key,
  l.year_month,
  l.customer_sk,
  l.customer_type,
  l.loyalty_tier,
  l.category,
  l.product_sk,
  l.region,
  l.store_sk,
  l.employee_sk,
  l.currency,
  l.sales_amount,
  l.cost_amount,
  l.gross_profit,
  l.local_amount,
  l.line_cnt,
  r.cust_name,
  r.gender,
  r.age,
  r.spending_score,
  r.annual_income_k
FROM j13_lnk_fact l
INNER JOIN j13_lnk_dim_customer r ON l.customer_sk = r.customer_sk
""")

# COMMAND ----------
# src_dim_product: dataset dim_product
@dp.temporary_view(name="j13_lnk_dim_product")
def j13_lnk_dim_product():
    return spark.read.table("dim_product")

# COMMAND ----------
# jn_Product: Join (inner) on product_sk
@dp.temporary_view(name="j13_lnk_p")
def j13_lnk_p():
    return spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.invoice_date_key,
  l.year_month,
  l.customer_sk,
  l.customer_type,
  l.loyalty_tier,
  l.category,
  l.product_sk,
  l.region,
  l.store_sk,
  l.employee_sk,
  l.currency,
  l.sales_amount,
  l.cost_amount,
  l.gross_profit,
  l.local_amount,
  l.line_cnt,
  l.cust_name,
  l.gender,
  l.age,
  l.spending_score,
  l.annual_income_k,
  r.product_name,
  r.list_price
FROM j13_lnk_c l
INNER JOIN j13_lnk_dim_product r ON l.product_sk = r.product_sk
""")

# COMMAND ----------
# src_dim_store: dataset dim_store
@dp.temporary_view(name="j13_lnk_dim_store")
def j13_lnk_dim_store():
    return spark.read.table("dim_store")

# COMMAND ----------
# jn_Store: Join (inner) on store_sk
@dp.temporary_view(name="j13_lnk_s")
def j13_lnk_s():
    return spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.invoice_date_key,
  l.year_month,
  l.customer_sk,
  l.customer_type,
  l.loyalty_tier,
  l.category,
  l.product_sk,
  l.region,
  l.store_sk,
  l.employee_sk,
  l.currency,
  l.sales_amount,
  l.cost_amount,
  l.gross_profit,
  l.local_amount,
  l.line_cnt,
  l.cust_name,
  l.gender,
  l.age,
  l.spending_score,
  l.annual_income_k,
  l.product_name,
  l.list_price,
  r.store_name,
  r.rating,
  r.no_of_employees
FROM j13_lnk_p l
INNER JOIN j13_lnk_dim_store r ON l.store_sk = r.store_sk
""")

# COMMAND ----------
# flt_Customer_Types: Filter
@dp.temporary_view(name="j13_lnk_types")
def j13_lnk_types():
    return spark.sql(f"""
SELECT * FROM j13_lnk_s
WHERE ((customer_type = 'citizen') OR (customer_type = 'foreign'))
""")

# COMMAND ----------
# xfm_Upper_Names: Transformer
@dp.temporary_view(name="j13_lnk_upper")
def j13_lnk_upper():
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
  sales_amount,
  cost_amount,
  gross_profit,
  local_amount,
  line_cnt,
  CAST(upper(cust_name) AS string) AS cust_name,
  gender,
  age,
  spending_score,
  annual_income_k,
  CAST(upper(product_name) AS string) AS product_name,
  list_price,
  store_name,
  rating,
  no_of_employees
FROM j13_lnk_types
""")

# COMMAND ----------
# srt_Mart: Sort
@dp.temporary_view(name="j13_lnk_sorted")
def j13_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j13_lnk_upper
ORDER BY invoice_no ASC, stock_code ASC
""")

# COMMAND ----------
# tgt_mart_retail_data_mart: target file -> materialized view RETAIL_DATA_MART
@dp.materialized_view(name="RETAIL_DATA_MART")
def RETAIL_DATA_MART():
    return spark.read.table("j13_lnk_sorted").select('invoice_no', 'stock_code', 'quantity', 'invoice_ts', 'unit_price', 'customer_id', 'country', 'store_id', 'invoice_date_key', 'year_month', 'customer_sk', 'customer_type', 'loyalty_tier', 'category', 'product_sk', 'region', 'store_sk', 'employee_sk', 'currency', 'sales_amount', 'cost_amount', 'gross_profit', 'local_amount', 'line_cnt', 'cust_name', 'gender', 'age', 'spending_score', 'annual_income_k', 'product_name', 'list_price', 'store_name', 'rating', 'no_of_employees')
