# Databricks notebook source
# J11_Fact_Returns - Fact_Returns from cancellation invoices (prefix C)
# Layer: 3_Facts. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_stg_orders: dataset stg_orders
@dp.temporary_view(name="j11_lnk_orders")
def j11_lnk_orders():
    return spark.read.table("stg_orders")

# COMMAND ----------
# flt_Cancels: Filter
@dp.temporary_view(name="j11_lnk_cancels")
def j11_lnk_cancels():
    return spark.sql(f"""
SELECT * FROM j11_lnk_orders
WHERE ((is_cancel = 1) AND (is_nonproduct = 0))
""")

# COMMAND ----------
# xfm_Return_Measures: Transformer
@dp.temporary_view(name="j11_lnk_ret")
def j11_lnk_ret():
    return spark.sql(f"""
SELECT
  invoice_no,
  stock_code,
  customer_id,
  country,
  invoice_date_key,
  CAST(date_format(invoice_ts, 'yyyyMM') AS string) AS year_month,
  CAST(abs(quantity) AS int) AS returned_qty,
  CAST(abs(line_amount) AS decimal(18,2)) AS returned_amount
FROM j11_lnk_cancels
""")

# COMMAND ----------
# src_dim_product: dataset dim_product
@dp.temporary_view(name="j11_lnk_dim_product")
def j11_lnk_dim_product():
    return spark.read.table("dim_product")

# COMMAND ----------
# jn_Product: Join (left outer) on stock_code
@dp.temporary_view(name="j11_lnk_p")
def j11_lnk_p():
    return spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.customer_id,
  l.country,
  l.invoice_date_key,
  l.year_month,
  l.returned_qty,
  l.returned_amount,
  r.category,
  r.product_sk
FROM j11_lnk_ret l
LEFT OUTER JOIN j11_lnk_dim_product r ON l.stock_code = r.stock_code
""")

# COMMAND ----------
# xfm_Product_Defaults: Transformer
@dp.temporary_view(name="j11_lnk_p_nn")
def j11_lnk_p_nn():
    return spark.sql(f"""
SELECT
  invoice_no,
  stock_code,
  customer_id,
  country,
  invoice_date_key,
  year_month,
  returned_qty,
  returned_amount,
  CAST(coalesce(category, 'Unknown') AS string) AS category,
  CAST(coalesce(product_sk, (-1)) AS int) AS product_sk
FROM j11_lnk_p
""")

# COMMAND ----------
# src_dim_customer: dataset dim_customer
@dp.temporary_view(name="j11_lnk_dim_customer")
def j11_lnk_dim_customer():
    return spark.read.table("dim_customer")

# COMMAND ----------
# flt_Current_Customer: Filter
@dp.temporary_view(name="j11_lnk_cur")
def j11_lnk_cur():
    return spark.sql(f"""
SELECT * FROM j11_lnk_dim_customer
WHERE (is_current = 1)
""")

# COMMAND ----------
# jn_Customer: Join (left outer) on customer_id
@dp.temporary_view(name="j11_lnk_c")
def j11_lnk_c():
    return spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.customer_id,
  l.country,
  l.invoice_date_key,
  l.year_month,
  l.returned_qty,
  l.returned_amount,
  l.category,
  l.product_sk,
  r.customer_sk
FROM j11_lnk_p_nn l
LEFT OUTER JOIN j11_lnk_cur r ON l.customer_id = r.customer_id
""")

# COMMAND ----------
# xfm_Customer_Defaults: Transformer
@dp.temporary_view(name="j11_lnk_c_nn")
def j11_lnk_c_nn():
    return spark.sql(f"""
SELECT
  invoice_no,
  stock_code,
  customer_id,
  country,
  invoice_date_key,
  year_month,
  returned_qty,
  returned_amount,
  category,
  product_sk,
  CAST(coalesce(customer_sk, (-1)) AS int) AS customer_sk
FROM j11_lnk_c
""")

# COMMAND ----------
# srt_Fact_Returns: Sort
@dp.temporary_view(name="j11_lnk_sorted")
def j11_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j11_lnk_c_nn
ORDER BY invoice_no ASC, stock_code ASC
""")

# COMMAND ----------
# cpy_Returns: Copy
@dp.temporary_view(name="j11_lnk_out")
def j11_lnk_out():
    return spark.sql(f"""
SELECT * FROM j11_lnk_sorted
""")

# COMMAND ----------
# cpy_Returns: Copy
@dp.temporary_view(name="j11_lnk_audit_in")
def j11_lnk_audit_in():
    return spark.sql(f"""
SELECT * FROM j11_lnk_sorted
""")

# COMMAND ----------
# tgt_fact_returns: target file -> materialized view fact_returns
@dp.materialized_view(name="fact_returns")
def fact_returns():
    return spark.read.table("j11_lnk_out").select('invoice_no', 'stock_code', 'customer_id', 'country', 'invoice_date_key', 'year_month', 'returned_qty', 'returned_amount', 'category', 'product_sk', 'customer_sk')

# COMMAND ----------
# agg_Returns_By_Category: Aggregator
@dp.temporary_view(name="j11_lnk_by_cat")
def j11_lnk_by_cat():
    return spark.sql(f"""
SELECT category, CAST(SUM(returned_amount) AS decimal(18,2)) AS returned_amount
FROM j11_lnk_audit_in
GROUP BY category
""")

# COMMAND ----------
# tgt_audit_returns_by_category: target file -> materialized view audit_returns_by_category
@dp.materialized_view(name="audit_returns_by_category")
def audit_returns_by_category():
    return spark.read.table("j11_lnk_by_cat").select('category', 'returned_amount')
