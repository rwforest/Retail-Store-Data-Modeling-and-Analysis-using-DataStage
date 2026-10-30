# Databricks notebook source
# MAGIC %md
# MAGIC # J17_Mart_Customer_360
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261030063735-t49e/J17_Mart_Customer_360.txt`
# COMMAND ----------
# COMMAND ----------
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir", "")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# src_fact_invoice: source file fact_invoice.csv written by another job -> read as dataset
@dp.temporary_view(name="j17_lnk_inv")
def j17_lnk_inv():
    return spark.read.table("fact_invoice")

# flt_Known_Customers: Filter
@dp.temporary_view(name="j17_lnk_known")
def j17_lnk_known():
    return spark.sql(f"""
SELECT * FROM j17_lnk_inv
WHERE customer_id > 0
""")

# agg_Cust_Revenue: Aggregator
@dp.temporary_view(name="j17_lnk_rev")
def j17_lnk_rev():
    return spark.sql(f"""
SELECT
  customer_id,
  CAST(SUM(invoice_sales) AS decimal(18,2)) AS total_sales,
  CAST(SUM(invoice_profit) AS decimal(18,2)) AS total_profit,
  CAST(SUM(invoice_cnt) AS bigint) AS invoice_total,
  CAST(MIN(invoice_date_key) AS int) AS first_date_key,
  CAST(MAX(invoice_date_key) AS int) AS last_date_key
FROM j17_lnk_known
GROUP BY customer_id
""")

# src_fact_returns: source file fact_returns.csv written by another job -> read as dataset
@dp.temporary_view(name="j17_lnk_ret")
def j17_lnk_ret():
    return spark.read.table("fact_returns")

# flt_Known_Returns: Filter
@dp.temporary_view(name="j17_lnk_ret_known")
def j17_lnk_ret_known():
    return spark.sql(f"""
SELECT * FROM j17_lnk_ret
WHERE customer_id > 0
""")

# agg_Cust_Returns: Aggregator
@dp.temporary_view(name="j17_lnk_ret_agg")
def j17_lnk_ret_agg():
    return spark.sql(f"""
SELECT
  customer_id,
  CAST(SUM(returned_amount) AS decimal(18,2)) AS total_returned
FROM j17_lnk_ret_known
GROUP BY customer_id
""")

# jn_Revenue_Returns: Join (left outer) on customer_id
@dp.temporary_view(name="j17_lnk_rr")
def j17_lnk_rr():
    return spark.sql(f"""
SELECT
  l.customer_id,
  l.total_sales,
  l.total_profit,
  l.invoice_total,
  l.first_date_key,
  l.last_date_key,
  r.total_returned
FROM j17_lnk_rev l
LEFT OUTER JOIN j17_lnk_ret_agg r ON l.customer_id = r.customer_id
""")

# xfm_Returns_Defaults: Transformer
@dp.temporary_view(name="j17_lnk_rr_nn")
def j17_lnk_rr_nn():
    return spark.sql(f"""
SELECT
  customer_id,
  total_sales,
  total_profit,
  invoice_total,
  first_date_key,
  last_date_key,
  CAST(coalesce(total_returned, 0) AS decimal(18,2)) AS total_returned
FROM j17_lnk_rr
""")

# src_dim_customer: source file dim_customer.csv written by another job -> read as dataset
@dp.temporary_view(name="j17_lnk_dim_customer")
def j17_lnk_dim_customer():
    return spark.read.table("dim_customer")

# flt_Current_Customer: Filter
@dp.temporary_view(name="j17_lnk_cur")
def j17_lnk_cur():
    return spark.sql(f"""
SELECT * FROM j17_lnk_dim_customer
WHERE is_current = 1
""")

# jn_Customer_Attrs: Join (inner) on customer_id
@dp.temporary_view(name="j17_lnk_attrs")
def j17_lnk_attrs():
    return spark.sql(f"""
SELECT
  l.customer_id,
  l.total_sales,
  l.total_profit,
  l.invoice_total,
  l.first_date_key,
  l.last_date_key,
  l.total_returned,
  r.customer_sk,
  r.cust_name,
  r.age,
  r.country,
  r.customer_type,
  r.loyalty_tier
FROM j17_lnk_rr_nn l
INNER JOIN j17_lnk_cur r ON l.customer_id = r.customer_id
""")

# xfm_RFM: Transformer
@dp.temporary_view(name="j17_lnk_rfm")
def j17_lnk_rfm():
    return spark.sql(f"""
SELECT
  customer_id,
  total_sales,
  total_profit,
  invoice_total,
  first_date_key,
  last_date_key,
  total_returned,
  customer_sk,
  cust_name,
  age,
  country,
  customer_type,
  loyalty_tier,
  CAST(datediff(to_date('{pRunDate}', 'yyyy-MM-dd'), to_date(CAST(last_date_key AS string), 'yyyyMMdd')) AS int) AS recency_days,
  CAST(CASE WHEN datediff(to_date('{pRunDate}', 'yyyy-MM-dd'), to_date(CAST(last_date_key AS string), 'yyyyMMdd')) <= 30 THEN 5
            ELSE CASE WHEN datediff(to_date('{pRunDate}', 'yyyy-MM-dd'), to_date(CAST(last_date_key AS string), 'yyyyMMdd')) <= 90 THEN 4
            ELSE CASE WHEN datediff(to_date('{pRunDate}', 'yyyy-MM-dd'), to_date(CAST(last_date_key AS string), 'yyyyMMdd')) <= 180 THEN 3
            ELSE CASE WHEN datediff(to_date('{pRunDate}', 'yyyy-MM-dd'), to_date(CAST(last_date_key AS string), 'yyyyMMdd')) <= 270 THEN 2
            ELSE 1 END END END END AS int) AS r_score,
  CAST(CASE WHEN invoice_total >= 10 THEN 5
            ELSE CASE WHEN invoice_total >= 6 THEN 4
            ELSE CASE WHEN invoice_total >= 3 THEN 3
            ELSE CASE WHEN invoice_total >= 2 THEN 2
            ELSE 1 END END END END AS int) AS f_score,
  CAST(CASE WHEN total_sales >= 5000 THEN 5
            ELSE CASE WHEN total_sales >= 2000 THEN 4
            ELSE CASE WHEN total_sales >= 800 THEN 3
            ELSE CASE WHEN total_sales >= 300 THEN 2
            ELSE 1 END END END END AS int) AS m_score,
  CAST((total_sales - total_returned) AS decimal(18,2)) AS net_sales
FROM j17_lnk_attrs
""")

# xfm_Segment: Transformer
@dp.temporary_view(name="j17_lnk_seg")
def j17_lnk_seg():
    return spark.sql(f"""
SELECT
  customer_id,
  total_sales,
  total_profit,
  invoice_total,
  first_date_key,
  last_date_key,
  total_returned,
  customer_sk,
  cust_name,
  age,
  country,
  customer_type,
  loyalty_tier,
  recency_days,
  r_score,
  f_score,
  m_score,
  net_sales,
  CAST(CASE WHEN r_score >= 4 AND f_score >= 4 THEN 'CHAMPION'
            ELSE CASE WHEN r_score >= 3 AND f_score >= 3 THEN 'LOYAL'
            ELSE CASE WHEN r_score >= 4 THEN 'RECENT'
            ELSE CASE WHEN r_score <= 2 AND f_score >= 3 THEN 'AT_RISK'
            ELSE 'HIBERNATING' END END END END AS string) AS segment
FROM j17_lnk_rfm
""")

# srt_Customer_360: Sort
@dp.temporary_view(name="j17_lnk_sorted")
def j17_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j17_lnk_seg
ORDER BY net_sales DESC
""")

# cpy_360: Copy
@dp.temporary_view(name="j17_lnk_out")
def j17_lnk_out():
    return spark.sql(f"""
SELECT * FROM j17_lnk_sorted
""")

# cpy_360: Copy
@dp.temporary_view(name="j17_lnk_seg_in")
def j17_lnk_seg_in():
    return spark.sql(f"""
SELECT * FROM j17_lnk_sorted
""")

# tgt_mart_customer_360: target file -> materialized view Customer_360
@dp.materialized_view(name="Customer_360")
def Customer_360():
    return spark.read.table("j17_lnk_out").select(
        'customer_id', 'total_sales', 'total_profit', 'invoice_total', 'first_date_key',
        'last_date_key', 'total_returned', 'customer_sk', 'cust_name', 'age', 'country',
        'customer_type', 'loyalty_tier', 'recency_days', 'r_score', 'f_score', 'm_score',
        'net_sales', 'segment')

# agg_Segments: Aggregator
@dp.temporary_view(name="j17_lnk_seg_counts")
def j17_lnk_seg_counts():
    return spark.sql(f"""
SELECT segment, CAST(COUNT(*) AS int) AS customers
FROM j17_lnk_seg_in
GROUP BY segment
""")

# tgt_segment_summary: target file -> materialized view Customer_Segments
@dp.materialized_view(name="Customer_Segments")
def Customer_Segments():
    return spark.read.table("j17_lnk_seg_counts").select('segment', 'customers')
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
