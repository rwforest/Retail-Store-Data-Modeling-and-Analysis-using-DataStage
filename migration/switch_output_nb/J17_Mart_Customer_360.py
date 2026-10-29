# Databricks notebook source
# MAGIC %md
# MAGIC # J17_Mart_Customer_360
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028071346-rdb5/J17_Mart_Customer_360.txt`
# COMMAND ----------
# COMMAND ----------
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "retail_dw")
dbutils.widgets.text("src_dir", "")
dbutils.widgets.text("pRunDate", "2011-12-31")
dbutils.widgets.text("pEffDate", "2011-07-01")
catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))
pRunDate, pEffDate = dbutils.widgets.get("pRunDate"), dbutils.widgets.get("pEffDate")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------
# src_fact_invoice: PxSequentialFile (Delta table fact_invoice)
lnk_inv = spark.table(f"{catalog}.{schema}.fact_invoice")
lnk_inv.createOrReplaceTempView("lnk_inv")

# COMMAND ----------
# flt_Known_Customers: PxFilter
lnk_known = spark.sql(f"""
SELECT * FROM lnk_inv
WHERE customer_id > 0
""")
lnk_known.createOrReplaceTempView("lnk_known")

# COMMAND ----------
# agg_Cust_Revenue: PxAggregator
lnk_rev = spark.sql(f"""
SELECT
  customer_id,
  CAST(SUM(invoice_sales) AS decimal(18,2)) AS total_sales,
  CAST(SUM(invoice_profit) AS decimal(18,2)) AS total_profit,
  CAST(SUM(invoice_cnt) AS bigint) AS invoice_total,
  CAST(MIN(invoice_date_key) AS int) AS first_date_key,
  CAST(MAX(invoice_date_key) AS int) AS last_date_key
FROM lnk_known
GROUP BY customer_id
""")
lnk_rev.createOrReplaceTempView("lnk_rev")

# COMMAND ----------
# src_fact_returns: PxSequentialFile (Delta table fact_returns)
lnk_ret = spark.table(f"{catalog}.{schema}.fact_returns")
lnk_ret.createOrReplaceTempView("lnk_ret")

# COMMAND ----------
# flt_Known_Returns: PxFilter
lnk_ret_known = spark.sql(f"""
SELECT * FROM lnk_ret
WHERE customer_id > 0
""")
lnk_ret_known.createOrReplaceTempView("lnk_ret_known")

# COMMAND ----------
# agg_Cust_Returns: PxAggregator
lnk_ret_agg = spark.sql(f"""
SELECT
  customer_id,
  CAST(SUM(returned_amount) AS decimal(18,2)) AS total_returned
FROM lnk_ret_known
GROUP BY customer_id
""")
lnk_ret_agg.createOrReplaceTempView("lnk_ret_agg")

# COMMAND ----------
# jn_Revenue_Returns: PxJoin (left outer) on customer_id
lnk_rr = spark.sql(f"""
SELECT
  l.customer_id,
  l.total_sales,
  l.total_profit,
  l.invoice_total,
  l.first_date_key,
  l.last_date_key,
  r.total_returned
FROM lnk_rev l
LEFT OUTER JOIN lnk_ret_agg r ON l.customer_id = r.customer_id
""")
lnk_rr.createOrReplaceTempView("lnk_rr")

# COMMAND ----------
# xfm_Returns_Defaults: CTransformerStage
lnk_rr_nn = spark.sql(f"""
SELECT
  customer_id,
  total_sales,
  total_profit,
  invoice_total,
  first_date_key,
  last_date_key,
  CAST(coalesce(total_returned, 0) AS decimal(18,2)) AS total_returned
FROM lnk_rr
""")
lnk_rr_nn.createOrReplaceTempView("lnk_rr_nn")

# COMMAND ----------
# src_dim_customer: PxSequentialFile (Delta table dim_customer)
lnk_dim_customer = spark.table(f"{catalog}.{schema}.dim_customer")
lnk_dim_customer.createOrReplaceTempView("lnk_dim_customer")

# COMMAND ----------
# flt_Current_Customer: PxFilter
lnk_cur = spark.sql(f"""
SELECT * FROM lnk_dim_customer
WHERE is_current = 1
""")
lnk_cur.createOrReplaceTempView("lnk_cur")

# COMMAND ----------
# jn_Customer_Attrs: PxJoin (inner) on customer_id
lnk_attrs = spark.sql(f"""
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
FROM lnk_rr_nn l
INNER JOIN lnk_cur r ON l.customer_id = r.customer_id
""")
lnk_attrs.createOrReplaceTempView("lnk_attrs")

# COMMAND ----------
# xfm_RFM: CTransformerStage
lnk_rfm = spark.sql(f"""
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
FROM lnk_attrs
""")
lnk_rfm.createOrReplaceTempView("lnk_rfm")

# COMMAND ----------
# xfm_Segment: CTransformerStage
lnk_seg = spark.sql(f"""
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
  CAST(CASE WHEN (r_score >= 4 AND f_score >= 4) THEN 'CHAMPION'
            ELSE CASE WHEN (r_score >= 3 AND f_score >= 3) THEN 'LOYAL'
            ELSE CASE WHEN r_score >= 4 THEN 'RECENT'
            ELSE CASE WHEN (r_score <= 2 AND f_score >= 3) THEN 'AT_RISK'
            ELSE 'HIBERNATING' END END END END AS string) AS segment
FROM lnk_rfm
""")
lnk_seg.createOrReplaceTempView("lnk_seg")

# COMMAND ----------
# srt_Customer_360: PxSort
lnk_sorted = spark.sql(f"""
SELECT * FROM lnk_seg
ORDER BY net_sales DESC
""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")

# COMMAND ----------
# cpy_360: PxCopy (output link lnk_out)
lnk_out = spark.sql(f"""
SELECT * FROM lnk_sorted
""")
lnk_out.createOrReplaceTempView("lnk_out")

# COMMAND ----------
# cpy_360: PxCopy (output link lnk_seg_in)
lnk_seg_in = spark.sql(f"""
SELECT * FROM lnk_sorted
""")
lnk_seg_in.createOrReplaceTempView("lnk_seg_in")

# COMMAND ----------
# tgt_mart_customer_360: write Delta table Customer_360
(lnk_out.select('customer_id', 'total_sales', 'total_profit', 'invoice_total', 'first_date_key', 'last_date_key', 'total_returned', 'customer_sk', 'cust_name', 'age', 'country', 'customer_type', 'loyalty_tier', 'recency_days', 'r_score', 'f_score', 'm_score', 'net_sales', 'segment').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Customer_360"))
print("Customer_360:", spark.table(f"{catalog}.{schema}.Customer_360").count(), "rows")

# COMMAND ----------
# agg_Segments: PxAggregator
lnk_seg_counts = spark.sql(f"""
SELECT segment, CAST(COUNT(*) AS int) AS customers
FROM lnk_seg_in
GROUP BY segment
""")
lnk_seg_counts.createOrReplaceTempView("lnk_seg_counts")

# COMMAND ----------
# tgt_segment_summary: write Delta table Customer_Segments
(lnk_seg_counts.select('segment', 'customers').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Customer_Segments"))
print("Customer_Segments:", spark.table(f"{catalog}.{schema}.Customer_Segments").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
