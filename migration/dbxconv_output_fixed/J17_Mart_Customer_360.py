# Databricks notebook source
# Code converted on 2026-11-19 16:23:03
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.functions import lit, to_timestamp, when, expr, col, explode
from databricks_conversion_supplements import DatabricksConversionSupplements


# COMMAND ----------

dbutils.widgets.text(name = 'pSrcDir', defaultValue = 'C:/case_study/Source_Data')
pSrcDir = dbutils.widgets.get("pSrcDir")

dbutils.widgets.text(name = 'pStgDir', defaultValue = 'C:/case_study/Staging')
pStgDir = dbutils.widgets.get("pStgDir")

dbutils.widgets.text(name = 'pDwhDir', defaultValue = 'C:/case_study/Warehouse')
pDwhDir = dbutils.widgets.get("pDwhDir")

dbutils.widgets.text(name = 'pMartDir', defaultValue = 'C:/case_study/Marts')
pMartDir = dbutils.widgets.get("pMartDir")

dbutils.widgets.text(name = 'pAuditDir', defaultValue = 'C:/case_study/Audit')
pAuditDir = dbutils.widgets.get("pAuditDir")

dbutils.widgets.text(name = 'pRunDate', defaultValue = '2011-12-31')
pRunDate = dbutils.widgets.get("pRunDate")

dbutils.widgets.text(name = 'pEffDate', defaultValue = '2011-07-01')
pEffDate = dbutils.widgets.get("pEffDate")

dbutils.widgets.text(name = 'catalog', defaultValue = 'unitygo')
catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text(name = 'schema', defaultValue = 'retail_dw_dbxconv')
schema = dbutils.widgets.get("schema")


# COMMAND ----------
# Component lnk_dim_customer, Type SOURCE Original node name src_dim_customer, link lnk_dim_customer
lnk_dim_customer = spark.read.table(f'{catalog}.{schema}.dim_customer')
lnk_dim_customer.createOrReplaceTempView("lnk_dim_customer")


# COMMAND ----------
# Component lnk_inv, Type SOURCE Original node name src_fact_invoice, link lnk_inv
lnk_inv = spark.read.table(f'{catalog}.{schema}.fact_invoice')
lnk_inv.createOrReplaceTempView("lnk_inv")


# COMMAND ----------
# Component lnk_ret, Type SOURCE Original node name src_fact_returns, link lnk_ret
lnk_ret = spark.read.table(f'{catalog}.{schema}.fact_returns')
lnk_ret.createOrReplaceTempView("lnk_ret")


# COMMAND ----------
# Component lnk_cur, Type TRANSFORMATION Original node name flt_Current_Customer, link lnk_cur
lnk_cur = spark.sql(f"""
SELECT
CAST(lnk_dim_customer.customer_sk AS int) as customer_sk,
CAST(lnk_dim_customer.customer_id AS int) as customer_id,
coalesce(lnk_dim_customer.cust_name, '') as cust_name,
coalesce(lnk_dim_customer.gender, '') as gender,
CAST(lnk_dim_customer.age AS int) as age,
CAST(lnk_dim_customer.spending_score AS int) as spending_score,
CAST(lnk_dim_customer.annual_income_k AS int) as annual_income_k,
coalesce(lnk_dim_customer.country, '') as country,
coalesce(lnk_dim_customer.customer_type, '') as customer_type,
coalesce(lnk_dim_customer.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_dim_customer.email, '') as email,
coalesce(lnk_dim_customer.city, '') as city,
CAST(lnk_dim_customer.attr_hash AS bigint) as attr_hash,
CAST(lnk_dim_customer.version_no AS int) as version_no,
CAST(lnk_dim_customer.eff_start_key AS int) as eff_start_key,
CAST(lnk_dim_customer.eff_end_key AS int) as eff_end_key,
CAST(lnk_dim_customer.is_current AS int) as is_current
FROM
lnk_dim_customer
WHERE is_current = 1""")
lnk_cur.createOrReplaceTempView("lnk_cur")


# COMMAND ----------
# Component lnk_known, Type TRANSFORMATION Original node name flt_Known_Customers, link lnk_known
lnk_known = spark.sql(f"""
SELECT
coalesce(lnk_inv.invoice_no, '') as invoice_no,
CAST(lnk_inv.customer_id AS int) as customer_id,
CAST(lnk_inv.customer_sk AS int) as customer_sk,
CAST(lnk_inv.store_sk AS int) as store_sk,
CAST(lnk_inv.employee_sk AS int) as employee_sk,
CAST(lnk_inv.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_inv.year_month, '') as year_month,
CAST(lnk_inv.invoice_sales AS decimal(18,2)) as invoice_sales,
CAST(lnk_inv.invoice_profit AS decimal(18,2)) as invoice_profit,
CAST(lnk_inv.item_qty AS bigint) as item_qty,
CAST(lnk_inv.line_total AS bigint) as line_total,
CAST(lnk_inv.avg_line_value AS decimal(18,2)) as avg_line_value,
CAST(lnk_inv.invoice_cnt AS int) as invoice_cnt
FROM
lnk_inv
WHERE customer_id > 0""")
lnk_known.createOrReplaceTempView("lnk_known")


# COMMAND ----------
# Component lnk_ret_known, Type TRANSFORMATION Original node name flt_Known_Returns, link lnk_ret_known
lnk_ret_known = spark.sql(f"""
SELECT
coalesce(lnk_ret.invoice_no, '') as invoice_no,
coalesce(lnk_ret.stock_code, '') as stock_code,
CAST(lnk_ret.customer_id AS int) as customer_id,
coalesce(lnk_ret.country, '') as country,
CAST(lnk_ret.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_ret.year_month, '') as year_month,
CAST(lnk_ret.returned_qty AS int) as returned_qty,
CAST(lnk_ret.returned_amount AS decimal(18,2)) as returned_amount,
coalesce(lnk_ret.category, '') as category,
CAST(lnk_ret.product_sk AS int) as product_sk,
CAST(lnk_ret.customer_sk AS int) as customer_sk
FROM
lnk_ret
WHERE customer_id > 0""")
lnk_ret_known.createOrReplaceTempView("lnk_ret_known")


# COMMAND ----------
# Component lnk_ret_agg, Type AGGREGATOR Original node name agg_Cust_Returns, link lnk_ret_agg
lnk_ret_agg = spark.sql(f"""
SELECT
CAST(lnk_ret_known.customer_id AS int) as customer_id,
SUM(returned_amount) as total_returned
FROM
lnk_ret_known
GROUP BY
CAST(lnk_ret_known.customer_id AS int)""")
lnk_ret_agg.createOrReplaceTempView("lnk_ret_agg")


# COMMAND ----------
# Component lnk_rev, Type AGGREGATOR Original node name agg_Cust_Revenue, link lnk_rev
lnk_rev = spark.sql(f"""
SELECT
CAST(lnk_known.customer_id AS int) as customer_id,
SUM(invoice_sales) as total_sales,
SUM(invoice_profit) as total_profit,
SUM(invoice_cnt) as invoice_total,
MIN(invoice_date_key) as first_date_key,
MAX(invoice_date_key) as last_date_key
FROM
lnk_known
GROUP BY
CAST(lnk_known.customer_id AS int)""")
lnk_rev.createOrReplaceTempView("lnk_rev")


# COMMAND ----------
# Component lnk_rr, Type JOINER Original node name jn_Revenue_Returns, link lnk_rr
lnk_rr = spark.sql(f"""
SELECT
CAST(lnk_rev.customer_id AS int) as customer_id,
CAST(lnk_rev.total_sales AS decimal(18,2)) as total_sales,
CAST(lnk_rev.total_profit AS decimal(18,2)) as total_profit,
CAST(lnk_rev.invoice_total AS bigint) as invoice_total,
CAST(lnk_rev.first_date_key AS int) as first_date_key,
CAST(lnk_rev.last_date_key AS int) as last_date_key,
CAST(lnk_ret_agg.total_returned AS decimal(18,2)) as total_returned
FROM
lnk_rev
LEFT JOIN lnk_ret_agg ON CAST(lnk_rev.customer_id AS int) = CAST(lnk_ret_agg.customer_id AS int)""")
lnk_rr.createOrReplaceTempView("lnk_rr")


# COMMAND ----------
# Component lnk_rr_nn, Type TRANSFORMATION Original node name xfm_Returns_Defaults, link lnk_rr_nn
lnk_rr_nn = spark.sql(f"""
SELECT
CAST(lnk_rr.customer_id AS int) as customer_id,
CAST(lnk_rr.total_sales AS decimal(18,2)) as total_sales,
CAST(lnk_rr.total_profit AS decimal(18,2)) as total_profit,
CAST(lnk_rr.invoice_total AS bigint) as invoice_total,
CAST(lnk_rr.first_date_key AS int) as first_date_key,
CAST(lnk_rr.last_date_key AS int) as last_date_key,
 COALESCE(CAST(lnk_rr.total_returned AS decimal(18,2)), 0) as total_returned
FROM
lnk_rr""")
lnk_rr_nn.createOrReplaceTempView("lnk_rr_nn")


# COMMAND ----------
# Component lnk_attrs, Type JOINER Original node name jn_Customer_Attrs, link lnk_attrs
lnk_attrs = spark.sql(f"""
SELECT
CAST(lnk_rr_nn.customer_id AS int) as customer_id,
CAST(lnk_rr_nn.total_sales AS decimal(18,2)) as total_sales,
CAST(lnk_rr_nn.total_profit AS decimal(18,2)) as total_profit,
CAST(lnk_rr_nn.invoice_total AS bigint) as invoice_total,
CAST(lnk_rr_nn.first_date_key AS int) as first_date_key,
CAST(lnk_rr_nn.last_date_key AS int) as last_date_key,
CAST(lnk_rr_nn.total_returned AS decimal(18,2)) as total_returned,
CAST(lnk_cur.customer_sk AS int) as customer_sk,
coalesce(lnk_cur.cust_name, '') as cust_name,
CAST(lnk_cur.age AS int) as age,
coalesce(lnk_cur.country, '') as country,
coalesce(lnk_cur.customer_type, '') as customer_type,
coalesce(lnk_cur.loyalty_tier, '') as loyalty_tier
FROM
lnk_rr_nn
INNER JOIN lnk_cur ON CAST(lnk_rr_nn.customer_id AS int) = CAST(lnk_cur.customer_id AS int)""")
lnk_attrs.createOrReplaceTempView("lnk_attrs")


# COMMAND ----------
# Component lnk_rfm, Type TRANSFORMATION Original node name xfm_RFM, link lnk_rfm
lnk_rfm = spark.sql(f"""
SELECT
CAST(lnk_attrs.customer_id AS int) as customer_id,
CAST(lnk_attrs.total_sales AS decimal(18,2)) as total_sales,
CAST(lnk_attrs.total_profit AS decimal(18,2)) as total_profit,
CAST(lnk_attrs.invoice_total AS bigint) as invoice_total,
CAST(lnk_attrs.first_date_key AS int) as first_date_key,
CAST(lnk_attrs.last_date_key AS int) as last_date_key,
CAST(lnk_attrs.total_returned AS decimal(18,2)) as total_returned,
CAST(lnk_attrs.customer_sk AS int) as customer_sk,
coalesce(lnk_attrs.cust_name, '') as cust_name,
CAST(lnk_attrs.age AS int) as age,
coalesce(lnk_attrs.country, '') as country,
coalesce(lnk_attrs.customer_type, '') as customer_type,
coalesce(lnk_attrs.loyalty_tier, '') as loyalty_tier,
DATEDIFF(cast('{pRunDate}' as date), to_date(cast(lnk_attrs.last_date_key as string), 'yyyyMMdd')) as recency_days,
IF ( DATEDIFF(cast('{pRunDate}' as date), to_date(cast(lnk_attrs.last_date_key as string), 'yyyyMMdd')) <= 30 , 5 , IF ( DATEDIFF(cast('{pRunDate}' as date), to_date(cast(lnk_attrs.last_date_key as string), 'yyyyMMdd')) <= 90 , 4 , IF ( DATEDIFF(cast('{pRunDate}' as date), to_date(cast(lnk_attrs.last_date_key as string), 'yyyyMMdd')) <= 180 , 3 , IF ( DATEDIFF(cast('{pRunDate}' as date), to_date(cast(lnk_attrs.last_date_key as string), 'yyyyMMdd')) <= 270 , 2 , 1 ) ) ) ) as r_score,
IF ( CAST(lnk_attrs.invoice_total AS bigint) >= 10 , 5 , IF ( CAST(lnk_attrs.invoice_total AS bigint) >= 6 , 4 , IF ( CAST(lnk_attrs.invoice_total AS bigint) >= 3 , 3 , IF ( CAST(lnk_attrs.invoice_total AS bigint) >= 2 , 2 , 1 ) ) ) ) as f_score,
IF ( CAST(lnk_attrs.total_sales AS decimal(18,2)) >= 5000 , 5 , IF ( CAST(lnk_attrs.total_sales AS decimal(18,2)) >= 2000 , 4 , IF ( CAST(lnk_attrs.total_sales AS decimal(18,2)) >= 800 , 3 , IF ( CAST(lnk_attrs.total_sales AS decimal(18,2)) >= 300 , 2 , 1 ) ) ) ) as m_score,
CAST(lnk_attrs.total_sales AS decimal(18,2)) - CAST(lnk_attrs.total_returned AS decimal(18,2)) as net_sales
FROM
lnk_attrs""")
lnk_rfm.createOrReplaceTempView("lnk_rfm")


# COMMAND ----------
# Component lnk_seg, Type TRANSFORMATION Original node name xfm_Segment, link lnk_seg
lnk_seg = spark.sql(f"""
SELECT
CAST(lnk_rfm.customer_id AS int) as customer_id,
CAST(lnk_rfm.total_sales AS decimal(18,2)) as total_sales,
CAST(lnk_rfm.total_profit AS decimal(18,2)) as total_profit,
CAST(lnk_rfm.invoice_total AS bigint) as invoice_total,
CAST(lnk_rfm.first_date_key AS int) as first_date_key,
CAST(lnk_rfm.last_date_key AS int) as last_date_key,
CAST(lnk_rfm.total_returned AS decimal(18,2)) as total_returned,
CAST(lnk_rfm.customer_sk AS int) as customer_sk,
coalesce(lnk_rfm.cust_name, '') as cust_name,
CAST(lnk_rfm.age AS int) as age,
coalesce(lnk_rfm.country, '') as country,
coalesce(lnk_rfm.customer_type, '') as customer_type,
coalesce(lnk_rfm.loyalty_tier, '') as loyalty_tier,
CAST(lnk_rfm.recency_days AS int) as recency_days,
CAST(lnk_rfm.r_score AS int) as r_score,
CAST(lnk_rfm.f_score AS int) as f_score,
CAST(lnk_rfm.m_score AS int) as m_score,
CAST(lnk_rfm.net_sales AS decimal(18,2)) as net_sales,
IF ( CAST(lnk_rfm.r_score AS int) >= 4 And CAST(lnk_rfm.f_score AS int) >= 4 , 'CHAMPION' , IF ( CAST(lnk_rfm.r_score AS int) >= 3 And CAST(lnk_rfm.f_score AS int) >= 3 , 'LOYAL' , IF ( CAST(lnk_rfm.r_score AS int) >= 4 , 'RECENT' , IF ( CAST(lnk_rfm.r_score AS int) <= 2 And CAST(lnk_rfm.f_score AS int) >= 3 , 'AT_RISK' , 'HIBERNATING' ) ) ) ) as segment
FROM
lnk_rfm""")
lnk_seg.createOrReplaceTempView("lnk_seg")


# COMMAND ----------
# Component lnk_sorted, Type SORTER Original node name srt_Customer_360, link lnk_sorted
lnk_sorted = spark.sql(f"""
SELECT
CAST(lnk_seg.customer_id AS int) as customer_id,
CAST(lnk_seg.total_sales AS decimal(18,2)) as total_sales,
CAST(lnk_seg.total_profit AS decimal(18,2)) as total_profit,
CAST(lnk_seg.invoice_total AS bigint) as invoice_total,
CAST(lnk_seg.first_date_key AS int) as first_date_key,
CAST(lnk_seg.last_date_key AS int) as last_date_key,
CAST(lnk_seg.total_returned AS decimal(18,2)) as total_returned,
CAST(lnk_seg.customer_sk AS int) as customer_sk,
coalesce(lnk_seg.cust_name, '') as cust_name,
CAST(lnk_seg.age AS int) as age,
coalesce(lnk_seg.country, '') as country,
coalesce(lnk_seg.customer_type, '') as customer_type,
coalesce(lnk_seg.loyalty_tier, '') as loyalty_tier,
CAST(lnk_seg.recency_days AS int) as recency_days,
CAST(lnk_seg.r_score AS int) as r_score,
CAST(lnk_seg.f_score AS int) as f_score,
CAST(lnk_seg.m_score AS int) as m_score,
CAST(lnk_seg.net_sales AS decimal(18,2)) as net_sales,
coalesce(lnk_seg.segment, '') as segment
FROM
lnk_seg
ORDER BY net_sales desc""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")


# COMMAND ----------
# Component lnk_seg_in, Type REPLICATE Original node name cpy_360, link lnk_seg_in
lnk_seg_in = spark.sql(f"""
SELECT
CAST(lnk_sorted.customer_id AS int) as customer_id,
CAST(lnk_sorted.total_sales AS decimal(18,2)) as total_sales,
CAST(lnk_sorted.total_profit AS decimal(18,2)) as total_profit,
CAST(lnk_sorted.invoice_total AS bigint) as invoice_total,
CAST(lnk_sorted.first_date_key AS int) as first_date_key,
CAST(lnk_sorted.last_date_key AS int) as last_date_key,
CAST(lnk_sorted.total_returned AS decimal(18,2)) as total_returned,
CAST(lnk_sorted.customer_sk AS int) as customer_sk,
coalesce(lnk_sorted.cust_name, '') as cust_name,
CAST(lnk_sorted.age AS int) as age,
coalesce(lnk_sorted.country, '') as country,
coalesce(lnk_sorted.customer_type, '') as customer_type,
coalesce(lnk_sorted.loyalty_tier, '') as loyalty_tier,
CAST(lnk_sorted.recency_days AS int) as recency_days,
CAST(lnk_sorted.r_score AS int) as r_score,
CAST(lnk_sorted.f_score AS int) as f_score,
CAST(lnk_sorted.m_score AS int) as m_score,
CAST(lnk_sorted.net_sales AS decimal(18,2)) as net_sales,
coalesce(lnk_sorted.segment, '') as segment
FROM
lnk_sorted""")
lnk_seg_in.createOrReplaceTempView("lnk_seg_in")


# COMMAND ----------
# Component lnk_out, Type REPLICATE Original node name cpy_360, link lnk_out
lnk_out = spark.sql(f"""
SELECT
CAST(lnk_sorted.customer_id AS int) as customer_id,
CAST(lnk_sorted.total_sales AS decimal(18,2)) as total_sales,
CAST(lnk_sorted.total_profit AS decimal(18,2)) as total_profit,
CAST(lnk_sorted.invoice_total AS bigint) as invoice_total,
CAST(lnk_sorted.first_date_key AS int) as first_date_key,
CAST(lnk_sorted.last_date_key AS int) as last_date_key,
CAST(lnk_sorted.total_returned AS decimal(18,2)) as total_returned,
CAST(lnk_sorted.customer_sk AS int) as customer_sk,
coalesce(lnk_sorted.cust_name, '') as cust_name,
CAST(lnk_sorted.age AS int) as age,
coalesce(lnk_sorted.country, '') as country,
coalesce(lnk_sorted.customer_type, '') as customer_type,
coalesce(lnk_sorted.loyalty_tier, '') as loyalty_tier,
CAST(lnk_sorted.recency_days AS int) as recency_days,
CAST(lnk_sorted.r_score AS int) as r_score,
CAST(lnk_sorted.f_score AS int) as f_score,
CAST(lnk_sorted.m_score AS int) as m_score,
CAST(lnk_sorted.net_sales AS decimal(18,2)) as net_sales,
coalesce(lnk_sorted.segment, '') as segment
FROM
lnk_sorted""")
lnk_out.createOrReplaceTempView("lnk_out")


# COMMAND ----------
# Component tgt_mart_customer_360, Type TARGET 

tgt_mart_customer_360 = lnk_out.select( \
	lnk_out.customer_id.cast('int').alias('customer_id'), \
	lnk_out.total_sales.cast('decimal(18,2)').alias('total_sales'), \
	lnk_out.total_profit.cast('decimal(18,2)').alias('total_profit'), \
	lnk_out.invoice_total.cast('bigint').alias('invoice_total'), \
	lnk_out.first_date_key.cast('int').alias('first_date_key'), \
	lnk_out.last_date_key.cast('int').alias('last_date_key'), \
	lnk_out.total_returned.cast('decimal(18,2)').alias('total_returned'), \
	lnk_out.customer_sk.cast('int').alias('customer_sk'), \
	coalesce(lnk_out.cust_name, lit('')).alias('cust_name'), \
	lnk_out.age.cast('int').alias('age'), \
	coalesce(lnk_out.country, lit('')).alias('country'), \
	coalesce(lnk_out.customer_type, lit('')).alias('customer_type'), \
	coalesce(lnk_out.loyalty_tier, lit('')).alias('loyalty_tier'), \
	lnk_out.recency_days.cast('int').alias('recency_days'), \
	lnk_out.r_score.cast('int').alias('r_score'), \
	lnk_out.f_score.cast('int').alias('f_score'), \
	lnk_out.m_score.cast('int').alias('m_score'), \
	lnk_out.net_sales.cast('decimal(18,2)').alias('net_sales'), \
	coalesce(lnk_out.segment, lit('')).alias('segment') \
)
tgt_mart_customer_360.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.Customer_360')


# COMMAND ----------
# Component lnk_seg_counts, Type AGGREGATOR Original node name agg_Segments, link lnk_seg_counts
lnk_seg_counts = spark.sql(f"""
SELECT
coalesce(lnk_seg_in.segment, '') as segment,
COUNT(*) as customers
FROM
lnk_seg_in
GROUP BY
coalesce(lnk_seg_in.segment, '')""")
lnk_seg_counts.createOrReplaceTempView("lnk_seg_counts")


# COMMAND ----------
# Component tgt_segment_summary, Type TARGET 

tgt_segment_summary = lnk_seg_counts.select( \
	coalesce(lnk_seg_counts.segment, lit('')).alias('segment'), \
	lnk_seg_counts.customers.cast('int').alias('customers') \
)
tgt_segment_summary.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.Customer_Segments')
