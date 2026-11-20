# Databricks notebook source
# Code converted on 2026-11-19 16:23:37
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
# Component lnk_fact, Type SOURCE Original node name src_fact_sales, link lnk_fact
lnk_fact = spark.read.table(f'{catalog}.{schema}.fact_sales')
lnk_fact.createOrReplaceTempView("lnk_fact")


# COMMAND ----------
# Component lnk_pos, Type TRANSFORMATION Original node name flt_Positive_Lines, link lnk_pos
lnk_pos = spark.sql(f"""
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
WHERE quantity > 0""")
lnk_pos.createOrReplaceTempView("lnk_pos")


# COMMAND ----------
# Component lnk_prep, Type TRANSFORMATION Original node name xfm_Pair_Prep, link lnk_prep
lnk_prep = spark.sql(f"""
SELECT
coalesce(lnk_pos.invoice_no, '') as invoice_no,
coalesce(lnk_pos.stock_code, '') as stock_code
FROM
lnk_pos""")
lnk_prep.createOrReplaceTempView("lnk_prep")


# COMMAND ----------
# Component lnk_dedup, Type UNIQUE_ROW Original node name rmd_Invoice_Product, link lnk_dedup
lnk_dedup = spark.sql(f"""
SELECT
invoice_no,
stock_code
FROM (
SELECT
coalesce(lnk_prep.invoice_no, '') as invoice_no,
coalesce(lnk_prep.stock_code, '') as stock_code,
ROW_NUMBER() OVER(PARTITION BY coalesce(lnk_prep.invoice_no, ''), coalesce(lnk_prep.stock_code, '') ORDER BY 1) dedup_rnk
FROM lnk_prep) dedup_lnk_dedup WHERE dedup_rnk = 1""")
lnk_dedup.createOrReplaceTempView("lnk_dedup")


# COMMAND ----------
# Component lnk_a, Type REPLICATE Original node name cpy_Basket, link lnk_a
lnk_a = spark.sql(f"""
SELECT
coalesce(lnk_dedup.invoice_no, '') as invoice_no,
coalesce(lnk_dedup.stock_code, '') as stock_code
FROM
lnk_dedup""")
lnk_a.createOrReplaceTempView("lnk_a")


# COMMAND ----------
# Component lnk_b_in, Type REPLICATE Original node name cpy_Basket, link lnk_b_in
lnk_b_in = spark.sql(f"""
SELECT
coalesce(lnk_dedup.invoice_no, '') as invoice_no,
coalesce(lnk_dedup.stock_code, '') as stock_code
FROM
lnk_dedup""")
lnk_b_in.createOrReplaceTempView("lnk_b_in")


# COMMAND ----------
# Component lnk_b, Type TRANSFORMATION Original node name xfm_Rename_B, link lnk_b
lnk_b = spark.sql(f"""
SELECT
coalesce(lnk_b_in.invoice_no, '') as invoice_no,
coalesce(lnk_b_in.stock_code, '') as stock_code_b
FROM
lnk_b_in""")
lnk_b.createOrReplaceTempView("lnk_b")


# COMMAND ----------
# Component lnk_pairs_all, Type JOINER Original node name jn_Self, link lnk_pairs_all
lnk_pairs_all = spark.sql(f"""
SELECT
coalesce(lnk_a.invoice_no, '') as invoice_no,
coalesce(lnk_a.stock_code, '') as stock_code,
coalesce(lnk_b.stock_code_b, '') as stock_code_b
FROM
lnk_a
INNER JOIN lnk_b ON coalesce(lnk_a.invoice_no, '') = coalesce(lnk_b.invoice_no, '')""")
lnk_pairs_all.createOrReplaceTempView("lnk_pairs_all")


# COMMAND ----------
# Component lnk_pairs, Type TRANSFORMATION Original node name flt_Ordered_Pairs, link lnk_pairs
lnk_pairs = spark.sql(f"""
SELECT
coalesce(lnk_pairs_all.invoice_no, '') as invoice_no,
coalesce(lnk_pairs_all.stock_code, '') as stock_code,
coalesce(lnk_pairs_all.stock_code_b, '') as stock_code_b
FROM
lnk_pairs_all
WHERE stock_code < stock_code_b""")
lnk_pairs.createOrReplaceTempView("lnk_pairs")


# COMMAND ----------
# Component lnk_counts, Type AGGREGATOR Original node name agg_Pair_Counts, link lnk_counts
lnk_counts = spark.sql(f"""
SELECT
coalesce(lnk_pairs.stock_code, '') as stock_code,
coalesce(lnk_pairs.stock_code_b, '') as stock_code_b,
COUNT(*) as pair_count
FROM
lnk_pairs
GROUP BY
coalesce(lnk_pairs.stock_code, ''),
coalesce(lnk_pairs.stock_code_b, '')""")
lnk_counts.createOrReplaceTempView("lnk_counts")


# COMMAND ----------
# Component lnk_support, Type TRANSFORMATION Original node name flt_Min_Support, link lnk_support
lnk_support = spark.sql(f"""
SELECT
coalesce(lnk_counts.stock_code, '') as stock_code,
coalesce(lnk_counts.stock_code_b, '') as stock_code_b,
CAST(lnk_counts.pair_count AS int) as pair_count
FROM
lnk_counts
WHERE pair_count >= 5""")
lnk_support.createOrReplaceTempView("lnk_support")


# COMMAND ----------
# Component lnk_sorted, Type SORTER Original node name srt_Pairs, link lnk_sorted
lnk_sorted = spark.sql(f"""
SELECT
coalesce(lnk_support.stock_code, '') as stock_code,
coalesce(lnk_support.stock_code_b, '') as stock_code_b,
CAST(lnk_support.pair_count AS int) as pair_count
FROM
lnk_support
ORDER BY pair_count desc""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")


lnk_top = spark.sql(f"""
SELECT * FROM lnk_sorted
ORDER BY pair_count DESC
LIMIT 1000
""")
lnk_top.createOrReplaceTempView("lnk_top")


# COMMAND ----------
# Component tgt_mart_basket_affinity, Type TARGET 

tgt_mart_basket_affinity = lnk_top.select( \
	coalesce(lnk_top.stock_code, lit('')).alias('stock_code'), \
	coalesce(lnk_top.stock_code_b, lit('')).alias('stock_code_b'), \
	lnk_top.pair_count.cast('int').alias('pair_count') \
)
tgt_mart_basket_affinity.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.Basket_Affinity')
