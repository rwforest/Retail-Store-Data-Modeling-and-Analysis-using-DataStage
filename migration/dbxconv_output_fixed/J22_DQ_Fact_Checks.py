# Databricks notebook source
# Code converted on 2026-11-19 16:24:34
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
# Component lnk_chk1, Type REPLICATE Original node name cpy_Fact, link lnk_chk1
lnk_chk1 = spark.sql(f"""
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
lnk_fact""")
lnk_chk1.createOrReplaceTempView("lnk_chk1")


# COMMAND ----------
# Component lnk_chk4, Type REPLICATE Original node name cpy_Fact, link lnk_chk4
lnk_chk4 = spark.sql(f"""
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
lnk_fact""")
lnk_chk4.createOrReplaceTempView("lnk_chk4")


# COMMAND ----------
# Component lnk_chk3, Type REPLICATE Original node name cpy_Fact, link lnk_chk3
lnk_chk3 = spark.sql(f"""
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
lnk_fact""")
lnk_chk3.createOrReplaceTempView("lnk_chk3")


# COMMAND ----------
# Component lnk_chk2, Type REPLICATE Original node name cpy_Fact, link lnk_chk2
lnk_chk2 = spark.sql(f"""
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
lnk_fact""")
lnk_chk2.createOrReplaceTempView("lnk_chk2")


# COMMAND ----------
# Component lnk_hit1, Type TRANSFORMATION Original node name flt_guest_customer_lines, link lnk_hit1
lnk_hit1 = spark.sql(f"""
SELECT
coalesce(lnk_chk1.invoice_no, '') as invoice_no,
coalesce(lnk_chk1.stock_code, '') as stock_code,
CAST(lnk_chk1.quantity AS int) as quantity,
lnk_chk1.invoice_ts as invoice_ts,
CAST(lnk_chk1.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_chk1.customer_id AS int) as customer_id,
coalesce(lnk_chk1.country, '') as country,
coalesce(lnk_chk1.store_id, '') as store_id,
CAST(lnk_chk1.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_chk1.year_month, '') as year_month,
CAST(lnk_chk1.customer_sk AS int) as customer_sk,
coalesce(lnk_chk1.customer_type, '') as customer_type,
coalesce(lnk_chk1.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_chk1.category, '') as category,
CAST(lnk_chk1.product_sk AS int) as product_sk,
coalesce(lnk_chk1.region, '') as region,
CAST(lnk_chk1.store_sk AS int) as store_sk,
CAST(lnk_chk1.employee_sk AS int) as employee_sk,
coalesce(lnk_chk1.currency, '') as currency,
CAST(lnk_chk1.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_chk1.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_chk1.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_chk1.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_chk1.line_cnt AS int) as line_cnt
FROM
lnk_chk1
WHERE customer_sk = -1""")
lnk_hit1.createOrReplaceTempView("lnk_hit1")


# COMMAND ----------
# Component lnk_hit4, Type TRANSFORMATION Original node name flt_negative_gross_profit, link lnk_hit4
lnk_hit4 = spark.sql(f"""
SELECT
coalesce(lnk_chk4.invoice_no, '') as invoice_no,
coalesce(lnk_chk4.stock_code, '') as stock_code,
CAST(lnk_chk4.quantity AS int) as quantity,
lnk_chk4.invoice_ts as invoice_ts,
CAST(lnk_chk4.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_chk4.customer_id AS int) as customer_id,
coalesce(lnk_chk4.country, '') as country,
coalesce(lnk_chk4.store_id, '') as store_id,
CAST(lnk_chk4.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_chk4.year_month, '') as year_month,
CAST(lnk_chk4.customer_sk AS int) as customer_sk,
coalesce(lnk_chk4.customer_type, '') as customer_type,
coalesce(lnk_chk4.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_chk4.category, '') as category,
CAST(lnk_chk4.product_sk AS int) as product_sk,
coalesce(lnk_chk4.region, '') as region,
CAST(lnk_chk4.store_sk AS int) as store_sk,
CAST(lnk_chk4.employee_sk AS int) as employee_sk,
coalesce(lnk_chk4.currency, '') as currency,
CAST(lnk_chk4.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_chk4.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_chk4.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_chk4.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_chk4.line_cnt AS int) as line_cnt
FROM
lnk_chk4
WHERE gross_profit < 0""")
lnk_hit4.createOrReplaceTempView("lnk_hit4")


# COMMAND ----------
# Component lnk_hit3, Type TRANSFORMATION Original node name flt_non_positive_sales, link lnk_hit3
lnk_hit3 = spark.sql(f"""
SELECT
coalesce(lnk_chk3.invoice_no, '') as invoice_no,
coalesce(lnk_chk3.stock_code, '') as stock_code,
CAST(lnk_chk3.quantity AS int) as quantity,
lnk_chk3.invoice_ts as invoice_ts,
CAST(lnk_chk3.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_chk3.customer_id AS int) as customer_id,
coalesce(lnk_chk3.country, '') as country,
coalesce(lnk_chk3.store_id, '') as store_id,
CAST(lnk_chk3.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_chk3.year_month, '') as year_month,
CAST(lnk_chk3.customer_sk AS int) as customer_sk,
coalesce(lnk_chk3.customer_type, '') as customer_type,
coalesce(lnk_chk3.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_chk3.category, '') as category,
CAST(lnk_chk3.product_sk AS int) as product_sk,
coalesce(lnk_chk3.region, '') as region,
CAST(lnk_chk3.store_sk AS int) as store_sk,
CAST(lnk_chk3.employee_sk AS int) as employee_sk,
coalesce(lnk_chk3.currency, '') as currency,
CAST(lnk_chk3.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_chk3.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_chk3.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_chk3.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_chk3.line_cnt AS int) as line_cnt
FROM
lnk_chk3
WHERE sales_amount <= 0""")
lnk_hit3.createOrReplaceTempView("lnk_hit3")


# COMMAND ----------
# Component lnk_hit2, Type TRANSFORMATION Original node name flt_orphan_product_lines, link lnk_hit2
lnk_hit2 = spark.sql(f"""
SELECT
coalesce(lnk_chk2.invoice_no, '') as invoice_no,
coalesce(lnk_chk2.stock_code, '') as stock_code,
CAST(lnk_chk2.quantity AS int) as quantity,
lnk_chk2.invoice_ts as invoice_ts,
CAST(lnk_chk2.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_chk2.customer_id AS int) as customer_id,
coalesce(lnk_chk2.country, '') as country,
coalesce(lnk_chk2.store_id, '') as store_id,
CAST(lnk_chk2.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_chk2.year_month, '') as year_month,
CAST(lnk_chk2.customer_sk AS int) as customer_sk,
coalesce(lnk_chk2.customer_type, '') as customer_type,
coalesce(lnk_chk2.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_chk2.category, '') as category,
CAST(lnk_chk2.product_sk AS int) as product_sk,
coalesce(lnk_chk2.region, '') as region,
CAST(lnk_chk2.store_sk AS int) as store_sk,
CAST(lnk_chk2.employee_sk AS int) as employee_sk,
coalesce(lnk_chk2.currency, '') as currency,
CAST(lnk_chk2.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_chk2.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_chk2.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_chk2.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_chk2.line_cnt AS int) as line_cnt
FROM
lnk_chk2
WHERE product_sk = -1""")
lnk_hit2.createOrReplaceTempView("lnk_hit2")


# COMMAND ----------
# Component lnk_tag1, Type TRANSFORMATION Original node name xfm_Tag_guest_customer_lines, link lnk_tag1
lnk_tag1 = spark.sql(f"""
SELECT
'guest_customer_lines' as check_name
FROM
lnk_hit1""")
lnk_tag1.createOrReplaceTempView("lnk_tag1")


# COMMAND ----------
# Component lnk_tag4, Type TRANSFORMATION Original node name xfm_Tag_negative_gross_profit, link lnk_tag4
lnk_tag4 = spark.sql(f"""
SELECT
'negative_gross_profit' as check_name
FROM
lnk_hit4""")
lnk_tag4.createOrReplaceTempView("lnk_tag4")


# COMMAND ----------
# Component lnk_tag3, Type TRANSFORMATION Original node name xfm_Tag_non_positive_sales, link lnk_tag3
lnk_tag3 = spark.sql(f"""
SELECT
'non_positive_sales' as check_name
FROM
lnk_hit3""")
lnk_tag3.createOrReplaceTempView("lnk_tag3")


# COMMAND ----------
# Component lnk_tag2, Type TRANSFORMATION Original node name xfm_Tag_orphan_product_lines, link lnk_tag2
lnk_tag2 = spark.sql(f"""
SELECT
'orphan_product_lines' as check_name
FROM
lnk_hit2""")
lnk_tag2.createOrReplaceTempView("lnk_tag2")


# COMMAND ----------
# Component lnk_cnt1, Type AGGREGATOR Original node name agg_Count_guest_customer_lines, link lnk_cnt1
lnk_cnt1 = spark.sql(f"""
SELECT
coalesce(lnk_tag1.check_name, '') as check_name,
COUNT(*) as hits
FROM
lnk_tag1
GROUP BY
coalesce(lnk_tag1.check_name, '')""")
lnk_cnt1.createOrReplaceTempView("lnk_cnt1")


# COMMAND ----------
# Component lnk_cnt4, Type AGGREGATOR Original node name agg_Count_negative_gross_profit, link lnk_cnt4
lnk_cnt4 = spark.sql(f"""
SELECT
coalesce(lnk_tag4.check_name, '') as check_name,
COUNT(*) as hits
FROM
lnk_tag4
GROUP BY
coalesce(lnk_tag4.check_name, '')""")
lnk_cnt4.createOrReplaceTempView("lnk_cnt4")


# COMMAND ----------
# Component lnk_cnt3, Type AGGREGATOR Original node name agg_Count_non_positive_sales, link lnk_cnt3
lnk_cnt3 = spark.sql(f"""
SELECT
coalesce(lnk_tag3.check_name, '') as check_name,
COUNT(*) as hits
FROM
lnk_tag3
GROUP BY
coalesce(lnk_tag3.check_name, '')""")
lnk_cnt3.createOrReplaceTempView("lnk_cnt3")


# COMMAND ----------
# Component lnk_cnt2, Type AGGREGATOR Original node name agg_Count_orphan_product_lines, link lnk_cnt2
lnk_cnt2 = spark.sql(f"""
SELECT
coalesce(lnk_tag2.check_name, '') as check_name,
COUNT(*) as hits
FROM
lnk_tag2
GROUP BY
coalesce(lnk_tag2.check_name, '')""")
lnk_cnt2.createOrReplaceTempView("lnk_cnt2")


# COMMAND ----------
# Component lnk_all_checks, Type UNION Original node name fnl_Checks, link lnk_all_checks
lnk_all_checks = spark.sql(f"""
SELECT
coalesce(lnk_cnt1.check_name, '') as check_name,
CAST(lnk_cnt1.hits AS int) as hits
FROM lnk_cnt1
UNION ALL
SELECT
coalesce(lnk_cnt2.check_name, '') as check_name,
CAST(lnk_cnt2.hits AS int) as hits
FROM lnk_cnt2
UNION ALL
SELECT
coalesce(lnk_cnt3.check_name, '') as check_name,
CAST(lnk_cnt3.hits AS int) as hits
FROM lnk_cnt3
UNION ALL
SELECT
coalesce(lnk_cnt4.check_name, '') as check_name,
CAST(lnk_cnt4.hits AS int) as hits
FROM lnk_cnt4""")
lnk_all_checks.createOrReplaceTempView("lnk_all_checks")


# COMMAND ----------
# Component lnk_sorted, Type SORTER Original node name srt_Checks, link lnk_sorted
lnk_sorted = spark.sql(f"""
SELECT
coalesce(lnk_all_checks.check_name, '') as check_name,
CAST(lnk_all_checks.hits AS int) as hits
FROM
lnk_all_checks
ORDER BY check_name asc""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")


# COMMAND ----------
# Component tgt_dq_fact_checks, Type TARGET 

tgt_dq_fact_checks = lnk_sorted.select( \
	coalesce(lnk_sorted.check_name, lit('')).alias('check_name'), \
	lnk_sorted.hits.cast('int').alias('hits') \
)
tgt_dq_fact_checks.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.dq_fact_checks')
