# Databricks notebook source
# Code converted on 2026-11-19 16:24:16
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
# Component lnk_dim_promo, Type SOURCE Original node name src_dim_promotion, link lnk_dim_promo
lnk_dim_promo = spark.read.table(f'{catalog}.{schema}.dim_promotion')
lnk_dim_promo.createOrReplaceTempView("lnk_dim_promo")


# COMMAND ----------
# Component lnk_fact, Type SOURCE Original node name src_fact_sales, link lnk_fact
lnk_fact = spark.read.table(f'{catalog}.{schema}.fact_sales')
lnk_fact.createOrReplaceTempView("lnk_fact")


# COMMAND ----------
# Component lnk_fp, Type JOINER Original node name jn_Promo_Category, link lnk_fp
lnk_fp = spark.sql(f"""
SELECT
CAST(lnk_fact.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_fact.category, '') as category,
CAST(lnk_fact.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_fact.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_fact.line_cnt AS int) as line_cnt,
coalesce(lnk_dim_promo.promo_id, '') as promo_id,
CAST(lnk_dim_promo.discount_pct AS int) as discount_pct,
CAST(lnk_dim_promo.start_key AS int) as start_key,
CAST(lnk_dim_promo.end_key AS int) as end_key
FROM
lnk_fact
INNER JOIN lnk_dim_promo ON coalesce(lnk_fact.category, '') = coalesce(lnk_dim_promo.category, '')""")
lnk_fp.createOrReplaceTempView("lnk_fp")


# COMMAND ----------
# Component lnk_window, Type TRANSFORMATION Original node name flt_In_Window, link lnk_window
lnk_window = spark.sql(f"""
SELECT
CAST(lnk_fp.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_fp.category, '') as category,
CAST(lnk_fp.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_fp.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_fp.line_cnt AS int) as line_cnt,
coalesce(lnk_fp.promo_id, '') as promo_id,
CAST(lnk_fp.discount_pct AS int) as discount_pct,
CAST(lnk_fp.start_key AS int) as start_key,
CAST(lnk_fp.end_key AS int) as end_key
FROM
lnk_fp
WHERE invoice_date_key >= start_key and invoice_date_key <= end_key""")
lnk_window.createOrReplaceTempView("lnk_window")


# COMMAND ----------
# Component lnk_promo, Type AGGREGATOR Original node name agg_By_Promo, link lnk_promo
lnk_promo = spark.sql(f"""
SELECT
coalesce(lnk_window.promo_id, '') as promo_id,
coalesce(lnk_window.category, '') as category,
CAST(lnk_window.discount_pct AS int) as discount_pct,
SUM(sales_amount) as promo_sales,
SUM(gross_profit) as promo_profit,
SUM(line_cnt) as promo_lines
FROM
lnk_window
GROUP BY
coalesce(lnk_window.promo_id, ''),
coalesce(lnk_window.category, ''),
CAST(lnk_window.discount_pct AS int)""")
lnk_promo.createOrReplaceTempView("lnk_promo")


# COMMAND ----------
# Component lnk_cost, Type TRANSFORMATION Original node name xfm_Discount_Cost, link lnk_cost
lnk_cost = spark.sql(f"""
SELECT
coalesce(lnk_promo.promo_id, '') as promo_id,
coalesce(lnk_promo.category, '') as category,
CAST(lnk_promo.discount_pct AS int) as discount_pct,
CAST(lnk_promo.promo_sales AS decimal(18,2)) as promo_sales,
CAST(lnk_promo.promo_profit AS decimal(18,2)) as promo_profit,
CAST(lnk_promo.promo_lines AS bigint) as promo_lines,
CAST(lnk_promo.promo_sales AS decimal(18,2)) * CAST(lnk_promo.discount_pct AS int) / 100 as est_discount_cost,
CAST(lnk_promo.promo_profit AS decimal(18,2)) - ( CAST(lnk_promo.promo_sales AS decimal(18,2)) * CAST(lnk_promo.discount_pct AS int) / 100 ) as profit_after_discount
FROM
lnk_promo""")
lnk_cost.createOrReplaceTempView("lnk_cost")


# COMMAND ----------
# Component lnk_sorted, Type SORTER Original node name srt_Promo, link lnk_sorted
lnk_sorted = spark.sql(f"""
SELECT
coalesce(lnk_cost.promo_id, '') as promo_id,
coalesce(lnk_cost.category, '') as category,
CAST(lnk_cost.discount_pct AS int) as discount_pct,
CAST(lnk_cost.promo_sales AS decimal(18,2)) as promo_sales,
CAST(lnk_cost.promo_profit AS decimal(18,2)) as promo_profit,
CAST(lnk_cost.promo_lines AS bigint) as promo_lines,
CAST(lnk_cost.est_discount_cost AS decimal(18,2)) as est_discount_cost,
CAST(lnk_cost.profit_after_discount AS decimal(18,2)) as profit_after_discount
FROM
lnk_cost
ORDER BY profit_after_discount desc""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")


# COMMAND ----------
# Component tgt_mart_promo_effectiveness, Type TARGET 

tgt_mart_promo_effectiveness = lnk_sorted.select( \
	coalesce(lnk_sorted.promo_id, lit('')).alias('promo_id'), \
	coalesce(lnk_sorted.category, lit('')).alias('category'), \
	lnk_sorted.discount_pct.cast('int').alias('discount_pct'), \
	lnk_sorted.promo_sales.cast('decimal(18,2)').alias('promo_sales'), \
	lnk_sorted.promo_profit.cast('decimal(18,2)').alias('promo_profit'), \
	lnk_sorted.promo_lines.cast('bigint').alias('promo_lines'), \
	lnk_sorted.est_discount_cost.cast('decimal(18,2)').alias('est_discount_cost'), \
	lnk_sorted.profit_after_discount.cast('decimal(18,2)').alias('profit_after_discount') \
)
tgt_mart_promo_effectiveness.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.Promo_Effectiveness')
