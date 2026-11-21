# Databricks notebook source
# Code converted on 2026-11-20 14:14:45
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
# Component lnk_mart, Type SOURCE Original node name src_mart, link lnk_mart
lnk_mart = spark.read.table(f'{catalog}.{schema}.RETAIL_DATA_MART')
lnk_mart.createOrReplaceTempView("lnk_mart")


# COMMAND ----------
# Component lnk_by_type, Type AGGREGATOR Original node name agg_By_Type, link lnk_by_type
lnk_by_type = spark.sql(f"""
SELECT
coalesce(lnk_mart.customer_type, '') as customer_type,
SUM(gross_profit) as profit_total,
SUM(sales_amount) as sales_total,
SUM(line_cnt) as lines
FROM
lnk_mart
GROUP BY
coalesce(lnk_mart.customer_type, '')""")
lnk_by_type.createOrReplaceTempView("lnk_by_type")


# COMMAND ----------
# Component lnk_ranked, Type SORTER Original node name srt_Profit, link lnk_ranked
lnk_ranked = spark.sql(f"""
SELECT
coalesce(lnk_by_type.customer_type, '') as customer_type,
CAST(lnk_by_type.profit_total AS decimal(18,2)) as profit_total,
CAST(lnk_by_type.sales_total AS decimal(18,2)) as sales_total,
CAST(lnk_by_type.lines AS bigint) as lines
FROM
lnk_by_type
ORDER BY profit_total desc""")
lnk_ranked.createOrReplaceTempView("lnk_ranked")


# COMMAND ----------
# Component lnk_win_in, Type REPLICATE Original node name cpy_Ranked, link lnk_win_in
lnk_win_in = spark.sql(f"""
SELECT
coalesce(lnk_ranked.customer_type, '') as customer_type,
CAST(lnk_ranked.profit_total AS decimal(18,2)) as profit_total,
CAST(lnk_ranked.sales_total AS decimal(18,2)) as sales_total,
CAST(lnk_ranked.lines AS bigint) as lines
FROM
lnk_ranked""")
lnk_win_in.createOrReplaceTempView("lnk_win_in")


# COMMAND ----------
# Component lnk_all, Type REPLICATE Original node name cpy_Ranked, link lnk_all
lnk_all = spark.sql(f"""
SELECT
coalesce(lnk_ranked.customer_type, '') as customer_type,
CAST(lnk_ranked.profit_total AS decimal(18,2)) as profit_total,
CAST(lnk_ranked.sales_total AS decimal(18,2)) as sales_total,
CAST(lnk_ranked.lines AS bigint) as lines
FROM
lnk_ranked""")
lnk_all.createOrReplaceTempView("lnk_all")


lnk_winner = spark.sql(f"""
SELECT * FROM lnk_win_in
ORDER BY profit_total DESC, customer_type ASC, sales_total ASC, lines ASC
LIMIT 1
""")
lnk_winner.createOrReplaceTempView("lnk_winner")


# COMMAND ----------
# Component tgt_customer_type_ranking, Type TARGET 

tgt_customer_type_ranking = lnk_all.select( \
	coalesce(lnk_all.customer_type, lit('')).alias('customer_type'), \
	lnk_all.profit_total.cast('decimal(18,2)').alias('profit_total'), \
	lnk_all.sales_total.cast('decimal(18,2)').alias('sales_total'), \
	lnk_all.lines.cast('bigint').alias('lines') \
)
tgt_customer_type_ranking.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.Q2_CustomerType_Profit')


# COMMAND ----------
# Component tgt_customer_type_winner, Type TARGET 

tgt_customer_type_winner = lnk_winner.select( \
	coalesce(lnk_winner.customer_type, lit('')).alias('customer_type'), \
	lnk_winner.profit_total.cast('decimal(18,2)').alias('profit_total'), \
	lnk_winner.sales_total.cast('decimal(18,2)').alias('sales_total'), \
	lnk_winner.lines.cast('bigint').alias('lines') \
)
tgt_customer_type_winner.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.Q2_Winning_CustomerType')
