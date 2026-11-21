# Databricks notebook source
# Code converted on 2026-11-20 14:15:10
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
# Component lnk_mart, Type SOURCE Original node name src_mart, link lnk_mart
lnk_mart = spark.read.table(f'{catalog}.{schema}.RETAIL_DATA_MART')
lnk_mart.createOrReplaceTempView("lnk_mart")


# COMMAND ----------
# Component lnk_cust_profit, Type AGGREGATOR Original node name agg_Customer_Profit, link lnk_cust_profit
lnk_cust_profit = spark.sql(f"""
SELECT
CAST(lnk_mart.customer_id AS int) as customer_id,
SUM(gross_profit) as profit_total
FROM
lnk_mart
GROUP BY
CAST(lnk_mart.customer_id AS int)""")
lnk_cust_profit.createOrReplaceTempView("lnk_cust_profit")


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
# Component lnk_ranked, Type SORTER Original node name srt_Profit, link lnk_ranked
lnk_ranked = spark.sql(f"""
SELECT
CAST(lnk_cust_profit.customer_id AS int) as customer_id,
CAST(lnk_cust_profit.profit_total AS decimal(18,2)) as profit_total
FROM
lnk_cust_profit
ORDER BY profit_total desc""")
lnk_ranked.createOrReplaceTempView("lnk_ranked")


lnk_top100 = spark.sql(f"""
SELECT * FROM lnk_ranked
ORDER BY profit_total DESC, customer_id ASC
LIMIT 100
""")
lnk_top100.createOrReplaceTempView("lnk_top100")


# COMMAND ----------
# Component lnk_named, Type JOINER Original node name jn_Customer, link lnk_named
lnk_named = spark.sql(f"""
SELECT
CAST(lnk_top100.customer_id AS int) as customer_id,
CAST(lnk_top100.profit_total AS decimal(18,2)) as profit_total,
CAST(lnk_cur.customer_sk AS int) as customer_sk,
coalesce(lnk_cur.cust_name, '') as cust_name,
coalesce(lnk_cur.customer_type, '') as customer_type,
coalesce(lnk_cur.loyalty_tier, '') as loyalty_tier
FROM
lnk_top100
INNER JOIN lnk_cur ON CAST(lnk_top100.customer_id AS int) = CAST(lnk_cur.customer_id AS int)""")
lnk_named.createOrReplaceTempView("lnk_named")


# COMMAND ----------
# Component lnk_bonus, Type TRANSFORMATION Original node name xfm_Bonus, link lnk_bonus
lnk_bonus = spark.sql(f"""
SELECT
CAST(lnk_named.customer_id AS int) as customer_id,
CAST(lnk_named.profit_total AS decimal(18,2)) as profit_total,
CAST(lnk_named.customer_sk AS int) as customer_sk,
coalesce(lnk_named.cust_name, '') as cust_name,
coalesce(lnk_named.customer_type, '') as customer_type,
coalesce(lnk_named.loyalty_tier, '') as loyalty_tier,
IF ( coalesce(lnk_named.loyalty_tier, '') = 'PLATINUM' , 0.05 , IF ( coalesce(lnk_named.loyalty_tier, '') = 'GOLD' , 0.04 , IF ( coalesce(lnk_named.loyalty_tier, '') = 'SILVER' , 0.03 , 0.02 ) ) ) as bonus_rate,
IF ( coalesce(lnk_named.loyalty_tier, '') = 'PLATINUM' , CAST(lnk_named.profit_total AS decimal(18,2)) * 0.05 , IF ( coalesce(lnk_named.loyalty_tier, '') = 'GOLD' , CAST(lnk_named.profit_total AS decimal(18,2)) * 0.04 , IF ( coalesce(lnk_named.loyalty_tier, '') = 'SILVER' , CAST(lnk_named.profit_total AS decimal(18,2)) * 0.03 , CAST(lnk_named.profit_total AS decimal(18,2)) * 0.02 ) ) ) as bonus_amount,
'BONUS_2011' as program
FROM
lnk_named""")
lnk_bonus.createOrReplaceTempView("lnk_bonus")


# COMMAND ----------
# Component lnk_total_in, Type REPLICATE Original node name cpy_Bonus, link lnk_total_in
lnk_total_in = spark.sql(f"""
SELECT
CAST(lnk_bonus.customer_id AS int) as customer_id,
CAST(lnk_bonus.profit_total AS decimal(18,2)) as profit_total,
CAST(lnk_bonus.customer_sk AS int) as customer_sk,
coalesce(lnk_bonus.cust_name, '') as cust_name,
coalesce(lnk_bonus.customer_type, '') as customer_type,
coalesce(lnk_bonus.loyalty_tier, '') as loyalty_tier,
CAST(lnk_bonus.bonus_rate AS decimal(18,6)) as bonus_rate,
CAST(lnk_bonus.bonus_amount AS decimal(18,2)) as bonus_amount,
coalesce(lnk_bonus.program, '') as program
FROM
lnk_bonus""")
lnk_total_in.createOrReplaceTempView("lnk_total_in")


# COMMAND ----------
# Component lnk_out, Type REPLICATE Original node name cpy_Bonus, link lnk_out
lnk_out = spark.sql(f"""
SELECT
CAST(lnk_bonus.customer_id AS int) as customer_id,
CAST(lnk_bonus.profit_total AS decimal(18,2)) as profit_total,
CAST(lnk_bonus.customer_sk AS int) as customer_sk,
coalesce(lnk_bonus.cust_name, '') as cust_name,
coalesce(lnk_bonus.customer_type, '') as customer_type,
coalesce(lnk_bonus.loyalty_tier, '') as loyalty_tier,
CAST(lnk_bonus.bonus_rate AS decimal(18,6)) as bonus_rate,
CAST(lnk_bonus.bonus_amount AS decimal(18,2)) as bonus_amount,
coalesce(lnk_bonus.program, '') as program
FROM
lnk_bonus""")
lnk_out.createOrReplaceTempView("lnk_out")


# COMMAND ----------
# Component tgt_bonus_program, Type TARGET 

tgt_bonus_program = lnk_out.select( \
	lnk_out.customer_id.cast('int').alias('customer_id'), \
	lnk_out.profit_total.cast('decimal(18,2)').alias('profit_total'), \
	lnk_out.customer_sk.cast('int').alias('customer_sk'), \
	coalesce(lnk_out.cust_name, lit('')).alias('cust_name'), \
	coalesce(lnk_out.customer_type, lit('')).alias('customer_type'), \
	coalesce(lnk_out.loyalty_tier, lit('')).alias('loyalty_tier'), \
	lnk_out.bonus_rate.cast('decimal(18,6)').alias('bonus_rate'), \
	lnk_out.bonus_amount.cast('decimal(18,2)').alias('bonus_amount'), \
	coalesce(lnk_out.program, lit('')).alias('program') \
)
tgt_bonus_program.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.Q3_Bonus_Program')


# COMMAND ----------
# Component lnk_total, Type AGGREGATOR Original node name agg_Bonus_Total, link lnk_total
lnk_total = spark.sql(f"""
SELECT
coalesce(lnk_total_in.program, '') as program,
SUM(bonus_amount) as bonus_total
FROM
lnk_total_in
GROUP BY
coalesce(lnk_total_in.program, '')""")
lnk_total.createOrReplaceTempView("lnk_total")


# COMMAND ----------
# Component tgt_bonus_summary, Type TARGET 

tgt_bonus_summary = lnk_total.select( \
	coalesce(lnk_total.program, lit('')).alias('program'), \
	lnk_total.bonus_total.cast('decimal(18,2)').alias('bonus_total') \
)
tgt_bonus_summary.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.Q3_Bonus_Summary')
