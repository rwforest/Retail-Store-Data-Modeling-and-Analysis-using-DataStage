# Databricks notebook source
# Code converted on 2026-11-19 16:20:30
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
# Component lnk_cal, Type SOURCE Original node name src_calendar, link lnk_cal
lnk_cal = spark.read.format('csv').option('header', 'true').option('escape', '"').load(f'''{pSrcDir}/calendar.csv''')
# Conforming fields names to the component layout
lnk_cal_conformed_cols = ["DateKey", "FullDate", "Year", "Quarter", "Month", "MonthName", "Day", "DayOfWeekISO", "IsWeekend", "IsHoliday"]
lnk_cal = DatabricksConversionSupplements.conform_df_columns(lnk_cal,lnk_cal_conformed_cols)
lnk_cal.createOrReplaceTempView("lnk_cal")


# COMMAND ----------
# Component lnk_cc, Type SOURCE Original node name src_country_currency, link lnk_cc
lnk_cc = spark.read.format('csv').option('header', 'true').option('escape', '"').load(f'''{pSrcDir}/country_currency.csv''')
# Conforming fields names to the component layout
lnk_cc_conformed_cols = ["country", "currency"]
lnk_cc = DatabricksConversionSupplements.conform_df_columns(lnk_cc,lnk_cc_conformed_cols)
lnk_cc.createOrReplaceTempView("lnk_cc")


# COMMAND ----------
# Component lnk_fx, Type SOURCE Original node name src_fx_rates, link lnk_fx
lnk_fx = spark.read.format('csv').option('header', 'true').option('escape', '"').load(f'''{pSrcDir}/fx_rates.csv''')
# Conforming fields names to the component layout
lnk_fx_conformed_cols = ["YearMonth", "currency", "RateFromGBP"]
lnk_fx = DatabricksConversionSupplements.conform_df_columns(lnk_fx,lnk_fx_conformed_cols)
lnk_fx.createOrReplaceTempView("lnk_fx")


# COMMAND ----------
# Component lnk_promo, Type SOURCE Original node name src_promotions, link lnk_promo
lnk_promo = spark.read.format('csv').option('header', 'true').option('escape', '"').load(f'''{pSrcDir}/promotions.csv''')
# Conforming fields names to the component layout
lnk_promo_conformed_cols = ["PromoID", "Category", "StartDate", "EndDate", "DiscountPct"]
lnk_promo = DatabricksConversionSupplements.conform_df_columns(lnk_promo,lnk_promo_conformed_cols)
lnk_promo.createOrReplaceTempView("lnk_promo")


# COMMAND ----------
# Component lnk_dim_date, Type TRANSFORMATION Original node name xfm_Date_Attrs, link lnk_dim_date
lnk_dim_date = spark.sql(f"""
SELECT
CAST(lnk_cal.DateKey AS int) as date_key,
lnk_cal.FullDate as full_date,
CAST(lnk_cal.Year AS int) as year,
CAST(lnk_cal.Quarter AS int) as quarter,
CAST(lnk_cal.Month AS int) as month,
coalesce(lnk_cal.MonthName, '') as month_name,
CAST(lnk_cal.Day AS int) as day,
CAST(lnk_cal.DayOfWeekISO AS int) as day_of_week,
date_format(lnk_cal.FullDate, 'yyyyMM') as year_month,
IF ( CAST(lnk_cal.IsWeekend AS int) = 0 And CAST(lnk_cal.IsHoliday AS int) = 0 , 1 , 0 ) as is_trading_day
FROM
lnk_cal""")
lnk_dim_date.createOrReplaceTempView("lnk_dim_date")


# COMMAND ----------
# Component lnk_dim_promo, Type TRANSFORMATION Original node name xfm_Promo_SK, link lnk_dim_promo
lnk_dim_promo = spark.sql(f"""
SELECT
row_number() over (order by 1) as promo_sk,
trim(coalesce(lnk_promo.PromoID, '')) as promo_id,
trim(coalesce(lnk_promo.Category, '')) as category,
lnk_promo.StartDate as start_date,
lnk_promo.EndDate as end_date,
CAST(lnk_promo.DiscountPct AS int) as discount_pct,
DATEDIFF(lnk_promo.EndDate, lnk_promo.StartDate) + 1 as duration_days,
date_format(lnk_promo.StartDate, 'yyyyMMdd') as start_key,
date_format(lnk_promo.EndDate, 'yyyyMMdd') as end_key
FROM
lnk_promo""")
lnk_dim_promo.createOrReplaceTempView("lnk_dim_promo")


# COMMAND ----------
# Component lnk_cc_fx, Type JOINER Original node name jn_Country_FX, link lnk_cc_fx
lnk_cc_fx = spark.sql(f"""
SELECT
coalesce(lnk_cc.country, '') as country,
coalesce(lnk_cc.currency, '') as currency,
coalesce(lnk_fx.YearMonth, '') as YearMonth,
CAST(lnk_fx.RateFromGBP AS decimal(18,6)) as RateFromGBP
FROM
lnk_cc
INNER JOIN lnk_fx ON coalesce(lnk_cc.currency, '') = coalesce(lnk_fx.currency, '')""")
lnk_cc_fx.createOrReplaceTempView("lnk_cc_fx")


# COMMAND ----------
# Component tgt_dim_date, Type TARGET 

tgt_dim_date = lnk_dim_date.select( \
	lnk_dim_date.date_key.cast('int').alias('date_key'), \
	lnk_dim_date.full_date.alias('full_date'), \
	lnk_dim_date.year.cast('int').alias('year'), \
	lnk_dim_date.quarter.cast('int').alias('quarter'), \
	lnk_dim_date.month.cast('int').alias('month'), \
	coalesce(lnk_dim_date.month_name, lit('')).alias('month_name'), \
	lnk_dim_date.day.cast('int').alias('day'), \
	lnk_dim_date.day_of_week.cast('int').alias('day_of_week'), \
	coalesce(lnk_dim_date.year_month, lit('')).alias('year_month'), \
	lnk_dim_date.is_trading_day.cast('int').alias('is_trading_day') \
)
tgt_dim_date.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.dim_date')


# COMMAND ----------
# Component tgt_dim_promotion, Type TARGET 

tgt_dim_promotion = lnk_dim_promo.select( \
	lnk_dim_promo.promo_sk.cast('int').alias('promo_sk'), \
	coalesce(lnk_dim_promo.promo_id, lit('')).alias('promo_id'), \
	coalesce(lnk_dim_promo.category, lit('')).alias('category'), \
	lnk_dim_promo.start_date.alias('start_date'), \
	lnk_dim_promo.end_date.alias('end_date'), \
	lnk_dim_promo.discount_pct.cast('int').alias('discount_pct'), \
	lnk_dim_promo.duration_days.cast('int').alias('duration_days'), \
	lnk_dim_promo.start_key.cast('int').alias('start_key'), \
	lnk_dim_promo.end_key.cast('int').alias('end_key') \
)
tgt_dim_promotion.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.dim_promotion')


# COMMAND ----------
# Component lnk_dim_fx, Type TRANSFORMATION Original node name xfm_FX_Key, link lnk_dim_fx
lnk_dim_fx = spark.sql(f"""
SELECT
trim(coalesce(lnk_cc_fx.country, '')) as country,
REGEXP_REPLACE(coalesce(lnk_cc_fx.YearMonth, ''), '-', '') as year_month,
coalesce(lnk_cc_fx.currency, '') as currency,
CAST(lnk_cc_fx.RateFromGBP AS decimal(18,6)) as fx_rate
FROM
lnk_cc_fx""")
lnk_dim_fx.createOrReplaceTempView("lnk_dim_fx")


# COMMAND ----------
# Component tgt_dim_fx, Type TARGET 

tgt_dim_fx = lnk_dim_fx.select( \
	coalesce(lnk_dim_fx.country, lit('')).alias('country'), \
	coalesce(lnk_dim_fx.year_month, lit('')).alias('year_month'), \
	coalesce(lnk_dim_fx.currency, lit('')).alias('currency'), \
	lnk_dim_fx.fx_rate.cast('decimal(18,6)').alias('fx_rate') \
)
tgt_dim_fx.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.dim_fx')
