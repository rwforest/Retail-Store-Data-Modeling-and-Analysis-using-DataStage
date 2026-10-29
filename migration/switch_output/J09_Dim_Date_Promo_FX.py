# Databricks notebook source
# MAGIC %md
# MAGIC # J09_Dim_Date_Promo_FX
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J09_Dim_Date_Promo_FX.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline (Python) equivalent of DataStage job J09_Dim_Date_Promo_FX
# Builds Dim_Date, Dim_Promotion and Dim_FX (country x month).
#
# Notes:
# - The ${language} placeholder was not filled in, so I used Python.
# - DataStage sequential-file targets (dim_date.csv, dim_promotion.csv, dim_fx.csv) become
#   pipeline-managed tables (materialized views) in the target catalog/schema.
# - Job parameters are read from pipeline configuration, with the original defaults.
#   Only pSrcDir is used by this job. The other parameters are kept for parity.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# ---------------------------------------------------------------------------
# Parameters (set these as pipeline configuration values to override defaults)
# ---------------------------------------------------------------------------
p_src_dir = spark.conf.get("pSrcDir", "C:/case_study/Source_Data")
p_stg_dir = spark.conf.get("pStgDir", "C:/case_study/Staging")      # unused in this job
p_dwh_dir = spark.conf.get("pDwhDir", "C:/case_study/Warehouse")    # replaced by pipeline tables
p_mart_dir = spark.conf.get("pMartDir", "C:/case_study/Marts")      # unused in this job
p_audit_dir = spark.conf.get("pAuditDir", "C:/case_study/Audit")    # unused in this job
p_run_date = spark.conf.get("pRunDate", "2011-12-31")              # unused in this job
p_eff_date = spark.conf.get("pEffDate", "2011-07-01")              # unused in this job


# COMMAND ----------
def src_calendar():
    schema = """
        DateKey INT, FullDate DATE, Year INT, Quarter INT, Month INT,
        MonthName STRING, Day INT, DayOfWeekISO INT, IsWeekend INT, IsHoliday INT
    """
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(f"{p_src_dir}/calendar.csv")
    )


# COMMAND ----------
def src_promotions():
    schema = """
        PromoID STRING, Category STRING, StartDate DATE, EndDate DATE, DiscountPct INT
    """
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(f"{p_src_dir}/promotions.csv")
    )


# COMMAND ----------
def src_fx_rates():
    schema = "YearMonth STRING, currency STRING, RateFromGBP DECIMAL(18,6)"
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(f"{p_src_dir}/fx_rates.csv")
    )


# COMMAND ----------
def src_country_currency():
    schema = "country STRING, currency STRING"
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(f"{p_src_dir}/country_currency.csv")
    )


# COMMAND ----------
def dim_date():
    cal = spark.read.table("src_calendar")
    return cal.select(
        F.col("DateKey").alias("date_key"),
        F.col("FullDate").alias("full_date"),
        F.col("Year").alias("year"),
        F.col("Quarter").alias("quarter"),
        F.col("Month").alias("month"),
        F.col("MonthName").alias("month_name"),
        F.col("Day").alias("day"),
        F.col("DayOfWeekISO").alias("day_of_week"),
        # DateToString(FullDate, "%yyyy%mm") -> yyyyMM
        F.date_format("FullDate", "yyyyMM").alias("year_month"),
        # Trading day = not weekend and not holiday
        F.when((F.col("IsWeekend") == 0) & (F.col("IsHoliday") == 0), F.lit(1))
         .otherwise(F.lit(0)).alias("is_trading_day"),
    )


# COMMAND ----------
def dim_promotion():
    promo = spark.read.table("src_promotions")

    # DataStage generated the surrogate key from partition number / row number.
    # Here we use a deterministic sequence ordered by the business key.
    sk_window = Window.orderBy(F.trim(F.col("PromoID")))

    return promo.select(
        F.row_number().over(sk_window).cast("int").alias("promo_sk"),
        F.trim(F.col("PromoID")).alias("promo_id"),
        F.trim(F.col("Category")).alias("category"),
        F.col("StartDate").alias("start_date"),
        F.col("EndDate").alias("end_date"),
        F.col("DiscountPct").alias("discount_pct"),
        # DaysSinceFromDate(EndDate, StartDate) + 1 -> inclusive duration in days
        (F.datediff(F.col("EndDate"), F.col("StartDate")) + 1).alias("duration_days"),
        # DateToString(..., "%yyyy%mm%dd") -> yyyyMMdd integer key
        F.date_format("StartDate", "yyyyMMdd").cast("int").alias("start_key"),
        F.date_format("EndDate", "yyyyMMdd").cast("int").alias("end_key"),
    )


# COMMAND ----------
def dim_fx():
    cc = spark.read.table("src_country_currency")
    fx = spark.read.table("src_fx_rates")

    # jn_Country_FX: inner join on currency
    joined = cc.alias("cc").join(fx.alias("fx"), on="currency", how="inner")

    # xfm_FX_Key: trim country, strip "-" from YearMonth, rename rate
    return joined.select(
        F.trim(F.col("country")).alias("country"),
        F.regexp_replace(F.col("YearMonth"), "-", "").alias("year_month"),
        F.col("currency").alias("currency"),
        F.col("RateFromGBP").alias("fx_rate"),
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
