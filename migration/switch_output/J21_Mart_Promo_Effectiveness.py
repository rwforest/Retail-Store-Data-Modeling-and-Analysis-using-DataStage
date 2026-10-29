# Databricks notebook source
# MAGIC %md
# MAGIC # J21_Mart_Promo_Effectiveness
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J21_Mart_Promo_Effectiveness.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline: J21_Mart_Promo_Effectiveness
# Purpose: Sales inside each promotion window (range join on category + date).
# Source: DataStage parallel job J21_Mart_Promo_Effectiveness (\\Retail_DW\\Pipeline\\4_Marts)
#
# Flow:
#   src_fact_sales + src_dim_promotion -> inner join on category -> filter on date window
#   -> aggregate by promo -> compute discount cost / profit after discount -> sort -> mart
#
# The original job wrote a CSV file (Promo_Effectiveness.csv, overwrite).
# In a declarative pipeline the mart is published as a materialized view (Delta table).
# Export it to CSV with a separate job if a file is still needed.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, DateType
)

# COMMAND ----------
# ---------------------------------------------------------------------------
# Parameters (DataStage job parameters -> pipeline configuration values)
# Set these in the pipeline settings "configuration" section to override defaults.
# ---------------------------------------------------------------------------
pSrcDir   = spark.conf.get("pSrcDir",   "C:/case_study/Source_Data")
pStgDir   = spark.conf.get("pStgDir",   "C:/case_study/Staging")
pDwhDir   = spark.conf.get("pDwhDir",   "C:/case_study/Warehouse")
pMartDir  = spark.conf.get("pMartDir",  "C:/case_study/Marts")
pAuditDir = spark.conf.get("pAuditDir", "C:/case_study/Audit")
pRunDate  = spark.conf.get("pRunDate",  "2011-12-31")
pEffDate  = spark.conf.get("pEffDate",  "2011-07-01")

# COMMAND ----------
# ---------------------------------------------------------------------------
# Schemas (taken from the DataStage column definitions)
# ---------------------------------------------------------------------------
fact_sales_schema = StructType([
    StructField("invoice_no",       StringType(),        False),
    StructField("stock_code",       StringType(),        False),
    StructField("quantity",         IntegerType(),       False),
    StructField("invoice_ts",       TimestampType(),     False),
    StructField("unit_price",       DecimalType(18, 2),  False),
    StructField("customer_id",      IntegerType(),       False),
    StructField("country",          StringType(),        False),
    StructField("store_id",         StringType(),        False),
    StructField("invoice_date_key", IntegerType(),       False),
    StructField("year_month",       StringType(),        False),
    StructField("customer_sk",      IntegerType(),       False),
    StructField("customer_type",    StringType(),        False),
    StructField("loyalty_tier",     StringType(),        False),
    StructField("category",         StringType(),        False),
    StructField("product_sk",       IntegerType(),       False),
    StructField("region",           StringType(),        False),
    StructField("store_sk",         IntegerType(),       False),
    StructField("employee_sk",      IntegerType(),       False),
    StructField("currency",         StringType(),        False),
    StructField("sales_amount",     DecimalType(18, 2),  False),
    StructField("cost_amount",      DecimalType(18, 2),  False),
    StructField("gross_profit",     DecimalType(18, 2),  False),
    StructField("local_amount",     DecimalType(18, 2),  False),
    StructField("line_cnt",         IntegerType(),       False),
])

# COMMAND ----------
dim_promotion_schema = StructType([
    StructField("promo_sk",       IntegerType(), False),
    StructField("promo_id",       StringType(),  False),
    StructField("category",       StringType(),  False),
    StructField("start_date",     DateType(),    False),
    StructField("end_date",       DateType(),    False),
    StructField("discount_pct",   IntegerType(), False),
    StructField("duration_days",  IntegerType(), False),
    StructField("start_key",      IntegerType(), False),
    StructField("end_key",        IntegerType(), False),
])


# COMMAND ----------
def src_fact_sales():
    return (
        spark.read
        .option("header", True)          # firstLineColumnNames
        .schema(fact_sales_schema)
        .csv(f"{pDwhDir}/fact_sales.csv")
    )


# COMMAND ----------
def src_dim_promotion():
    return (
        spark.read
        .option("header", True)
        .schema(dim_promotion_schema)
        .csv(f"{pDwhDir}/dim_promotion.csv")
    )


# COMMAND ----------
def jn_promo_category():
    fact = spark.read.table("src_fact_sales").select(
        "invoice_date_key", "category", "sales_amount", "gross_profit", "line_cnt"
    )
    promo = spark.read.table("src_dim_promotion").select(
        "category", "promo_id", "discount_pct", "start_key", "end_key"
    )
    # Join on the key column "category" (appears once in the output)
    return fact.join(promo, on="category", how="inner")


# COMMAND ----------
def flt_in_window():
    return spark.read.table("jn_promo_category").where(
        (F.col("invoice_date_key") >= F.col("start_key"))
        & (F.col("invoice_date_key") <= F.col("end_key"))
    )


# COMMAND ----------
def agg_by_promo():
    return (
        spark.read.table("flt_in_window")
        .groupBy("promo_id", "category", "discount_pct")
        .agg(
            F.sum("sales_amount").cast(DecimalType(18, 2)).alias("promo_sales"),
            F.sum("gross_profit").cast(DecimalType(18, 2)).alias("promo_profit"),
            F.sum("line_cnt").cast("bigint").alias("promo_lines"),
        )
    )


# COMMAND ----------
def xfm_discount_cost():
    est_cost = F.col("promo_sales") * F.col("discount_pct") / F.lit(100)
    return (
        spark.read.table("agg_by_promo")
        .withColumn("est_discount_cost", est_cost.cast(DecimalType(18, 2)))
        .withColumn(
            "profit_after_discount",
            (F.col("promo_profit") - est_cost).cast(DecimalType(18, 2)),
        )
    )


# COMMAND ----------
def mart_promo_effectiveness():
    return (
        spark.read.table("xfm_discount_cost")
        .select(
            "promo_id",
            "category",
            "discount_pct",
            "promo_sales",
            "promo_profit",
            "promo_lines",
            "est_discount_cost",
            "profit_after_discount",
        )
        .orderBy(F.col("profit_after_discount").desc())
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
