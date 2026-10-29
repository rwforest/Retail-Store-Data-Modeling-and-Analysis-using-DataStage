# Databricks notebook source
# MAGIC %md
# MAGIC # J12_Fact_Invoice
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J12_Fact_Invoice.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline: J12_Fact_Invoice
# Invoice-grain fact (basket level) rolled up from Fact_Sales.
#
# Original DataStage flow:
#   src_fact_sales -> agg_Invoice -> xfm_Basket_Metrics -> srt_Fact_Invoice -> tgt_fact_invoice
#
# Notes:
#  - The language was not specified in the request, so Python is used.
#  - DataStage job parameters are read from the pipeline configuration (spark.conf) with the original defaults.
#    On Databricks, point pDwhDir to a Unity Catalog Volume or cloud storage path instead of a local C:/ path.
#  - The CSV target (fact_invoice.csv, overwrite) becomes a materialized view managed by the pipeline.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
)

# COMMAND ----------
# ---------------------------------------------------------------------------
# Job parameters (defaults taken from the DataStage job definition)
# Only pDwhDir is used by this job; the others are kept for parity and future use.
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
# Schema of fact_sales.csv (from the src_fact_sales stage column definitions)
# ---------------------------------------------------------------------------
fact_sales_schema = StructType([
    StructField("invoice_no",       StringType(),       False),
    StructField("stock_code",       StringType(),       False),
    StructField("quantity",         IntegerType(),      False),
    StructField("invoice_ts",       TimestampType(),    False),
    StructField("unit_price",       DecimalType(18, 2), False),
    StructField("customer_id",      IntegerType(),      False),
    StructField("country",          StringType(),       False),
    StructField("store_id",         StringType(),       False),
    StructField("invoice_date_key", IntegerType(),      False),
    StructField("year_month",       StringType(),       False),
    StructField("customer_sk",      IntegerType(),      False),
    StructField("customer_type",    StringType(),       False),
    StructField("loyalty_tier",     StringType(),       False),
    StructField("category",         StringType(),       False),
    StructField("product_sk",       IntegerType(),      False),
    StructField("region",           StringType(),       False),
    StructField("store_sk",         IntegerType(),      False),
    StructField("employee_sk",      IntegerType(),      False),
    StructField("currency",         StringType(),       False),
    StructField("sales_amount",     DecimalType(18, 2), False),
    StructField("cost_amount",      DecimalType(18, 2), False),
    StructField("gross_profit",     DecimalType(18, 2), False),
    StructField("local_amount",     DecimalType(18, 2), False),
    StructField("line_cnt",         IntegerType(),      False),
])


# COMMAND ----------
def src_fact_sales():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(fact_sales_schema)
        .load(f"{pDwhDir}/fact_sales.csv")
    )


# COMMAND ----------
def agg_invoice():
    return (
        spark.read.table("src_fact_sales")
        .groupBy(
            "invoice_no",
            "customer_id",
            "customer_sk",
            "store_sk",
            "employee_sk",
            "invoice_date_key",
            "year_month",
        )
        .agg(
            F.sum("sales_amount").cast(DecimalType(18, 2)).alias("invoice_sales"),
            F.sum("gross_profit").cast(DecimalType(18, 2)).alias("invoice_profit"),
            F.sum("quantity").cast("bigint").alias("item_qty"),
            F.sum("line_cnt").cast("bigint").alias("line_total"),
        )
    )


# COMMAND ----------
def xfm_basket_metrics():
    return (
        spark.read.table("agg_invoice")
        .withColumn(
            "avg_line_value",
            F.when(F.col("line_total") > 0,
                   F.col("invoice_sales") / F.col("line_total"))
             .otherwise(F.lit(0))
             .cast(DecimalType(18, 2))
        )
        .withColumn("invoice_cnt", F.lit(1).cast("int"))
    )


# COMMAND ----------
def fact_invoice():
    return (
        spark.read.table("xfm_basket_metrics")
        .select(
            "invoice_no",
            "customer_id",
            "customer_sk",
            "store_sk",
            "employee_sk",
            "invoice_date_key",
            "year_month",
            "invoice_sales",
            "invoice_profit",
            "item_qty",
            "line_total",
            "avg_line_value",
            "invoice_cnt",
        )
        .orderBy("invoice_no")
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
