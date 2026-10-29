# Databricks notebook source
# MAGIC %md
# MAGIC # J23_Reconcile_Source_To_Fact
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J23_Reconcile_Source_To_Fact.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline (Python)
# Converted from DataStage parallel job: J23_Reconcile_Source_To_Fact
# Purpose: Reconcile the total staged order line amounts (after filtering) to the total Fact_Sales amount
# and write a PASS/FAIL result.
# Note: The target language was not specified, so Python is used.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
)

# COMMAND ----------
# ---------------------------------------------------------------------------
# Job parameters (DataStage parameters -> pipeline configuration values)
# Set these in the pipeline settings "configuration" section to override defaults.
# Only the directories used by this job are read, but all are kept for parity.
# ---------------------------------------------------------------------------
p_src_dir   = spark.conf.get("pSrcDir",   "C:/case_study/Source_Data")
p_stg_dir   = spark.conf.get("pStgDir",   "C:/case_study/Staging")
p_dwh_dir   = spark.conf.get("pDwhDir",   "C:/case_study/Warehouse")
p_mart_dir  = spark.conf.get("pMartDir",  "C:/case_study/Marts")
p_audit_dir = spark.conf.get("pAuditDir", "C:/case_study/Audit")
p_run_date  = spark.conf.get("pRunDate",  "2011-12-31")
p_eff_date  = spark.conf.get("pEffDate",  "2011-07-01")

# COMMAND ----------
# ---------------------------------------------------------------------------
# Explicit schemas (taken from the DataStage column definitions)
# ---------------------------------------------------------------------------
dec_18_2 = DecimalType(18, 2)

# COMMAND ----------
stg_orders_schema = StructType([
    StructField("invoice_no", StringType(), False),
    StructField("stock_code", StringType(), False),
    StructField("description", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("invoice_ts", TimestampType(), False),
    StructField("unit_price", dec_18_2, False),
    StructField("customer_id", IntegerType(), False),
    StructField("country", StringType(), False),
    StructField("store_id", StringType(), False),
    StructField("employee_id", StringType(), False),
    StructField("invoice_date_key", IntegerType(), False),
    StructField("line_amount", dec_18_2, False),
    StructField("is_cancel", IntegerType(), False),
    StructField("is_nonproduct", IntegerType(), False),
    StructField("is_guest", IntegerType(), False),
])

# COMMAND ----------
fact_sales_schema = StructType([
    StructField("invoice_no", StringType(), False),
    StructField("stock_code", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("invoice_ts", TimestampType(), False),
    StructField("unit_price", dec_18_2, False),
    StructField("customer_id", IntegerType(), False),
    StructField("country", StringType(), False),
    StructField("store_id", StringType(), False),
    StructField("invoice_date_key", IntegerType(), False),
    StructField("year_month", StringType(), False),
    StructField("customer_sk", IntegerType(), False),
    StructField("customer_type", StringType(), False),
    StructField("loyalty_tier", StringType(), False),
    StructField("category", StringType(), False),
    StructField("product_sk", IntegerType(), False),
    StructField("region", StringType(), False),
    StructField("store_sk", IntegerType(), False),
    StructField("employee_sk", IntegerType(), False),
    StructField("currency", StringType(), False),
    StructField("sales_amount", dec_18_2, False),
    StructField("cost_amount", dec_18_2, False),
    StructField("gross_profit", dec_18_2, False),
    StructField("local_amount", dec_18_2, False),
    StructField("line_cnt", IntegerType(), False),
])


# COMMAND ----------
def stg_sales_lines():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(stg_orders_schema)
        .load(f"{p_stg_dir}/stg_orders.csv")
        .where("is_cancel = 0 AND is_nonproduct = 0 AND quantity > 0")
    )


# COMMAND ----------
def recon_stage_amount():
    return (
        spark.read.table("stg_sales_lines")
        .select("line_amount", F.lit("ALL").alias("rec_key"))
        .groupBy("rec_key")
        .agg(F.sum("line_amount").cast(dec_18_2).alias("stage_amount"))
    )


# COMMAND ----------
def recon_fact_amount():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(fact_sales_schema)
        .load(f"{p_dwh_dir}/fact_sales.csv")
        .select("sales_amount", F.lit("ALL").alias("rec_key"))
        .groupBy("rec_key")
        .agg(F.sum("sales_amount").cast(dec_18_2).alias("fact_amount"))
    )


# COMMAND ----------
def audit_reconciliation():
    stage_df = spark.read.table("recon_stage_amount")
    fact_df = spark.read.table("recon_fact_amount")

    return (
        stage_df.join(fact_df, on="rec_key", how="inner")
        .select(
            "rec_key",
            "stage_amount",
            "fact_amount",
            (F.col("stage_amount") - F.col("fact_amount")).cast(dec_18_2).alias("diff_amount"),
            F.when(
                F.abs(F.col("stage_amount") - F.col("fact_amount")) < F.lit(0.01),
                F.lit("PASS")
            ).otherwise(F.lit("FAIL")).alias("status"),
        )
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
