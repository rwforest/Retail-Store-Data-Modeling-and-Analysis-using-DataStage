# Databricks notebook source
# MAGIC %md
# MAGIC # J05_Dim_Customer_Initial
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J05_Dim_Customer_Initial.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline: J05_Dim_Customer_Initial
# Initial load of Dim_Customer (SCD2 version 1) from the 2010 customer snapshot.
#
# Original DataStage flow:
#   src_stg_customers_2010 (CSV) -> xfm_Version1 (Transformer) -> srt_Dim_Customer (Sort) -> tgt_dim_customer (CSV)
#
# Conversion notes:
#   - The sequential file source becomes a batch read of the staging CSV.
#   - The Transformer becomes a view that derives the SCD2 columns.
#   - The Sort stage becomes an orderBy on (customer_id, version_no).
#   - The sequential file target (overwrite) becomes a materialized view,
#     which is fully recomputed on each update. This matches the overwrite semantics of an initial load.
#   - Only the job parameters used by this job (pStgDir, pDwhDir) are carried over.
#     The others (pSrcDir, pMartDir, pAuditDir, pRunDate, pEffDate) are not used here.
#     pDwhDir is no longer needed because the output is a pipeline-managed table, not a file.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DateType, LongType
)

# COMMAND ----------
# Job parameters, exposed as pipeline configuration values (with the original defaults)
p_stg_dir = spark.conf.get("pStgDir", "C:/case_study/Staging")

# COMMAND ----------
# Explicit schema of the staging file (matches the column definitions of src_stg_customers_2010)
stg_customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("cust_name", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("spending_score", IntegerType(), False),
    StructField("annual_income_k", IntegerType(), False),
    StructField("country", StringType(), False),
    StructField("customer_type", StringType(), False),
    StructField("loyalty_tier", StringType(), False),
    StructField("email", StringType(), False),
    StructField("city", StringType(), False),
    StructField("snapshot_date", DateType(), False),
    StructField("attr_hash", LongType(), False),
])


# COMMAND ----------
def stg_customers_2010():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(stg_customers_schema)
        .load(f"{p_stg_dir}/stg_customers_2010.csv")
    )


# COMMAND ----------
def dim_customer_v1():
    stg = spark.read.table("stg_customers_2010")
    return stg.select(
        (F.col("customer_id") * 1000 + 1).cast("int").alias("customer_sk"),
        F.col("customer_id"),
        F.col("cust_name"),
        F.col("gender"),
        F.col("age"),
        F.col("spending_score"),
        F.col("annual_income_k"),
        F.col("country"),
        F.col("customer_type"),
        F.col("loyalty_tier"),
        F.col("email"),
        F.col("city"),
        F.col("attr_hash"),
        F.lit(1).cast("int").alias("version_no"),
        F.lit(19000101).cast("int").alias("eff_start_key"),
        F.lit(99991231).cast("int").alias("eff_end_key"),
        F.lit(1).cast("int").alias("is_current"),
    )


# COMMAND ----------
def dim_customer():
    return (
        spark.read.table("dim_customer_v1")
        .orderBy("customer_id", "version_no")
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
