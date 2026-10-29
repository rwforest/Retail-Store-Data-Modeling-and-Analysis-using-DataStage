# Databricks notebook source
# MAGIC %md
# MAGIC # J02_Land_Customers
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J02_Land_Customers.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline converted from the DataStage parallel job J02_Land_Customers.
# Purpose: land the two customer snapshots (2010 and 2011), standardise attributes,
#          compute a CRC32 hash of the tracked attributes, and de-duplicate by customer_id.
#
# The ${language} placeholder was not resolved in the request, so this is written in Python.
#
# Conversion notes:
#   - The DataStage sequential-file targets (stg_customers_*.csv) become pipeline datasets
#     (materialized views) in the target catalog/schema. Downstream pipelines read them as tables.
#   - Job parameters are read from pipeline configuration, with the original defaults.
#     On Databricks, point pSrcDir at a Unity Catalog Volume or cloud path,
#     e.g. /Volumes/<catalog>/<schema>/<volume>/Source_Data.
#   - pStgDir, pDwhDir, pMartDir, pAuditDir, pRunDate and pEffDate are not used by this job.
#     They are kept for parity and for the downstream jobs.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------
# ---------------------------------------------------------------------------
# Job parameters (from pipeline configuration, with DataStage defaults)
# ---------------------------------------------------------------------------
p_src_dir = spark.conf.get("pSrcDir", "C:/case_study/Source_Data")
p_stg_dir = spark.conf.get("pStgDir", "C:/case_study/Staging")      # unused: staging is now a pipeline table
p_dwh_dir = spark.conf.get("pDwhDir", "C:/case_study/Warehouse")    # unused in this job
p_mart_dir = spark.conf.get("pMartDir", "C:/case_study/Marts")      # unused in this job
p_audit_dir = spark.conf.get("pAuditDir", "C:/case_study/Audit")    # unused in this job
p_run_date = spark.conf.get("pRunDate", "2011-12-31")               # unused in this job
p_eff_date = spark.conf.get("pEffDate", "2011-07-01")               # unused in this job

# COMMAND ----------
# ---------------------------------------------------------------------------
# Source schema (same for both snapshot files, all columns NOT NULL in DataStage)
# ---------------------------------------------------------------------------
raw_schema = StructType([
    StructField("CustomerID", IntegerType(), False),
    StructField("CustomerName", StringType(), False),
    StructField("Gender", StringType(), False),
    StructField("Age", IntegerType(), False),
    StructField("SpendingScore", IntegerType(), False),
    StructField("AnnualIncomeK", IntegerType(), False),
    StructField("Country", StringType(), False),
    StructField("CustomerType", StringType(), False),
    StructField("LoyaltyTier", StringType(), False),
    StructField("Email", StringType(), False),
    StructField("City", StringType(), False),
    StructField("SnapshotDate", StringType(), False),
])


# COMMAND ----------
def ds_trim(c):
    """
    DataStage Trim(): removes leading/trailing spaces and collapses
    repeated internal spaces to a single space.
    """
    return F.regexp_replace(F.trim(c), " +", " ")


# COMMAND ----------
def read_raw_snapshot(file_name):
    """Read a raw customer snapshot CSV (header row present), equivalent to the PxSequentialFile source."""
    return (
        spark.read.format("csv")
        .option("header", True)
        .schema(raw_schema)
        .load(f"{p_src_dir}/{file_name}")
    )


# COMMAND ----------
def standardise_and_dedup(raw_df):
    """
    Equivalent to the CTransformerStage (standardisation + CRC32 hash)
    followed by the PxRemDup stage (keep first per customer_id).
    """
    # Standardised values, reused for both output columns and the hash
    cust_name = F.upper(ds_trim(F.col("CustomerName")))
    country = ds_trim(F.col("Country"))
    loyalty_tier = F.upper(ds_trim(F.col("LoyaltyTier")))
    city = ds_trim(F.col("City"))
    email = F.lower(ds_trim(F.col("Email")))

    std_df = raw_df.select(
        F.col("CustomerID").alias("customer_id"),
        cust_name.alias("cust_name"),
        ds_trim(F.col("Gender")).alias("gender"),
        F.col("Age").alias("age"),
        F.col("SpendingScore").alias("spending_score"),
        F.col("AnnualIncomeK").alias("annual_income_k"),
        country.alias("country"),
        ds_trim(F.col("CustomerType")).alias("customer_type"),
        loyalty_tier.alias("loyalty_tier"),
        email.alias("email"),
        city.alias("city"),
        # StringToDate(..., "%yyyy-%mm-%dd")
        F.to_date(F.col("SnapshotDate"), "yyyy-MM-dd").alias("snapshot_date"),
        # CRC32 over the pipe-delimited tracked attributes (BIGINT result, as in DataStage)
        F.crc32(
            F.concat_ws(
                "|",
                cust_name,
                F.col("Age").cast("string"),
                F.col("SpendingScore").cast("string"),
                F.col("AnnualIncomeK").cast("string"),
                country,
                loyalty_tier,
                city,
                email,
            )
        ).cast("bigint").alias("attr_hash"),
    )

    # PxRemDup: key = customer_id, keep = first.
    # Spark has no inherent row order, so dropDuplicates keeps an arbitrary row per key.
    # If the source can contain duplicate customer_ids with different data,
    # add an explicit ordering (e.g. a row_number window) to make this deterministic.
    return std_df.dropDuplicates(["customer_id"])


# COMMAND ----------
def stg_customers_2010():
    return standardise_and_dedup(read_raw_snapshot("customers_snapshot_2010.csv"))


# COMMAND ----------
def stg_customers_2011():
    return standardise_and_dedup(read_raw_snapshot("customers_snapshot_2011.csv"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
