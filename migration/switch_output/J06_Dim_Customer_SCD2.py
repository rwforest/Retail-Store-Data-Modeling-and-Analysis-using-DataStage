# Databricks notebook source
# MAGIC %md
# MAGIC # J06_Dim_Customer_SCD2
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J06_Dim_Customer_SCD2.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline (Python) for DataStage job J06_Dim_Customer_SCD2.
# Applies the 2011 customer snapshot to Dim_Customer as SCD Type 2:
#   - expires the current row of customers whose attribute hash changed
#   - adds a new current version for new or changed customers
#   - carries over unchanged current rows and all historical rows
#
# Notes on the conversion:
#   * DataStage parameters become pipeline configuration values read with spark.conf.get().
#   * The original job reads dim_customer.csv and writes dim_customer_next.csv.
#     A pipeline cannot read and overwrite the same dataset, so the existing dimension
#     stays an external CSV input and the result is published as the dataset dim_customer_next.
#   * The audit CSV output becomes the dataset audit_scd2_customer.
#   * No language was specified in the request, so the output is in Python.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType, DateType
)

# COMMAND ----------
# ---------------------------------------------------------------------------
# Parameters (DataStage job parameters -> pipeline configuration)
# ---------------------------------------------------------------------------
pStgDir = spark.conf.get("pStgDir", "C:/case_study/Staging")
pDwhDir = spark.conf.get("pDwhDir", "C:/case_study/Warehouse")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")  # kept for parity, unused by this job
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# ---------------------------------------------------------------------------
# Schemas (taken from the DataStage column definitions)
# ---------------------------------------------------------------------------
dim_schema = StructType([
    StructField("customer_sk", IntegerType(), False),
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
    StructField("attr_hash", LongType(), False),
    StructField("version_no", IntegerType(), False),
    StructField("eff_start_key", IntegerType(), False),
    StructField("eff_end_key", IntegerType(), False),
    StructField("is_current", IntegerType(), False),
])

# COMMAND ----------
snap_schema = StructType([
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
# Column lists used for the old_ prefixing and for building output rows
DIM_COLS = [
    "customer_sk", "customer_id", "cust_name", "gender", "age", "spending_score",
    "annual_income_k", "country", "customer_type", "loyalty_tier", "email", "city",
    "attr_hash", "version_no", "eff_start_key", "eff_end_key", "is_current",
]


# COMMAND ----------
def src_dim_customer():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(dim_schema)
        .load(f"{pDwhDir}/dim_customer.csv")
    )


# COMMAND ----------
def src_stg_customers_2011():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(snap_schema)
        .load(f"{pStgDir}/stg_customers_2011.csv")
    )


# COMMAND ----------
def dim_current_old():
    cur = spark.read.table("src_dim_customer").where("is_current = 1")
    return cur.select(
        "customer_id",
        *[F.col(c).alias(f"old_{c}") for c in DIM_COLS if c != "customer_id"],
    )


# COMMAND ----------
def dim_history():
    return spark.read.table("src_dim_customer").where("is_current = 0")


# COMMAND ----------
def detected_changes():
    snap = spark.read.table("src_stg_customers_2011")
    old = spark.read.table("dim_current_old")
    joined = snap.join(old, on="customer_id", how="left")

    flag = (
        F.when(F.col("old_attr_hash").isNull(), F.lit("N"))
        .when(F.col("old_attr_hash") != F.col("attr_hash"), F.lit("C"))
        .otherwise(F.lit("U"))
    )

    # Defaults: 0 for numeric columns, empty string for string columns
    string_old = ["old_cust_name", "old_gender", "old_country", "old_customer_type",
                  "old_loyalty_tier", "old_email", "old_city"]
    numeric_old = ["old_customer_sk", "old_age", "old_spending_score",
                   "old_annual_income_k", "old_attr_hash", "old_version_no",
                   "old_eff_start_key", "old_eff_end_key", "old_is_current"]

    out = joined.withColumn("change_flag", flag)
    for c in string_old:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit("")))
    for c in numeric_old:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
    return out


# COMMAND ----------
def carry_unchanged():
    d = spark.read.table("detected_changes").where(F.col("change_flag") == "U")
    return d.select(
        F.col("old_customer_sk").alias("customer_sk"),
        "customer_id",
        F.col("old_cust_name").alias("cust_name"),
        F.col("old_gender").alias("gender"),
        F.col("old_age").alias("age"),
        F.col("old_spending_score").alias("spending_score"),
        F.col("old_annual_income_k").alias("annual_income_k"),
        F.col("old_country").alias("country"),
        F.col("old_customer_type").alias("customer_type"),
        F.col("old_loyalty_tier").alias("loyalty_tier"),
        F.col("old_email").alias("email"),
        F.col("old_city").alias("city"),
        F.col("old_attr_hash").alias("attr_hash"),
        F.col("old_version_no").alias("version_no"),
        F.col("old_eff_start_key").alias("eff_start_key"),
        F.col("old_eff_end_key").alias("eff_end_key"),
        F.col("old_is_current").alias("is_current"),
    )


# COMMAND ----------
def expire_old():
    d = spark.read.table("detected_changes").where(F.col("change_flag") == "C")
    # DateFromDaysSince(-1, pEffDate) -> day before the effective date, formatted yyyyMMdd
    expire_key = F.date_format(
        F.date_sub(F.to_date(F.lit(pEffDate), "yyyy-MM-dd"), 1), "yyyyMMdd"
    ).cast("int")
    return d.select(
        F.col("old_customer_sk").alias("customer_sk"),
        "customer_id",
        F.col("old_cust_name").alias("cust_name"),
        F.col("old_gender").alias("gender"),
        F.col("old_age").alias("age"),
        F.col("old_spending_score").alias("spending_score"),
        F.col("old_annual_income_k").alias("annual_income_k"),
        F.col("old_country").alias("country"),
        F.col("old_customer_type").alias("customer_type"),
        F.col("old_loyalty_tier").alias("loyalty_tier"),
        F.col("old_email").alias("email"),
        F.col("old_city").alias("city"),
        F.col("old_attr_hash").alias("attr_hash"),
        F.col("old_version_no").alias("version_no"),
        F.col("old_eff_start_key").alias("eff_start_key"),
        expire_key.alias("eff_end_key"),
        F.lit(0).alias("is_current"),
    )


# COMMAND ----------
def new_versions():
    d = spark.read.table("detected_changes").where(F.col("change_flag") != "U")
    eff_start_int = F.lit(int(pEffDate.replace("-", "")))
    return d.select(
        (F.col("customer_id") * 1000 + F.col("old_version_no") + 1)
        .cast("int").alias("customer_sk"),
        "customer_id",
        "cust_name",
        "gender",
        "age",
        "spending_score",
        "annual_income_k",
        "country",
        "customer_type",
        "loyalty_tier",
        "email",
        "city",
        "attr_hash",
        (F.col("old_version_no") + 1).cast("int").alias("version_no"),
        F.when(F.col("old_version_no") == 0, F.lit(19000101))
        .otherwise(eff_start_int).cast("int").alias("eff_start_key"),
        F.lit(99991231).alias("eff_end_key"),
        F.lit(1).alias("is_current"),
    )


# COMMAND ----------
def dim_customer_next():
    history = spark.read.table("dim_history").select(*DIM_COLS)
    carry = spark.read.table("carry_unchanged").select(*DIM_COLS)
    expired = spark.read.table("expire_old").select(*DIM_COLS)
    newver = spark.read.table("new_versions").select(*DIM_COLS)

    return (
        history.unionByName(carry)
        .unionByName(expired)
        .unionByName(newver)
        .orderBy("customer_id", "version_no")
    )


# COMMAND ----------
def audit_scd2_customer():
    return (
        spark.read.table("detected_changes")
        .groupBy("change_flag")
        .agg(F.count(F.lit(1)).cast("int").alias("customers"))
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
