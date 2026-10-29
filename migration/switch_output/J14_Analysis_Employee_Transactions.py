# Databricks notebook source
# MAGIC %md
# MAGIC # J14_Analysis_Employee_Transactions
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J14_Analysis_Employee_Transactions.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline (Python) converted from DataStage job J14_Analysis_Employee_Transactions
# Purpose: Q1 - number of transactions per employee in each store.
# Note: the target language was not specified, so Python is used.
#
# Pipeline parameters are read from the pipeline configuration (Settings > Configuration),
# with the DataStage defaults as fallbacks.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, TimestampType, DateType
)

# COMMAND ----------
# ---------------------------------------------------------------------------
# Parameters (DataStage job parameters)
# ---------------------------------------------------------------------------
p_dwh_dir  = spark.conf.get("pDwhDir",  "C:/case_study/Warehouse")  # dimensions/facts directory
p_mart_dir = spark.conf.get("pMartDir", "C:/case_study/Marts")      # data mart directory
# COMMAND ----------
# pSrcDir, pStgDir, pAuditDir, pRunDate and pEffDate are not used by this job.

# ---------------------------------------------------------------------------
# Explicit schemas, mirroring the DataStage column definitions
# ---------------------------------------------------------------------------
mart_schema = StructType([
    StructField("invoice_no", StringType(), False),
    StructField("stock_code", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("invoice_ts", TimestampType(), False),
    StructField("unit_price", DecimalType(18, 2), False),
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
    StructField("sales_amount", DecimalType(18, 2), False),
    StructField("cost_amount", DecimalType(18, 2), False),
    StructField("gross_profit", DecimalType(18, 2), False),
    StructField("local_amount", DecimalType(18, 2), False),
    StructField("line_cnt", IntegerType(), False),
    StructField("cust_name", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("spending_score", IntegerType(), False),
    StructField("annual_income_k", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("list_price", DecimalType(18, 2), False),
    StructField("store_name", StringType(), False),
    StructField("rating", IntegerType(), False),
    StructField("no_of_employees", IntegerType(), False),
])

# COMMAND ----------
dim_employee_schema = StructType([
    StructField("employee_id", StringType(), False),
    StructField("store_id", StringType(), False),
    StructField("employee_name", StringType(), False),
    StructField("role", StringType(), False),
    StructField("hire_date", DateType(), False),
    StructField("tenure_years", IntegerType(), False),
    StructField("store_sk", IntegerType(), False),
    StructField("employee_sk", IntegerType(), False),
])


# COMMAND ----------
def src_mart():
    return (
        spark.read.format("csv")
        .option("header", "true")  # firstLineColumnNames
        .schema(mart_schema)
        .load(f"{p_mart_dir}/RETAIL_DATA_MART.csv")
    )


# COMMAND ----------
def rmd_invoices():
    return spark.read.table("src_mart").dropDuplicates(["store_sk", "employee_sk", "invoice_no"])


# COMMAND ----------
def agg_transactions():
    return (
        spark.read.table("rmd_invoices")
        .groupBy("store_sk", "employee_sk")
        .agg(F.count(F.lit(1)).cast("int").alias("total_transactions"))
    )


# COMMAND ----------
def src_dim_employee():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(dim_employee_schema)
        .load(f"{p_dwh_dir}/dim_employee.csv")
    )


# COMMAND ----------
def jn_employee():
    counts = spark.read.table("agg_transactions").alias("c")
    emp = spark.read.table("src_dim_employee").alias("e")
    return (
        counts.join(emp, F.col("c.employee_sk") == F.col("e.employee_sk"), "inner")
        .select(
            F.col("c.store_sk").alias("store_sk"),
            F.col("c.employee_sk").alias("employee_sk"),
            F.col("c.total_transactions").alias("total_transactions"),
            F.col("e.employee_id").alias("employee_id"),
            F.col("e.employee_name").alias("employee_name"),
            F.col("e.role").alias("role"),
        )
    )


# COMMAND ----------
def srt_store_rank():
    return spark.read.table("jn_employee").orderBy(
        F.col("store_sk").asc(), F.col("total_transactions").desc()
    )


# COMMAND ----------
def q1_employee_transactions():
    return spark.read.table("srt_store_rank").orderBy(
        F.col("store_sk").asc(), F.col("total_transactions").desc()
    )


# COMMAND ----------
def q1_top10_employees():
    return (
        spark.read.table("srt_store_rank")
        .orderBy(F.col("store_sk").asc(), F.col("total_transactions").desc())
        .limit(10)
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
