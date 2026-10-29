# Databricks notebook source
# MAGIC %md
# MAGIC # J20_Mart_Store_Monthly
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J20_Mart_Store_Monthly.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline (Python) for DataStage job J20_Mart_Store_Monthly
# Purpose: Store x month sales KPIs, including sales per employee and margin %.
#
# Assumption: the target language was not specified, so this is written in Python.
#
# Flow:
#   fact_sales.csv -> aggregate by store_sk, year_month -> inner join dim_store.csv
#   -> derive KPIs -> sort -> mart table (Store_Monthly)
#
# Pipeline configuration parameters (set in the pipeline settings; defaults match the DataStage job):
#   pDwhDir  : warehouse directory with fact_sales.csv and dim_store.csv
#   pMartDir : mart output directory (see note at the end)

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
)

# COMMAND ----------
# ---------------------------------------------------------------------------
# Parameters (DataStage job parameters)
# ---------------------------------------------------------------------------
p_dwh_dir = spark.conf.get("pDwhDir", "C:/case_study/Warehouse")
p_mart_dir = spark.conf.get("pMartDir", "C:/case_study/Marts")  # kept for reference / export

# COMMAND ----------
# ---------------------------------------------------------------------------
# Schemas (taken from the DataStage column definitions, all columns NOT NULL)
# ---------------------------------------------------------------------------
fact_sales_schema = StructType([
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
])

# COMMAND ----------
dim_store_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("store_name", StringType(), False),
    StructField("country", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("rating", IntegerType(), False),
    StructField("no_of_employees", IntegerType(), False),
    StructField("region", StringType(), False),
    StructField("emp_cnt", IntegerType(), False),
    StructField("store_sk", IntegerType(), False),
])


# COMMAND ----------
def src_fact_sales():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(fact_sales_schema)
        .load(f"{p_dwh_dir}/fact_sales.csv")
    )


# COMMAND ----------
def agg_store_month():
    return (
        spark.read.table("src_fact_sales")
        .groupBy("store_sk", "year_month")
        .agg(
            F.sum("sales_amount").cast("decimal(18,2)").alias("sales"),
            F.sum("gross_profit").cast("decimal(18,2)").alias("profit"),
            F.sum("line_cnt").cast("bigint").alias("lines"),
        )
    )


# COMMAND ----------
def src_dim_store():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(dim_store_schema)
        .load(f"{p_dwh_dir}/dim_store.csv")
    )


# COMMAND ----------
def jn_store():
    sm = spark.read.table("agg_store_month").alias("sm")
    ds = spark.read.table("src_dim_store").alias("ds")
    return (
        sm.join(ds, on="store_sk", how="inner")
        .select(
            F.col("store_sk"),
            F.col("sm.year_month").alias("year_month"),
            F.col("sm.sales").alias("sales"),
            F.col("sm.profit").alias("profit"),
            F.col("sm.lines").alias("lines"),
            F.col("ds.store_id").alias("store_id"),
            F.col("ds.store_name").alias("store_name"),
            F.col("ds.no_of_employees").alias("no_of_employees"),
            F.col("ds.region").alias("region"),
        )
    )


# COMMAND ----------
def mart_store_monthly():
    df = spark.read.table("jn_store")
    return (
        df.withColumn(
            "sales_per_employee",
            F.when(F.col("no_of_employees") > 0, F.col("sales") / F.col("no_of_employees"))
            .otherwise(F.lit(0))
            .cast("decimal(18,2)"),
        )
        .withColumn(
            "margin_pct",
            F.when(F.col("sales") > 0, F.col("profit") / F.col("sales"))
            .otherwise(F.lit(0))
            .cast("decimal(18,6)"),
        )
        .select(
            "store_sk", "year_month", "sales", "profit", "lines",
            "store_id", "store_name", "no_of_employees", "region",
            "sales_per_employee", "margin_pct",
        )
        # Sort as in the original job (store_sk, year_month)
        .orderBy("store_sk", "year_month")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
