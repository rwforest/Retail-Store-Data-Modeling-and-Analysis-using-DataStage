# Databricks notebook source
# MAGIC %md
# MAGIC # J15_Analysis_CustomerType_Profit
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J15_Analysis_CustomerType_Profit.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline (Python) converted from the DataStage job J15_Analysis_CustomerType_Profit.
# Purpose: Q2 - which customer type (citizen / foreign) generates the highest profit.
#
# Original flow:
#   src_mart -> agg_By_Type -> srt_Profit -> cpy_Ranked -+-> tgt_customer_type_ranking (CSV)
#                                                        +-> hd_Winner -> tgt_customer_type_winner (CSV)
#
# Conversion notes:
#   - The DataStage job parameters are read from the pipeline configuration (spark.conf),
#     with the same defaults as the original job. Only pMartDir is used by this job.
#   - The Copy stage is not needed. Two downstream datasets read from the same upstream dataset.
#   - The sequential CSV targets become pipeline datasets (materialized views) in the
#     target catalog/schema. Add a separate export step if CSV files are still required.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
)

# COMMAND ----------
# ---------------------------------------------------------------------------
# Job parameters (set in the pipeline configuration to override the defaults)
# ---------------------------------------------------------------------------
p_mart_dir = spark.conf.get("pMartDir", "C:/case_study/Marts")

# COMMAND ----------
# ---------------------------------------------------------------------------
# Explicit schema of the data mart file (matches the DataStage column definitions)
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
def src_mart():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(mart_schema)
        .load(f"{p_mart_dir}/RETAIL_DATA_MART.csv")
    )


# COMMAND ----------
def customer_type_ranking():
    return (
        spark.read.table("src_mart")
        .groupBy("customer_type")
        .agg(
            F.sum("gross_profit").cast("decimal(18,2)").alias("profit_total"),
            F.sum("sales_amount").cast("decimal(18,2)").alias("sales_total"),
            F.sum("line_cnt").cast("bigint").alias("lines"),
        )
        # srt_Profit: sort by profit_total descending
        .orderBy(F.col("profit_total").desc())
    )


# COMMAND ----------
def winning_customer_type():
    return (
        spark.read.table("customer_type_ranking")
        .orderBy(F.col("profit_total").desc())
        .limit(1)
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
