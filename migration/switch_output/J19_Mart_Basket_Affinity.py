# Databricks notebook source
# MAGIC %md
# MAGIC # J19_Mart_Basket_Affinity
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J19_Mart_Basket_Affinity.txt`
# COMMAND ----------
# COMMAND ----------
# Converted from DataStage parallel job J19_Mart_Basket_Affinity.
# The task did not specify a target language, so this is written in Python.
# Purpose: market-basket analysis. Find product pairs bought on the same invoice,
# keep pairs with at least 5 co-occurrences, and publish the top 1000 by pair_count.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
)

# COMMAND ----------
# ---------------------------------------------------------------------------
# Job parameters (DataStage parameters become pipeline configuration values).
# Defaults match the original job. Override them in the pipeline settings.
# ---------------------------------------------------------------------------
pDwhDir  = spark.conf.get("pDwhDir",  "C:/case_study/Warehouse")
pMartDir = spark.conf.get("pMartDir", "C:/case_study/Marts")  # target dir of the original CSV output

# COMMAND ----------
# Business rule constants taken from the original filter stages
MIN_SUPPORT = 5     # flt_Min_Support: pair_count >= 5
TOP_N       = 1000  # hd_Top1000: keep the first 1000 rows

# COMMAND ----------
# Schema of fact_sales.csv (from the src_fact_sales column definitions)
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
def basket_lines_prep():
    return (
        spark.read
        .format("csv")
        .option("header", True)  # firstLineColumnNames
        .schema(fact_sales_schema)
        .load(f"{pDwhDir}/fact_sales.csv")
        .where("quantity > 0")                    # flt_Positive_Lines
        .select("invoice_no", "stock_code")       # xfm_Pair_Prep
    )


# COMMAND ----------
def basket_dedup():
    return spark.read.table("basket_lines_prep").dropDuplicates(["invoice_no", "stock_code"])


# COMMAND ----------
def basket_pairs():
    a = spark.read.table("basket_dedup").alias("a")
    b = (
        spark.read.table("basket_dedup")
        .select("invoice_no", F.col("stock_code").alias("stock_code_b"))
        .alias("b")
    )
    return (
        a.join(b, on="invoice_no", how="inner")            # jn_Self
        .where(F.col("stock_code") < F.col("stock_code_b"))  # flt_Ordered_Pairs
        .select("invoice_no", "stock_code", "stock_code_b")
    )


# COMMAND ----------
def mart_basket_affinity():
    return (
        spark.read.table("basket_pairs")
        .groupBy("stock_code", "stock_code_b")
        .agg(F.count("*").cast("int").alias("pair_count"))   # agg_Pair_Counts
        .where(F.col("pair_count") >= MIN_SUPPORT)           # flt_Min_Support
        .orderBy(F.col("pair_count").desc())                 # srt_Pairs (desc)
        .limit(TOP_N)                                        # hd_Top1000
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
