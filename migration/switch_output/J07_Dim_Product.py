# Databricks notebook source
# MAGIC %md
# MAGIC # J07_Dim_Product
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J07_Dim_Product.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline (Python) equivalent of DataStage job J07_Dim_Product.
# The requested ${language} placeholder was not filled in, so I used Python.
# Purpose: build Dim_Product with a surrogate key and a category group, plus an audit count of products per category.
#
# Flow: stg_products.csv -> remove duplicates (stock_code) -> sort -> add product_sk and category_group
#       -> dim_product, and -> audit_products_by_category (count per category)
#
# Notes:
# - The DataStage job writes CSV files (dim_product.csv, audit_products_by_category.csv).
#   In a declarative pipeline the outputs are pipeline-managed datasets (materialized views),
#   so the file sinks become datasets named dim_product and audit_products_by_category.
# - The intermediate stages (PxRemDup, PxSort, two Transformers, PxCopy) are collapsed into
#   one logical step each, because no logic is lost.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, IntegerType
)

# COMMAND ----------
# ---------------------------------------------------------------------------
# Job parameters (read from pipeline configuration, with the DataStage defaults)
# Only pStgDir is used by this job. The other parameters are kept for reference.
# ---------------------------------------------------------------------------
pSrcDir   = spark.conf.get("pSrcDir",   "C:/case_study/Source_Data")
pStgDir   = spark.conf.get("pStgDir",   "C:/case_study/Staging")
pDwhDir   = spark.conf.get("pDwhDir",   "C:/case_study/Warehouse")
pMartDir  = spark.conf.get("pMartDir",  "C:/case_study/Marts")
pAuditDir = spark.conf.get("pAuditDir", "C:/case_study/Audit")
pRunDate  = spark.conf.get("pRunDate",  "2011-12-31")
pEffDate  = spark.conf.get("pEffDate",  "2011-07-01")

# COMMAND ----------
# Schema of stg_products.csv (matches the column definitions of src_stg_products)
stg_products_schema = StructType([
    StructField("stock_code",        StringType(),       False),
    StructField("product_name",      StringType(),       False),
    StructField("category",          StringType(),       False),
    StructField("list_price",        DecimalType(18, 2), False),
    StructField("supplier_id",       StringType(),       False),
    StructField("status",            StringType(),       False),
    StructField("unit_cost",         DecimalType(18, 2), False),
    StructField("price_band",        StringType(),       False),
    StructField("supplier_name",     StringType(),       False),
    StructField("supplier_country",  StringType(),       False),
    StructField("lead_time_days",    IntegerType(),      False),
    StructField("reliability_score", DecimalType(18, 2), False),
])


# COMMAND ----------
def stg_products():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(stg_products_schema)
        .load(f"{pStgDir}/stg_products.csv")
    )


# COMMAND ----------
def dim_product():
    # Remove duplicates, keeping one row per stock_code (PxRemDup)
    deduped = spark.read.table("stg_products").dropDuplicates(["stock_code"])

    # Surrogate key: the DataStage expression
    #   @PARTITIONNUM + (@NUMPARTITIONS * (@INROWNUM - 1)) + 1
    # yields a unique, sequential number 1..N across the sorted rows.
    # It is reproduced deterministically with row_number over stock_code (the sort key).
    sk_window = Window.orderBy("stock_code")

    # Category group: Seasonal -> SEASONAL, General Merchandise -> OTHER, anything else -> CORE
    category_group = (
        F.when(F.col("category") == "Seasonal", F.lit("SEASONAL"))
         .when(F.col("category") == "General Merchandise", F.lit("OTHER"))
         .otherwise(F.lit("CORE"))
    )

    return (
        deduped
        .withColumn("product_sk", F.row_number().over(sk_window).cast("int"))
        .withColumn("category_group", category_group)
        .select(
            "stock_code", "product_name", "category", "list_price",
            "supplier_id", "status", "unit_cost", "price_band",
            "supplier_name", "supplier_country", "lead_time_days",
            "reliability_score", "product_sk", "category_group",
        )
        .orderBy("stock_code")
    )


# COMMAND ----------
def audit_products_by_category():
    return (
        spark.read.table("dim_product")
        .groupBy("category")
        .agg(F.count(F.lit(1)).cast("int").alias("products"))
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
