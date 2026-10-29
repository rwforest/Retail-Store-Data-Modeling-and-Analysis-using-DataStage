# Databricks notebook source
# MAGIC %md
# MAGIC # J03_Land_Products
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J03_Land_Products.txt`
# COMMAND ----------
# COMMAND ----------
# Python was assumed because the `${language}` placeholder in the prompt was not filled in.
# Lakeflow Spark Declarative Pipeline: J03_Land_Products
# Source: DataStage Parallel Job "J03_Land_Products"
# Purpose: Land product catalogue + suppliers, derive unit cost and price band.
#
# Flow:
#   src_products  -> xfm_Std_Product  \
#                                       -> jn_Product_Supplier (left outer on supplier_id)
#   src_suppliers -> xfm_Std_Supplier /      -> xfm_Supplier_Defaults -> rmd_Products (dedup on stock_code)
#       -> cpy_Products -> tgt_stg_products (all rows)
#                       -> flt_Active_Products -> tgt_stg_active_products (status = 'ACTIVE')
#
# Notes:
#  - DataStage job parameters are read from the pipeline configuration, with the original defaults.
#  - Only pSrcDir is used by this job. The other parameters are kept for parity.
#  - The two sequential-file targets (stg_products.csv, stg_active_products.csv) become
#    pipeline tables named stg_products and stg_active_products. Their location is managed by
#    the pipeline's catalog and schema, not by pStgDir.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType

# COMMAND ----------
# ---------------------------------------------------------------------------
# Job parameters (set in pipeline configuration; defaults match the DataStage job)
# ---------------------------------------------------------------------------
pSrcDir = spark.conf.get("pSrcDir", "C:/case_study/Source_Data")   # Source files directory
pStgDir = spark.conf.get("pStgDir", "C:/case_study/Staging")       # Unused: targets are pipeline tables
pRunDate = spark.conf.get("pRunDate", "2011-12-31")                # Unused in this job

# COMMAND ----------
# ---------------------------------------------------------------------------
# Source schemas (from the DataStage column definitions)
# ---------------------------------------------------------------------------
products_schema = StructType([
    StructField("StockCode", StringType(), False),
    StructField("Description", StringType(), False),
    StructField("Category", StringType(), False),
    StructField("ListPrice", DecimalType(18, 2), False),
    StructField("SupplierID", StringType(), False),
    StructField("Status", StringType(), False),
])

# COMMAND ----------
suppliers_schema = StructType([
    StructField("SupplierID", StringType(), False),
    StructField("SupplierName", StringType(), False),
    StructField("SupplierCountry", StringType(), False),
    StructField("LeadTimeDays", IntegerType(), False),
    StructField("ReliabilityScore", DecimalType(18, 2), False),
])


# COMMAND ----------
def std_products():
    src = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(products_schema)
        .load(f"{pSrcDir}/products_catalog.csv")
    )
    return src.select(
        F.upper(F.trim(F.col("StockCode"))).alias("stock_code"),
        F.when(F.length(F.trim(F.col("Description"))) == 0, F.lit("UNKNOWN"))
         .otherwise(F.upper(F.trim(F.col("Description")))).alias("product_name"),
        F.trim(F.col("Category")).alias("category"),
        F.col("ListPrice").alias("list_price"),
        F.trim(F.col("SupplierID")).alias("supplier_id"),
        F.trim(F.col("Status")).alias("status"),
        # unit_cost = ListPrice * 0.55, kept as DECIMAL(18,2)
        (F.col("ListPrice") * F.lit(0.55)).cast(DecimalType(18, 2)).alias("unit_cost"),
        # price_band: <1 LOW, <5 MID, else HIGH
        F.when(F.col("ListPrice") < 1, F.lit("LOW"))
         .when(F.col("ListPrice") < 5, F.lit("MID"))
         .otherwise(F.lit("HIGH")).alias("price_band"),
    )


# COMMAND ----------
def std_suppliers():
    src = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(suppliers_schema)
        .load(f"{pSrcDir}/suppliers.csv")
    )
    return src.select(
        F.trim(F.col("SupplierID")).alias("supplier_id"),
        F.trim(F.col("SupplierName")).alias("supplier_name"),
        F.trim(F.col("SupplierCountry")).alias("supplier_country"),
        F.col("LeadTimeDays").alias("lead_time_days"),
        F.col("ReliabilityScore").alias("reliability_score"),
    )


# COMMAND ----------
def products_with_suppliers():
    prod = spark.read.table("std_products")
    sup = spark.read.table("std_suppliers")

    joined = prod.join(sup, on="supplier_id", how="left")

    return joined.select(
        "stock_code",
        "product_name",
        "category",
        "list_price",
        "supplier_id",
        "status",
        "unit_cost",
        "price_band",
        F.coalesce(F.col("supplier_name"), F.lit("UNKNOWN")).alias("supplier_name"),
        F.coalesce(F.col("supplier_country"), F.lit("UNKNOWN")).alias("supplier_country"),
        F.coalesce(F.col("lead_time_days"), F.lit(0)).cast(IntegerType()).alias("lead_time_days"),
        F.coalesce(F.col("reliability_score"), F.lit(0)).cast(DecimalType(18, 2)).alias("reliability_score"),
    )


# COMMAND ----------
def stg_products():
    df = spark.read.table("products_with_suppliers")
    # PxRemDup keep=first. DataStage keeps the first record in input order, which has no
    # equivalent in distributed Spark. A window with row_number approximates it, ordered by
    # a generated id as a tie-breaker.
    w = Window.partitionBy("stock_code").orderBy(F.monotonically_increasing_id())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


# COMMAND ----------
def stg_active_products():
    return spark.read.table("stg_products").filter(F.col("status") == "ACTIVE")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
