# Databricks notebook source
# MAGIC %md
# MAGIC # J18_Mart_Product_Performance
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J18_Mart_Product_Performance.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline for DataStage job J18_Mart_Product_Performance
# Product performance mart: revenue, margin, return rate and ABC class.
#
# Conversion notes:
# - The target language was not specified, so this is written in Python.
# - DataStage sequential file sources become batch reads of the warehouse CSV files.
# - Each DataStage stage chain becomes a dataset (view or materialized view).
# - The two sequential-file targets (Product_Performance.csv and Product_Top50.csv)
#   become pipeline-managed materialized views in the target catalog and schema.
#   The pMartDir folder is therefore not needed. To also export CSV files, add a
#   separate job or task downstream of the pipeline.
# - Only pDwhDir is used by this job. The other parameters (pSrcDir, pStgDir, pMartDir,
#   pAuditDir, pRunDate, pEffDate) are not referenced by the logic.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, IntegerType

# COMMAND ----------
# ---------------------------------------------------------------------------
# Parameters (set as pipeline configuration; default matches the DataStage job)
# ---------------------------------------------------------------------------
P_DWH_DIR = spark.conf.get("pDwhDir", "C:/case_study/Warehouse")


# COMMAND ----------
def agg_product_sales():
    fact_sales = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(f"{P_DWH_DIR}/fact_sales.csv")
    )
    return (
        fact_sales
        .select(
            F.col("product_sk").cast(IntegerType()).alias("product_sk"),
            F.col("quantity").cast(IntegerType()).alias("quantity"),
            F.col("sales_amount").cast(DecimalType(18, 2)).alias("sales_amount"),
            F.col("gross_profit").cast(DecimalType(18, 2)).alias("gross_profit"),
        )
        .groupBy("product_sk")
        .agg(
            F.sum("quantity").cast(LongType()).alias("units_sold"),
            F.sum("sales_amount").cast(DecimalType(18, 2)).alias("revenue"),
            F.sum("gross_profit").cast(DecimalType(18, 2)).alias("profit"),
        )
    )


# COMMAND ----------
def agg_product_returns():
    fact_returns = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(f"{P_DWH_DIR}/fact_returns.csv")
    )
    return (
        fact_returns
        .select(
            F.col("product_sk").cast(IntegerType()).alias("product_sk"),
            F.col("returned_amount").cast(DecimalType(18, 2)).alias("returned_amount"),
        )
        .groupBy("product_sk")
        .agg(F.sum("returned_amount").cast(DecimalType(18, 2)).alias("returned"))
    )


# COMMAND ----------
def sales_with_returns():
    sales = spark.read.table("agg_product_sales")
    returns = spark.read.table("agg_product_returns")
    return (
        sales.join(returns, on="product_sk", how="left")
        .select(
            "product_sk",
            "units_sold",
            "revenue",
            "profit",
            F.coalesce(F.col("returned"), F.lit(0)).cast(DecimalType(18, 2)).alias("returned"),
        )
    )


# COMMAND ----------
def product_kpis():
    df = spark.read.table("sales_with_returns")
    return (
        df
        .withColumn(
            "return_rate",
            F.when(F.col("revenue") > 0, F.col("returned") / F.col("revenue"))
             .otherwise(F.lit(0))
             .cast(DecimalType(18, 6)),
        )
        .withColumn(
            "margin_pct",
            F.when(F.col("revenue") > 0, F.col("profit") / F.col("revenue"))
             .otherwise(F.lit(0))
             .cast(DecimalType(18, 6)),
        )
        .withColumn(
            "abc_class",
            F.when(F.col("revenue") >= 5000, F.lit("A"))
             .when(F.col("revenue") >= 1000, F.lit("B"))
             .otherwise(F.lit("C")),
        )
    )


# COMMAND ----------
def product_kpis_named():
    kpi = spark.read.table("product_kpis")
    dim_product = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(f"{P_DWH_DIR}/dim_product.csv")
        .select(
            F.col("product_sk").cast(IntegerType()).alias("product_sk"),
            "stock_code",
            "product_name",
            "category",
            "supplier_name",
        )
    )
    return kpi.join(dim_product, on="product_sk", how="inner").select(
        "product_sk",
        "units_sold",
        "revenue",
        "profit",
        "returned",
        "return_rate",
        "margin_pct",
        "abc_class",
        "stock_code",
        "product_name",
        "category",
        "supplier_name",
    )


# COMMAND ----------
def mart_product_performance():
    return spark.read.table("product_kpis_named").orderBy(F.col("revenue").desc())


# COMMAND ----------
def mart_product_top50():
    return (
        spark.read.table("product_kpis_named")
        .orderBy(F.col("revenue").desc())
        .limit(50)
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
