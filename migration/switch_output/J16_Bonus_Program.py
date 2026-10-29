# Databricks notebook source
# MAGIC %md
# MAGIC # J16_Bonus_Program
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J16_Bonus_Program.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline: J16_Bonus_Program
# Converted from the DataStage parallel job J16_Bonus_Program (Q3: bonus for top-profit customers, scaled by loyalty tier).
#
# The ${language} placeholder in the request was not filled in, so this uses Python.
#
# Conversion notes:
#   - DataStage job parameters (pMartDir, pDwhDir) become pipeline configuration values read with spark.conf.get.
#     Set them in the pipeline settings. The defaults are the original DataStage defaults. In Databricks,
#     point them at a Unity Catalog Volume path, e.g. /Volumes/<catalog>/<schema>/<volume>/Marts.
#   - Intermediate DataStage links (aggregator, sort, head, filter, join, transformer, copy) become temporary views.
#   - The two sequential-file targets (Q3_Bonus_Program.csv and Q3_Bonus_Summary.csv) become
#     materialized views, because a declarative pipeline persists its outputs as datasets rather than
#     overwriting CSV files. Add a separate export step if the CSV files are still needed.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------
# ---------------------------------------------------------------------------
# Job parameters (pipeline configuration)
# ---------------------------------------------------------------------------
P_MART_DIR = spark.conf.get("pMartDir", "C:/case_study/Marts")
P_DWH_DIR = spark.conf.get("pDwhDir", "C:/case_study/Warehouse")

# COMMAND ----------
# ---------------------------------------------------------------------------
# Explicit schemas, mirroring the DataStage column definitions
# (sql=12 -> STRING, sql=4 -> INT, sql=3 -> DECIMAL(18,2), sql=11 -> TIMESTAMP, sql=-5 -> BIGINT)
# ---------------------------------------------------------------------------
MART_SCHEMA = """
    invoice_no STRING, stock_code STRING, quantity INT, invoice_ts TIMESTAMP,
    unit_price DECIMAL(18,2), customer_id INT, country STRING, store_id STRING,
    invoice_date_key INT, year_month STRING, customer_sk INT, customer_type STRING,
    loyalty_tier STRING, category STRING, product_sk INT, region STRING,
    store_sk INT, employee_sk INT, currency STRING, sales_amount DECIMAL(18,2),
    cost_amount DECIMAL(18,2), gross_profit DECIMAL(18,2), local_amount DECIMAL(18,2),
    line_cnt INT, cust_name STRING, gender STRING, age INT, spending_score INT,
    annual_income_k INT, product_name STRING, list_price DECIMAL(18,2),
    store_name STRING, rating INT, no_of_employees INT
"""

# COMMAND ----------
DIM_CUSTOMER_SCHEMA = """
    customer_sk INT, customer_id INT, cust_name STRING, gender STRING, age INT,
    spending_score INT, annual_income_k INT, country STRING, customer_type STRING,
    loyalty_tier STRING, email STRING, city STRING, attr_hash BIGINT,
    version_no INT, eff_start_key INT, eff_end_key INT, is_current INT
"""


# COMMAND ----------
def lnk_cust_profit():
    src_mart = (
        spark.read.format("csv")
        .option("header", True)  # firstLineColumnNames
        .schema(MART_SCHEMA)
        .load(f"{P_MART_DIR}/RETAIL_DATA_MART.csv")
    )
    return (
        src_mart.groupBy("customer_id")
        .agg(F.sum("gross_profit").alias("profit_total"))
    )


# COMMAND ----------
def lnk_top100():
    return (
        spark.read.table("lnk_cust_profit")
        .orderBy(F.col("profit_total").desc(), F.col("customer_id").asc())
        .limit(100)
    )


# COMMAND ----------
def lnk_cur():
    dim_customer = (
        spark.read.format("csv")
        .option("header", True)
        .schema(DIM_CUSTOMER_SCHEMA)
        .load(f"{P_DWH_DIR}/dim_customer.csv")
    )
    return dim_customer.where("is_current = 1")


# COMMAND ----------
def lnk_named():
    top100 = spark.read.table("lnk_top100")
    cur = spark.read.table("lnk_cur")
    return (
        top100.join(cur, on="customer_id", how="inner")
        .select(
            top100["customer_id"],
            top100["profit_total"],
            cur["customer_sk"],
            cur["cust_name"],
            cur["customer_type"],
            cur["loyalty_tier"],
        )
    )


# COMMAND ----------
def q3_bonus_program():
    df = spark.read.table("lnk_named")

    bonus_rate = (
        F.when(F.col("loyalty_tier") == "PLATINUM", F.lit(0.05))
        .when(F.col("loyalty_tier") == "GOLD", F.lit(0.04))
        .when(F.col("loyalty_tier") == "SILVER", F.lit(0.03))
        .otherwise(F.lit(0.02))
    )

    return (
        df.withColumn("bonus_rate", bonus_rate.cast("decimal(18,6)"))
        .withColumn(
            "bonus_amount",
            (F.col("profit_total") * bonus_rate).cast("decimal(18,2)"),
        )
        .withColumn("program", F.lit("BONUS_2011"))
        .select(
            "customer_id", "profit_total", "customer_sk", "cust_name",
            "customer_type", "loyalty_tier", "bonus_rate", "bonus_amount", "program",
        )
    )


# COMMAND ----------
def q3_bonus_summary():
    return (
        spark.read.table("q3_bonus_program")
        .groupBy("program")
        .agg(F.sum("bonus_amount").cast("decimal(18,2)").alias("bonus_total"))
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
