# Databricks notebook source
# MAGIC %md
# MAGIC # J11_Fact_Returns
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J11_Fact_Returns.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline: J11_Fact_Returns
# Source: DataStage parallel job "Fact_Returns from cancellation invoices (prefix C)"
#
# Data flow:
#   stg_orders -> filter cancels -> return measures -> left join dim_product (defaults)
#   -> left join current dim_customer (defaults) -> sort -> fact_returns
#                                                       -> aggregate by category -> audit_returns_by_category
#
# Notes:
#   - The DataStage job parameters become pipeline configuration values, read with spark.conf.get.
#     Set them in the pipeline settings (for example "pStgDir") to override the defaults.
#   - The sequential-file targets (fact_returns.csv, audit_returns_by_category.csv) are modeled
#     as materialized views managed by the pipeline.
#   - The unused parameters (pSrcDir, pMartDir, pRunDate, pEffDate) are kept for reference only.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------
# ---------------------------------------------------------------------------
# Job parameters (pipeline configuration with the DataStage defaults)
# ---------------------------------------------------------------------------
pSrcDir   = spark.conf.get("pSrcDir",   "C:/case_study/Source_Data")
pStgDir   = spark.conf.get("pStgDir",   "C:/case_study/Staging")
pDwhDir   = spark.conf.get("pDwhDir",   "C:/case_study/Warehouse")
pMartDir  = spark.conf.get("pMartDir",  "C:/case_study/Marts")
pAuditDir = spark.conf.get("pAuditDir", "C:/case_study/Audit")
pRunDate  = spark.conf.get("pRunDate",  "2011-12-31")
pEffDate  = spark.conf.get("pEffDate",  "2011-07-01")

# COMMAND ----------
# ---------------------------------------------------------------------------
# Explicit schemas matching the DataStage column definitions
# ---------------------------------------------------------------------------
ORDERS_SCHEMA = """
    invoice_no STRING, stock_code STRING, description STRING, quantity INT,
    invoice_ts TIMESTAMP, unit_price DECIMAL(18,2), customer_id INT, country STRING,
    store_id STRING, employee_id STRING, invoice_date_key INT, line_amount DECIMAL(18,2),
    is_cancel INT, is_nonproduct INT, is_guest INT
"""

# COMMAND ----------
DIM_PRODUCT_SCHEMA = """
    stock_code STRING, product_name STRING, category STRING, list_price DECIMAL(18,2),
    supplier_id STRING, status STRING, unit_cost DECIMAL(18,2), price_band STRING,
    supplier_name STRING, supplier_country STRING, lead_time_days INT,
    reliability_score DECIMAL(18,2), product_sk INT, category_group STRING
"""

# COMMAND ----------
DIM_CUSTOMER_SCHEMA = """
    customer_sk INT, customer_id INT, cust_name STRING, gender STRING, age INT,
    spending_score INT, annual_income_k INT, country STRING, customer_type STRING,
    loyalty_tier STRING, email STRING, city STRING, attr_hash BIGINT, version_no INT,
    eff_start_key INT, eff_end_key INT, is_current INT
"""

# COMMAND ----------
def src_stg_orders():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(ORDERS_SCHEMA)
        .load(f"{pStgDir}/stg_orders.csv")
    )

# COMMAND ----------
def flt_cancels():
    return spark.read.table("src_stg_orders").where("is_cancel = 1 AND is_nonproduct = 0")

# COMMAND ----------
def xfm_return_measures():
    return spark.read.table("flt_cancels").select(
        "invoice_no",
        "stock_code",
        "customer_id",
        "country",
        "invoice_date_key",
        F.date_format("invoice_ts", "yyyyMM").alias("year_month"),
        F.abs("quantity").alias("returned_qty"),
        F.abs("line_amount").alias("returned_amount"),
    )

# COMMAND ----------
def src_dim_product():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(DIM_PRODUCT_SCHEMA)
        .load(f"{pDwhDir}/dim_product.csv")
    )

# COMMAND ----------
def xfm_product_defaults():
    ret = spark.read.table("xfm_return_measures")
    prod = spark.read.table("src_dim_product").select("stock_code", "category", "product_sk")
    joined = ret.join(prod, on="stock_code", how="left")
    return joined.select(
        "invoice_no",
        "stock_code",
        "customer_id",
        "country",
        "invoice_date_key",
        "year_month",
        "returned_qty",
        "returned_amount",
        F.coalesce(F.col("category"), F.lit("Unknown")).alias("category"),
        F.coalesce(F.col("product_sk"), F.lit(-1)).alias("product_sk"),
    )

# COMMAND ----------
def flt_current_customer():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(DIM_CUSTOMER_SCHEMA)
        .load(f"{pDwhDir}/dim_customer.csv")
        .where("is_current = 1")
    )

# COMMAND ----------
def xfm_customer_defaults():
    base = spark.read.table("xfm_product_defaults")
    cust = spark.read.table("flt_current_customer").select("customer_id", "customer_sk")
    joined = base.join(cust, on="customer_id", how="left")
    return joined.select(
        "invoice_no",
        "stock_code",
        "customer_id",
        "country",
        "invoice_date_key",
        "year_month",
        "returned_qty",
        "returned_amount",
        "category",
        "product_sk",
        F.coalesce(F.col("customer_sk"), F.lit(-1)).alias("customer_sk"),
    )

# COMMAND ----------
def fact_returns():
    return spark.read.table("xfm_customer_defaults").orderBy("invoice_no", "stock_code")

# COMMAND ----------
def audit_returns_by_category():
    return (
        spark.read.table("fact_returns")
        .groupBy("category")
        .agg(F.sum("returned_amount").alias("returned_amount"))
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
