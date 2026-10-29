# Databricks notebook source
# MAGIC %md
# MAGIC # J13_Mart_RETAIL_DATA_MART
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J13_Mart_RETAIL_DATA_MART.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline (Python)
# Converted from DataStage parallel job: J13_Mart_RETAIL_DATA_MART
# Purpose: build RETAIL_DATA_MART, i.e. every identified-customer (citizen/foreign) transaction
#          enriched with customer, product and store attributes.
#
# Flow: fact_sales + dim_customer -> join (customer_sk)
#       -> join dim_product (product_sk) -> join dim_store (store_sk)
#       -> filter customer_type in (citizen, foreign)
#       -> upper-case cust_name / product_name -> sort -> RETAIL_DATA_MART
#
# NOTE: the template placeholder ${language} was not filled in, so Python was assumed.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------
# ---------------------------------------------------------------------------
# Pipeline parameters (DataStage job parameters).
# Set them in the pipeline configuration. The defaults mirror the original job.
# In Databricks, point these at Unity Catalog Volumes or cloud storage
# (e.g. /Volumes/<catalog>/<schema>/<volume>/Warehouse) instead of local C:/ paths.
# ---------------------------------------------------------------------------
p_dwh_dir = spark.conf.get("pDwhDir", "C:/case_study/Warehouse")
p_mart_dir = spark.conf.get("pMartDir", "C:/case_study/Marts")  # original CSV target dir (see note on the final table)
# COMMAND ----------
# pSrcDir, pStgDir, pAuditDir, pRunDate and pEffDate are not used by this job.


# ---------------------------------------------------------------------------
# Source: fact_sales.csv (PxSequentialFile src_fact_sales, first line = column names)
# ---------------------------------------------------------------------------
fact_schema = """
    invoice_no STRING, stock_code STRING, quantity INT, invoice_ts TIMESTAMP,
    unit_price DECIMAL(18,2), customer_id INT, country STRING, store_id STRING,
    invoice_date_key INT, year_month STRING, customer_sk INT, customer_type STRING,
    loyalty_tier STRING, category STRING, product_sk INT, region STRING,
    store_sk INT, employee_sk INT, currency STRING, sales_amount DECIMAL(18,2),
    cost_amount DECIMAL(18,2), gross_profit DECIMAL(18,2), local_amount DECIMAL(18,2),
    line_cnt INT
"""

# COMMAND ----------
def src_fact_sales():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(fact_schema)
        .load(f"{p_dwh_dir}/fact_sales.csv")
    )


# COMMAND ----------
# ---------------------------------------------------------------------------
# Source: dim_customer.csv (src_dim_customer)
# ---------------------------------------------------------------------------
dim_customer_schema = """
    customer_sk INT, customer_id INT, cust_name STRING, gender STRING, age INT,
    spending_score INT, annual_income_k INT, country STRING, customer_type STRING,
    loyalty_tier STRING, email STRING, city STRING, attr_hash BIGINT, version_no INT,
    eff_start_key INT, eff_end_key INT, is_current INT
"""

# COMMAND ----------
def src_dim_customer():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(dim_customer_schema)
        .load(f"{p_dwh_dir}/dim_customer.csv")
    )


# COMMAND ----------
# ---------------------------------------------------------------------------
# Source: dim_product.csv (src_dim_product)
# ---------------------------------------------------------------------------
dim_product_schema = """
    stock_code STRING, product_name STRING, category STRING, list_price DECIMAL(18,2),
    supplier_id STRING, status STRING, unit_cost DECIMAL(18,2), price_band STRING,
    supplier_name STRING, supplier_country STRING, lead_time_days INT,
    reliability_score DECIMAL(18,2), product_sk INT, category_group STRING
"""

# COMMAND ----------
def src_dim_product():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(dim_product_schema)
        .load(f"{p_dwh_dir}/dim_product.csv")
    )


# COMMAND ----------
# ---------------------------------------------------------------------------
# Source: dim_store.csv (src_dim_store)
# ---------------------------------------------------------------------------
dim_store_schema = """
    store_id STRING, store_name STRING, country STRING, channel STRING, rating INT,
    no_of_employees INT, region STRING, emp_cnt INT, store_sk INT
"""

# COMMAND ----------
def src_dim_store():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(dim_store_schema)
        .load(f"{p_dwh_dir}/dim_store.csv")
    )


# COMMAND ----------
def jn_customer():
    fact = spark.read.table("src_fact_sales") if False else dp.read("src_fact_sales")
    cust = dp.read("src_dim_customer").select(
        "customer_sk", "cust_name", "gender", "age", "spending_score", "annual_income_k"
    )
    return fact.join(cust, on="customer_sk", how="inner")


# COMMAND ----------
def jn_product():
    prod = dp.read("src_dim_product").select("product_sk", "product_name", "list_price")
    return dp.read("jn_customer").join(prod, on="product_sk", how="inner")


# COMMAND ----------
def jn_store():
    store = dp.read("src_dim_store").select("store_sk", "store_name", "rating", "no_of_employees")
    return dp.read("jn_product").join(store, on="store_sk", how="inner")


# COMMAND ----------
def xfm_upper_names():
    return (
        dp.read("jn_store")
        .where((F.col("customer_type") == "citizen") | (F.col("customer_type") == "foreign"))
        .withColumn("cust_name", F.upper(F.col("cust_name")))
        .withColumn("product_name", F.upper(F.col("product_name")))
    )


# COMMAND ----------
def retail_data_mart():
    return (
        dp.read("xfm_upper_names")
        .select(
            "invoice_no", "stock_code", "quantity", "invoice_ts", "unit_price",
            "customer_id", "country", "store_id", "invoice_date_key", "year_month",
            "customer_sk", "customer_type", "loyalty_tier", "category", "product_sk",
            "region", "store_sk", "employee_sk", "currency", "sales_amount",
            "cost_amount", "gross_profit", "local_amount", "line_cnt",
            "cust_name", "gender", "age", "spending_score", "annual_income_k",
            "product_name", "list_price", "store_name", "rating", "no_of_employees",
        )
        .orderBy("invoice_no", "stock_code")
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
