# Databricks notebook source
# MAGIC %md
# MAGIC # J10_Fact_Sales
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J10_Fact_Sales.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline: J10_Fact_Sales
# Converted from DataStage parallel job J10_Fact_Sales (\\Retail_DW\\Pipeline\\3_Facts)
# Note: the target language placeholder was not filled in, so this is written in Python.
#
# Purpose: build Fact_Sales at invoice-line grain, with a point-in-time (SCD2) customer lookup.
#
# Flow:
#   stg_orders -> filter valid sales lines -> add year_month
#     -> left join dim_customer (point-in-time filter) -> defaults
#     -> left join dim_product -> defaults
#     -> inner join dim_store -> inner join dim_employee
#     -> left join dim_fx -> defaults
#     -> compute measures -> sort -> fact_sales
#   fact_sales -> dq_orphan_products (product_sk = -1)
#   fact_sales -> audit_fact_by_month (sum of sales_amount and gross_profit by year_month)
#
# Mapping of DataStage stages to this pipeline:
#   PxSequentialFile (source)     -> temporary views reading CSV
#   PxFilter / CTransformerStage  -> filter / withColumn / coalesce
#   PxJoin                        -> DataFrame joins
#   PxSort                        -> orderBy
#   PxCopy                        -> fan-out, since multiple datasets read the same fact_sales table
#   PxAggregator                  -> groupBy().agg()
#   PxSequentialFile (target)     -> pipeline tables (materialized views)

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------
# ---------------------------------------------------------------------------
# Job parameters (set them in the pipeline configuration; the defaults match the DataStage job)
# ---------------------------------------------------------------------------
p_stg_dir = spark.conf.get("pStgDir", "C:/case_study/Staging")
p_dwh_dir = spark.conf.get("pDwhDir", "C:/case_study/Warehouse")
# COMMAND ----------
# pSrcDir, pMartDir, pRunDate and pEffDate are declared in the job but never used by this job's stages.
# pAuditDir is not needed either, because audit outputs are written as pipeline tables.


def read_csv(path, ddl_schema):
    """Read a CSV file with a header row, using an explicit schema (columns are matched by position)."""
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(ddl_schema)
        .load(path)
    )


# COMMAND ----------
def src_stg_orders():
    schema = """
        invoice_no STRING, stock_code STRING, description STRING, quantity INT,
        invoice_ts TIMESTAMP, unit_price DECIMAL(18,2), customer_id INT, country STRING,
        store_id STRING, employee_id STRING, invoice_date_key INT, line_amount DECIMAL(18,2),
        is_cancel INT, is_nonproduct INT, is_guest INT
    """
    return read_csv(f"{p_stg_dir}/stg_orders.csv", schema)


# COMMAND ----------
def src_dim_customer():
    schema = """
        customer_sk INT, customer_id INT, cust_name STRING, gender STRING, age INT,
        spending_score INT, annual_income_k INT, country STRING, customer_type STRING,
        loyalty_tier STRING, email STRING, city STRING, attr_hash BIGINT, version_no INT,
        eff_start_key INT, eff_end_key INT, is_current INT
    """
    return read_csv(f"{p_dwh_dir}/dim_customer.csv", schema)


# COMMAND ----------
def src_dim_product():
    schema = """
        stock_code STRING, product_name STRING, category STRING, list_price DECIMAL(18,2),
        supplier_id STRING, status STRING, unit_cost DECIMAL(18,2), price_band STRING,
        supplier_name STRING, supplier_country STRING, lead_time_days INT,
        reliability_score DECIMAL(18,2), product_sk INT, category_group STRING
    """
    return read_csv(f"{p_dwh_dir}/dim_product.csv", schema)


# COMMAND ----------
def src_dim_store():
    schema = """
        store_id STRING, store_name STRING, country STRING, channel STRING, rating INT,
        no_of_employees INT, region STRING, emp_cnt INT, store_sk INT
    """
    return read_csv(f"{p_dwh_dir}/dim_store.csv", schema)


# COMMAND ----------
def src_dim_employee():
    schema = """
        employee_id STRING, store_id STRING, employee_name STRING, role STRING,
        hire_date DATE, tenure_years INT, store_sk INT, employee_sk INT
    """
    return read_csv(f"{p_dwh_dir}/dim_employee.csv", schema)


# COMMAND ----------
def src_dim_fx():
    schema = """
        country STRING, year_month STRING, currency STRING, fx_rate DECIMAL(18,6)
    """
    return read_csv(f"{p_dwh_dir}/dim_fx.csv", schema)


# COMMAND ----------
def sales_lines_keyed():
    return (
        spark.read.table("src_stg_orders")
        .filter("is_cancel = 0 AND is_nonproduct = 0 AND quantity > 0")
        # DataStage TimestampToString(invoice_ts, "%yyyy%mm") -> yyyyMM
        .withColumn("year_month", F.date_format("invoice_ts", "yyyyMM"))
    )


# COMMAND ----------
def sales_with_customer():
    cust = spark.read.table("src_dim_customer").select(
        "customer_id", "customer_sk", "customer_type", "loyalty_tier",
        "eff_start_key", "eff_end_key",
    )
    joined = spark.read.table("sales_lines_keyed").join(cust, on="customer_id", how="left")
    pit = joined.filter(
        "customer_sk IS NULL OR (eff_start_key <= invoice_date_key AND eff_end_key >= invoice_date_key)"
    )
    return (
        pit.withColumn("customer_sk", F.coalesce("customer_sk", F.lit(-1)))
        .withColumn("customer_type", F.coalesce("customer_type", F.lit("unknown")))
        .withColumn("loyalty_tier", F.coalesce("loyalty_tier", F.lit("NONE")))
        .drop("eff_start_key", "eff_end_key")
    )


# COMMAND ----------
def sales_with_product():
    prod = spark.read.table("src_dim_product").select(
        "stock_code", "category", "supplier_id", "unit_cost", "product_sk"
    )
    joined = spark.read.table("sales_with_customer").join(prod, on="stock_code", how="left")
    return (
        joined.withColumn("category", F.coalesce("category", F.lit("Unknown")))
        .withColumn("supplier_id", F.coalesce("supplier_id", F.lit("UNKNOWN")))
        .withColumn("unit_cost", F.coalesce("unit_cost", F.lit(0).cast("decimal(18,2)")))
        .withColumn("product_sk", F.coalesce("product_sk", F.lit(-1)))
    )


# COMMAND ----------
def sales_with_store():
    store = spark.read.table("src_dim_store").select("store_id", "region", "store_sk")
    return spark.read.table("sales_with_product").join(store, on="store_id", how="inner")


# COMMAND ----------
def sales_with_employee():
    emp = spark.read.table("src_dim_employee").select("employee_id", "employee_sk")
    return spark.read.table("sales_with_store").join(emp, on="employee_id", how="inner")


# COMMAND ----------
def sales_with_fx():
    fx = spark.read.table("src_dim_fx").select("country", "year_month", "currency", "fx_rate")
    joined = spark.read.table("sales_with_employee").join(fx, on=["country", "year_month"], how="left")
    return (
        joined.withColumn("currency", F.coalesce("currency", F.lit("GBP")))
        .withColumn("fx_rate", F.coalesce("fx_rate", F.lit(1).cast("decimal(18,6)")))
    )


# COMMAND ----------
def fact_sales():
    df = spark.read.table("sales_with_fx")
    return (
        df.select(
            "invoice_no", "stock_code", "quantity", "invoice_ts", "unit_price",
            "customer_id", "country", "store_id", "invoice_date_key", "year_month",
            "customer_sk", "customer_type", "loyalty_tier", "category", "product_sk",
            "region", "store_sk", "employee_sk", "currency",
            F.col("line_amount").alias("sales_amount"),
            (F.col("quantity") * F.col("unit_cost")).cast("decimal(18,2)").alias("cost_amount"),
            (F.col("line_amount") - F.col("quantity") * F.col("unit_cost"))
                .cast("decimal(18,2)").alias("gross_profit"),
            (F.col("line_amount") * F.col("fx_rate")).cast("decimal(18,2)").alias("local_amount"),
            F.lit(1).alias("line_cnt"),
        )
        .orderBy("invoice_no", "stock_code")
    )


# COMMAND ----------
def dq_orphan_products():
    return spark.read.table("fact_sales").filter("product_sk = -1")


# COMMAND ----------
def audit_fact_by_month():
    return (
        spark.read.table("fact_sales")
        .groupBy("year_month")
        .agg(
            F.sum("sales_amount").alias("sales_amount"),
            F.sum("gross_profit").alias("gross_profit"),
        )
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
