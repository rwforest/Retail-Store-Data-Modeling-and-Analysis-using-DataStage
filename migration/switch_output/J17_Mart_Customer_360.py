# Databricks notebook source
# MAGIC %md
# MAGIC # J17_Mart_Customer_360
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J17_Mart_Customer_360.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline: J17_Mart_Customer_360
# Converted from DataStage parallel job J17_Mart_Customer_360.
# Customer 360 mart with RFM scoring, returns netting and segmentation.
#
# Notes on the conversion:
#  - DataStage job parameters become pipeline configuration values, read with spark.conf.get().
#  - Sequential file sources are read as CSV with a header row, using the declared schemas.
#  - Sequential file targets (Customer_360.csv, Customer_Segments.csv) become
#    materialized views in the pipeline's target catalog and schema.
#  - Filter, Aggregator, Join, Transformer, Sort and Copy stages become DataFrame transformations.
#    The Copy stage's fan-out becomes two downstream datasets that both read from the Customer_360 dataset.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------
# ---------------------------------------------------------------------------
# Job parameters (set in the pipeline configuration, defaults as in the job)
# ---------------------------------------------------------------------------
pDwhDir = spark.conf.get("pDwhDir", "C:/case_study/Warehouse")
pMartDir = spark.conf.get("pMartDir", "C:/case_study/Marts")  # kept for reference; outputs are tables
pRunDate = spark.conf.get("pRunDate", "2011-12-31")  # business run date (yyyy-mm-dd)
# COMMAND ----------
def src_fact_invoice():
    schema = """
        invoice_no STRING, customer_id INT, customer_sk INT, store_sk INT,
        employee_sk INT, invoice_date_key INT, year_month STRING,
        invoice_sales DECIMAL(18,2), invoice_profit DECIMAL(18,2),
        item_qty BIGINT, line_total BIGINT, avg_line_value DECIMAL(18,2),
        invoice_cnt INT
    """
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(f"{pDwhDir}/fact_invoice.csv")
    )


# COMMAND ----------
def cust_revenue():
    return (
        spark.read.table("src_fact_invoice")
        .filter(F.col("customer_id") > 0)  # flt_Known_Customers
        .groupBy("customer_id")            # agg_Cust_Revenue
        .agg(
            F.sum("invoice_sales").alias("total_sales"),
            F.sum("invoice_profit").alias("total_profit"),
            F.sum("invoice_cnt").cast("bigint").alias("invoice_total"),
            F.min("invoice_date_key").alias("first_date_key"),
            F.max("invoice_date_key").alias("last_date_key"),
        )
    )


# COMMAND ----------
def src_fact_returns():
    schema = """
        invoice_no STRING, stock_code STRING, customer_id INT, country STRING,
        invoice_date_key INT, year_month STRING, returned_qty INT,
        returned_amount DECIMAL(18,2), category STRING, product_sk INT,
        customer_sk INT
    """
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(f"{pDwhDir}/fact_returns.csv")
    )


# COMMAND ----------
def cust_returns():
    return (
        spark.read.table("src_fact_returns")
        .filter(F.col("customer_id") > 0)  # flt_Known_Returns
        .groupBy("customer_id")            # agg_Cust_Returns
        .agg(F.sum("returned_amount").alias("total_returned"))
    )


# COMMAND ----------
def cust_revenue_returns():
    rev = spark.read.table("cust_revenue")
    ret = spark.read.table("cust_returns")
    return (
        rev.join(ret, on="customer_id", how="left")
        .withColumn(
            "total_returned",
            F.coalesce(F.col("total_returned"), F.lit(0)).cast("decimal(18,2)"),
        )
    )


# COMMAND ----------
def src_dim_customer():
    schema = """
        customer_sk INT, customer_id INT, cust_name STRING, gender STRING,
        age INT, spending_score INT, annual_income_k INT, country STRING,
        customer_type STRING, loyalty_tier STRING, email STRING, city STRING,
        attr_hash BIGINT, version_no INT, eff_start_key INT, eff_end_key INT,
        is_current INT
    """
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(f"{pDwhDir}/dim_customer.csv")
    )


# COMMAND ----------
def dim_customer_current():
    return spark.read.table("src_dim_customer").filter(F.col("is_current") == 1)


# COMMAND ----------
def customer_360_scored():
    rr = spark.read.table("cust_revenue_returns")
    cur = spark.read.table("dim_customer_current").select(
        "customer_id", "customer_sk", "cust_name", "age", "country",
        "customer_type", "loyalty_tier",
    )

    # jn_Customer_Attrs: inner join on customer_id
    joined = rr.join(cur, on="customer_id", how="inner")

    # xfm_RFM: recency = days between run date and last invoice date
    last_date = F.to_date(F.col("last_date_key").cast("string"), "yyyyMMdd")
    recency = F.datediff(F.to_date(F.lit(pRunDate), "yyyy-MM-dd"), last_date)

    scored = (
        joined
        .withColumn("recency_days", recency.cast("int"))
        # Recency score: more recent purchase = higher score
        .withColumn(
            "r_score",
            F.when(F.col("recency_days") <= 30, 5)
             .when(F.col("recency_days") <= 90, 4)
             .when(F.col("recency_days") <= 180, 3)
             .when(F.col("recency_days") <= 270, 2)
             .otherwise(1),
        )
        # Frequency score based on number of invoices
        .withColumn(
            "f_score",
            F.when(F.col("invoice_total") >= 10, 5)
             .when(F.col("invoice_total") >= 6, 4)
             .when(F.col("invoice_total") >= 3, 3)
             .when(F.col("invoice_total") >= 2, 2)
             .otherwise(1),
        )
        # Monetary score based on total sales
        .withColumn(
            "m_score",
            F.when(F.col("total_sales") >= 5000, 5)
             .when(F.col("total_sales") >= 2000, 4)
             .when(F.col("total_sales") >= 800, 3)
             .when(F.col("total_sales") >= 300, 2)
             .otherwise(1),
        )
        # Net sales after returns
        .withColumn(
            "net_sales",
            (F.col("total_sales") - F.col("total_returned")).cast("decimal(18,2)"),
        )
    )

    # xfm_Segment: customer segment derived from the R and F scores
    return scored.withColumn(
        "segment",
        F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4), "CHAMPION")
         .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3), "LOYAL")
         .when(F.col("r_score") >= 4, "RECENT")
         .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3), "AT_RISK")
         .otherwise("HIBERNATING"),
    )


# COMMAND ----------
def mart_customer_360():
    return (
        spark.read.table("customer_360_scored")
        .select(
            "customer_id", "total_sales", "total_profit", "invoice_total",
            "first_date_key", "last_date_key", "total_returned", "customer_sk",
            "cust_name", "age", "country", "customer_type", "loyalty_tier",
            "recency_days", "r_score", "f_score", "m_score", "net_sales",
            "segment",
        )
        .orderBy(F.col("net_sales").desc())  # srt_Customer_360
    )


# COMMAND ----------
def mart_customer_segments():
    return (
        spark.read.table("mart_customer_360")
        .groupBy("segment")
        .agg(F.count(F.lit(1)).cast("int").alias("customers"))
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
