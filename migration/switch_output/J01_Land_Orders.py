# Databricks notebook source
# MAGIC %md
# MAGIC # J01_Land_Orders
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J01_Land_Orders.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline: J01_Land_Orders
# Converted from DataStage parallel job J01_Land_Orders.
# Flow: orders_raw.csv -> typing/flagging -> split valid/invalid -> de-duplicate -> staging table,
#       plus DQ anomalies and row counts by country (audit).
# Note: no target language was specified, so Python is used.
# Note: DataStage sequential file targets (stg_orders.csv, rej_orders.csv, dq_order_anomalies.csv,
#       audit_land_orders.csv) become pipeline tables. Export them to files separately if needed.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------
# ---------------------------------------------------------------------------
# Parameters (DataStage job parameters -> pipeline configuration values)
# Set these in the pipeline settings "configuration" block to override defaults.
# Only pSrcDir is used by this job. The others (pStgDir, pAuditDir, pDwhDir, pMartDir,
# pRunDate, pEffDate) are not needed because outputs are tables.
# ---------------------------------------------------------------------------
SRC_DIR = spark.conf.get("pSrcDir", "C:/case_study/Source_Data")

# COMMAND ----------
# Stock codes treated as non-product lines (postage, fees, manual adjustments, etc.)
NON_PRODUCT_CODES = ["POST", "D", "M", "DOT", "CRUK", "BANK CHARGES",
                     "AMAZONFEE", "S", "B", "C2", "PADS"]

# COMMAND ----------
# Timestamp format equivalent to DataStage "%yyyy-%mm-%dd %hh:%nn:%ss"
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"


# COMMAND ----------
def orders_raw():
    return (
        spark.read
        .option("header", True)          # firstLineColumnNames
        .option("inferSchema", False)    # keep everything as string, like the DataStage definition
        .csv(f"{SRC_DIR}/orders_raw.csv")
    )


# COMMAND ----------
def typed_orders():
    r = spark.read.table("orders_raw") if False else dp.read("orders_raw")

    # Trimmed helper expressions
    stock_trim_upper = F.upper(F.trim(F.col("StockCode")))
    cust_trim = F.trim(F.col("CustomerID"))
    invoice_ts = F.to_timestamp(F.col("InvoiceDate"), TS_FORMAT)

    return r.select(
        F.trim(F.col("InvoiceNo")).alias("invoice_no"),
        stock_trim_upper.alias("stock_code"),
        F.trim(F.col("Description")).alias("description"),
        F.trim(F.col("Quantity")).cast("int").alias("quantity"),
        invoice_ts.alias("invoice_ts"),
        F.trim(F.col("UnitPrice")).cast("decimal(18,2)").alias("unit_price"),
        # Missing customer id -> 0 (guest customer)
        F.when(F.length(cust_trim) == 0, F.lit(0))
         .otherwise(cust_trim.cast("int")).alias("customer_id"),
        F.trim(F.col("Country")).alias("country"),
        F.trim(F.col("StoreID")).alias("store_id"),
        F.trim(F.col("EmployeeID")).alias("employee_id"),
        # yyyymmdd integer date key
        F.date_format(invoice_ts, "yyyyMMdd").cast("int").alias("invoice_date_key"),
        # line amount = quantity * unit price
        (F.trim(F.col("Quantity")).cast("decimal(18,2)")
         * F.trim(F.col("UnitPrice")).cast("decimal(18,2)")).cast("decimal(18,2)").alias("line_amount"),
        # Cancellation invoices start with "C"
        F.when(F.substring(F.trim(F.col("InvoiceNo")), 1, 1) == "C", F.lit(1))
         .otherwise(F.lit(0)).alias("is_cancel"),
        # Non-product stock codes
        F.when(stock_trim_upper.isin(NON_PRODUCT_CODES), F.lit(1))
         .otherwise(F.lit(0)).alias("is_nonproduct"),
        # Guest flag: no customer id supplied
        F.when(F.length(cust_trim) == 0, F.lit(1))
         .otherwise(F.lit(0)).alias("is_guest"),
    )


# COMMAND ----------
def rej_orders():
    return dp.read("typed_orders").where("quantity = 0 or unit_price < 0")


# COMMAND ----------
def dedup_orders():
    valid = dp.read("typed_orders").where("quantity <> 0 and unit_price >= 0")
    # De-duplicate on the business key of the order line
    return valid.dropDuplicates(
        ["invoice_no", "stock_code", "invoice_ts", "customer_id", "quantity", "unit_price"]
    )


# COMMAND ----------
def stg_orders():
    return dp.read("dedup_orders")


# COMMAND ----------
def dq_order_anomalies():
    return dp.read("dedup_orders").where(
        "unit_price = 0 or quantity > 5000 or quantity < -5000"
    )


# COMMAND ----------
def audit_land_orders():
    return (
        dp.read("dedup_orders")
        .groupBy("country")
        .agg(F.count(F.lit(1)).cast("int").alias("rows_loaded"))
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
