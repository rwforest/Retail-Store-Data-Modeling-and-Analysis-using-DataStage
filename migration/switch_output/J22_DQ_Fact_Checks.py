# Databricks notebook source
# MAGIC %md
# MAGIC # J22_DQ_Fact_Checks
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J22_DQ_Fact_Checks.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline (Python) equivalent of the DataStage job J22_DQ_Fact_Checks.
# The input did not specify a target language, so Python is used.
#
# Original flow:
#   fact_sales.csv -> Copy (4 branches) -> Filter (one rule per branch)
#     -> Transformer (tag rows with check_name) -> Aggregator (count hits per check_name)
#     -> Funnel (union of all branches) -> Sort by check_name -> dq_fact_checks.csv
#
# Notes on the conversion:
#  - Job parameters (pDwhDir, pAuditDir, ...) become pipeline configuration values read with spark.conf.get.
#  - The Copy stage is not needed. All checks read from the same source dataset.
#  - The final CSV write is replaced by a pipeline-managed table (dq_fact_checks). Output location and
#    format are controlled by the pipeline's target catalog and schema. To also export CSV to
#    pAuditDir, add a separate job or task after the pipeline.
#  - As in the original Aggregator, a check that has zero violating rows produces NO result row.
#  - Sorting inside a pipeline table is not persisted. The ORDER BY is kept to mirror the original Sort stage.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
)

# COMMAND ----------
# ---------------------------------------------------------------------------
# Parameters (pipeline configuration with the same defaults as the DataStage job)
# ---------------------------------------------------------------------------
p_dwh_dir = spark.conf.get("pDwhDir", "C:/case_study/Warehouse")    # Warehouse (dimensions/facts) directory
p_audit_dir = spark.conf.get("pAuditDir", "C:/case_study/Audit")    # Audit / DQ output directory (informational only)

# COMMAND ----------
# ---------------------------------------------------------------------------
# Source schema (from the src_fact_sales column definitions, all NOT NULL)
# ---------------------------------------------------------------------------
fact_sales_schema = StructType([
    StructField("invoice_no", StringType(), False),
    StructField("stock_code", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("invoice_ts", TimestampType(), False),
    StructField("unit_price", DecimalType(18, 2), False),
    StructField("customer_id", IntegerType(), False),
    StructField("country", StringType(), False),
    StructField("store_id", StringType(), False),
    StructField("invoice_date_key", IntegerType(), False),
    StructField("year_month", StringType(), False),
    StructField("customer_sk", IntegerType(), False),
    StructField("customer_type", StringType(), False),
    StructField("loyalty_tier", StringType(), False),
    StructField("category", StringType(), False),
    StructField("product_sk", IntegerType(), False),
    StructField("region", StringType(), False),
    StructField("store_sk", IntegerType(), False),
    StructField("employee_sk", IntegerType(), False),
    StructField("currency", StringType(), False),
    StructField("sales_amount", DecimalType(18, 2), False),
    StructField("cost_amount", DecimalType(18, 2), False),
    StructField("gross_profit", DecimalType(18, 2), False),
    StructField("local_amount", DecimalType(18, 2), False),
    StructField("line_cnt", IntegerType(), False),
])


# COMMAND ----------
def src_fact_sales():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(fact_sales_schema)
        .load(f"{p_dwh_dir}/fact_sales.csv")
    )


# COMMAND ----------
# ---------------------------------------------------------------------------
# DQ rules: (check_name, filter condition)
# Each tuple replaces one chain: PxFilter -> Transformer (tag) -> Aggregator (count)
# ---------------------------------------------------------------------------
DQ_CHECKS = [
    ("guest_customer_lines",  "customer_sk = -1"),   # lines tied to the guest/unknown customer
    ("orphan_product_lines",  "product_sk = -1"),    # lines with no matching product dimension row
    ("non_positive_sales",    "sales_amount <= 0"),  # zero or negative sales amount
    ("negative_gross_profit", "gross_profit < 0"),   # lines sold at a loss
]


# COMMAND ----------
def _make_check_view(check_name: str, condition: str):
    """Create one view per DQ rule that returns (check_name, hits).

    Filter -> tag with check_name -> count rows grouped by check_name.
    No row is returned when no record violates the rule (same as the DataStage Aggregator).
    """
    view_name = f"cnt_{check_name}"

    @dp.temporary_view(name=view_name, comment=f"Hit count for DQ check '{check_name}' ({condition})")
    def _check():
        return (
            spark.read.table("src_fact_sales")
            .filter(F.expr(condition))                      # PxFilter
            .withColumn("check_name", F.lit(check_name))    # Transformer: tag rows
            .groupBy("check_name")                          # Aggregator: key = check_name
            .agg(F.count(F.lit(1)).cast("int").alias("hits"))
        )

    return view_name


# COMMAND ----------
check_views = [_make_check_view(name, cond) for name, cond in DQ_CHECKS]


# COMMAND ----------
def dq_fact_checks():
    # Funnel: union all per-check count outputs
    result = None
    for v in check_views:
        df = spark.read.table(v).select("check_name", "hits")
        result = df if result is None else result.unionByName(df)

    # Sort stage: order by check_name
    return result.orderBy("check_name")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
