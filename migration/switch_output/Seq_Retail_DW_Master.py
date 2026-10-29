# Databricks notebook source
# MAGIC %md
# MAGIC # Seq_Retail_DW_Master
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/Seq_Retail_DW_Master.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline (Python) equivalent of the DataStage sequence job Seq_Retail_DW_Master.
#
# The ${language} placeholder in the request was not filled in, so this is written in Python.
#
# CONVERSION NOTES
# The source is an orchestration-only sequence. The logic of child jobs J01-J23 was not supplied,
# so each job becomes one dataset (streaming table or materialized view) whose logic is inferred
# from its name. Column names and file layouts are ASSUMPTIONS and are marked "ASSUMPTION".
#
# Sequence activity -> declarative pipeline mapping:
#   act_Prepare_Dirs (create dirs)    -> not needed; storage is Unity Catalog volumes/tables
#   wff_Source_Orders (wait for file) -> Auto Loader picks up files as they arrive
#   seq_* sequencers (AND-join)       -> implicit; a dataset runs after every dataset it reads
#   act_Check_Dim_Customer / J05      -> not needed; create_auto_cdc_flow (SCD2) handles the initial
#                                        load and later changes with the same flow
#   act_Promote_Dim_Customer          -> not needed; no separate staging-to-warehouse copy
#   exc_Any_Failure / act_Notify_*    -> configure email/webhook notifications on the Lakeflow Job
#                                        that runs this pipeline (on failure / on success).
#                                        A failed update fails the job, which replaces term_Abort.
#   pStgDir / pDwhDir / pMartDir / pAuditDir -> replaced by pipeline tables (target catalog/schema).
#                                        Audit/DQ outputs are tables.
#
# Pipeline parameters (set as pipeline configuration keys), defaults from the DataStage job:
#   pSrcDir  = C:/case_study/Source_Data  (replace with a UC volume path, e.g. /Volumes/<cat>/<sch>/source_data)
#   pRunDate = 2011-12-31
#   pEffDate = 2011-07-01

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------
# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------
SRC_DIR  = spark.conf.get("pSrcDir",  "C:/case_study/Source_Data")
RUN_DATE = spark.conf.get("pRunDate", "2011-12-31")
EFF_DATE = spark.conf.get("pEffDate", "2011-07-01")


# COMMAND ----------
def _land(subdir: str):
    """Incrementally ingest CSV files from <pSrcDir>/<subdir> with Auto Loader.
    ASSUMPTION: source files are headered CSVs, one sub-folder per entity."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{SRC_DIR}/{subdir}")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# COMMAND ----------
def land_orders():
    return _land("orders")


# COMMAND ----------
def land_customers():
    return _land("customers")


# COMMAND ----------
def land_products():
    return _land("products")


# COMMAND ----------
def land_stores():
    return _land("stores")


# COMMAND ----------
def land_employees():
    return _land("employees")


# COMMAND ----------
def customer_changes():
    return (
        spark.readStream.table("land_customers")
        # ASSUMPTION: changed attributes become effective on pEffDate
        .withColumn("eff_date", F.to_date(F.lit(EFF_DATE)))
    )


# COMMAND ----------
dp.create_streaming_table(
    name="dim_customer",
    comment="Customer dimension, SCD Type 2 (J05/J06). Current row has __END_AT IS NULL",
)

# COMMAND ----------
dp.create_auto_cdc_flow(
    target="dim_customer",
    source="customer_changes",
    keys=["customer_id"],
    sequence_by=F.col("eff_date"),
    stored_as_scd_type=2,
    except_column_list=["_ingest_ts", "_source_file", "_rescued_data"],
)


# COMMAND ----------
def dim_product():
    return (
        spark.read.table("land_products")
        .dropDuplicates(["product_id"])
        .drop("_ingest_ts", "_source_file", "_rescued_data")
    )


# COMMAND ----------
def dim_store():
    return (
        spark.read.table("land_stores")
        .dropDuplicates(["store_id"])
        .drop("_ingest_ts", "_source_file", "_rescued_data")
    )


# COMMAND ----------
def dim_employee():
    return (
        spark.read.table("land_employees")
        .dropDuplicates(["employee_id"])
        .drop("_ingest_ts", "_source_file", "_rescued_data")
    )


# COMMAND ----------
def dim_date():
    # ASSUMPTION: order_date is a date/timestamp column in orders
    bounds = spark.read.table("land_orders").agg(
        F.min(F.to_date("order_date")).alias("min_d"),
        F.max(F.to_date("order_date")).alias("max_d"),
    )
    return (
        bounds.select(F.explode(F.sequence("min_d", "max_d")).alias("date_key"))
        .select(
            "date_key",
            F.year("date_key").alias("year"),
            F.quarter("date_key").alias("quarter"),
            F.month("date_key").alias("month"),
            F.date_format("date_key", "yyyy-MM").alias("year_month"),
            F.dayofweek("date_key").alias("day_of_week"),
        )
    )


# COMMAND ----------
def dim_promotion():
    # ASSUMPTION: promotions are identified by promo_id on the order lines
    return (
        spark.read.table("land_orders")
        .where(F.col("promo_id").isNotNull())
        .select("promo_id")
        .distinct()
    )


# COMMAND ----------
def dim_fx_rate():
    # ASSUMPTION: FX rates are provided as CSV in <pSrcDir>/fx_rates
    # with columns currency, rate_to_base
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{SRC_DIR}/fx_rates")
        .select("currency", F.col("rate_to_base").cast("double").alias("rate_to_base"))
    )


# COMMAND ----------
def fact_sales():
    o = spark.read.table("land_orders").where(F.col("quantity") > 0)  # ASSUMPTION: sales have quantity > 0
    # Point-in-time join to the SCD2 customer dimension
    c = spark.read.table("dim_customer").select(
        "customer_id",
        F.col("customer_id").alias("customer_sk"),
        "__START_AT",
        "__END_AT",
    )
    p = spark.read.table("dim_product").select(
        F.col("product_id").alias("product_id_dim"), "product_id"
    ).withColumnRenamed("product_id", "p_product_id")
    s = spark.read.table("dim_store").select("store_id")
    e = spark.read.table("dim_employee").select("employee_id")
    d = spark.read.table("dim_date").select("date_key")
    pr = spark.read.table("dim_promotion").select(F.col("promo_id").alias("promo_id_dim"))
    fx = spark.read.table("dim_fx_rate")

    return (
        o.alias("o")
        .join(
            c.alias("c"),
            (F.col("o.customer_id") == F.col("c.customer_id"))
            & (F.to_date("o.order_date") >= F.to_date(F.col("c.__START_AT")))
            & (F.col("c.__END_AT").isNull() | (F.to_date("o.order_date") < F.to_date(F.col("c.__END_AT")))),
            "left",
        )
        .join(p, F.col("o.product_id") == F.col("p_product_id"), "left")
        .join(s, "store_id", "left_semi")  # enforce dependency / referential check on store
        .join(e, "employee_id", "left_semi")  # enforce dependency / referential check on employee
        .join(d, F.to_date(F.col("o.order_date")) == F.col("date_key"), "left_semi")
        .join(pr, F.col("o.promo_id") == F.col("promo_id_dim"), "left")
        .join(fx, F.col("o.currency") == fx.currency, "left")
        .select(
            "o.order_id",
            F.to_date("o.order_date").alias("order_date"),
            "o.customer_id",
            F.col("c.customer_sk"),
            "o.product_id",
            F.col("product_id_dim"),
            "o.store_id",
            "o.employee_id",
            "o.promo_id",
            "o.quantity",
            "o.unit_price",
            (F.col("o.quantity") * F.col("o.unit_price")).alias("gross_amount_local"),
            (F.col("o.quantity") * F.col("o.unit_price") * F.coalesce(F.col("rate_to_base"), F.lit(1.0))).alias("gross_amount_base"),
            F.to_date(F.lit(RUN_DATE)).alias("run_date"),
        )
    )


# COMMAND ----------
def fact_returns():
    dim_deps = spark.read.table("dim_product").select("product_id")
    return (
        spark.read.table("land_orders")
        .where(F.col("quantity") < 0)  # ASSUMPTION: returns are negative-quantity order lines
        .join(dim_deps, "product_id", "left_semi")
        .select(
            "order_id",
            F.to_date("order_date").alias("return_date"),
            "customer_id",
            "product_id",
            "store_id",
            (-F.col("quantity")).alias("returned_quantity"),
            (-F.col("quantity") * F.col("unit_price")).alias("returned_amount"),
            F.to_date(F.lit(RUN_DATE)).alias("run_date"),
        )
    )


# COMMAND ----------
def fact_invoice():
    return (
        spark.read.table("fact_sales")
        .groupBy("order_id", "order_date", "customer_id", "store_id", "employee_id")
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("gross_amount_local").alias("invoice_amount_local"),
            F.sum("gross_amount_base").alias("invoice_amount_base"),
            F.count("*").alias("line_count"),
        )
        .withColumn("run_date", F.to_date(F.lit(RUN_DATE)))
    )


# COMMAND ----------
def retail_data_mart():
    fs = spark.read.table("fact_sales")
    fr = spark.read.table("fact_returns").groupBy("order_id", "product_id").agg(
        F.sum("returned_amount").alias("returned_amount")
    )
    return (
        fs.join(fr, ["order_id", "product_id"], "left")
        .withColumn("returned_amount", F.coalesce("returned_amount", F.lit(0.0)))
        .withColumn("net_amount_base", F.col("gross_amount_base") - F.col("returned_amount"))
    )


# COMMAND ----------
def analysis_employee_transactions():
    return (
        spark.read.table("retail_data_mart")
        .groupBy("employee_id")
        .agg(
            F.countDistinct("order_id").alias("transaction_count"),
            F.sum("net_amount_base").alias("net_sales"),
        )
    )


# COMMAND ----------
def analysis_customertype_profit():
    cust = (
        spark.read.table("dim_customer")
        .where("__END_AT IS NULL")
        .select("customer_id", "customer_type")  # ASSUMPTION: customer_type column
    )
    return (
        spark.read.table("retail_data_mart")
        .join(cust, "customer_id", "left")
        .groupBy("customer_type")
        .agg(F.sum("net_amount_base").alias("profit"))  # ASSUMPTION: net amount used as profit proxy
    )


# COMMAND ----------
def bonus_program():
    # ASSUMPTION: 2% of net sales as bonus for employees; adjust to real rules
    return (
        spark.read.table("analysis_employee_transactions")
        .withColumn("bonus_amount", F.round(F.col("net_sales") * F.lit(0.02), 2))
        .withColumn("run_date", F.to_date(F.lit(RUN_DATE)))    )


# COMMAND ----------
def mart_customer_360():
    sales = (
        spark.read.table("fact_sales")
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_purchased"),
            F.sum("gross_amount_base").alias("lifetime_sales"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
    )
    returns = (
        spark.read.table("fact_returns")
        .groupBy("customer_id")
        .agg(
            F.sum("returned_quantity").alias("units_returned"),
            F.sum("returned_amount").alias("lifetime_returns"),
        )
    )
    # Current version of each customer from the SCD2 dimension
    cust = spark.read.table("dim_customer").where("__END_AT IS NULL").drop("__START_AT", "__END_AT")
    return (
        cust.join(sales, "customer_id", "left")
        .join(returns, "customer_id", "left")
        .withColumn("units_returned", F.coalesce("units_returned", F.lit(0)))
        .withColumn("lifetime_returns", F.coalesce("lifetime_returns", F.lit(0.0)))
        .withColumn("net_lifetime_sales", F.coalesce("lifetime_sales", F.lit(0.0)) - F.col("lifetime_returns"))
        # Recency relative to the business run date (pRunDate)
        .withColumn("days_since_last_order", F.datediff(F.to_date(F.lit(RUN_DATE)), F.col("last_order_date")))
    )


# COMMAND ----------
def mart_product_performance():
    sales = (
        spark.read.table("fact_sales")
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("gross_amount_base").alias("sales_amount"),
            F.countDistinct("order_id").alias("order_count"),
        )
    )
    returns = (
        spark.read.table("fact_returns")
        .groupBy("product_id")
        .agg(
            F.sum("returned_quantity").alias("units_returned"),
            F.sum("returned_amount").alias("returned_amount"),
        )
    )
    return (
        sales.join(returns, "product_id", "left")
        .withColumn("units_returned", F.coalesce("units_returned", F.lit(0)))
        .withColumn("returned_amount", F.coalesce("returned_amount", F.lit(0.0)))
        .withColumn("return_rate", F.col("units_returned") / F.col("units_sold"))
        .withColumn("net_sales_amount", F.col("sales_amount") - F.col("returned_amount"))
        .withColumn("sales_rank", F.dense_rank().over(Window.orderBy(F.col("net_sales_amount").desc())))
    )


# COMMAND ----------
def mart_basket_affinity():
    lines = spark.read.table("fact_sales").select("order_id", "product_id").distinct()
    a = lines.alias("a")
    b = lines.alias("b")
    # Self join on order; the "<" condition keeps each unordered pair once
    pairs = (
        a.join(b, (F.col("a.order_id") == F.col("b.order_id")) & (F.col("a.product_id") < F.col("b.product_id")))
        .select(
            F.col("a.product_id").alias("product_a"),
            F.col("b.product_id").alias("product_b"),
            F.col("a.order_id").alias("order_id"),
        )
    )
    total_orders = spark.read.table("fact_sales").select("order_id").distinct().count()
    return (
        pairs.groupBy("product_a", "product_b")
        .agg(F.countDistinct("order_id").alias("pair_order_count"))
        .withColumn("support", F.col("pair_order_count") / F.lit(float(total_orders) if total_orders else 1.0))
    )


# COMMAND ----------
def mart_store_monthly():
    sales = (
        spark.read.table("fact_sales")
        .withColumn("year_month", F.date_format("order_date", "yyyy-MM"))
        .groupBy("store_id", "year_month")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("gross_amount_base").alias("sales_amount"),
        )
    )
    returns = (
        spark.read.table("fact_returns")
        .withColumn("year_month", F.date_format("return_date", "yyyy-MM"))
        .groupBy("store_id", "year_month")
        .agg(F.sum("returned_amount").alias("returned_amount"))
    )
    return (
        sales.join(returns, ["store_id", "year_month"], "left")
        .withColumn("returned_amount", F.coalesce("returned_amount", F.lit(0.0)))
        .withColumn("net_sales_amount", F.col("sales_amount") - F.col("returned_amount"))
    )


# COMMAND ----------
def mart_promo_effectiveness():
    # ASSUMPTION: lines with a NULL promo_id are treated as non-promoted baseline sales
    return (
        spark.read.table("fact_sales")
        .withColumn("promo_flag", F.when(F.col("promo_id").isNotNull(), F.lit("PROMO")).otherwise(F.lit("NO_PROMO")))
        .groupBy("promo_id", "promo_flag")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("gross_amount_base").alias("sales_amount"),
            F.avg("unit_price").alias("avg_unit_price"),
        )
    )


# COMMAND ----------
def dq_fact_checks():
    fs = spark.read.table("fact_sales")
    fr = spark.read.table("fact_returns")
    fi = spark.read.table("fact_invoice")

    def _check(df, check_name, failed_condition):
        """Return a single row with the check name, total rows, and failing rows."""
        return df.agg(
            F.lit(check_name).alias("check_name"),
            F.count("*").alias("rows_checked"),
            F.sum(F.when(F.expr(failed_condition), 1).otherwise(0)).alias("rows_failed"),
        )

    results = (
        _check(fs, "fact_sales.order_id_not_null", "order_id IS NULL")
        .unionByName(_check(fs, "fact_sales.customer_resolved", "customer_sk IS NULL"))
        .unionByName(_check(fs, "fact_sales.product_resolved", "product_id_dim IS NULL"))
        .unionByName(_check(fs, "fact_sales.positive_amount", "gross_amount_base <= 0"))
        .unionByName(_check(fr, "fact_returns.positive_returned_quantity", "returned_quantity <= 0"))
        .unionByName(_check(fi, "fact_invoice.positive_invoice_amount", "invoice_amount_base <= 0"))
    )
    return (
        results
        .withColumn("status", F.when(F.col("rows_failed") == 0, "PASS").otherwise("FAIL"))
        .withColumn("run_date", F.to_date(F.lit(RUN_DATE)))
    )


# COMMAND ----------
def reconcile_source_to_fact():
    src = spark.read.table("land_orders")
    src_sales = src.where(F.col("quantity") > 0).agg(
        F.lit("sales").alias("subject"),
        F.count("*").alias("source_rows"),
        F.sum(F.col("quantity") * F.col("unit_price")).alias("source_amount"),
    )
    src_returns = src.where(F.col("quantity") < 0).agg(
        F.lit("returns").alias("subject"),
        F.count("*").alias("source_rows"),
        F.sum(-F.col("quantity") * F.col("unit_price")).alias("source_amount"),
    )
    tgt_sales = spark.read.table("fact_sales").agg(
        F.lit("sales").alias("subject"),
        F.count("*").alias("fact_rows"),
        F.sum("gross_amount_local").alias("fact_amount"),
    )
    tgt_returns = spark.read.table("fact_returns").agg(
        F.lit("returns").alias("subject"),
        F.count("*").alias("fact_rows"),
        F.sum("returned_amount").alias("fact_amount"),
    )
    return (
        src_sales.unionByName(src_returns)
        .join(tgt_sales.unionByName(tgt_returns), "subject")
        .withColumn("row_diff", F.col("source_rows") - F.col("fact_rows"))
        .withColumn("amount_diff", F.round(F.col("source_amount") - F.col("fact_amount"), 2))
        .withColumn("status", F.when((F.col("row_diff") == 0) & (F.col("amount_diff") == 0), "MATCH").otherwise("MISMATCH"))
        .withColumn("run_date", F.to_date(F.lit(RUN_DATE)))
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
