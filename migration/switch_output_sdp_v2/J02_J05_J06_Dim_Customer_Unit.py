# Databricks notebook source
# MAGIC %md
# MAGIC # J02_J05_J06_Dim_Customer_Unit
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261030071908-zgpy/J02_J05_J06_Dim_Customer_Unit.txt`
# COMMAND ----------
# COMMAND ----------
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# The orchestration (initial load only if the dimension is missing, SCD2 apply job,
# promotion of dim_customer_next) is replaced by Auto CDC from snapshots below.
# The expire / carry / insert / funnel / promote stages are not translated one by one.


# J02_Land_Customers: staged snapshot (src_cust_<year> -> xfm_Std_Cust_<year> -> rmd_Cust_<year>)
# Plain Python function: reads the CSV directly, never a pipeline dataset.
def staged_snapshot(year):
    raw = spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/customers_snapshot_{year}.csv")
    assert len(raw.columns) == 12, f"customers_snapshot_{year}.csv: expected 12 columns, got {len(raw.columns)}"
    # src_cust_<year>: Sequential File (columns mapped by position, as DataStage does)
    typed = raw.toDF('CustomerID', 'CustomerName', 'Gender', 'Age', 'SpendingScore', 'AnnualIncomeK',
                     'Country', 'CustomerType', 'LoyaltyTier', 'Email', 'City', 'SnapshotDate').selectExpr(
        "CAST(`CustomerID` AS int) AS CustomerID",
        "coalesce(`CustomerName`, '') AS CustomerName",
        "coalesce(`Gender`, '') AS Gender",
        "CAST(`Age` AS int) AS Age",
        "CAST(`SpendingScore` AS int) AS SpendingScore",
        "CAST(`AnnualIncomeK` AS int) AS AnnualIncomeK",
        "coalesce(`Country`, '') AS Country",
        "coalesce(`CustomerType`, '') AS CustomerType",
        "coalesce(`LoyaltyTier`, '') AS LoyaltyTier",
        "coalesce(`Email`, '') AS Email",
        "coalesce(`City`, '') AS City",
        "coalesce(`SnapshotDate`, '') AS SnapshotDate")
    # xfm_Std_Cust_<year>: Transformer
    std = typed.selectExpr(
        "CAST(CustomerID AS int) AS customer_id",
        "CAST(upper(trim(CustomerName)) AS string) AS cust_name",
        "CAST(trim(Gender) AS string) AS gender",
        "CAST(Age AS int) AS age",
        "CAST(SpendingScore AS int) AS spending_score",
        "CAST(AnnualIncomeK AS int) AS annual_income_k",
        "CAST(trim(Country) AS string) AS country",
        "CAST(trim(CustomerType) AS string) AS customer_type",
        "CAST(upper(trim(LoyaltyTier)) AS string) AS loyalty_tier",
        "CAST(lower(trim(Email)) AS string) AS email",
        "CAST(trim(City) AS string) AS city",
        "CAST(to_date(SnapshotDate, 'yyyy-MM-dd') AS date) AS snapshot_date",
        "CAST(crc32(concat(CAST(upper(trim(CustomerName)) AS string), '|', CAST(Age AS string), '|', "
        "CAST(SpendingScore AS string), '|', CAST(AnnualIncomeK AS string), '|', CAST(trim(Country) AS string), '|', "
        "CAST(upper(trim(LoyaltyTier)) AS string), '|', CAST(trim(City) AS string), '|', "
        "CAST(lower(trim(Email)) AS string))) AS bigint) AS attr_hash")
    # rmd_Cust_<year>: Remove Duplicates (keep first by customer_id; no upstream sort -> arbitrary stable id)
    return (std.selectExpr("*", "monotonically_increasing_id() AS _mid")
               .selectExpr("*", "row_number() OVER (PARTITION BY customer_id ORDER BY _mid) AS _rn")
               .where("_rn = 1")
               .drop("_mid", "_rn"))


# tgt_stg_customers_2010: target file -> materialized view stg_customers_2010
@dp.materialized_view(name="stg_customers_2010")
def stg_customers_2010():
    return staged_snapshot(2010)


# tgt_stg_customers_2011: target file -> materialized view stg_customers_2011
@dp.materialized_view(name="stg_customers_2011")
def stg_customers_2011():
    return staged_snapshot(2011)


# History of the dimension: SCD Type 2 maintained by Auto CDC from successive full snapshots.
# Snapshot order: 2010 snapshot = version 1, 2011 snapshot = version 2.
def next_snapshot_and_version(latest_version):
    if latest_version is None:
        return (staged_snapshot(2010), 1)
    if latest_version == 1:
        return (staged_snapshot(2011), 2)
    return None


dp.create_streaming_table(name="dim_customer_history")

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_customer_history",
    source=next_snapshot_and_version,
    keys=["customer_id"],
    stored_as_scd_type=2,
    # The jobs detect a change by comparing the attribute hash only
    track_history_column_list=["attr_hash"],
)
# Note: the DataStage SCD2 job drives from the new snapshot (left join), so a customer missing
# from the 2011 snapshot is dropped there; Auto CDC instead closes its current version.


# State of the dimension after all snapshots were applied (version 1 + version 2).
# Version number, surrogate key, effective keys and current flag follow the jobs' derivations.
def dim_customer_state():
    h = spark.read.table("dim_customer_history").selectExpr(
        "*",
        "CAST(row_number() OVER (PARTITION BY customer_id ORDER BY `__START_AT`) AS int) AS _version_no",
        "(`__END_AT` IS NULL) AS _is_current")
    return h.selectExpr(
        # initial version: customer_id * 1000 + 1; new version: customer_id * 1000 + old_version_no + 1
        "CAST(customer_id * 1000 + _version_no AS int) AS customer_sk",
        "customer_id",
        "cust_name",
        "gender",
        "age",
        "spending_score",
        "annual_income_k",
        "country",
        "customer_type",
        "loyalty_tier",
        "email",
        "city",
        "attr_hash",
        "CAST(_version_no AS int) AS version_no",
        # version 1 starts at 19000101; later versions start at pEffDate (without dashes)
        f"CAST(CASE WHEN _version_no = 1 THEN 19000101 ELSE CAST(replace('{pEffDate}', '-', '') AS int) END AS int) AS eff_start_key",
        # current version ends at 99991231; an expired version ends the day before pEffDate
        f"CAST(CASE WHEN _is_current THEN 99991231 ELSE CAST(replace(date_format(date_add(to_date('{pEffDate}', 'yyyy-MM-dd'), -1), 'yyyy-MM-dd'), '-', '') AS int) END AS int) AS eff_end_key",
        "CAST(CASE WHEN _is_current THEN 1 ELSE 0 END AS int) AS is_current",
    ).orderBy("customer_id", "version_no")


# tgt_dim_customer: target file -> materialized view dim_customer (state after the promotion)
@dp.materialized_view(name="dim_customer")
def dim_customer():
    return dim_customer_state()


# tgt_dim_customer_next: target file -> materialized view dim_customer_next (same content as the promoted dim_customer)
@dp.materialized_view(name="dim_customer_next")
def dim_customer_next():
    return dim_customer_state()


# tgt_audit_scd2_customer: target file -> materialized view audit_scd2_customer
# Change flags computed from the staged snapshots: N = new customer, C = changed hash, U = unchanged.
@dp.materialized_view(name="audit_scd2_customer")
def audit_scd2_customer():
    snap = staged_snapshot(2011)
    old = staged_snapshot(2010).selectExpr("customer_id", "attr_hash AS old_attr_hash")
    flagged = snap.join(old, "customer_id", "left").selectExpr(
        "CAST(CASE WHEN old_attr_hash IS NULL THEN 'N' WHEN old_attr_hash <> attr_hash THEN 'C' ELSE 'U' END AS string) AS change_flag")
    return (flagged.groupBy("change_flag").count()
                   .selectExpr("change_flag", "CAST(`count` AS int) AS customers"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
