# Databricks notebook source
# MAGIC %md
# MAGIC # J02_J05_J06_Dim_Customer_Unit
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261030063735-t49e/J02_J05_J06_Dim_Customer_Unit.txt`
# COMMAND ----------
# COMMAND ----------
from pyspark import pipelines as dp

# Provide a default so the pipeline does not fail when the "src_dir" config is not set
src_dir = spark.conf.get("src_dir", "/Volumes/main/default/retail_src")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# Orchestration notes (load v1 only if the dimension does not exist, promote the next file)
# have no equivalent: the dimension is maintained declaratively by Auto CDC from snapshots.
# Snapshot order: 2010 file = version 1, 2011 file = version 2.


# J02_Land_Customers: staged snapshot builder (plain Python function, no pipeline dataset is read)
# Covers src_cust_<year> -> xfm_Std_Cust_<year> -> rmd_Cust_<year>
def staged_snapshot(year):
    raw = spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/customers_snapshot_{year}.csv")
    assert len(raw.columns) == 12, f"customers_snapshot_{year}.csv: expected 12 columns, got {len(raw.columns)}"
    # Source file: columns mapped by position, typed as declared
    typed = raw.toDF('CustomerID', 'CustomerName', 'Gender', 'Age', 'SpendingScore', 'AnnualIncomeK',
                     'Country', 'CustomerType', 'LoyaltyTier', 'Email', 'City', 'SnapshotDate').selectExpr(
        'CAST(`CustomerID` AS int) AS CustomerID',
        "coalesce(`CustomerName`, '') AS CustomerName",
        "coalesce(`Gender`, '') AS Gender",
        'CAST(`Age` AS int) AS Age',
        'CAST(`SpendingScore` AS int) AS SpendingScore',
        'CAST(`AnnualIncomeK` AS int) AS AnnualIncomeK',
        "coalesce(`Country`, '') AS Country",
        "coalesce(`CustomerType`, '') AS CustomerType",
        "coalesce(`LoyaltyTier`, '') AS LoyaltyTier",
        "coalesce(`Email`, '') AS Email",
        "coalesce(`City`, '') AS City",
        "coalesce(`SnapshotDate`, '') AS SnapshotDate")
    # Transformer: standardise and hash the tracked attributes
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
        "CAST(lower(trim(Email)) AS string))) AS bigint) AS attr_hash",
        "monotonically_increasing_id() AS _ord")
    # Remove Duplicates: keep first per customer_id (no upstream sort, so arrival order)
    dedup = (std.selectExpr("*", "row_number() OVER (PARTITION BY customer_id ORDER BY _ord) AS _rn")
                .where("_rn = 1"))
    return dedup.select('customer_id', 'cust_name', 'gender', 'age', 'spending_score', 'annual_income_k',
                        'country', 'customer_type', 'loyalty_tier', 'email', 'city', 'snapshot_date', 'attr_hash')


# tgt_stg_customers_2010: target file -> materialized view stg_customers_2010
@dp.materialized_view(name="stg_customers_2010")
def stg_customers_2010():
    return staged_snapshot(2010)


# tgt_stg_customers_2011: target file -> materialized view stg_customers_2011
@dp.materialized_view(name="stg_customers_2011")
def stg_customers_2011():
    return staged_snapshot(2011)


# Dimension history: SCD Type 2 maintained by Auto CDC from the successive snapshots
dp.create_streaming_table(name="dim_customer_history")


def next_snapshot_and_version(latest_version):
    # Version 1 = 2010 snapshot, version 2 = 2011 snapshot, then nothing more
    if latest_version is None:
        return (staged_snapshot(2010), 1)
    if latest_version == 1:
        return (staged_snapshot(2011), 2)
    return None


dp.create_auto_cdc_from_snapshot_flow(
    target="dim_customer_history",
    source=next_snapshot_and_version,
    keys=["customer_id"],
    stored_as_scd_type=2,
    track_history_column_list=["attr_hash"],
)


# Versions derived from the history table (replaces expire / carry / insert / funnel stages)
@dp.temporary_view(name="cust_history_versions")
def cust_history_versions():
    return spark.sql(f"""
WITH h AS (
  SELECT customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type,
         loyalty_tier, email, city, attr_hash,
         `__START_AT` AS start_version,
         `__END_AT` AS end_version,
         row_number() OVER (PARTITION BY customer_id ORDER BY `__START_AT`) AS version_no
  FROM dim_customer_history
)
SELECT *, max(version_no) OVER (PARTITION BY customer_id) AS max_version_no
FROM h
""")


# tgt_dim_customer (J05_Dim_Customer_Initial): version 1 = rows that started with the 2010 snapshot
@dp.materialized_view(name="dim_customer")
def dim_customer():
    return spark.sql(f"""
SELECT
  CAST(customer_id * 1000 + 1 AS int) AS customer_sk,
  customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type,
  loyalty_tier, email, city, attr_hash,
  CAST(1 AS int) AS version_no,
  CAST(19000101 AS int) AS eff_start_key,
  CAST(99991231 AS int) AS eff_end_key,
  CAST(1 AS int) AS is_current
FROM cust_history_versions
WHERE start_version = 1
ORDER BY customer_id, version_no
""")


# tgt_dim_customer_next (J06_Dim_Customer_SCD2): all versions after applying the 2011 snapshot
# Customers that disappear from the 2011 snapshot are dropped, as the job's left join from the snapshot does.
@dp.materialized_view(name="dim_customer_next")
def dim_customer_next():
    return spark.sql(f"""
SELECT
  CAST(customer_id * 1000 + version_no AS int) AS customer_sk,
  customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type,
  loyalty_tier, email, city, attr_hash,
  CAST(version_no AS int) AS version_no,
  CAST(CASE WHEN version_no = 1 THEN 19000101
            ELSE CAST(replace('{pEffDate}', '-', '') AS decimal(18,4)) END AS int) AS eff_start_key,
  CAST(CASE WHEN end_version IS NULL THEN 99991231
            ELSE CAST(replace(date_format(date_add(to_date('{pEffDate}', 'yyyy-MM-dd'), -1), 'yyyy-MM-dd'), '-', '') AS decimal(18,4))
       END AS int) AS eff_end_key,
  CAST(CASE WHEN end_version IS NULL THEN 1 ELSE 0 END AS int) AS is_current
FROM cust_history_versions
WHERE NOT (end_version IS NOT NULL AND version_no = max_version_no)
ORDER BY customer_id, version_no
""")


# agg_Change_Counts / tgt_audit_scd2_customer: change flags computed from the staged snapshots
# N = new customer, C = changed (hash differs), U = unchanged
@dp.materialized_view(name="audit_scd2_customer")
def audit_scd2_customer():
    new_snap = staged_snapshot(2011)
    old_snap = staged_snapshot(2010).selectExpr("customer_id AS old_customer_id", "attr_hash AS old_attr_hash")
    flagged = (new_snap.join(old_snap, new_snap["customer_id"] == old_snap["old_customer_id"], "left")
               .selectExpr("CAST(CASE WHEN old_attr_hash IS NULL THEN 'N' "
                           "WHEN old_attr_hash <> attr_hash THEN 'C' ELSE 'U' END AS string) AS change_flag"))
    return (flagged.groupBy("change_flag").count()
            .selectExpr("change_flag", "CAST(`count` AS int) AS customers"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
