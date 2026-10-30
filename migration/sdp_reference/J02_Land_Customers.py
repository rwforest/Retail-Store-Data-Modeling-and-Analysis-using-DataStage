# Databricks notebook source
# J02_Land_Customers - Land the two customer snapshots, standardise and hash tracked attributes
# Layer: 1_Land. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_cust_2010: source file customers_snapshot_2010.csv (columns mapped by position, as DataStage does)
@dp.temporary_view(name="j02_lnk_raw_2010")
def j02_lnk_raw_2010():
    raw = spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/customers_snapshot_2010.csv")
    assert len(raw.columns) == 12, f"customers_snapshot_2010.csv: expected 12 columns, got {len(raw.columns)}"
    return raw.toDF('CustomerID', 'CustomerName', 'Gender', 'Age', 'SpendingScore', 'AnnualIncomeK', 'Country', 'CustomerType', 'LoyaltyTier', 'Email', 'City', 'SnapshotDate').selectExpr(
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

# COMMAND ----------
# xfm_Std_Cust_2010: Transformer
@dp.temporary_view(name="j02_lnk_std_2010")
def j02_lnk_std_2010():
    return spark.sql(f"""
SELECT
  CustomerID AS customer_id,
  CAST(upper(trim(CustomerName)) AS string) AS cust_name,
  CAST(trim(Gender) AS string) AS gender,
  Age AS age,
  SpendingScore AS spending_score,
  AnnualIncomeK AS annual_income_k,
  CAST(trim(Country) AS string) AS country,
  CAST(trim(CustomerType) AS string) AS customer_type,
  CAST(upper(trim(LoyaltyTier)) AS string) AS loyalty_tier,
  CAST(lower(trim(Email)) AS string) AS email,
  CAST(trim(City) AS string) AS city,
  CAST(to_date(SnapshotDate, 'yyyy-MM-dd') AS date) AS snapshot_date,
  CAST(crc32(concat(CAST(upper(trim(CustomerName)) AS string), CAST('|' AS string), CAST(Age AS string), CAST('|' AS string), CAST(SpendingScore AS string), CAST('|' AS string), CAST(AnnualIncomeK AS string), CAST('|' AS string), CAST(trim(Country) AS string), CAST('|' AS string), CAST(upper(trim(LoyaltyTier)) AS string), CAST('|' AS string), CAST(trim(City) AS string), CAST('|' AS string), CAST(lower(trim(Email)) AS string))) AS bigint) AS attr_hash
FROM j02_lnk_raw_2010
""")

# COMMAND ----------
# rmd_Cust_2010: Remove Duplicates
@dp.temporary_view(name="j02_lnk_dedup_2010")
def j02_lnk_dedup_2010():
    return spark.sql(f"""
SELECT customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type, loyalty_tier, email, city, snapshot_date, attr_hash FROM (
  SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY monotonically_increasing_id()) AS _rn FROM j02_lnk_std_2010
) WHERE _rn = 1
""")

# COMMAND ----------
# tgt_stg_customers_2010: target file -> materialized view stg_customers_2010
@dp.materialized_view(name="stg_customers_2010")
def stg_customers_2010():
    return spark.read.table("j02_lnk_dedup_2010").select('customer_id', 'cust_name', 'gender', 'age', 'spending_score', 'annual_income_k', 'country', 'customer_type', 'loyalty_tier', 'email', 'city', 'snapshot_date', 'attr_hash')

# COMMAND ----------
# src_cust_2011: source file customers_snapshot_2011.csv (columns mapped by position, as DataStage does)
@dp.temporary_view(name="j02_lnk_raw_2011")
def j02_lnk_raw_2011():
    raw = spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/customers_snapshot_2011.csv")
    assert len(raw.columns) == 12, f"customers_snapshot_2011.csv: expected 12 columns, got {len(raw.columns)}"
    return raw.toDF('CustomerID', 'CustomerName', 'Gender', 'Age', 'SpendingScore', 'AnnualIncomeK', 'Country', 'CustomerType', 'LoyaltyTier', 'Email', 'City', 'SnapshotDate').selectExpr(
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

# COMMAND ----------
# xfm_Std_Cust_2011: Transformer
@dp.temporary_view(name="j02_lnk_std_2011")
def j02_lnk_std_2011():
    return spark.sql(f"""
SELECT
  CustomerID AS customer_id,
  CAST(upper(trim(CustomerName)) AS string) AS cust_name,
  CAST(trim(Gender) AS string) AS gender,
  Age AS age,
  SpendingScore AS spending_score,
  AnnualIncomeK AS annual_income_k,
  CAST(trim(Country) AS string) AS country,
  CAST(trim(CustomerType) AS string) AS customer_type,
  CAST(upper(trim(LoyaltyTier)) AS string) AS loyalty_tier,
  CAST(lower(trim(Email)) AS string) AS email,
  CAST(trim(City) AS string) AS city,
  CAST(to_date(SnapshotDate, 'yyyy-MM-dd') AS date) AS snapshot_date,
  CAST(crc32(concat(CAST(upper(trim(CustomerName)) AS string), CAST('|' AS string), CAST(Age AS string), CAST('|' AS string), CAST(SpendingScore AS string), CAST('|' AS string), CAST(AnnualIncomeK AS string), CAST('|' AS string), CAST(trim(Country) AS string), CAST('|' AS string), CAST(upper(trim(LoyaltyTier)) AS string), CAST('|' AS string), CAST(trim(City) AS string), CAST('|' AS string), CAST(lower(trim(Email)) AS string))) AS bigint) AS attr_hash
FROM j02_lnk_raw_2011
""")

# COMMAND ----------
# rmd_Cust_2011: Remove Duplicates
@dp.temporary_view(name="j02_lnk_dedup_2011")
def j02_lnk_dedup_2011():
    return spark.sql(f"""
SELECT customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type, loyalty_tier, email, city, snapshot_date, attr_hash FROM (
  SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY monotonically_increasing_id()) AS _rn FROM j02_lnk_std_2011
) WHERE _rn = 1
""")

# COMMAND ----------
# tgt_stg_customers_2011: target file -> materialized view stg_customers_2011
@dp.materialized_view(name="stg_customers_2011")
def stg_customers_2011():
    return spark.read.table("j02_lnk_dedup_2011").select('customer_id', 'cust_name', 'gender', 'age', 'spending_score', 'annual_income_k', 'country', 'customer_type', 'loyalty_tier', 'email', 'city', 'snapshot_date', 'attr_hash')
