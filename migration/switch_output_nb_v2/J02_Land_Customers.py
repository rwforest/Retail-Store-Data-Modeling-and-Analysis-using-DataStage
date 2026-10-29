# Databricks notebook source
# MAGIC %md
# MAGIC # J02_Land_Customers
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028235854-szp7/J02_Land_Customers.txt`
# COMMAND ----------
# COMMAND ----------
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "retail_dw")
dbutils.widgets.text("src_dir", "")
dbutils.widgets.text("pRunDate", "2011-12-31")
dbutils.widgets.text("pEffDate", "2011-07-01")
catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))
pRunDate, pEffDate = dbutils.widgets.get("pRunDate"), dbutils.widgets.get("pEffDate")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------
# src_cust_2010: source file customers_snapshot_2010.csv (columns mapped by position, as DataStage does)
_lnk_raw_2010_raw = (spark.read.option("header", "true").option("escape", '"')
    .csv(f"{src_dir}/customers_snapshot_2010.csv"))
assert len(_lnk_raw_2010_raw.columns) == 12, f"customers_snapshot_2010.csv: expected 12 columns, got {len(_lnk_raw_2010_raw.columns)}"
_lnk_raw_2010_raw = _lnk_raw_2010_raw.toDF('CustomerID', 'CustomerName', 'Gender', 'Age', 'SpendingScore', 'AnnualIncomeK', 'Country', 'CustomerType', 'LoyaltyTier', 'Email', 'City', 'SnapshotDate')
_lnk_raw_2010_raw.createOrReplaceTempView("_lnk_raw_2010_raw")
lnk_raw_2010 = spark.sql(f"""
SELECT
  CAST(`CustomerID` AS int) AS CustomerID,
  CAST(`CustomerName` AS string) AS CustomerName,
  CAST(`Gender` AS string) AS Gender,
  CAST(`Age` AS int) AS Age,
  CAST(`SpendingScore` AS int) AS SpendingScore,
  CAST(`AnnualIncomeK` AS int) AS AnnualIncomeK,
  CAST(`Country` AS string) AS Country,
  CAST(`CustomerType` AS string) AS CustomerType,
  CAST(`LoyaltyTier` AS string) AS LoyaltyTier,
  CAST(`Email` AS string) AS Email,
  CAST(`City` AS string) AS City,
  CAST(`SnapshotDate` AS string) AS SnapshotDate
FROM _lnk_raw_2010_raw
""")
lnk_raw_2010.createOrReplaceTempView("lnk_raw_2010")

# COMMAND ----------
# xfm_Std_Cust_2010: Transformer
lnk_std_2010 = spark.sql(f"""
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
  CAST(crc32(concat(CAST(upper(trim(CustomerName)) AS string), '|', CAST(Age AS string), '|', CAST(SpendingScore AS string), '|', CAST(AnnualIncomeK AS string), '|', CAST(trim(Country) AS string), '|', CAST(upper(trim(LoyaltyTier)) AS string), '|', CAST(trim(City) AS string), '|', CAST(lower(trim(Email)) AS string))) AS bigint) AS attr_hash
FROM lnk_raw_2010
""")
lnk_std_2010.createOrReplaceTempView("lnk_std_2010")

# COMMAND ----------
# rmd_Cust_2010: Remove Duplicates (key customer_id, keep first; no Sort upstream -> monotonically_increasing_id order)
lnk_dedup_2010 = spark.sql(f"""
SELECT customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type, loyalty_tier, email, city, snapshot_date, attr_hash
FROM (
  SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY _mid) AS _rn
  FROM (SELECT *, monotonically_increasing_id() AS _mid FROM lnk_std_2010)
)
WHERE _rn = 1
""")
lnk_dedup_2010.createOrReplaceTempView("lnk_dedup_2010")

# COMMAND ----------
# tgt_stg_customers_2010: write Delta table stg_customers_2010
(lnk_dedup_2010.select('customer_id', 'cust_name', 'gender', 'age', 'spending_score', 'annual_income_k', 'country', 'customer_type', 'loyalty_tier', 'email', 'city', 'snapshot_date', 'attr_hash').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.stg_customers_2010"))
print("stg_customers_2010:", spark.table(f"{catalog}.{schema}.stg_customers_2010").count(), "rows")

# COMMAND ----------
# src_cust_2011: source file customers_snapshot_2011.csv (columns mapped by position, as DataStage does)
_lnk_raw_2011_raw = (spark.read.option("header", "true").option("escape", '"')
    .csv(f"{src_dir}/customers_snapshot_2011.csv"))
assert len(_lnk_raw_2011_raw.columns) == 12, f"customers_snapshot_2011.csv: expected 12 columns, got {len(_lnk_raw_2011_raw.columns)}"
_lnk_raw_2011_raw = _lnk_raw_2011_raw.toDF('CustomerID', 'CustomerName', 'Gender', 'Age', 'SpendingScore', 'AnnualIncomeK', 'Country', 'CustomerType', 'LoyaltyTier', 'Email', 'City', 'SnapshotDate')
_lnk_raw_2011_raw.createOrReplaceTempView("_lnk_raw_2011_raw")
lnk_raw_2011 = spark.sql(f"""
SELECT
  CAST(`CustomerID` AS int) AS CustomerID,
  CAST(`CustomerName` AS string) AS CustomerName,
  CAST(`Gender` AS string) AS Gender,
  CAST(`Age` AS int) AS Age,
  CAST(`SpendingScore` AS int) AS SpendingScore,
  CAST(`AnnualIncomeK` AS int) AS AnnualIncomeK,
  CAST(`Country` AS string) AS Country,
  CAST(`CustomerType` AS string) AS CustomerType,
  CAST(`LoyaltyTier` AS string) AS LoyaltyTier,
  CAST(`Email` AS string) AS Email,
  CAST(`City` AS string) AS City,
  CAST(`SnapshotDate` AS string) AS SnapshotDate
FROM _lnk_raw_2011_raw
""")
lnk_raw_2011.createOrReplaceTempView("lnk_raw_2011")

# COMMAND ----------
# xfm_Std_Cust_2011: Transformer
lnk_std_2011 = spark.sql(f"""
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
  CAST(crc32(concat(CAST(upper(trim(CustomerName)) AS string), '|', CAST(Age AS string), '|', CAST(SpendingScore AS string), '|', CAST(AnnualIncomeK AS string), '|', CAST(trim(Country) AS string), '|', CAST(upper(trim(LoyaltyTier)) AS string), '|', CAST(trim(City) AS string), '|', CAST(lower(trim(Email)) AS string))) AS bigint) AS attr_hash
FROM lnk_raw_2011
""")
lnk_std_2011.createOrReplaceTempView("lnk_std_2011")

# COMMAND ----------
# rmd_Cust_2011: Remove Duplicates (key customer_id, keep first; no Sort upstream -> monotonically_increasing_id order)
lnk_dedup_2011 = spark.sql(f"""
SELECT customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type, loyalty_tier, email, city, snapshot_date, attr_hash
FROM (
  SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY _mid) AS _rn
  FROM (SELECT *, monotonically_increasing_id() AS _mid FROM lnk_std_2011)
)
WHERE _rn = 1
""")
lnk_dedup_2011.createOrReplaceTempView("lnk_dedup_2011")

# COMMAND ----------
# tgt_stg_customers_2011: write Delta table stg_customers_2011
(lnk_dedup_2011.select('customer_id', 'cust_name', 'gender', 'age', 'spending_score', 'annual_income_k', 'country', 'customer_type', 'loyalty_tier', 'email', 'city', 'snapshot_date', 'attr_hash').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.stg_customers_2011"))
print("stg_customers_2011:", spark.table(f"{catalog}.{schema}.stg_customers_2011").count(), "rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
