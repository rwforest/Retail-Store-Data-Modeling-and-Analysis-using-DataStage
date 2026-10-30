# Databricks notebook source
# J05_Dim_Customer_Initial - Initial load of Dim_Customer (SCD2 version 1) from the 2010 snapshot
# Layer: 2_Dimensions. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_stg_customers_2010: dataset stg_customers_2010
@dp.temporary_view(name="j05_lnk_stg")
def j05_lnk_stg():
    return spark.read.table("stg_customers_2010")

# COMMAND ----------
# xfm_Version1: Transformer
@dp.temporary_view(name="j05_lnk_v1")
def j05_lnk_v1():
    return spark.sql(f"""
SELECT
  CAST(((customer_id * 1000) + 1) AS int) AS customer_sk,
  customer_id,
  cust_name,
  gender,
  age,
  spending_score,
  annual_income_k,
  country,
  customer_type,
  loyalty_tier,
  email,
  city,
  attr_hash,
  CAST(1 AS int) AS version_no,
  CAST(19000101 AS int) AS eff_start_key,
  CAST(99991231 AS int) AS eff_end_key,
  CAST(1 AS int) AS is_current
FROM j05_lnk_stg
""")

# COMMAND ----------
# srt_Dim_Customer: Sort
@dp.temporary_view(name="j05_lnk_sorted")
def j05_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j05_lnk_v1
ORDER BY customer_id ASC, version_no ASC
""")

# COMMAND ----------
# tgt_dim_customer: target file -> materialized view dim_customer
@dp.materialized_view(name="dim_customer")
def dim_customer():
    return spark.read.table("j05_lnk_sorted").select('customer_sk', 'customer_id', 'cust_name', 'gender', 'age', 'spending_score', 'annual_income_k', 'country', 'customer_type', 'loyalty_tier', 'email', 'city', 'attr_hash', 'version_no', 'eff_start_key', 'eff_end_key', 'is_current')
