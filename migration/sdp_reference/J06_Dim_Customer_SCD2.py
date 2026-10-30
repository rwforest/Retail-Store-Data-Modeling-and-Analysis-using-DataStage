# Databricks notebook source
# J06_Dim_Customer_SCD2 - Apply the 2011 snapshot to Dim_Customer: expire changed rows, add new versions
# Layer: 2_Dimensions. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_dim_customer: dataset dim_customer
@dp.temporary_view(name="j06_lnk_dim")
def j06_lnk_dim():
    return spark.read.table("dim_customer")

# COMMAND ----------
# cpy_Dim: Copy
@dp.temporary_view(name="j06_lnk_dim_a")
def j06_lnk_dim_a():
    return spark.sql(f"""
SELECT * FROM j06_lnk_dim
""")

# COMMAND ----------
# cpy_Dim: Copy
@dp.temporary_view(name="j06_lnk_dim_b")
def j06_lnk_dim_b():
    return spark.sql(f"""
SELECT * FROM j06_lnk_dim
""")

# COMMAND ----------
# flt_Current_Rows: Filter
@dp.temporary_view(name="j06_lnk_current")
def j06_lnk_current():
    return spark.sql(f"""
SELECT * FROM j06_lnk_dim_a
WHERE (is_current = 1)
""")

# COMMAND ----------
# flt_History_Rows: Filter
@dp.temporary_view(name="j06_lnk_history")
def j06_lnk_history():
    return spark.sql(f"""
SELECT * FROM j06_lnk_dim_b
WHERE (is_current = 0)
""")

# COMMAND ----------
# xfm_Prefix_Old: Transformer
@dp.temporary_view(name="j06_lnk_old")
def j06_lnk_old():
    return spark.sql(f"""
SELECT
  customer_id,
  customer_sk AS old_customer_sk,
  cust_name AS old_cust_name,
  gender AS old_gender,
  age AS old_age,
  spending_score AS old_spending_score,
  annual_income_k AS old_annual_income_k,
  country AS old_country,
  customer_type AS old_customer_type,
  loyalty_tier AS old_loyalty_tier,
  email AS old_email,
  city AS old_city,
  attr_hash AS old_attr_hash,
  version_no AS old_version_no,
  eff_start_key AS old_eff_start_key,
  eff_end_key AS old_eff_end_key,
  is_current AS old_is_current
FROM j06_lnk_current
""")

# COMMAND ----------
# src_stg_customers_2011: dataset stg_customers_2011
@dp.temporary_view(name="j06_lnk_snap")
def j06_lnk_snap():
    return spark.read.table("stg_customers_2011")

# COMMAND ----------
# jn_Snapshot_vs_Current: Join (left outer) on customer_id
@dp.temporary_view(name="j06_lnk_cmp")
def j06_lnk_cmp():
    return spark.sql(f"""
SELECT
  l.customer_id,
  l.cust_name,
  l.gender,
  l.age,
  l.spending_score,
  l.annual_income_k,
  l.country,
  l.customer_type,
  l.loyalty_tier,
  l.email,
  l.city,
  l.snapshot_date,
  l.attr_hash,
  r.old_customer_sk,
  r.old_cust_name,
  r.old_gender,
  r.old_age,
  r.old_spending_score,
  r.old_annual_income_k,
  r.old_country,
  r.old_customer_type,
  r.old_loyalty_tier,
  r.old_email,
  r.old_city,
  r.old_attr_hash,
  r.old_version_no,
  r.old_eff_start_key,
  r.old_eff_end_key,
  r.old_is_current
FROM j06_lnk_snap l
LEFT OUTER JOIN j06_lnk_old r ON l.customer_id = r.customer_id
""")

# COMMAND ----------
# xfm_Detect_Change: Transformer
@dp.temporary_view(name="j06_lnk_detected")
def j06_lnk_detected():
    return spark.sql(f"""
SELECT
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
  snapshot_date,
  attr_hash,
  CAST(coalesce(old_customer_sk, 0) AS int) AS old_customer_sk,
  CAST(coalesce(old_cust_name, '') AS string) AS old_cust_name,
  CAST(coalesce(old_gender, '') AS string) AS old_gender,
  CAST(coalesce(old_age, 0) AS int) AS old_age,
  CAST(coalesce(old_spending_score, 0) AS int) AS old_spending_score,
  CAST(coalesce(old_annual_income_k, 0) AS int) AS old_annual_income_k,
  CAST(coalesce(old_country, '') AS string) AS old_country,
  CAST(coalesce(old_customer_type, '') AS string) AS old_customer_type,
  CAST(coalesce(old_loyalty_tier, '') AS string) AS old_loyalty_tier,
  CAST(coalesce(old_email, '') AS string) AS old_email,
  CAST(coalesce(old_city, '') AS string) AS old_city,
  CAST(coalesce(old_attr_hash, 0) AS bigint) AS old_attr_hash,
  CAST(coalesce(old_version_no, 0) AS int) AS old_version_no,
  CAST(coalesce(old_eff_start_key, 0) AS int) AS old_eff_start_key,
  CAST(coalesce(old_eff_end_key, 0) AS int) AS old_eff_end_key,
  CAST(coalesce(old_is_current, 0) AS int) AS old_is_current,
  CAST(CASE WHEN (old_attr_hash IS NULL) THEN 'N' ELSE CASE WHEN (old_attr_hash <> attr_hash) THEN 'C' ELSE 'U' END END AS string) AS change_flag
FROM j06_lnk_cmp
""")

# COMMAND ----------
# cpy_Detect: Copy
@dp.temporary_view(name="j06_lnk_u")
def j06_lnk_u():
    return spark.sql(f"""
SELECT * FROM j06_lnk_detected
""")

# COMMAND ----------
# cpy_Detect: Copy
@dp.temporary_view(name="j06_lnk_nc")
def j06_lnk_nc():
    return spark.sql(f"""
SELECT * FROM j06_lnk_detected
""")

# COMMAND ----------
# cpy_Detect: Copy
@dp.temporary_view(name="j06_lnk_c")
def j06_lnk_c():
    return spark.sql(f"""
SELECT * FROM j06_lnk_detected
""")

# COMMAND ----------
# cpy_Detect: Copy
@dp.temporary_view(name="j06_lnk_audit")
def j06_lnk_audit():
    return spark.sql(f"""
SELECT * FROM j06_lnk_detected
""")

# COMMAND ----------
# flt_Unchanged: Filter
@dp.temporary_view(name="j06_lnk_unchanged")
def j06_lnk_unchanged():
    return spark.sql(f"""
SELECT * FROM j06_lnk_u
WHERE (change_flag = 'U')
""")

# COMMAND ----------
# flt_New_Or_Changed: Filter
@dp.temporary_view(name="j06_lnk_new_or_changed")
def j06_lnk_new_or_changed():
    return spark.sql(f"""
SELECT * FROM j06_lnk_nc
WHERE (change_flag <> 'U')
""")

# COMMAND ----------
# flt_Changed: Filter
@dp.temporary_view(name="j06_lnk_changed")
def j06_lnk_changed():
    return spark.sql(f"""
SELECT * FROM j06_lnk_c
WHERE (change_flag = 'C')
""")

# COMMAND ----------
# xfm_Carry_Unchanged: Transformer
@dp.temporary_view(name="j06_lnk_carry")
def j06_lnk_carry():
    return spark.sql(f"""
SELECT
  old_customer_sk AS customer_sk,
  customer_id,
  old_cust_name AS cust_name,
  old_gender AS gender,
  old_age AS age,
  old_spending_score AS spending_score,
  old_annual_income_k AS annual_income_k,
  old_country AS country,
  old_customer_type AS customer_type,
  old_loyalty_tier AS loyalty_tier,
  old_email AS email,
  old_city AS city,
  old_attr_hash AS attr_hash,
  old_version_no AS version_no,
  old_eff_start_key AS eff_start_key,
  old_eff_end_key AS eff_end_key,
  old_is_current AS is_current
FROM j06_lnk_unchanged
""")

# COMMAND ----------
# xfm_Expire_Old: Transformer
@dp.temporary_view(name="j06_lnk_expired")
def j06_lnk_expired():
    return spark.sql(f"""
SELECT
  old_customer_sk AS customer_sk,
  customer_id,
  old_cust_name AS cust_name,
  old_gender AS gender,
  old_age AS age,
  old_spending_score AS spending_score,
  old_annual_income_k AS annual_income_k,
  old_country AS country,
  old_customer_type AS customer_type,
  old_loyalty_tier AS loyalty_tier,
  old_email AS email,
  old_city AS city,
  old_attr_hash AS attr_hash,
  old_version_no AS version_no,
  old_eff_start_key AS eff_start_key,
  CAST(CAST(replace(date_format(date_add(to_date('{pEffDate}', 'yyyy-MM-dd'), (-1)), 'yyyy-MM-dd'), '-', '') AS decimal(18,4)) AS int) AS eff_end_key,
  CAST(0 AS int) AS is_current
FROM j06_lnk_changed
""")

# COMMAND ----------
# xfm_New_Version: Transformer
@dp.temporary_view(name="j06_lnk_newver")
def j06_lnk_newver():
    return spark.sql(f"""
SELECT
  CAST((((customer_id * 1000) + old_version_no) + 1) AS int) AS customer_sk,
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
  CAST((old_version_no + 1) AS int) AS version_no,
  CAST(CASE WHEN (old_version_no = 0) THEN 19000101 ELSE CAST(replace('{pEffDate}', '-', '') AS decimal(18,4)) END AS int) AS eff_start_key,
  CAST(99991231 AS int) AS eff_end_key,
  CAST(1 AS int) AS is_current
FROM j06_lnk_new_or_changed
""")

# COMMAND ----------
# fnl_Dim_Next: Funnel
@dp.temporary_view(name="j06_lnk_all_versions")
def j06_lnk_all_versions():
    return spark.sql(f"""
SELECT customer_sk, customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type, loyalty_tier, email, city, attr_hash, version_no, eff_start_key, eff_end_key, is_current FROM j06_lnk_history
UNION ALL
SELECT customer_sk, customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type, loyalty_tier, email, city, attr_hash, version_no, eff_start_key, eff_end_key, is_current FROM j06_lnk_carry
UNION ALL
SELECT customer_sk, customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type, loyalty_tier, email, city, attr_hash, version_no, eff_start_key, eff_end_key, is_current FROM j06_lnk_expired
UNION ALL
SELECT customer_sk, customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type, loyalty_tier, email, city, attr_hash, version_no, eff_start_key, eff_end_key, is_current FROM j06_lnk_newver
""")

# COMMAND ----------
# srt_Dim_Next: Sort
@dp.temporary_view(name="j06_lnk_sorted")
def j06_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j06_lnk_all_versions
ORDER BY customer_id ASC, version_no ASC
""")

# COMMAND ----------
# tgt_dim_customer_next: target file -> materialized view dim_customer_next
@dp.materialized_view(name="dim_customer_next")
def dim_customer_next():
    return spark.read.table("j06_lnk_sorted").select('customer_sk', 'customer_id', 'cust_name', 'gender', 'age', 'spending_score', 'annual_income_k', 'country', 'customer_type', 'loyalty_tier', 'email', 'city', 'attr_hash', 'version_no', 'eff_start_key', 'eff_end_key', 'is_current')

# COMMAND ----------
# agg_Change_Counts: Aggregator
@dp.temporary_view(name="j06_lnk_change_counts")
def j06_lnk_change_counts():
    return spark.sql(f"""
SELECT change_flag, CAST(COUNT(*) AS int) AS customers
FROM j06_lnk_audit
GROUP BY change_flag
""")

# COMMAND ----------
# tgt_audit_scd2_customer: target file -> materialized view audit_scd2_customer
@dp.materialized_view(name="audit_scd2_customer")
def audit_scd2_customer():
    return spark.read.table("j06_lnk_change_counts").select('change_flag', 'customers')
