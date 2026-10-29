# Databricks notebook source
# MAGIC %md
# MAGIC # J06_Dim_Customer_SCD2
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028071346-rdb5/J06_Dim_Customer_SCD2.txt`
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
# src_dim_customer: Delta table dim_customer (was #pDwhDir#/dim_customer.csv)
lnk_dim = spark.table(f"{catalog}.{schema}.dim_customer")
lnk_dim.createOrReplaceTempView("lnk_dim")

# COMMAND ----------
# cpy_Dim: Copy (output link lnk_dim_a)
lnk_dim_a = spark.sql(f"""
SELECT * FROM lnk_dim
""")
lnk_dim_a.createOrReplaceTempView("lnk_dim_a")

# COMMAND ----------
# cpy_Dim: Copy (output link lnk_dim_b)
lnk_dim_b = spark.sql(f"""
SELECT * FROM lnk_dim
""")
lnk_dim_b.createOrReplaceTempView("lnk_dim_b")

# COMMAND ----------
# flt_Current_Rows: Filter
lnk_current = spark.sql(f"""
SELECT * FROM lnk_dim_a
WHERE is_current = 1
""")
lnk_current.createOrReplaceTempView("lnk_current")

# COMMAND ----------
# flt_History_Rows: Filter
lnk_history = spark.sql(f"""
SELECT * FROM lnk_dim_b
WHERE is_current = 0
""")
lnk_history.createOrReplaceTempView("lnk_history")

# COMMAND ----------
# xfm_Prefix_Old: Transformer
lnk_old = spark.sql(f"""
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
FROM lnk_current
""")
lnk_old.createOrReplaceTempView("lnk_old")

# COMMAND ----------
# src_stg_customers_2011: Delta table stg_customers_2011 (was #pStgDir#/stg_customers_2011.csv)
lnk_snap = spark.table(f"{catalog}.{schema}.stg_customers_2011")
lnk_snap.createOrReplaceTempView("lnk_snap")

# COMMAND ----------
# jn_Snapshot_vs_Current: Join (left outer) on customer_id
lnk_cmp = spark.sql(f"""
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
FROM lnk_snap l
LEFT OUTER JOIN lnk_old r ON l.customer_id = r.customer_id
""")
lnk_cmp.createOrReplaceTempView("lnk_cmp")

# COMMAND ----------
# xfm_Detect_Change: Transformer
lnk_detected = spark.sql(f"""
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
FROM lnk_cmp
""")
lnk_detected.createOrReplaceTempView("lnk_detected")

# COMMAND ----------
# cpy_Detect: Copy (output link lnk_u)
lnk_u = spark.sql(f"""
SELECT * FROM lnk_detected
""")
lnk_u.createOrReplaceTempView("lnk_u")

# COMMAND ----------
# cpy_Detect: Copy (output link lnk_nc)
lnk_nc = spark.sql(f"""
SELECT * FROM lnk_detected
""")
lnk_nc.createOrReplaceTempView("lnk_nc")

# COMMAND ----------
# cpy_Detect: Copy (output link lnk_c)
lnk_c = spark.sql(f"""
SELECT * FROM lnk_detected
""")
lnk_c.createOrReplaceTempView("lnk_c")

# COMMAND ----------
# cpy_Detect: Copy (output link lnk_audit)
lnk_audit = spark.sql(f"""
SELECT * FROM lnk_detected
""")
lnk_audit.createOrReplaceTempView("lnk_audit")

# COMMAND ----------
# flt_Unchanged: Filter
lnk_unchanged = spark.sql(f"""
SELECT * FROM lnk_u
WHERE change_flag = 'U'
""")
lnk_unchanged.createOrReplaceTempView("lnk_unchanged")

# COMMAND ----------
# flt_New_Or_Changed: Filter
lnk_new_or_changed = spark.sql(f"""
SELECT * FROM lnk_nc
WHERE change_flag <> 'U'
""")
lnk_new_or_changed.createOrReplaceTempView("lnk_new_or_changed")

# COMMAND ----------
# flt_Changed: Filter
lnk_changed = spark.sql(f"""
SELECT * FROM lnk_c
WHERE change_flag = 'C'
""")
lnk_changed.createOrReplaceTempView("lnk_changed")

# COMMAND ----------
# xfm_Carry_Unchanged: Transformer
lnk_carry = spark.sql(f"""
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
FROM lnk_unchanged
""")
lnk_carry.createOrReplaceTempView("lnk_carry")

# COMMAND ----------
# xfm_Expire_Old: Transformer
lnk_expired = spark.sql(f"""
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
  CAST(CAST(replace(date_format(date_add(to_date('{pEffDate}', 'yyyy-MM-dd'), -1), 'yyyy-MM-dd'), '-', '') AS decimal(18,4)) AS int) AS eff_end_key,
  CAST(0 AS int) AS is_current
FROM lnk_changed
""")
lnk_expired.createOrReplaceTempView("lnk_expired")

# COMMAND ----------
# xfm_New_Version: Transformer
lnk_newver = spark.sql(f"""
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
FROM lnk_new_or_changed
""")
lnk_newver.createOrReplaceTempView("lnk_newver")

# COMMAND ----------
# fnl_Dim_Next: Funnel (UNION ALL of lnk_history, lnk_carry, lnk_expired, lnk_newver)
lnk_all_versions = spark.sql(f"""
SELECT customer_sk, customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type, loyalty_tier, email, city, attr_hash, version_no, eff_start_key, eff_end_key, is_current FROM lnk_history
UNION ALL
SELECT customer_sk, customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type, loyalty_tier, email, city, attr_hash, version_no, eff_start_key, eff_end_key, is_current FROM lnk_carry
UNION ALL
SELECT customer_sk, customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type, loyalty_tier, email, city, attr_hash, version_no, eff_start_key, eff_end_key, is_current FROM lnk_expired
UNION ALL
SELECT customer_sk, customer_id, cust_name, gender, age, spending_score, annual_income_k, country, customer_type, loyalty_tier, email, city, attr_hash, version_no, eff_start_key, eff_end_key, is_current FROM lnk_newver
""")
lnk_all_versions.createOrReplaceTempView("lnk_all_versions")

# COMMAND ----------
# srt_Dim_Next: Sort
lnk_sorted = spark.sql(f"""
SELECT * FROM lnk_all_versions
ORDER BY customer_id ASC, version_no ASC
""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")

# COMMAND ----------
# tgt_dim_customer_next: write Delta table dim_customer_next
(lnk_sorted.select('customer_sk', 'customer_id', 'cust_name', 'gender', 'age', 'spending_score', 'annual_income_k', 'country', 'customer_type', 'loyalty_tier', 'email', 'city', 'attr_hash', 'version_no', 'eff_start_key', 'eff_end_key', 'is_current').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_customer_next"))
print("dim_customer_next:", spark.table(f"{catalog}.{schema}.dim_customer_next").count(), "rows")

# COMMAND ----------
# agg_Change_Counts: Aggregator
lnk_change_counts = spark.sql(f"""
SELECT change_flag, CAST(COUNT(*) AS int) AS customers
FROM lnk_audit
GROUP BY change_flag
""")
lnk_change_counts.createOrReplaceTempView("lnk_change_counts")

# COMMAND ----------
# tgt_audit_scd2_customer: write Delta table audit_scd2_customer
(lnk_change_counts.select('change_flag', 'customers').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.audit_scd2_customer"))
print("audit_scd2_customer:", spark.table(f"{catalog}.{schema}.audit_scd2_customer").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
