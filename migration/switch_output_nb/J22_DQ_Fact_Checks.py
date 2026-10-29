# Databricks notebook source
# MAGIC %md
# MAGIC # J22_DQ_Fact_Checks
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028071346-rdb5/J22_DQ_Fact_Checks.txt`
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
# src_fact_sales: Delta table fact_sales (was #pDwhDir#/fact_sales.csv)
lnk_fact = spark.table(f"{catalog}.{schema}.fact_sales")
lnk_fact.createOrReplaceTempView("lnk_fact")

# COMMAND ----------
# cpy_Fact: Copy (output link lnk_chk1)
lnk_chk1 = spark.sql(f"""
SELECT * FROM lnk_fact
""")
lnk_chk1.createOrReplaceTempView("lnk_chk1")

# COMMAND ----------
# cpy_Fact: Copy (output link lnk_chk2)
lnk_chk2 = spark.sql(f"""
SELECT * FROM lnk_fact
""")
lnk_chk2.createOrReplaceTempView("lnk_chk2")

# COMMAND ----------
# cpy_Fact: Copy (output link lnk_chk3)
lnk_chk3 = spark.sql(f"""
SELECT * FROM lnk_fact
""")
lnk_chk3.createOrReplaceTempView("lnk_chk3")

# COMMAND ----------
# cpy_Fact: Copy (output link lnk_chk4)
lnk_chk4 = spark.sql(f"""
SELECT * FROM lnk_fact
""")
lnk_chk4.createOrReplaceTempView("lnk_chk4")

# COMMAND ----------
# flt_guest_customer_lines: Filter
lnk_hit1 = spark.sql(f"""
SELECT * FROM lnk_chk1
WHERE customer_sk = -1
""")
lnk_hit1.createOrReplaceTempView("lnk_hit1")

# COMMAND ----------
# xfm_Tag_guest_customer_lines: Transformer
lnk_tag1 = spark.sql(f"""
SELECT
  CAST('guest_customer_lines' AS string) AS check_name
FROM lnk_hit1
""")
lnk_tag1.createOrReplaceTempView("lnk_tag1")

# COMMAND ----------
# agg_Count_guest_customer_lines: Aggregator
lnk_cnt1 = spark.sql(f"""
SELECT check_name, CAST(COUNT(*) AS int) AS hits
FROM lnk_tag1
GROUP BY check_name
""")
lnk_cnt1.createOrReplaceTempView("lnk_cnt1")

# COMMAND ----------
# flt_orphan_product_lines: Filter
lnk_hit2 = spark.sql(f"""
SELECT * FROM lnk_chk2
WHERE product_sk = -1
""")
lnk_hit2.createOrReplaceTempView("lnk_hit2")

# COMMAND ----------
# xfm_Tag_orphan_product_lines: Transformer
lnk_tag2 = spark.sql(f"""
SELECT
  CAST('orphan_product_lines' AS string) AS check_name
FROM lnk_hit2
""")
lnk_tag2.createOrReplaceTempView("lnk_tag2")

# COMMAND ----------
# agg_Count_orphan_product_lines: Aggregator
lnk_cnt2 = spark.sql(f"""
SELECT check_name, CAST(COUNT(*) AS int) AS hits
FROM lnk_tag2
GROUP BY check_name
""")
lnk_cnt2.createOrReplaceTempView("lnk_cnt2")

# COMMAND ----------
# flt_non_positive_sales: Filter
lnk_hit3 = spark.sql(f"""
SELECT * FROM lnk_chk3
WHERE sales_amount <= 0
""")
lnk_hit3.createOrReplaceTempView("lnk_hit3")

# COMMAND ----------
# xfm_Tag_non_positive_sales: Transformer
lnk_tag3 = spark.sql(f"""
SELECT
  CAST('non_positive_sales' AS string) AS check_name
FROM lnk_hit3
""")
lnk_tag3.createOrReplaceTempView("lnk_tag3")

# COMMAND ----------
# agg_Count_non_positive_sales: Aggregator
lnk_cnt3 = spark.sql(f"""
SELECT check_name, CAST(COUNT(*) AS int) AS hits
FROM lnk_tag3
GROUP BY check_name
""")
lnk_cnt3.createOrReplaceTempView("lnk_cnt3")

# COMMAND ----------
# flt_negative_gross_profit: Filter
lnk_hit4 = spark.sql(f"""
SELECT * FROM lnk_chk4
WHERE gross_profit < 0
""")
lnk_hit4.createOrReplaceTempView("lnk_hit4")

# COMMAND ----------
# xfm_Tag_negative_gross_profit: Transformer
lnk_tag4 = spark.sql(f"""
SELECT
  CAST('negative_gross_profit' AS string) AS check_name
FROM lnk_hit4
""")
lnk_tag4.createOrReplaceTempView("lnk_tag4")

# COMMAND ----------
# agg_Count_negative_gross_profit: Aggregator
lnk_cnt4 = spark.sql(f"""
SELECT check_name, CAST(COUNT(*) AS int) AS hits
FROM lnk_tag4
GROUP BY check_name
""")
lnk_cnt4.createOrReplaceTempView("lnk_cnt4")

# COMMAND ----------
# fnl_Checks: Funnel (UNION ALL of the four count links)
lnk_all_checks = spark.sql(f"""
SELECT check_name, hits FROM lnk_cnt1
UNION ALL
SELECT check_name, hits FROM lnk_cnt2
UNION ALL
SELECT check_name, hits FROM lnk_cnt3
UNION ALL
SELECT check_name, hits FROM lnk_cnt4
""")
lnk_all_checks.createOrReplaceTempView("lnk_all_checks")

# COMMAND ----------
# srt_Checks: Sort
lnk_sorted = spark.sql(f"""
SELECT * FROM lnk_all_checks
ORDER BY check_name ASC
""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")

# COMMAND ----------
# tgt_dq_fact_checks: write Delta table dq_fact_checks
(lnk_sorted.select('check_name', 'hits').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dq_fact_checks"))
print("dq_fact_checks:", spark.table(f"{catalog}.{schema}.dq_fact_checks").count(), "rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
