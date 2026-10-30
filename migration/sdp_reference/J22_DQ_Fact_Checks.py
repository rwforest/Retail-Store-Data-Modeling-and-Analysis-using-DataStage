# Databricks notebook source
# J22_DQ_Fact_Checks - Row-level data-quality rules on Fact_Sales, one result row per rule
# Layer: 5_DataQuality. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_fact_sales: dataset fact_sales
@dp.temporary_view(name="j22_lnk_fact")
def j22_lnk_fact():
    return spark.read.table("fact_sales")

# COMMAND ----------
# cpy_Fact: Copy
@dp.temporary_view(name="j22_lnk_chk1")
def j22_lnk_chk1():
    return spark.sql(f"""
SELECT * FROM j22_lnk_fact
""")

# COMMAND ----------
# cpy_Fact: Copy
@dp.temporary_view(name="j22_lnk_chk2")
def j22_lnk_chk2():
    return spark.sql(f"""
SELECT * FROM j22_lnk_fact
""")

# COMMAND ----------
# cpy_Fact: Copy
@dp.temporary_view(name="j22_lnk_chk3")
def j22_lnk_chk3():
    return spark.sql(f"""
SELECT * FROM j22_lnk_fact
""")

# COMMAND ----------
# cpy_Fact: Copy
@dp.temporary_view(name="j22_lnk_chk4")
def j22_lnk_chk4():
    return spark.sql(f"""
SELECT * FROM j22_lnk_fact
""")

# COMMAND ----------
# flt_guest_customer_lines: Filter
@dp.temporary_view(name="j22_lnk_hit1")
def j22_lnk_hit1():
    return spark.sql(f"""
SELECT * FROM j22_lnk_chk1
WHERE (customer_sk = (-1))
""")

# COMMAND ----------
# xfm_Tag_guest_customer_lines: Transformer
@dp.temporary_view(name="j22_lnk_tag1")
def j22_lnk_tag1():
    return spark.sql(f"""
SELECT
  CAST('guest_customer_lines' AS string) AS check_name
FROM j22_lnk_hit1
""")

# COMMAND ----------
# agg_Count_guest_customer_lines: Aggregator
@dp.temporary_view(name="j22_lnk_cnt1")
def j22_lnk_cnt1():
    return spark.sql(f"""
SELECT check_name, CAST(COUNT(*) AS int) AS hits
FROM j22_lnk_tag1
GROUP BY check_name
""")

# COMMAND ----------
# flt_orphan_product_lines: Filter
@dp.temporary_view(name="j22_lnk_hit2")
def j22_lnk_hit2():
    return spark.sql(f"""
SELECT * FROM j22_lnk_chk2
WHERE (product_sk = (-1))
""")

# COMMAND ----------
# xfm_Tag_orphan_product_lines: Transformer
@dp.temporary_view(name="j22_lnk_tag2")
def j22_lnk_tag2():
    return spark.sql(f"""
SELECT
  CAST('orphan_product_lines' AS string) AS check_name
FROM j22_lnk_hit2
""")

# COMMAND ----------
# agg_Count_orphan_product_lines: Aggregator
@dp.temporary_view(name="j22_lnk_cnt2")
def j22_lnk_cnt2():
    return spark.sql(f"""
SELECT check_name, CAST(COUNT(*) AS int) AS hits
FROM j22_lnk_tag2
GROUP BY check_name
""")

# COMMAND ----------
# flt_non_positive_sales: Filter
@dp.temporary_view(name="j22_lnk_hit3")
def j22_lnk_hit3():
    return spark.sql(f"""
SELECT * FROM j22_lnk_chk3
WHERE (sales_amount <= 0)
""")

# COMMAND ----------
# xfm_Tag_non_positive_sales: Transformer
@dp.temporary_view(name="j22_lnk_tag3")
def j22_lnk_tag3():
    return spark.sql(f"""
SELECT
  CAST('non_positive_sales' AS string) AS check_name
FROM j22_lnk_hit3
""")

# COMMAND ----------
# agg_Count_non_positive_sales: Aggregator
@dp.temporary_view(name="j22_lnk_cnt3")
def j22_lnk_cnt3():
    return spark.sql(f"""
SELECT check_name, CAST(COUNT(*) AS int) AS hits
FROM j22_lnk_tag3
GROUP BY check_name
""")

# COMMAND ----------
# flt_negative_gross_profit: Filter
@dp.temporary_view(name="j22_lnk_hit4")
def j22_lnk_hit4():
    return spark.sql(f"""
SELECT * FROM j22_lnk_chk4
WHERE (gross_profit < 0)
""")

# COMMAND ----------
# xfm_Tag_negative_gross_profit: Transformer
@dp.temporary_view(name="j22_lnk_tag4")
def j22_lnk_tag4():
    return spark.sql(f"""
SELECT
  CAST('negative_gross_profit' AS string) AS check_name
FROM j22_lnk_hit4
""")

# COMMAND ----------
# agg_Count_negative_gross_profit: Aggregator
@dp.temporary_view(name="j22_lnk_cnt4")
def j22_lnk_cnt4():
    return spark.sql(f"""
SELECT check_name, CAST(COUNT(*) AS int) AS hits
FROM j22_lnk_tag4
GROUP BY check_name
""")

# COMMAND ----------
# fnl_Checks: Funnel
@dp.temporary_view(name="j22_lnk_all_checks")
def j22_lnk_all_checks():
    return spark.sql(f"""
SELECT check_name, hits FROM j22_lnk_cnt1
UNION ALL
SELECT check_name, hits FROM j22_lnk_cnt2
UNION ALL
SELECT check_name, hits FROM j22_lnk_cnt3
UNION ALL
SELECT check_name, hits FROM j22_lnk_cnt4
""")

# COMMAND ----------
# srt_Checks: Sort
@dp.temporary_view(name="j22_lnk_sorted")
def j22_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j22_lnk_all_checks
ORDER BY check_name ASC
""")

# COMMAND ----------
# tgt_dq_fact_checks: target file -> materialized view dq_fact_checks
@dp.materialized_view(name="dq_fact_checks")
def dq_fact_checks():
    return spark.read.table("j22_lnk_sorted").select('check_name', 'hits')
