# Databricks notebook source
# J23_Reconcile_Source_To_Fact - Reconcile staged order amounts to Fact_Sales amounts
# Layer: 5_DataQuality. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_stg_orders: dataset stg_orders
@dp.temporary_view(name="j23_lnk_orders")
def j23_lnk_orders():
    return spark.read.table("stg_orders")

# COMMAND ----------
# flt_Sales_Lines: Filter
@dp.temporary_view(name="j23_lnk_lines")
def j23_lnk_lines():
    return spark.sql(f"""
SELECT * FROM j23_lnk_orders
WHERE (((is_cancel = 0) AND (is_nonproduct = 0)) AND (quantity > 0))
""")

# COMMAND ----------
# xfm_Key_Stage: Transformer
@dp.temporary_view(name="j23_lnk_k1")
def j23_lnk_k1():
    return spark.sql(f"""
SELECT
  line_amount,
  CAST('ALL' AS string) AS rec_key
FROM j23_lnk_lines
""")

# COMMAND ----------
# agg_Stage_Amount: Aggregator
@dp.temporary_view(name="j23_lnk_a1")
def j23_lnk_a1():
    return spark.sql(f"""
SELECT rec_key, CAST(SUM(line_amount) AS decimal(18,2)) AS stage_amount
FROM j23_lnk_k1
GROUP BY rec_key
""")

# COMMAND ----------
# src_fact_sales: dataset fact_sales
@dp.temporary_view(name="j23_lnk_fact")
def j23_lnk_fact():
    return spark.read.table("fact_sales")

# COMMAND ----------
# xfm_Key_Fact: Transformer
@dp.temporary_view(name="j23_lnk_k2")
def j23_lnk_k2():
    return spark.sql(f"""
SELECT
  sales_amount,
  CAST('ALL' AS string) AS rec_key
FROM j23_lnk_fact
""")

# COMMAND ----------
# agg_Fact_Amount: Aggregator
@dp.temporary_view(name="j23_lnk_a2")
def j23_lnk_a2():
    return spark.sql(f"""
SELECT rec_key, CAST(SUM(sales_amount) AS decimal(18,2)) AS fact_amount
FROM j23_lnk_k2
GROUP BY rec_key
""")

# COMMAND ----------
# jn_Reconcile: Join (inner) on rec_key
@dp.temporary_view(name="j23_lnk_rec")
def j23_lnk_rec():
    return spark.sql(f"""
SELECT
  l.rec_key,
  l.stage_amount,
  r.fact_amount
FROM j23_lnk_a1 l
INNER JOIN j23_lnk_a2 r ON l.rec_key = r.rec_key
""")

# COMMAND ----------
# xfm_Diff: Transformer
@dp.temporary_view(name="j23_lnk_diff")
def j23_lnk_diff():
    return spark.sql(f"""
SELECT
  rec_key,
  stage_amount,
  fact_amount,
  CAST((stage_amount - fact_amount) AS decimal(18,2)) AS diff_amount,
  CAST(CASE WHEN (abs((stage_amount - fact_amount)) < 0.01) THEN 'PASS' ELSE 'FAIL' END AS string) AS status
FROM j23_lnk_rec
""")

# COMMAND ----------
# tgt_audit_reconciliation: target file -> materialized view audit_reconciliation
@dp.materialized_view(name="audit_reconciliation")
def audit_reconciliation():
    return spark.read.table("j23_lnk_diff").select('rec_key', 'stage_amount', 'fact_amount', 'diff_amount', 'status')
