# Databricks notebook source
# J23_Reconcile_Source_To_Fact - Reconcile staged order amounts to Fact_Sales amounts
# Layer: 5_DataQuality. Compiled from the DataStage job by migration/ds2spark.py; do not edit by hand.

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
# src_stg_orders: Delta table stg_orders
lnk_orders = spark.table(f"{catalog}.{schema}.stg_orders")
lnk_orders.createOrReplaceTempView("lnk_orders")

# COMMAND ----------
# flt_Sales_Lines: Filter
lnk_lines = spark.sql(f"""
SELECT * FROM lnk_orders
WHERE (((is_cancel = 0) AND (is_nonproduct = 0)) AND (quantity > 0))
""")
lnk_lines.createOrReplaceTempView("lnk_lines")

# COMMAND ----------
# xfm_Key_Stage: Transformer
lnk_k1 = spark.sql(f"""
SELECT
  line_amount,
  CAST('ALL' AS string) AS rec_key
FROM lnk_lines
""")
lnk_k1.createOrReplaceTempView("lnk_k1")

# COMMAND ----------
# agg_Stage_Amount: Aggregator
lnk_a1 = spark.sql(f"""
SELECT rec_key, CAST(SUM(line_amount) AS decimal(18,2)) AS stage_amount
FROM lnk_k1
GROUP BY rec_key
""")
lnk_a1.createOrReplaceTempView("lnk_a1")

# COMMAND ----------
# src_fact_sales: Delta table fact_sales
lnk_fact = spark.table(f"{catalog}.{schema}.fact_sales")
lnk_fact.createOrReplaceTempView("lnk_fact")

# COMMAND ----------
# xfm_Key_Fact: Transformer
lnk_k2 = spark.sql(f"""
SELECT
  sales_amount,
  CAST('ALL' AS string) AS rec_key
FROM lnk_fact
""")
lnk_k2.createOrReplaceTempView("lnk_k2")

# COMMAND ----------
# agg_Fact_Amount: Aggregator
lnk_a2 = spark.sql(f"""
SELECT rec_key, CAST(SUM(sales_amount) AS decimal(18,2)) AS fact_amount
FROM lnk_k2
GROUP BY rec_key
""")
lnk_a2.createOrReplaceTempView("lnk_a2")

# COMMAND ----------
# jn_Reconcile: Join (inner) on rec_key
lnk_rec = spark.sql(f"""
SELECT
  l.rec_key,
  l.stage_amount,
  r.fact_amount
FROM lnk_a1 l
INNER JOIN lnk_a2 r ON l.rec_key = r.rec_key
""")
lnk_rec.createOrReplaceTempView("lnk_rec")

# COMMAND ----------
# xfm_Diff: Transformer
lnk_diff = spark.sql(f"""
SELECT
  rec_key,
  stage_amount,
  fact_amount,
  CAST((stage_amount - fact_amount) AS decimal(18,2)) AS diff_amount,
  CAST(CASE WHEN (abs((stage_amount - fact_amount)) < 0.01) THEN 'PASS' ELSE 'FAIL' END AS string) AS status
FROM lnk_rec
""")
lnk_diff.createOrReplaceTempView("lnk_diff")

# COMMAND ----------
# tgt_audit_reconciliation: write Delta table audit_reconciliation
(lnk_diff.select('rec_key', 'stage_amount', 'fact_amount', 'diff_amount', 'status').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.audit_reconciliation"))
print("audit_reconciliation:", spark.table(f"{catalog}.{schema}.audit_reconciliation").count(), "rows")
