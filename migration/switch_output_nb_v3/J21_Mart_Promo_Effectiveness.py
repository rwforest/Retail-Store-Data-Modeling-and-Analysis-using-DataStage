# Databricks notebook source
# MAGIC %md
# MAGIC # J21_Mart_Promo_Effectiveness
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261029064410-juws/J21_Mart_Promo_Effectiveness.txt`
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
# src_fact_sales: Sequential File (Delta table fact_sales)
lnk_fact = spark.table(f"{catalog}.{schema}.fact_sales")
lnk_fact.createOrReplaceTempView("lnk_fact")

# COMMAND ----------
# src_dim_promotion: Sequential File (Delta table dim_promotion)
lnk_dim_promo = spark.table(f"{catalog}.{schema}.dim_promotion")
lnk_dim_promo.createOrReplaceTempView("lnk_dim_promo")

# COMMAND ----------
# jn_Promo_Category: Join (inner) on category
lnk_fp = spark.sql(f"""
SELECT
  l.invoice_date_key,
  l.category,
  l.sales_amount,
  l.gross_profit,
  l.line_cnt,
  r.promo_id,
  r.discount_pct,
  r.start_key,
  r.end_key
FROM lnk_fact l
INNER JOIN lnk_dim_promo r ON l.category = r.category
""")
lnk_fp.createOrReplaceTempView("lnk_fp")

# COMMAND ----------
# flt_In_Window: Filter
lnk_window = spark.sql(f"""
SELECT * FROM lnk_fp
WHERE invoice_date_key >= start_key AND invoice_date_key <= end_key
""")
lnk_window.createOrReplaceTempView("lnk_window")

# COMMAND ----------
# agg_By_Promo: Aggregator
lnk_promo = spark.sql(f"""
SELECT
  promo_id,
  category,
  discount_pct,
  CAST(SUM(sales_amount) AS decimal(18,2)) AS promo_sales,
  CAST(SUM(gross_profit) AS decimal(18,2)) AS promo_profit,
  CAST(SUM(line_cnt) AS bigint) AS promo_lines
FROM lnk_window
GROUP BY promo_id, category, discount_pct
""")
lnk_promo.createOrReplaceTempView("lnk_promo")

# COMMAND ----------
# xfm_Discount_Cost: Transformer
lnk_cost = spark.sql(f"""
SELECT
  promo_id,
  category,
  discount_pct,
  promo_sales,
  promo_profit,
  promo_lines,
  CAST(promo_sales * discount_pct / 100 AS decimal(18,2)) AS est_discount_cost,
  CAST(promo_profit - (promo_sales * discount_pct / 100) AS decimal(18,2)) AS profit_after_discount
FROM lnk_promo
""")
lnk_cost.createOrReplaceTempView("lnk_cost")

# COMMAND ----------
# srt_Promo: Sort
lnk_sorted = spark.sql(f"""
SELECT * FROM lnk_cost
ORDER BY profit_after_discount DESC
""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")

# COMMAND ----------
# tgt_mart_promo_effectiveness: write Delta table Promo_Effectiveness
(lnk_sorted.select('promo_id', 'category', 'discount_pct', 'promo_sales', 'promo_profit', 'promo_lines', 'est_discount_cost', 'profit_after_discount').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Promo_Effectiveness"))
print("Promo_Effectiveness:", spark.table(f"{catalog}.{schema}.Promo_Effectiveness").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
