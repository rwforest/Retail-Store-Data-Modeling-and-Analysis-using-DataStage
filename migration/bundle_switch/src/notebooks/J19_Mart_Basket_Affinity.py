# Databricks notebook source
# MAGIC %md
# MAGIC # J19_Mart_Basket_Affinity
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261029064410-juws/J19_Mart_Basket_Affinity.txt`
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
# src_fact_sales: Delta table fact_sales (former #pDwhDir#/fact_sales.csv)
lnk_fact = spark.table(f"{catalog}.{schema}.fact_sales")
lnk_fact.createOrReplaceTempView("lnk_fact")

# COMMAND ----------
# flt_Positive_Lines: Filter (quantity > 0)
lnk_pos = spark.sql(f"""
SELECT * FROM lnk_fact
WHERE quantity > 0
""")
lnk_pos.createOrReplaceTempView("lnk_pos")

# COMMAND ----------
# xfm_Pair_Prep: Transformer (keeps only invoice_no and stock_code)
lnk_prep = spark.sql(f"""
SELECT
  CAST(invoice_no AS string) AS invoice_no,
  CAST(stock_code AS string) AS stock_code
FROM lnk_pos
""")
lnk_prep.createOrReplaceTempView("lnk_prep")

# COMMAND ----------
# rmd_Invoice_Product: Remove Duplicates (keys invoice_no, stock_code; keep first; no Sort upstream -> monotonically_increasing_id order)
lnk_dedup = spark.sql(f"""
SELECT invoice_no, stock_code
FROM (
  SELECT invoice_no, stock_code,
         row_number() OVER (PARTITION BY invoice_no, stock_code ORDER BY monotonically_increasing_id()) AS _rn
  FROM lnk_prep
) t
WHERE _rn = 1
""")
lnk_dedup.createOrReplaceTempView("lnk_dedup")

# COMMAND ----------
# cpy_Basket: Copy
lnk_a = spark.sql(f"""
SELECT invoice_no, stock_code FROM lnk_dedup
""")
lnk_a.createOrReplaceTempView("lnk_a")

# COMMAND ----------
# cpy_Basket: Copy
lnk_b_in = spark.sql(f"""
SELECT invoice_no, stock_code FROM lnk_dedup
""")
lnk_b_in.createOrReplaceTempView("lnk_b_in")

# COMMAND ----------
# xfm_Rename_B: Transformer (stock_code renamed to stock_code_b)
lnk_b = spark.sql(f"""
SELECT
  CAST(invoice_no AS string) AS invoice_no,
  CAST(stock_code AS string) AS stock_code_b
FROM lnk_b_in
""")
lnk_b.createOrReplaceTempView("lnk_b")

# COMMAND ----------
# jn_Self: Join (inner) on invoice_no
lnk_pairs_all = spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  r.stock_code_b
FROM lnk_a l
INNER JOIN lnk_b r ON l.invoice_no = r.invoice_no
""")
lnk_pairs_all.createOrReplaceTempView("lnk_pairs_all")

# COMMAND ----------
# flt_Ordered_Pairs: Filter (stock_code < stock_code_b)
lnk_pairs = spark.sql(f"""
SELECT * FROM lnk_pairs_all
WHERE stock_code < stock_code_b
""")
lnk_pairs.createOrReplaceTempView("lnk_pairs")

# COMMAND ----------
# agg_Pair_Counts: Aggregator (count per stock_code, stock_code_b)
lnk_counts = spark.sql(f"""
SELECT stock_code, stock_code_b, CAST(COUNT(*) AS int) AS pair_count
FROM lnk_pairs
GROUP BY stock_code, stock_code_b
""")
lnk_counts.createOrReplaceTempView("lnk_counts")

# COMMAND ----------
# flt_Min_Support: Filter (pair_count >= 5)
lnk_support = spark.sql(f"""
SELECT * FROM lnk_counts
WHERE pair_count >= 5
""")
lnk_support.createOrReplaceTempView("lnk_support")

# COMMAND ----------
# srt_Pairs: Sort (pair_count desc)
lnk_sorted = spark.sql(f"""
SELECT * FROM lnk_support
ORDER BY pair_count DESC
""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")

# COMMAND ----------
# hd_Top1000: Head (first 1000 rows in the upstream sort order)
lnk_top = spark.sql(f"""
SELECT * FROM lnk_sorted
ORDER BY pair_count DESC
LIMIT 1000
""")
lnk_top.createOrReplaceTempView("lnk_top")

# COMMAND ----------
# tgt_mart_basket_affinity: write Delta table Basket_Affinity
(lnk_top.select('stock_code', 'stock_code_b', 'pair_count').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Basket_Affinity"))
print("Basket_Affinity:", spark.table(f"{catalog}.{schema}.Basket_Affinity").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
