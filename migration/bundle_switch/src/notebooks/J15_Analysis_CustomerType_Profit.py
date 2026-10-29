# Databricks notebook source
# MAGIC %md
# MAGIC # J15_Analysis_CustomerType_Profit
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261029064410-juws/J15_Analysis_CustomerType_Profit.txt`
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
# src_mart: Sequential File source (#pMartDir#/RETAIL_DATA_MART.csv is a Delta table in {catalog}.{schema})
lnk_mart = spark.table(f"{catalog}.{schema}.RETAIL_DATA_MART")
lnk_mart.createOrReplaceTempView("lnk_mart")

# COMMAND ----------
# agg_By_Type: Aggregator (key: customer_type; sum reductions)
lnk_by_type = spark.sql(f"""
SELECT
  customer_type,
  CAST(SUM(gross_profit) AS decimal(18,2)) AS profit_total,
  CAST(SUM(sales_amount) AS decimal(18,2)) AS sales_total,
  CAST(SUM(line_cnt) AS bigint) AS lines
FROM lnk_mart
GROUP BY customer_type
""")
lnk_by_type.createOrReplaceTempView("lnk_by_type")

# COMMAND ----------
# srt_Profit: Sort (profit_total descending)
lnk_ranked = spark.sql(f"""
SELECT customer_type, profit_total, sales_total, lines
FROM lnk_by_type
ORDER BY profit_total DESC
""")
lnk_ranked.createOrReplaceTempView("lnk_ranked")

# COMMAND ----------
# cpy_Ranked: Copy (output link lnk_all)
lnk_all = spark.sql(f"""
SELECT customer_type, profit_total, sales_total, lines
FROM lnk_ranked
""")
lnk_all.createOrReplaceTempView("lnk_all")

# COMMAND ----------
# cpy_Ranked: Copy (output link lnk_win_in)
lnk_win_in = spark.sql(f"""
SELECT customer_type, profit_total, sales_total, lines
FROM lnk_ranked
""")
lnk_win_in.createOrReplaceTempView("lnk_win_in")

# COMMAND ----------
# tgt_customer_type_ranking: Sequential File target -> Delta table Q2_CustomerType_Profit (overwrite)
(lnk_all.select('customer_type', 'profit_total', 'sales_total', 'lines').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Q2_CustomerType_Profit"))
print("Q2_CustomerType_Profit:", spark.table(f"{catalog}.{schema}.Q2_CustomerType_Profit").count(), "rows")

# COMMAND ----------
# hd_Winner: Head (nrecs = 1; first row in the upstream Sort order: profit_total DESC)
lnk_winner = spark.sql(f"""
SELECT customer_type, profit_total, sales_total, lines
FROM lnk_win_in
ORDER BY profit_total DESC
LIMIT 1
""")
lnk_winner.createOrReplaceTempView("lnk_winner")

# COMMAND ----------
# tgt_customer_type_winner: Sequential File target -> Delta table Q2_Winning_CustomerType (overwrite)
(lnk_winner.select('customer_type', 'profit_total', 'sales_total', 'lines').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Q2_Winning_CustomerType"))
print("Q2_Winning_CustomerType:", spark.table(f"{catalog}.{schema}.Q2_Winning_CustomerType").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
