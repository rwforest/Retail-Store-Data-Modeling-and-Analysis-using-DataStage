# Databricks notebook source
# J15_Analysis_CustomerType_Profit - Q2: which customer type (citizen/foreign) generates the highest profit
# Layer: 4_Marts. Compiled from the DataStage job by migration/ds2spark.py; do not edit by hand.

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
# src_mart: Delta table RETAIL_DATA_MART
lnk_mart = spark.table(f"{catalog}.{schema}.RETAIL_DATA_MART")
lnk_mart.createOrReplaceTempView("lnk_mart")

# COMMAND ----------
# agg_By_Type: Aggregator
lnk_by_type = spark.sql(f"""
SELECT customer_type, CAST(SUM(gross_profit) AS decimal(18,2)) AS profit_total, CAST(SUM(sales_amount) AS decimal(18,2)) AS sales_total, CAST(SUM(line_cnt) AS bigint) AS lines
FROM lnk_mart
GROUP BY customer_type
""")
lnk_by_type.createOrReplaceTempView("lnk_by_type")

# COMMAND ----------
# srt_Profit: Sort
lnk_ranked = spark.sql(f"""
SELECT * FROM lnk_by_type
ORDER BY profit_total DESC
""")
lnk_ranked.createOrReplaceTempView("lnk_ranked")

# COMMAND ----------
# cpy_Ranked: Copy
lnk_all = spark.sql(f"""
SELECT * FROM lnk_ranked
""")
lnk_all.createOrReplaceTempView("lnk_all")

# COMMAND ----------
# cpy_Ranked: Copy
lnk_win_in = spark.sql(f"""
SELECT * FROM lnk_ranked
""")
lnk_win_in.createOrReplaceTempView("lnk_win_in")

# COMMAND ----------
# tgt_customer_type_ranking: write Delta table Q2_CustomerType_Profit
(lnk_all.select('customer_type', 'profit_total', 'sales_total', 'lines').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Q2_CustomerType_Profit"))
print("Q2_CustomerType_Profit:", spark.table(f"{catalog}.{schema}.Q2_CustomerType_Profit").count(), "rows")

# COMMAND ----------
# hd_Winner: Head
lnk_winner = spark.sql(f"""
SELECT * FROM lnk_win_in
ORDER BY profit_total DESC
LIMIT 1
""")
lnk_winner.createOrReplaceTempView("lnk_winner")

# COMMAND ----------
# tgt_customer_type_winner: write Delta table Q2_Winning_CustomerType
(lnk_winner.select('customer_type', 'profit_total', 'sales_total', 'lines').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Q2_Winning_CustomerType"))
print("Q2_Winning_CustomerType:", spark.table(f"{catalog}.{schema}.Q2_Winning_CustomerType").count(), "rows")
