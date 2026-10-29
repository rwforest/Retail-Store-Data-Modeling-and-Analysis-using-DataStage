# Databricks notebook source
# MAGIC %md
# MAGIC # J16_Bonus_Program
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028071346-rdb5/J16_Bonus_Program.txt`
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
# src_mart: Delta table RETAIL_DATA_MART (was #pMartDir#/RETAIL_DATA_MART.csv)
lnk_mart = spark.table(f"{catalog}.{schema}.RETAIL_DATA_MART")
lnk_mart.createOrReplaceTempView("lnk_mart")

# COMMAND ----------
# agg_Customer_Profit: Aggregator (group by customer_id, sum of gross_profit)
lnk_cust_profit = spark.sql(f"""
SELECT
  customer_id,
  CAST(SUM(gross_profit) AS decimal(18,2)) AS profit_total
FROM lnk_mart
GROUP BY customer_id
""")
lnk_cust_profit.createOrReplaceTempView("lnk_cust_profit")

# COMMAND ----------
# srt_Profit: Sort (profit_total descending)
lnk_ranked = spark.sql(f"""
SELECT customer_id, profit_total
FROM lnk_cust_profit
ORDER BY profit_total DESC
""")
lnk_ranked.createOrReplaceTempView("lnk_ranked")

# COMMAND ----------
# hd_Top100: Head (first 100 records; the per-partition setting "part 0" is not reproduced, global top 100 is used)
lnk_top100 = spark.sql(f"""
SELECT customer_id, profit_total
FROM lnk_ranked
LIMIT 100
""")
lnk_top100.createOrReplaceTempView("lnk_top100")

# COMMAND ----------
# src_dim_customer: Delta table dim_customer (was #pDwhDir#/dim_customer.csv)
lnk_dim_customer = spark.table(f"{catalog}.{schema}.dim_customer")
lnk_dim_customer.createOrReplaceTempView("lnk_dim_customer")

# COMMAND ----------
# flt_Current_Customer: Filter (is_current = 1)
lnk_cur = spark.sql(f"""
SELECT * FROM lnk_dim_customer
WHERE is_current = 1
""")
lnk_cur.createOrReplaceTempView("lnk_cur")

# COMMAND ----------
# jn_Customer: Join (inner) on customer_id
lnk_named = spark.sql(f"""
SELECT
  l.customer_id,
  l.profit_total,
  r.customer_sk,
  r.cust_name,
  r.customer_type,
  r.loyalty_tier
FROM lnk_top100 l
INNER JOIN lnk_cur r ON l.customer_id = r.customer_id
""")
lnk_named.createOrReplaceTempView("lnk_named")

# COMMAND ----------
# xfm_Bonus: Transformer (bonus rate and amount by loyalty tier)
lnk_bonus = spark.sql(f"""
SELECT
  customer_id,
  profit_total,
  customer_sk,
  cust_name,
  customer_type,
  loyalty_tier,
  CAST(CASE WHEN loyalty_tier = 'PLATINUM' THEN 0.05
            ELSE CASE WHEN loyalty_tier = 'GOLD' THEN 0.04
            ELSE CASE WHEN loyalty_tier = 'SILVER' THEN 0.03
            ELSE 0.02 END END END AS decimal(18,6)) AS bonus_rate,
  CAST(CASE WHEN loyalty_tier = 'PLATINUM' THEN profit_total * 0.05
            ELSE CASE WHEN loyalty_tier = 'GOLD' THEN profit_total * 0.04
            ELSE CASE WHEN loyalty_tier = 'SILVER' THEN profit_total * 0.03
            ELSE profit_total * 0.02 END END END AS decimal(18,2)) AS bonus_amount,
  CAST('BONUS_2011' AS string) AS program
FROM lnk_named
""")
lnk_bonus.createOrReplaceTempView("lnk_bonus")

# COMMAND ----------
# cpy_Bonus: Copy (output link lnk_out)
lnk_out = spark.sql(f"""
SELECT customer_id, profit_total, customer_sk, cust_name, customer_type, loyalty_tier, bonus_rate, bonus_amount, program
FROM lnk_bonus
""")
lnk_out.createOrReplaceTempView("lnk_out")

# COMMAND ----------
# cpy_Bonus: Copy (output link lnk_total_in)
lnk_total_in = spark.sql(f"""
SELECT customer_id, profit_total, customer_sk, cust_name, customer_type, loyalty_tier, bonus_rate, bonus_amount, program
FROM lnk_bonus
""")
lnk_total_in.createOrReplaceTempView("lnk_total_in")

# COMMAND ----------
# tgt_bonus_program: write Delta table Q3_Bonus_Program
(lnk_out.select('customer_id', 'profit_total', 'customer_sk', 'cust_name', 'customer_type', 'loyalty_tier', 'bonus_rate', 'bonus_amount', 'program').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Q3_Bonus_Program"))
print("Q3_Bonus_Program:", spark.table(f"{catalog}.{schema}.Q3_Bonus_Program").count(), "rows")

# COMMAND ----------
# agg_Bonus_Total: Aggregator (group by program, sum of bonus_amount)
lnk_total = spark.sql(f"""
SELECT
  program,
  CAST(SUM(bonus_amount) AS decimal(18,2)) AS bonus_total
FROM lnk_total_in
GROUP BY program
""")
lnk_total.createOrReplaceTempView("lnk_total")

# COMMAND ----------
# tgt_bonus_summary: write Delta table Q3_Bonus_Summary
(lnk_total.select('program', 'bonus_total').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Q3_Bonus_Summary"))
print("Q3_Bonus_Summary:", spark.table(f"{catalog}.{schema}.Q3_Bonus_Summary").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
