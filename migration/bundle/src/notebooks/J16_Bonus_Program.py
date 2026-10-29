# Databricks notebook source
# J16_Bonus_Program - Q3: bonus for the customers contributing the most profit, scaled by loyalty tier
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
# agg_Customer_Profit: Aggregator
lnk_cust_profit = spark.sql(f"""
SELECT customer_id, CAST(SUM(gross_profit) AS decimal(18,2)) AS profit_total
FROM lnk_mart
GROUP BY customer_id
""")
lnk_cust_profit.createOrReplaceTempView("lnk_cust_profit")

# COMMAND ----------
# srt_Profit: Sort
lnk_ranked = spark.sql(f"""
SELECT * FROM lnk_cust_profit
ORDER BY profit_total DESC
""")
lnk_ranked.createOrReplaceTempView("lnk_ranked")

# COMMAND ----------
# hd_Top100: Head
lnk_top100 = spark.sql(f"""
SELECT * FROM lnk_ranked
ORDER BY profit_total DESC
LIMIT 100
""")
lnk_top100.createOrReplaceTempView("lnk_top100")

# COMMAND ----------
# src_dim_customer: Delta table dim_customer
lnk_dim_customer = spark.table(f"{catalog}.{schema}.dim_customer")
lnk_dim_customer.createOrReplaceTempView("lnk_dim_customer")

# COMMAND ----------
# flt_Current_Customer: Filter
lnk_cur = spark.sql(f"""
SELECT * FROM lnk_dim_customer
WHERE (is_current = 1)
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
# xfm_Bonus: Transformer
lnk_bonus = spark.sql(f"""
SELECT
  customer_id,
  profit_total,
  customer_sk,
  cust_name,
  customer_type,
  loyalty_tier,
  CAST(CASE WHEN (loyalty_tier = 'PLATINUM') THEN 0.05 ELSE CASE WHEN (loyalty_tier = 'GOLD') THEN 0.04 ELSE CASE WHEN (loyalty_tier = 'SILVER') THEN 0.03 ELSE 0.02 END END END AS decimal(18,6)) AS bonus_rate,
  CAST(CASE WHEN (loyalty_tier = 'PLATINUM') THEN (profit_total * 0.05) ELSE CASE WHEN (loyalty_tier = 'GOLD') THEN (profit_total * 0.04) ELSE CASE WHEN (loyalty_tier = 'SILVER') THEN (profit_total * 0.03) ELSE (profit_total * 0.02) END END END AS decimal(18,2)) AS bonus_amount,
  CAST('BONUS_2011' AS string) AS program
FROM lnk_named
""")
lnk_bonus.createOrReplaceTempView("lnk_bonus")

# COMMAND ----------
# cpy_Bonus: Copy
lnk_out = spark.sql(f"""
SELECT * FROM lnk_bonus
""")
lnk_out.createOrReplaceTempView("lnk_out")

# COMMAND ----------
# cpy_Bonus: Copy
lnk_total_in = spark.sql(f"""
SELECT * FROM lnk_bonus
""")
lnk_total_in.createOrReplaceTempView("lnk_total_in")

# COMMAND ----------
# tgt_bonus_program: write Delta table Q3_Bonus_Program
(lnk_out.select('customer_id', 'profit_total', 'customer_sk', 'cust_name', 'customer_type', 'loyalty_tier', 'bonus_rate', 'bonus_amount', 'program').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Q3_Bonus_Program"))
print("Q3_Bonus_Program:", spark.table(f"{catalog}.{schema}.Q3_Bonus_Program").count(), "rows")

# COMMAND ----------
# agg_Bonus_Total: Aggregator
lnk_total = spark.sql(f"""
SELECT program, CAST(SUM(bonus_amount) AS decimal(18,2)) AS bonus_total
FROM lnk_total_in
GROUP BY program
""")
lnk_total.createOrReplaceTempView("lnk_total")

# COMMAND ----------
# tgt_bonus_summary: write Delta table Q3_Bonus_Summary
(lnk_total.select('program', 'bonus_total').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Q3_Bonus_Summary"))
print("Q3_Bonus_Summary:", spark.table(f"{catalog}.{schema}.Q3_Bonus_Summary").count(), "rows")
