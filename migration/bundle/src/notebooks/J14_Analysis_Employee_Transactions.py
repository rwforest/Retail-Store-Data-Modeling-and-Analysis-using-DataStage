# Databricks notebook source
# J14_Analysis_Employee_Transactions - Q1: number of transactions per employee in each store
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
# rmd_Invoices: Remove Duplicates
lnk_invoices = spark.sql(f"""
SELECT invoice_no, stock_code, quantity, invoice_ts, unit_price, customer_id, country, store_id, invoice_date_key, year_month, customer_sk, customer_type, loyalty_tier, category, product_sk, region, store_sk, employee_sk, currency, sales_amount, cost_amount, gross_profit, local_amount, line_cnt, cust_name, gender, age, spending_score, annual_income_k, product_name, list_price, store_name, rating, no_of_employees FROM (
  SELECT *, row_number() OVER (PARTITION BY store_sk, employee_sk, invoice_no ORDER BY monotonically_increasing_id()) AS _rn FROM lnk_mart
) WHERE _rn = 1
""")
lnk_invoices.createOrReplaceTempView("lnk_invoices")

# COMMAND ----------
# agg_Transactions: Aggregator
lnk_counts = spark.sql(f"""
SELECT store_sk, employee_sk, CAST(COUNT(*) AS int) AS total_transactions
FROM lnk_invoices
GROUP BY store_sk, employee_sk
""")
lnk_counts.createOrReplaceTempView("lnk_counts")

# COMMAND ----------
# src_dim_employee: Delta table dim_employee
lnk_dim_emp = spark.table(f"{catalog}.{schema}.dim_employee")
lnk_dim_emp.createOrReplaceTempView("lnk_dim_emp")

# COMMAND ----------
# jn_Employee: Join (inner) on employee_sk
lnk_named = spark.sql(f"""
SELECT
  l.store_sk,
  l.employee_sk,
  l.total_transactions,
  r.employee_id,
  r.employee_name,
  r.role
FROM lnk_counts l
INNER JOIN lnk_dim_emp r ON l.employee_sk = r.employee_sk
""")
lnk_named.createOrReplaceTempView("lnk_named")

# COMMAND ----------
# srt_Store_Rank: Sort
lnk_ranked = spark.sql(f"""
SELECT * FROM lnk_named
ORDER BY store_sk ASC, total_transactions DESC
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
lnk_top_in = spark.sql(f"""
SELECT * FROM lnk_ranked
""")
lnk_top_in.createOrReplaceTempView("lnk_top_in")

# COMMAND ----------
# tgt_employee_transactions: write Delta table Q1_Employee_Transactions
(lnk_all.select('store_sk', 'employee_sk', 'total_transactions', 'employee_id', 'employee_name', 'role').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Q1_Employee_Transactions"))
print("Q1_Employee_Transactions:", spark.table(f"{catalog}.{schema}.Q1_Employee_Transactions").count(), "rows")

# COMMAND ----------
# hd_Top10: Head
lnk_top10 = spark.sql(f"""
SELECT * FROM lnk_top_in
ORDER BY store_sk ASC, total_transactions DESC
LIMIT 10
""")
lnk_top10.createOrReplaceTempView("lnk_top10")

# COMMAND ----------
# tgt_top10_employees: write Delta table Q1_Top10_Employees
(lnk_top10.select('store_sk', 'employee_sk', 'total_transactions', 'employee_id', 'employee_name', 'role').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Q1_Top10_Employees"))
print("Q1_Top10_Employees:", spark.table(f"{catalog}.{schema}.Q1_Top10_Employees").count(), "rows")
