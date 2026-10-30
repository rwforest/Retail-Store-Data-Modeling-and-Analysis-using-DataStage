# Databricks notebook source
# J14_Analysis_Employee_Transactions - Q1: number of transactions per employee in each store
# Layer: 4_Marts. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_mart: dataset RETAIL_DATA_MART
@dp.temporary_view(name="j14_lnk_mart")
def j14_lnk_mart():
    return spark.read.table("RETAIL_DATA_MART")

# COMMAND ----------
# rmd_Invoices: Remove Duplicates
@dp.temporary_view(name="j14_lnk_invoices")
def j14_lnk_invoices():
    return spark.sql(f"""
SELECT invoice_no, stock_code, quantity, invoice_ts, unit_price, customer_id, country, store_id, invoice_date_key, year_month, customer_sk, customer_type, loyalty_tier, category, product_sk, region, store_sk, employee_sk, currency, sales_amount, cost_amount, gross_profit, local_amount, line_cnt, cust_name, gender, age, spending_score, annual_income_k, product_name, list_price, store_name, rating, no_of_employees FROM (
  SELECT *, row_number() OVER (PARTITION BY store_sk, employee_sk, invoice_no ORDER BY monotonically_increasing_id()) AS _rn FROM j14_lnk_mart
) WHERE _rn = 1
""")

# COMMAND ----------
# agg_Transactions: Aggregator
@dp.temporary_view(name="j14_lnk_counts")
def j14_lnk_counts():
    return spark.sql(f"""
SELECT store_sk, employee_sk, CAST(COUNT(*) AS int) AS total_transactions
FROM j14_lnk_invoices
GROUP BY store_sk, employee_sk
""")

# COMMAND ----------
# src_dim_employee: dataset dim_employee
@dp.temporary_view(name="j14_lnk_dim_emp")
def j14_lnk_dim_emp():
    return spark.read.table("dim_employee")

# COMMAND ----------
# jn_Employee: Join (inner) on employee_sk
@dp.temporary_view(name="j14_lnk_named")
def j14_lnk_named():
    return spark.sql(f"""
SELECT
  l.store_sk,
  l.employee_sk,
  l.total_transactions,
  r.employee_id,
  r.employee_name,
  r.role
FROM j14_lnk_counts l
INNER JOIN j14_lnk_dim_emp r ON l.employee_sk = r.employee_sk
""")

# COMMAND ----------
# srt_Store_Rank: Sort
@dp.temporary_view(name="j14_lnk_ranked")
def j14_lnk_ranked():
    return spark.sql(f"""
SELECT * FROM j14_lnk_named
ORDER BY store_sk ASC, total_transactions DESC
""")

# COMMAND ----------
# cpy_Ranked: Copy
@dp.temporary_view(name="j14_lnk_all")
def j14_lnk_all():
    return spark.sql(f"""
SELECT * FROM j14_lnk_ranked
""")

# COMMAND ----------
# cpy_Ranked: Copy
@dp.temporary_view(name="j14_lnk_top_in")
def j14_lnk_top_in():
    return spark.sql(f"""
SELECT * FROM j14_lnk_ranked
""")

# COMMAND ----------
# tgt_employee_transactions: target file -> materialized view Q1_Employee_Transactions
@dp.materialized_view(name="Q1_Employee_Transactions")
def Q1_Employee_Transactions():
    return spark.read.table("j14_lnk_all").select('store_sk', 'employee_sk', 'total_transactions', 'employee_id', 'employee_name', 'role')

# COMMAND ----------
# hd_Top10: Head
@dp.temporary_view(name="j14_lnk_top10")
def j14_lnk_top10():
    return spark.sql(f"""
SELECT * FROM j14_lnk_top_in
ORDER BY store_sk ASC, total_transactions DESC
LIMIT 10
""")

# COMMAND ----------
# tgt_top10_employees: target file -> materialized view Q1_Top10_Employees
@dp.materialized_view(name="Q1_Top10_Employees")
def Q1_Top10_Employees():
    return spark.read.table("j14_lnk_top10").select('store_sk', 'employee_sk', 'total_transactions', 'employee_id', 'employee_name', 'role')
