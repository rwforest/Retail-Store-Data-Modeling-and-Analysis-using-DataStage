# Databricks notebook source
# J12_Fact_Invoice - Invoice-grain fact (basket level) rolled up from Fact_Sales
# Layer: 3_Facts. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_fact_sales: dataset fact_sales
@dp.temporary_view(name="j12_lnk_fact")
def j12_lnk_fact():
    return spark.read.table("fact_sales")

# COMMAND ----------
# agg_Invoice: Aggregator
@dp.temporary_view(name="j12_lnk_inv")
def j12_lnk_inv():
    return spark.sql(f"""
SELECT invoice_no, customer_id, customer_sk, store_sk, employee_sk, invoice_date_key, year_month, CAST(SUM(sales_amount) AS decimal(18,2)) AS invoice_sales, CAST(SUM(gross_profit) AS decimal(18,2)) AS invoice_profit, CAST(SUM(quantity) AS bigint) AS item_qty, CAST(SUM(line_cnt) AS bigint) AS line_total
FROM j12_lnk_fact
GROUP BY invoice_no, customer_id, customer_sk, store_sk, employee_sk, invoice_date_key, year_month
""")

# COMMAND ----------
# xfm_Basket_Metrics: Transformer
@dp.temporary_view(name="j12_lnk_basket")
def j12_lnk_basket():
    return spark.sql(f"""
SELECT
  invoice_no,
  customer_id,
  customer_sk,
  store_sk,
  employee_sk,
  invoice_date_key,
  year_month,
  invoice_sales,
  invoice_profit,
  item_qty,
  line_total,
  CAST(CASE WHEN (line_total > 0) THEN (invoice_sales / line_total) ELSE 0 END AS decimal(18,2)) AS avg_line_value,
  CAST(1 AS int) AS invoice_cnt
FROM j12_lnk_inv
""")

# COMMAND ----------
# srt_Fact_Invoice: Sort
@dp.temporary_view(name="j12_lnk_sorted")
def j12_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j12_lnk_basket
ORDER BY invoice_no ASC
""")

# COMMAND ----------
# tgt_fact_invoice: target file -> materialized view fact_invoice
@dp.materialized_view(name="fact_invoice")
def fact_invoice():
    return spark.read.table("j12_lnk_sorted").select('invoice_no', 'customer_id', 'customer_sk', 'store_sk', 'employee_sk', 'invoice_date_key', 'year_month', 'invoice_sales', 'invoice_profit', 'item_qty', 'line_total', 'avg_line_value', 'invoice_cnt')
