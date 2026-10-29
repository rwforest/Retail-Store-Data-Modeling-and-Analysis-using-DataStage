# Databricks notebook source
# J12_Fact_Invoice - Invoice-grain fact (basket level) rolled up from Fact_Sales
# Layer: 3_Facts. Compiled from the DataStage job by migration/ds2spark.py; do not edit by hand.

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
# src_fact_sales: Delta table fact_sales
lnk_fact = spark.table(f"{catalog}.{schema}.fact_sales")
lnk_fact.createOrReplaceTempView("lnk_fact")

# COMMAND ----------
# agg_Invoice: Aggregator
lnk_inv = spark.sql(f"""
SELECT invoice_no, customer_id, customer_sk, store_sk, employee_sk, invoice_date_key, year_month, CAST(SUM(sales_amount) AS decimal(18,2)) AS invoice_sales, CAST(SUM(gross_profit) AS decimal(18,2)) AS invoice_profit, CAST(SUM(quantity) AS bigint) AS item_qty, CAST(SUM(line_cnt) AS bigint) AS line_total
FROM lnk_fact
GROUP BY invoice_no, customer_id, customer_sk, store_sk, employee_sk, invoice_date_key, year_month
""")
lnk_inv.createOrReplaceTempView("lnk_inv")

# COMMAND ----------
# xfm_Basket_Metrics: Transformer
lnk_basket = spark.sql(f"""
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
FROM lnk_inv
""")
lnk_basket.createOrReplaceTempView("lnk_basket")

# COMMAND ----------
# srt_Fact_Invoice: Sort
lnk_sorted = spark.sql(f"""
SELECT * FROM lnk_basket
ORDER BY invoice_no ASC
""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")

# COMMAND ----------
# tgt_fact_invoice: write Delta table fact_invoice
(lnk_sorted.select('invoice_no', 'customer_id', 'customer_sk', 'store_sk', 'employee_sk', 'invoice_date_key', 'year_month', 'invoice_sales', 'invoice_profit', 'item_qty', 'line_total', 'avg_line_value', 'invoice_cnt').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.fact_invoice"))
print("fact_invoice:", spark.table(f"{catalog}.{schema}.fact_invoice").count(), "rows")
