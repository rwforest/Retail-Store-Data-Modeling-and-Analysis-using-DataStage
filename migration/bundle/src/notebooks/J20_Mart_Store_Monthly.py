# Databricks notebook source
# J20_Mart_Store_Monthly - Store x month sales KPIs incl. sales per employee
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
# src_fact_sales: Delta table fact_sales
lnk_fact = spark.table(f"{catalog}.{schema}.fact_sales")
lnk_fact.createOrReplaceTempView("lnk_fact")

# COMMAND ----------
# agg_Store_Month: Aggregator
lnk_sm = spark.sql(f"""
SELECT store_sk, year_month, CAST(SUM(sales_amount) AS decimal(18,2)) AS sales, CAST(SUM(gross_profit) AS decimal(18,2)) AS profit, CAST(SUM(line_cnt) AS bigint) AS lines
FROM lnk_fact
GROUP BY store_sk, year_month
""")
lnk_sm.createOrReplaceTempView("lnk_sm")

# COMMAND ----------
# src_dim_store: Delta table dim_store
lnk_dim_store = spark.table(f"{catalog}.{schema}.dim_store")
lnk_dim_store.createOrReplaceTempView("lnk_dim_store")

# COMMAND ----------
# jn_Store: Join (inner) on store_sk
lnk_named = spark.sql(f"""
SELECT
  l.store_sk,
  l.year_month,
  l.sales,
  l.profit,
  l.lines,
  r.store_id,
  r.store_name,
  r.no_of_employees,
  r.region
FROM lnk_sm l
INNER JOIN lnk_dim_store r ON l.store_sk = r.store_sk
""")
lnk_named.createOrReplaceTempView("lnk_named")

# COMMAND ----------
# xfm_Store_KPIs: Transformer
lnk_kpi = spark.sql(f"""
SELECT
  store_sk,
  year_month,
  sales,
  profit,
  lines,
  store_id,
  store_name,
  no_of_employees,
  region,
  CAST(CASE WHEN (no_of_employees > 0) THEN (sales / no_of_employees) ELSE 0 END AS decimal(18,2)) AS sales_per_employee,
  CAST(CASE WHEN (sales > 0) THEN (profit / sales) ELSE 0 END AS decimal(18,6)) AS margin_pct
FROM lnk_named
""")
lnk_kpi.createOrReplaceTempView("lnk_kpi")

# COMMAND ----------
# srt_Store_Month: Sort
lnk_sorted = spark.sql(f"""
SELECT * FROM lnk_kpi
ORDER BY store_sk ASC, year_month ASC
""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")

# COMMAND ----------
# tgt_mart_store_monthly: write Delta table Store_Monthly
(lnk_sorted.select('store_sk', 'year_month', 'sales', 'profit', 'lines', 'store_id', 'store_name', 'no_of_employees', 'region', 'sales_per_employee', 'margin_pct').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Store_Monthly"))
print("Store_Monthly:", spark.table(f"{catalog}.{schema}.Store_Monthly").count(), "rows")
