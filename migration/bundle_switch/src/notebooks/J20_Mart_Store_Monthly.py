# Databricks notebook source
# MAGIC %md
# MAGIC # J20_Mart_Store_Monthly
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261029064410-juws/J20_Mart_Store_Monthly.txt`
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
# src_fact_sales: Sequential File source (#pDwhDir#/fact_sales.csv -> Delta table fact_sales)
lnk_fact = spark.table(f"{catalog}.{schema}.fact_sales")
lnk_fact.createOrReplaceTempView("lnk_fact")

# COMMAND ----------
# agg_Store_Month: Aggregator
lnk_sm = spark.sql(f"""
SELECT
  store_sk,
  year_month,
  CAST(SUM(sales_amount) AS decimal(18,2)) AS sales,
  CAST(SUM(gross_profit) AS decimal(18,2)) AS profit,
  CAST(SUM(line_cnt) AS bigint) AS lines
FROM lnk_fact
GROUP BY store_sk, year_month
""")
lnk_sm.createOrReplaceTempView("lnk_sm")

# COMMAND ----------
# src_dim_store: Sequential File source (#pDwhDir#/dim_store.csv -> Delta table dim_store)
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
  CAST(store_sk AS int) AS store_sk,
  CAST(year_month AS string) AS year_month,
  CAST(sales AS decimal(18,2)) AS sales,
  CAST(profit AS decimal(18,2)) AS profit,
  CAST(lines AS bigint) AS lines,
  CAST(store_id AS string) AS store_id,
  CAST(store_name AS string) AS store_name,
  CAST(no_of_employees AS int) AS no_of_employees,
  CAST(region AS string) AS region,
  CAST(CASE WHEN no_of_employees > 0 THEN sales / no_of_employees ELSE 0 END AS decimal(18,2)) AS sales_per_employee,
  CAST(CASE WHEN sales > 0 THEN profit / sales ELSE 0 END AS decimal(18,6)) AS margin_pct
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
(lnk_sorted.select('store_sk', 'year_month', 'sales', 'profit', 'lines', 'store_id', 'store_name',
                   'no_of_employees', 'region', 'sales_per_employee', 'margin_pct').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Store_Monthly"))
print("Store_Monthly:", spark.table(f"{catalog}.{schema}.Store_Monthly").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
