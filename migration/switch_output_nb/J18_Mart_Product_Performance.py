# Databricks notebook source
# MAGIC %md
# MAGIC # J18_Mart_Product_Performance
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028071346-rdb5/J18_Mart_Product_Performance.txt`
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
# src_fact_sales: Delta table fact_sales (was #pDwhDir#/fact_sales.csv)
lnk_fact = spark.table(f"{catalog}.{schema}.fact_sales")
lnk_fact.createOrReplaceTempView("lnk_fact")

# COMMAND ----------
# agg_Product_Sales: Aggregator (group by product_sk; sum quantity, sales_amount, gross_profit)
lnk_sales = spark.sql(f"""
SELECT
  product_sk,
  CAST(SUM(quantity) AS bigint) AS units_sold,
  CAST(SUM(sales_amount) AS decimal(18,2)) AS revenue,
  CAST(SUM(gross_profit) AS decimal(18,2)) AS profit
FROM lnk_fact
GROUP BY product_sk
""")
lnk_sales.createOrReplaceTempView("lnk_sales")

# COMMAND ----------
# src_fact_returns: Delta table fact_returns (was #pDwhDir#/fact_returns.csv)
lnk_ret = spark.table(f"{catalog}.{schema}.fact_returns")
lnk_ret.createOrReplaceTempView("lnk_ret")

# COMMAND ----------
# agg_Product_Returns: Aggregator (group by product_sk; sum returned_amount)
lnk_ret_agg = spark.sql(f"""
SELECT
  product_sk,
  CAST(SUM(returned_amount) AS decimal(18,2)) AS returned
FROM lnk_ret
GROUP BY product_sk
""")
lnk_ret_agg.createOrReplaceTempView("lnk_ret_agg")

# COMMAND ----------
# jn_Sales_Returns: Join (left outer) on product_sk
lnk_sr = spark.sql(f"""
SELECT
  l.product_sk,
  l.units_sold,
  l.revenue,
  l.profit,
  r.returned
FROM lnk_sales l
LEFT OUTER JOIN lnk_ret_agg r ON l.product_sk = r.product_sk
""")
lnk_sr.createOrReplaceTempView("lnk_sr")

# COMMAND ----------
# xfm_Returns_Defaults: Transformer
lnk_sr_nn = spark.sql(f"""
SELECT
  product_sk,
  units_sold,
  revenue,
  profit,
  CAST(coalesce(returned, 0) AS decimal(18,2)) AS returned
FROM lnk_sr
""")
lnk_sr_nn.createOrReplaceTempView("lnk_sr_nn")

# COMMAND ----------
# xfm_KPIs: Transformer
lnk_kpi = spark.sql(f"""
SELECT
  product_sk,
  units_sold,
  revenue,
  profit,
  returned,
  CAST(CASE WHEN (revenue > 0) THEN (returned / revenue) ELSE 0 END AS decimal(18,6)) AS return_rate,
  CAST(CASE WHEN (revenue > 0) THEN (profit / revenue) ELSE 0 END AS decimal(18,6)) AS margin_pct,
  CAST(CASE WHEN (revenue >= 5000) THEN 'A' ELSE CASE WHEN (revenue >= 1000) THEN 'B' ELSE 'C' END END AS string) AS abc_class
FROM lnk_sr_nn
""")
lnk_kpi.createOrReplaceTempView("lnk_kpi")

# COMMAND ----------
# src_dim_product: Delta table dim_product (was #pDwhDir#/dim_product.csv)
lnk_dim_product = spark.table(f"{catalog}.{schema}.dim_product")
lnk_dim_product.createOrReplaceTempView("lnk_dim_product")

# COMMAND ----------
# jn_Product: Join (inner) on product_sk
lnk_named = spark.sql(f"""
SELECT
  l.product_sk,
  l.units_sold,
  l.revenue,
  l.profit,
  l.returned,
  l.return_rate,
  l.margin_pct,
  l.abc_class,
  r.stock_code,
  r.product_name,
  r.category,
  r.supplier_name
FROM lnk_kpi l
INNER JOIN lnk_dim_product r ON l.product_sk = r.product_sk
""")
lnk_named.createOrReplaceTempView("lnk_named")

# COMMAND ----------
# srt_Revenue: Sort (revenue desc)
lnk_sorted = spark.sql(f"""
SELECT * FROM lnk_named
ORDER BY revenue DESC
""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")

# COMMAND ----------
# cpy_Perf: Copy (output link lnk_all)
lnk_all = spark.sql(f"""
SELECT * FROM lnk_sorted
""")
lnk_all.createOrReplaceTempView("lnk_all")

# COMMAND ----------
# cpy_Perf: Copy (output link lnk_top_in)
lnk_top_in = spark.sql(f"""
SELECT * FROM lnk_sorted
""")
lnk_top_in.createOrReplaceTempView("lnk_top_in")

# COMMAND ----------
# tgt_mart_product_performance: write Delta table Product_Performance (was #pMartDir#/Product_Performance.csv)
(lnk_all.select('product_sk', 'units_sold', 'revenue', 'profit', 'returned', 'return_rate', 'margin_pct', 'abc_class', 'stock_code', 'product_name', 'category', 'supplier_name').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Product_Performance"))
print("Product_Performance:", spark.table(f"{catalog}.{schema}.Product_Performance").count(), "rows")

# COMMAND ----------
# hd_Top50: Head (first 50 rows; the part=0 partition setting has no equivalent in this single-node-logic batch)
lnk_top50 = spark.sql(f"""
SELECT * FROM lnk_top_in
LIMIT 50
""")
lnk_top50.createOrReplaceTempView("lnk_top50")

# COMMAND ----------
# tgt_top50_products: write Delta table Product_Top50 (was #pMartDir#/Product_Top50.csv)
(lnk_top50.select('product_sk', 'units_sold', 'revenue', 'profit', 'returned', 'return_rate', 'margin_pct', 'abc_class', 'stock_code', 'product_name', 'category', 'supplier_name').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.Product_Top50"))
print("Product_Top50:", spark.table(f"{catalog}.{schema}.Product_Top50").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
