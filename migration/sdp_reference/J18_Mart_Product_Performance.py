# Databricks notebook source
# J18_Mart_Product_Performance - Product performance: revenue, margin, return rate, ABC class
# Layer: 4_Marts. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_fact_sales: dataset fact_sales
@dp.temporary_view(name="j18_lnk_fact")
def j18_lnk_fact():
    return spark.read.table("fact_sales")

# COMMAND ----------
# agg_Product_Sales: Aggregator
@dp.temporary_view(name="j18_lnk_sales")
def j18_lnk_sales():
    return spark.sql(f"""
SELECT product_sk, CAST(SUM(quantity) AS bigint) AS units_sold, CAST(SUM(sales_amount) AS decimal(18,2)) AS revenue, CAST(SUM(gross_profit) AS decimal(18,2)) AS profit
FROM j18_lnk_fact
GROUP BY product_sk
""")

# COMMAND ----------
# src_fact_returns: dataset fact_returns
@dp.temporary_view(name="j18_lnk_ret")
def j18_lnk_ret():
    return spark.read.table("fact_returns")

# COMMAND ----------
# agg_Product_Returns: Aggregator
@dp.temporary_view(name="j18_lnk_ret_agg")
def j18_lnk_ret_agg():
    return spark.sql(f"""
SELECT product_sk, CAST(SUM(returned_amount) AS decimal(18,2)) AS returned
FROM j18_lnk_ret
GROUP BY product_sk
""")

# COMMAND ----------
# jn_Sales_Returns: Join (left outer) on product_sk
@dp.temporary_view(name="j18_lnk_sr")
def j18_lnk_sr():
    return spark.sql(f"""
SELECT
  l.product_sk,
  l.units_sold,
  l.revenue,
  l.profit,
  r.returned
FROM j18_lnk_sales l
LEFT OUTER JOIN j18_lnk_ret_agg r ON l.product_sk = r.product_sk
""")

# COMMAND ----------
# xfm_Returns_Defaults: Transformer
@dp.temporary_view(name="j18_lnk_sr_nn")
def j18_lnk_sr_nn():
    return spark.sql(f"""
SELECT
  product_sk,
  units_sold,
  revenue,
  profit,
  CAST(coalesce(returned, 0) AS decimal(18,2)) AS returned
FROM j18_lnk_sr
""")

# COMMAND ----------
# xfm_KPIs: Transformer
@dp.temporary_view(name="j18_lnk_kpi")
def j18_lnk_kpi():
    return spark.sql(f"""
SELECT
  product_sk,
  units_sold,
  revenue,
  profit,
  returned,
  CAST(CASE WHEN (revenue > 0) THEN (returned / revenue) ELSE 0 END AS decimal(18,6)) AS return_rate,
  CAST(CASE WHEN (revenue > 0) THEN (profit / revenue) ELSE 0 END AS decimal(18,6)) AS margin_pct,
  CAST(CASE WHEN (revenue >= 5000) THEN 'A' ELSE CASE WHEN (revenue >= 1000) THEN 'B' ELSE 'C' END END AS string) AS abc_class
FROM j18_lnk_sr_nn
""")

# COMMAND ----------
# src_dim_product: dataset dim_product
@dp.temporary_view(name="j18_lnk_dim_product")
def j18_lnk_dim_product():
    return spark.read.table("dim_product")

# COMMAND ----------
# jn_Product: Join (inner) on product_sk
@dp.temporary_view(name="j18_lnk_named")
def j18_lnk_named():
    return spark.sql(f"""
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
FROM j18_lnk_kpi l
INNER JOIN j18_lnk_dim_product r ON l.product_sk = r.product_sk
""")

# COMMAND ----------
# srt_Revenue: Sort
@dp.temporary_view(name="j18_lnk_sorted")
def j18_lnk_sorted():
    return spark.sql(f"""
SELECT * FROM j18_lnk_named
ORDER BY revenue DESC
""")

# COMMAND ----------
# cpy_Perf: Copy
@dp.temporary_view(name="j18_lnk_all")
def j18_lnk_all():
    return spark.sql(f"""
SELECT * FROM j18_lnk_sorted
""")

# COMMAND ----------
# cpy_Perf: Copy
@dp.temporary_view(name="j18_lnk_top_in")
def j18_lnk_top_in():
    return spark.sql(f"""
SELECT * FROM j18_lnk_sorted
""")

# COMMAND ----------
# tgt_mart_product_performance: target file -> materialized view Product_Performance
@dp.materialized_view(name="Product_Performance")
def Product_Performance():
    return spark.read.table("j18_lnk_all").select('product_sk', 'units_sold', 'revenue', 'profit', 'returned', 'return_rate', 'margin_pct', 'abc_class', 'stock_code', 'product_name', 'category', 'supplier_name')

# COMMAND ----------
# hd_Top50: Head
@dp.temporary_view(name="j18_lnk_top50")
def j18_lnk_top50():
    return spark.sql(f"""
SELECT * FROM j18_lnk_top_in
ORDER BY revenue DESC
LIMIT 50
""")

# COMMAND ----------
# tgt_top50_products: target file -> materialized view Product_Top50
@dp.materialized_view(name="Product_Top50")
def Product_Top50():
    return spark.read.table("j18_lnk_top50").select('product_sk', 'units_sold', 'revenue', 'profit', 'returned', 'return_rate', 'margin_pct', 'abc_class', 'stock_code', 'product_name', 'category', 'supplier_name')
