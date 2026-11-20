# Databricks notebook source
# Code converted on 2026-11-19 16:23:20
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.functions import lit, to_timestamp, when, expr, col, explode
from databricks_conversion_supplements import DatabricksConversionSupplements


# COMMAND ----------

dbutils.widgets.text(name = 'pSrcDir', defaultValue = 'C:/case_study/Source_Data')
pSrcDir = dbutils.widgets.get("pSrcDir")

dbutils.widgets.text(name = 'pStgDir', defaultValue = 'C:/case_study/Staging')
pStgDir = dbutils.widgets.get("pStgDir")

dbutils.widgets.text(name = 'pDwhDir', defaultValue = 'C:/case_study/Warehouse')
pDwhDir = dbutils.widgets.get("pDwhDir")

dbutils.widgets.text(name = 'pMartDir', defaultValue = 'C:/case_study/Marts')
pMartDir = dbutils.widgets.get("pMartDir")

dbutils.widgets.text(name = 'pAuditDir', defaultValue = 'C:/case_study/Audit')
pAuditDir = dbutils.widgets.get("pAuditDir")

dbutils.widgets.text(name = 'pRunDate', defaultValue = '2011-12-31')
pRunDate = dbutils.widgets.get("pRunDate")

dbutils.widgets.text(name = 'pEffDate', defaultValue = '2011-07-01')
pEffDate = dbutils.widgets.get("pEffDate")

dbutils.widgets.text(name = 'catalog', defaultValue = 'unitygo')
catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text(name = 'schema', defaultValue = 'retail_dw_dbxconv')
schema = dbutils.widgets.get("schema")


# COMMAND ----------
# Component lnk_dim_product, Type SOURCE Original node name src_dim_product, link lnk_dim_product
lnk_dim_product = spark.read.table(f'{catalog}.{schema}.dim_product')
lnk_dim_product.createOrReplaceTempView("lnk_dim_product")


# COMMAND ----------
# Component lnk_ret, Type SOURCE Original node name src_fact_returns, link lnk_ret
lnk_ret = spark.read.table(f'{catalog}.{schema}.fact_returns')
lnk_ret.createOrReplaceTempView("lnk_ret")


# COMMAND ----------
# Component lnk_fact, Type SOURCE Original node name src_fact_sales, link lnk_fact
lnk_fact = spark.read.table(f'{catalog}.{schema}.fact_sales')
lnk_fact.createOrReplaceTempView("lnk_fact")


# COMMAND ----------
# Component lnk_ret_agg, Type AGGREGATOR Original node name agg_Product_Returns, link lnk_ret_agg
lnk_ret_agg = spark.sql(f"""
SELECT
CAST(lnk_ret.product_sk AS int) as product_sk,
SUM(returned_amount) as returned
FROM
lnk_ret
GROUP BY
CAST(lnk_ret.product_sk AS int)""")
lnk_ret_agg.createOrReplaceTempView("lnk_ret_agg")


# COMMAND ----------
# Component lnk_sales, Type AGGREGATOR Original node name agg_Product_Sales, link lnk_sales
lnk_sales = spark.sql(f"""
SELECT
CAST(lnk_fact.product_sk AS int) as product_sk,
SUM(quantity) as units_sold,
SUM(sales_amount) as revenue,
SUM(gross_profit) as profit
FROM
lnk_fact
GROUP BY
CAST(lnk_fact.product_sk AS int)""")
lnk_sales.createOrReplaceTempView("lnk_sales")


# COMMAND ----------
# Component lnk_sr, Type JOINER Original node name jn_Sales_Returns, link lnk_sr
lnk_sr = spark.sql(f"""
SELECT
CAST(lnk_sales.product_sk AS int) as product_sk,
CAST(lnk_sales.units_sold AS bigint) as units_sold,
CAST(lnk_sales.revenue AS decimal(18,2)) as revenue,
CAST(lnk_sales.profit AS decimal(18,2)) as profit,
CAST(lnk_ret_agg.returned AS decimal(18,2)) as returned
FROM
lnk_sales
LEFT JOIN lnk_ret_agg ON CAST(lnk_sales.product_sk AS int) = CAST(lnk_ret_agg.product_sk AS int)""")
lnk_sr.createOrReplaceTempView("lnk_sr")


# COMMAND ----------
# Component lnk_sr_nn, Type TRANSFORMATION Original node name xfm_Returns_Defaults, link lnk_sr_nn
lnk_sr_nn = spark.sql(f"""
SELECT
CAST(lnk_sr.product_sk AS int) as product_sk,
CAST(lnk_sr.units_sold AS bigint) as units_sold,
CAST(lnk_sr.revenue AS decimal(18,2)) as revenue,
CAST(lnk_sr.profit AS decimal(18,2)) as profit,
 COALESCE(CAST(lnk_sr.returned AS decimal(18,2)), 0) as returned
FROM
lnk_sr""")
lnk_sr_nn.createOrReplaceTempView("lnk_sr_nn")


# COMMAND ----------
# Component lnk_kpi, Type TRANSFORMATION Original node name xfm_KPIs, link lnk_kpi
lnk_kpi = spark.sql(f"""
SELECT
CAST(lnk_sr_nn.product_sk AS int) as product_sk,
CAST(lnk_sr_nn.units_sold AS bigint) as units_sold,
CAST(lnk_sr_nn.revenue AS decimal(18,2)) as revenue,
CAST(lnk_sr_nn.profit AS decimal(18,2)) as profit,
CAST(lnk_sr_nn.returned AS decimal(18,2)) as returned,
IF ( CAST(lnk_sr_nn.revenue AS decimal(18,2)) > 0 , CAST(lnk_sr_nn.returned AS decimal(18,2)) / CAST(lnk_sr_nn.revenue AS decimal(18,2)) , 0 ) as return_rate,
IF ( CAST(lnk_sr_nn.revenue AS decimal(18,2)) > 0 , CAST(lnk_sr_nn.profit AS decimal(18,2)) / CAST(lnk_sr_nn.revenue AS decimal(18,2)) , 0 ) as margin_pct,
IF ( CAST(lnk_sr_nn.revenue AS decimal(18,2)) >= 5000 , 'A' , IF ( CAST(lnk_sr_nn.revenue AS decimal(18,2)) >= 1000 , 'B' , 'C' ) ) as abc_class
FROM
lnk_sr_nn""")
lnk_kpi.createOrReplaceTempView("lnk_kpi")


# COMMAND ----------
# Component lnk_named, Type JOINER Original node name jn_Product, link lnk_named
lnk_named = spark.sql(f"""
SELECT
CAST(lnk_kpi.product_sk AS int) as product_sk,
CAST(lnk_kpi.units_sold AS bigint) as units_sold,
CAST(lnk_kpi.revenue AS decimal(18,2)) as revenue,
CAST(lnk_kpi.profit AS decimal(18,2)) as profit,
CAST(lnk_kpi.returned AS decimal(18,2)) as returned,
CAST(lnk_kpi.return_rate AS decimal(18,6)) as return_rate,
CAST(lnk_kpi.margin_pct AS decimal(18,6)) as margin_pct,
coalesce(lnk_kpi.abc_class, '') as abc_class,
coalesce(lnk_dim_product.stock_code, '') as stock_code,
coalesce(lnk_dim_product.product_name, '') as product_name,
coalesce(lnk_dim_product.category, '') as category,
coalesce(lnk_dim_product.supplier_name, '') as supplier_name
FROM
lnk_kpi
INNER JOIN lnk_dim_product ON CAST(lnk_kpi.product_sk AS int) = CAST(lnk_dim_product.product_sk AS int)""")
lnk_named.createOrReplaceTempView("lnk_named")


# COMMAND ----------
# Component lnk_sorted, Type SORTER Original node name srt_Revenue, link lnk_sorted
lnk_sorted = spark.sql(f"""
SELECT
CAST(lnk_named.product_sk AS int) as product_sk,
CAST(lnk_named.units_sold AS bigint) as units_sold,
CAST(lnk_named.revenue AS decimal(18,2)) as revenue,
CAST(lnk_named.profit AS decimal(18,2)) as profit,
CAST(lnk_named.returned AS decimal(18,2)) as returned,
CAST(lnk_named.return_rate AS decimal(18,6)) as return_rate,
CAST(lnk_named.margin_pct AS decimal(18,6)) as margin_pct,
coalesce(lnk_named.abc_class, '') as abc_class,
coalesce(lnk_named.stock_code, '') as stock_code,
coalesce(lnk_named.product_name, '') as product_name,
coalesce(lnk_named.category, '') as category,
coalesce(lnk_named.supplier_name, '') as supplier_name
FROM
lnk_named
ORDER BY revenue desc""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")


# COMMAND ----------
# Component lnk_top_in, Type REPLICATE Original node name cpy_Perf, link lnk_top_in
lnk_top_in = spark.sql(f"""
SELECT
CAST(lnk_sorted.product_sk AS int) as product_sk,
CAST(lnk_sorted.units_sold AS bigint) as units_sold,
CAST(lnk_sorted.revenue AS decimal(18,2)) as revenue,
CAST(lnk_sorted.profit AS decimal(18,2)) as profit,
CAST(lnk_sorted.returned AS decimal(18,2)) as returned,
CAST(lnk_sorted.return_rate AS decimal(18,6)) as return_rate,
CAST(lnk_sorted.margin_pct AS decimal(18,6)) as margin_pct,
coalesce(lnk_sorted.abc_class, '') as abc_class,
coalesce(lnk_sorted.stock_code, '') as stock_code,
coalesce(lnk_sorted.product_name, '') as product_name,
coalesce(lnk_sorted.category, '') as category,
coalesce(lnk_sorted.supplier_name, '') as supplier_name
FROM
lnk_sorted""")
lnk_top_in.createOrReplaceTempView("lnk_top_in")


# COMMAND ----------
# Component lnk_all, Type REPLICATE Original node name cpy_Perf, link lnk_all
lnk_all = spark.sql(f"""
SELECT
CAST(lnk_sorted.product_sk AS int) as product_sk,
CAST(lnk_sorted.units_sold AS bigint) as units_sold,
CAST(lnk_sorted.revenue AS decimal(18,2)) as revenue,
CAST(lnk_sorted.profit AS decimal(18,2)) as profit,
CAST(lnk_sorted.returned AS decimal(18,2)) as returned,
CAST(lnk_sorted.return_rate AS decimal(18,6)) as return_rate,
CAST(lnk_sorted.margin_pct AS decimal(18,6)) as margin_pct,
coalesce(lnk_sorted.abc_class, '') as abc_class,
coalesce(lnk_sorted.stock_code, '') as stock_code,
coalesce(lnk_sorted.product_name, '') as product_name,
coalesce(lnk_sorted.category, '') as category,
coalesce(lnk_sorted.supplier_name, '') as supplier_name
FROM
lnk_sorted""")
lnk_all.createOrReplaceTempView("lnk_all")


lnk_top50 = spark.sql(f"""
SELECT * FROM lnk_top_in
ORDER BY revenue DESC
LIMIT 50
""")
lnk_top50.createOrReplaceTempView("lnk_top50")


# COMMAND ----------
# Component tgt_mart_product_performance, Type TARGET 

tgt_mart_product_performance = lnk_all.select( \
	lnk_all.product_sk.cast('int').alias('product_sk'), \
	lnk_all.units_sold.cast('bigint').alias('units_sold'), \
	lnk_all.revenue.cast('decimal(18,2)').alias('revenue'), \
	lnk_all.profit.cast('decimal(18,2)').alias('profit'), \
	lnk_all.returned.cast('decimal(18,2)').alias('returned'), \
	lnk_all.return_rate.cast('decimal(18,6)').alias('return_rate'), \
	lnk_all.margin_pct.cast('decimal(18,6)').alias('margin_pct'), \
	coalesce(lnk_all.abc_class, lit('')).alias('abc_class'), \
	coalesce(lnk_all.stock_code, lit('')).alias('stock_code'), \
	coalesce(lnk_all.product_name, lit('')).alias('product_name'), \
	coalesce(lnk_all.category, lit('')).alias('category'), \
	coalesce(lnk_all.supplier_name, lit('')).alias('supplier_name') \
)
tgt_mart_product_performance.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.Product_Performance')


# COMMAND ----------
# Component tgt_top50_products, Type TARGET 

tgt_top50_products = lnk_top50.select( \
	lnk_top50.product_sk.cast('int').alias('product_sk'), \
	lnk_top50.units_sold.cast('bigint').alias('units_sold'), \
	lnk_top50.revenue.cast('decimal(18,2)').alias('revenue'), \
	lnk_top50.profit.cast('decimal(18,2)').alias('profit'), \
	lnk_top50.returned.cast('decimal(18,2)').alias('returned'), \
	lnk_top50.return_rate.cast('decimal(18,6)').alias('return_rate'), \
	lnk_top50.margin_pct.cast('decimal(18,6)').alias('margin_pct'), \
	coalesce(lnk_top50.abc_class, lit('')).alias('abc_class'), \
	coalesce(lnk_top50.stock_code, lit('')).alias('stock_code'), \
	coalesce(lnk_top50.product_name, lit('')).alias('product_name'), \
	coalesce(lnk_top50.category, lit('')).alias('category'), \
	coalesce(lnk_top50.supplier_name, lit('')).alias('supplier_name') \
)
tgt_top50_products.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.Product_Top50')
