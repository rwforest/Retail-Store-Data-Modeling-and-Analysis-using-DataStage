# Databricks notebook source
# Code converted on 2026-11-19 16:19:55
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
# Component lnk_stg, Type SOURCE Original node name src_stg_products, link lnk_stg
lnk_stg = spark.read.table(f'{catalog}.{schema}.stg_products')
lnk_stg.createOrReplaceTempView("lnk_stg")


# COMMAND ----------
# Component lnk_dedup, Type UNIQUE_ROW Original node name rmd_Product, link lnk_dedup
lnk_dedup = spark.sql(f"""
SELECT
stock_code,
product_name,
category,
list_price,
supplier_id,
status,
unit_cost,
price_band,
supplier_name,
supplier_country,
lead_time_days,
reliability_score
FROM (
SELECT
coalesce(lnk_stg.stock_code, '') as stock_code,
coalesce(lnk_stg.product_name, '') as product_name,
coalesce(lnk_stg.category, '') as category,
CAST(lnk_stg.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_stg.supplier_id, '') as supplier_id,
coalesce(lnk_stg.status, '') as status,
CAST(lnk_stg.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_stg.price_band, '') as price_band,
coalesce(lnk_stg.supplier_name, '') as supplier_name,
coalesce(lnk_stg.supplier_country, '') as supplier_country,
CAST(lnk_stg.lead_time_days AS int) as lead_time_days,
CAST(lnk_stg.reliability_score AS decimal(18,2)) as reliability_score,
ROW_NUMBER() OVER(PARTITION BY coalesce(lnk_stg.stock_code, '') ORDER BY coalesce(lnk_stg.product_name, ''), coalesce(lnk_stg.category, ''), CAST(lnk_stg.list_price AS decimal(18,2)), coalesce(lnk_stg.supplier_id, ''), coalesce(lnk_stg.status, ''), CAST(lnk_stg.unit_cost AS decimal(18,2)), coalesce(lnk_stg.price_band, ''), coalesce(lnk_stg.supplier_name, ''), coalesce(lnk_stg.supplier_country, ''), CAST(lnk_stg.lead_time_days AS int), CAST(lnk_stg.reliability_score AS decimal(18,2))) dedup_rnk
FROM lnk_stg) dedup_lnk_dedup WHERE dedup_rnk = 1""")
lnk_dedup.createOrReplaceTempView("lnk_dedup")


# COMMAND ----------
# Component lnk_sorted, Type SORTER Original node name srt_Product, link lnk_sorted
lnk_sorted = spark.sql(f"""
SELECT
coalesce(lnk_dedup.stock_code, '') as stock_code,
coalesce(lnk_dedup.product_name, '') as product_name,
coalesce(lnk_dedup.category, '') as category,
CAST(lnk_dedup.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_dedup.supplier_id, '') as supplier_id,
coalesce(lnk_dedup.status, '') as status,
CAST(lnk_dedup.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_dedup.price_band, '') as price_band,
coalesce(lnk_dedup.supplier_name, '') as supplier_name,
coalesce(lnk_dedup.supplier_country, '') as supplier_country,
CAST(lnk_dedup.lead_time_days AS int) as lead_time_days,
CAST(lnk_dedup.reliability_score AS decimal(18,2)) as reliability_score
FROM
lnk_dedup
ORDER BY stock_code asc""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")


# COMMAND ----------
# Component lnk_dim, Type TRANSFORMATION Original node name xfm_Product_SK, link lnk_dim
lnk_dim = spark.sql(f"""
SELECT
coalesce(lnk_sorted.stock_code, '') as stock_code,
coalesce(lnk_sorted.product_name, '') as product_name,
coalesce(lnk_sorted.category, '') as category,
CAST(lnk_sorted.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_sorted.supplier_id, '') as supplier_id,
coalesce(lnk_sorted.status, '') as status,
CAST(lnk_sorted.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_sorted.price_band, '') as price_band,
coalesce(lnk_sorted.supplier_name, '') as supplier_name,
coalesce(lnk_sorted.supplier_country, '') as supplier_country,
CAST(lnk_sorted.lead_time_days AS int) as lead_time_days,
CAST(lnk_sorted.reliability_score AS decimal(18,2)) as reliability_score,
row_number() over (order by 1) as product_sk,
IF ( coalesce(lnk_sorted.category, '') = 'Seasonal' , 'SEASONAL' , IF ( coalesce(lnk_sorted.category, '') = 'General Merchandise' , 'OTHER' , 'CORE' ) ) as category_group
FROM
lnk_sorted""")
lnk_dim.createOrReplaceTempView("lnk_dim")


# COMMAND ----------
# Component lnk_dim_cols, Type TRANSFORMATION Original node name xfm_Product_Cols, link lnk_dim_cols
lnk_dim_cols = spark.sql(f"""
SELECT
coalesce(lnk_dim.stock_code, '') as stock_code,
coalesce(lnk_dim.product_name, '') as product_name,
coalesce(lnk_dim.category, '') as category,
CAST(lnk_dim.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_dim.supplier_id, '') as supplier_id,
coalesce(lnk_dim.status, '') as status,
CAST(lnk_dim.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_dim.price_band, '') as price_band,
coalesce(lnk_dim.supplier_name, '') as supplier_name,
coalesce(lnk_dim.supplier_country, '') as supplier_country,
CAST(lnk_dim.lead_time_days AS int) as lead_time_days,
CAST(lnk_dim.reliability_score AS decimal(18,2)) as reliability_score,
CAST(lnk_dim.product_sk AS int) as product_sk,
coalesce(lnk_dim.category_group, '') as category_group
FROM
lnk_dim""")
lnk_dim_cols.createOrReplaceTempView("lnk_dim_cols")


# COMMAND ----------
# Component lnk_audit_in, Type REPLICATE Original node name cpy_Dim_Product, link lnk_audit_in
lnk_audit_in = spark.sql(f"""
SELECT
coalesce(lnk_dim_cols.stock_code, '') as stock_code,
coalesce(lnk_dim_cols.product_name, '') as product_name,
coalesce(lnk_dim_cols.category, '') as category,
CAST(lnk_dim_cols.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_dim_cols.supplier_id, '') as supplier_id,
coalesce(lnk_dim_cols.status, '') as status,
CAST(lnk_dim_cols.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_dim_cols.price_band, '') as price_band,
coalesce(lnk_dim_cols.supplier_name, '') as supplier_name,
coalesce(lnk_dim_cols.supplier_country, '') as supplier_country,
CAST(lnk_dim_cols.lead_time_days AS int) as lead_time_days,
CAST(lnk_dim_cols.reliability_score AS decimal(18,2)) as reliability_score,
CAST(lnk_dim_cols.product_sk AS int) as product_sk,
coalesce(lnk_dim_cols.category_group, '') as category_group
FROM
lnk_dim_cols""")
lnk_audit_in.createOrReplaceTempView("lnk_audit_in")


# COMMAND ----------
# Component lnk_dim_out, Type REPLICATE Original node name cpy_Dim_Product, link lnk_dim_out
lnk_dim_out = spark.sql(f"""
SELECT
coalesce(lnk_dim_cols.stock_code, '') as stock_code,
coalesce(lnk_dim_cols.product_name, '') as product_name,
coalesce(lnk_dim_cols.category, '') as category,
CAST(lnk_dim_cols.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_dim_cols.supplier_id, '') as supplier_id,
coalesce(lnk_dim_cols.status, '') as status,
CAST(lnk_dim_cols.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_dim_cols.price_band, '') as price_band,
coalesce(lnk_dim_cols.supplier_name, '') as supplier_name,
coalesce(lnk_dim_cols.supplier_country, '') as supplier_country,
CAST(lnk_dim_cols.lead_time_days AS int) as lead_time_days,
CAST(lnk_dim_cols.reliability_score AS decimal(18,2)) as reliability_score,
CAST(lnk_dim_cols.product_sk AS int) as product_sk,
coalesce(lnk_dim_cols.category_group, '') as category_group
FROM
lnk_dim_cols""")
lnk_dim_out.createOrReplaceTempView("lnk_dim_out")


# COMMAND ----------
# Component tgt_dim_product, Type TARGET 

tgt_dim_product = lnk_dim_out.select( \
	coalesce(lnk_dim_out.stock_code, lit('')).alias('stock_code'), \
	coalesce(lnk_dim_out.product_name, lit('')).alias('product_name'), \
	coalesce(lnk_dim_out.category, lit('')).alias('category'), \
	lnk_dim_out.list_price.cast('decimal(18,2)').alias('list_price'), \
	coalesce(lnk_dim_out.supplier_id, lit('')).alias('supplier_id'), \
	coalesce(lnk_dim_out.status, lit('')).alias('status'), \
	lnk_dim_out.unit_cost.cast('decimal(18,2)').alias('unit_cost'), \
	coalesce(lnk_dim_out.price_band, lit('')).alias('price_band'), \
	coalesce(lnk_dim_out.supplier_name, lit('')).alias('supplier_name'), \
	coalesce(lnk_dim_out.supplier_country, lit('')).alias('supplier_country'), \
	lnk_dim_out.lead_time_days.cast('int').alias('lead_time_days'), \
	lnk_dim_out.reliability_score.cast('decimal(18,2)').alias('reliability_score'), \
	lnk_dim_out.product_sk.cast('int').alias('product_sk'), \
	coalesce(lnk_dim_out.category_group, lit('')).alias('category_group') \
)
tgt_dim_product.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.dim_product')


# COMMAND ----------
# Component lnk_cat_counts, Type AGGREGATOR Original node name agg_Products_By_Category, link lnk_cat_counts
lnk_cat_counts = spark.sql(f"""
SELECT
coalesce(lnk_audit_in.category, '') as category,
COUNT(*) as products
FROM
lnk_audit_in
GROUP BY
coalesce(lnk_audit_in.category, '')""")
lnk_cat_counts.createOrReplaceTempView("lnk_cat_counts")


# COMMAND ----------
# Component tgt_audit_products_by_category, Type TARGET 

tgt_audit_products_by_category = lnk_cat_counts.select( \
	coalesce(lnk_cat_counts.category, lit('')).alias('category'), \
	lnk_cat_counts.products.cast('int').alias('products') \
)
tgt_audit_products_by_category.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.audit_products_by_category')
