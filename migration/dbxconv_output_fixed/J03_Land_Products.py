# Databricks notebook source
# Code converted on 2026-11-19 16:18:25
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
# Component lnk_products, Type SOURCE Original node name src_products, link lnk_products
lnk_products = spark.read.format('csv').option('header', 'true').option('escape', '"').load(f'''{pSrcDir}/products_catalog.csv''')
# Conforming fields names to the component layout
lnk_products_conformed_cols = ["StockCode", "Description", "Category", "ListPrice", "SupplierID", "Status"]
lnk_products = DatabricksConversionSupplements.conform_df_columns(lnk_products,lnk_products_conformed_cols)
lnk_products.createOrReplaceTempView("lnk_products")


# COMMAND ----------
# Component lnk_suppliers, Type SOURCE Original node name src_suppliers, link lnk_suppliers
lnk_suppliers = spark.read.format('csv').option('header', 'true').option('escape', '"').load(f'''{pSrcDir}/suppliers.csv''')
# Conforming fields names to the component layout
lnk_suppliers_conformed_cols = ["SupplierID", "SupplierName", "SupplierCountry", "LeadTimeDays", "ReliabilityScore"]
lnk_suppliers = DatabricksConversionSupplements.conform_df_columns(lnk_suppliers,lnk_suppliers_conformed_cols)
lnk_suppliers.createOrReplaceTempView("lnk_suppliers")


# COMMAND ----------
# Component lnk_std_prod, Type TRANSFORMATION Original node name xfm_Std_Product, link lnk_std_prod
lnk_std_prod = spark.sql(f"""
SELECT
UPPER(trim(coalesce(lnk_products.StockCode, ''))) as stock_code,
IF ( Len ( trim(coalesce(lnk_products.Description, '')) ) = 0 , 'UNKNOWN' , UPPER(trim(coalesce(lnk_products.Description, ''))) ) as product_name,
trim(coalesce(lnk_products.Category, '')) as category,
CAST(lnk_products.ListPrice AS decimal(18,2)) as list_price,
trim(coalesce(lnk_products.SupplierID, '')) as supplier_id,
trim(coalesce(lnk_products.Status, '')) as status,
CAST(lnk_products.ListPrice AS decimal(18,2)) * 0.55 as unit_cost,
IF ( CAST(lnk_products.ListPrice AS decimal(18,2)) < 1 , 'LOW' , IF ( CAST(lnk_products.ListPrice AS decimal(18,2)) < 5 , 'MID' , 'HIGH' ) ) as price_band
FROM
lnk_products""")
lnk_std_prod.createOrReplaceTempView("lnk_std_prod")


# COMMAND ----------
# Component lnk_std_sup, Type TRANSFORMATION Original node name xfm_Std_Supplier, link lnk_std_sup
lnk_std_sup = spark.sql(f"""
SELECT
trim(coalesce(lnk_suppliers.SupplierID, '')) as supplier_id,
trim(coalesce(lnk_suppliers.SupplierName, '')) as supplier_name,
trim(coalesce(lnk_suppliers.SupplierCountry, '')) as supplier_country,
CAST(lnk_suppliers.LeadTimeDays AS int) as lead_time_days,
CAST(lnk_suppliers.ReliabilityScore AS decimal(18,2)) as reliability_score
FROM
lnk_suppliers""")
lnk_std_sup.createOrReplaceTempView("lnk_std_sup")


# COMMAND ----------
# Component lnk_prod_sup, Type JOINER Original node name jn_Product_Supplier, link lnk_prod_sup
lnk_prod_sup = spark.sql(f"""
SELECT
coalesce(lnk_std_prod.stock_code, '') as stock_code,
coalesce(lnk_std_prod.product_name, '') as product_name,
coalesce(lnk_std_prod.category, '') as category,
CAST(lnk_std_prod.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_std_prod.supplier_id, '') as supplier_id,
coalesce(lnk_std_prod.status, '') as status,
CAST(lnk_std_prod.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_std_prod.price_band, '') as price_band,
lnk_std_sup.supplier_name as supplier_name,
lnk_std_sup.supplier_country as supplier_country,
CAST(lnk_std_sup.lead_time_days AS int) as lead_time_days,
CAST(lnk_std_sup.reliability_score AS decimal(18,2)) as reliability_score
FROM
lnk_std_prod
LEFT JOIN lnk_std_sup ON coalesce(lnk_std_prod.supplier_id, '') = lnk_std_sup.supplier_id""")
lnk_prod_sup.createOrReplaceTempView("lnk_prod_sup")


# COMMAND ----------
# Component lnk_prod_sup_nn, Type TRANSFORMATION Original node name xfm_Supplier_Defaults, link lnk_prod_sup_nn
lnk_prod_sup_nn = spark.sql(f"""
SELECT
coalesce(lnk_prod_sup.stock_code, '') as stock_code,
coalesce(lnk_prod_sup.product_name, '') as product_name,
coalesce(lnk_prod_sup.category, '') as category,
CAST(lnk_prod_sup.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_prod_sup.supplier_id, '') as supplier_id,
coalesce(lnk_prod_sup.status, '') as status,
CAST(lnk_prod_sup.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_prod_sup.price_band, '') as price_band,
 COALESCE(lnk_prod_sup.supplier_name, 'UNKNOWN') as supplier_name,
 COALESCE(lnk_prod_sup.supplier_country, 'UNKNOWN') as supplier_country,
 COALESCE(CAST(lnk_prod_sup.lead_time_days AS int), 0) as lead_time_days,
 COALESCE(CAST(lnk_prod_sup.reliability_score AS decimal(18,2)), 0) as reliability_score
FROM
lnk_prod_sup""")
lnk_prod_sup_nn.createOrReplaceTempView("lnk_prod_sup_nn")


# COMMAND ----------
# Component lnk_dedup, Type UNIQUE_ROW Original node name rmd_Products, link lnk_dedup
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
coalesce(lnk_prod_sup_nn.stock_code, '') as stock_code,
coalesce(lnk_prod_sup_nn.product_name, '') as product_name,
coalesce(lnk_prod_sup_nn.category, '') as category,
CAST(lnk_prod_sup_nn.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_prod_sup_nn.supplier_id, '') as supplier_id,
coalesce(lnk_prod_sup_nn.status, '') as status,
CAST(lnk_prod_sup_nn.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_prod_sup_nn.price_band, '') as price_band,
coalesce(lnk_prod_sup_nn.supplier_name, '') as supplier_name,
coalesce(lnk_prod_sup_nn.supplier_country, '') as supplier_country,
CAST(lnk_prod_sup_nn.lead_time_days AS int) as lead_time_days,
CAST(lnk_prod_sup_nn.reliability_score AS decimal(18,2)) as reliability_score,
ROW_NUMBER() OVER(PARTITION BY coalesce(lnk_prod_sup_nn.stock_code, '') ORDER BY coalesce(lnk_prod_sup_nn.product_name, ''), coalesce(lnk_prod_sup_nn.category, ''), CAST(lnk_prod_sup_nn.list_price AS decimal(18,2)), coalesce(lnk_prod_sup_nn.supplier_id, ''), coalesce(lnk_prod_sup_nn.status, ''), CAST(lnk_prod_sup_nn.unit_cost AS decimal(18,2)), coalesce(lnk_prod_sup_nn.price_band, ''), coalesce(lnk_prod_sup_nn.supplier_name, ''), coalesce(lnk_prod_sup_nn.supplier_country, ''), CAST(lnk_prod_sup_nn.lead_time_days AS int), CAST(lnk_prod_sup_nn.reliability_score AS decimal(18,2))) dedup_rnk
FROM lnk_prod_sup_nn) dedup_lnk_dedup WHERE dedup_rnk = 1""")
lnk_dedup.createOrReplaceTempView("lnk_dedup")


# COMMAND ----------
# Component lnk_for_active, Type REPLICATE Original node name cpy_Products, link lnk_for_active
lnk_for_active = spark.sql(f"""
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
lnk_dedup""")
lnk_for_active.createOrReplaceTempView("lnk_for_active")


# COMMAND ----------
# Component lnk_all, Type REPLICATE Original node name cpy_Products, link lnk_all
lnk_all = spark.sql(f"""
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
lnk_dedup""")
lnk_all.createOrReplaceTempView("lnk_all")


# COMMAND ----------
# Component lnk_active, Type TRANSFORMATION Original node name flt_Active_Products, link lnk_active
lnk_active = spark.sql(f"""
SELECT
coalesce(lnk_for_active.stock_code, '') as stock_code,
coalesce(lnk_for_active.product_name, '') as product_name,
coalesce(lnk_for_active.category, '') as category,
CAST(lnk_for_active.list_price AS decimal(18,2)) as list_price,
coalesce(lnk_for_active.supplier_id, '') as supplier_id,
coalesce(lnk_for_active.status, '') as status,
CAST(lnk_for_active.unit_cost AS decimal(18,2)) as unit_cost,
coalesce(lnk_for_active.price_band, '') as price_band,
coalesce(lnk_for_active.supplier_name, '') as supplier_name,
coalesce(lnk_for_active.supplier_country, '') as supplier_country,
CAST(lnk_for_active.lead_time_days AS int) as lead_time_days,
CAST(lnk_for_active.reliability_score AS decimal(18,2)) as reliability_score
FROM
lnk_for_active
WHERE status = 'ACTIVE'""")
lnk_active.createOrReplaceTempView("lnk_active")


# COMMAND ----------
# Component tgt_stg_active_products, Type TARGET 

tgt_stg_active_products = lnk_active.select( \
	coalesce(lnk_active.stock_code, lit('')).alias('stock_code'), \
	coalesce(lnk_active.product_name, lit('')).alias('product_name'), \
	coalesce(lnk_active.category, lit('')).alias('category'), \
	lnk_active.list_price.cast('decimal(18,2)').alias('list_price'), \
	coalesce(lnk_active.supplier_id, lit('')).alias('supplier_id'), \
	coalesce(lnk_active.status, lit('')).alias('status'), \
	lnk_active.unit_cost.cast('decimal(18,2)').alias('unit_cost'), \
	coalesce(lnk_active.price_band, lit('')).alias('price_band'), \
	coalesce(lnk_active.supplier_name, lit('')).alias('supplier_name'), \
	coalesce(lnk_active.supplier_country, lit('')).alias('supplier_country'), \
	lnk_active.lead_time_days.cast('int').alias('lead_time_days'), \
	lnk_active.reliability_score.cast('decimal(18,2)').alias('reliability_score') \
)
tgt_stg_active_products.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.stg_active_products')


# COMMAND ----------
# Component tgt_stg_products, Type TARGET 

tgt_stg_products = lnk_all.select( \
	coalesce(lnk_all.stock_code, lit('')).alias('stock_code'), \
	coalesce(lnk_all.product_name, lit('')).alias('product_name'), \
	coalesce(lnk_all.category, lit('')).alias('category'), \
	lnk_all.list_price.cast('decimal(18,2)').alias('list_price'), \
	coalesce(lnk_all.supplier_id, lit('')).alias('supplier_id'), \
	coalesce(lnk_all.status, lit('')).alias('status'), \
	lnk_all.unit_cost.cast('decimal(18,2)').alias('unit_cost'), \
	coalesce(lnk_all.price_band, lit('')).alias('price_band'), \
	coalesce(lnk_all.supplier_name, lit('')).alias('supplier_name'), \
	coalesce(lnk_all.supplier_country, lit('')).alias('supplier_country'), \
	lnk_all.lead_time_days.cast('int').alias('lead_time_days'), \
	lnk_all.reliability_score.cast('decimal(18,2)').alias('reliability_score') \
)
tgt_stg_products.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.stg_products')
