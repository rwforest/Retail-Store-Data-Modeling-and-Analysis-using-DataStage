# Databricks notebook source
# Code converted on 2026-11-19 16:20:49
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
# Component lnk_dim_customer, Type SOURCE Original node name src_dim_customer, link lnk_dim_customer
lnk_dim_customer = spark.read.table(f'{catalog}.{schema}.dim_customer')
lnk_dim_customer.createOrReplaceTempView("lnk_dim_customer")


# COMMAND ----------
# Component lnk_dim_emp, Type SOURCE Original node name src_dim_employee, link lnk_dim_emp
lnk_dim_emp = spark.read.table(f'{catalog}.{schema}.dim_employee')
lnk_dim_emp.createOrReplaceTempView("lnk_dim_emp")


# COMMAND ----------
# Component lnk_dim_fx, Type SOURCE Original node name src_dim_fx, link lnk_dim_fx
lnk_dim_fx = spark.read.table(f'{catalog}.{schema}.dim_fx')
lnk_dim_fx.createOrReplaceTempView("lnk_dim_fx")


# COMMAND ----------
# Component lnk_dim_product, Type SOURCE Original node name src_dim_product, link lnk_dim_product
lnk_dim_product = spark.read.table(f'{catalog}.{schema}.dim_product')
lnk_dim_product.createOrReplaceTempView("lnk_dim_product")


# COMMAND ----------
# Component lnk_dim_store, Type SOURCE Original node name src_dim_store, link lnk_dim_store
lnk_dim_store = spark.read.table(f'{catalog}.{schema}.dim_store')
lnk_dim_store.createOrReplaceTempView("lnk_dim_store")


# COMMAND ----------
# Component lnk_orders, Type SOURCE Original node name src_stg_orders, link lnk_orders
lnk_orders = spark.read.table(f'{catalog}.{schema}.stg_orders')
lnk_orders.createOrReplaceTempView("lnk_orders")


# COMMAND ----------
# Component lnk_lines, Type TRANSFORMATION Original node name flt_Sales_Lines, link lnk_lines
lnk_lines = spark.sql(f"""
SELECT
coalesce(lnk_orders.invoice_no, '') as invoice_no,
coalesce(lnk_orders.stock_code, '') as stock_code,
coalesce(lnk_orders.description, '') as description,
CAST(lnk_orders.quantity AS int) as quantity,
lnk_orders.invoice_ts as invoice_ts,
CAST(lnk_orders.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_orders.customer_id AS int) as customer_id,
coalesce(lnk_orders.country, '') as country,
coalesce(lnk_orders.store_id, '') as store_id,
coalesce(lnk_orders.employee_id, '') as employee_id,
CAST(lnk_orders.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_orders.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_orders.is_cancel AS int) as is_cancel,
CAST(lnk_orders.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_orders.is_guest AS int) as is_guest
FROM
lnk_orders
WHERE is_cancel = 0 and is_nonproduct = 0 and quantity > 0""")
lnk_lines.createOrReplaceTempView("lnk_lines")


# COMMAND ----------
# Component lnk_keyed, Type TRANSFORMATION Original node name xfm_Fact_Keys, link lnk_keyed
lnk_keyed = spark.sql(f"""
SELECT
coalesce(lnk_lines.invoice_no, '') as invoice_no,
coalesce(lnk_lines.stock_code, '') as stock_code,
coalesce(lnk_lines.description, '') as description,
CAST(lnk_lines.quantity AS int) as quantity,
lnk_lines.invoice_ts as invoice_ts,
CAST(lnk_lines.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_lines.customer_id AS int) as customer_id,
coalesce(lnk_lines.country, '') as country,
coalesce(lnk_lines.store_id, '') as store_id,
coalesce(lnk_lines.employee_id, '') as employee_id,
CAST(lnk_lines.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_lines.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_lines.is_cancel AS int) as is_cancel,
CAST(lnk_lines.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_lines.is_guest AS int) as is_guest,
date_format(lnk_lines.invoice_ts, 'yyyyMM') as year_month
FROM
lnk_lines""")
lnk_keyed.createOrReplaceTempView("lnk_keyed")


# COMMAND ----------
# Component lnk_c, Type JOINER Original node name jn_Customer, link lnk_c
lnk_c = spark.sql(f"""
SELECT
coalesce(lnk_keyed.invoice_no, '') as invoice_no,
coalesce(lnk_keyed.stock_code, '') as stock_code,
coalesce(lnk_keyed.description, '') as description,
CAST(lnk_keyed.quantity AS int) as quantity,
lnk_keyed.invoice_ts as invoice_ts,
CAST(lnk_keyed.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_keyed.customer_id AS int) as customer_id,
coalesce(lnk_keyed.country, '') as country,
coalesce(lnk_keyed.store_id, '') as store_id,
coalesce(lnk_keyed.employee_id, '') as employee_id,
CAST(lnk_keyed.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_keyed.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_keyed.is_cancel AS int) as is_cancel,
CAST(lnk_keyed.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_keyed.is_guest AS int) as is_guest,
coalesce(lnk_keyed.year_month, '') as year_month,
CAST(lnk_dim_customer.customer_sk AS int) as customer_sk,
lnk_dim_customer.customer_type as customer_type,
lnk_dim_customer.loyalty_tier as loyalty_tier,
CAST(lnk_dim_customer.eff_start_key AS int) as eff_start_key,
CAST(lnk_dim_customer.eff_end_key AS int) as eff_end_key
FROM
lnk_keyed
LEFT JOIN lnk_dim_customer ON CAST(lnk_keyed.customer_id AS int) = CAST(lnk_dim_customer.customer_id AS int)""")
lnk_c.createOrReplaceTempView("lnk_c")


# COMMAND ----------
# Component lnk_c_pit, Type TRANSFORMATION Original node name flt_PIT_Customer, link lnk_c_pit
lnk_c_pit = spark.sql(f"""
SELECT
coalesce(lnk_c.invoice_no, '') as invoice_no,
coalesce(lnk_c.stock_code, '') as stock_code,
coalesce(lnk_c.description, '') as description,
CAST(lnk_c.quantity AS int) as quantity,
lnk_c.invoice_ts as invoice_ts,
CAST(lnk_c.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_c.customer_id AS int) as customer_id,
coalesce(lnk_c.country, '') as country,
coalesce(lnk_c.store_id, '') as store_id,
coalesce(lnk_c.employee_id, '') as employee_id,
CAST(lnk_c.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_c.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_c.is_cancel AS int) as is_cancel,
CAST(lnk_c.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_c.is_guest AS int) as is_guest,
coalesce(lnk_c.year_month, '') as year_month,
CAST(lnk_c.customer_sk AS int) as customer_sk,
lnk_c.customer_type as customer_type,
lnk_c.loyalty_tier as loyalty_tier,
CAST(lnk_c.eff_start_key AS int) as eff_start_key,
CAST(lnk_c.eff_end_key AS int) as eff_end_key
FROM
lnk_c
WHERE customer_sk is null or (eff_start_key <= invoice_date_key and eff_end_key >= invoice_date_key)""")
lnk_c_pit.createOrReplaceTempView("lnk_c_pit")


# COMMAND ----------
# Component lnk_c_nn, Type TRANSFORMATION Original node name xfm_Customer_Defaults, link lnk_c_nn
lnk_c_nn = spark.sql(f"""
SELECT
coalesce(lnk_c_pit.invoice_no, '') as invoice_no,
coalesce(lnk_c_pit.stock_code, '') as stock_code,
coalesce(lnk_c_pit.description, '') as description,
CAST(lnk_c_pit.quantity AS int) as quantity,
lnk_c_pit.invoice_ts as invoice_ts,
CAST(lnk_c_pit.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_c_pit.customer_id AS int) as customer_id,
coalesce(lnk_c_pit.country, '') as country,
coalesce(lnk_c_pit.store_id, '') as store_id,
coalesce(lnk_c_pit.employee_id, '') as employee_id,
CAST(lnk_c_pit.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_c_pit.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_c_pit.is_cancel AS int) as is_cancel,
CAST(lnk_c_pit.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_c_pit.is_guest AS int) as is_guest,
coalesce(lnk_c_pit.year_month, '') as year_month,
 COALESCE(CAST(lnk_c_pit.customer_sk AS int), - 1) as customer_sk,
 COALESCE(lnk_c_pit.customer_type, 'unknown') as customer_type,
 COALESCE(lnk_c_pit.loyalty_tier, 'NONE') as loyalty_tier
FROM
lnk_c_pit""")
lnk_c_nn.createOrReplaceTempView("lnk_c_nn")


# COMMAND ----------
# Component lnk_p, Type JOINER Original node name jn_Product, link lnk_p
lnk_p = spark.sql(f"""
SELECT
coalesce(lnk_c_nn.invoice_no, '') as invoice_no,
coalesce(lnk_c_nn.stock_code, '') as stock_code,
coalesce(lnk_c_nn.description, '') as description,
CAST(lnk_c_nn.quantity AS int) as quantity,
lnk_c_nn.invoice_ts as invoice_ts,
CAST(lnk_c_nn.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_c_nn.customer_id AS int) as customer_id,
coalesce(lnk_c_nn.country, '') as country,
coalesce(lnk_c_nn.store_id, '') as store_id,
coalesce(lnk_c_nn.employee_id, '') as employee_id,
CAST(lnk_c_nn.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_c_nn.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_c_nn.is_cancel AS int) as is_cancel,
CAST(lnk_c_nn.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_c_nn.is_guest AS int) as is_guest,
coalesce(lnk_c_nn.year_month, '') as year_month,
CAST(lnk_c_nn.customer_sk AS int) as customer_sk,
coalesce(lnk_c_nn.customer_type, '') as customer_type,
coalesce(lnk_c_nn.loyalty_tier, '') as loyalty_tier,
lnk_dim_product.category as category,
lnk_dim_product.supplier_id as supplier_id,
CAST(lnk_dim_product.unit_cost AS decimal(18,2)) as unit_cost,
CAST(lnk_dim_product.product_sk AS int) as product_sk
FROM
lnk_c_nn
LEFT JOIN lnk_dim_product ON coalesce(lnk_c_nn.stock_code, '') = lnk_dim_product.stock_code""")
lnk_p.createOrReplaceTempView("lnk_p")


# COMMAND ----------
# Component lnk_p_nn, Type TRANSFORMATION Original node name xfm_Product_Defaults, link lnk_p_nn
lnk_p_nn = spark.sql(f"""
SELECT
coalesce(lnk_p.invoice_no, '') as invoice_no,
coalesce(lnk_p.stock_code, '') as stock_code,
coalesce(lnk_p.description, '') as description,
CAST(lnk_p.quantity AS int) as quantity,
lnk_p.invoice_ts as invoice_ts,
CAST(lnk_p.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_p.customer_id AS int) as customer_id,
coalesce(lnk_p.country, '') as country,
coalesce(lnk_p.store_id, '') as store_id,
coalesce(lnk_p.employee_id, '') as employee_id,
CAST(lnk_p.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_p.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_p.is_cancel AS int) as is_cancel,
CAST(lnk_p.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_p.is_guest AS int) as is_guest,
coalesce(lnk_p.year_month, '') as year_month,
CAST(lnk_p.customer_sk AS int) as customer_sk,
coalesce(lnk_p.customer_type, '') as customer_type,
coalesce(lnk_p.loyalty_tier, '') as loyalty_tier,
 COALESCE(lnk_p.category, 'Unknown') as category,
 COALESCE(lnk_p.supplier_id, 'UNKNOWN') as supplier_id,
 COALESCE(CAST(lnk_p.unit_cost AS decimal(18,2)), 0) as unit_cost,
 COALESCE(CAST(lnk_p.product_sk AS int), - 1) as product_sk
FROM
lnk_p""")
lnk_p_nn.createOrReplaceTempView("lnk_p_nn")


# COMMAND ----------
# Component lnk_s, Type JOINER Original node name jn_Store, link lnk_s
lnk_s = spark.sql(f"""
SELECT
coalesce(lnk_p_nn.invoice_no, '') as invoice_no,
coalesce(lnk_p_nn.stock_code, '') as stock_code,
coalesce(lnk_p_nn.description, '') as description,
CAST(lnk_p_nn.quantity AS int) as quantity,
lnk_p_nn.invoice_ts as invoice_ts,
CAST(lnk_p_nn.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_p_nn.customer_id AS int) as customer_id,
coalesce(lnk_p_nn.country, '') as country,
coalesce(lnk_p_nn.store_id, '') as store_id,
coalesce(lnk_p_nn.employee_id, '') as employee_id,
CAST(lnk_p_nn.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_p_nn.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_p_nn.is_cancel AS int) as is_cancel,
CAST(lnk_p_nn.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_p_nn.is_guest AS int) as is_guest,
coalesce(lnk_p_nn.year_month, '') as year_month,
CAST(lnk_p_nn.customer_sk AS int) as customer_sk,
coalesce(lnk_p_nn.customer_type, '') as customer_type,
coalesce(lnk_p_nn.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_p_nn.category, '') as category,
coalesce(lnk_p_nn.supplier_id, '') as supplier_id,
CAST(lnk_p_nn.unit_cost AS decimal(18,2)) as unit_cost,
CAST(lnk_p_nn.product_sk AS int) as product_sk,
coalesce(lnk_dim_store.region, '') as region,
CAST(lnk_dim_store.store_sk AS int) as store_sk
FROM
lnk_p_nn
INNER JOIN lnk_dim_store ON coalesce(lnk_p_nn.store_id, '') = coalesce(lnk_dim_store.store_id, '')""")
lnk_s.createOrReplaceTempView("lnk_s")


# COMMAND ----------
# Component lnk_e, Type JOINER Original node name jn_Employee, link lnk_e
lnk_e = spark.sql(f"""
SELECT
coalesce(lnk_s.invoice_no, '') as invoice_no,
coalesce(lnk_s.stock_code, '') as stock_code,
coalesce(lnk_s.description, '') as description,
CAST(lnk_s.quantity AS int) as quantity,
lnk_s.invoice_ts as invoice_ts,
CAST(lnk_s.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_s.customer_id AS int) as customer_id,
coalesce(lnk_s.country, '') as country,
coalesce(lnk_s.store_id, '') as store_id,
coalesce(lnk_s.employee_id, '') as employee_id,
CAST(lnk_s.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_s.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_s.is_cancel AS int) as is_cancel,
CAST(lnk_s.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_s.is_guest AS int) as is_guest,
coalesce(lnk_s.year_month, '') as year_month,
CAST(lnk_s.customer_sk AS int) as customer_sk,
coalesce(lnk_s.customer_type, '') as customer_type,
coalesce(lnk_s.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_s.category, '') as category,
coalesce(lnk_s.supplier_id, '') as supplier_id,
CAST(lnk_s.unit_cost AS decimal(18,2)) as unit_cost,
CAST(lnk_s.product_sk AS int) as product_sk,
coalesce(lnk_s.region, '') as region,
CAST(lnk_s.store_sk AS int) as store_sk,
CAST(lnk_dim_emp.employee_sk AS int) as employee_sk
FROM
lnk_s
INNER JOIN lnk_dim_emp ON coalesce(lnk_s.employee_id, '') = coalesce(lnk_dim_emp.employee_id, '')""")
lnk_e.createOrReplaceTempView("lnk_e")


# COMMAND ----------
# Component lnk_f, Type JOINER Original node name jn_FX, link lnk_f
lnk_f = spark.sql(f"""
SELECT
coalesce(lnk_e.invoice_no, '') as invoice_no,
coalesce(lnk_e.stock_code, '') as stock_code,
coalesce(lnk_e.description, '') as description,
CAST(lnk_e.quantity AS int) as quantity,
lnk_e.invoice_ts as invoice_ts,
CAST(lnk_e.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_e.customer_id AS int) as customer_id,
coalesce(lnk_e.country, '') as country,
coalesce(lnk_e.store_id, '') as store_id,
coalesce(lnk_e.employee_id, '') as employee_id,
CAST(lnk_e.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_e.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_e.is_cancel AS int) as is_cancel,
CAST(lnk_e.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_e.is_guest AS int) as is_guest,
coalesce(lnk_e.year_month, '') as year_month,
CAST(lnk_e.customer_sk AS int) as customer_sk,
coalesce(lnk_e.customer_type, '') as customer_type,
coalesce(lnk_e.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_e.category, '') as category,
coalesce(lnk_e.supplier_id, '') as supplier_id,
CAST(lnk_e.unit_cost AS decimal(18,2)) as unit_cost,
CAST(lnk_e.product_sk AS int) as product_sk,
coalesce(lnk_e.region, '') as region,
CAST(lnk_e.store_sk AS int) as store_sk,
CAST(lnk_e.employee_sk AS int) as employee_sk,
lnk_dim_fx.currency as currency,
CAST(lnk_dim_fx.fx_rate AS decimal(18,6)) as fx_rate
FROM
lnk_e
LEFT JOIN lnk_dim_fx ON coalesce(lnk_e.country, '') = lnk_dim_fx.country AND coalesce(lnk_e.year_month, '') = lnk_dim_fx.year_month""")
lnk_f.createOrReplaceTempView("lnk_f")


# COMMAND ----------
# Component lnk_f_nn, Type TRANSFORMATION Original node name xfm_FX_Defaults, link lnk_f_nn
lnk_f_nn = spark.sql(f"""
SELECT
coalesce(lnk_f.invoice_no, '') as invoice_no,
coalesce(lnk_f.stock_code, '') as stock_code,
coalesce(lnk_f.description, '') as description,
CAST(lnk_f.quantity AS int) as quantity,
lnk_f.invoice_ts as invoice_ts,
CAST(lnk_f.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_f.customer_id AS int) as customer_id,
coalesce(lnk_f.country, '') as country,
coalesce(lnk_f.store_id, '') as store_id,
coalesce(lnk_f.employee_id, '') as employee_id,
CAST(lnk_f.invoice_date_key AS int) as invoice_date_key,
CAST(lnk_f.line_amount AS decimal(18,2)) as line_amount,
CAST(lnk_f.is_cancel AS int) as is_cancel,
CAST(lnk_f.is_nonproduct AS int) as is_nonproduct,
CAST(lnk_f.is_guest AS int) as is_guest,
coalesce(lnk_f.year_month, '') as year_month,
CAST(lnk_f.customer_sk AS int) as customer_sk,
coalesce(lnk_f.customer_type, '') as customer_type,
coalesce(lnk_f.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_f.category, '') as category,
coalesce(lnk_f.supplier_id, '') as supplier_id,
CAST(lnk_f.unit_cost AS decimal(18,2)) as unit_cost,
CAST(lnk_f.product_sk AS int) as product_sk,
coalesce(lnk_f.region, '') as region,
CAST(lnk_f.store_sk AS int) as store_sk,
CAST(lnk_f.employee_sk AS int) as employee_sk,
 COALESCE(lnk_f.currency, 'GBP') as currency,
 COALESCE(CAST(lnk_f.fx_rate AS decimal(18,6)), 1) as fx_rate
FROM
lnk_f""")
lnk_f_nn.createOrReplaceTempView("lnk_f_nn")


# COMMAND ----------
# Component lnk_fact, Type TRANSFORMATION Original node name xfm_Fact_Measures, link lnk_fact
lnk_fact = spark.sql(f"""
SELECT
coalesce(lnk_f_nn.invoice_no, '') as invoice_no,
coalesce(lnk_f_nn.stock_code, '') as stock_code,
CAST(lnk_f_nn.quantity AS int) as quantity,
lnk_f_nn.invoice_ts as invoice_ts,
CAST(lnk_f_nn.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_f_nn.customer_id AS int) as customer_id,
coalesce(lnk_f_nn.country, '') as country,
coalesce(lnk_f_nn.store_id, '') as store_id,
CAST(lnk_f_nn.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_f_nn.year_month, '') as year_month,
CAST(lnk_f_nn.customer_sk AS int) as customer_sk,
coalesce(lnk_f_nn.customer_type, '') as customer_type,
coalesce(lnk_f_nn.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_f_nn.category, '') as category,
CAST(lnk_f_nn.product_sk AS int) as product_sk,
coalesce(lnk_f_nn.region, '') as region,
CAST(lnk_f_nn.store_sk AS int) as store_sk,
CAST(lnk_f_nn.employee_sk AS int) as employee_sk,
coalesce(lnk_f_nn.currency, '') as currency,
CAST(lnk_f_nn.line_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_f_nn.quantity AS int) * CAST(lnk_f_nn.unit_cost AS decimal(18,2)) as cost_amount,
CAST(lnk_f_nn.line_amount AS decimal(18,2)) - ( CAST(lnk_f_nn.quantity AS int) * CAST(lnk_f_nn.unit_cost AS decimal(18,2)) ) as gross_profit,
CAST(lnk_f_nn.line_amount AS decimal(18,2)) * CAST(lnk_f_nn.fx_rate AS decimal(18,6)) as local_amount,
1 as line_cnt
FROM
lnk_f_nn""")
lnk_fact.createOrReplaceTempView("lnk_fact")


# COMMAND ----------
# Component lnk_sorted, Type SORTER Original node name srt_Fact_Sales, link lnk_sorted
lnk_sorted = spark.sql(f"""
SELECT
coalesce(lnk_fact.invoice_no, '') as invoice_no,
coalesce(lnk_fact.stock_code, '') as stock_code,
CAST(lnk_fact.quantity AS int) as quantity,
lnk_fact.invoice_ts as invoice_ts,
CAST(lnk_fact.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_fact.customer_id AS int) as customer_id,
coalesce(lnk_fact.country, '') as country,
coalesce(lnk_fact.store_id, '') as store_id,
CAST(lnk_fact.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_fact.year_month, '') as year_month,
CAST(lnk_fact.customer_sk AS int) as customer_sk,
coalesce(lnk_fact.customer_type, '') as customer_type,
coalesce(lnk_fact.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_fact.category, '') as category,
CAST(lnk_fact.product_sk AS int) as product_sk,
coalesce(lnk_fact.region, '') as region,
CAST(lnk_fact.store_sk AS int) as store_sk,
CAST(lnk_fact.employee_sk AS int) as employee_sk,
coalesce(lnk_fact.currency, '') as currency,
CAST(lnk_fact.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_fact.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_fact.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_fact.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_fact.line_cnt AS int) as line_cnt
FROM
lnk_fact
ORDER BY invoice_no asc, stock_code asc""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")


# COMMAND ----------
# Component lnk_audit_in, Type REPLICATE Original node name cpy_Fact, link lnk_audit_in
lnk_audit_in = spark.sql(f"""
SELECT
coalesce(lnk_sorted.invoice_no, '') as invoice_no,
coalesce(lnk_sorted.stock_code, '') as stock_code,
CAST(lnk_sorted.quantity AS int) as quantity,
lnk_sorted.invoice_ts as invoice_ts,
CAST(lnk_sorted.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_sorted.customer_id AS int) as customer_id,
coalesce(lnk_sorted.country, '') as country,
coalesce(lnk_sorted.store_id, '') as store_id,
CAST(lnk_sorted.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_sorted.year_month, '') as year_month,
CAST(lnk_sorted.customer_sk AS int) as customer_sk,
coalesce(lnk_sorted.customer_type, '') as customer_type,
coalesce(lnk_sorted.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_sorted.category, '') as category,
CAST(lnk_sorted.product_sk AS int) as product_sk,
coalesce(lnk_sorted.region, '') as region,
CAST(lnk_sorted.store_sk AS int) as store_sk,
CAST(lnk_sorted.employee_sk AS int) as employee_sk,
coalesce(lnk_sorted.currency, '') as currency,
CAST(lnk_sorted.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_sorted.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_sorted.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_sorted.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_sorted.line_cnt AS int) as line_cnt
FROM
lnk_sorted""")
lnk_audit_in.createOrReplaceTempView("lnk_audit_in")


# COMMAND ----------
# Component lnk_orph_in, Type REPLICATE Original node name cpy_Fact, link lnk_orph_in
lnk_orph_in = spark.sql(f"""
SELECT
coalesce(lnk_sorted.invoice_no, '') as invoice_no,
coalesce(lnk_sorted.stock_code, '') as stock_code,
CAST(lnk_sorted.quantity AS int) as quantity,
lnk_sorted.invoice_ts as invoice_ts,
CAST(lnk_sorted.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_sorted.customer_id AS int) as customer_id,
coalesce(lnk_sorted.country, '') as country,
coalesce(lnk_sorted.store_id, '') as store_id,
CAST(lnk_sorted.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_sorted.year_month, '') as year_month,
CAST(lnk_sorted.customer_sk AS int) as customer_sk,
coalesce(lnk_sorted.customer_type, '') as customer_type,
coalesce(lnk_sorted.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_sorted.category, '') as category,
CAST(lnk_sorted.product_sk AS int) as product_sk,
coalesce(lnk_sorted.region, '') as region,
CAST(lnk_sorted.store_sk AS int) as store_sk,
CAST(lnk_sorted.employee_sk AS int) as employee_sk,
coalesce(lnk_sorted.currency, '') as currency,
CAST(lnk_sorted.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_sorted.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_sorted.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_sorted.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_sorted.line_cnt AS int) as line_cnt
FROM
lnk_sorted""")
lnk_orph_in.createOrReplaceTempView("lnk_orph_in")


# COMMAND ----------
# Component lnk_fact_out, Type REPLICATE Original node name cpy_Fact, link lnk_fact_out
lnk_fact_out = spark.sql(f"""
SELECT
coalesce(lnk_sorted.invoice_no, '') as invoice_no,
coalesce(lnk_sorted.stock_code, '') as stock_code,
CAST(lnk_sorted.quantity AS int) as quantity,
lnk_sorted.invoice_ts as invoice_ts,
CAST(lnk_sorted.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_sorted.customer_id AS int) as customer_id,
coalesce(lnk_sorted.country, '') as country,
coalesce(lnk_sorted.store_id, '') as store_id,
CAST(lnk_sorted.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_sorted.year_month, '') as year_month,
CAST(lnk_sorted.customer_sk AS int) as customer_sk,
coalesce(lnk_sorted.customer_type, '') as customer_type,
coalesce(lnk_sorted.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_sorted.category, '') as category,
CAST(lnk_sorted.product_sk AS int) as product_sk,
coalesce(lnk_sorted.region, '') as region,
CAST(lnk_sorted.store_sk AS int) as store_sk,
CAST(lnk_sorted.employee_sk AS int) as employee_sk,
coalesce(lnk_sorted.currency, '') as currency,
CAST(lnk_sorted.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_sorted.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_sorted.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_sorted.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_sorted.line_cnt AS int) as line_cnt
FROM
lnk_sorted""")
lnk_fact_out.createOrReplaceTempView("lnk_fact_out")


# COMMAND ----------
# Component lnk_orphans, Type TRANSFORMATION Original node name flt_Orphan_Product, link lnk_orphans
lnk_orphans = spark.sql(f"""
SELECT
coalesce(lnk_orph_in.invoice_no, '') as invoice_no,
coalesce(lnk_orph_in.stock_code, '') as stock_code,
CAST(lnk_orph_in.quantity AS int) as quantity,
lnk_orph_in.invoice_ts as invoice_ts,
CAST(lnk_orph_in.unit_price AS decimal(18,2)) as unit_price,
CAST(lnk_orph_in.customer_id AS int) as customer_id,
coalesce(lnk_orph_in.country, '') as country,
coalesce(lnk_orph_in.store_id, '') as store_id,
CAST(lnk_orph_in.invoice_date_key AS int) as invoice_date_key,
coalesce(lnk_orph_in.year_month, '') as year_month,
CAST(lnk_orph_in.customer_sk AS int) as customer_sk,
coalesce(lnk_orph_in.customer_type, '') as customer_type,
coalesce(lnk_orph_in.loyalty_tier, '') as loyalty_tier,
coalesce(lnk_orph_in.category, '') as category,
CAST(lnk_orph_in.product_sk AS int) as product_sk,
coalesce(lnk_orph_in.region, '') as region,
CAST(lnk_orph_in.store_sk AS int) as store_sk,
CAST(lnk_orph_in.employee_sk AS int) as employee_sk,
coalesce(lnk_orph_in.currency, '') as currency,
CAST(lnk_orph_in.sales_amount AS decimal(18,2)) as sales_amount,
CAST(lnk_orph_in.cost_amount AS decimal(18,2)) as cost_amount,
CAST(lnk_orph_in.gross_profit AS decimal(18,2)) as gross_profit,
CAST(lnk_orph_in.local_amount AS decimal(18,2)) as local_amount,
CAST(lnk_orph_in.line_cnt AS int) as line_cnt
FROM
lnk_orph_in
WHERE product_sk = -1""")
lnk_orphans.createOrReplaceTempView("lnk_orphans")


# COMMAND ----------
# Component tgt_dq_orphan_products, Type TARGET 

tgt_dq_orphan_products = lnk_orphans.select( \
	coalesce(lnk_orphans.invoice_no, lit('')).alias('invoice_no'), \
	coalesce(lnk_orphans.stock_code, lit('')).alias('stock_code'), \
	lnk_orphans.quantity.cast('int').alias('quantity'), \
	lnk_orphans.invoice_ts.alias('invoice_ts'), \
	lnk_orphans.unit_price.cast('decimal(18,2)').alias('unit_price'), \
	lnk_orphans.customer_id.cast('int').alias('customer_id'), \
	coalesce(lnk_orphans.country, lit('')).alias('country'), \
	coalesce(lnk_orphans.store_id, lit('')).alias('store_id'), \
	lnk_orphans.invoice_date_key.cast('int').alias('invoice_date_key'), \
	coalesce(lnk_orphans.year_month, lit('')).alias('year_month'), \
	lnk_orphans.customer_sk.cast('int').alias('customer_sk'), \
	coalesce(lnk_orphans.customer_type, lit('')).alias('customer_type'), \
	coalesce(lnk_orphans.loyalty_tier, lit('')).alias('loyalty_tier'), \
	coalesce(lnk_orphans.category, lit('')).alias('category'), \
	lnk_orphans.product_sk.cast('int').alias('product_sk'), \
	coalesce(lnk_orphans.region, lit('')).alias('region'), \
	lnk_orphans.store_sk.cast('int').alias('store_sk'), \
	lnk_orphans.employee_sk.cast('int').alias('employee_sk'), \
	coalesce(lnk_orphans.currency, lit('')).alias('currency'), \
	lnk_orphans.sales_amount.cast('decimal(18,2)').alias('sales_amount'), \
	lnk_orphans.cost_amount.cast('decimal(18,2)').alias('cost_amount'), \
	lnk_orphans.gross_profit.cast('decimal(18,2)').alias('gross_profit'), \
	lnk_orphans.local_amount.cast('decimal(18,2)').alias('local_amount'), \
	lnk_orphans.line_cnt.cast('int').alias('line_cnt') \
)
tgt_dq_orphan_products.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.dq_orphan_products')


# COMMAND ----------
# Component tgt_fact_sales, Type TARGET 

tgt_fact_sales = lnk_fact_out.select( \
	coalesce(lnk_fact_out.invoice_no, lit('')).alias('invoice_no'), \
	coalesce(lnk_fact_out.stock_code, lit('')).alias('stock_code'), \
	lnk_fact_out.quantity.cast('int').alias('quantity'), \
	lnk_fact_out.invoice_ts.alias('invoice_ts'), \
	lnk_fact_out.unit_price.cast('decimal(18,2)').alias('unit_price'), \
	lnk_fact_out.customer_id.cast('int').alias('customer_id'), \
	coalesce(lnk_fact_out.country, lit('')).alias('country'), \
	coalesce(lnk_fact_out.store_id, lit('')).alias('store_id'), \
	lnk_fact_out.invoice_date_key.cast('int').alias('invoice_date_key'), \
	coalesce(lnk_fact_out.year_month, lit('')).alias('year_month'), \
	lnk_fact_out.customer_sk.cast('int').alias('customer_sk'), \
	coalesce(lnk_fact_out.customer_type, lit('')).alias('customer_type'), \
	coalesce(lnk_fact_out.loyalty_tier, lit('')).alias('loyalty_tier'), \
	coalesce(lnk_fact_out.category, lit('')).alias('category'), \
	lnk_fact_out.product_sk.cast('int').alias('product_sk'), \
	coalesce(lnk_fact_out.region, lit('')).alias('region'), \
	lnk_fact_out.store_sk.cast('int').alias('store_sk'), \
	lnk_fact_out.employee_sk.cast('int').alias('employee_sk'), \
	coalesce(lnk_fact_out.currency, lit('')).alias('currency'), \
	lnk_fact_out.sales_amount.cast('decimal(18,2)').alias('sales_amount'), \
	lnk_fact_out.cost_amount.cast('decimal(18,2)').alias('cost_amount'), \
	lnk_fact_out.gross_profit.cast('decimal(18,2)').alias('gross_profit'), \
	lnk_fact_out.local_amount.cast('decimal(18,2)').alias('local_amount'), \
	lnk_fact_out.line_cnt.cast('int').alias('line_cnt') \
)
tgt_fact_sales.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.fact_sales')


# COMMAND ----------
# Component lnk_by_month, Type AGGREGATOR Original node name agg_Fact_By_Month, link lnk_by_month
lnk_by_month = spark.sql(f"""
SELECT
coalesce(lnk_audit_in.year_month, '') as year_month,
SUM(sales_amount) as sales_amount,
SUM(gross_profit) as gross_profit
FROM
lnk_audit_in
GROUP BY
coalesce(lnk_audit_in.year_month, '')""")
lnk_by_month.createOrReplaceTempView("lnk_by_month")


# COMMAND ----------
# Component tgt_audit_fact_by_month, Type TARGET 

tgt_audit_fact_by_month = lnk_by_month.select( \
	coalesce(lnk_by_month.year_month, lit('')).alias('year_month'), \
	lnk_by_month.sales_amount.cast('decimal(18,2)').alias('sales_amount'), \
	lnk_by_month.gross_profit.cast('decimal(18,2)').alias('gross_profit') \
)
tgt_audit_fact_by_month.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.audit_fact_by_month')
