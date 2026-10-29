# Databricks notebook source
# MAGIC %md
# MAGIC # J13_Mart_RETAIL_DATA_MART
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028235854-szp7/J13_Mart_RETAIL_DATA_MART.txt`
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
# src_fact_sales: Delta table fact_sales (file in #pDwhDir# -> table)
lnk_fact = spark.table(f"{catalog}.{schema}.fact_sales")
lnk_fact.createOrReplaceTempView("lnk_fact")

# COMMAND ----------
# src_dim_customer: Delta table dim_customer (file in #pDwhDir# -> table)
lnk_dim_customer = spark.table(f"{catalog}.{schema}.dim_customer")
lnk_dim_customer.createOrReplaceTempView("lnk_dim_customer")

# COMMAND ----------
# jn_Customer: Join (inner) on customer_sk
lnk_c = spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.invoice_date_key,
  l.year_month,
  l.customer_sk,
  l.customer_type,
  l.loyalty_tier,
  l.category,
  l.product_sk,
  l.region,
  l.store_sk,
  l.employee_sk,
  l.currency,
  l.sales_amount,
  l.cost_amount,
  l.gross_profit,
  l.local_amount,
  l.line_cnt,
  r.cust_name,
  r.gender,
  r.age,
  r.spending_score,
  r.annual_income_k
FROM lnk_fact l
INNER JOIN lnk_dim_customer r ON l.customer_sk = r.customer_sk
""")
lnk_c.createOrReplaceTempView("lnk_c")

# COMMAND ----------
# src_dim_product: Delta table dim_product (file in #pDwhDir# -> table)
lnk_dim_product = spark.table(f"{catalog}.{schema}.dim_product")
lnk_dim_product.createOrReplaceTempView("lnk_dim_product")

# COMMAND ----------
# jn_Product: Join (inner) on product_sk
lnk_p = spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.invoice_date_key,
  l.year_month,
  l.customer_sk,
  l.customer_type,
  l.loyalty_tier,
  l.category,
  l.product_sk,
  l.region,
  l.store_sk,
  l.employee_sk,
  l.currency,
  l.sales_amount,
  l.cost_amount,
  l.gross_profit,
  l.local_amount,
  l.line_cnt,
  l.cust_name,
  l.gender,
  l.age,
  l.spending_score,
  l.annual_income_k,
  r.product_name,
  r.list_price
FROM lnk_c l
INNER JOIN lnk_dim_product r ON l.product_sk = r.product_sk
""")
lnk_p.createOrReplaceTempView("lnk_p")

# COMMAND ----------
# src_dim_store: Delta table dim_store (file in #pDwhDir# -> table)
lnk_dim_store = spark.table(f"{catalog}.{schema}.dim_store")
lnk_dim_store.createOrReplaceTempView("lnk_dim_store")

# COMMAND ----------
# jn_Store: Join (inner) on store_sk
lnk_s = spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.invoice_date_key,
  l.year_month,
  l.customer_sk,
  l.customer_type,
  l.loyalty_tier,
  l.category,
  l.product_sk,
  l.region,
  l.store_sk,
  l.employee_sk,
  l.currency,
  l.sales_amount,
  l.cost_amount,
  l.gross_profit,
  l.local_amount,
  l.line_cnt,
  l.cust_name,
  l.gender,
  l.age,
  l.spending_score,
  l.annual_income_k,
  l.product_name,
  l.list_price,
  r.store_name,
  r.rating,
  r.no_of_employees
FROM lnk_p l
INNER JOIN lnk_dim_store r ON l.store_sk = r.store_sk
""")
lnk_s.createOrReplaceTempView("lnk_s")

# COMMAND ----------
# flt_Customer_Types: Filter
lnk_types = spark.sql(f"""
SELECT * FROM lnk_s
WHERE customer_type = 'citizen' OR customer_type = 'foreign'
""")
lnk_types.createOrReplaceTempView("lnk_types")

# COMMAND ----------
# xfm_Upper_Names: Transformer
lnk_upper = spark.sql(f"""
SELECT
  invoice_no,
  stock_code,
  quantity,
  invoice_ts,
  unit_price,
  customer_id,
  country,
  store_id,
  invoice_date_key,
  year_month,
  customer_sk,
  customer_type,
  loyalty_tier,
  category,
  product_sk,
  region,
  store_sk,
  employee_sk,
  currency,
  sales_amount,
  cost_amount,
  gross_profit,
  local_amount,
  line_cnt,
  CAST(upper(cust_name) AS string) AS cust_name,
  gender,
  age,
  spending_score,
  annual_income_k,
  CAST(upper(product_name) AS string) AS product_name,
  list_price,
  store_name,
  rating,
  no_of_employees
FROM lnk_types
""")
lnk_upper.createOrReplaceTempView("lnk_upper")

# COMMAND ----------
# srt_Mart: Sort (invoice_no asc, stock_code asc)
lnk_sorted = spark.sql(f"""
SELECT * FROM lnk_upper
ORDER BY invoice_no ASC, stock_code ASC
""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")

# COMMAND ----------
# tgt_mart_retail_data_mart: write Delta table RETAIL_DATA_MART
(lnk_sorted.select('invoice_no', 'stock_code', 'quantity', 'invoice_ts', 'unit_price', 'customer_id', 'country',
                   'store_id', 'invoice_date_key', 'year_month', 'customer_sk', 'customer_type', 'loyalty_tier',
                   'category', 'product_sk', 'region', 'store_sk', 'employee_sk', 'currency', 'sales_amount',
                   'cost_amount', 'gross_profit', 'local_amount', 'line_cnt', 'cust_name', 'gender', 'age',
                   'spending_score', 'annual_income_k', 'product_name', 'list_price', 'store_name', 'rating',
                   'no_of_employees')
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.RETAIL_DATA_MART"))
print("RETAIL_DATA_MART:", spark.table(f"{catalog}.{schema}.RETAIL_DATA_MART").count(), "rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
