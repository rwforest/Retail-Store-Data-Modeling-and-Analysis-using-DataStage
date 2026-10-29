# Databricks notebook source
# MAGIC %md
# MAGIC # J13_Mart_RETAIL_DATA_MART
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261029064410-juws/J13_Mart_RETAIL_DATA_MART.txt`
# COMMAND ----------
# COMMAND ----------
dbutils.widgets.text("catalog", ""); dbutils.widgets.text("schema", "retail_dw"); dbutils.widgets.text("src_dir", "")
dbutils.widgets.text("pRunDate", "2011-12-31"); dbutils.widgets.text("pEffDate", "2011-07-01")
catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))
pRunDate, pEffDate = dbutils.widgets.get("pRunDate"), dbutils.widgets.get("pEffDate")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------
# src_fact_sales: Sequential File source (file from the warehouse directory -> Delta table fact_sales)
lnk_fact = spark.table(f"{catalog}.{schema}.fact_sales")
lnk_fact.createOrReplaceTempView("lnk_fact")

# COMMAND ----------
# src_dim_customer: Sequential File source (file from the warehouse directory -> Delta table dim_customer)
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
# src_dim_product: Sequential File source (file from the warehouse directory -> Delta table dim_product)
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
# src_dim_store: Sequential File source (file from the warehouse directory -> Delta table dim_store)
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
  CAST(invoice_no AS string) AS invoice_no,
  CAST(stock_code AS string) AS stock_code,
  CAST(quantity AS int) AS quantity,
  CAST(invoice_ts AS timestamp) AS invoice_ts,
  CAST(unit_price AS decimal(18,2)) AS unit_price,
  CAST(customer_id AS int) AS customer_id,
  CAST(country AS string) AS country,
  CAST(store_id AS string) AS store_id,
  CAST(invoice_date_key AS int) AS invoice_date_key,
  CAST(year_month AS string) AS year_month,
  CAST(customer_sk AS int) AS customer_sk,
  CAST(customer_type AS string) AS customer_type,
  CAST(loyalty_tier AS string) AS loyalty_tier,
  CAST(category AS string) AS category,
  CAST(product_sk AS int) AS product_sk,
  CAST(region AS string) AS region,
  CAST(store_sk AS int) AS store_sk,
  CAST(employee_sk AS int) AS employee_sk,
  CAST(currency AS string) AS currency,
  CAST(sales_amount AS decimal(18,2)) AS sales_amount,
  CAST(cost_amount AS decimal(18,2)) AS cost_amount,
  CAST(gross_profit AS decimal(18,2)) AS gross_profit,
  CAST(local_amount AS decimal(18,2)) AS local_amount,
  CAST(line_cnt AS int) AS line_cnt,
  CAST(upper(cust_name) AS string) AS cust_name,
  CAST(gender AS string) AS gender,
  CAST(age AS int) AS age,
  CAST(spending_score AS int) AS spending_score,
  CAST(annual_income_k AS int) AS annual_income_k,
  CAST(upper(product_name) AS string) AS product_name,
  CAST(list_price AS decimal(18,2)) AS list_price,
  CAST(store_name AS string) AS store_name,
  CAST(rating AS int) AS rating,
  CAST(no_of_employees AS int) AS no_of_employees
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
    'store_id', 'invoice_date_key', 'year_month', 'customer_sk', 'customer_type', 'loyalty_tier', 'category',
    'product_sk', 'region', 'store_sk', 'employee_sk', 'currency', 'sales_amount', 'cost_amount', 'gross_profit',
    'local_amount', 'line_cnt', 'cust_name', 'gender', 'age', 'spending_score', 'annual_income_k', 'product_name',
    'list_price', 'store_name', 'rating', 'no_of_employees')
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.RETAIL_DATA_MART"))
print("RETAIL_DATA_MART:", spark.table(f"{catalog}.{schema}.RETAIL_DATA_MART").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
