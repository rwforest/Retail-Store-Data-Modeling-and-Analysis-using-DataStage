# Databricks notebook source
# MAGIC %md
# MAGIC # J11_Fact_Returns
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028235854-szp7/J11_Fact_Returns.txt`
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
# src_stg_orders: Sequential File source (#pStgDir#/stg_orders.csv -> Delta table stg_orders)
lnk_orders = spark.table(f"{catalog}.{schema}.stg_orders")
lnk_orders.createOrReplaceTempView("lnk_orders")

# COMMAND ----------
# flt_Cancels: Filter
lnk_cancels = spark.sql(f"""
SELECT * FROM lnk_orders
WHERE is_cancel = 1 AND is_nonproduct = 0
""")
lnk_cancels.createOrReplaceTempView("lnk_cancels")

# COMMAND ----------
# xfm_Return_Measures: Transformer
lnk_ret = spark.sql(f"""
SELECT
  invoice_no,
  stock_code,
  customer_id,
  country,
  invoice_date_key,
  CAST(date_format(invoice_ts, 'yyyyMM') AS string) AS year_month,
  CAST(abs(quantity) AS int) AS returned_qty,
  CAST(abs(line_amount) AS decimal(18,2)) AS returned_amount
FROM lnk_cancels
""")
lnk_ret.createOrReplaceTempView("lnk_ret")

# COMMAND ----------
# src_dim_product: Sequential File source (#pDwhDir#/dim_product.csv -> Delta table dim_product)
lnk_dim_product = spark.table(f"{catalog}.{schema}.dim_product")
lnk_dim_product.createOrReplaceTempView("lnk_dim_product")

# COMMAND ----------
# jn_Product: Join (left outer) on stock_code
lnk_p = spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.customer_id,
  l.country,
  l.invoice_date_key,
  l.year_month,
  l.returned_qty,
  l.returned_amount,
  r.category,
  r.product_sk
FROM lnk_ret l
LEFT OUTER JOIN lnk_dim_product r ON l.stock_code = r.stock_code
""")
lnk_p.createOrReplaceTempView("lnk_p")

# COMMAND ----------
# xfm_Product_Defaults: Transformer
lnk_p_nn = spark.sql(f"""
SELECT
  invoice_no,
  stock_code,
  customer_id,
  country,
  invoice_date_key,
  year_month,
  returned_qty,
  returned_amount,
  CAST(coalesce(category, 'Unknown') AS string) AS category,
  CAST(coalesce(product_sk, -1) AS int) AS product_sk
FROM lnk_p
""")
lnk_p_nn.createOrReplaceTempView("lnk_p_nn")

# COMMAND ----------
# src_dim_customer: Sequential File source (#pDwhDir#/dim_customer.csv -> Delta table dim_customer)
lnk_dim_customer = spark.table(f"{catalog}.{schema}.dim_customer")
lnk_dim_customer.createOrReplaceTempView("lnk_dim_customer")

# COMMAND ----------
# flt_Current_Customer: Filter
lnk_cur = spark.sql(f"""
SELECT * FROM lnk_dim_customer
WHERE is_current = 1
""")
lnk_cur.createOrReplaceTempView("lnk_cur")

# COMMAND ----------
# jn_Customer: Join (left outer) on customer_id
lnk_c = spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.customer_id,
  l.country,
  l.invoice_date_key,
  l.year_month,
  l.returned_qty,
  l.returned_amount,
  l.category,
  l.product_sk,
  r.customer_sk
FROM lnk_p_nn l
LEFT OUTER JOIN lnk_cur r ON l.customer_id = r.customer_id
""")
lnk_c.createOrReplaceTempView("lnk_c")

# COMMAND ----------
# xfm_Customer_Defaults: Transformer
lnk_c_nn = spark.sql(f"""
SELECT
  invoice_no,
  stock_code,
  customer_id,
  country,
  invoice_date_key,
  year_month,
  returned_qty,
  returned_amount,
  category,
  product_sk,
  CAST(coalesce(customer_sk, -1) AS int) AS customer_sk
FROM lnk_c
""")
lnk_c_nn.createOrReplaceTempView("lnk_c_nn")

# COMMAND ----------
# srt_Fact_Returns: Sort (invoice_no, stock_code ascending)
lnk_sorted = spark.sql(f"""
SELECT * FROM lnk_c_nn
ORDER BY invoice_no, stock_code
""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")

# COMMAND ----------
# cpy_Returns: Copy (output link lnk_out)
lnk_out = spark.sql(f"""
SELECT * FROM lnk_sorted
""")
lnk_out.createOrReplaceTempView("lnk_out")

# COMMAND ----------
# cpy_Returns: Copy (output link lnk_audit_in)
lnk_audit_in = spark.sql(f"""
SELECT * FROM lnk_sorted
""")
lnk_audit_in.createOrReplaceTempView("lnk_audit_in")

# COMMAND ----------
# tgt_fact_returns: Sequential File target (#pDwhDir#/fact_returns.csv -> Delta table fact_returns)
(lnk_out.select('invoice_no', 'stock_code', 'customer_id', 'country', 'invoice_date_key', 'year_month',
                'returned_qty', 'returned_amount', 'category', 'product_sk', 'customer_sk')
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.fact_returns"))
print("fact_returns:", spark.table(f"{catalog}.{schema}.fact_returns").count(), "rows")

# COMMAND ----------
# agg_Returns_By_Category: Aggregator
lnk_by_cat = spark.sql(f"""
SELECT category, CAST(SUM(returned_amount) AS decimal(18,2)) AS returned_amount
FROM lnk_audit_in
GROUP BY category
""")
lnk_by_cat.createOrReplaceTempView("lnk_by_cat")

# COMMAND ----------
# tgt_audit_returns_by_category: Sequential File target (#pAuditDir#/audit_returns_by_category.csv -> Delta table audit_returns_by_category)
(lnk_by_cat.select('category', 'returned_amount')
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.audit_returns_by_category"))
print("audit_returns_by_category:", spark.table(f"{catalog}.{schema}.audit_returns_by_category").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
