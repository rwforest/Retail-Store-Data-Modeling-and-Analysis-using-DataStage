# Databricks notebook source
# MAGIC %md
# MAGIC # J10_Fact_Sales
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261029064410-juws/J10_Fact_Sales.txt`
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
# src_stg_orders: Delta table stg_orders (file #pStgDir#/stg_orders.csv)
lnk_orders = spark.table(f"{catalog}.{schema}.stg_orders")
lnk_orders.createOrReplaceTempView("lnk_orders")

# COMMAND ----------
# flt_Sales_Lines: Filter
lnk_lines = spark.sql(f"""
SELECT * FROM lnk_orders
WHERE is_cancel = 0 AND is_nonproduct = 0 AND quantity > 0
""")
lnk_lines.createOrReplaceTempView("lnk_lines")

# COMMAND ----------
# xfm_Fact_Keys: Transformer
lnk_keyed = spark.sql(f"""
SELECT
  invoice_no,
  stock_code,
  description,
  quantity,
  invoice_ts,
  unit_price,
  customer_id,
  country,
  store_id,
  employee_id,
  invoice_date_key,
  line_amount,
  is_cancel,
  is_nonproduct,
  is_guest,
  CAST(date_format(invoice_ts, 'yyyyMM') AS string) AS year_month
FROM lnk_lines
""")
lnk_keyed.createOrReplaceTempView("lnk_keyed")

# COMMAND ----------
# src_dim_customer: Delta table dim_customer (file #pDwhDir#/dim_customer.csv)
lnk_dim_customer = spark.table(f"{catalog}.{schema}.dim_customer")
lnk_dim_customer.createOrReplaceTempView("lnk_dim_customer")

# COMMAND ----------
# jn_Customer: Join (left outer) on customer_id
lnk_c = spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.description,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.employee_id,
  l.invoice_date_key,
  l.line_amount,
  l.is_cancel,
  l.is_nonproduct,
  l.is_guest,
  l.year_month,
  r.customer_sk,
  r.customer_type,
  r.loyalty_tier,
  r.eff_start_key,
  r.eff_end_key
FROM lnk_keyed l
LEFT OUTER JOIN lnk_dim_customer r ON l.customer_id = r.customer_id
""")
lnk_c.createOrReplaceTempView("lnk_c")

# COMMAND ----------
# flt_PIT_Customer: Filter (point-in-time customer version)
lnk_c_pit = spark.sql(f"""
SELECT * FROM lnk_c
WHERE customer_sk IS NULL OR (eff_start_key <= invoice_date_key AND eff_end_key >= invoice_date_key)
""")
lnk_c_pit.createOrReplaceTempView("lnk_c_pit")

# COMMAND ----------
# xfm_Customer_Defaults: Transformer
lnk_c_nn = spark.sql(f"""
SELECT
  invoice_no,
  stock_code,
  description,
  quantity,
  invoice_ts,
  unit_price,
  customer_id,
  country,
  store_id,
  employee_id,
  invoice_date_key,
  line_amount,
  is_cancel,
  is_nonproduct,
  is_guest,
  year_month,
  CAST(coalesce(customer_sk, -1) AS int) AS customer_sk,
  CAST(coalesce(customer_type, 'unknown') AS string) AS customer_type,
  CAST(coalesce(loyalty_tier, 'NONE') AS string) AS loyalty_tier
FROM lnk_c_pit
""")
lnk_c_nn.createOrReplaceTempView("lnk_c_nn")

# COMMAND ----------
# src_dim_product: Delta table dim_product (file #pDwhDir#/dim_product.csv)
lnk_dim_product = spark.table(f"{catalog}.{schema}.dim_product")
lnk_dim_product.createOrReplaceTempView("lnk_dim_product")

# COMMAND ----------
# jn_Product: Join (left outer) on stock_code
lnk_p = spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.description,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.employee_id,
  l.invoice_date_key,
  l.line_amount,
  l.is_cancel,
  l.is_nonproduct,
  l.is_guest,
  l.year_month,
  l.customer_sk,
  l.customer_type,
  l.loyalty_tier,
  r.category,
  r.supplier_id,
  r.unit_cost,
  r.product_sk
FROM lnk_c_nn l
LEFT OUTER JOIN lnk_dim_product r ON l.stock_code = r.stock_code
""")
lnk_p.createOrReplaceTempView("lnk_p")

# COMMAND ----------
# xfm_Product_Defaults: Transformer
lnk_p_nn = spark.sql(f"""
SELECT
  invoice_no,
  stock_code,
  description,
  quantity,
  invoice_ts,
  unit_price,
  customer_id,
  country,
  store_id,
  employee_id,
  invoice_date_key,
  line_amount,
  is_cancel,
  is_nonproduct,
  is_guest,
  year_month,
  customer_sk,
  customer_type,
  loyalty_tier,
  CAST(coalesce(category, 'Unknown') AS string) AS category,
  CAST(coalesce(supplier_id, 'UNKNOWN') AS string) AS supplier_id,
  CAST(coalesce(unit_cost, 0) AS decimal(18,2)) AS unit_cost,
  CAST(coalesce(product_sk, -1) AS int) AS product_sk
FROM lnk_p
""")
lnk_p_nn.createOrReplaceTempView("lnk_p_nn")

# COMMAND ----------
# src_dim_store: Delta table dim_store (file #pDwhDir#/dim_store.csv)
lnk_dim_store = spark.table(f"{catalog}.{schema}.dim_store")
lnk_dim_store.createOrReplaceTempView("lnk_dim_store")

# COMMAND ----------
# jn_Store: Join (inner) on store_id
lnk_s = spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.description,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.employee_id,
  l.invoice_date_key,
  l.line_amount,
  l.is_cancel,
  l.is_nonproduct,
  l.is_guest,
  l.year_month,
  l.customer_sk,
  l.customer_type,
  l.loyalty_tier,
  l.category,
  l.supplier_id,
  l.unit_cost,
  l.product_sk,
  r.region,
  r.store_sk
FROM lnk_p_nn l
INNER JOIN lnk_dim_store r ON l.store_id = r.store_id
""")
lnk_s.createOrReplaceTempView("lnk_s")

# COMMAND ----------
# src_dim_employee: Delta table dim_employee (file #pDwhDir#/dim_employee.csv)
lnk_dim_emp = spark.table(f"{catalog}.{schema}.dim_employee")
lnk_dim_emp.createOrReplaceTempView("lnk_dim_emp")

# COMMAND ----------
# jn_Employee: Join (inner) on employee_id
lnk_e = spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.description,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.employee_id,
  l.invoice_date_key,
  l.line_amount,
  l.is_cancel,
  l.is_nonproduct,
  l.is_guest,
  l.year_month,
  l.customer_sk,
  l.customer_type,
  l.loyalty_tier,
  l.category,
  l.supplier_id,
  l.unit_cost,
  l.product_sk,
  l.region,
  l.store_sk,
  r.employee_sk
FROM lnk_s l
INNER JOIN lnk_dim_emp r ON l.employee_id = r.employee_id
""")
lnk_e.createOrReplaceTempView("lnk_e")

# COMMAND ----------
# src_dim_fx: Delta table dim_fx (file #pDwhDir#/dim_fx.csv)
lnk_dim_fx = spark.table(f"{catalog}.{schema}.dim_fx")
lnk_dim_fx.createOrReplaceTempView("lnk_dim_fx")

# COMMAND ----------
# jn_FX: Join (left outer) on country, year_month
lnk_f = spark.sql(f"""
SELECT
  l.invoice_no,
  l.stock_code,
  l.description,
  l.quantity,
  l.invoice_ts,
  l.unit_price,
  l.customer_id,
  l.country,
  l.store_id,
  l.employee_id,
  l.invoice_date_key,
  l.line_amount,
  l.is_cancel,
  l.is_nonproduct,
  l.is_guest,
  l.year_month,
  l.customer_sk,
  l.customer_type,
  l.loyalty_tier,
  l.category,
  l.supplier_id,
  l.unit_cost,
  l.product_sk,
  l.region,
  l.store_sk,
  l.employee_sk,
  r.currency,
  r.fx_rate
FROM lnk_e l
LEFT OUTER JOIN lnk_dim_fx r ON l.country = r.country AND l.year_month = r.year_month
""")
lnk_f.createOrReplaceTempView("lnk_f")

# COMMAND ----------
# xfm_FX_Defaults: Transformer
lnk_f_nn = spark.sql(f"""
SELECT
  invoice_no,
  stock_code,
  description,
  quantity,
  invoice_ts,
  unit_price,
  customer_id,
  country,
  store_id,
  employee_id,
  invoice_date_key,
  line_amount,
  is_cancel,
  is_nonproduct,
  is_guest,
  year_month,
  customer_sk,
  customer_type,
  loyalty_tier,
  category,
  supplier_id,
  unit_cost,
  product_sk,
  region,
  store_sk,
  employee_sk,
  CAST(coalesce(currency, 'GBP') AS string) AS currency,
  CAST(coalesce(fx_rate, 1) AS decimal(18,6)) AS fx_rate
FROM lnk_f
""")
lnk_f_nn.createOrReplaceTempView("lnk_f_nn")

# COMMAND ----------
# xfm_Fact_Measures: Transformer
lnk_fact = spark.sql(f"""
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
  CAST(line_amount AS decimal(18,2)) AS sales_amount,
  CAST(quantity * unit_cost AS decimal(18,2)) AS cost_amount,
  CAST(line_amount - (quantity * unit_cost) AS decimal(18,2)) AS gross_profit,
  CAST(line_amount * fx_rate AS decimal(18,2)) AS local_amount,
  CAST(1 AS int) AS line_cnt
FROM lnk_f_nn
""")
lnk_fact.createOrReplaceTempView("lnk_fact")

# COMMAND ----------
# srt_Fact_Sales: Sort (invoice_no asc, stock_code asc)
lnk_sorted = spark.sql(f"""
SELECT * FROM lnk_fact
ORDER BY invoice_no ASC, stock_code ASC
""")
lnk_sorted.createOrReplaceTempView("lnk_sorted")

# COMMAND ----------
# cpy_Fact: Copy
lnk_fact_out = spark.sql(f"""
SELECT * FROM lnk_sorted
""")
lnk_fact_out.createOrReplaceTempView("lnk_fact_out")

# COMMAND ----------
# cpy_Fact: Copy
lnk_orph_in = spark.sql(f"""
SELECT * FROM lnk_sorted
""")
lnk_orph_in.createOrReplaceTempView("lnk_orph_in")

# COMMAND ----------
# cpy_Fact: Copy
lnk_audit_in = spark.sql(f"""
SELECT * FROM lnk_sorted
""")
lnk_audit_in.createOrReplaceTempView("lnk_audit_in")

# COMMAND ----------
# tgt_fact_sales: write Delta table fact_sales
(lnk_fact_out.select('invoice_no', 'stock_code', 'quantity', 'invoice_ts', 'unit_price', 'customer_id', 'country', 'store_id',
                     'invoice_date_key', 'year_month', 'customer_sk', 'customer_type', 'loyalty_tier', 'category', 'product_sk',
                     'region', 'store_sk', 'employee_sk', 'currency', 'sales_amount', 'cost_amount', 'gross_profit',
                     'local_amount', 'line_cnt').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.fact_sales"))
print("fact_sales:", spark.table(f"{catalog}.{schema}.fact_sales").count(), "rows")

# COMMAND ----------
# flt_Orphan_Product: Filter
lnk_orphans = spark.sql(f"""
SELECT * FROM lnk_orph_in
WHERE product_sk = -1
""")
lnk_orphans.createOrReplaceTempView("lnk_orphans")

# COMMAND ----------
# tgt_dq_orphan_products: write Delta table dq_orphan_products
(lnk_orphans.select('invoice_no', 'stock_code', 'quantity', 'invoice_ts', 'unit_price', 'customer_id', 'country', 'store_id',
                    'invoice_date_key', 'year_month', 'customer_sk', 'customer_type', 'loyalty_tier', 'category', 'product_sk',
                    'region', 'store_sk', 'employee_sk', 'currency', 'sales_amount', 'cost_amount', 'gross_profit',
                    'local_amount', 'line_cnt').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dq_orphan_products"))
print("dq_orphan_products:", spark.table(f"{catalog}.{schema}.dq_orphan_products").count(), "rows")

# COMMAND ----------
# agg_Fact_By_Month: Aggregator
lnk_by_month = spark.sql(f"""
SELECT year_month,
  CAST(SUM(sales_amount) AS decimal(18,2)) AS sales_amount,
  CAST(SUM(gross_profit) AS decimal(18,2)) AS gross_profit
FROM lnk_audit_in
GROUP BY year_month
""")
lnk_by_month.createOrReplaceTempView("lnk_by_month")

# COMMAND ----------
# tgt_audit_fact_by_month: write Delta table audit_fact_by_month
(lnk_by_month.select('year_month', 'sales_amount', 'gross_profit').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.audit_fact_by_month"))
print("audit_fact_by_month:", spark.table(f"{catalog}.{schema}.audit_fact_by_month").count(), "rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
