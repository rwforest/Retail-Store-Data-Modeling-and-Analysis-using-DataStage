# Databricks notebook source
# act_Prepare_Dirs + wff_Source_Orders: create the schema, then wait for the source files to land.

# COMMAND ----------
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "retail_dw")
dbutils.widgets.text("src_dir", "")
catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))

import os, time

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
needed = ["orders_raw.csv", "customers_snapshot_2010.csv", "customers_snapshot_2011.csv", "products_catalog.csv",
          "suppliers.csv", "stores.csv", "employees.csv", "calendar.csv", "promotions.csv", "fx_rates.csv",
          "country_currency.csv"]
deadline = time.time() + 600
while (missing := [f for f in needed if not os.path.exists(f"{src_dir}/{f}")]) and time.time() < deadline:
    print("waiting for", missing)
    time.sleep(15)
assert not missing, f"source files not found in {src_dir}: {missing}"
print("sources present in", src_dir)
