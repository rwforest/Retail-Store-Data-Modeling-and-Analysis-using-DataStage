# Databricks notebook source
# MAGIC %md
# MAGIC # J04_Land_Stores_Employees
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J04_Land_Stores_Employees.txt`
# COMMAND ----------
# COMMAND ----------
# Lakeflow Spark Declarative Pipeline (Python) converted from DataStage job J04_Land_Stores_Employees.
# The prompt's ${language} placeholder was not filled in, so I assumed Python.
# Purpose: land stores and employees, derive region and per-store headcount.
#
# Flow:
#   src_stores    -> xfm_Store_Region ----------------------------┐
#   src_employees -> xfm_Emp_Std -> cpy_Emp -> stg_employees      ├-> jn_Store_EmpCount -> stg_stores
#                                         └-> agg_Emp_By_Store ---┘

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------
# ---------------------------------------------------------------------------
# Job parameters. Set them as pipeline configuration values; the defaults
# match the DataStage job defaults.
# ---------------------------------------------------------------------------
pSrcDir = spark.conf.get("pSrcDir", "C:/case_study/Source_Data")   # Source files directory
pStgDir = spark.conf.get("pStgDir", "C:/case_study/Staging")       # Staging directory (the tables below replace the staging CSVs)
pRunDate = spark.conf.get("pRunDate", "2011-12-31")                # Business run date (yyyy-mm-dd)

# COMMAND ----------
# ---------------------------------------------------------------------------
# Explicit source schemas (from the PxSequentialFile column definitions)
# ---------------------------------------------------------------------------
stores_schema = StructType([
    StructField("StoreID", StringType(), False),
    StructField("StoreName", StringType(), False),
    StructField("Country", StringType(), False),
    StructField("Channel", StringType(), False),
    StructField("Rating", IntegerType(), False),
    StructField("NoOfEmployees", IntegerType(), False),
])

# COMMAND ----------
employees_schema = StructType([
    StructField("EmployeeID", StringType(), False),
    StructField("StoreID", StringType(), False),
    StructField("EmployeeName", StringType(), False),
    StructField("Role", StringType(), False),
    StructField("HireDate", StringType(), False),   # read as string, parsed in the transform
])


# COMMAND ----------
def src_stores():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(stores_schema)
        .load(f"{pSrcDir}/stores.csv")
    )


# COMMAND ----------
def src_employees():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(employees_schema)
        .load(f"{pSrcDir}/employees.csv")
    )


# COMMAND ----------
def std_store():
    s = spark.read.table("src_stores")
    country = F.trim(F.col("Country"))
    channel = F.trim(F.col("Channel"))
    return s.select(
        F.trim(F.col("StoreID")).alias("store_id"),
        F.upper(F.trim(F.col("StoreName"))).alias("store_name"),
        country.alias("country"),
        channel.alias("channel"),
        F.col("Rating").alias("rating"),
        F.col("NoOfEmployees").alias("no_of_employees"),
        # Region: UK -> "UK", EIRE -> "UK&I", ONLINE channel -> "ONLINE", otherwise "EUROPE_ROW"
        F.when(country == "United Kingdom", F.lit("UK"))
        .when(country == "EIRE", F.lit("UK&I"))
        .when(channel == "ONLINE", F.lit("ONLINE"))
        .otherwise(F.lit("EUROPE_ROW"))
        .alias("region"),
    )


# COMMAND ----------
def std_emp():
    e = spark.read.table("src_employees")
    hire_date = F.to_date(F.col("HireDate"), "yyyy-MM-dd")
    run_date = F.to_date(F.lit(pRunDate), "yyyy-MM-dd")
    return e.select(
        F.trim(F.col("EmployeeID")).alias("employee_id"),
        F.trim(F.col("StoreID")).alias("store_id"),
        F.upper(F.trim(F.col("EmployeeName"))).alias("employee_name"),
        F.upper(F.trim(F.col("Role"))).alias("role"),
        hire_date.alias("hire_date"),
        # Whole years: days between hire date and run date / 365, truncated to an integer
        (F.datediff(run_date, hire_date) / F.lit(365)).cast("int").alias("tenure_years"),
    )


# COMMAND ----------
def stg_employees():
    return spark.read.table("std_emp").select(
        "employee_id", "store_id", "employee_name", "role", "hire_date", "tenure_years"
    )


# COMMAND ----------
def emp_by_store():
    return (
        spark.read.table("std_emp")
        .groupBy("store_id")
        .agg(F.count(F.lit(1)).cast("int").alias("emp_cnt"))
    )


# COMMAND ----------
def stg_stores():
    st = spark.read.table("std_store").alias("s")
    ec = spark.read.table("emp_by_store").alias("e")
    return (
        st.join(ec, on="store_id", how="inner")
        .select(
            "store_id",
            F.col("s.store_name"),
            F.col("s.country"),
            F.col("s.channel"),
            F.col("s.rating"),
            F.col("s.no_of_employees"),
            F.col("s.region"),
            F.col("e.emp_cnt"),
        )
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
