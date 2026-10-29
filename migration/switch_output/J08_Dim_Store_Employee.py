# Databricks notebook source
# MAGIC %md
# MAGIC # J08_Dim_Store_Employee
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC 
# MAGIC Source script: `/Volumes/unitygo/retail_dw/switch_volume/input-20261028062919-wlbd/J08_Dim_Store_Employee.txt`
# COMMAND ----------
# COMMAND ----------
# Converted from DataStage parallel job: J08_Dim_Store_Employee
# Purpose: Build Dim_Store and Dim_Employee with surrogate keys.
# Language: Python (the ${language} placeholder was not resolved, so I used Python).
#
# Notes on the conversion:
#  - The DataStage surrogate key expression
#      @PARTITIONNUM + (@NUMPARTITIONS * (@INROWNUM - 1)) + 1
#    produces a unique sequential number per row across partitions. Here it is
#    replaced by a deterministic row_number() over a stable ordering of the
#    business key, so keys are reproducible between runs.
#  - Sequential file targets (dim_store.csv, dim_employee.csv) become
#    materialized views in the pipeline's target catalog/schema.
#  - The pSrcDir, pMartDir, pAuditDir, pRunDate and pEffDate parameters are
#    declared for parity with the job but are not used by this job's logic.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------
# ---------------------------------------------------------------------------
# Job parameters (set via pipeline configuration; defaults match the DataStage job)
# ---------------------------------------------------------------------------
pSrcDir   = spark.conf.get("pSrcDir",   "C:/case_study/Source_Data")
pStgDir   = spark.conf.get("pStgDir",   "C:/case_study/Staging")
pDwhDir   = spark.conf.get("pDwhDir",   "C:/case_study/Warehouse")
pMartDir  = spark.conf.get("pMartDir",  "C:/case_study/Marts")
pAuditDir = spark.conf.get("pAuditDir", "C:/case_study/Audit")
pRunDate  = spark.conf.get("pRunDate",  "2011-12-31")
pEffDate  = spark.conf.get("pEffDate",  "2011-07-01")

# COMMAND ----------
# ---------------------------------------------------------------------------
# Schemas (from the sequential file column definitions)
# ---------------------------------------------------------------------------
store_schema = StructType([
    StructField("store_id",         StringType(),  False),
    StructField("store_name",       StringType(),  False),
    StructField("country",          StringType(),  False),
    StructField("channel",          StringType(),  False),
    StructField("rating",           IntegerType(), False),
    StructField("no_of_employees",  IntegerType(), False),
    StructField("region",           StringType(),  False),
    StructField("emp_cnt",          IntegerType(), False),
])

# COMMAND ----------
employee_schema = StructType([
    StructField("employee_id",    StringType(),  False),
    StructField("store_id",       StringType(),  False),
    StructField("employee_name",  StringType(),  False),
    StructField("role",           StringType(),  False),
    StructField("hire_date",      DateType(),    False),
    StructField("tenure_years",   IntegerType(), False),
])

# COMMAND ----------
def stg_stores():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(store_schema)
        .load(f"{pStgDir}/stg_stores.csv")
    )

# COMMAND ----------
def dim_store():
    # Sequential surrogate key starting at 1, ordered by the business key
    w = Window.orderBy("store_id")
    return (
        spark.read.table("stg_stores")
        .withColumn("store_sk", F.row_number().over(w).cast("int"))
        .select(
            "store_id", "store_name", "country", "channel", "rating",
            "no_of_employees", "region", "emp_cnt", "store_sk"
        )
    )

# COMMAND ----------
def store_keys():
    return spark.read.table("dim_store").select("store_id", "store_sk")

# COMMAND ----------
def stg_employees():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(employee_schema)
        .load(f"{pStgDir}/stg_employees.csv")
    )

# COMMAND ----------
def emp_store():
    emp = spark.read.table("stg_employees")
    keys = spark.read.table("store_keys")
    return emp.join(keys, on="store_id", how="inner")

# COMMAND ----------
def dim_employee():
    # Sequential surrogate key starting at 1, ordered by the business key
    w = Window.orderBy("employee_id")
    return (
        spark.read.table("emp_store")
        .withColumn("employee_sk", F.row_number().over(w).cast("int"))
        .select(
            "employee_id", "store_id", "employee_name", "role",
            "hire_date", "tenure_years", "store_sk", "employee_sk"
        )
    )
# COMMAND ----------
# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
