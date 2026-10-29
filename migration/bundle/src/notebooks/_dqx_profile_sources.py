# Databricks notebook source
# Pre-migration analysis: profile the DataStage source files with DQX and store the generated rule candidates
# plus summary statistics. Candidates are a starting point for review, not applied as-is (see _dqx_validate).
# Requires databricks-labs-dqx (job environment `dqx`).

# COMMAND ----------
import json, yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "retail_dw")
dbutils.widgets.text("src_dir", "")
catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))

ws = WorkspaceClient()
profiler, generator = DQProfiler(ws), DQGenerator(ws)
SOURCES = ["orders_raw", "customers_snapshot_2010", "customers_snapshot_2011", "products_catalog", "suppliers",
           "stores", "employees", "calendar", "promotions", "fx_rates", "country_currency"]

# COMMAND ----------
rows, candidates = [], {}
for name in SOURCES:
    df = spark.read.option("header", "true").option("inferSchema", "true").option("escape", '"').csv(f"{src_dir}/{name}.csv")
    stats, profiles = profiler.profile(df, options={"criticality": "warn"})
    candidates[name] = generator.generate_dq_rules(profiles)
    rows.append((name, df.count(), len(df.columns), len(candidates[name]), json.dumps(stats, default=str)))
    print(f"{name}: {rows[-1][1]} rows, {rows[-1][2]} cols, {rows[-1][3]} candidate rules")

spark.createDataFrame(rows, "source string, row_count long, column_count int, candidate_rules int, summary_stats string") \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dq_source_profile")
with open(f"{src_dir}/dqx_source_candidates.yml", "w") as f:
    yaml.safe_dump(candidates, f, sort_keys=True)
display(spark.table(f"{catalog}.{schema}.dq_source_profile").drop("summary_stats"))
