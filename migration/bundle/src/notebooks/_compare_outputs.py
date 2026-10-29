# Databricks notebook source
# DQX reconciliation of the two migrated pipeline runs: Switch output (checked) vs compiled output (reference).
#
#   1. Generate one DQX rule set per output table (metadata), save it to DQX checks storage
#      (<out_schema>.dq_compare_checks, run_config_name = table).
#   2. Run every table through DQEngine.apply_checks_and_save_in_tables with one RunConfig per table, metrics only
#      (summary metrics -> <out_schema>.dq_compare_metrics).
#   3. Report from the DQX metrics table (per-check error/warning counts).
#   4. Re-run only the tables that differ with a quarantine config -> <out_schema>.<table>_dq_quarantine.
#
# Rules per table (reference = the compiled table):
#   error  schema_matches       has_valid_schema(strict)              same columns, order and types
#   error  rows_match           compare_datasets(natural key, missing records, ignoring input-order surrogate keys)
#   warn   surrogate_keys_match compare_datasets(natural key) incl. store_sk / employee_sk / promo_sk, which the
#                               DataStage jobs number in input order - a difference here alone is not a defect
# Requires databricks-labs-dqx (job environment `dqx`).

# COMMAND ----------
import json
from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.config import InputConfig, OutputConfig, RunConfig, TableChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("left_schema", "retail_dw_compiled")   # reference
dbutils.widgets.text("right_schema", "retail_dw_switch")    # checked
dbutils.widgets.text("out_schema", "retail_dw_compare")
catalog, ref, chk, out = (dbutils.widgets.get(k) for k in ("catalog", "left_schema", "right_schema", "out_schema"))
fq = lambda s, t: f"{catalog}.{s}.{t}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{out}")

CHECKS_TABLE, METRICS_TABLE = fq(out, "dq_compare_checks"), fq(out, "dq_compare_metrics")
RUN_NAME = f"{chk}_vs_{ref}"

# Natural key (grain) of each output table; tables not listed are matched on all their columns.
KEYS = {
    "stg_orders": ["invoice_no", "stock_code", "invoice_ts", "customer_id", "quantity", "unit_price"],
    "stg_customers_2010": ["customer_id"], "stg_customers_2011": ["customer_id"], "stg_products": ["stock_code"],
    "stg_active_products": ["stock_code"], "stg_stores": ["store_id"], "stg_employees": ["employee_id"],
    "dim_customer": ["customer_sk"], "dim_customer_next": ["customer_sk"], "dim_product": ["stock_code"],
    "dim_store": ["store_id"], "dim_employee": ["employee_id"], "dim_date": ["date_key"], "dim_promotion": ["promo_id"],
    "dim_fx": ["country", "year_month"], "fact_invoice": ["invoice_no"], "customer_360": ["customer_id"],
    "product_performance": ["stock_code"], "product_top50": ["stock_code"], "store_monthly": ["store_id", "year_month"],
    "promo_effectiveness": ["promo_id"], "q1_employee_transactions": ["employee_id"], "q1_top10_employees": ["employee_id"],
    "q2_customertype_profit": ["customer_type"], "q2_winning_customertype": ["customer_type"],
    "q3_bonus_program": ["customer_id"], "customer_segments": ["segment"], "basket_affinity": ["stock_code", "stock_code_b"],
}
INPUT_ORDER_SKS = {"store_sk", "employee_sk", "promo_sk"}
SKIP = ("dq_", "lakebridge_")


def tables(schema):
    return {r.tableName for r in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
            if not r.tableName.lower().startswith(SKIP)}


ref_tables, chk_tables = tables(ref), tables(chk)
missing = sorted(ref_tables ^ chk_tables)
common = sorted(ref_tables & chk_tables)
print(f"{len(common)} tables to reconcile; only on one side: {missing or 'none'}")

# COMMAND ----------
# 1. Rule sets -> DQX checks storage (one run_config_name per table)
from concurrent.futures import ThreadPoolExecutor

PARALLELISM = 12
dq = DQEngine(WorkspaceClient(), spark=spark)


def rule_set(t):
    ref_fq, cols = fq(ref, t), spark.table(fq(ref, t)).columns
    unique_key = t.lower() in KEYS  # listed keys are the table's grain: one row per key
    keys = [k for k in KEYS.get(t.lower(), cols) if k in cols]
    order_sks = [c for c in cols if c in INPUT_ORDER_SKS and c not in keys]
    compare = {"columns": keys, "ref_columns": keys, "ref_table": ref_fq, "check_missing_records": True,
               # unique keys: single join instead of duplicate-safe pairing (raises if the grain is violated)
               "raise_on_duplicate_keys": unique_key}
    rules = [
        {"name": "schema_matches", "criticality": "error", "user_metadata": {"check_type": "schema", "reference": ref_fq},
         "check": {"function": "has_valid_schema", "arguments": {"ref_table": ref_fq, "strict": True}}},
        {"name": "rows_match", "criticality": "error", "user_metadata": {"check_type": "data", "reference": ref_fq},
         "check": {"function": "compare_datasets", "arguments": dict(compare, exclude_columns=order_sks)}},
    ]
    if order_sks:
        rules.append(
            {"name": "surrogate_keys_match", "criticality": "warn",
             "user_metadata": {"check_type": "input_order_surrogate_keys", "columns": ",".join(order_sks)},
             "check": {"function": "compare_datasets", "arguments": compare}})
    return rules


def save(t):
    rules = rule_set(t)
    status = DQEngine.validate_checks(rules)
    assert not status.has_errors, f"{t}: {status}"
    # DQX skips the write when the rule-set fingerprint for this run_config_name is unchanged
    dq.save_checks(rules, config=TableChecksStorageConfig(location=CHECKS_TABLE, run_config_name=t, mode="overwrite"))


with ThreadPoolExecutor(PARALLELISM) as pool:
    list(pool.map(save, common))
print("rule sets saved to", CHECKS_TABLE)

# drop quarantine tables left by earlier runs; this run recreates only the ones that differ
stale = [r.tableName for r in spark.sql(f"SHOW TABLES IN {catalog}.{out}").collect() if r.tableName.endswith("_dq_quarantine")]
with ThreadPoolExecutor(PARALLELISM) as pool:
    list(pool.map(lambda t: spark.sql(f"DROP TABLE IF EXISTS {catalog}.{out}.{t}"), stale))

# COMMAND ----------
# 2. Apply, metrics only: one RunConfig per table, all tables in parallel
observer = DQMetricsObserver(name=RUN_NAME)
dq_run = DQEngine(WorkspaceClient(), spark=spark, observer=observer)
run_configs = [RunConfig(name=t, input_config=InputConfig(location=fq(chk, t)), checks_location=CHECKS_TABLE,
                         metrics_config=OutputConfig(location=METRICS_TABLE, mode="append")) for t in common]
# the first run creates the metrics table if needed, so the parallel appends never race on CREATE TABLE
dq_run.apply_checks_and_save_in_tables(run_configs[:1], max_parallelism=1)
dq_run.apply_checks_and_save_in_tables(run_configs[1:], max_parallelism=PARALLELISM)

# COMMAND ----------
# 3. Report from the DQX summary metrics of this run
m = spark.table(METRICS_TABLE).filter(F.col("run_id") == observer.id)
wide = (m.groupBy("input_location").pivot("metric_name", ["input_row_count", "error_row_count", "warning_row_count",
                                                          "check_metrics"]).agg(F.first("metric_value")))
per_check = []
for r in wide.collect():
    for c in json.loads(r["check_metrics"] or "[]"):
        per_check.append((r["input_location"].split(".")[-1], c["check_name"], int(c.get("error_count") or 0),
                          int(c.get("warning_count") or 0), int(r["input_row_count"] or 0)))
pc = spark.createDataFrame(per_check, "table_name string, check_name string, error_rows long, warning_rows long, input_rows long")
display(pc.orderBy(F.desc("error_rows"), F.desc("warning_rows"), "table_name"))

failing = pc.filter("error_rows > 0").select("table_name").distinct().count()
sk_only = (pc.groupBy("table_name").agg(F.sum("error_rows").alias("e"), F.sum("warning_rows").alias("w"))
           .filter("e = 0 AND w > 0").count())
# COMMAND ----------
# 4. Quarantine pass: only tables with a difference get <table>_dq_quarantine (same checks, no metrics)
differing = [r.table_name for r in pc.groupBy("table_name").agg(F.sum("error_rows").alias("e"), F.sum("warning_rows").alias("w"))
             .filter("e > 0 OR w > 0").collect()]
if differing:
    by_name = {t.lower(): t for t in common}
    DQEngine(WorkspaceClient(), spark=spark).apply_checks_and_save_in_tables(
        [RunConfig(name=by_name[t.lower()], input_config=InputConfig(location=fq(chk, by_name[t.lower()])),
                   checks_location=CHECKS_TABLE,
                   quarantine_config=OutputConfig(location=fq(out, f"{by_name[t.lower()]}_dq_quarantine"), mode="overwrite"))
         for t in differing], max_parallelism=PARALLELISM)
print("quarantined:", differing or "none")

summary = {"run_id": observer.id, "tables": len(common), "tables_with_errors": failing,
           "tables_differing_only_in_input_order_keys": sk_only, "tables_only_on_one_side": missing}
print("SUMMARY:", summary)
dbutils.notebook.exit(json.dumps(summary))
