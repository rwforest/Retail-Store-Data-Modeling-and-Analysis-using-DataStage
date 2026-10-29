# Databricks notebook source
# Requires databricks-labs-dqx (installed by the job environment `dqx`).
# DQX validation of the migrated retail DW. Runs after all 23 jobs (seq_All_Done) and fails the run
# if any error-criticality rule is violated. Per-rule failure counts land in <schema>.dq_validation_results,
# failing rows (with _errors/_warnings) in <schema>.dq_quarantine_<table>.

# COMMAND ----------
import yaml
from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "retail_dw")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
fq = lambda t: f"{catalog}.{schema}.{t}"
dq = DQEngine(WorkspaceClient(), spark=spark)


def not_null(*cols, crit="error"):
    return [{"criticality": crit, "check": {"function": "is_not_null", "for_each_column": list(cols)}}]


def unique(*cols, crit="error"):
    return [{"criticality": crit, "name": "unique_" + "_".join(cols),
             "check": {"function": "is_unique", "arguments": {"columns": list(cols)}}}]


def expr(name, expression, crit="error", flt=None):
    c = {"criticality": crit, "name": name, "check": {"function": "sql_expression",
                                                       "arguments": {"expression": expression, "msg": name}}}
    if flt:
        c["filter"] = flt
    return [c]


def in_list(col, allowed, crit="error"):
    # DQX >= 0.16 reads a bare string in `allowed` as a column expression: quote string literals
    allowed = [f"'{v}'" if isinstance(v, str) else v for v in allowed]
    return [{"criticality": crit, "check": {"function": "is_in_list",
                                            "arguments": {"column": col, "allowed": allowed}}}]


def fk(col, ref, ref_col, crit="error", flt=None):
    c = {"criticality": crit, "name": f"fk_{col}_{ref}",
         "check": {"function": "foreign_key", "arguments": {"columns": [col], "ref_columns": [ref_col],
                                                            "ref_table": fq(ref)}}}
    if flt:
        c["filter"] = flt
    return [c]


# Business rules reconstructed from the DataStage jobs (J01-J23); see migration/README.md for the mapping.
CHECKS = {
    "stg_orders": (not_null("invoice_no", "stock_code", "invoice_ts", "customer_id", "country")
                   + expr("quantity_not_zero", "quantity <> 0") + expr("unit_price_not_negative", "unit_price >= 0")
                   + in_list("is_cancel", [0, 1]) + in_list("is_guest", [0, 1])
                   + expr("cancel_flag_matches_invoice_prefix", "is_cancel = CASE WHEN left(invoice_no,1)='C' THEN 1 ELSE 0 END")),
    "stg_products": not_null("stock_code", "product_name", "category") + unique("stock_code"),
    "stg_stores": not_null("store_id", "store_name") + unique("store_id")
                  + in_list("region", ["UK", "UK&I", "ONLINE", "EUROPE_ROW"]),
    "stg_employees": not_null("employee_id", "store_id") + unique("employee_id"),
    "dim_customer": (not_null("customer_sk", "customer_id", "version_no", "eff_start_key", "eff_end_key")
                     + unique("customer_sk") + unique("customer_id", "version_no") + in_list("is_current", [0, 1])
                     + expr("effective_range_ordered", "eff_start_key <= eff_end_key")
                     + expr("current_row_is_open_ended", "is_current = 0 OR eff_end_key = 99991231")),
    "dim_product": not_null("product_sk", "stock_code", "category_group") + unique("product_sk") + unique("stock_code")
                   + in_list("category_group", ["SEASONAL", "OTHER", "CORE"]) + expr("list_price_positive", "list_price >= 0", "warn"),
    "dim_store": not_null("store_sk", "store_id") + unique("store_sk") + unique("store_id"),
    "dim_employee": not_null("employee_sk", "employee_id") + unique("employee_sk") + unique("employee_id"),
    "dim_date": not_null("date_key", "full_date") + unique("date_key") + in_list("is_trading_day", [0, 1]),
    "dim_promotion": not_null("promo_sk", "promo_id") + unique("promo_sk") + expr("promo_dates_ordered", "start_key <= end_key"),
    "dim_fx": not_null("country", "year_month", "currency", "fx_rate") + unique("country", "year_month")
              + expr("fx_rate_positive", "fx_rate > 0"),
    "fact_sales": (not_null("invoice_no", "stock_code", "customer_sk", "product_sk", "store_sk", "employee_sk", "year_month")
                   + expr("sales_amount_not_negative", "sales_amount >= 0") + expr("zero_price_line", "sales_amount > 0", "warn") + expr("quantity_positive", "quantity > 0")
                   + expr("gross_profit_not_negative", "gross_profit >= 0", "warn")
                   + expr("orphan_product", "product_sk <> -1", "warn")
                   + expr("guest_customer", "customer_sk <> -1", "warn")
                   + fk("store_sk", "dim_store", "store_sk") + fk("employee_sk", "dim_employee", "employee_sk")
                   + fk("product_sk", "dim_product", "product_sk", flt="product_sk <> -1")
                   + fk("customer_sk", "dim_customer", "customer_sk", flt="customer_sk <> -1")),
    "fact_returns": not_null("invoice_no", "stock_code", "product_sk") + expr("returned_qty_positive", "returned_qty > 0")
                    + expr("returned_amount_not_negative", "returned_amount >= 0"),
    "fact_invoice": not_null("invoice_no", "store_sk") + unique("invoice_no") + expr("invoice_sales_not_negative", "invoice_sales >= 0") + expr("zero_value_invoice", "invoice_sales > 0", "warn")
                    + expr("line_total_positive", "line_total > 0"),
    "RETAIL_DATA_MART": not_null("customer_id", "product_name", "customer_type") + in_list("customer_type", ["citizen", "foreign"]),
    "Customer_360": unique("customer_id") + in_list("segment", ["CHAMPION", "LOYAL", "RECENT", "AT_RISK", "HIBERNATING"])
                    + expr("rfm_scores_in_range", "r_score BETWEEN 1 AND 5 AND f_score BETWEEN 1 AND 5 AND m_score BETWEEN 1 AND 5"),
    "Product_Performance": expr("abc_class_valid", "abc_class IN ('A','B','C')"),
    "Q3_Bonus_Program": expr("bonus_rate_valid", "bonus_rate BETWEEN 0.02 AND 0.05"),
    "audit_reconciliation": expr("source_to_fact_reconciles", "status = 'PASS'"),
}

# COMMAND ----------
for t, checks in CHECKS.items():
    status = DQEngine.validate_checks(checks)  # fail fast on malformed rules
    assert not status.has_errors, f"{t}: {status}"

results, failed = [], []
for table, checks in CHECKS.items():
    df = spark.table(fq(table))
    total = df.count()
    checked = dq.apply_checks_by_metadata(df, checks)  # no .cache(): unsupported on serverless env < 5
    for col, sev in (("_errors", "error"), ("_warnings", "warn")):
        agg = (checked.select(F.explode(col).alias("r")).groupBy("r.name").agg(F.count("*").alias("n")).collect())
        for row in agg:
            results.append((table, row["name"], sev, row["n"], total))
    bad = checked.filter(F.size("_errors") > 0).count()
    if bad:
        failed.append((table, bad))
        (checked.filter((F.size("_errors") > 0) | (F.size("_warnings") > 0)).limit(100_000)
         .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq(f"dq_quarantine_{table}")))
    print(f"{table}: {total} rows, {bad} rows with error-level violations")

out = spark.createDataFrame(results, "table_name string, rule string, severity string, failed_rows long, total_rows long")
(out.withColumn("run_ts", F.current_timestamp()).write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(fq("dq_validation_results")))
display(out.orderBy("severity", F.desc("failed_rows")))

# COMMAND ----------
if failed:
    raise Exception(f"DQX validation failed (error-level rules): {failed}. See {fq('dq_validation_results')}")
print("DQX validation passed: no error-level violations")
