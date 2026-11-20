#!/usr/bin/env python3
"""Reconcile the dbxconv DBSQL-target deployment against SDP, table by table.

Unlike dbxconv_output_fixed/reconcile_notebook.py (a Databricks notebook using
spark.sql, since that deployment runs on cluster/serverless compute), this target is
SQL-Warehouse-only -- there's no cluster to run a notebook on, so this drives the same
comparison from a local Python script shelling out to `databricks api post
/api/2.0/sql/statements` (the REST endpoint a SQL Warehouse answers directly). Fill in
WAREHOUSE_ID/PROFILE for your own workspace before running.
"""
import json
import subprocess
import sys

WAREHOUSE_ID = "<warehouse_id>"
PROFILE = "<your CLI profile>"
CATALOG = "unitygo"
SCHEMA_A = "retail_dw_dbxconv_dbsql"
SCHEMA_B = "retail_dw_sdp"


def run_sql(statement, timeout="50s"):
    payload = json.dumps({"warehouse_id": WAREHOUSE_ID, "statement": statement, "wait_timeout": timeout})
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements", "--profile", PROFILE, "--json", payload],
        capture_output=True, text=True,
    )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"error": result.stdout + result.stderr}


def show_tables(schema):
    d = run_sql(f"SHOW TABLES IN {CATALOG}.{schema}")
    if "result" not in d:
        print(f"ERROR listing {schema}: {d}", file=sys.stderr)
        return set()
    return {r[1] for r in d["result"]["data_array"]}


def columns(schema, table):
    d = run_sql(f"DESCRIBE TABLE {CATALOG}.{schema}.{table}")
    if "result" not in d or d["result"].get("row_count", 0) == 0:
        return []
    return [r[0] for r in d["result"]["data_array"] if r[0] and not r[0].startswith("#")]


def scalar(statement):
    d = run_sql(statement)
    if "result" not in d:
        return None, d
    rows = d["result"].get("data_array", [])
    return (rows[0][0] if rows else None), None


tables_a = show_tables(SCHEMA_A)
tables_b = show_tables(SCHEMA_B)
common = sorted(tables_a & tables_b)
only_a = sorted(tables_a - tables_b)
only_b = sorted(tables_b - tables_a)

results = []
for t in common:
    row = {"table": t}
    cols_a = columns(SCHEMA_A, t)
    cols_b = columns(SCHEMA_B, t)
    row["schemaMatch"] = sorted(cols_a) == sorted(cols_b)
    row["colsOnlyInA"] = sorted(set(cols_a) - set(cols_b))
    row["colsOnlyInB"] = sorted(set(cols_b) - set(cols_a))
    if not row["schemaMatch"]:
        row["match"] = False
        row["countA"] = None
        row["countB"] = None
        results.append(row)
        continue
    common_cols = sorted(set(cols_a))
    col_list = ", ".join(f"`{c}`" for c in common_cols)
    a_ref = f"(SELECT {col_list} FROM {CATALOG}.{SCHEMA_A}.{t})"
    b_ref = f"(SELECT {col_list} FROM {CATALOG}.{SCHEMA_B}.{t})"
    count_a, err_a = scalar(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA_A}.{t}")
    count_b, err_b = scalar(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA_B}.{t}")
    only_in_a, err1 = scalar(f"SELECT COUNT(*) FROM ({a_ref} EXCEPT {b_ref})")
    only_in_b, err2 = scalar(f"SELECT COUNT(*) FROM ({b_ref} EXCEPT {a_ref})")
    row["countA"] = count_a
    row["countB"] = count_b
    row["rowsOnlyInA"] = only_in_a
    row["rowsOnlyInB"] = only_in_b
    row["match"] = (only_in_a == "0" and only_in_b == "0")
    if err_a or err_b or err1 or err2:
        row["error"] = str(err_a or err_b or err1 or err2)[:300]
        row["match"] = False
    results.append(row)
    print(f"{t:35s} countA={count_a} countB={count_b} onlyA={only_in_a} onlyB={only_in_b} match={row['match']}", flush=True)

summary = {
    "tablesOnlyInA": only_a,
    "tablesOnlyInB": only_b,
    "commonTables": len(common),
    "matchingTables": sum(1 for r in results if r.get("match")),
    "mismatchedTables": [r["table"] for r in results if not r.get("match")],
    "details": results,
}
with open("reconciliation_result.json", "w") as f:
    json.dump(summary, f, indent=2)
print()
print("=== SUMMARY ===")
print("matchingTables:", summary["matchingTables"], "/", summary["commonTables"])
print("mismatchedTables:", summary["mismatchedTables"])
print("tablesOnlyInA:", only_a)
print("tablesOnlyInB:", only_b)
