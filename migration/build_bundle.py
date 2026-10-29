#!/usr/bin/env python3
"""Emit the Databricks Asset Bundle for the migrated pipeline.

    python3 migration/build_bundle.py     # after ds2spark.py

Writes migration/bundle/{databricks.yml, resources/retail_dw_job.yml, src/notebooks/_*.py}.
The job mirrors Seq_Retail_DW_Master (pipeline/pipeline_dag.json):
  sequencers are flattened into task dependencies, the "dim_customer exists?" exec activity becomes a
  notebook + condition task, the file-move promote becomes a table copy, and the failure handler becomes
  the job's on_failure email list.
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BUNDLE = ROOT / "migration" / "bundle"
NB = BUNDLE / "src" / "notebooks"
DAG = json.loads((ROOT / "pipeline" / "pipeline_dag.json").read_text())["nodes"]
NODE = {n["name"]: n for n in DAG}

WIDGETS = '''dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "retail_dw")
dbutils.widgets.text("src_dir", "")
catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))
'''

SUPPORT = {
    "_setup": f'''# Databricks notebook source
# act_Prepare_Dirs + wff_Source_Orders: create the schema, then wait for the source files to land.

# COMMAND ----------
{WIDGETS}
import os, time

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{catalog}}.{{schema}}")
needed = ["orders_raw.csv", "customers_snapshot_2010.csv", "customers_snapshot_2011.csv", "products_catalog.csv",
          "suppliers.csv", "stores.csv", "employees.csv", "calendar.csv", "promotions.csv", "fx_rates.csv",
          "country_currency.csv"]
deadline = time.time() + 600
while (missing := [f for f in needed if not os.path.exists(f"{{src_dir}}/{{f}}")]) and time.time() < deadline:
    print("waiting for", missing)
    time.sleep(15)
assert not missing, f"source files not found in {{src_dir}}: {{missing}}"
print("sources present in", src_dir)
''',
    "_check_dim_customer": f'''# Databricks notebook source
# act_Check_Dim_Customer: dim_customer.csv existed? -> EXISTS / MISSING (drives the initial-load branch).

# COMMAND ----------
{WIDGETS}
state = "EXISTS" if spark.catalog.tableExists(f"{{catalog}}.{{schema}}.dim_customer") else "MISSING"
print("dim_state =", state)
dbutils.jobs.taskValues.set(key="dim_state", value=state)
''',
    "_promote_dim_customer": f'''# Databricks notebook source
# act_Promote_Dim_Customer: mv dim_customer_next.csv dim_customer.csv -> replace the Delta table.

# COMMAND ----------
{WIDGETS}
spark.sql(f"CREATE OR REPLACE TABLE {{catalog}}.{{schema}}.dim_customer AS SELECT * FROM {{catalog}}.{{schema}}.dim_customer_next")
print(spark.table(f"{{catalog}}.{{schema}}.dim_customer").count(), "rows promoted")
''',
}


def job_name(node):
    return NODE[node]["job"] if NODE[node]["kind"] == "job" else None


def resolve(dep):
    """Flatten sequencer / wait / exec nodes into the task(s) a job depends on."""
    n = NODE[dep]
    if n["kind"] == "seq":
        out = []
        for d, _ in n["deps"]:
            out += resolve(d)
        return out
    return [dep]


TASK_OF = {"act_Prepare_Dirs": "setup", "wff_Source_Orders": "setup", "act_Check_Dim_Customer": "check_dim_customer",
           "act_Promote_Dim_Customer": "promote_dim_customer"}


def task_key(node):
    n = NODE[node]
    return TASK_OF.get(node) or (n["job"] if n["kind"] == "job" else node)


def build_tasks():
    tasks = []
    for n in DAG:
        if n["kind"] in ("seq", "notify", "exc", "term") or n["name"] == "wff_Source_Orders":
            continue
        key = task_key(n["name"])
        deps = []
        for d, trig in n["deps"]:
            for r in resolve(d):
                deps.append((TASK_OF.get(r) or task_key(r), trig))
        deps = [(k, t) for k, t in dict.fromkeys(deps) if k != key]
        t = {"key": key, "deps": deps}
        if n["name"] == "act_Prepare_Dirs":
            t["nb"] = "_setup"
        elif n["name"] == "act_Check_Dim_Customer":
            t["nb"] = "_check_dim_customer"
        elif n["name"] == "act_Promote_Dim_Customer":
            t["nb"] = "_promote_dim_customer"
        else:
            t["nb"] = n["job"]
        tasks.append(t)
    return tasks


def yml(tasks):
    out = ["resources:", "  jobs:", "    retail_dw_master:", "      name: Seq_Retail_DW_Master (${bundle.target})",
           "      description: Migrated DataStage master sequence (23 parallel jobs) with DQX validation",
           "      parameters:",
           "        - {name: catalog, default: '${var.catalog}'}",
           "        - {name: schema, default: '${var.schema}'}",
           "        - {name: src_dir, default: '${var.src_dir}'}",
           "        - {name: pRunDate, default: '2011-12-31'}",
           "        - {name: pEffDate, default: '2011-07-01'}",
           "      environments:",
           "        - environment_key: dqx",
           "          spec:",
           "            environment_version: '5'",
           "            dependencies: ['databricks-labs-dqx==0.17.0']",
           "      tasks:"]
    all_task_keys = [t["key"] for t in tasks]

    def emit(key, nb, deps, extra=()):
        out.append(f"        - task_key: {key}")
        if deps:
            out.append("          depends_on:")
            for k, o in deps:
                out.append(f"            - task_key: {k}" + (f"\n              outcome: '{o}'" if o else ""))
        out.append("          notebook_task:")
        out.append(f"            notebook_path: ../src/notebooks/{nb}.py")
        out.append("            base_parameters:")
        for p in ("catalog", "schema", "src_dir", "pRunDate", "pEffDate"):
            out.append(f"              {p}: '{{{{job.parameters.{p}}}}}'")
        out.extend(extra)

    for t in tasks:
        deps = []
        for k, trig in t["deps"]:
            deps.append((k, None))
        extra = []
        key = t["key"]
        if key == "J05_Dim_Customer_Initial":  # runs only when dim_customer is MISSING
            deps = [("cond_dim_missing", "true")]
        if key == "J06_Dim_Customer_SCD2":  # ANY of (J05 ran, dim already existed)
            deps = [("J05_Dim_Customer_Initial", None), ("check_dim_customer", None)]
            extra = ["          run_if: NONE_FAILED"]
        if key == "check_dim_customer":
            emit(key, t["nb"], deps)
            out.append("        - task_key: cond_dim_missing")
            out.append("          depends_on:\n            - task_key: check_dim_customer")
            out.append("          condition_task:\n            op: EQUAL_TO")
            out.append("            left: '{{tasks.check_dim_customer.values.dim_state}}'")
            out.append("            right: MISSING")
            continue
        emit(key, t["nb"], deps, extra)
    # validation: runs once every pipeline job has finished (seq_All_Done), then fails the run on error rules
    final = [k for k in all_task_keys if k.startswith("J") and int(k[1:3]) >= 14]
    emit("dqx_validate", "_dqx_validate", [(k, None) for k in final])
    out.append("          environment_key: dqx")
    return "\n".join(out) + "\n"


def main():
    NB.mkdir(parents=True, exist_ok=True)
    for name, body in SUPPORT.items():
        (NB / f"{name}.py").write_text(body)
    (BUNDLE / "resources").mkdir(exist_ok=True)
    (BUNDLE / "resources" / "retail_dw_job.yml").write_text(yml(build_tasks()))
    print("tasks:", len(build_tasks()) + 2)


if __name__ == "__main__":
    main()
