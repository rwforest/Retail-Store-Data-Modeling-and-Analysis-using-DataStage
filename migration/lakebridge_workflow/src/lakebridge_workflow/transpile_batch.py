#!/usr/bin/env python3
"""Transpile one planned, closed-lineage batch using the repository's deterministic compiler.

For each new parallel job this script uses ``ds2spark.Compiler`` (the same compiler as the
checked-in reference notebooks). Notebook compilation, Switch-spec copying, and standalone
XML generation run in an isolated process pool. Planning, workflow generation, and migration
state updates remain single-writer operations so history and manifests stay deterministic.
Jobs already present in ``migration_state.json`` are not recompiled; the workflow points to
their earlier notebook path. If source DSX files are available, standalone XML files are
also emitted for optional offline BladeBridge conversion.

Example::

    lakebridge-workflow transpile --num-jobs <target-new-items> --workers <worker-count>
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .common import atomic_json
from .paths import find_migration_root
from .plan_migration_batch import apply_manifest, empty_state, make_plan, normalize_state

MIGRATION_ROOT = find_migration_root()
REPO = MIGRATION_ROOT.parent


def load_module(name: str, path: Path):
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pipeline = load_module("retail_pipeline_def", REPO / "pipeline" / "pipeline_def.py")
ds2spark = load_module("retail_ds2spark", MIGRATION_ROOT / "ds2spark.py")
build_bundle = load_module("retail_build_bundle", MIGRATION_ROOT / "build_bundle.py")
dsx_to_xml = load_module("retail_dsx_to_xml", MIGRATION_ROOT / "dsx_to_xml.py")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Transpile a planned closed-lineage migration batch")
    p.add_argument("--input-json", type=Path, default=MIGRATION_ROOT / "job_hierarchy.json")
    p.add_argument("--state-file", type=Path, default=MIGRATION_ROOT / "migration_state.json")
    p.add_argument("--num-jobs", type=int)
    p.add_argument("--filter")
    p.add_argument("--reset-history", action="store_true")
    p.add_argument("--output-root", type=Path, default=MIGRATION_ROOT / "migrated_batches")
    p.add_argument("--switch-input-dir", type=Path, default=MIGRATION_ROOT / "switch_input")
    p.add_argument("--source-dsx-dir", type=Path, default=REPO / "DataStage Jobs" / "Pipeline")
    p.add_argument("--no-xml", action="store_true", help="Do not create standalone XML copies")
    p.add_argument("--workers", type=int, default=0, metavar="N",
                   help="Transpiler worker processes (0 = automatically use available CPUs)")
    p.add_argument("--overwrite", action="store_true", help="Replace an existing batch directory")
    args = p.parse_args(argv)
    if args.num_jobs is None:
        p.error("--num-jobs is required")
    if args.num_jobs <= 0:
        p.error("--num-jobs must be positive")
    if args.workers < 0:
        p.error("--workers must be zero or positive")
    return args


def load_state(path: Path, reset: bool) -> dict[str, Any]:
    if reset:
        return empty_state()
    if not path.is_file():
        return empty_state()
    try:
        return normalize_state(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"error: cannot read {path}: {exc}") from exc


def write_notebook(job: Any, directory: Path) -> Path:
    path = directory / f"{job.name}.py"
    path.write_text(ds2spark.Compiler(job).compile(), encoding="utf-8")
    return path


def create_xml(name: str, source_dsx_dir: Path, xml_dir: Path) -> Path | None:
    source = source_dsx_dir / f"{name}.dsx"
    if not source.is_file():
        return None
    nodes = dsx_to_xml.parse(source.read_text(encoding="cp1252"))
    xml_dir.mkdir(parents=True, exist_ok=True)
    target = xml_dir / f"{name}.xml"
    target.write_text(dsx_to_xml.to_xml(nodes), encoding="utf-8")
    return target


def copy_switch_spec(name: str, source_dir: Path, target_dir: Path) -> bool:
    source = source_dir / f"{name}.txt"
    if not source.is_file():
        return False
    target_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target_dir / source.name)
    return True


def copy_support_notebooks(notebook_dir: Path) -> int:
    names = ["_setup.py", "_check_dim_customer.py", "_promote_dim_customer.py", "_dqx_validate.py"]
    source_dir = MIGRATION_ROOT / "bundle" / "src" / "notebooks"
    copied = 0
    for name in names:
        source = source_dir / name
        if source.is_file():
            shutil.copy2(source, notebook_dir / name)
            copied += 1
    return copied


@dataclass(frozen=True)
class TranspileTask:
    """One independently writable job artifact set.

    ProcessPoolExecutor tasks must be picklable. The pipeline Job model is a plain object
    graph and serializes successfully; workers compile it without sharing mutable state with
    the parent process.
    """

    name: str
    job: Any | None
    notebook_dir: Path
    switch_source_dir: Path
    switch_dir: Path
    xml_dir: Path
    source_dsx_dir: Path
    create_notebook: bool
    create_xml: bool


def transpile_worker(task: TranspileTask) -> dict[str, Any]:
    """Compile one job and create its Switch/XML artifacts in an isolated process."""
    started = time.perf_counter()
    result = {
        "name": task.name,
        "notebook": False,
        "switch_spec": False,
        "xml": False,
    }

    if task.create_notebook:
        if task.job is None:
            raise RuntimeError(f"selected parallel job has no pipeline model: {task.name}")
        write_notebook(task.job, task.notebook_dir)
        result["notebook"] = True

    result["switch_spec"] = copy_switch_spec(task.name, task.switch_source_dir, task.switch_dir)
    if task.create_xml:
        result["xml"] = create_xml(task.name, task.source_dsx_dir, task.xml_dir) is not None

    result["elapsed_seconds"] = round(time.perf_counter() - started, 4)
    return result


def run_parallel_transpile(tasks: list[TranspileTask], requested_workers: int) -> list[dict[str, Any]]:
    """Run independent job artifact tasks in a process pool.

    ``requested_workers == 0`` selects an automatic count bounded by both available CPUs and
    the number of tasks. The pool is closed before returning, so no worker can mutate batch
    state or workflow YAML after the single-writer phase begins.
    """
    if not tasks:
        return []
    cpu_count = os.cpu_count() or 1
    worker_count = requested_workers or min(cpu_count, len(tasks))
    worker_count = max(1, min(worker_count, len(tasks)))
    print(f"Transpiling {len(tasks)} item(s) with {worker_count} worker process(es)", flush=True)

    results: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=worker_count) as pool:
        futures = {pool.submit(transpile_worker, task): task.name for task in tasks}
        try:
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                artifact_types = [k for k in ("notebook", "switch_spec", "xml") if result.get(k)]
                print(f"  {result['name']:<40} {', '.join(artifact_types) or 'no artifacts'} "
                      f"({result['elapsed_seconds']:.3f}s)", flush=True)
        except BaseException:
            for future in futures:
                future.cancel()
            pool.shutdown(wait=True, cancel_futures=True)
            raise
    return results


def reused_notebook_path(state: dict[str, Any], name: str) -> str:
    artifact = str(state.get("migrated", {}).get(name, {}).get("artifact_path", ""))
    prefix = "migrated_batches/"
    if artifact.startswith(prefix):
        return "../../" + artifact[len(prefix):]
    return f"../../{name}.py"


def workflow_yaml(batch_id: int, root_names: list[str], new_jobs: set[str],
                  all_jobs: set[str], state: dict[str, Any]) -> str:
    tasks = build_bundle.build_tasks()
    selected = all_jobs
    # Keep support tasks needed by the retail DAG; they are copied into this batch.
    included = [t for t in tasks if t["nb"].startswith("_") or t["nb"] in selected]
    keys = {t["key"] for t in included}
    out = [
        "resources:", "  jobs:", f"    workflow_batch_{batch_id:03d}:",
        f"      name: {' + '.join(root_names)} batch_{batch_id:03d} (${{bundle.target}})",
        "      description: Complete DataStage lineage closure migrated by transpile_batch.py",
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
        "      tasks:",
    ]

    def emit(key: str, notebook: str, deps: list[tuple[str, str | None]], extra: list[str] = []):
        out.append(f"        - task_key: {key}")
        if deps:
            out.append("          depends_on:")
            for dep_key, _trigger in deps:
                if dep_key in keys or dep_key == "cond_dim_missing":
                    out.append(f"            - task_key: {dep_key}")
        out.append("          notebook_task:")
        out.append(f"            notebook_path: {notebook}")
        out.append("            base_parameters:")
        for param in ("catalog", "schema", "src_dir", "pRunDate", "pEffDate"):
            out.append(f"              {param}: '{{{{job.parameters.{param}}}}}'")
        out.extend(extra)

    for task in included:
        key, nb, deps = task["key"], task["nb"], task["deps"]
        if key == "J05_Dim_Customer_Initial":
            deps = [("cond_dim_missing", "true")]
        elif key == "J06_Dim_Customer_SCD2":
            deps = [("J05_Dim_Customer_Initial", None), ("check_dim_customer", None)]
        if nb.startswith("_"):
            path = f"../notebooks/{nb}.py"
        elif nb in new_jobs:
            path = f"../notebooks/{nb}.py"
        else:
            path = reused_notebook_path(state, nb)
        extra = ["          run_if: NONE_FAILED"] if key == "J06_Dim_Customer_SCD2" else []
        emit(key, path, deps, extra)
        if key == "check_dim_customer":
            out.extend([
                "        - task_key: cond_dim_missing",
                "          depends_on:", "            - task_key: check_dim_customer",
                "          condition_task:", "            op: EQUAL_TO",
                "            left: '{{tasks.check_dim_customer.values.dim_state}}'",
                "            right: MISSING",
            ])

    final = [key for key in sorted(selected) if key.startswith("J") and key[1:3].isdigit() and int(key[1:3]) >= 14]
    if final:
        emit("dqx_validate", "../notebooks/_dqx_validate.py", [(k, None) for k in final])
        out.append("          environment_key: dqx")
    return "\n".join(out) + "\n"


def write_batch_summary(manifest: dict[str, Any], batch_dir: Path) -> Path:
    import csv
    path = batch_dir / "batch_summary.csv"
    rows = [["Batch_ID", "Job_Name", "Type", "Complexity", "Folder", "Action",
             "Root_Sequence", "Total_Nodes", "Source_Chunk", "From_Batch_ID"]]
    for job in manifest["new_jobs_in_bottom_up_order"]:
        rows.append([manifest["batch_id"], job["name"], job["type"], job["complexity"],
                     job["folder"], job["action"], job["root_sequence"], job["total_nodes"],
                     job["source_chunk"], ""])
    for job in manifest["reused_jobs_from_history"]:
        rows.append([manifest["batch_id"], job["name"], job["type"], job["complexity"],
                     job["folder"], job["action"], job["root_sequence"], job["total_nodes"],
                     job["source_chunk"], job.get("from_batch_id", "")])
    with path.open("w", newline="", encoding="utf-8") as handle:
        csv.writer(handle).writerows(rows)
    return path


def databricks_yaml(batch_id: int) -> str:
    return f"""bundle:
  name: retail_dw_batch_{batch_id:03d}

variables:
  catalog:
    description: Unity Catalog catalog that holds the migrated tables
  schema:
    default: retail_dw_batch_{batch_id:03d}
  src_dir:
    description: UC volume path holding the Source_Data CSVs

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      profile: DEFAULT
    variables:
      catalog: unitygo
      schema: retail_dw_batch_{batch_id:03d}
      src_dir: /Volumes/unitygo/retail_dw/landing
"""


def run(args: argparse.Namespace) -> int:
    state = load_state(args.state_file, args.reset_history)
    manifest = make_plan(args, state)
    if manifest is None:
        print("No eligible unmigrated root sequences remain.")
        return 0

    batch_id = int(manifest["batch_id"])
    batch_dir = args.output_root / f"batch_{batch_id:03d}"
    if batch_dir.exists():
        if not args.overwrite:
            raise SystemExit(f"error: {batch_dir} already exists; pass --overwrite to replace it")
        shutil.rmtree(batch_dir)
    notebooks = batch_dir / "notebooks"
    switch_dir = batch_dir / "switch_input"
    xml_dir = batch_dir / "xml"
    bundle = batch_dir / "bundle"
    resources = bundle / "resources"
    notebooks.mkdir(parents=True)
    switch_dir.mkdir(parents=True)
    resources.mkdir(parents=True)

    pipeline_jobs = {j.name: j for j in pipeline.build_all()}
    new_jobs = [job for job in manifest["new_jobs_in_bottom_up_order"]]
    new_names = {job["name"] for job in new_jobs if job["action"] == "MIGRATE_NOTEBOOK"}
    all_names = {job["name"] for job in new_jobs} | {
        job["name"] for job in manifest["reused_jobs_from_history"]
    }

    # Create shared directories before workers start. Every worker then writes only to
    # uniquely named files, avoiding directory-creation races and shared temporary state.
    xml_dir.mkdir(parents=True, exist_ok=True)
    copied_support = copy_support_notebooks(notebooks)

    # Each newly migrated item owns one task. Parallel jobs compile a notebook; sequence
    # orchestrators generate only their Switch spec and optional standalone XML. Reused
    # history jobs are deliberately omitted; the workflow references their prior artifacts.
    transpile_tasks: list[TranspileTask] = []
    for job in new_jobs:
        name = job["name"]
        create_notebook = job["action"] == "MIGRATE_NOTEBOOK"
        if create_notebook and name not in pipeline_jobs:
            raise SystemExit(f"error: selected parallel job has no pipeline_def model: {name}")
        transpile_tasks.append(TranspileTask(
            name=name,
            job=pipeline_jobs.get(name) if create_notebook else None,
            notebook_dir=notebooks,
            switch_source_dir=args.switch_input_dir,
            switch_dir=switch_dir,
            xml_dir=xml_dir,
            source_dsx_dir=args.source_dsx_dir,
            create_notebook=create_notebook,
            create_xml=not args.no_xml,
        ))

    try:
        results = run_parallel_transpile(transpile_tasks, args.workers)
    except Exception as exc:
        shutil.rmtree(batch_dir, ignore_errors=True)
        raise SystemExit(f"error: parallel transpilation failed: {exc}") from exc

    compiled = sum(bool(result.get("notebook")) for result in results)
    specs = sum(bool(result.get("switch_spec")) for result in results)
    xml_count = sum(bool(result.get("xml")) for result in results)

    root_names = list(manifest["selected_root_sequences"])
    (bundle / "databricks.yml").write_text(databricks_yaml(batch_id), encoding="utf-8")
    (resources / f"workflow_batch_{batch_id:03d}.yml").write_text(
        workflow_yaml(batch_id, root_names, new_names, all_names, state), encoding="utf-8")
    atomic_json(batch_dir / "manifest.json", manifest)
    summary_path = write_batch_summary(manifest, batch_dir)

    state = apply_manifest(state, manifest)
    atomic_json(args.state_file, state)

    print(f"Batch {batch_id:03d}: {batch_dir}")
    print(f"  New notebooks:      {compiled}")
    print(f"  Support notebooks: {copied_support}")
    print(f"  Switch specs:      {specs}")
    print(f"  Standalone XML:    {xml_count}")
    print(f"  Reused jobs:       {manifest['reused_jobs_from_history_count']}")
    print(f"  Workflow:          {resources / f'workflow_batch_{batch_id:03d}.yml'}")
    print(f"  Summary:           {summary_path}")
    print(f"  State:             {args.state_file}")
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
