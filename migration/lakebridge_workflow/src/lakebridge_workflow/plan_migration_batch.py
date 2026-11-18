#!/usr/bin/env python3
"""Plan a complete, history-aware DataStage-to-Databricks migration batch.

Reads ``job_hierarchy.json`` produced by ``build_hierarchy.py``. A selected root sequence
always migrates with its full descendant closure; shared descendants already present in
``migration_state.json`` are marked ``REUSE_FROM_HISTORY`` rather than transpiled again.

Examples
--------
::

    lakebridge-workflow plan --num-jobs 12 --dry-run
    lakebridge-workflow transpile --num-jobs 12 --reset-history
    lakebridge-workflow plan --status
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .common import atomic_json, is_backup_name, now_utc

SEVERITY = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "VERY_HIGH": 4}
ROLE_ROOT = "1_ROOT_SEQUENCE"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Plan a closed-lineage migration batch")
    p.add_argument("--input-json", type=Path, default=Path("job_hierarchy.json"))
    p.add_argument("--state-file", type=Path, default=Path("migration_state.json"))
    p.add_argument("--num-jobs", type=int, help="Target number of new items (required for planning)")
    p.add_argument("--filter", help="Optional root name/folder substring filter")
    p.add_argument("--dry-run", action="store_true", help="Print the plan without changing state")
    p.add_argument("--reset-history", action="store_true", help="Start from an empty migration state")
    p.add_argument("--status", action="store_true", help="Print migration history and exit")
    p.add_argument("--plan-json", type=Path, help="Optional JSON output for the selected plan")
    args = p.parse_args(argv)
    if not args.status and args.num_jobs is None:
        p.error("--num-jobs is required for planning")
    if args.num_jobs is not None and args.num_jobs <= 0:
        p.error("--num-jobs must be positive")
    if args.status and any((args.dry_run, args.reset_history, args.filter, args.num_jobs is not None)):
        p.error("--status cannot be combined with planning options")
    return args


def load_json(path: Path, required: bool = True) -> dict[str, Any]:
    if not path.is_file():
        if required:
            raise SystemExit(f"error: required JSON not found: {path}")
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"error: cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SystemExit(f"error: {path} must contain a JSON object")
    return value


def empty_state() -> dict[str, Any]:
    return {"version": 1, "created_at": now_utc(), "updated_at": now_utc(),
            "batches": [], "migrated": {}}


def normalize_state(state: dict[str, Any]) -> dict[str, Any]:
    state.setdefault("version", 1)
    state.setdefault("batches", [])
    state.setdefault("migrated", {})
    return state


def load_hierarchy(path: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    hierarchy = load_json(path)
    rows = hierarchy.get("allJobs")
    if not isinstance(rows, list) or not rows:
        raise SystemExit(f"error: {path} has no allJobs array; run build_hierarchy.py first")
    items = {str(row.get("name", "")): row for row in rows if row.get("name")}
    if len(items) != len(rows):
        raise SystemExit(f"error: duplicate or unnamed jobs in {path}")
    return hierarchy, items


def closure(root: str, items: dict[str, dict[str, Any]]) -> list[str]:
    """Return root plus all descendants in deterministic breadth-first order."""
    found: list[str] = []
    seen: set[str] = set()
    queue = [root]
    while queue:
        name = queue.pop(0)
        if name in seen or name not in items:
            continue
        seen.add(name)
        found.append(name)
        queue.extend(items[name].get("children", []))
    return found


def bottom_up_order(names: list[str], items: dict[str, dict[str, Any]]) -> list[str]:
    """Post-order DFS: notebooks/leaf jobs first, orchestrating sequences last."""
    selected = set(names)
    ordered: list[str] = []
    visited: set[str] = set()
    active: set[str] = set()

    def visit(name: str) -> None:
        if name in visited or name in active:
            return
        active.add(name)
        for child in items.get(name, {}).get("children", []):
            if child in selected:
                visit(child)
        active.remove(name)
        visited.add(name)
        ordered.append(name)

    roots = [name for name in names if not any(parent in selected for parent in items.get(name, {}).get("parents", []))]
    for name in roots or names:
        visit(name)
    for name in names:
        visit(name)
    return ordered


def severity(item: dict[str, Any]) -> int:
    return SEVERITY.get(str(item.get("complexity", "")).upper(), 0)


def total_nodes(item: dict[str, Any]) -> int:
    try:
        return int(item.get("totalNodes") or 0)
    except (TypeError, ValueError):
        return 0


def item_record(item: dict[str, Any], root: str, action: str, state: dict[str, Any]) -> dict[str, Any]:
    name = str(item.get("name", ""))
    history = state.get("migrated", {}).get(name, {})
    return {
        "name": name,
        "type": str(item.get("type", "")),
        "role": str(item.get("role", "")),
        "complexity": str(item.get("complexity", "")),
        "folder": str(item.get("folder", "")),
        "action": action,
        "root_sequence": root,
        "total_nodes": total_nodes(item),
        "source_chunk": str(item.get("sourceFile", "") or history.get("source_chunk", "")),
        "from_batch_id": history.get("batch_id") if action == "REUSE_FROM_HISTORY" else None,
    }


def eligible_roots(items: dict[str, dict[str, Any]], state: dict[str, Any], filter_text: str | None) -> list[str]:
    migrated = state.get("migrated", {})
    roots: list[str] = []
    for name, item in items.items():
        if item.get("role") != ROLE_ROOT or name in migrated or is_backup_name(name):
            continue
        haystack = f"{name} {item.get('folder', '')}".lower()
        if filter_text and filter_text.lower() not in haystack:
            continue
        roots.append(name)
    return sorted(roots, key=lambda n: (-severity(items[n]), -total_nodes(items[n]), n))


def make_plan(args: argparse.Namespace, state: dict[str, Any]) -> dict[str, Any] | None:
    _, items = load_hierarchy(args.input_json)
    roots = eligible_roots(items, state, args.filter)
    if not roots:
        return None

    selected_roots: list[str] = []
    planned_new: set[str] = set()
    planned_reused: set[str] = set()
    first_overage = False

    for root in roots:
        members = [name for name in closure(root, items) if name in items]
        new_members = {name for name in members if name not in state["migrated"]}
        reused_members = {name for name in members if name in state["migrated"]}
        if not selected_roots:
            if len(new_members) > args.num_jobs:
                first_overage = True
        elif len(planned_new) + len(new_members) > args.num_jobs:
            continue
        selected_roots.append(root)
        planned_new |= new_members
        planned_reused |= reused_members - planned_new

    if not selected_roots:
        return None

    records: dict[str, dict[str, Any]] = {}
    for root in selected_roots:
        for name in closure(root, items):
            if name not in items or name in records:
                continue
            if name in planned_new:
                action = "MIGRATE_ORCHESTRATION" if items[name].get("type") == "SEQUENCE" else "MIGRATE_NOTEBOOK"
                records[name] = item_record(items[name], root, action, state)
            elif name in planned_reused:
                records[name] = item_record(items[name], root, "REUSE_FROM_HISTORY", state)

    new_names = bottom_up_order([n for n, r in records.items() if r["action"] != "REUSE_FROM_HISTORY"], items)
    reused_names = sorted(n for n, r in records.items() if r["action"] == "REUSE_FROM_HISTORY")
    batch_id = max((int(b.get("batch_id", 0)) for b in state.get("batches", [])), default=0) + 1
    return {
        "batch_id": batch_id,
        "created_at": now_utc(),
        "requested_num_jobs": args.num_jobs,
        "actual_new_jobs_count": len(new_names),
        "reused_jobs_from_history_count": len(reused_names),
        "selected_root_sequences": selected_roots,
        "complete_lineage_closure": True,
        "budget_exceeded_for_first_root": first_overage,
        "new_jobs_in_bottom_up_order": [records[n] for n in new_names],
        "reused_jobs_from_history": [records[n] for n in reused_names],
    }


def apply_manifest(state: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    batch_id = int(manifest["batch_id"])
    for job in manifest["new_jobs_in_bottom_up_order"]:
        name = job["name"]
        state["migrated"][name] = {
            "batch_id": batch_id,
            "root_sequence": job["root_sequence"],
            "artifact_path": (
                f"migrated_batches/batch_{batch_id:03d}/bundle/resources/workflow_batch_{batch_id:03d}.yml"
                if job["type"] == "SEQUENCE" else
                f"migrated_batches/batch_{batch_id:03d}/notebooks/{name}.py"
            ),
            "source_chunk": job["source_chunk"],
        }
    state["batches"].append(manifest)
    state["updated_at"] = now_utc()
    return state


def print_status(state: dict[str, Any]) -> None:
    batches = state.get("batches", [])
    print(f"Migration state: {len(batches)} batch(es), {len(state.get('migrated', {}))} unique migrated item(s)")
    for batch in batches:
        print(f"  Batch {batch.get('batch_id', 0):03d}: "
              f"new={batch.get('actual_new_jobs_count', 0)} "
              f"reused={batch.get('reused_jobs_from_history_count', 0)} "
              f"roots={', '.join(batch.get('selected_root_sequences', []))}")
        for job in batch.get("new_jobs_in_bottom_up_order", []):
            print(f"    {job.get('action', ''):<22} {job.get('name', '')} "
                  f"({job.get('type', '')}, {job.get('complexity', '')})")


def run(args: argparse.Namespace) -> int:
    state = empty_state() if args.reset_history else normalize_state(load_json(args.state_file, required=False))
    if args.status:
        print_status(state)
        return 0

    manifest = make_plan(args, state)
    if manifest is None:
        print("No eligible unmigrated root sequences remain.")
        return 0
    if args.plan_json:
        atomic_json(args.plan_json, manifest)

    print(f"Batch {manifest['batch_id']:03d} plan")
    print(f"  Requested new items: {manifest['requested_num_jobs']}")
    print(f"  Selected roots:      {', '.join(manifest['selected_root_sequences'])}")
    print(f"  New items:           {manifest['actual_new_jobs_count']}")
    print(f"  Reused items:        {manifest['reused_jobs_from_history_count']}")
    if manifest["budget_exceeded_for_first_root"]:
        print("  Note: the first complete root closure exceeded the target; no tree was cut.")
    print("  Bottom-up order:")
    for job in manifest["new_jobs_in_bottom_up_order"]:
        print(f"    {job['name']:<38} {job['type']:<9} {job['complexity']:<9} {job['action']}")

    if args.dry_run:
        print("  State:               dry-run (not modified)")
    else:
        state = apply_manifest(state, manifest)
        atomic_json(args.state_file, state)
        print(f"  State:               {args.state_file}")
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
