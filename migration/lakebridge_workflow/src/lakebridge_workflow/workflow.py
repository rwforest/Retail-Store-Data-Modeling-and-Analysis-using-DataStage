#!/usr/bin/env python3
"""Run the complete current-repository Lakebridge assessment and migration workflow."""
from __future__ import annotations

import argparse
from pathlib import Path

from . import analyze, build_hierarchy, plan_migration_batch, transpile_batch
from .paths import find_migration_root


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    root = find_migration_root()
    p = argparse.ArgumentParser(
        description="Run assessment, hierarchy, planning, and batch transpilation end to end",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--num-jobs", type=int, help="Target number of new items (required)")
    p.add_argument("--assessment-prefix", type=Path, default=root / "consolidated_all_jobs")
    p.add_argument("--hierarchy-prefix", type=Path, default=root / "job_hierarchy")
    p.add_argument("--state-file", type=Path, default=root / "migration_state.json")
    p.add_argument("--output-root", type=Path, default=root / "migrated_batches")
    p.add_argument("--filter")
    p.add_argument("--transpile-workers", type=int, default=0, metavar="N",
                   help="Transpiler worker processes (0 = automatically use available CPUs)")
    p.add_argument("--reset-history", action="store_true")
    p.add_argument("--overwrite", action="store_true", help="Replace an existing batch directory")
    p.add_argument("--plan-only", action="store_true", help="Stop after the dry-run batch plan")
    args = p.parse_args(argv)
    if args.num_jobs is None:
        p.error("--num-jobs is required")
    if args.num_jobs <= 0:
        p.error("--num-jobs must be positive")
    if args.transpile_workers < 0:
        p.error("--transpile-workers must be zero or positive")
    return args


def run(args: argparse.Namespace) -> int:
    root = find_migration_root()
    xlsx = args.assessment_prefix.with_suffix(".xlsx")
    report_json = args.assessment_prefix.with_suffix(".json")
    hierarchy_json = args.hierarchy_prefix.with_suffix(".json")

    print("Step 1/4: offline assessment", flush=True)
    rc = analyze.main([
        "--source-directory", str(root / "switch_input"),
        "--report-file", str(xlsx),
        "--source-tech", "Datastage",
        "--generate-json",
        "--json-file", str(report_json),
    ])
    if rc:
        return rc

    print("Step 2/4: lineage hierarchy", flush=True)
    rc = build_hierarchy.main([
        "--input-json", str(report_json),
        "--out-prefix", str(args.hierarchy_prefix),
    ])
    if rc:
        return rc

    print("Step 3/4: closed-lineage plan", flush=True)
    plan_args = [
        "--input-json", str(hierarchy_json),
        "--state-file", str(args.state_file),
        "--num-jobs", str(args.num_jobs),
        "--dry-run",
    ]
    if args.filter:
        plan_args += ["--filter", args.filter]
    rc = plan_migration_batch.main(plan_args)
    if rc or args.plan_only:
        return rc

    print("Step 4/4: batch transpilation", flush=True)
    transpile_args = [
        "--input-json", str(hierarchy_json),
        "--state-file", str(args.state_file),
        "--output-root", str(args.output_root),
        "--num-jobs", str(args.num_jobs),
        "--workers", str(args.transpile_workers),
    ]
    if args.filter:
        transpile_args += ["--filter", args.filter]
    if args.reset_history:
        transpile_args.append("--reset-history")
    if args.overwrite:
        transpile_args.append("--overwrite")
    rc = transpile_batch.main(transpile_args)
    if rc:
        return rc

    print("Workflow complete", flush=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
