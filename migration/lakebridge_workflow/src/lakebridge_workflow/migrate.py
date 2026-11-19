#!/usr/bin/env python3
"""Run the complete Lakebridge migration workflow from a DSExport or condensed jobs."""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from . import (
    analyze,
    analyze_parallel,
    build_hierarchy,
    merge_reports,
    plan_migration_batch,
    split_datastage_xml,
    transpile_batch,
)
from .paths import find_migration_root


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    root = find_migration_root()
    p = argparse.ArgumentParser(
        description="Run split, parallel analysis, merge, hierarchy, planning, and transpilation end to end",
    )
    p.add_argument("--source", type=Path, required=True,
                   help="Required: large DSExport XML/TXT file or condensed-job directory")
    p.add_argument("--num-jobs", type=int, required=True,
                   help="Required: target number of new migration items")
    p.add_argument("--jobs-per-file", type=int, default=500,
                   help="Optional: jobs per XML chunk when splitting a DSExport (default: 500)")
    p.add_argument("--workers", type=int, default=None,
                   help="Optional: analyzer workers; omitted uses one per XML chunk. A lower "
                        "explicit count distributes chunks across workers "
                        "(default: one per XML chunk)")
    p.add_argument("--transpile-workers", type=int, default=0,
                   help="Optional: transpiler worker processes; 0 means automatic (default: 0)")
    p.add_argument("--analysis-dir", type=Path, default=root / "lakebridge_workflow_analysis",
                   help="Optional: directory for chunks, reports, merged report, and hierarchy "
                        "(default: <migration-root>/lakebridge_workflow_analysis)")
    p.add_argument("--state-file", type=Path, default=root / "migration_state.json",
                   help="Optional: migration history JSON file (default: <migration-root>/migration_state.json)")
    p.add_argument("--output-root", type=Path, default=root / "migrated_batches",
                   help="Optional: batch output root (default: <migration-root>/migrated_batches)")
    p.add_argument("--filter",
                   help="Optional: root name or folder substring filter (default: no filter)")
    p.add_argument("--external-command",
                   help="Optional: official analyzer command with {source}, {report}, and {json} "
                        "placeholders (default: discover databricks-bb-analyzer on PATH)")
    p.add_argument("--reset-history", action="store_true",
                   help="Optional: start from an empty migration state (default: false)")
    p.add_argument("--overwrite", action="store_true",
                   help="Optional: replace an existing batch output directory (default: false)")
    p.add_argument("--plan-only", action="store_true",
                   help="Optional: stop after the dry-run batch plan (default: false)")
    p.add_argument("--no-xml", action="store_true",
                   help="Optional: do not create standalone XML copies; notebooks are still "
                        "generated (default: false)")
    args = p.parse_args(argv)

    if args.num_jobs <= 0:
        p.error("--num-jobs must be positive")
    if args.jobs_per_file <= 0:
        p.error("--jobs-per-file must be positive")
    if args.workers is not None and args.workers < 0:
        p.error("--workers must be zero or positive")
    if args.transpile_workers < 0:
        p.error("--transpile-workers must be zero or positive")
    return args


def is_dsexport(source: Path) -> bool:
    """Detect a large IBM DataStage XML export, even when saved with a .txt extension."""
    if source.is_dir():
        return False
    try:
        head = source.read_text(encoding="utf-8-sig", errors="ignore")[:4096].lstrip().lower()
    except OSError:
        return False
    return head.startswith("<?xml") or head.startswith("<dsexport")


def run_dsexport_analysis(args: argparse.Namespace) -> Path:
    """Split, analyze, and merge reports from a DSExport source."""
    seq_chunks = args.analysis_dir / "sequence_chunks"
    etl_chunks = args.analysis_dir / "etl_chunks"
    seq_reports = args.analysis_dir / "sequence_reports"
    etl_reports = args.analysis_dir / "etl_reports"
    merged_prefix = args.analysis_dir / "consolidated_all_jobs"

    # These generated subdirectories are always recreated. This prevents stale chunks or
    # reports from a previous run with a different export from entering the new analysis.
    for path in (seq_chunks, etl_chunks, seq_reports, etl_reports):
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)

    split_datastage_xml.main([
        str(args.source),
        "--out-dir", str(seq_chunks),
        "--jobs-per-file", str(args.jobs_per_file),
        "--only-job-type", "2",
        "--quiet",
    ])
    split_datastage_xml.main([
        str(args.source),
        "--out-dir", str(etl_chunks),
        "--jobs-per-file", str(args.jobs_per_file),
        "--exclude-job-type", "2",
        "--skip-table-defs",
        "--quiet",
    ])

    analyzer_args = [
        "--generate-json",
    ]
    if args.workers is not None:
        analyzer_args += ["--workers", str(args.workers)]
    if args.external_command:
        analyzer_args += ["--external-command", args.external_command]

    seq_files = list(seq_chunks.glob("*.xml"))
    etl_files = list(etl_chunks.glob("*.xml"))
    if not seq_files and not etl_files:
        raise SystemExit(f"error: no jobs found in DSExport source: {args.source}")

    report_dirs: list[Path] = []
    if seq_files:
        rc = analyze_parallel.main([
            "--chunks-dir", str(seq_chunks),
            "--out-dir", str(seq_reports),
            "--merge-prefix", str(seq_reports / "consolidated_sequence_jobs"),
            *analyzer_args,
        ])
        if rc:
            raise SystemExit(f"error: sequence analysis failed with exit code {rc}")
        report_dirs.append(seq_reports)
    if etl_files:
        rc = analyze_parallel.main([
            "--chunks-dir", str(etl_chunks),
            "--out-dir", str(etl_reports),
            "--merge-prefix", str(etl_reports / "consolidated_etl_jobs"),
            *analyzer_args,
        ])
        if rc:
            raise SystemExit(f"error: ETL analysis failed with exit code {rc}")
        report_dirs.append(etl_reports)

    merge_args: list[str] = []
    for directory in report_dirs:
        merge_args += ["--reports-dir", str(directory)]
    merge_args += ["--glob", "consolidated_*.json", "--out-prefix", str(merged_prefix)]
    rc = merge_reports.main(merge_args)
    if rc:
        raise SystemExit(f"error: report merge failed with exit code {rc}")

    return merged_prefix.with_suffix(".json")


def run_condensed_analysis(args: argparse.Namespace) -> Path:
    """Analyze condensed jobs and build the hierarchy without XML splitting."""
    report_prefix = args.analysis_dir / "consolidated_all_jobs"
    args.analysis_dir.mkdir(parents=True, exist_ok=True)

    rc = analyze.main([
        "--source-directory", str(args.source),
        "--report-file", str(report_prefix.with_suffix(".xlsx")),
        "--source-tech", "Datastage",
        "--generate-json",
        "--json-file", str(report_prefix.with_suffix(".json")),
    ])
    if rc:
        raise SystemExit(f"error: condensed analysis failed with exit code {rc}")

    return report_prefix.with_suffix(".json")


def run(args: argparse.Namespace) -> int:
    if not args.source.exists():
        raise SystemExit(f"error: source not found: {args.source}")

    if is_dsexport(args.source):
        print("Phase 1/5: split, analyze, and merge DSExport chunks", flush=True)
        report_json = run_dsexport_analysis(args)
    else:
        print("Phase 1/5: analyze condensed jobs", flush=True)
        report_json = run_condensed_analysis(args)

    print("Phase 2/5: lineage hierarchy", flush=True)
    hierarchy_prefix = args.analysis_dir / "job_hierarchy"
    rc = build_hierarchy.main([
        "--input-json", str(report_json),
        "--out-prefix", str(hierarchy_prefix),
    ])
    if rc:
        raise SystemExit(f"error: hierarchy build failed with exit code {rc}")
    hierarchy_json = hierarchy_prefix.with_suffix(".json")

    print("Phase 3/5: closed-lineage plan", flush=True)
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

    print("Phase 4/5: parallel batch transpilation", flush=True)
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
    if args.no_xml:
        transpile_args.append("--no-xml")
    rc = transpile_batch.main(transpile_args)
    if rc:
        return rc

    print("Phase 5/5: migration state update", flush=True)
    # transpile_batch updates migration_state atomically.
    print("Migration workflow complete", flush=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
