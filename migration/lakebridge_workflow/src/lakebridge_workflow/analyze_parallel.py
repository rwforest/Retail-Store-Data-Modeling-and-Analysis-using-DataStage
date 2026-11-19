#!/usr/bin/env python3
"""Run analyzer chunk workers concurrently, then merge their reports.

The official analyzer is single-threaded and also collides on timestamped ``*.tmp`` files
when workers share one folder. This runner gives every worker its own input staging
directory and output directory, then merges the JSON reports.

For this repository (no source XML export), pass ``switch_input`` and
``--local-fallback``: the 24 condensed jobs are grouped into workers and analyzed offline.

Example (current repository)::

    lakebridge-workflow analyze-parallel --chunks-dir switch_input --out-dir parallel_reports \\
        --merge-prefix consolidated_parallel --workers 6 --generate-json --local-fallback
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from . import analyze, merge_reports
from .common import atomic_json, now_utc

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Parallel Lakebridge Analyzer runner")
    p.add_argument("--chunks-dir", required=True, type=Path,
                   help="Directory of XML chunks, or directory of condensed .txt jobs")
    p.add_argument("--out-dir", required=True, type=Path, help="Root output directory")
    p.add_argument("--merge-prefix", type=Path, help="Merged report prefix (default: <out-dir>/consolidated)")
    p.add_argument("--workers", type=int, default=None,
                   help="Analyzer workers; omitted uses one per XML chunk or condensed worker "
                        "group. A lower explicit count distributes chunks across workers")
    p.add_argument("--jobs-per-worker", type=int, default=4,
                   help="Condensed jobs grouped per local worker (ignored for XML chunks)")
    p.add_argument("--generate-json", action="store_true", help="Generate chunk and merged JSON reports")
    p.add_argument("--local-fallback", action="store_true",
                   help="Use the repository condensed-job analyzer instead of an official XML analyzer")
    p.add_argument("--external-command",
                   help="Official analyzer command with {source}, {report}, and {json} placeholders (XML chunks)")
    p.add_argument("--no-merge", action="store_true", help="Do not merge worker reports")
    return p.parse_args(argv)


def discover_chunks(args: argparse.Namespace) -> tuple[list[Path], bool]:
    root = args.chunks_dir
    if not root.is_dir():
        raise SystemExit(f"error: chunks directory not found: {root}")
    xml = sorted(root.glob("*.xml"))
    txt = sorted(root.glob("*.txt"))
    if xml and txt:
        raise SystemExit("error: chunks directory contains both .xml and .txt; split them into separate directories")
    if not xml and not txt:
        raise SystemExit(f"error: no .xml or .txt chunks found under {root}")
    return (xml if xml else txt), bool(xml)


def group_condensed(files: list[Path], size: int) -> list[list[Path]]:
    return [files[i:i + size] for i in range(0, len(files), size)]


def local_worker(index: int, files: list[Path], out_dir: Path, generate_json: bool) -> Path:
    """Stage a group of condensed specs and run the local analyzer in isolation."""
    with tempfile.TemporaryDirectory(prefix=f"analyzer_worker_{index:04d}_") as td:
        stage = Path(td) / "input"
        output = out_dir / f"worker_{index:04d}"
        stage.mkdir(parents=True)
        output.mkdir(parents=True, exist_ok=True)
        for source in files:
            shutil.copy2(source, stage / source.name)
        report = output / f"report_chunk_{index:04d}.xlsx"
        json_path = report.with_suffix(".json") if generate_json else None
        xlsx, actual_json, data = analyze.local_analyze(stage, report, json_path, False)
        # local_analyze returns paths derived from its arguments; normalize names for merge.
        if actual_json and actual_json != output / f"report_chunk_{index:04d}.json":
            shutil.move(str(actual_json), output / f"report_chunk_{index:04d}.json")
        # Record provenance alongside the report.
        atomic_json(output / "worker_manifest.json", {
            "worker": index, "inputs": [str(x) for x in files], "startedAt": now_utc()
        })
        return output / f"report_chunk_{index:04d}.json" if generate_json else output / f"report_chunk_{index:04d}.xlsx"


def external_worker(index: int, source: Path, out_dir: Path, command: str | None) -> Path:
    output = out_dir / f"worker_{index:04d}"
    output.mkdir(parents=True, exist_ok=True)
    # Some analyzer builds consolidate timestamped *.tmp files through the platform temp
    # directory as well as the report folder. Give each worker both isolated locations.
    temp_dir = output / "tmp"
    temp_dir.mkdir(exist_ok=True)
    report = output / f"report_chunk_{index:04d}.xlsx"
    cmd = [
        sys.executable, "-m", "lakebridge_workflow.analyze",
        "--source-directory", str(source), "--report-file", str(report),
        "--source-tech", "Datastage", "--generate-json",
    ]
    if command:
        cmd += ["--external-command", command]
    env = dict(os.environ, TMPDIR=str(temp_dir), TEMP=str(temp_dir), TMP=str(temp_dir))
    result = subprocess.run(cmd, check=False, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"official analyzer returned {result.returncode} for {source}")
    return report.with_suffix(".json")


def run(args: argparse.Namespace) -> int:
    if args.workers is not None and args.workers < 0:
        raise SystemExit("error: --workers must be zero or positive")
    if args.jobs_per_worker <= 0:
        raise SystemExit("error: --jobs-per-worker must be positive")
    files, is_xml = discover_chunks(args)
    if is_xml and args.local_fallback:
        raise SystemExit("error: --local-fallback supports condensed .txt jobs, not XML chunks")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    merge_prefix = args.merge_prefix or (args.out_dir / "consolidated")

    started = time.time()
    report_paths: list[Path] = []
    if is_xml:
        # analyze.py discovers databricks-bb-analyzer on PATH; --external-command remains
        # available for installations that expose a different launcher or binary path.
        worker_count = min(args.workers or len(files), len(files))
        print(f"Running {len(files)} official analyzer chunk(s) across {worker_count} worker(s)")
        if worker_count < len(files):
            print(f"Distributing {len(files)} chunk(s) across {worker_count} analyzer worker(s)")
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="analyzer") as pool:
            futures = {pool.submit(external_worker, i, source, args.out_dir, args.external_command): i
                       for i, source in enumerate(files, 1)}
            for future in as_completed(futures):
                index = futures[future]
                path = future.result()
                report_paths.append(path)
                print(f"  worker {index:04d}: {path}")

        if not args.no_merge:
            if not args.generate_json:
                raise SystemExit("error: auto-merge requires --generate-json")
            namespace = argparse.Namespace(
                reports_dir=[args.out_dir / f"worker_{i:04d}" for i in range(1, len(files) + 1)],
                glob="report_chunk_*.json", out_prefix=merge_prefix, csv=False,
            )
            merge_reports.run(namespace)
        print(f"Completed {len(report_paths)} worker report(s) in {time.time() - started:.1f}s")
        return 0

    sequence_files = [p for p in files if p.read_text(encoding="utf-8", errors="replace").startswith("DATASTAGE SEQUENCE JOB:")]
    parallel_files = [p for p in files if p not in sequence_files]
    groups = group_condensed(parallel_files, args.jobs_per_worker) if parallel_files else [sequence_files]
    # Every worker needs the sequence specs to infer their subset of subJobInfo edges.
    # merge_reports.py deduplicates the repeated sequence inventory later.
    if parallel_files:
        groups = [group + sequence_files for group in groups]
    worker_count = min(args.workers or len(groups), len(groups))
    print(f"Running {len(groups)} local worker chunk(s) across {worker_count} thread(s)")
    if worker_count < len(groups):
        print(f"Distributing {len(groups)} group(s) across {worker_count} local worker(s)")
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="analyzer") as pool:
        futures = {
            pool.submit(local_worker, i, group, args.out_dir, args.generate_json): i
            for i, group in enumerate(groups, 1)
        }
        for future in as_completed(futures):
            index = futures[future]
            try:
                path = future.result()
                report_paths.append(path)
                print(f"  worker {index:04d}: {path}")
            except Exception as exc:
                raise SystemExit(f"error: worker {index:04d} failed: {exc}") from exc

    if not args.no_merge:
        if not args.generate_json:
            raise SystemExit("error: auto-merge requires --generate-json")
        namespace = argparse.Namespace(
            reports_dir=[args.out_dir / f"worker_{i:04d}" for i in range(1, len(groups) + 1)], glob="report_chunk_*.json",
            out_prefix=merge_prefix, csv=False
        )
        merge_reports.run(namespace)
    elapsed = time.time() - started
    print(f"Completed {len(report_paths)} worker report(s) in {elapsed:.1f}s")
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
