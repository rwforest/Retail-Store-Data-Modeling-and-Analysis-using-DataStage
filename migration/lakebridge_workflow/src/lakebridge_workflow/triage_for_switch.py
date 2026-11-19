#!/usr/bin/env python3
"""Scope the (expensive) Switch/LLM pass to the jobs `optimize` says are worth it.

`optimize` is free: a regex pass over the same analyzer JSON `build-hierarchy` reads, with
no API call. Switch is not -- this repository's own 23-job run cost about 526k tokens on
`databricks-claude-sonnet-5-5` (migration/README.md §8, "Cost"), roughly 22.9k tokens per
job. At real migration scale -- hundreds of jobs, thousands, or far more for a source with
hundreds of millions of embedded queries -- sending every item through an LLM "to see if it
helps" is not a cost most budgets survive, and the whole point of `optimize`'s HIGH/MEDIUM/
LOW index is to answer that question in advance, for free, instead.

This script is the connecting step: it reads `optimize`'s own JSON report and copies only
the matching jobs' condensed specifications into a fresh Switch input directory, so Switch
only ever sees the subset selected for a stated reason (a HIGH band, an SCD2 candidate, a
cartesian-risk join, or an explicit list) -- never "everything, to be safe."

Recommended order for a real migration, cheapest and least risky first:

  1. `dbxconv` (or the deterministic `transpile`/compiler) over the full estate. Free,
     offline, no API call -- see migration/README.md §8a; the literal lift-and-shift.
  2. Validate that output against the source before trusting it (run it; reconcile it
     against source behavior where you can). This package does not automate that
     validation; migration/README.md §5-6 is how this repository did it (DQX).
  3. `optimize` over the same full estate. Free, regex-only, no API call: the
     HIGH/MEDIUM/LOW optimization index and merge-candidate list (§8b).
  4. `triage-for-switch` (this command) to turn that index into a small, deliberately
     scoped Switch input directory.
  5. Switch, only on that subset. The only step here with a per-token cost.

Example::

    lakebridge-workflow triage-for-switch \\
        --optimization-json optimization_report.json \\
        --condensed-dir switch_input \\
        --out-dir switch_input_triaged \\
        --bands HIGH
"""
from __future__ import annotations

import argparse
import csv
import json
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Any

from .common import atomic_json, now_utc
from .paths import find_migration_root

# This repository's own measured Switch run (migration/README.md §3, §8): 526k tokens for
# 23 jobs. Used only as an illustrative default for the cost estimate below; override
# --avg-tokens-per-job with a number measured on your own source, since token cost per job
# depends heavily on job size and how much the condensing step shrinks it (§3's own
# dsx_condense.py got 4-10x smaller, 9x for most jobs).
MEASURED_TOKENS_PER_JOB = 526_000 / 23


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    try:
        root = find_migration_root()
    except RuntimeError:
        root = Path(".")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--optimization-json", type=Path, default=root / "optimization_report.json",
                   help="optimize's JSON report (default: <migration-root>/optimization_report.json)")
    p.add_argument("--condensed-dir", type=Path, default=root / "switch_input",
                   help="directory of condensed job .txt files to select from (default: switch_input)")
    p.add_argument("--out-dir", type=Path, required=True,
                   help="destination directory for the selected jobs' condensed specs")
    p.add_argument("--bands", default="HIGH",
                   help="comma-separated optimization bands to include (default: HIGH)")
    p.add_argument("--min-score", type=int, default=None,
                   help="also include any job at or above this optimization score, regardless of band")
    p.add_argument("--no-scd2", dest="include_scd2", action="store_false", default=True,
                   help="do not force-include SCD2/change-detection candidates (default: force-included)")
    p.add_argument("--no-cartesian-risk", dest="include_cartesian", action="store_false", default=True,
                   help="do not force-include cartesian-risk-join candidates (default: force-included)")
    p.add_argument("--jobs", default="",
                   help="comma-separated explicit job names to force-include regardless of score")
    p.add_argument("--avg-tokens-per-job", type=float, default=MEASURED_TOKENS_PER_JOB,
                   help="tokens/job for the illustrative cost estimate "
                        "(default: this repository's own measured Switch run)")
    p.add_argument("--overwrite", action="store_true", help="replace an existing, non-empty --out-dir")
    p.add_argument("--dry-run", action="store_true",
                   help="print the selection and cost estimate without copying any files")
    args = p.parse_args(argv)
    args.band_set = {b.strip().upper() for b in args.bands.split(",") if b.strip()}
    args.job_set = {j.strip() for j in args.jobs.split(",") if j.strip()}
    return args


def select_jobs(result: dict[str, Any], band_set: set[str], min_score: int | None,
                include_scd2: bool, include_cartesian: bool,
                explicit_jobs: set[str]) -> dict[str, dict[str, Any]]:
    """Return {job: {"band", "score", "findings", "reasons"}} for every selected job.

    A job's displayed band/score/findings always come from the optimization index when
    it's present there, regardless of *which* reason triggered its inclusion -- so an SCD2
    candidate that's also independently HIGH-banded shows its real score, not a blank one,
    even if the band check never fired for it.
    """
    index_by_job = {str(row.get("job")): row for row in result.get("optimizationIndex", [])}
    reasons_by_job: dict[str, list[str]] = defaultdict(list)

    for job, row in index_by_job.items():
        band, score = str(row.get("band", "")), int(row.get("score", 0))
        if band in band_set:
            reasons_by_job[job].append(f"band={band}")
        if min_score is not None and score >= min_score:
            reasons_by_job[job].append(f"score>={min_score}")

    if include_scd2:
        for row in result.get("scd2Candidates", []):
            reasons_by_job[str(row.get("job"))].append("scd2Candidate")
    if include_cartesian:
        for row in result.get("cartesianRiskJoins", []):
            reasons_by_job[str(row.get("job"))].append("cartesianRisk")
    for job in explicit_jobs:
        reasons_by_job[job].append("explicit")

    selected: dict[str, dict[str, Any]] = {}
    for job, reasons in reasons_by_job.items():
        row = index_by_job.get(job, {})
        selected[job] = {
            "band": str(row.get("band") or "N/A"), "score": int(row.get("score", 0)),
            "findings": row.get("findings", []), "reasons": reasons,
        }
    return selected


def run(args: argparse.Namespace) -> int:
    if not args.optimization_json.is_file():
        raise SystemExit(f"error: optimization report not found: {args.optimization_json}")
    result = json.loads(args.optimization_json.read_text(encoding="utf-8"))
    if not args.condensed_dir.is_dir():
        raise SystemExit(f"error: condensed job directory not found: {args.condensed_dir}")

    total_jobs = int(result.get("summary", {}).get("totalJobsScanned", 0)) or len(result.get("optimizationIndex", []))
    selected = select_jobs(result, args.band_set, args.min_score, args.include_scd2,
                           args.include_cartesian, args.job_set)

    rows: list[dict[str, Any]] = []
    missing: list[str] = []
    for job, info in sorted(selected.items()):
        source = args.condensed_dir / f"{job}.txt"
        rows.append({
            "job": job, "band": info["band"] or "N/A", "score": info["score"],
            "reasons": ", ".join(info["reasons"]),
            "findings": "; ".join(info["findings"]),
            "sourceFound": source.is_file(),
        })
        if not source.is_file():
            missing.append(job)

    if missing:
        print(f"warning: {len(missing)} selected job(s) have no condensed .txt in "
              f"{args.condensed_dir}, skipped: {', '.join(sorted(missing))}")

    copyable = [r for r in rows if r["sourceFound"]]
    tokens_selected = len(copyable) * args.avg_tokens_per_job
    tokens_all = total_jobs * args.avg_tokens_per_job
    pct_selected = (len(copyable) / total_jobs * 100) if total_jobs else 0.0
    pct_saved = 100.0 - pct_selected

    print(f"Selected {len(copyable)} of {total_jobs} jobs ({pct_selected:.1f}%) for Switch/LLM conversion.")
    for r in sorted(copyable, key=lambda r: (-r["score"], r["job"])):
        print(f"  {r['job']:<40} {r['band']:<7} score={r['score']:<3} {r['reasons']}")
    print(f"Illustrative token estimate at {args.avg_tokens_per_job:,.0f} tokens/job "
          f"(override with --avg-tokens-per-job):")
    print(f"  Selected subset:  ~{tokens_selected:,.0f} tokens")
    print(f"  Full estate:      ~{tokens_all:,.0f} tokens")
    print(f"  Estimated saving: ~{pct_saved:.1f}% versus running Switch on every job")

    if args.dry_run:
        print("(--dry-run: no files copied)")
        return 0

    if args.out_dir.exists():
        if any(args.out_dir.iterdir()) and not args.overwrite:
            raise SystemExit(f"error: {args.out_dir} already exists and is not empty; pass --overwrite")
        shutil.rmtree(args.out_dir)
    args.out_dir.mkdir(parents=True)

    for r in copyable:
        shutil.copy2(args.condensed_dir / f"{r['job']}.txt", args.out_dir / f"{r['job']}.txt")

    manifest = {
        "runInfo": {"builtAt": now_utc(), "optimizationReport": str(args.optimization_json),
                    "condensedDir": str(args.condensed_dir)},
        "selection": {"bands": sorted(args.band_set), "minScore": args.min_score,
                      "includeScd2": args.include_scd2, "includeCartesianRisk": args.include_cartesian,
                      "explicitJobs": sorted(args.job_set)},
        "summary": {"totalJobsScanned": total_jobs, "selected": len(copyable), "missing": missing,
                    "percentSelected": round(pct_selected, 1),
                    "avgTokensPerJob": args.avg_tokens_per_job,
                    "estimatedTokensSelected": round(tokens_selected),
                    "estimatedTokensFullEstate": round(tokens_all)},
        "jobs": rows,
    }
    manifest_path = args.out_dir.parent / f"{args.out_dir.name}_manifest.json"
    atomic_json(manifest_path, manifest)
    csv_path = args.out_dir.parent / f"{args.out_dir.name}_manifest.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["job", "band", "score", "reasons", "findings", "sourceFound"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Condensed specs: {args.out_dir}")
    print(f"  Manifest JSON:   {manifest_path}")
    print(f"  Manifest CSV:    {csv_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
