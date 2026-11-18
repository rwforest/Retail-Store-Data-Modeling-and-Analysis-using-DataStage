#!/usr/bin/env python3
"""Command-line dispatcher for the packaged Lakebridge workflow tools."""
from __future__ import annotations

import argparse
from typing import Callable

from . import (
    analyze, analyze_parallel, build_hierarchy, merge_reports, plan_migration_batch,
    run_dbxconv, split_datastage_xml, transpile_batch, workflow,
)
from . import __version__


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="lakebridge-workflow",
        description="Lakebridge Analyzer, hierarchy, planning, transpilation, and BladeBridge tooling",
    )
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    p.add_argument("command", choices=[
        "analyze", "analyze-parallel", "build-hierarchy", "merge-reports", "plan",
        "run-dbxconv", "split", "transpile", "workflow",
    ], help="Workflow command")
    p.add_argument("args", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)
    ns = p.parse_args(argv)
    commands: dict[str, Callable[[list[str] | None], int]] = {
        "analyze": analyze.main,
        "analyze-parallel": analyze_parallel.main,
        "build-hierarchy": build_hierarchy.main,
        "merge-reports": merge_reports.main,
        "plan": plan_migration_batch.main,
        "run-dbxconv": run_dbxconv.main,
        "split": split_datastage_xml.main,
        "transpile": transpile_batch.main,
        "workflow": workflow.main,
    }
    return commands[ns.command](ns.args)


if __name__ == "__main__":
    raise SystemExit(main())
