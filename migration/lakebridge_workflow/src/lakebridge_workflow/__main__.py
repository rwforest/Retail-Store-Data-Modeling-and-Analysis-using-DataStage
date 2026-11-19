#!/usr/bin/env python3
"""Command-line dispatcher for the packaged Lakebridge workflow tools."""
from __future__ import annotations

import argparse
import importlib

from . import __version__


COMMAND_MODULES = {
    "analyze": "analyze",
    "analyze-parallel": "analyze_parallel",
    "build-hierarchy": "build_hierarchy",
    "merge-reports": "merge_reports",
    "migrate": "migrate",
    "optimize": "optimize",
    "plan": "plan_migration_batch",
    "run-dbxconv": "run_dbxconv",
    "split": "split_datastage_xml",
    "transpile": "transpile_batch",
    "triage-for-switch": "triage_for_switch",
    "workflow": "workflow",
}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="lakebridge-workflow",
        description="Lakebridge Analyzer, hierarchy, planning, transpilation, and BladeBridge tooling",
    )
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    p.add_argument("command", choices=sorted(COMMAND_MODULES), help="Workflow command")
    p.add_argument("args", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)
    ns = p.parse_args(argv)

    module = importlib.import_module(f".{COMMAND_MODULES[ns.command]}", package=__package__)
    return module.main(ns.args)


if __name__ == "__main__":
    raise SystemExit(main())
