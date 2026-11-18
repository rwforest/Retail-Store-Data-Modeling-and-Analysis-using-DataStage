#!/usr/bin/env python3
"""Merge multiple Lakebridge Analyzer JSON reports into one deduplicated JSON/XLSX report.

Examples
--------
    lakebridge-workflow merge-reports --reports-dir seq_reports --reports-dir etl_reports \\
        --out-prefix consolidated_all_jobs

The merger is intentionally offline and dependency-free. It recomputes summary totals after
deduplicating inventory items by job name and lineage edges by parent/child/activity.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .common import atomic_json, now_utc, rows_to_csv, write_xlsx


LIST_TABLES = {
    "jobsTransformationsList": ["job", "transformation", "count"],
    "itemNodeInfo": ["job", "node", "nodeType", "conformedType", "order"],
    "jobParameters": ["job", "name", "type", "default", "help"],
    "transformationExpressions": ["job", "transformation", "field", "expression"],
    "miscJobAttributes": ["job", "stage", "link", "name", "value"],
    "embeddedSQLPrograms": ["job", "stage", "sql"],
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Merge Lakebridge Analyzer JSON reports")
    p.add_argument("--reports-dir", action="append", required=True, type=Path,
                   help="Directory containing report JSON files (repeatable)")
    p.add_argument("--glob", default="*.json", help="JSON glob (default: *.json)")
    p.add_argument("--out-prefix", required=True, type=Path, help="Output path prefix")
    p.add_argument("--csv", action="store_true", help="Also write a job inventory CSV")
    return p.parse_args(argv)


def load_reports(directories: list[Path], pattern: str) -> list[tuple[Path, dict[str, Any]]]:
    reports: list[tuple[Path, dict[str, Any]]] = []
    for directory in directories:
        if not directory.is_dir():
            raise SystemExit(f"error: reports directory not found: {directory}")
        for path in sorted(directory.glob(pattern)):
            if not path.is_file():
                continue
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise SystemExit(f"error: cannot read {path}: {exc}") from exc
            if not isinstance(data, dict) or "inventory" not in data:
                print(f"warning: skipping non-analyzer JSON: {path}")
                continue
            reports.append((path, data))
    if not reports:
        raise SystemExit("error: no analyzer JSON reports found")
    return reports


def key_for(table: str, row: dict[str, Any]) -> tuple[Any, ...]:
    if table == "jobsTransformationsList":
        return row.get("job"), row.get("transformation")
    if table == "itemNodeInfo":
        return row.get("job"), row.get("node"), row.get("order")
    if table == "jobParameters":
        return row.get("job"), row.get("name")
    if table == "transformationExpressions":
        return row.get("job"), row.get("transformation"), row.get("field"), row.get("expression")
    if table == "miscJobAttributes":
        return row.get("job"), row.get("stage"), row.get("link"), row.get("name"), row.get("value")
    if table == "embeddedSQLPrograms":
        return row.get("job"), row.get("stage"), row.get("sql")
    return tuple(sorted((k, str(v)) for k, v in row.items()))


def merge(reports: list[tuple[Path, dict[str, Any]]]) -> dict[str, Any]:
    inventory_by_name: dict[str, dict[str, Any]] = {}
    subjobs_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    table_rows: dict[str, dict[tuple[Any, ...], dict[str, Any]]] = {name: {} for name in LIST_TABLES}
    # Transformation and SQL counts are derived after table-row deduplication. This
    # prevents a sequence copied into every local worker from being counted six times.
    sources: list[str] = []
    starts: list[str] = []
    engines: set[str] = set()

    for path, report in reports:
        sources.append(str(path))
        run_info = report.get("runInfo", {})
        if run_info.get("startTime"):
            starts.append(str(run_info["startTime"]))
        if run_info.get("engine"):
            engines.add(str(run_info["engine"]))
        for item in report.get("inventory", []):
            name = str(item.get("name", ""))
            if name and name not in inventory_by_name:
                enriched = dict(item)
                if not enriched.get("sourceFile"):
                    enriched["sourceFile"] = str(path)
                inventory_by_name[name] = enriched
        for edge in report.get("subJobInfo", []):
            key = (edge.get("parent"), edge.get("child"), edge.get("activity", ""))
            if key not in subjobs_by_key:
                merged = dict(edge)
                merged["count"] = int(edge.get("count", 1) or 1)
                subjobs_by_key[key] = merged
            else:
                subjobs_by_key[key]["count"] = max(subjobs_by_key[key]["count"], int(edge.get("count", 1) or 1))
        for table, rows in LIST_TABLES.items():
            for row in report.get(table, []):
                if isinstance(row, dict):
                    table_rows[table][key_for(table, row)] = dict(row)

    inventory = [inventory_by_name[name] for name in sorted(inventory_by_name)]
    types = Counter(str(x.get("type", "")) for x in inventory)
    complexity = Counter(str(x.get("complexityLevel", "")) for x in inventory)
    total_nodes = sum(int(x.get("totalNodes", 0) or sum(x.get("nodes", {}).values())) for x in inventory)
    subjobs = [subjobs_by_key[k] for k in sorted(subjobs_by_key, key=lambda x: tuple(str(v) for v in x))]
    transformation_counts: Counter[str] = Counter()
    for row in table_rows["jobsTransformationsList"].values():
        transformation_counts[str(row.get("transformation", ""))] += int(row.get("count", 0) or 0)
    embedded_counts: Counter[str] = Counter()
    for row in table_rows["embeddedSQLPrograms"].values():
        embedded_counts[str(row.get("job", ""))] += 1
    embedded_summary = [{"job": job, "statementCount": count} for job, count in sorted(embedded_counts.items())]

    return {
        "runInfo": {
            "sourceTechnology": reports[0][1].get("runInfo", {}).get("sourceTechnology", "Datastage"),
            "startTime": min(starts) if starts else now_utc(),
            "mergedAt": now_utc(),
            "runDurationSeconds": None,
            "reportName": "merged",
            "engine": "merge_reports.py",
            "sourceEngines": sorted(engines),
            "sources": sources,
        },
        "summary": {
            "totalItems": len(inventory), "totalNodes": total_nodes,
            "byType": dict(sorted(types.items())), "byComplexity": dict(sorted(complexity.items())),
            "subjobLinks": len(subjobs),
        },
        "inventory": inventory,
        "subJobInfo": subjobs,
        "transformations": [{"transformation": k, "count": v} for k, v in sorted(transformation_counts.items())],
        **{table: list(rows.values()) for table, rows in table_rows.items()},
        "embeddedSQLSummary": embedded_summary,
    }


def workbook(report: dict[str, Any]) -> list[tuple[str, list[list[Any]]]]:
    inventory = report["inventory"]
    return [
        ("Summary", [
            ["Metric", "Value"], ["Total items", report["summary"]["totalItems"]],
            ["Total nodes", report["summary"]["totalNodes"]],
            ["Subjob links", report["summary"].get("subjobLinks", len(report.get("subJobInfo", [])))],
            *[[f"Type: {k}", v] for k, v in report["summary"]["byType"].items()],
            *[[f"Complexity: {k}", v] for k, v in report["summary"]["byComplexity"].items()],
            ["Sources", "\n".join(report["runInfo"].get("sources", []))],
        ]),
        ("Job Details", [["Job Name", "Folder", "Source File", "Included", "Job Type", "Categorization",
                          "Number of Nodes", "Checksum Extended"]] + [
            [x.get("name"), x.get("folder"), x.get("sourceFile"), "YES", x.get("type"),
             x.get("complexityLevel"), x.get("totalNodes"), x.get("checksumExtended", "")] for x in inventory]),
        ("Transformations", [["Transformation", "Count"]] + [
            [x.get("transformation"), x.get("count")] for x in report.get("transformations", [])]),
        ("Jobs Transformations List", [LIST_TABLES["jobsTransformationsList"]] + [
            [row.get(k) for k in LIST_TABLES["jobsTransformationsList"]]
            for row in report.get("jobsTransformationsList", [])]),
        ("Subjob Info", [["Parent", "Child", "Parent Type", "Child Type", "Count", "Activity"]] + [
            [x.get("parent"), x.get("child"), x.get("parentType"), x.get("childType"),
             x.get("count"), x.get("activity", "")] for x in report.get("subJobInfo", [])]),
        ("Item Node Info", [LIST_TABLES["itemNodeInfo"]] + [
            [row.get(k) for k in LIST_TABLES["itemNodeInfo"]] for row in report.get("itemNodeInfo", [])]),
        ("Job Parameters", [LIST_TABLES["jobParameters"]] + [
            [row.get(k) for k in LIST_TABLES["jobParameters"]] for row in report.get("jobParameters", [])]),
        ("Transformation Expressions", [LIST_TABLES["transformationExpressions"]] + [
            [row.get(k) for k in LIST_TABLES["transformationExpressions"]]
            for row in report.get("transformationExpressions", [])]),
        ("Misc Job Attributes", [LIST_TABLES["miscJobAttributes"]] + [
            [row.get(k) for k in LIST_TABLES["miscJobAttributes"]]
            for row in report.get("miscJobAttributes", [])]),
        ("Embedded SQL Summary", [["Job", "Statement Count"]] + [
            [x.get("job"), x.get("statementCount")] for x in report.get("embeddedSQLSummary", [])]),
        ("Embedded SQL Programs", [LIST_TABLES["embeddedSQLPrograms"]] + [
            [row.get(k) for k in LIST_TABLES["embeddedSQLPrograms"]]
            for row in report.get("embeddedSQLPrograms", [])]),
    ]


def run(args: argparse.Namespace) -> int:
    reports = load_reports(args.reports_dir, args.glob)
    merged = merge(reports)
    json_path = args.out_prefix.with_suffix(".json")
    xlsx_path = args.out_prefix.with_suffix(".xlsx")
    atomic_json(json_path, merged)
    write_xlsx(xlsx_path, workbook(merged))
    if args.csv:
        rows = [["Name", "Folder", "Type", "Complexity", "TotalNodes", "SourceFile"]]
        rows += [[x.get("name"), x.get("folder"), x.get("type"), x.get("complexityLevel"),
                  x.get("totalNodes"), x.get("sourceFile")] for x in merged["inventory"]]
        args.out_prefix.with_suffix(".inventory.csv").write_text(rows_to_csv(rows), encoding="utf-8")
    print(f"Merged {len(reports)} reports -> {len(merged['inventory'])} unique items, "
          f"{merged['summary']['totalNodes']} nodes")
    print(f"  JSON:  {json_path}")
    print(f"  Excel: {xlsx_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
