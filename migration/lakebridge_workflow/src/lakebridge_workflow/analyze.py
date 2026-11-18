#!/usr/bin/env python3
# /// script
# dependencies = []
# ///
"""Lakebridge Analyzer runner with an offline, repository-aware fallback.

For a large genuine DSExport, install/use the official analyzer (``databricks-bb-analyzer``).
When it is unavailable—as in this repository, which has no source XML/TXT export—the script
analyzes the already-condensed ``switch_input/*.txt`` jobs and emits the same useful JSON and
multi-tab Excel structures. It also reads the checked-in authoritative analyzer metrics from
``analysis/lakebridge-analyzer.xlsx`` when job names match.

Examples
--------
Analyze the jobs in this repository::

    lakebridge-workflow analyze --source-directory switch_input \\
        --report-file consolidated_all_jobs.xlsx --source-tech Datastage --generate-json

Analyze one XML chunk with official analyzer if installed, otherwise local fallback::

    lakebridge-workflow analyze --source-directory seq_only_chunks/chunk_0001.xml \\
        --report-file report_chunk_0001.xlsx --source-tech Datastage --generate-json
"""
from __future__ import annotations

import argparse
import shlex
import shutil
import subprocess
import time
from collections import Counter, defaultdict
from pathlib import Path

from .paths import find_migration_root
from .common import (
    CondensedJob, atomic_json, complexity_from_nodes, csv_escape, inferred_subjob_edges,
    iter_condensed_jobs, now_utc, read_condensed_job, read_xlsx_rows, write_xlsx,
)

SUPPORTED_TECHNOLOGIES = ["Datastage"]
AUTHORITATIVE_REPORT: Path | None = None


def authoritative_report_path() -> Path:
    if AUTHORITATIVE_REPORT is not None:
        return AUTHORITATIVE_REPORT
    try:
        return find_migration_root() / "analysis" / "lakebridge-analyzer.xlsx"
    except RuntimeError:
        return Path("analysis") / "lakebridge-analyzer.xlsx"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run or locally emulate Lakebridge Analyzer")
    p.add_argument("--list-tech", action="store_true", help="List supported source technologies")
    p.add_argument("--source-directory", type=Path, help="Condensed .txt directory/file, XML directory/file")
    p.add_argument("--report-file", type=Path, default=Path("report.xlsx"), help="Excel report path")
    p.add_argument("--source-tech", default="Datastage", help="Source technology (Datastage)")
    p.add_argument("--generate-json", action="store_true", help="Also write a JSON report")
    p.add_argument("--json-file", type=Path, help="Explicit JSON report path (default: report stem + .json)")
    p.add_argument("--csv", action="store_true", help="Also write a job-inventory CSV")
    p.add_argument("--offline-fallback", action="store_true",
                   help="Use the local analyzer even if an official executable is found")
    p.add_argument("--no-fallback", action="store_true", help="Fail instead of using the local analyzer")
    p.add_argument("--external-command", help="Official analyzer command with {source}, {report}, and optional {json} placeholders")
    args = p.parse_args(argv)
    if not args.list_tech and args.source_directory is None:
        p.error("--source-directory is required unless --list-tech is used")
    args.source_tech = args.source_tech.strip().lower().replace(" ", "")
    if args.source_tech != "datastage":
        p.error("unsupported --source-tech; use the exact supported value 'Datastage'")
    args.source_tech = "Datastage"
    if args.no_fallback and args.offline_fallback:
        p.error("--no-fallback and --offline-fallback are mutually exclusive")
    return args


def read_authoritative_metrics(path: Path | None = None) -> dict[str, dict[str, object]]:
    path = path or authoritative_report_path()
    """Read the checked-in official analyzer's Job Details tab."""
    if not path.is_file():
        return {}
    try:
        rows = read_xlsx_rows(path, "Job Details")["Job Details"]
    except (OSError, KeyError, ValueError):
        return {}
    if not rows:
        return {}
    header = [str(x or "") for x in rows[0]]
    idx = {name: i for i, name in enumerate(header)}
    out: dict[str, dict[str, object]] = {}
    for row in rows[1:]:
        def cell(name: str) -> object:
            i = idx.get(name, -1)
            return row[i] if 0 <= i < len(row) else ""
        name = str(cell("Job Name") or "")
        if not name:
            continue
        try:
            nodes = int(cell("Number of Nodes") or 0)
        except (TypeError, ValueError):
            nodes = 0
        out[name] = {
            "folder": str(cell("Folder") or ""),
            "sourceFile": str(cell("Source File") or ""),
            "complexityLevel": str(cell("Categorization") or ""),
            "totalNodes": nodes,
            "checksumExtended": str(cell("Checksum Extended") or ""),
        }
    return out


def read_authoritative_xref(path: Path | None = None) -> list[dict[str, object]]:
    path = path or authoritative_report_path()
    """Read the official analyzer's Jobs Transformations List tab."""
    if not path.is_file():
        return []
    try:
        rows = read_xlsx_rows(path, "Jobs Transformations List")["Jobs Transformations List"]
    except (OSError, KeyError, ValueError):
        return []
    if not rows:
        return []
    header = [str(x or "") for x in rows[0]]
    return [{str(k): v for k, v in zip(header, row + [""] * (len(header) - len(row)))}
            for row in rows[1:] if any(v not in (None, "") for v in row)]


def condensed_inventory(source: Path) -> tuple[list[CondensedJob], list[dict[str, object]]]:
    jobs: list[CondensedJob]
    if source.is_file():
        jobs = [read_condensed_job(source)]
    else:
        jobs = list(iter_condensed_jobs(source, "*.txt"))
    if not jobs:
        raise SystemExit(f"error: no condensed DataStage jobs found under {source}")
    metrics = read_authoritative_metrics()
    inventory: list[dict[str, object]] = []
    for job in jobs:
        known = metrics.get(job.name, {})
        total_nodes = int(known.get("totalNodes") or 0) or job.total_nodes
        expressions = job.expression_count
        complexity = str(known.get("complexityLevel") or "") or complexity_from_nodes(total_nodes, expressions)
        inventory.append({
            "name": job.name,
            "folder": str(known.get("folder") or job.folder),
            "fullyQualifiedName": f"{job.folder}\\{job.name}" if job.folder else job.name,
            "type": job.type,
            "complexityLevel": complexity,
            "nodes": job.nodes,
            "totalNodes": total_nodes,
            "jobParameters": [
                {"name": p["name"], "type": "STRING", "default": p["default"], "help": p["prompt"]}
                for p in job.parameters
            ],
            "sourceFile": str(job.source_path) if job.source_path else "",
            "checksumExtended": str(known.get("checksumExtended") or ""),
        })
    return jobs, inventory


def local_analyze(source: Path, report_file: Path, json_file: Path | None,
                  csv_flag: bool) -> tuple[Path, Path | None, dict[str, object]]:
    jobs, inventory = condensed_inventory(source)
    by_name = {j.name: j for j in jobs}
    inv_by_name = {x["name"]: x for x in inventory}  # type: ignore[index]
    subjobs = inferred_subjob_edges(jobs)

    authoritative_xref = read_authoritative_xref()
    xref_by_job: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
    for row in authoritative_xref:
        xref_by_job[str(row.get("Job", ""))].append(row)
    transformations: Counter[str] = Counter()
    job_transformations: list[dict[str, object]] = []
    item_nodes: list[dict[str, object]] = []
    parameters: list[dict[str, object]] = []
    expressions: list[dict[str, object]] = []
    attributes: list[dict[str, object]] = []
    embedded_sql: list[dict[str, object]] = []

    for job in jobs:
        stage_kinds = Counter(st.kind for st in job.stages)
        official_rows = xref_by_job.get(job.name, [])
        if official_rows:
            stage_kinds = Counter({str(x.get("Transformation", "")): int(x.get("Count") or 0)
                                   for x in official_rows})
            inv_by_name[job.name]["nodes"] = dict(stage_kinds)  # type: ignore[index]
        for kind, count in sorted(stage_kinds.items()):
            job_transformations.append({"job": job.name, "transformation": kind, "count": count})
            transformations[kind] += count
        for order, st in enumerate(job.stages, 1):
            item_nodes.append({
                "job": job.name, "node": st.name, "nodeType": st.kind,
                "conformedType": st.kind, "order": order,
            })
            for key, value in st.properties:
                attributes.append({"job": job.name, "stage": st.name, "name": key, "value": value})
                if "select " in value.lower() or "insert " in value.lower():
                    embedded_sql.append({"job": job.name, "stage": st.name, "sql": value})
            for lk in st.links:
                for key, value in lk.properties:
                    attributes.append({"job": job.name, "stage": st.name, "link": lk.link_name,
                                       "name": key, "value": value})
                for col in lk.columns:
                    if col.get("derivation"):
                        expressions.append({"job": job.name, "transformation": st.kind,
                                            "field": col["name"], "expression": col["derivation"]})
        for p in job.parameters:
            parameters.append({"job": job.name, "name": p["name"], "type": "STRING", "default": p["default"], "help": p["prompt"]})

    type_counts = Counter(x["type"] for x in inventory)  # type: ignore[arg-type]
    complexity_counts = Counter(x["complexityLevel"] for x in inventory)  # type: ignore[arg-type]
    started = time.time()
    report = {
        "runInfo": {
            "sourceTechnology": "Datastage",
            "startTime": now_utc(),
            "runDurationSeconds": round(time.time() - started, 3),
            "reportName": str(report_file),
            "engine": "offline-condensed-fallback",
            "source": str(source),
        },
        "summary": {
            "totalItems": len(inventory),
            "totalNodes": sum(int(x["totalNodes"]) for x in inventory),  # type: ignore[arg-type]
            "byType": dict(sorted(type_counts.items())),
            "byComplexity": dict(sorted(complexity_counts.items())),
            "totalSubjobLinks": len(subjobs),
        },
        "inventory": inventory,
        "subJobInfo": subjobs,
        "transformations": [{"transformation": k, "count": v} for k, v in sorted(transformations.items())],
        "jobsTransformationsList": job_transformations,
        "itemNodeInfo": item_nodes,
        "jobParameters": parameters,
        "transformationExpressions": expressions,
        "miscJobAttributes": attributes,
        "embeddedSQLSummary": [
            {"job": x["job"], "statementCount": 1} for x in embedded_sql
        ],
        "embeddedSQLPrograms": embedded_sql,
    }

    report_file.parent.mkdir(parents=True, exist_ok=True)
    write_xlsx(report_file, [
        ("Summary", [
            ["Metric", "Value"], ["Total items", report["summary"]["totalItems"]],
            ["Total nodes", report["summary"]["totalNodes"]],
            *[[f"Type: {k}", v] for k, v in report["summary"]["byType"].items()],
            *[[f"Complexity: {k}", v] for k, v in report["summary"]["byComplexity"].items()],
            ["Engine", report["runInfo"]["engine"]],
        ]),
        ("Job Details", [[
            "Job Name", "Folder", "Source File", "Included", "Job Type", "Categorization",
            "Number of Nodes", "Checksum Extended"
        ]] + [[
            x["name"], x["folder"], x["sourceFile"], "YES", x["type"],
            x["complexityLevel"], x["totalNodes"], x["checksumExtended"]
        ] for x in inventory]),
        ("Transformations", [["Transformation", "Count"]] + [
            [x["transformation"], x["count"]] for x in report["transformations"]]),
        ("Jobs Transformations List", [["Job", "Transformation", "Count"]] + [
            [x["job"], x["transformation"], x["count"]] for x in job_transformations]),
        ("Subjob Info", [["Parent Item", "Child Item", "Parent Type", "Child Type", "Count", "Activity"]] + [
            [x["parent"], x["child"], x["parentType"], x["childType"], x["count"], x.get("activity", "")]
            for x in subjobs]),
        ("Item Node Info", [["Job", "Node Name", "Orig Node Type", "Conformed Node Type", "Node Order"]] + [
            [x["job"], x["node"], x["nodeType"], x["conformedType"], x["order"]] for x in item_nodes]),
        ("Job Parameters", [["Job", "Name", "Type", "Default", "Help"]] + [
            [x["job"], x["name"], x["type"], x["default"], x["help"]] for x in parameters]),
        ("Transformation Expressions", [["Job", "Transformation", "Field", "Expression"]] + [
            [x["job"], x["transformation"], x["field"], x["expression"]] for x in expressions]),
        ("Misc Job Attributes", [["Job", "Stage", "Link", "Name", "Value"]] + [
            [x["job"], x["stage"], x.get("link", ""), x["name"], x["value"]] for x in attributes]),
        ("Embedded SQL Summary", [["Job", "Statement Count"]] + [
            [x["job"], x["statementCount"]] for x in report["embeddedSQLSummary"]]),
        ("Embedded SQL Programs", [["Job", "Stage", "SQL"]] + [
            [x["job"], x["stage"], x["sql"]] for x in embedded_sql]),
    ])

    json_path: Path | None = None
    if json_file is not None:
        atomic_json(json_file, report)
        json_path = json_file
    if csv_flag:
        csv_path = report_file.with_suffix(".inventory.csv")
        csv_path.write_text(
            "Name,Folder,Type,Complexity,TotalNodes,SourceFile\n" +
            "\n".join(",".join(csv_escape(v) for v in row) for row in (
                [x["name"], x["folder"], x["type"], x["complexityLevel"], x["totalNodes"], x["sourceFile"]]
                for x in inventory)) + "\n", encoding="utf-8")
    return report_file, json_path, report


def discovered_external_command() -> str | None:
    """Return the official analyzer command when its console script is on PATH."""
    executable = shutil.which("databricks-bb-analyzer")
    if executable is None:
        return None
    # Keep the command as a placeholder string so users can override this exact shape.
    return (
        f"{subprocess.list2cmdline([executable])} "
        "--source-directory {source} --report-file {report} "
        "--source-tech Datastage --generate-json"
    )


def external_analyze(args: argparse.Namespace, source: Path, report_file: Path,
                     json_file: Path | None) -> int:
    """Run a user-supplied official analyzer command without guessing its release CLI.

    Placeholders keep the wrapper portable across analyzer versions::

        --external-command "analyzer.exe -i {source} -o {report} -j {json}"
    """
    if not args.external_command:
        return 2
    command = [part.format(source=str(source), report=str(report_file),
                           json=str(json_file or "")) for part in shlex.split(args.external_command)]
    result = subprocess.run(command, check=False)
    if result.returncode == 0:
        if not report_file.is_file():
            raise SystemExit(f"error: external analyzer did not create {report_file}")
        if json_file is not None and not json_file.is_file():
            raise SystemExit(f"error: external analyzer did not create {json_file}")
    return result.returncode

def run(args: argparse.Namespace) -> int:
    if args.list_tech:
        print("Supported source technologies:")
        for tech in SUPPORTED_TECHNOLOGIES:
            print(f"  {tech}")
        return 0
    source = args.source_directory
    if not source.exists():
        raise SystemExit(f"error: source not found: {source}")
    report_file = args.report_file
    if report_file.suffix.lower() != ".xlsx":
        report_file = report_file.with_suffix(".xlsx")
    json_file = args.json_file or (report_file.with_suffix(".json") if args.generate_json else None)

    txt_files = list(source.glob("*.txt")) if source.is_dir() else []
    xml_files = list(source.glob("*.xml")) if source.is_dir() else []
    if txt_files and xml_files:
        raise SystemExit(
            "error: source directory contains both condensed .txt jobs and .xml chunks; "
            "analyze them in separate directories"
        )
    is_condensed = (source.is_file() and source.suffix.lower() == ".txt") or bool(txt_files)
    if is_condensed and args.no_fallback:
        raise SystemExit(
            "error: --no-fallback cannot be used with condensed .txt jobs; the official analyzer scans .xml"
        )
    if not is_condensed:
        if args.offline_fallback:
            raise SystemExit("error: --offline-fallback supports condensed .txt jobs, not XML chunks")
        if not args.external_command and not args.no_fallback:
            args.external_command = discovered_external_command()
        if args.external_command:
            return external_analyze(args, source, report_file, json_file)
        raise SystemExit(
            "error: no official analyzer executable was found for XML chunks. "
            "Install databricks-bb-analyzer, put databricks-bb-analyzer on PATH, or supply "
            "--external-command. For this repository's available condensed jobs, use "
            "--source-directory switch_input."
        )

    xlsx, jsonp, report = local_analyze(source, report_file, json_file, args.csv)
    print(f"Analyzed {report['summary']['totalItems']} items, {report['summary']['totalNodes']} nodes")
    print(f"  Excel: {xlsx}")
    if jsonp:
        print(f"  JSON:  {jsonp}")
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
