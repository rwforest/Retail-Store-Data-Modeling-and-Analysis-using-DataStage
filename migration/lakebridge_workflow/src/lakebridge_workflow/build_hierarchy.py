#!/usr/bin/env python3
"""Build the sequence-to-ETL execution hierarchy from a consolidated analyzer report.

The hierarchy answers the migration's first planning questions: which sequences are
root orchestrators, which ETL jobs are actually called, which jobs are orphans or
backups, and how deep the execution graph is.

Example::

    lakebridge-workflow build-hierarchy --input-json consolidated_all_jobs.json --out-prefix job_hierarchy
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any

from .common import atomic_json, is_backup_name, now_utc, write_xlsx

ROLE_ROOT = "1_ROOT_SEQUENCE"
ROLE_SUB = "2_SUB_SEQUENCE"
ROLE_LEAF_SEQ = "3_LEAF_SEQUENCE"
ROLE_ACTIVE_ETL = "4_ACTIVE_ETL_JOB"
ROLE_ORPHAN_SEQ = "5_ORPHAN_SEQUENCE"
ROLE_ORPHAN_ETL = "6_ORPHAN_ETL_JOB"
ROLE_ROUTINE = "7_ROUTINE"
ROLE_MISSING = "8_MISSING_EXTERNAL_CHILD_REF"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input-json", type=Path, required=True, help="consolidated analyzer JSON report")
    p.add_argument("--out-prefix", type=Path, default=Path("job_hierarchy"), help="output path prefix")
    p.add_argument("--max-paths", type=int, default=0,
                   help="stop path enumeration after N root-to-leaf paths (0 = unlimited)")
    return p.parse_args(argv)


def build(input_json: Path, max_paths: int = 0) -> dict[str, Any]:
    report = json.loads(input_json.read_text(encoding="utf-8"))
    inventory = report.get("inventory", [])
    if not inventory:
        raise RuntimeError(f"no inventory records in {input_json}")
    by_name = {str(item.get("name")): item for item in inventory if item.get("name")}
    subjobs = report.get("subJobInfo", [])
    children: dict[str, list[str]] = defaultdict(list)
    parents: dict[str, list[str]] = defaultdict(list)
    missing_children: set[str] = set()
    seen_edges: set[tuple[str, str]] = set()
    for edge in subjobs:
        parent, child = str(edge.get("parent", "")), str(edge.get("child", ""))
        if not parent or not child or (parent, child) in seen_edges:
            continue
        seen_edges.add((parent, child))
        children[parent].append(child)
        parents[child].append(parent)
        if child not in by_name:
            missing_children.add(child)

    # Unique, deterministic child order. Keep repeated call counts in the edge source;
    # planning only needs whether a dependency exists.
    for parent in children:
        children[parent] = list(dict.fromkeys(children[parent]))

    def is_sequence(name: str) -> bool:
        return str(by_name.get(name, {}).get("type", "")).upper() == "SEQUENCE"

    def is_routine(name: str) -> bool:
        return str(by_name.get(name, {}).get("type", "")).upper() == "ROUTINE"

    referenced = set(parents)
    roles: dict[str, str] = {}
    for name, item in by_name.items():
        typ = str(item.get("type", "")).upper()
        has_children = bool(children.get(name))
        called = name in referenced
        if is_routine(name):
            roles[name] = ROLE_ROUTINE
        elif typ == "SEQUENCE":
            if not called and has_children:
                roles[name] = ROLE_ROOT
            elif not called and not has_children:
                roles[name] = ROLE_ORPHAN_SEQ
            elif called and has_children:
                roles[name] = ROLE_SUB
            else:
                roles[name] = ROLE_LEAF_SEQ
        else:
            roles[name] = ROLE_ACTIVE_ETL if called else ROLE_ORPHAN_ETL

    # A sequence called by a non-sequence still behaves as a root orchestration tree.
    for name in by_name:
        if is_sequence(name) and all(not is_sequence(p) for p in parents.get(name, [])) and children.get(name):
            if roles[name] == ROLE_SUB:
                roles[name] = ROLE_ROOT

    # Reachable descendants with cycle protection.
    def descendants(root: str) -> tuple[list[str], int]:
        found: dict[str, int] = {}
        queue: deque[tuple[str, int]] = deque([(root, 0)])
        while queue:
            node, depth = queue.popleft()
            for child in children.get(node, []):
                if child not in by_name or child in found:
                    continue
                found[child] = depth + 1
                if depth + 1 < 200:  # hard cycle guard; real sequence graphs are much shallower
                    queue.append((child, depth + 1))
        order = sorted(found, key=lambda n: (found[n], n))
        return order, max(found.values(), default=0)

    root_names = sorted(name for name, role in roles.items() if role == ROLE_ROOT)
    root_trees: list[dict[str, Any]] = []
    for root in root_names:
        item = by_name[root]
        desc, depth = descendants(root)
        root_trees.append({
            "name": root, "role": roles[root], "type": item.get("type"),
            "complexity": item.get("complexityLevel"), "folder": item.get("folder"),
            "totalNodes": item.get("totalNodes", 0), "maxDepth": depth,
            "directChildren": children.get(root, []),
            "descendants": desc,
            "downstreamSequences": [n for n in desc if is_sequence(n)],
            "downstreamETLJobs": [n for n in desc if not is_sequence(n)],
            "downstreamNodeCount": sum(int(by_name[n].get("totalNodes", 0) or 0) for n in desc if n in by_name),
        })

    paths: list[dict[str, Any]] = []
    path_limit_exceeded = False
    for root in root_names:
        stack: list[tuple[str, list[str]]] = [(root, [root])]
        while stack:
            node, path = stack.pop()
            next_nodes = children.get(node, [])
            if not next_nodes:
                paths.append({
                    "rootSequence": root, "depth": len(path) - 1, "path": path,
                    "leafJob": node, "leafType": by_name.get(node, {}).get("type", "MISSING"),
                    "leafComplexity": by_name.get(node, {}).get("complexityLevel", ""),
                    "leafNodes": by_name.get(node, {}).get("totalNodes", ""),
                    "leafFolder": by_name.get(node, {}).get("folder", ""),
                })
                if max_paths and len(paths) >= max_paths:
                    path_limit_exceeded = True
                    break
                continue
            for child in reversed(next_nodes):
                if child not in path and (child in by_name or child not in by_name):
                    stack.append((child, path + [child]))
            if max_paths and len(paths) >= max_paths:
                break
        if max_paths and len(paths) >= max_paths:
            break

    depth_counts = Counter(str(item["depth"]) for item in paths)
    all_jobs: list[dict[str, Any]] = []
    for name in sorted(set(by_name) | set(children) | set(parents)):
        item = by_name.get(name, {})
        ancestors: set[str] = set()
        ancestor_queue = list(parents.get(name, []))
        while ancestor_queue:
            ancestor = ancestor_queue.pop()
            if ancestor in ancestors:
                continue
            ancestors.add(ancestor)
            ancestor_queue.extend(parents.get(ancestor, []))
        root_ancestors = sorted(
            ancestor for ancestor in ancestors
            if is_sequence(ancestor) and all(not is_sequence(p) for p in parents.get(ancestor, []))
        )
        all_jobs.append({
            "rootAncestors": root_ancestors,
            "name": name, "role": roles.get(name, ROLE_MISSING if name in missing_children else ROLE_MISSING),
            "type": item.get("type", "MISSING"), "complexity": item.get("complexityLevel", ""),
            "totalNodes": item.get("totalNodes", ""), "folder": item.get("folder", ""),
            "sourceFile": item.get("sourceFile", ""), "parents": parents.get(name, []),
            "children": children.get(name, []), "backup": is_backup_name(name),
        })
    orphans = [item for item in all_jobs if item["role"] in (ROLE_ORPHAN_SEQ, ROLE_ORPHAN_ETL)]
    backups = [item for item in all_jobs if item["backup"]]
    missing = [item for item in all_jobs if item["role"] == ROLE_MISSING]
    # A backup can also be orphaned; report it once with its strongest reason.
    unique_attention: dict[str, dict[str, Any]] = {}
    for item in orphans + backups + missing:
        unique_attention.setdefault(str(item["name"]), item)

    summary = {
        "totalUniqueItems": len(by_name),
        "rootSequences": len(root_names),
        "subSequences": sum(role == ROLE_SUB for role in roles.values()),
        "leafSequences": sum(role == ROLE_LEAF_SEQ for role in roles.values()),
        "activeETLJobs": sum(role == ROLE_ACTIVE_ETL for role in roles.values()),
        "orphanSequences": sum(x["role"] == ROLE_ORPHAN_SEQ for x in orphans),
        "orphanETLJobs": sum(x["role"] == ROLE_ORPHAN_ETL for x in orphans),
        "routines": sum(role == ROLE_ROUTINE for role in roles.values()),
        "missingExternalChildRefs": len(missing),
        "rootToLeafPaths": len(paths),
        "depthDistribution": {f"Depth {k}": v for k, v in sorted(depth_counts.items())},
        "pathLimitExceeded": path_limit_exceeded,
    }
    return {
        "runInfo": {"sourceReport": str(input_json), "builtAt": now_utc(),
                    "sourceTechnology": report.get("runInfo", {}).get("sourceTechnology", "Datastage")},
        "summary": summary,
        "rootTrees": root_trees,
        "rootSequences": [root["name"] for root in root_trees],
        "lineagePaths": paths,
        "allJobs": all_jobs,
        "orphansAndBackups": list(unique_attention.values()),
    }


def workbook(hierarchy: dict[str, Any]) -> list[tuple[str, list[list[Any]]]]:
    summary = hierarchy["summary"]
    summary_rows = [["Metric", "Value"]] + [[k, v] for k, v in summary.items()]
    roots = [["Root Sequence", "Role", "Complexity", "Direct Children", "Downstream Sequences",
              "Downstream ETL Jobs", "Downstream Node Count", "Max Depth"]]
    for root in hierarchy["rootTrees"]:
        roots.append([root["name"], root["role"], root["complexity"], len(root["directChildren"]),
                      len(root["downstreamSequences"]), len(root["downstreamETLJobs"]),
                      root["downstreamNodeCount"], root["maxDepth"]])
    paths = [["Root Sequence", "Depth", "Path", "Leaf Job", "Leaf Type", "Leaf Complexity", "Leaf Nodes", "Folder"]]
    for item in hierarchy["lineagePaths"]:
        paths.append([item["rootSequence"], item["depth"], " -> ".join(item["path"]), item["leafJob"],
                      item["leafType"], item["leafComplexity"], item["leafNodes"], item["leafFolder"]])
    jobs = [["Name", "Role", "Type", "Complexity", "Total Nodes", "Folder", "Parents", "Children",
             "Root Ancestors", "Backup"]]
    for item in hierarchy["allJobs"]:
        jobs.append([item["name"], item["role"], item["type"], item["complexity"], item["totalNodes"],
                     item["folder"], ", ".join(item["parents"]), ", ".join(item["children"]),
                     ", ".join(item.get("rootAncestors", [])), item["backup"]])
    orphan = [["Name", "Role", "Type", "Complexity", "Total Nodes", "Folder", "Reason"]]
    for item in hierarchy["orphansAndBackups"]:
        reason = ("Backup name" if item["backup"] else
                  "Not referenced by any sequence" if "ORPHAN" in item["role"] else
                  "External child reference missing")
        orphan.append([item["name"], item["role"], item["type"], item["complexity"], item["totalNodes"],
                       item["folder"], reason])
    return [
        ("Hierarchy Summary", summary_rows),
        ("Root Master Sequences", roots),
        ("End-to-End Lineage Paths", paths),
        ("All Jobs Classified", jobs),
        ("Orphan & Backup Jobs", orphan),
    ]


def run(args: argparse.Namespace) -> int:
    hierarchy = build(args.input_json, args.max_paths)
    json_path = args.out_prefix.with_suffix(".json")
    xlsx_path = args.out_prefix.with_suffix(".xlsx")
    atomic_json(json_path, hierarchy)
    write_xlsx(xlsx_path, workbook(hierarchy))
    print("Hierarchy summary:")
    for key, value in hierarchy["summary"].items():
        if not isinstance(value, dict):
            print(f"  {key}: {value}")
    print(f"  JSON:  {json_path}")
    print(f"  Excel: {xlsx_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
