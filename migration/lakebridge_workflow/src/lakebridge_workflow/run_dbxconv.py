#!/usr/bin/env python3
"""Run BladeBridge ``dbxconv`` offline for one XML file or a folder of XML files.

This is a literal lift-and-shift: ``dbxconv`` converts each job stage by stage with no
re-architecture. For optimization opportunities (truncate-and-reload writes, hand-rolled
SCD2, duplicate queries, missing filters), run ``lakebridge-workflow optimize`` separately
over the analyzer report; it does not change what this converter produces.

Usage (current, plugin option)::

    lakebridge-workflow run-dbxconv --plugin-dir bb_plugin \\
        migrated_batches/batch_001/xml migrated_batches/batch_001/bladebridge_dbsql DBSQL

Legacy positional form is also accepted::

    lakebridge-workflow run-dbxconv bb_plugin input_dir output_dir DBSQL

Each job is converted in an isolated work folder:
``<output>/<job>/originals/<job>.xml``, ``<output>/<job>/out/``, and
``<output>/<job>/dbxconv.log``. The process exits nonzero if any conversion fails.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

TARGETS = ("PYSPARK", "SPARKSQL", "DBSQL")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Offline BladeBridge dbxconv runner",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("paths", nargs="*", help="Legacy [plugin] input output [target], or input output [target]")
    p.add_argument("--plugin-dir", type=Path, default=Path("bb_plugin"),
                   help="Manually unpacked databricks_bb_plugin wheel root; not installed by "
                        "the workflow package")
    p.add_argument("--target", choices=TARGETS, help="Conversion target (default DBSQL)")
    p.add_argument("--fail-fast", action="store_true", help="Stop at first failed conversion")
    p.add_argument("--continue-on-error", action="store_true",
                   help="Compatibility alias; process all jobs before returning nonzero (default behavior)")
    p.add_argument("--quiet", action="store_true", help="Suppress generated notebook path details")
    args = p.parse_args(argv)
    if args.fail_fast and args.continue_on_error:
        p.error("--fail-fast and --continue-on-error are mutually exclusive")

    values = list(args.paths)
    plugin = args.plugin_dir
    target = args.target

    # Legacy form: plugin input output [target].
    if values and Path(values[0]).is_dir() and (Path(values[0]) / "databricks/labs/bladebridge").is_dir():
        plugin = Path(values.pop(0))

    if len(values) == 2:
        source, output = map(Path, values)
    elif len(values) == 3:
        source, output = map(Path, values[:2])
        if target is not None:
            p.error("target supplied twice")
        target = values[2]
    else:
        p.error("expected INPUT OUTPUT [TARGET], optionally preceded by PLUGIN_DIR")

    args.source, args.output, args.plugin_dir = source, output, plugin
    args.target = (target or "DBSQL").upper()
    if args.target not in TARGETS:
        p.error(f"invalid target {args.target}; choose one of {', '.join(TARGETS)}")
    return args


CONFIG_BY_TARGET = {
    "PYSPARK": "base_datastage2databricks_pyspark.json",
    "SPARKSQL": "base_datastage2databricks_sparksql.json",
    "DBSQL": "datastage2dbsql.json",
}


def plugin_paths(plugin: Path) -> tuple[Path, Path, str]:
    root = plugin.resolve()
    bladebridge = root / "databricks" / "labs" / "bladebridge"
    converter = bladebridge / "Converter"
    binaries = {
        "darwin": converter / "bin" / "MacOS" / "dbxconv",
        "linux": converter / "bin" / "Linux" / "dbxconv",
        "win32": converter / "bin" / "Windows" / "dbxconv.exe",
    }
    binary = binaries.get(sys.platform)
    if binary is None or not bladebridge.is_dir() or not binary.is_file():
        raise SystemExit(
            f"error: BladeBridge plugin or dbxconv binary not found under {root} "
            f"(expected {binary if binary else '<platform binary>'})"
        )
    return root, converter, str(binary)


def validate_environment() -> None:
    if not ((3, 10) <= sys.version_info[:2] < (3, 15)):
        raise SystemExit(
            "error: BladeBridge's pyarmor runtime requires Python 3.10-3.14; "
            "run with: lakebridge-workflow run-dbxconv ..."
        )


def load_hasher(plugin_root: Path):
    sys.path.insert(0, str(plugin_root))
    try:
        from databricks.labs.bladebridge.hasher import compute_hash  # type: ignore
    except Exception as exc:
        raise SystemExit(f"error: cannot import BladeBridge hasher from {plugin_root}: {exc}") from exc
    return compute_hash


def xml_files(source: Path) -> list[Path]:
    if source.is_file():
        if source.suffix.lower() != ".xml":
            raise SystemExit(f"error: input is not an XML file: {source}")
        return [source]
    if source.is_dir():
        files = sorted(source.glob("*.xml"))
        if files:
            return files
        raise SystemExit(f"error: no .xml files found in {source}")
    raise SystemExit(f"error: input not found: {source}")


def convert_one(source: Path, output_root: Path, plugin_root: Path, converter: Path,
                binary: str, compute_hash, target: str) -> tuple[int, list[Path]]:
    work = output_root / source.stem
    shutil.rmtree(work, ignore_errors=True)
    originals = work / "originals"
    out_dir = work / "out"
    originals.mkdir(parents=True)
    out_dir.mkdir()
    shutil.copy2(source, originals / source.name)

    configs = converter / "Configs"
    config_file = configs / CONFIG_BY_TARGET[target]
    if not config_file.is_file():
        raise SystemExit(f"error: BladeBridge config not found: {config_file}")
    args = [
        binary, "DATASTAGE", "-u", config_file.name,
        *(["-M", "CodeGeneration::SQL"] if target == "DBSQL" else []),
        "-n", "out", "-i", f"originals/{source.name}",
    ]
    args += ["-H", compute_hash(args)]
    env = dict(os.environ, BB_CONFIG_CONVERTER_DIR=str(configs), UTF8_NOT_SUPPORTED="1")
    result = subprocess.run(args, cwd=work, env=env, capture_output=True, text=True)
    (work / "dbxconv.log").write_text(result.stdout + result.stderr, encoding="utf-8")
    notebooks = [
        path for path in out_dir.rglob("*")
        if path.is_file() and path.suffix in (".py", ".sql", ".json")
        and path.name != "databricks_conversion_supplements.py"
    ]
    return result.returncode, notebooks


def run(args: argparse.Namespace) -> int:
    validate_environment()
    plugin_root, converter, binary = plugin_paths(args.plugin_dir)
    compute_hash = load_hasher(plugin_root)
    files = xml_files(args.source)
    args.output.mkdir(parents=True, exist_ok=True)

    failures: list[tuple[Path, int]] = []
    for source in files:
        try:
            rc, notebooks = convert_one(source, args.output, plugin_root, converter,
                                        binary, compute_hash, args.target)
        except Exception as exc:
            failures.append((source, 1))
            print(f"{source.stem:<40} rc=1 error={exc}", flush=True)
            if args.fail_fast:
                break
            continue
        print(f"{source.stem:<40} rc={rc} notebooks={len(notebooks)}", flush=True)
        if not args.quiet:
            for notebook in notebooks:
                try:
                    print(f"    {notebook.relative_to(args.output / source.stem / 'out')}", flush=True)
                except ValueError:
                    print(f"    {notebook}", flush=True)
        if rc != 0:
            failures.append((source, rc))
            if args.fail_fast:
                break

    if failures:
        print(f"dbxconv finished with {len(failures)} failed job(s):", file=sys.stderr)
        for source, rc in failures:
            print(f"  {source.name}: rc={rc}", file=sys.stderr)
        return 1
    print(f"dbxconv completed {len(files)} job(s) successfully")
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
