#!/usr/bin/env python3
"""Run BladeBridge's dbxconv converter directly, offline (no Databricks CLI, login or workspace).

    python migration/run_dbxconv.py <bladebridge plugin dir> <input dir of .xml> <output dir> [PYSPARK|SPARKSQL|DBSQL]

<bladebridge plugin dir> is an extracted databricks_bb_plugin wheel (the folder containing databricks/labs/bladebridge).
Use a Python whose version has a pyarmor runtime in that wheel (3.10-3.12 or 3.14 on macOS arm64): dbxconv requires a
handshake hash (-H) that only the wheel's hasher can compute. Each input is converted on its own, in its own work
directory; dbxconv writes notebooks under <output>/<file stem>/out/ following the job's Category path.
"""
import os
import shutil
import subprocess
import sys
from pathlib import Path

plugin, src, dst = (Path(a).resolve() for a in sys.argv[1:4])
target = sys.argv[4] if len(sys.argv) > 4 else "PYSPARK"
sys.path.insert(0, str(plugin))
from databricks.labs.bladebridge.hasher import compute_hash  # noqa: E402

conv = plugin / "databricks/labs/bladebridge/Converter"
binary = conv / "bin" / {"darwin": "MacOS/dbxconv", "linux": "Linux/dbxconv", "win32": "Windows/dbxconv.exe"}[sys.platform]
# DBSQL = Databricks SQL scripts (Converter/Configs/Datastage/README.txt: -M CodeGeneration::SQL -u datastage2dbsql.json)
config, extra = {"PYSPARK": ("base_datastage2databricks_pyspark.json", []),
                 "SPARKSQL": ("base_datastage2databricks_sparksql.json", []),
                 "DBSQL": ("datastage2dbsql.json", ["-M", "CodeGeneration::SQL"])}[target]
env = dict(os.environ, BB_CONFIG_CONVERTER_DIR=str(conv / "Configs"), UTF8_NOT_SUPPORTED="1")

for f in sorted(src.glob("*.xml")):
    work = dst / f.stem
    shutil.rmtree(work, ignore_errors=True)
    (work / "originals").mkdir(parents=True)
    (work / "out").mkdir()  # dbxconv requires the output folder to exist
    shutil.copy(f, work / "originals" / f.name)
    args = [str(binary), "DATASTAGE", "-u", config, *extra, "-n", "out", "-i", f"originals/{f.name}"]
    args += ["-H", compute_hash(args)]
    r = subprocess.run(args, cwd=work, env=env, capture_output=True, text=True)
    (work / "dbxconv.log").write_text(r.stdout + r.stderr)
    nbs = [p for p in (work / "out").rglob("*") if p.is_file() and p.suffix in (".py", ".sql", ".json")
           and p.name != "databricks_conversion_supplements.py"]
    print(f"{f.stem:40} rc={r.returncode} notebooks={len(nbs)}"
          + "".join(f"\n    {p.relative_to(work / 'out')}" for p in nbs), flush=True)
