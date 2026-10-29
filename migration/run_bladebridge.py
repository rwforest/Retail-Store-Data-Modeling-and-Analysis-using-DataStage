"""Run Lakebridge's BladeBridge transpiler (DataStage XML -> Databricks PySpark) over migration/xml.

    python -I migration/run_bladebridge.py [PYSPARK|SPARKSQL]
"""
import asyncio, email, email.policy, sys
from pathlib import Path
from databricks.labs.bladebridge.transpiler import Transpiler

target = sys.argv[1] if len(sys.argv) > 1 else "PYSPARK"
root = Path("migration")
out = root / "transpiled" / target.lower()
out.mkdir(parents=True, exist_ok=True)
t = Transpiler("DATASTAGE", target)


async def main():
    ok = fail = 0
    for f in sorted((root / "xml").glob("*.xml")):
        edits, diags = await t.transpile(f.name, f.read_text(encoding="utf-8"))
        if diags:
            fail += 1
            print("FAIL", f.name, diags[0].code, diags[0].message[:300])
        else:
            ok += 1
            msg = email.message_from_string(edits[0].new_text, policy=email.policy.default)
            parts = list(msg.iter_attachments()) if msg.is_multipart() else [msg]
            d = out / f.stem
            d.mkdir(exist_ok=True)
            for p in parts:
                name = Path(p.get_filename() or f.stem + ".py").name
                (d / name).write_text(p.get_content())
    print(f"{target}: ok={ok} fail={fail}")

asyncio.run(main())
