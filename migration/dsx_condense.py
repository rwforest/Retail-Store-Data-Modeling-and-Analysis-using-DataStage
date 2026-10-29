#!/usr/bin/env python3
"""Render a DataStage .dsx job as a compact, human-readable description for LLM conversion (Lakebridge Switch).

Raw DSX repeats every schema several times per pin (100-400 KB per job here); Switch skips files over its token
threshold. This keeps what defines the job - parameters, stages, stage properties, links, column derivations -
parsed from the export itself (no pipeline_def.py involved).

    python3 migration/dsx_condense.py "DataStage Jobs/Pipeline" migration/switch_input
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dsx_to_xml import parse  # noqa: E402

PROPS_DROPPED = {"Owner", "APTFieldProp", "Description"}


def props(node):
    return {it[1]: it[2] for it in node.items if it[0] == "prop"}


def subs(node):
    return [props(it[1]) for it in node.items if it[0] == "rec" and it[1].kind == "DSSUBRECORD"]


def sub_groups(node):
    """property name -> list of subrecord dicts that follow it (DSX: collection header, then subrecords)."""
    out, cur = {}, None
    for it in node.items:
        if it[0] == "prop":
            cur = it[1]
        elif it[0] == "rec" and it[1].kind == "DSSUBRECORD" and cur:
            out.setdefault(cur, []).append(props(it[1]))
    return out


def render(job):
    recs = {it[1].ident: it[1] for it in job.items if it[0] == "rec"}
    root = recs["ROOT"]
    rp = props(root)
    out = [f"DATASTAGE {'SEQUENCE' if rp.get('JobType') == '2' else 'PARALLEL'} JOB: {job.ident}",
           f"Description: {rp.get('ShortDescription', '')}", f"Folder: {rp.get('Category', '')}", "Parameters:"]
    for p in sub_groups(root).get("Parameters", []):
        out.append(f"  - {p.get('Name')} (default {p.get('Default')!r}): {p.get('Prompt')}")
    stages = {i: r for i, r in recs.items() if props(r).get("OLEType", "") in
              ("CCustomStage", "CTransformerStage", "CJSJobActivity", "CJSExecCmdActivity", "CJSSequencer",
               "CJSWaitForFileActivity", "CJSNotifyActivity", "CJSTerminatorActivity", "CJSExceptionHandler")
              or props(r).get("StageType")}
    # pin id -> (link name, owning stage)
    pins = {}
    for i, r in recs.items():
        p = props(r)
        if p.get("OLEType", "").endswith(("Input", "Output")) or "Partner" in p:
            pins[i] = r
    name_of = {i: props(r).get("Name", i) for i, r in stages.items()}
    out.append("")
    for sid, r in stages.items():
        p = props(r)
        out.append(f"STAGE {p.get('Name')}  [{p.get('StageType') or p.get('OLEType')}]")
        for g in sub_groups(r).get("Properties", []):
            if g.get("Name") not in PROPS_DROPPED and g.get("Value") not in (None, "", " "):
                out.append(f"    property {g['Name']} = {g['Value']}")
        for k in ("InputPins", "OutputPins"):
            for pid in (p.get(k) or "").split("|"):
                pin = pins.get(pid)
                if not pin:
                    continue
                pp = props(pin)
                direction = "in " if k == "InputPins" else "out"
                partner = pp.get("Partner", "").split("|")[0]
                out.append(f"    {direction} link {pp.get('Name')}  <-> {name_of.get(partner, partner)}")
                for g in sub_groups(pin).get("Properties", []):
                    if g.get("Name") not in PROPS_DROPPED and g.get("Value") not in (None, "", " ") \
                            and g.get("Name") not in ("Schema",):
                        out.append(f"        property {g['Name']} = {g['Value']}")
                cols = sub_groups(pin).get("Columns", [])
                if direction == "out" or not cols:
                    for c in cols:
                        d = c.get("Derivation")
                        out.append(f"        col {c.get('Name')} sql={c.get('SqlType')} prec={c.get('Precision')},"
                                   f"{c.get('Scale')} nullable={c.get('Nullable')}" + (f"  := {d}" if d else ""))
        out.append("")
    return "\n".join(out)


def main():
    src, dst = Path(sys.argv[1]), Path(sys.argv[2])
    dst.mkdir(parents=True, exist_ok=True)
    for f in sorted(src.glob("*.dsx")):
        if f.name == "Retail_DW_Pipeline_ALL.dsx":
            continue
        nodes = parse(f.read_text(encoding="cp1252"))
        for n in nodes:
            if n.kind == "DSJOB":
                text = render(n)
                (dst / f"{n.ident}.txt").write_text(text)
                print(f"{n.ident}: {len(text):,} chars")


if __name__ == "__main__":
    main()
