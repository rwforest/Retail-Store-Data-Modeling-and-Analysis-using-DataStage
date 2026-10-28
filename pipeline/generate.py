#!/usr/bin/env python3
"""Generate the DataStage deliverables from pipeline_def.py.

    python3 pipeline/generate.py

Writes:
  DataStage Jobs/Pipeline/<job>.dsx            one export per parallel job (23)
  DataStage Jobs/Pipeline/Seq_Retail_DW_Master.dsx   master sequence job
  DataStage Jobs/Pipeline/Retail_DW_Pipeline_ALL.dsx every job + the sequence in one import file
  pipeline/run_pipeline.sh                     the same DAG as a dsjob runner (fallback / CI)
  pipeline/pipeline_dag.json                   machine-readable DAG
  PIPELINE.md                                  step inventory + DAG diagram
"""
import json, re, sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import dsx_builder as B
import pipeline_def as P

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "DataStage Jobs" / "Pipeline"
PROJECT = "RetailDW"  # DataStage project name used by the runner; change to yours

# ---------------------------------------------------------------------------------------------------------------
# Master sequence DAG.  kind: exec | wait | job | seq(all|any) | notify | term | exc
# deps: [(node, trigger)] trigger: ok | custom:<expr> | otherwise | failed
# ---------------------------------------------------------------------------------------------------------------
JOBS = {j.name.split("_")[0]: j for j in P.build_all()}


def J(code):
    return next(k for k in JOBS if k == code)


def jn(code):
    return JOBS[code].name


NODES = []


def node(name, kind, deps=(), **kw):
    NODES.append(dict(name=name, kind=kind, deps=[d if isinstance(d, tuple) else (d, "ok") for d in deps], **kw))


node("act_Prepare_Dirs", "exec", command="mkdir -p #pStgDir# #pDwhDir# #pMartDir# #pAuditDir#")
node("wff_Source_Orders", "wait", ["act_Prepare_Dirs"], file="#pSrcDir#/orders_raw.csv")
for c in ("J01", "J02", "J03", "J04"):
    node(f"run_{jn(c)}", "job", ["wff_Source_Orders"], job=jn(c))
node("seq_Land_Done", "seq", [f"run_{jn(c)}" for c in ("J01", "J02", "J03", "J04")], mode="ALL")
node("act_Check_Dim_Customer", "exec", ["seq_Land_Done"],
     command='test -f "#pDwhDir#/dim_customer.csv" && echo EXISTS || echo MISSING', run_var="dim_state")
node(f"run_{jn('J05')}", "job", [("act_Check_Dim_Customer", 'custom:act_Check_Dim_Customer.$CommandOutput = "MISSING"')],
     job=jn("J05"), run_if="MISSING")
node("seq_Dim_Customer_Ready", "seq", [(f"run_{jn('J05')}", "ok"), ("act_Check_Dim_Customer", "otherwise")], mode="ANY")
node(f"run_{jn('J06')}", "job", ["seq_Dim_Customer_Ready"], job=jn("J06"))
node("act_Promote_Dim_Customer", "exec", [f"run_{jn('J06')}"],
     command='mv -f "#pDwhDir#/dim_customer_next.csv" "#pDwhDir#/dim_customer.csv"')
for c in ("J07", "J08", "J09"):
    node(f"run_{jn(c)}", "job", ["seq_Land_Done"], job=jn(c))
node("seq_Dims_Done", "seq", ["act_Promote_Dim_Customer"] + [f"run_{jn(c)}" for c in ("J07", "J08", "J09")], mode="ALL")
node(f"run_{jn('J10')}", "job", ["seq_Dims_Done"], job=jn("J10"))
node(f"run_{jn('J11')}", "job", ["seq_Dims_Done"], job=jn("J11"))
node(f"run_{jn('J12')}", "job", [f"run_{jn('J10')}"], job=jn("J12"))
node("seq_Facts_Done", "seq", [f"run_{jn('J11')}", f"run_{jn('J12')}"], mode="ALL")
node(f"run_{jn('J13')}", "job", ["seq_Facts_Done"], job=jn("J13"))
for c in ("J14", "J15", "J16"):
    node(f"run_{jn(c)}", "job", [f"run_{jn('J13')}"], job=jn(c))
for c in ("J17", "J18", "J19", "J20", "J21", "J22", "J23"):
    node(f"run_{jn(c)}", "job", ["seq_Facts_Done"], job=jn(c))
node("seq_All_Done", "seq", [f"run_{jn(c)}" for c in ("J14", "J15", "J16", "J17", "J18", "J19", "J20", "J21", "J22", "J23")],
     mode="ALL")
node("act_Notify_Success", "notify", ["seq_All_Done"], subject="Retail DW pipeline succeeded for #pRunDate#")
node("exc_Any_Failure", "exc", [])
node("act_Notify_Failure", "notify", ["exc_Any_Failure"], subject="Retail DW pipeline FAILED for #pRunDate#")
node("term_Abort", "term", ["act_Notify_Failure"])
NODE = {n["name"]: n for n in NODES}
for n in NODES:
    for d, _ in n["deps"]:
        assert d in NODE, d

# ---------------------------------------------------------------------------------------------------------------
# Sequence-job DSX (best effort: modelled on DataStage's sequence-activity class names; see PIPELINE.md caveats)
# ---------------------------------------------------------------------------------------------------------------
OLE = {"job": "CJSJobActivity", "exec": "CJSExecCmdActivity", "wait": "CJSWaitForFileActivity",
       "seq": "CJSSequencer", "notify": "CJSNotifyActivity", "term": "CJSTerminatorActivity",
       "exc": "CJSExceptionHandler"}


def sequence_dsx():
    names = [n["name"] for n in NODES]
    sid = {n: f"V0S{i + 1}" for i, n in enumerate(names)}
    kids = defaultdict(list)  # parent -> [(child, trigger)]
    for n in NODES:
        for d, t in n["deps"]:
            kids[d].append((n["name"], t))
    # layout by depth
    depth = {}
    for n in NODES:
        depth[n["name"]] = 0 if not n["deps"] else 1 + max(depth[d] for d, _ in n["deps"])
    rows = defaultdict(int)
    pos = {}
    for n in names:
        pos[n] = (72 + depth[n] * 216, 72 + rows[depth[n]] * 96)
        rows[depth[n]] += 1
    o = []
    w = o.append
    w("BEGIN DSJOB")
    w('   Identifier "Seq_Retail_DW_Master"')
    w(f'   DateModified "{B.HEADER_DATE}"')
    w(f'   TimeModified "{B.HEADER_TIME}"')
    w("   BEGIN DSRECORD")
    for k, v in [("Identifier", "ROOT"), ("OLEType", "CJobDefn"), ("Readonly", "0"), ("Name", "Seq_Retail_DW_Master"),
                 ("NextID", str(len(names) + 1)), ("Container", "V0"), ("JobVersion", "56.0.0"),
                 ("ControlAfterSubr", "0"),
                 ("ShortDescription", "Master sequence: land -> dimensions -> facts -> marts -> DQ, with failure notification")]:
        w(f'      {k} "{v}"')
    w('      Parameters "CParameters"')
    for pn, prompt, default in B.Job.PARAMS:
        w("      BEGIN DSSUBRECORD")
        for k, v in [("Name", pn), ("Prompt", prompt), ("Default", default), ("HelpTxt", ""), ("ParamType", "0"),
                     ("ParamLength", "0"), ("ParamScale", "0")]:
            w(f'         {k} "{v}"')
        w("      END DSSUBRECORD")
    for k, v in [("IsTemplate", 0), ("JobType", 2), ("Category", r"\\Retail_DW\\Pipeline\\0_Control"),
                 ("CenturyBreakYear", 30), ("NextAliasID", 2), ("ParameterFileDDName", "DD00001"),
                 ("ReservedWordCheck", 1), ("TransactionSize", 0), ("ValidationStatus", 0), ("Uploadable", 0),
                 ("PgmCustomizationFlag", 0), ("JobReportFlag", 0), ("AllowMultipleInvocations", 0),
                 ("ExpressionSemanticCheckFlag", 0), ("RelStagesInJobStatus", -1), ("CopyLibPrefix", "ARDT"),
                 ("RecordPerformanceResults", 0)]:
        w(f'      {k} "{v}"')
    w("   END DSRECORD")
    w("   BEGIN DSRECORD")
    for k, v in [("Identifier", "V0"), ("OLEType", "CContainerView"), ("Readonly", "0"), ("Name", "Job"),
                 ("NextID", "1"), ("IsTopLevel", "0")]:
        w(f'      {k} "{v}"')
    w(f'      StageList "{"|".join(sid[n] for n in names)}"')
    w(f'      StageXPos "{"|".join(str(pos[n][0]) for n in names)}"')
    w(f'      StageYPos "{"|".join(str(pos[n][1]) for n in names)}"')
    w(f'      StageTypes "{"|".join(OLE[NODE[n]["kind"]] for n in names)}"')
    w(f'      NextStageID "{len(names) + 1}"')
    w(f'      StageNames "{"|".join(names)}"')
    w("   END DSRECORD")
    pin_of = {}  # (parent, child) -> output pin id
    for n in names:
        outs = kids[n]
        for k, (c, t) in enumerate(outs, 1):
            pin_of[(n, c)] = f"{sid[n]}P{k}"
    in_of = {}
    for n in NODES:
        for k, (d, t) in enumerate(n["deps"], 1):
            in_of[(d, n["name"])] = f"{sid[n['name']]}P{100 + k}"
    for n in NODES:
        nm, kind = n["name"], n["kind"]
        w("   BEGIN DSRECORD")
        w(f'      Identifier "{sid[nm]}"')
        w(f'      OLEType "{OLE[kind]}"')
        w('      Readonly "0"')
        w(f'      Name "{nm}"')
        w(f'      NextID "{len(kids[nm]) + 1}"')
        ins = [in_of[(d, nm)] for d, _ in n["deps"]]
        if ins:
            w(f'      InputPins "{"|".join(ins)}"')
        outs = [pin_of[(nm, c)] for c, _ in kids[nm]]
        if outs:
            w(f'      OutputPins "{"|".join(outs)}"')
        if kind == "job":
            w(f'      JobName "{n["job"]}"')
            w(f'      ParamNames "{"|".join(p[0] for p in B.Job.PARAMS)}"')
            w(f'      ParamValues "{"|".join(p[0] for p in B.Job.PARAMS)}"')
            w('      ExecutionMode "0"')
        elif kind == "exec":
            w(f'      Command "{B.esc(n["command"])}"')
        elif kind == "wait":
            w(f'      FileName "{n["file"]}"')
            w('      WaitForFileCreation "1"')
            w('      TimeoutLength "3600"')
        elif kind == "seq":
            w(f'      Mode "{n["mode"]}"')
        elif kind == "notify":
            w(f'      Subject "{n["subject"]}"')
            w('      Recipient "data-platform@example.com"')
        elif kind == "term":
            w('      Abort "1"')
            w('      JobsToStop "ALL"')
        w("   END DSRECORD")
        for d, t in n["deps"]:
            w("   BEGIN DSRECORD")
            w(f'      Identifier "{in_of[(d, nm)]}"')
            w('      OLEType "CTriggerInput"')
            w('      Readonly "0"')
            w(f'      Name "{d}_to_{nm}"')
            w(f'      Partner "{sid[d]}|{pin_of[(d, nm)]}"')
            w("   END DSRECORD")
        for c, t in kids[nm]:
            w("   BEGIN DSRECORD")
            w(f'      Identifier "{pin_of[(nm, c)]}"')
            w('      OLEType "CTriggerOutput"')
            w('      Readonly "0"')
            tname = {"ok": "OK", "otherwise": "Otherwise", "failed": "Failed"}.get(t.split(":")[0], "Conditional")
            w(f'      Name "{tname}"')
            w(f'      Partner "{sid[c]}|{in_of[(nm, c)]}"')
            w(f'      TriggerType "{ {"ok": 1, "failed": 2, "otherwise": 7}.get(t.split(":")[0], 8)}"')
            if t.startswith("custom:"):
                w(f'      Expression "{B.esc(t[7:])}"')
            w("   END DSRECORD")
    w("END DSJOB")
    return "\n".join(o) + "\n"


# ---------------------------------------------------------------------------------------------------------------
# dsjob runner generated from the same DAG
# ---------------------------------------------------------------------------------------------------------------
def levels():
    lv = {}
    for n in NODES:
        lv[n["name"]] = 0 if not n["deps"] else 1 + max(lv[d] for d, _ in n["deps"])
    out = defaultdict(list)
    for n in NODES:
        out[lv[n["name"]]].append(n)
    return [out[k] for k in sorted(out)]


def runner_sh():
    L = ["#!/usr/bin/env bash",
         "# Generated by pipeline/generate.py from the same DAG as Seq_Retail_DW_Master. Do not edit by hand.",
         "# Runs the pipeline with dsjob: one wave per DAG level, jobs inside a wave run in parallel.",
         "set -u",
         f'PROJECT="${{DS_PROJECT:-{PROJECT}}}"',
         ]
    for pn, _, d in B.Job.PARAMS:
        L.append(f'{pn}="${{{pn}:-{d}}}"')
    L += ['DSJOB="${DSJOB:-dsjob}"', 'FAILED=0', 'DIM_STATE=""', "",
          "run_job() {  # dsjob -jobstatus: exit 1 = finished OK, 2 = finished with warnings",
          '  local job="$1"; shift',
          '  "$DSJOB" -run -jobstatus -mode NORMAL \\',
          *[f'    -param {pn}="${pn}" \\' for pn, _, _ in B.Job.PARAMS],
          '    "$PROJECT" "$job"',
          '  local rc=$?',
          '  if [ $rc -ne 1 ] && [ $rc -ne 2 ]; then echo "FAILED: $job (dsjob status $rc)" >&2; return 1; fi',
          '  echo "OK: $job (status $rc)"', "}", "",
          "notify() { echo \"NOTIFY: $1\"; }  # replace with mail/Slack",
          "trap 'notify \"Retail DW pipeline FAILED for $pRunDate\"' ERR", ""]
    for i, wave in enumerate(levels()):
        L.append(f"# ---- wave {i} " + "-" * 60)
        jobs = [n for n in wave if n["kind"] == "job"]
        for n in wave:
            if n["kind"] == "exec":
                cmd = n["command"]
                for pn, _, _ in B.Job.PARAMS:
                    cmd = cmd.replace(f"#{pn}#", f"${pn}")
                if n.get("run_var"):
                    L.append(f"DIM_STATE=$({cmd})")
                else:
                    L.append(cmd)
            elif n["kind"] == "wait":
                f = n["file"]
                for pn, _, _ in B.Job.PARAMS:
                    f = f.replace(f"#{pn}#", f"${pn}")
                L.append(f'until [ -f "{f}" ]; do echo "waiting for {f}"; sleep 30; done')
            elif n["kind"] == "notify" and "succeeded" in n["subject"]:
                L.append('notify "Retail DW pipeline succeeded for $pRunDate"')
        if jobs:
            L.append("pids=()")
            for n in jobs:
                if n.get("run_if"):  # conditional trigger: skipped job must not count as a failure
                    L.append(f'( if [ "$DIM_STATE" = "{n["run_if"]}" ]; then run_job {n["job"]}; '
                             f'else echo "SKIPPED: {n["job"]} (dim_customer already exists)"; fi ) & pids+=($!)')
                else:
                    L.append(f'( run_job {n["job"]} ) & pids+=($!)')
            L.append('for p in "${pids[@]}"; do wait "$p" || FAILED=1; done')
            L.append('[ "$FAILED" -eq 0 ] || { notify "Retail DW pipeline FAILED for $pRunDate"; exit 1; }')
        L.append("")
    return "\n".join(L) + "\n"


# ---------------------------------------------------------------------------------------------------------------
# Lint the generated DSX (structure only -- NOT a substitute for importing into Designer)
# ---------------------------------------------------------------------------------------------------------------
def lint(text, label):
    depth = 0
    pins, partners = {}, []
    stack = []
    for ln, line in enumerate(text.splitlines(), 1):
        s = line.strip()
        if s.startswith("BEGIN "):
            stack.append(s.split()[1])
        elif s.startswith("END "):
            assert stack and stack[-1] == s.split()[1], f"{label}:{ln} unbalanced {s}"
            stack.pop()
        m = re.match(r'Identifier "(V0S\d+P\d+)"', s)
        if m:
            assert m.group(1) not in pins, f"{label}: duplicate pin {m.group(1)}"
            pins[m.group(1)] = ln
        m = re.match(r'Partner "(V0S\d+)\|(V0S\d+P\d+)"', s)
        if m:
            partners.append((m.group(2), ln))
    assert not stack, f"{label}: unclosed {stack}"
    for p, ln in partners:
        assert p in pins, f"{label}:{ln} partner pin {p} does not exist"
    return len(pins)


# ---------------------------------------------------------------------------------------------------------------
def inventory_md(jobs, n_seq):
    kind_name = {"seq_src": "Sequential File (read)", "seq_tgt": "Sequential File (write)", "xfm": "Transformer",
                 "filter": "Filter", "sort": "Sort", "remdup": "Remove Duplicates", "agg": "Aggregator",
                 "join": "Join", "copy": "Copy", "funnel": "Funnel", "head": "Head"}
    tot = defaultdict(int)
    rows = []
    for j in jobs:
        rd = [s.seq["file"].split("/")[-1] for s in j.stages if s.kind == "seq_src"]
        wr = [s.seq["file"].split("/")[-1] for s in j.stages if s.kind == "seq_tgt"]
        for s in j.stages:
            tot[s.kind] += 1
        rows.append(f"| {j.name} | {j.layer} | {len(j.stages)} | {', '.join(rd)} | {', '.join(wr)} |")
    mix = "\n".join(f"| {kind_name[k]} | {v} |" for k, v in sorted(tot.items(), key=lambda kv: -kv[1]))
    return rows, mix, sum(tot.values())


def mermaid():
    L = ["flowchart LR"]
    ident = {n["name"]: f"n{i}" for i, n in enumerate(NODES)}
    shape = {"job": ('[', ']'), "exec": ('([', '])'), "wait": ('([', '])'), "seq": ('{{', '}}'), "notify": ('>', ']'),
             "term": ('((', '))'), "exc": ('((', '))')}
    for n in NODES:
        a, b = shape[n["kind"]]
        label = n["job"] if n["kind"] == "job" else n["name"]
        L.append(f'  {ident[n["name"]]}{a}"{label}"{b}')
    for n in NODES:
        for d, t in n["deps"]:
            lab = "" if t == "ok" else f'|{"MISSING" if t.startswith("custom") else t}|'
            L.append(f"  {ident[d]} -->{lab} {ident[n['name']]}")
    return "\n".join(L)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    jobs = list(JOBS.values())
    total_pins = 0
    for j in jobs:
        text = B.dsx_file([j])
        total_pins += lint(text, j.name)
        (OUT / f"{j.name}.dsx").write_text(text, newline="\r\n" if False else None)
    seq = sequence_dsx()
    seq_text = B.dsx_file([]) + seq
    lint(seq_text, "Seq_Retail_DW_Master")
    (OUT / "Seq_Retail_DW_Master.dsx").write_text(seq_text)
    (OUT / "Retail_DW_Pipeline_ALL.dsx").write_text(B.dsx_file(jobs, [seq]))
    (ROOT / "pipeline" / "run_pipeline.sh").write_text(runner_sh())
    (ROOT / "pipeline" / "run_pipeline.sh").chmod(0o755)
    dag = {"project": PROJECT, "nodes": [{k: v for k, v in n.items()} for n in NODES]}
    (ROOT / "pipeline" / "pipeline_dag.json").write_text(json.dumps(dag, indent=2))
    rows, mix, n_stages = inventory_md(jobs, len(NODES))
    n_links = sum(len(j.links) for j in jobs)
    md = f"""# Retail DW pipeline - step inventory

*Generated by `pipeline/generate.py` - do not edit by hand.*

**{n_stages} parallel-job stages** in **{len(jobs)} jobs**, plus **{len(NODES)} sequence activities** in the master
sequence = **{n_stages + len(NODES)} steps**, connected by {n_links} links.

## Stage mix

| Stage type | Count |
|---|---|
{mix}

## Jobs

| Job | Layer | Stages | Reads | Writes |
|---|---|---|---|---|
""" + "\n".join(rows) + f"""

## Master sequence (`Seq_Retail_DW_Master`)

```mermaid
{mermaid()}
```
"""
    (ROOT / "PIPELINE.md").write_text(md)
    print(f"{len(jobs)} jobs, {n_stages} stages, {n_links} links, {len(NODES)} sequence activities "
          f"=> {n_stages + len(NODES)} steps; {total_pins} pins linted")


if __name__ == "__main__":
    main()
