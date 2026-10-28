"""Tiny DSL that emits IBM InfoSphere DataStage parallel-job .dsx text.

The record layout (ROOT / container view / stage / pin / column records and the
property encodings) is modelled on the four jobs already in this repo
(PxSequentialFile, PxJoin, PxFilter, PxSort, PxRemDup, PxHead, PxAggregator-count,
CTransformerStage). Stage types that do NOT appear in those jobs are flagged in
UNVERIFIED_STAGES.

Every column reference is checked while the job is built, so a typo fails here
instead of in Designer.
"""
import re
from dataclasses import dataclass, field

HEADER_DATE, HEADER_TIME = "2026-10-27", "12.00.00"

TYPES = {  # name: (SqlType, precision, scale, display, orchestrate type, extended precision)
    "int": (4, 10, 0, 10, "int32", 0),
    "bigint": (-5, 19, 0, 19, "int64", 0),
    "str": (12, 255, 0, 32, "ustring[max=255]", 1),
    "dec": (3, 18, 2, 18, "decimal[18,2]", 0),
    "rate": (3, 18, 6, 18, "decimal[18,6]", 0),
    "ts": (11, 26, 0, 19, "timestamp", 0),
    "date": (9, 10, 0, 10, "date", 0),
}
UNVERIFIED_STAGES = {"PxCopy", "PxFunnel", "PxAggregator(calculation)"}


@dataclass
class Col:
    name: str
    type: str
    nullable: bool = False

    def renamed(self, n):
        return Col(n, self.type, self.nullable)


@dataclass
class Link:
    name: str
    cols: list
    src: "Stage" = None
    dst: "Stage" = None
    src_pin: str = ""
    dst_pin: str = ""

    def col(self, n):
        for c in self.cols:
            if c.name == n:
                return c
        raise KeyError(f"link {self.name}: no column {n!r} (have {[c.name for c in self.cols]})")

    @property
    def names(self):
        return [c.name for c in self.cols]


@dataclass
class Stage:
    idx: int
    name: str
    kind: str
    ptype: str  # PxSequentialFile, PxJoin, ...
    props: list = field(default_factory=list)  # [(name, encoded value or None)]
    inputs: list = field(default_factory=list)
    outputs: list = field(default_factory=list)
    xfm_cols: dict = field(default_factory=dict)  # output link name -> [(Col, derivation)]
    seq: dict = field(default_factory=dict)  # file, append

    @property
    def sid(self):
        return f"V0S{self.idx}"


def esc(s):
    return s.replace('"', '\\"')


def enc_keys(keys, prop="key", opts=None):
    """DataStage multi-value property encoding, e.g. \\(2)\\(2)0\\(1)\\(3)key\\(2)A\\(2)0 ..."""
    out = r"\(2)\(2)0"
    for k in keys:
        out += rf"\(1)\(3){prop}\(2){k}\(2)0"
        if opts and k in opts:
            for sub, val in opts[k]:
                out += rf"\(1)\(3)\(3){sub}\(2){val}\(2)0"
    return out


class Job:
    PARAMS = [  # name, prompt, default
        ("pSrcDir", "Source files directory", "C:/case_study/Source_Data"),
        ("pStgDir", "Staging directory", "C:/case_study/Staging"),
        ("pDwhDir", "Warehouse (dimensions/facts) directory", "C:/case_study/Warehouse"),
        ("pMartDir", "Data mart directory", "C:/case_study/Marts"),
        ("pAuditDir", "Audit / DQ output directory", "C:/case_study/Audit"),
        ("pRunDate", "Business run date (yyyy-mm-dd)", "2011-12-31"),
        ("pEffDate", "SCD2 effective date for changed customer attributes (yyyy-mm-dd)", "2011-07-01"),
    ]

    def __init__(self, name, short, layer):
        self.name, self.short, self.layer = name, short, layer
        self.stages, self.links = [], []

    # ---- internals -------------------------------------------------------------------------------------------
    def _stage(self, name, kind, ptype, props=None):
        assert all(s.name != name for s in self.stages), f"{self.name}: duplicate stage {name}"
        s = Stage(len(self.stages) + 1, name, kind, ptype, props or [])
        self.stages.append(s)
        return s

    def _out(self, st, name, cols):
        assert all(l.name != name for l in self.links), f"{self.name}: duplicate link {name}"
        lk = Link(name, cols, src=st)
        st.outputs.append(lk)
        self.links.append(lk)
        return lk

    def _in(self, st, lk):
        assert lk.dst is None, f"{self.name}: link {lk.name} already consumed (use copy())"
        lk.dst = st
        st.inputs.append(lk)

    @staticmethod
    def _need(lk, cols, what):
        for c in cols:
            lk.col(c)  # raises with a helpful message

    # ---- stages ------------------------------------------------------------------------------------------------
    def seq_src(self, name, file, cols, out):
        st = self._stage(name, "seq_src", "PxSequentialFile")
        st.seq = {"file": file}
        return self._out(st, out, [Col(n, t) for n, t in cols])

    def seq_tgt(self, name, inp, file, append=False):
        st = self._stage(name, "seq_tgt", "PxSequentialFile")
        for c in inp.cols:
            assert not c.nullable, f"{self.name}.{name}: nullable column {c.name} written to file without NullToValue"
        st.seq = {"file": file, "append": append}
        self._in(st, inp)

    def xfm(self, name, inp, out, add=(), drop=(), only=None):
        """Transformer. add: [(col, type, expr)] with {i} = input link name. only: restrict to these input columns."""
        st = self._stage(name, "xfm", "CTransformerStage")
        self._in(st, inp)
        base = [c for c in inp.cols if (only is None or c.name in only) and c.name not in drop]
        if only is not None:
            self._need(inp, only, name)
        outcols = {c.name: (Col(c.name, c.type, c.nullable), f"{inp.name}.{c.name}") for c in base}
        for cname, ctype, expr in add:
            for ref in re.findall(r"\{i\}\.(\w+)", expr):
                inp.col(ref)
            outcols[cname] = (Col(cname, ctype, False), expr.replace("{i}", inp.name))
        lk = self._out(st, out, [c for c, _ in outcols.values()])
        st.xfm_cols[out] = list(outcols.values())
        return lk

    def filter(self, name, inp, where, out, uses=()):
        """where: PxFilter expression. uses: columns it references (validated)."""
        self._need(inp, uses, name)
        st = self._stage(name, "filter", "PxFilter", [("where", rf"\(2)\(2)0\(1)\(3)where\(2){esc(where)}\(2)0"),
                                                      ("first", " "), ("reject", " ")])
        self._in(st, inp)
        return self._out(st, out, [Col(c.name, c.type, c.nullable) for c in inp.cols])

    def sort(self, name, inp, keys, out):
        """keys: [(col, 'asc'|'desc')]"""
        self._need(inp, [k for k, _ in keys], name)
        opts = {k: [(r"asc\\desc", d)] for k, d in keys if d == "desc"}
        st = self._stage(name, "sort", "PxSort", [("operator", "psort -sorter unix"),
                                                  ("key", enc_keys([k for k, _ in keys], opts=opts)),
                                                  ("nonStable", None)])
        self._in(st, inp)
        return self._out(st, out, [Col(c.name, c.type, c.nullable) for c in inp.cols])

    def remdup(self, name, inp, keys, out, keep="first"):
        self._need(inp, keys, name)
        st = self._stage(name, "remdup", "PxRemDup", [("keep", keep), ("key", enc_keys(keys))])
        self._in(st, inp)
        return self._out(st, out, [Col(c.name, c.type, c.nullable) for c in inp.cols])

    def head(self, name, inp, n, out):
        st = self._stage(name, "head", "PxHead", [("all", " "), ("nrecs", str(n)), ("selection", "part"),
                                                  ("part", r"\(2)\(2)0\(1)\(3)part\(2)0\(2)0")])
        self._in(st, inp)
        return self._out(st, out, [Col(c.name, c.type, c.nullable) for c in inp.cols])

    def copy(self, name, inp, outs):
        st = self._stage(name, "copy", "PxCopy")
        self._in(st, inp)
        return [self._out(st, o, [Col(c.name, c.type, c.nullable) for c in inp.cols]) for o in outs]

    def funnel(self, name, inps, out):
        st = self._stage(name, "funnel", "PxFunnel", [("operator", "funnel")])
        first = [(c.name, c.type) for c in inps[0].cols]
        for lk in inps:
            assert [(c.name, c.type) for c in lk.cols] == first, \
                f"{self.name}.{name}: funnel inputs differ: {lk.name}"
            self._in(st, lk)
        return self._out(st, out, [Col(c.name, c.type, c.nullable) for c in inps[0].cols])

    def join(self, name, left, right, keys, out, how="inner", take=None, left_cols=None):
        """take: right-side columns to add (default: all non-key). left_cols: left columns to keep (default: all)."""
        self._need(left, keys, name)
        self._need(right, keys, name)
        self._need(left, left_cols or [], name)
        self._need(right, take or [], name)
        op = {"inner": "innerjoin", "left": "leftouterjoin", "right": "rightouterjoin", "full": "fullouterjoin"}[how]
        st = self._stage(name, "join", "PxJoin", [("operator", op), ("key", enc_keys(keys))])
        self._in(st, left)
        self._in(st, right)
        cols, seen = [], set()
        for c in left.cols:
            if left_cols is None or c.name in left_cols or c.name in keys:
                cols.append(Col(c.name, c.type, c.nullable or how in ("right", "full")))
                seen.add(c.name)
        for c in right.cols:
            if c.name in keys or (take is not None and c.name not in take):
                continue
            assert c.name not in seen, f"{self.name}.{name}: column {c.name} on both join inputs"
            cols.append(Col(c.name, c.type, c.nullable or how in ("left", "full")))
            seen.add(c.name)
        st.seq = {"left_names": set(left.names)}
        return self._out(st, out, cols)

    def agg(self, name, inp, keys, out, count=None, calc=()):
        """count='colname' -> row count (verified encoding). calc=[(fn, src, outcol, type)] fn in sum|min|max."""
        self._need(inp, keys, name)
        cols = [Col(k, inp.col(k).type) for k in keys]
        if count:
            assert not calc
            st = self._stage(name, "agg", "PxAggregator", [("method", "sort"), ("key", enc_keys(keys)),
                                                           ("selection", "count"), ("nul_res", " "),
                                                           ("countField", enc_keys([count], "countField"))])
            cols.append(Col(count, "int"))
            st.seq = {"mode": "count"}
        else:
            by_src = {}
            for fn, src, o, t in calc:
                inp.col(src)
                by_src.setdefault(src, []).append((fn, o))
                cols.append(Col(o, t))
            val = r"\(2)\(2)0"
            for src, subs in by_src.items():
                val += rf"\(1)\(3)reduce\(2){src}\(2)0"
                for fn, o in subs:
                    val += rf"\(1)\(3)\(3){fn}\(2){o}\(2)0"
            st = self._stage(name, "agg", "PxAggregator", [("method", "sort"), ("key", enc_keys(keys)),
                                                           ("selection", "reduce"), ("reduce", val)])
            st.seq = {"mode": "calc"}
        self._in(st, inp)
        return self._out(st, out, cols)

    # ---- emission --------------------------------------------------------------------------------------------
    def layout(self):
        depth = {}
        for s in self.stages:  # stages are created in topological order
            depth[s.idx] = 0 if not s.inputs else 1 + max(depth[l.src.idx] for l in s.inputs)
        rows, pos = {}, {}
        for s in self.stages:
            r = rows.get(depth[s.idx], 0)
            rows[depth[s.idx]] = r + 1
            pos[s.idx] = (72 + depth[s.idx] * 192, 96 + r * 120)
        return pos

    def validate(self):
        for lk in self.links:
            assert lk.dst is not None, f"{self.name}: dangling link {lk.name}"
        for s in self.stages:
            if s.kind == "join":
                assert len(s.inputs) == 2
            if s.kind in ("seq_tgt",):
                assert not s.outputs

    def to_dsx(self):
        self.validate()
        pos = self.layout()
        o = []
        w = o.append
        # pin ids
        for s in self.stages:
            n = 0
            for lk in s.inputs:
                n += 1
                lk.dst_pin = f"{s.sid}P{n}"
            for lk in s.outputs:
                n += 1
                lk.src_pin = f"{s.sid}P{n}"
            s.npins = n
        w("BEGIN DSJOB")
        w(f'   Identifier "{self.name}"')
        w(f'   DateModified "{HEADER_DATE}"')
        w(f'   TimeModified "{HEADER_TIME}"')
        w("   BEGIN DSRECORD")
        w('      Identifier "ROOT"')
        w('      OLEType "CJobDefn"')
        w('      Readonly "0"')
        w(f'      Name "{self.name}"')
        w(f'      NextID "{len(self.stages) + 1}"')
        w('      Container "V0"')
        w('      JobVersion "56.0.0"')
        w('      ControlAfterSubr "0"')
        w(f'      ShortDescription "{esc(self.short)}"')
        w('      Parameters "CParameters"')
        for pn, prompt, default in self.PARAMS:
            w("      BEGIN DSSUBRECORD")
            w(f'         Name "{pn}"')
            w(f'         Prompt "{prompt}"')
            w(f'         Default "{default}"')
            w('         HelpTxt ""')
            w('         ParamType "0"')
            w('         ParamLength "0"')
            w('         ParamScale "0"')
            w("      END DSSUBRECORD")
        w('      MetaBag "CMetaProperty"')
        for nm, val in (("AdvancedRuntimeOptions", "#DSProjectARTOptions#"),
                        ("IdentList", "|".join(s.name for s in self.stages)), ("ClientCodePage", "1252")):
            w("      BEGIN DSSUBRECORD")
            w('         Owner "APT"')
            w(f'         Name "{nm}"')
            w(f'         Value "{val}"')
            w("      END DSSUBRECORD")
        w('      NULLIndicatorPosition "0"')
        for k, v in [("IsTemplate", 0), ("JobType", 3), ("Category", r"\\Retail_DW\\Pipeline" + "\\\\" + self.layer),
                     ("CenturyBreakYear", 30), ("NextAliasID", 2), ("ParameterFileDDName", "DD00001"),
                     ("ReservedWordCheck", 1), ("TransactionSize", 0), ("ValidationStatus", 0), ("Uploadable", 0),
                     ("PgmCustomizationFlag", 0), ("JobReportFlag", 0), ("AllowMultipleInvocations", 0),
                     ("Act2ActOverideDefaults", 0), ("Act2ActEnableRowBuffer", 0), ("Act2ActUseIPC", 0),
                     ("Act2ActBufferSize", 0), ("Act2ActIPCTimeout", 0), ("ExpressionSemanticCheckFlag", 0),
                     ("TraceOption", 0), ("EnableCacheSharing", 0), ("RuntimeColumnPropagation", 0),
                     ("RelStagesInJobStatus", -1), ("WebServiceEnabled", 0), ("MFProcessMetaData", 0),
                     ("MFProcessMetaDataXMLFileExchangeMethod", 0), ("IMSProgType", 0), ("CopyLibPrefix", "ARDT"),
                     ("RecordPerformanceResults", 0)]:
            w(f'      {k} "{v}"')
        w("   END DSRECORD")
        # container view
        w("   BEGIN DSRECORD")
        for k, v in [("Identifier", "V0"), ("OLEType", "CContainerView"), ("Readonly", "0"), ("Name", "Job"),
                     ("NextID", "1"), ("IsTopLevel", "0")]:
            w(f'      {k} "{v}"')
        stg = self.stages
        links_by_stage = [(s, (s.outputs or [None])) for s in stg]
        flat = [(s, lk) for s, lks in links_by_stage for lk in lks]

        def arr(f):
            return "|".join(f(s, lk) for s, lk in flat)

        stage_cols = lambda f: "|".join(f(s) for s in stg)
        mode = lambda s: "1" if s.kind in ("seq_src", "seq_tgt") else "2"
        w(f'      StageList "{stage_cols(lambda s: s.sid)}"')
        w(f'      StageXPos "{stage_cols(lambda s: str(pos[s.idx][0]))}"')
        w(f'      StageYPos "{stage_cols(lambda s: str(pos[s.idx][1]))}"')
        w(f'      StageTypes "{stage_cols(lambda s: "CTransformerStage" if s.ptype == "CTransformerStage" else "CCustomStage")}"')
        w(f'      NextStageID "{len(stg) + 1}"')
        for k, v in [("SnapToGrid", 1), ("GridLines", 0), ("ZoomValue", 100)]:
            w(f'      {k} "{v}"')
        w(f'      StageXSize "{stage_cols(lambda s: "48")}"')
        w(f'      StageYSize "{stage_cols(lambda s: "48")}"')
        w('      ContainerViewSizing "0000 0000 1638 0397 0000 0001 0160 0082"')
        w(f'      StageNames "{stage_cols(lambda s: s.name)}"')
        w(f'      StageTypeIDs "{stage_cols(lambda s: s.ptype)}"')
        w(f'      LinkNames "{arr(lambda s, l: l.name if l else " ")}"')
        w(f'      LinkHasMetaDatas "{arr(lambda s, l: "True" if l else " ")}"')
        w(f'      LinkTypes "{arr(lambda s, l: "1" if l else " ")}"')
        mx = lambda s, l: str((pos[s.idx][0] + pos[l.dst.idx][0]) // 2) if l else " "
        my = lambda s, l: str(pos[s.idx][1] + 6) if l else " "
        w(f'      LinkNamePositionXs "{arr(mx)}"')
        w(f'      LinkNamePositionYs "{arr(my)}"')
        w(f'      TargetStageIDs "{arr(lambda s, l: l.dst.sid if l else " ")}"')
        w(f'      SourceStageEffectiveExecutionModes "{arr(lambda s, l: mode(s) if l else " ")}"')
        w(f'      SourceStageRuntimeExecutionModes "{arr(lambda s, l: mode(s) if l else " ")}"')
        w(f'      TargetStageEffectiveExecutionModes "{arr(lambda s, l: mode(l.dst) if l else " ")}"')
        w(f'      TargetStageRuntimeExecutionModes "{arr(lambda s, l: mode(l.dst) if l else " ")}"')
        w(f'      LinkIsSingleOperatorLookup "{arr(lambda s, l: "False" if l else " ")}"')
        w(f'      LinkIsSortSequential "{arr(lambda s, l: "False" if l else " ")}"')
        w(f'      LinkSortMode "{arr(lambda s, l: "0" if l else " ")}"')
        w(f'      LinkPartColMode "{arr(lambda s, l: "1" if l else " ")}"')
        w(f'      LinkSourcePinIDs "{arr(lambda s, l: l.src_pin if l else " ")}"')
        w("   END DSRECORD")
        for s in stg:
            self._emit_stage(s, pos, w)
        w("END DSJOB")
        return "\n".join(o) + "\n"

    # ---- record emitters -------------------------------------------------------------------------------------
    def _emit_stage(self, s, pos, w):
        is_x = s.ptype == "CTransformerStage"
        w("   BEGIN DSRECORD")
        w(f'      Identifier "{s.sid}"')
        w(f'      OLEType "{"CTransformerStage" if is_x else "CCustomStage"}"')
        w('      Readonly "0"')
        w(f'      Name "{s.name}"')
        w(f'      NextID "{s.npins + 1}"')
        if s.inputs:
            w(f'      InputPins "{"|".join(l.dst_pin for l in s.inputs)}"')
        if s.outputs:
            w(f'      OutputPins "{"|".join(l.src_pin for l in s.outputs)}"')
        if is_x:
            w('      ValidationStatus "0"')
            w('      StageType "CTransformerStage"')
            for k, v in [("BlockSize", 0), ("SKKeySourceType", "file"), ("StageVarsMinimised", 0),
                         ("LoopVarsMaximised", 0), ("MaxLoopIterations", 0)]:
                w(f'      {k} "{v}"')
        else:
            w(f'      StageType "{s.ptype}"')
            w('      AllowColumnMapping "0"')
            if s.props:
                w('      Properties "CCustomProperty"')
                for n, v in s.props:
                    w("      BEGIN DSSUBRECORD")
                    w(f'         Name "{n}"')
                    if v is not None:
                        w(f'         Value "{v}"')
                    w("      END DSSUBRECORD")
            w('      NextRecordID "0"')
        w("   END DSRECORD")
        for lk in s.inputs:
            self._emit_in(s, lk, w, is_x)
        for lk in s.outputs:
            self._emit_out(s, lk, pos, w, is_x)

    def _emit_in(self, s, lk, w, is_x):
        w("   BEGIN DSRECORD")
        w(f'      Identifier "{lk.dst_pin}"')
        w(f'      OLEType "{"CTrxInput" if is_x else "CCustomInput"}"')
        w('      Readonly "0"')
        w(f'      Name "{lk.name}"')
        w(f'      Partner "{lk.src.sid}|{lk.src_pin}"')
        w('      LinkType "1"')
        if not is_x:
            w('      ConditionNotMet "fail"')
            w('      LookupFail "fail"')
        if s.kind == "seq_tgt":
            w('      Properties "CCustomProperty"')
            f = s.seq["file"]
            for n, v in [("schema", None), ("file", rf"\(2)\(2)0\(1)\(3)file\(2){f}\(2)0"),
                         (r"append\\overwrite", "append" if s.seq["append"] else "overwrite"),
                         ("nocleanup", " "), ("rejects", "continue"),
                         ("firstLineColumnNames", "firstLineColumnNames"),
                         ("writemethod", r"\(2)\(2)0\(1)\(3)writemethod\(2) \(2)0")]:
                w("      BEGIN DSSUBRECORD")
                w(f'         Name "{n}"')
                if v is not None:
                    w(f'         Value "{v}"')
                w("      END DSSUBRECORD")
        w('      MetaBag "CMetaProperty"')
        w("      BEGIN DSSUBRECORD")
        w('         Owner "APT"')
        w('         Name "RTColumnProp"')
        w('         Value "0"')
        w("      END DSSUBRECORD")
        if s.kind == "seq_tgt":
            w("      BEGIN DSSUBRECORD")
            w('         Owner "APT"')
            w('         Name "SchemaFormat"')
            w("         Value \"final_delim=end, delim=',', quote=double\"")
            w("      END DSSUBRECORD")
        if is_x:
            w("      BEGIN DSSUBRECORD")
            w('         Owner "APT"')
            w('         Name "Schema"')
            w("         Value =+=+=+=")
            w("record\n(")
            for c in lk.cols:
                w(f"  {c.name}:{'nullable ' if c.nullable else ''}{TYPES[c.type][4]};")
            w(")")
            w("=+=+=+=")
            w("      END DSSUBRECORD")
            w('      MultiRow "0"')
            w('      LinkMinimised "0"')
        else:
            w('      TransactionSize "0"')
            w('      TXNBehaviour "0"')
            w('      EnableTxGroup "0"')
            w('      LinkMinimised "0"')
        w("   END DSRECORD")

    def _emit_out(self, s, lk, pos, w, is_x):
        w("   BEGIN DSRECORD")
        w(f'      Identifier "{lk.src_pin}"')
        w(f'      OLEType "{"CTrxOutput" if is_x else "CCustomOutput"}"')
        w('      Readonly "0"')
        w(f'      Name "{lk.name}"')
        w(f'      Partner "{lk.dst.sid}|{lk.dst_pin}"')
        if is_x:
            w('      Reject "0"')
            w('      ErrorPin "0"')
            w('      RowLimit "0"')
        if s.kind == "seq_src":
            f = s.seq["file"]
            w('      Properties "CCustomProperty"')
            for n, v in [("schema", None), ("selection", "file"), ("keepPartitions", " "), ("rejects", "continue"),
                         ("reportProgress", "yes"), ("firstLineColumnNames", "firstLineColumnNames"),
                         ("file ", rf"\(2)\(2)0\(1)\(3)file \(2){f}\(2)0"), ("missingFile", " ")]:
                w("      BEGIN DSSUBRECORD")
                w(f'         Name "{n}"')
                if v is not None:
                    w(f'         Value "{v}"')
                w("      END DSSUBRECORD")
        w('      Columns "COutputColumn"')
        deriv = {}
        if is_x:
            deriv = {c.name: d for c, d in s.xfm_cols[lk.name]}
        for i, c in enumerate(lk.cols):
            sql, prec, scale, disp, _, ext = TYPES[c.type]
            w("      BEGIN DSSUBRECORD")
            w(f'         Name "{c.name}"')
            w(f'         SqlType "{sql}"')
            w(f'         Precision "{prec}"')
            w(f'         Scale "{scale}"')
            w(f'         Nullable "{1 if c.nullable else 0}"')
            w('         KeyPosition "0"')
            w(f'         DisplaySize "{disp}"')
            d = None
            if s.kind == "seq_src":
                pass
            elif is_x:
                d = deriv[c.name]
            elif s.kind == "agg":
                d = f"{s.inputs[0].name}.{c.name}" if c.name in s.inputs[0].names else None
            elif s.kind == "join":
                side = s.inputs[0] if c.name in s.seq["left_names"] else s.inputs[1]
                d = f"{side.name}.{c.name}"
            elif s.inputs and c.name in s.inputs[0].names:
                d = f"{s.inputs[0].name}.{c.name}"
            if d:
                w(f'         Derivation "{esc(d)}"')
            w('         Group "0"')
            if d:
                w(f'         ParsedDerivation "{esc(d)}"')
                if re.fullmatch(r"\w+\.\w+", d):
                    w(f'         SourceColumn "{d}"')
            for k in ("SortKey", "SortType"):
                w(f'         {k} "0"')
            for k in ("AllowCRLF", "LevelNo", "Occurs", "PadNulls", "SignOption", "SortingOrder", "ArrayHandling",
                      "SyncIndicator"):
                w(f'         {k} "0"')
            w('         PadChar ""')
            if s.kind == "seq_src":
                w(f'         ColumnReference "{c.name}"')
            if c.type not in ("str", "ts", "date"):
                w('         APTFieldProp "quote=none"')
            w(f'         ExtendedPrecision "{ext}"')
            for k in ("TaggedSubrec", "OccursVarying", "PKeyIsCaseless", "SCDPurpose"):
                w(f'         {k} "0"')
            w("      END DSSUBRECORD")
        w('      MetaBag "CMetaProperty"')
        for n, v in (("DiskWriteInc", 1048576), ("BufFreeRun", 50), ("MaxMemBufSize", 3145728), ("QueueUpperSize", 0)):
            w("      BEGIN DSSUBRECORD")
            w('         Owner "APT"')
            w(f'         Name "{n}"')
            w(f'         Value "{v}"')
            w("      END DSSUBRECORD")
        if s.kind == "seq_src":
            w("      BEGIN DSSUBRECORD")
            w('         Owner "APT"')
            w('         Name "SchemaFormat"')
            w("         Value \"final_delim=end, delim=',', quote=double\"")
            w("      END DSSUBRECORD")
        x, y = (pos[s.idx][0] + pos[lk.dst.idx][0]) // 2, pos[s.idx][1] + 6
        w(f'      LeftTextPos "{x}"')
        w(f'      TopTextPos "{y}"')
        w('      LinkMinimised "0"')
        w("   END DSRECORD")


def dsx_file(jobs, extra_blocks=()):
    head = f'''BEGIN HEADER
   CharacterSet "CP1252"
   ExportingTool "IBM InfoSphere DataStage Export"
   ToolVersion "8"
   ServerName "IBMDSSERVER"
   ToolInstanceID "DS39"
   MDISVersion "1.0"
   Date "{HEADER_DATE}"
   Time "{HEADER_TIME}"
   ServerVersion "9.1"
END HEADER
'''
    return head + "".join(j.to_dsx() for j in jobs) + "".join(extra_blocks)
