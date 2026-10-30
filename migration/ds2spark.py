#!/usr/bin/env python3
"""Compile the retail DW DataStage jobs (pipeline/pipeline_def.py) into Databricks notebooks.

Lakebridge's BladeBridge emits no notebooks for these exports (see migration/README.md), so this
compiles the same in-memory Job model that generated the DSX. One notebook per job, one temp view
per link, Delta tables instead of CSV files between jobs.

    python3 migration/ds2spark.py            # writes migration/bundle/src/notebooks/*.py
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
import pipeline_def as P  # noqa: E402

OUT = ROOT / "migration" / "bundle" / "src" / "notebooks"

SQL_TYPES = {"int": "int", "bigint": "bigint", "str": "string", "dec": "decimal(18,2)",
             "rate": "decimal(18,6)", "ts": "timestamp", "date": "date"}

# --------------------------------------------------------------------------------------------------------------
# DataStage derivation / constraint expressions -> Spark SQL
# --------------------------------------------------------------------------------------------------------------
TOKEN = re.compile(r'\s*(?:(?P<str>"(?:[^"\\]|\\.)*")|(?P<num>\d+(?:\.\d+)?)|(?P<id>@?[A-Za-z_]\w*(?:\.\w+)?)'
                   r'|(?P<op><>|<=|>=|[-+*/=<>():,]))')
FMT = [("%yyyy", "yyyy"), ("%mm", "MM"), ("%dd", "dd"), ("%hh", "HH"), ("%nn", "mm"), ("%ss", "ss")]
PARAMS = {"pRunDate", "pEffDate"}


def fmt(s):
    for a, b in FMT:
        s = s.replace(a, b)
    return s


class Expr:
    """Recursive-descent translator. `order` is the SQL ORDER BY used for @INROWNUM surrogate keys."""

    def __init__(self, text, order=None):
        self.toks, i = [], 0
        text = text.strip()
        while i < len(text):
            m = TOKEN.match(text, i)
            if not m:
                raise ValueError(f"cannot tokenize {text[i:i + 30]!r} in {text!r}")
            kind = m.lastgroup
            self.toks.append((kind, m.group(kind)))
            i = m.end()
        self.p, self.order, self.text = 0, order, text

    def peek(self, k=0):
        return self.toks[self.p + k] if self.p + k < len(self.toks) else (None, None)

    def kw(self, *words):
        k, v = self.peek()
        return k == "id" and v.lower() in words

    def take(self):
        t = self.toks[self.p]
        self.p += 1
        return t

    def expect(self, v):
        k, t = self.take()
        assert t == v, f"expected {v!r} got {t!r} in {self.text!r}"

    def parse(self):
        if "@INROWNUM" in self.text:  # parallel surrogate-key idiom
            return f"row_number() OVER (ORDER BY {self.order or 'monotonically_increasing_id()'})"
        out = self.expr()
        assert self.p == len(self.toks), f"trailing tokens in {self.text!r}"
        return out

    def expr(self):
        if self.kw("if"):
            self.take()
            c = self.expr()
            assert self.kw("then"), self.text
            self.take()
            a = self.expr()
            assert self.kw("else"), self.text
            self.take()
            b = self.expr()
            return f"CASE WHEN {c} THEN {a} ELSE {b} END"
        return self.or_()

    def or_(self):
        x = self.and_()
        while self.kw("or"):
            self.take()
            x = f"({x} OR {self.and_()})"
        return x

    def and_(self):
        x = self.not_()
        while self.kw("and"):
            self.take()
            x = f"({x} AND {self.not_()})"
        return x

    def not_(self):
        if self.kw("not"):
            self.take()
            return f"(NOT {self.not_()})"
        return self.cmp()

    def cmp(self):
        x = self.concat()
        if self.kw("is"):
            self.take()
            neg = ""
            if self.kw("not"):
                self.take()
                neg = " NOT"
            assert self.kw("null"), self.text
            self.take()
            return f"({x} IS{neg} NULL)"
        k, v = self.peek()
        if k == "op" and v in ("=", "<>", "<", ">", "<=", ">="):
            self.take()
            return f"({x} {v} {self.concat()})"
        return x

    def concat(self):
        parts = [self.add()]
        while self.peek() == ("op", ":"):
            self.take()
            parts.append(self.add())
        if len(parts) == 1:
            return parts[0]
        return "concat(" + ", ".join(f"CAST({p} AS string)" for p in parts) + ")"

    def add(self):
        x = self.mul()
        while self.peek()[1] in ("+", "-") and self.peek()[0] == "op":
            op = self.take()[1]
            x = f"({x} {op} {self.mul()})"
        return x

    def mul(self):
        x = self.unary()
        while self.peek()[1] in ("*", "/") and self.peek()[0] == "op":
            op = self.take()[1]
            x = f"({x} {op} {self.unary()})"
        return x

    def unary(self):
        if self.peek() == ("op", "-"):
            self.take()
            return f"(-{self.primary()})"
        return self.primary()

    def primary(self):
        k, v = self.take()
        if k == "num":
            return v
        if k == "str":
            return "'" + v[1:-1].replace("\\\"", '"').replace("'", "''") + "'"
        if k == "op" and v == "(":
            x = self.expr()
            self.expect(")")
            return x
        if k == "id":
            if self.peek() == ("op", "("):
                self.take()
                args = []
                if self.peek() != ("op", ")"):
                    args.append(self.expr_arg())
                    while self.peek() == ("op", ","):
                        self.take()
                        args.append(self.expr_arg())
                self.expect(")")
                return self.func(v, args)
            if v in PARAMS:
                return "'{%s}'" % v
            return v.split(".")[-1]  # link.col -> col (each stage reads one view per input)
        raise ValueError(f"unexpected token {v!r} in {self.text!r}")

    def expr_arg(self):  # string-literal format arguments keep DataStage format text until func() sees them
        return self.expr()

    def func(self, name, a):
        n = name.lower()
        f = {
            "trim": lambda: f"trim({a[0]})", "upcase": lambda: f"upper({a[0]})", "downcase": lambda: f"lower({a[0]})",
            "len": lambda: f"length({a[0]})", "abs": lambda: f"abs({a[0]})", "left": lambda: f"left({a[0]}, {a[1]})",
            "nulltovalue": lambda: f"coalesce({a[0]}, {a[1]})",
            "stringtodecimal": lambda: f"CAST({a[0]} AS decimal(18,4))",
            "stringtodate": lambda: f"to_date({a[0]}, {self.fmt_lit(a[1])})",
            "stringtotimestamp": lambda: f"to_timestamp({a[0]}, {self.fmt_lit(a[1])})",
            "datetostring": lambda: f"date_format({a[0]}, {self.fmt_lit(a[1])})",
            "timestamptostring": lambda: f"date_format({a[0]}, {self.fmt_lit(a[1])})",
            "dayssincefromdate": lambda: f"datediff({a[0]}, {a[1]})",
            "datefromdayssince": lambda: f"date_add({a[1]}, {a[0]})",
            "ereplace": lambda: f"replace({a[0]}, {a[1]}, {a[2]})",
            "crc32": lambda: f"crc32({a[0]})",
            "isnull": lambda: f"({a[0]} IS NULL)",
        }.get(n)
        if f is None:
            raise ValueError(f"no Spark mapping for DataStage function {name} in {self.text!r}")
        return f()

    @staticmethod
    def fmt_lit(s):
        return "'" + fmt(s[1:-1]) + "'"


def sql(expr, order=None):
    return Expr(expr, order).parse()


# --------------------------------------------------------------------------------------------------------------
# Job -> notebook
# --------------------------------------------------------------------------------------------------------------
def tbl(path):
    return Path(path).stem


def is_src_file(path):
    return path.startswith("#pSrcDir#/")


class Compiler:
    def __init__(self, job):
        self.job, self.cells, self.order = job, [], {}  # order: link name -> ORDER BY sql or None

    def view(self, link, body, comment):
        self.cells.append(f'# {comment}\n{link} = spark.sql(f"""\n{body}\n""")\n{link}.createOrReplaceTempView("{link}")')

    def cols(self, lk, alias=""):
        a = alias + "." if alias else ""
        return ", ".join(f"{a}{c.name}" for c in lk.cols)

    def compile(self):
        for s in self.job.stages:
            getattr(self, "st_" + s.kind)(s)
        return self.render()

    # ---- stages ---------------------------------------------------------------------------------------------
    def st_seq_src(self, s):
        lk = s.outputs[0]
        path = s.seq["file"]
        if is_src_file(path):
            raw = Path(path).name
            sel = ",\n  ".join(self.read_col(c) for c in lk.cols)
            names = ", ".join(repr(c.name) for c in lk.cols)
            # DataStage maps file columns by position, not by header text: rename to the declared layout
            self.cells.append(
                f"# {s.name}: source file {raw} (columns mapped by position, as DataStage does)\n"
                f'_{lk.name}_raw = (spark.read.option("header", "true").option("escape", \'"\')\n'
                f'    .csv(f"{{src_dir}}/{raw}"))\n'
                f'assert len(_{lk.name}_raw.columns) == {len(lk.cols)}, '
                f'f"{raw}: expected {len(lk.cols)} columns, got {{len(_{lk.name}_raw.columns)}}"\n'
                f'_{lk.name}_raw = _{lk.name}_raw.toDF({names})\n'
                f'_{lk.name}_raw.createOrReplaceTempView("_{lk.name}_raw")\n'
                f'{lk.name} = spark.sql(f"""SELECT\n  {sel}\nFROM _{lk.name}_raw""")\n'
                f'{lk.name}.createOrReplaceTempView("{lk.name}")')
        else:
            self.cells.append(f'# {s.name}: Delta table {tbl(path)}\n'
                              f'{lk.name} = spark.table(f"{{catalog}}.{{schema}}.{tbl(path)}")\n'
                              f'{lk.name}.createOrReplaceTempView("{lk.name}")')
        self.order[lk.name] = None

    @staticmethod
    def read_col(c):
        t = SQL_TYPES[c.type]
        if c.type == "str":
            # DataStage reads an empty field of a non-nullable string column as "", Spark as NULL
            return f"`{c.name}` AS {c.name}" if c.nullable else f"coalesce(`{c.name}`, '') AS {c.name}"
        return f"CAST(`{c.name}` AS {t}) AS {c.name}"

    def st_xfm(self, s):
        inp, out = s.inputs[0], s.outputs[0]
        order = self.order.get(inp.name)
        parts = []
        for col, d in s.xfm_cols[out.name]:
            if re.fullmatch(r"\w+\.\w+", d):
                parts.append(col.name if d.split(".")[1] == col.name else f"{d.split('.')[1]} AS {col.name}")
            else:
                parts.append(f"CAST({sql(d, order)} AS {SQL_TYPES[col.type]}) AS {col.name}")
        self.view(out.name, "SELECT\n  " + ",\n  ".join(parts) + f"\nFROM {inp.name}", f"{s.name}: Transformer")
        names = set(out.names)
        self.order[out.name] = order if order and all(k in names for k in re.findall(r"\w+", order)
                                                      if k.upper() not in ("ASC", "DESC")) else None

    def st_filter(self, s):
        inp, out = s.inputs[0], s.outputs[0]
        where = dict(s.props)["where"].replace(r"\(2)\(2)0\(1)\(3)where\(2)", "")
        where = re.sub(r"\\\(2\)0$", "", where).replace('\\"', '"')
        self.view(out.name, f"SELECT * FROM {inp.name}\nWHERE {sql(where)}", f"{s.name}: Filter")
        self.order[out.name] = self.order.get(inp.name)

    def st_sort(self, s):
        inp, out = s.inputs[0], s.outputs[0]
        keys = re.findall(r"\\\(1\)\\\(3\)key\\\(2\)(\w+)", dict(s.props)["key"])
        order = []
        enc = dict(s.props)["key"]
        for k in keys:
            m = re.search(rf"key\\\(2\){k}\\\(2\)0(\\\(1\)\\\(3\)\\\(3\)asc\\\\desc\\\(2\)(\w+))?", enc)
            order.append(f"{k} {'DESC' if m and m.group(2) == 'desc' else 'ASC'}")
        self.order[out.name] = ", ".join(order)
        self.view(out.name, f"SELECT * FROM {inp.name}\nORDER BY {self.order[out.name]}", f"{s.name}: Sort")

    def st_remdup(self, s):
        inp, out = s.inputs[0], s.outputs[0]
        keys = re.findall(r"\\\(1\)\\\(3\)key\\\(2\)(\w+)", dict(s.props)["key"])
        order = self.order.get(inp.name) or "monotonically_increasing_id()"
        self.view(out.name,
                  f"SELECT {self.cols(out)} FROM (\n  SELECT *, row_number() OVER (PARTITION BY {', '.join(keys)} "
                  f"ORDER BY {order}) AS _rn FROM {inp.name}\n) WHERE _rn = 1", f"{s.name}: Remove Duplicates")
        self.order[out.name] = self.order.get(inp.name)

    def st_head(self, s):
        inp, out = s.inputs[0], s.outputs[0]
        n = dict(s.props)["nrecs"]
        o = self.order.get(inp.name)
        self.view(out.name, f"SELECT * FROM {inp.name}" + (f"\nORDER BY {o}" if o else "") + f"\nLIMIT {n}",
                  f"{s.name}: Head")
        self.order[out.name] = o

    def st_copy(self, s):
        inp = s.inputs[0]
        for out in s.outputs:
            self.view(out.name, f"SELECT * FROM {inp.name}", f"{s.name}: Copy")
            self.order[out.name] = self.order.get(inp.name)

    def st_funnel(self, s):
        out = s.outputs[0]
        body = "\nUNION ALL\n".join(f"SELECT {self.cols(out)} FROM {i.name}" for i in s.inputs)
        self.view(out.name, body, f"{s.name}: Funnel")
        self.order[out.name] = None

    def st_join(self, s):
        left, right = s.inputs
        out = s.outputs[0]
        op = dict(s.props)["operator"]
        keys = re.findall(r"\\\(1\)\\\(3\)key\\\(2\)(\w+)", dict(s.props)["key"])
        how = {"innerjoin": "INNER", "leftouterjoin": "LEFT OUTER", "rightouterjoin": "RIGHT OUTER",
               "fullouterjoin": "FULL OUTER"}[op]
        lnames = s.seq["left_names"]
        sel = ",\n  ".join(f"{'l' if c.name in lnames else 'r'}.{c.name}" for c in out.cols)
        on = " AND ".join(f"l.{k} = r.{k}" for k in keys)
        self.view(out.name, f"SELECT\n  {sel}\nFROM {left.name} l\n{how} JOIN {right.name} r ON {on}",
                  f"{s.name}: Join ({how.lower()}) on {', '.join(keys)}")
        self.order[out.name] = None

    def st_agg(self, s):
        inp, out = s.inputs[0], s.outputs[0]
        keys = re.findall(r"\\\(1\)\\\(3\)key\\\(2\)(\w+)", dict(s.props)["key"])
        if s.seq["mode"] == "count":
            cnt = out.cols[-1].name
            sel = ", ".join(keys + [f"CAST(COUNT(*) AS int) AS {cnt}"])
        else:
            red = dict(s.props)["reduce"]
            calc = []
            for srcc, rest in re.findall(r"reduce\\\(2\)(\w+)\\\(2\)0((?:\\\(1\)\\\(3\)\\\(3\)\w+\\\(2\)\w+\\\(2\)0)+)", red):
                for fn, o in re.findall(r"\\\(3\)\\\(3\)(\w+)\\\(2\)(\w+)\\\(2\)0", rest):
                    calc.append((fn, srcc, o))
            types = {c.name: c.type for c in out.cols}
            sel = ", ".join(keys + [f"CAST({fn.upper()}({sc}) AS {SQL_TYPES[types[o]]}) AS {o}" for fn, sc, o in calc])
        self.view(out.name, f"SELECT {sel}\nFROM {inp.name}\nGROUP BY {', '.join(keys)}", f"{s.name}: Aggregator")
        self.order[out.name] = None

    def st_seq_tgt(self, s):
        inp = s.inputs[0]
        mode = "append" if s.seq.get("append") else "overwrite"
        t = tbl(s.seq["file"])
        self.cells.append(f'# {s.name}: write Delta table {t}\n'
                          f'({inp.name}.select({", ".join(repr(c.name) for c in inp.cols)}).write.mode("{mode}")\n'
                          f'    .option("overwriteSchema", "true").saveAsTable(f"{{catalog}}.{{schema}}.{t}"))\n'
                          f'print("{t}:", spark.table(f"{{catalog}}.{{schema}}.{t}").count(), "rows")')

    # ---- notebook text --------------------------------------------------------------------------------------
    def render(self):
        j = self.job
        head = f'''# Databricks notebook source
# {j.name} - {j.short}
# Layer: {j.layer}. Compiled from the DataStage job by migration/ds2spark.py; do not edit by hand.

# COMMAND ----------
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "retail_dw")
dbutils.widgets.text("src_dir", "")
dbutils.widgets.text("pRunDate", "2011-12-31")
dbutils.widgets.text("pEffDate", "2011-07-01")
catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))
pRunDate, pEffDate = dbutils.widgets.get("pRunDate"), dbutils.widgets.get("pEffDate")
spark.sql(f"USE {{catalog}}.{{schema}}")
'''
        return head + "".join("\n# COMMAND ----------\n" + c + "\n" for c in self.cells)


class SdpCompiler(Compiler):
    """Same translation, emitted as a Spark Declarative Pipeline source file (pyspark.pipelines).

    Dataset names are pipeline-wide, so every link becomes a temporary view prefixed with the job code
    (j04_lnk_stores); every target file becomes a materialized view named after the file stem. Used to produce
    few-shot examples for the Switch SDP prompt (SCD2 jobs J05/J06 are converted differently, see README).
    """

    def __init__(self, job):
        super().__init__(job)
        self.prefix = job.name.split("_")[0].lower()
        self.links = sorted((lk.name for lk in job.links), key=len, reverse=True)

    def ref(self, text):
        for lk in self.links:
            text = re.sub(rf"\b{lk}\b", f"{self.prefix}_{lk}", text)
        return text

    def view(self, link, body, comment):
        name = f"{self.prefix}_{link}"
        self.cells.append(f'# {comment}\n@dp.temporary_view(name="{name}")\ndef {name}():\n'
                          f'    return spark.sql(f"""\n{self.ref(body)}\n""")')

    def st_seq_src(self, s):
        lk = s.outputs[0]
        path, name = s.seq["file"], f"{self.prefix}_{lk.name}"
        if is_src_file(path):
            raw = Path(path).name
            sel = ",\n        ".join(repr(self.read_col(c)) for c in lk.cols)
            names = ", ".join(repr(c.name) for c in lk.cols)
            self.cells.append(
                f"# {s.name}: source file {raw} (columns mapped by position, as DataStage does)\n"
                f'@dp.temporary_view(name="{name}")\ndef {name}():\n'
                f'    raw = spark.read.option("header", "true").option("escape", \'"\').csv(f"{{src_dir}}/{raw}")\n'
                f'    assert len(raw.columns) == {len(lk.cols)}, f"{raw}: expected {len(lk.cols)} columns, got {{len(raw.columns)}}"\n'
                f'    return raw.toDF({names}).selectExpr(\n        {sel})')
        else:
            self.cells.append(f'# {s.name}: dataset {tbl(path)}\n@dp.temporary_view(name="{name}")\ndef {name}():\n'
                              f'    return spark.read.table("{tbl(path)}")')
        self.order[lk.name] = None

    def st_seq_tgt(self, s):
        inp, t = s.inputs[0], tbl(s.seq["file"])
        self.cells.append(f'# {s.name}: target file -> materialized view {t}\n@dp.materialized_view(name="{t}")\n'
                          f'def {t}():\n    return spark.read.table("{self.prefix}_{inp.name}").select('
                          f'{", ".join(repr(c.name) for c in inp.cols)})')

    def render(self):
        j = self.job
        head = f'''# Databricks notebook source
# {j.name} - {j.short}
# Layer: {j.layer}. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")
'''
        return head + "".join("\n# COMMAND ----------\n" + c + "\n" for c in self.cells)


def main():
    sdp = "--sdp" in sys.argv
    out = ROOT / "migration" / "sdp_reference" if sdp else OUT
    out.mkdir(parents=True, exist_ok=True)
    jobs = P.build_all()
    for j in jobs:
        (out / f"{j.name}.py").write_text((SdpCompiler if sdp else Compiler)(j).compile())
        print("wrote", j.name)
    print(len(jobs), "SDP source files" if sdp else "notebooks", "->", out)


if __name__ == "__main__":
    main()
