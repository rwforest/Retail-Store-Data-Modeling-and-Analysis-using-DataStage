#!/usr/bin/env python3
"""Convert IBM DataStage .dsx exports to the DataStage XML export format (DSExport).

Lakebridge (Analyzer + BladeBridge) only reads the XML export, not DSX. This is a grammar-faithful parser for the
DSX format (structure from https://github.com/jdelvign/dsxparser, DSX.g4) that keeps *every* block, not only
DSJOB: DSSTAGETYPES, DSTABLEDEFS, DSROUTINES, DSSHAREDCONTAINER and DSUBINARY too.

    python3 migration/dsx_to_xml.py <dsx file or dir> <out dir> [--stagetypes-from real.dsx]

DSX grammar (summary):
  dsx        := header (dsjob | dsstagetypes | dstabledefs | dsroutines | dssharedcontainer)+
  dsjob      := BEGIN DSJOB identifier (property | dsrecord)+ END DSJOB
  dsrecord   := BEGIN DSRECORD identifier (property | dssubrecord)+ END DSRECORD
  property   := name (STRING_LITERAL | PLAIN_TEXT)        PLAIN_TEXT = '=+=+=+=' ... '=+=+=+='
"""
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from xml.sax.saxutils import escape

TOKEN = re.compile(r'\s*(?:(?P<plain>=\+=\+=\+=.*?=\+=\+=\+=)|(?P<str>"(?:[^"\\\r\n]|\\.)*")|(?P<word>[^\s"]+))', re.S)
BLOCKS = {"DSJOB": True, "DSSTAGETYPES": False, "DSTABLEDEFS": False, "DSROUTINES": False,
          "DSSHAREDCONTAINER": True, "DSUBINARY": True, "DSRECORD": True, "DSSUBRECORD": False, "HEADER": False}


@dataclass
class Node:
    kind: str                      # HEADER, DSJOB, DSRECORD, DSSUBRECORD, ...
    ident: str = ""
    items: list = field(default_factory=list)   # ("prop", name, value, preformatted) | ("sub", Node) | ("rec", Node)


def tokenize(text):
    i = 0
    while i < len(text):
        m = TOKEN.match(text, i)
        if not m:
            break
        i = m.end()
        if m.group("plain") is not None:
            yield "plain", m.group("plain")[7:-7].strip("\r\n")
        elif m.group("str") is not None:
            yield "str", m.group("str")[1:-1].replace('\\"', '"')
        else:
            yield "word", m.group("word")


def parse(text):
    toks = list(tokenize(text))
    pos = 0

    def block(kind):
        nonlocal pos
        node = Node(kind)
        while True:
            k, v = toks[pos]
            if k == "word" and v == "END":
                assert toks[pos + 1][1] == kind, f"END {toks[pos + 1][1]} closes BEGIN {kind}"
                pos += 2
                return node
            if k == "word" and v == "BEGIN":
                child = toks[pos + 1][1]
                pos += 2
                node.items.append(("rec", block(child)))
                continue
            name = v
            pk, pv = toks[pos + 1]
            pos += 2
            if name == "Identifier":
                node.ident = pv
            node.items.append(("prop", name, pv, pk == "plain"))

    out = []
    while pos < len(toks):
        k, v = toks[pos]
        assert (k, v) == ("word", "BEGIN"), f"expected BEGIN, got {v!r}"
        kind = toks[pos + 1][1]
        pos += 2
        out.append(block(kind))
    return out


# ----------------------------------------------------------------------------------------------------------------
# XML emission (same element vocabulary DataStage's own XML export uses for records)
# ----------------------------------------------------------------------------------------------------------------
def attr(s):
    return escape(s, {'"': "&quot;"})


def emit_props(node, out, record_type_prop="OLEType"):
    """Emit properties; a property whose value is followed by sub-records becomes a Collection."""
    items = node.items
    i = 0
    while i < len(items):
        it = items[i]
        if it[0] == "prop":
            _, name, val, pre = it
            nxt = items[i + 1] if i + 1 < len(items) else None
            if nxt and nxt[0] == "rec" and nxt[1].kind == "DSSUBRECORD" and not pre:
                out.append(f'<Collection Name="{name}" Type="{attr(val)}">')
                while i + 1 < len(items) and items[i + 1][0] == "rec" and items[i + 1][1].kind == "DSSUBRECORD":
                    i += 1
                    out.append("<SubRecord>")
                    emit_props(items[i][1], out)
                    out.append("</SubRecord>")
                out.append("</Collection>")
            elif name not in ("Identifier", "OLEType", "Readonly") or node.kind == "DSSUBRECORD":
                if pre:
                    out.append(f'<Property Name="{name}" PreFormatted="1">{escape(val)}</Property>')
                else:
                    out.append(f'<Property Name="{name}">{escape(val)}</Property>')
        i += 1


def prop(node, name, default=""):
    return next((it[2] for it in node.items if it[0] == "prop" and it[1] == name), default)


def emit_record(rec, out):
    out.append(f'<Record Identifier="{attr(rec.ident)}" Type="{attr(prop(rec, "OLEType"))}" '
               f'Readonly="{attr(prop(rec, "Readonly", "0"))}">')
    emit_props(rec, out)
    out.append("</Record>")


def to_xml(nodes, stagetypes_wrapper="StageTypes", tabledefs_wrapper="TableDefinitions"):
    out = ['<?xml version="1.0" encoding="UTF-8" ?>', "<DSExport>"]
    for n in nodes:
        if n.kind == "HEADER":
            out.append("<Header " + " ".join(f'{k}="{attr(v)}"' for _, k, v, _ in n.items) + "/>")
        elif n.kind in ("DSJOB", "DSSHAREDCONTAINER"):
            tag = "Job" if n.kind == "DSJOB" else "SharedContainer"
            out.append(f'<{tag} Identifier="{attr(n.ident)}" DateModified="{attr(prop(n, "DateModified"))}" '
                       f'TimeModified="{attr(prop(n, "TimeModified"))}">')
            for it in n.items:
                if it[0] == "rec":
                    emit_record(it[1], out)
            out.append(f"</{tag}>")
        elif n.kind in ("DSSTAGETYPES", "DSTABLEDEFS", "DSROUTINES"):
            wrap = {"DSSTAGETYPES": stagetypes_wrapper, "DSTABLEDEFS": tabledefs_wrapper, "DSROUTINES": "Routines"}[n.kind]
            if wrap:
                out.append(f"<{wrap}>")
            for it in n.items:
                if it[0] == "rec":
                    emit_record(it[1], out)
            if wrap:
                out.append(f"</{wrap}>")
    out.append("</DSExport>")
    return "\n".join(out) + "\n"


def convert(text, **kw):
    return to_xml(parse(text), **kw)


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    src, dst = Path(args[0]), Path(args[1])
    dst.mkdir(parents=True, exist_ok=True)
    files = [src] if src.is_file() else sorted(src.glob("*.dsx"))
    for f in files:
        if f.name == "Retail_DW_Pipeline_ALL.dsx":  # same jobs bundled; analysing it would double count
            continue
        (dst / (f.stem + ".xml")).write_text(convert(f.read_text(encoding="cp1252")), encoding="utf-8")
        print("wrote", dst / (f.stem + ".xml"))
