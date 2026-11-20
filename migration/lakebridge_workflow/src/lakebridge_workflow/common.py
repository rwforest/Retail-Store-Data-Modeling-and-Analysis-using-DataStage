#!/usr/bin/env python3
"""Shared helpers for the repository-aware Lakebridge analysis and migration tools.

The Windows guide was written for a very large DataStage XML export.  This repository
does not contain that export; it contains the 23 retail parallel jobs and one master
sequence as condensed ``switch_input/*.txt`` specifications.  These helpers let the
same workflow operate on those jobs now, while the XML scripts remain available for
the original export when it is supplied later.

Only Python's standard library is required.  Excel workbooks are written with inline
strings (no openpyxl dependency), which is sufficient for migration inventory reports.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from xml.etree import ElementTree as ET
from zipfile import ZIP_DEFLATED, ZipFile
import hashlib
import json
import os
import re
import tempfile


STAGE_RE = re.compile(r"^STAGE\s+(.+?)\s+\[([^\]]+)\]\s*$")
LINK_RE = re.compile(r"^(in|out)\s+link\s+(.+?)\s+<->\s+(.+?)\s*$")
PROPERTY_RE = re.compile(r"^property\s+(.+?)\s*=\s*(.*)$")
COL_RE = re.compile(r"^col\s+(\S+)\s+sql=(\S+)\s+prec=(\S+),(\S+)\s+nullable=(\S+)(?:\s+:=\s+(.*))?$")
PARAM_RE = re.compile(r"^\s*-\s+(\S+)(?:\s+\(default\s+(.*?)\))?(?:\s*:\s*(.*))?$")
BACKUP_RE = re.compile(r"(?:^copyof|_(?:orig|old|bak|bkp|test)$)", re.IGNORECASE)


@dataclass
class Link:
    direction: str
    link_name: str
    partner: str
    properties: list[tuple[str, str]] = field(default_factory=list)
    columns: list[dict[str, Any]] = field(default_factory=list)

    @property
    def schema_size(self) -> int:
        return len(self.columns)


@dataclass
class Stage:
    name: str
    kind: str
    properties: list[tuple[str, str]] = field(default_factory=list)
    links: list[Link] = field(default_factory=list)

    @property
    def prop_map(self) -> dict[str, str]:
        return {k: v for k, v in self.properties}


@dataclass
class CondensedJob:
    name: str
    kind: str
    description: str = ""
    folder: str = ""
    parameters: list[dict[str, str]] = field(default_factory=list)
    stages: list[Stage] = field(default_factory=list)
    source_path: Path | None = None

    @property
    def type(self) -> str:
        return "SEQUENCE" if self.kind == "SEQUENCE" else "PARALLEL"

    @property
    def nodes(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for st in self.stages:
            counts[st.kind] = counts.get(st.kind, 0) + 1
        return dict(sorted(counts.items()))

    @property
    def total_nodes(self) -> int:
        return sum(self.nodes.values())

    @property
    def link_count(self) -> int:
        return sum(len(st.links) for st in self.stages)

    @property
    def parameter_count(self) -> int:
        return len(self.parameters)

    @property
    def expression_count(self) -> int:
        return sum(1 for st in self.stages for lk in st.links
                   for col in lk.columns if col.get("derivation"))


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def atomic_write(path: Path | str, text: str, encoding: str = "utf-8") -> None:
    """Write text atomically, preserving a UTF-8 BOM only when explicitly included."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent), text=True)
    try:
        with os.fdopen(fd, "w", encoding=encoding, newline="") as fh:
            fh.write(text)
        os.replace(tmp, path)
        os.chmod(path, 0o644)
    finally:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass


def atomic_json(path: Path | str, obj: Any, indent: int = 2) -> None:
    atomic_write(path, json.dumps(obj, indent=indent, ensure_ascii=False) + "\n")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def read_condensed_job(path: Path | str) -> CondensedJob:
    """Parse the compact specification emitted by ``dsx_condense.py``.

    The format is intentionally line-oriented and preserves enough information for
    inventory, lineage, expressions and batch planning without a DataStage export.
    """
    path = Path(path)
    text = path.read_text(encoding="utf-8", errors="replace")
    header = text.splitlines()[0] if text.splitlines() else ""
    match = re.match(r"DATASTAGE\s+(PARALLEL|SEQUENCE)\s+JOB:\s*(.+?)\s*$", header)
    if not match:
        raise ValueError(f"{path}: not a condensed DataStage job specification")
    job = CondensedJob(name=match.group(2).strip(), kind=match.group(1).upper(), source_path=path)
    stage: Stage | None = None
    link: Link | None = None

    for line in text.splitlines()[1:]:
        if not line.strip():
            stage = link = None
            continue
        if line.startswith("DATASTAGE "):
            stage = link = None
            continue
        if line.startswith("Description:"):
            job.description = line.split(":", 1)[1].strip()
            continue
        if line.startswith("Folder:"):
            job.folder = line.split(":", 1)[1].strip()
            continue
        m = STAGE_RE.match(line)
        if m:
            stage = Stage(name=m.group(1), kind=m.group(2))
            job.stages.append(stage)
            link = None
            continue
        if stage and not line.startswith("    "):
            # Unknown top-level section; do not treat it as a stage property.
            continue
        pm = PARAM_RE.match(line) if stage is None else None
        if pm:
            job.parameters.append({
                "name": pm.group(1), "default": (pm.group(2) or "").strip("'\""),
                "prompt": (pm.group(3) or "").strip(),
            })
            continue
        if stage is None:
            continue
        lm = LINK_RE.match(line.strip())
        if lm:
            link = Link(direction=lm.group(1), link_name=lm.group(2), partner=lm.group(3))
            stage.links.append(link)
            continue
        if not line.startswith("        "):
            pm = PROPERTY_RE.match(line.strip())
            if pm and stage:
                stage.properties.append((pm.group(1), pm.group(2).strip()))
            continue
        if link is None:
            continue
        pm = PROPERTY_RE.match(line.strip())
        if pm:
            link.properties.append((pm.group(1), pm.group(2).strip()))
            continue
        cm = COL_RE.match(line.strip())
        if cm:
            link.columns.append({
                "name": cm.group(1), "sqlType": int(cm.group(2)), "precision": int(cm.group(3)),
                "scale": int(cm.group(4)), "nullable": cm.group(5) == "1",
                "derivation": cm.group(6) or "",
            })
    if not job.stages:
        raise ValueError(f"{path}: no stages parsed")
    return job


def iter_condensed_jobs(directory: Path | str, pattern: str = "*.txt") -> Iterable[CondensedJob]:
    root = Path(directory)
    for path in sorted(root.glob(pattern)):
        if not path.is_file():
            continue
        try:
            job = read_condensed_job(path)
        except ValueError as exc:
            print(f"warning: {exc}")
            continue
        if job.name != path.stem:
            print(f"warning: {path}: header name {job.name!r} differs from file stem {path.stem!r}; using header name")
        yield job


def inferred_subjob_edges(jobs: Iterable[CondensedJob]) -> list[dict[str, Any]]:
    """Infer sequence -> job edges from condensed ``CJSJobActivity`` stage names.

    A real analyzer derives this from ``subJobInfo`` in the XML.  ``dsx_condense.py``
    preserves activity names; in this repository they are ``run_<job name>``.  If a
    stage has a ``JobName`` property, that property wins.
    """
    jobs = list(jobs)
    names = {j.name for j in jobs}
    out: list[dict[str, Any]] = []
    for parent in jobs:
        if parent.type != "SEQUENCE":
            continue
        for stage in parent.stages:
            if stage.kind != "CJSJobActivity":
                continue
            child = stage.prop_map.get("JobName") or stage.name
            if child.startswith("run_"):
                child = child[4:]
            if child in names:
                out.append({
                    "parent": parent.name, "parentType": parent.type,
                    "child": child, "childType": "PARALLEL", "count": 1,
                    "activity": stage.name,
                })
    return out


def complexity_from_nodes(total_nodes: int, expressions: int = 0) -> str:
    """Repository-compatible complexity heuristic.

    The checked-in analyzer report contains the authoritative values when available;
    this heuristic is used only for jobs that are not in that workbook.
    """
    score = total_nodes + expressions * 2
    if score >= 60:
        return "VERY_HIGH"
    if score >= 25:
        return "HIGH"
    if score >= 10:
        return "MEDIUM"
    return "LOW"


def is_backup_name(name: str) -> bool:
    return bool(BACKUP_RE.search(name))


# dsx_condense.py renders some condensed properties (``where``, ``key``, ...) verbatim from
# the DSX export's raw Value field, which for these is DataStage's own BASIC/UniVerse
# multi-value list encoding (``\(1)``/``\(2)``/``\(3)`` are its field/value/subvalue marks)
# rather than plain text -- see migration/README.md §8a.7 on how little is documented about
# this format. Unwrap the common single-value shape so callers see the real value; a
# multi-value shape (several marks in the same property, e.g. a multi-column join key) is
# left exactly as condensed, since mangling that silently would be worse than returning the
# raw text unchanged. Shared by optimize.py (report readability) and fix_dbxconv_output.py
# (it needs the real file path / sort key out of these same properties to patch generated
# code, not just display it). The inner tag name is sometimes followed by a stray space
# before its closing mark (seen on a Sequential File source link's own "file" property,
# not on an otherwise-identical target link's) -- an inconsistency in the raw DSX itself,
# not a condensing bug, so the pattern tolerates it rather than requiring either shape.
DSX_PROPERTY_WRAPPER_RE = re.compile(
    r"^\\\(2\)\\\(2\)0\\\(1\)\\\(3\)\w+ ?\\\(2\)(?P<value>(?:(?!\\\(1\)).)*)\\\(2\)0$",
    re.IGNORECASE | re.DOTALL,
)


def unwrap_dsx_value(value: str) -> str:
    match = DSX_PROPERTY_WRAPPER_RE.match(value.strip())
    return match.group("value") if match else value


# Minimal SpreadsheetML writer.  This avoids a hard dependency on openpyxl and emits
# ordinary inline-string cells, which Excel, Numbers and pandas can read.
_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


def _cell_xml(value: Any, row: int, col: int) -> str:
    ref = ""
    n = col
    while n:
        n, rem = divmod(n - 1, 26)
        ref = chr(65 + rem) + ref
    ref += str(row)
    if value is None:
        return f'<c r="{ref}" />'
    if isinstance(value, bool):
        return f'<c r="{ref}" t="b"><v>{1 if value else 0}</v></c>'
    if isinstance(value, (int, float)):
        return f'<c r="{ref}"><v>{value}</v></c>'
    text = str(value)
    return f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">{_xml_escape(text)}</t></is></c>'


def _xml_escape(text: str) -> str:
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;").replace("\n", "&#10;"))


def write_xlsx(path: Path | str, sheets: list[tuple[str, list[list[Any]]]]) -> None:
    """Write a dependency-free XLSX workbook with one table per worksheet."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not sheets:
        raise ValueError("cannot write an empty workbook")

    content_types = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">',
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
        '<Default Extension="xml" ContentType="application/xml"/>',
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
    ]
    workbook_sheets = []
    workbook_rels = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
                     f'<Relationships xmlns="{_REL_NS}">']
    sheet_xmls: list[str] = []

    for i, (name, rows) in enumerate(sheets, 1):
        safe_name = re.sub(r"[][:*?/\\]", "_", name)[:31] or f"Sheet{i}"
        content_types.append(
            f'<Override PartName="/xl/worksheets/sheet{i}.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>')
        workbook_sheets.append(f'<sheet name="{_xml_escape(safe_name)}" sheetId="{i}" r:id="rId{i}"/>')
        workbook_rels.append(
            f'<Relationship Id="rId{i}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{i}.xml"/>')
        tr = []
        for r, row in enumerate(rows or [[]], 1):
            cells = []
            last = 0
            for c, value in enumerate(row, 1):
                cells.append(_cell_xml(value, r, c))
                last = c
            while last < 1:
                cells.append(_cell_xml("", r, 1))
                last = 1
            tr.append(f'<row r="{r}">{"".join(cells)}</row>')
        sheet_xmls.append('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                           f'<worksheet xmlns="{_NS}"><sheetData>{"".join(tr)}</sheetData></worksheet>')
    content_types.append('<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>')
    content_types.append('</Types>')
    workbook_rels.append(
        f'<Relationship Id="rId{len(sheets)+1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>')
    workbook_rels.append('</Relationships>')
    workbook = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
                f'<sheets>{"".join(workbook_sheets)}</sheets></workbook>')
    styles = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
              f'<styleSheet xmlns="{_NS}"><fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>'
              '<fills count="1"><fill><patternFill patternType="none"/></fill></fills>'
              '<borders count="1"><border/></borders><cellStyleXfs count="1"><xf/></cellStyleXfs>'
              '<cellXfs count="1"><xf/></cellXfs></styleSheet>')

    root_rels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                 f'<Relationships xmlns="{_REL_NS}"><Relationship Id="rId1" '
                 'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
                 'Target="xl/workbook.xml"/></Relationships>')

    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    os.close(fd)
    try:
        with ZipFile(tmp, "w", ZIP_DEFLATED) as zf:
            zf.writestr("[Content_Types].xml", "".join(content_types))
            zf.writestr("_rels/.rels", root_rels)
            zf.writestr("xl/workbook.xml", workbook)
            zf.writestr("xl/_rels/workbook.xml.rels", "".join(workbook_rels))
            zf.writestr("xl/styles.xml", styles)
            for i, xml in enumerate(sheet_xmls, 1):
                zf.writestr(f"xl/worksheets/sheet{i}.xml", xml)
        os.replace(tmp, path)
        os.chmod(path, 0o644)
    finally:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass


def read_xlsx_rows(path: Path | str, sheet: str | None = None) -> dict[str, list[list[Any]]]:
    """Read inline-string XLSX values used by this project and the checked-in analyzer report."""
    path = Path(path)
    ns = {"a": _NS}
    with ZipFile(path) as zf:
        wb = ET.fromstring(zf.read("xl/workbook.xml"))
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rel_map = {r.attrib["Id"]: r.attrib["Target"] for r in rels}
        result: dict[str, list[list[Any]]] = {}
        for sh in wb.findall(".//a:sheet", ns):
            name = sh.attrib["name"]
            if sheet is not None and name != sheet:
                continue
            rid = sh.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            target = rel_map.get(rid, "")
            part = target[1:] if target.startswith("/") else "xl/" + target
            root = ET.fromstring(zf.read(part))
            rows: list[list[Any]] = []
            for row in root.findall(".//a:row", ns):
                vals: list[Any] = []
                expected = 1
                for c in row.findall("a:c", ns):
                    ref = c.attrib.get("r", "")
                    letters = "".join(ch for ch in ref if ch.isalpha())
                    colnum = 0
                    for ch in letters:
                        colnum = colnum * 26 + ord(ch.upper()) - 64
                    if colnum > len(vals):
                        vals.extend([""] * (colnum - len(vals) - 1))
                    text = "".join(t.text or "" for t in c.findall(".//a:t", ns))
                    v = c.find("a:v", ns)
                    if not text and v is not None and v.text is not None:
                        text = v.text
                    if colnum > len(vals):
                        vals.append(text)
                    else:
                        vals[colnum - 1] = text
                    expected = max(expected, colnum)
                rows.append(vals)
            result[name] = rows
        return result


def csv_escape(value: Any) -> str:
    s = "" if value is None else str(value)
    if any(ch in s for ch in (",", '"', "\n", "\r")):
        return '"' + s.replace('"', '""') + '"'
    return s


def rows_to_csv(rows: list[list[Any]]) -> str:
    return "\n".join(",".join(csv_escape(v) for v in row) for row in rows) + "\n"
