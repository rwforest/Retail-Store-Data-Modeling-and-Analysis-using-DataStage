#!/usr/bin/env python3
"""Stream, profile and split a large DataStage ``DSExport`` XML or ``.txt`` export.

The repository contains condensed jobs, not the original 1.2 GB export.  This script is
nevertheless ready for that export: it uses incremental XML parsing, keeps only one
top-level record in memory, and emits valid ``<DSExport>`` chunks that Lakebridge can
analyze independently.

Examples
--------
Profile only::

    lakebridge-workflow split EFBI_PRD_for_Sequence_Job_Only.txt --profile-only

Split sequence jobs::

    lakebridge-workflow split EFBI_PRD_for_Sequence_Job_Only.txt \\
        --out-dir seq_only_chunks --jobs-per-file 500 --only-job-type 2

Split ETL jobs::

    lakebridge-workflow split EFBI_PRD_for_Sequence_Job_Only.txt \\
        --out-dir etl_only_chunks --jobs-per-file 500 --exclude-job-type 2 --skip-table-defs
"""
from __future__ import annotations

import argparse
import os
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.sax.saxutils import quoteattr

from .common import atomic_json


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Streaming DataStage XML profiler and splitter")
    p.add_argument("source", type=Path, help="Input .xml or XML-in-.txt DSExport file")
    p.add_argument("--profile-only", action="store_true", help="Count tags/job types without writing chunks")
    p.add_argument("--out-dir", type=Path, help="Output directory for chunk_*.xml files")
    p.add_argument("--jobs-per-file", type=int, default=500, metavar="N",
                   help="Jobs per output XML chunk (default: 500; must be positive)")
    p.add_argument("--only-job-type", type=int, metavar="TYPE",
                   help="Keep only this JobType (2=sequence, 3=parallel, 0=server)")
    p.add_argument("--exclude-job-type", type=int, metavar="TYPE", help="Exclude this JobType")
    p.add_argument("--skip-table-defs", action="store_true", help="Omit DSTABLEDEFS-like non-job records from chunks")
    p.add_argument("--no-non-jobs", action="store_true",
                   help="Omit ParameterSets, Routines and other non-job records (they are included by default)")
    p.add_argument("--profile-json", type=Path, help="Optional JSON profile output path")
    p.add_argument("--quiet", action="store_true", help="Do not print progress per chunk")
    args = p.parse_args(argv)
    if args.only_job_type is not None and args.exclude_job_type is not None:
        p.error("--only-job-type and --exclude-job-type are mutually exclusive")
    if args.jobs_per_file <= 0:
        p.error("--jobs-per-file must be positive")
    if not args.profile_only and not args.out_dir:
        p.error("--out-dir is required unless --profile-only is used")
    return args


def _text(value: ET.Element | None) -> str:
    return (value.text or "").strip() if value is not None else ""


def job_name(elem: ET.Element) -> str:
    """Extract a stable job name from common IBM DataStage XML shapes."""
    for key in ("Identifier", "Name", "JobName"):
        if key in elem.attrib:
            return elem.attrib[key].strip()
    for key in ("Identifier", "Name", "JobName"):
        value = _text(elem.find(f".//{key}"))
        if value:
            return value
    for child in elem.iter():
        if child.tag in ("Identifier", "Name") and child.keys():
            for key in ("Identifier", "Name", "VALUE"):
                if key in child.attrib:
                    return child.attrib[key].strip()
    return elem.attrib.get("id", f"job_{id(elem):x}")


def job_type(elem: ET.Element) -> int | None:
    """Return JobType from either an XML attribute or a nested DSExport property."""
    raw = elem.attrib.get("JobType")
    if raw is None:
        for child in elem.iter("Property"):
            if child.attrib.get("Name") == "JobType" and child.text:
                raw = child.text
                break
    if raw is None:
        for child in elem.iter():
            if child.tag in ("Job", "Record", "ROOT"):
                raw = child.attrib.get("JobType")
                if raw is not None:
                    break
    try:
        return int(str(raw).strip()) if raw is not None else None
    except ValueError:
        return None


def selected(jtype: int | None, args: argparse.Namespace) -> bool:
    if args.only_job_type is not None:
        return jtype == args.only_job_type
    if args.exclude_job_type is not None:
        return jtype != args.exclude_job_type
    return True


def root_open_tag(root: ET.Element) -> str:
    attrs = "".join(f" {k}={quoteattr(v)}" for k, v in root.attrib.items())
    return f"<{root.tag}{attrs}>"


def iter_top_level(source: Path):
    """Yield ``(tag, element, depth-1 tag)`` for each direct child of DSExport."""
    context = ET.iterparse(str(source), events=("start", "end"))
    root = None
    stack: list[ET.Element] = []
    try:
        for event, elem in context:
            if event == "start":
                if root is None:
                    root = elem
                stack.append(elem)
            else:
                stack.pop()
                if root is not None and elem is not root and len(stack) == 1:
                    parent_tag = stack[0].tag
                    yield parent_tag, elem, root
                    # Keep only the root skeleton alive. This is the key to handling
                    # multi-GB exports with bounded memory.
                    root.clear()
    except ET.ParseError as exc:
        raise SystemExit(f"error: {source}: invalid XML: {exc}") from exc


class ChunkWriter:
    """Atomic DSExport chunk writer."""

    def __init__(self, out_dir: Path, prefix: str, root_open: str, root_tag: str, quiet: bool):
        self.out_dir = out_dir
        self.prefix = prefix
        self.root_open = root_open
        self.root_tag = root_tag
        self.header_xml = ""
        self.quiet = quiet
        self.index = 0
        self.count = 0
        self.jobs = 0
        self.files: list[Path] = []
        self.handle = None
        self.tmp = None
        self.current: Path | None = None

    def _open(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.index += 1
        self.current = self.out_dir / f"{self.prefix}_{self.index:04d}.xml"
        fd, tmp = tempfile.mkstemp(prefix=f".{self.current.name}.", dir=self.out_dir)
        self.tmp = Path(tmp)
        self.handle = os.fdopen(fd, "w", encoding="utf-8", newline="")
        self.handle.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        self.handle.write(self.root_open + "\n")
        if self.header_xml:
            self.handle.write(self.header_xml + "\n")
        self.count = 0
        if not self.quiet:
            print(f"opening {self.current}", flush=True)

    def add(self, xml: str):
        if self.handle is None:
            self._open()
        self.handle.write(xml + "\n")
        self.count += 1
        self.jobs += 1

    def roll(self):
        if self.handle is not None:
            self._close()

    def _close(self):
        self.handle.write(f"</{self.root_tag}>\n")
        self.handle.close()
        os.replace(self.tmp, self.current)
        self.files.append(self.current)
        self.handle = None
        self.tmp = None

    def finalize(self):
        if self.handle is not None:
            self._close()
        return self.files

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.handle is not None:
            try:
                self.handle.close()
            finally:
                try:
                    os.unlink(self.tmp)
                except (TypeError, FileNotFoundError):
                    pass
        return False


def run(args: argparse.Namespace) -> int:
    source = args.source
    if not source.is_file():
        raise SystemExit(f"error: source not found: {source}")
    if source.stat().st_size == 0:
        raise SystemExit(f"error: source is empty: {source}")

    top_counts: Counter[str] = Counter()
    top_sizes: defaultdict[str, int] = defaultdict(int)
    top_examples: defaultdict[str, list[str]] = defaultdict(list)
    job_type_counts: Counter[str] = Counter()
    job_type_sizes: defaultdict[str, int] = defaultdict(int)
    header_xml = ""
    open_tag = "<DSExport>"
    nonjobs: list[tuple[str, str]] = []
    selected_jobs = 0
    selected_bytes = 0

    writer = None
    files: list[Path] = []

    for _parent, elem, root in iter_top_level(source):
        if not open_tag.startswith("<DSExport ") and root.attrib:
            open_tag = root_open_tag(root)
        if writer is None and not args.profile_only:
            writer = ChunkWriter(args.out_dir, "chunk", root_open_tag(root), root.tag, args.quiet)
        if elem.tag == "Header" and not header_xml:
            header_xml = ET.tostring(elem, encoding="unicode")
            if writer is not None:
                writer.header_xml = header_xml
        xml = ET.tostring(elem, encoding="unicode")
        size = len(xml.encode("utf-8"))
        top_counts[elem.tag] += 1
        top_sizes[elem.tag] += size
        if len(top_examples[elem.tag]) < 3:
            top_examples[elem.tag].append(job_name(elem))

        if elem.tag == "Job":
            jtype = job_type(elem)
            label = str(jtype) if jtype is not None else "UNKNOWN"
            job_type_counts[label] += 1
            job_type_sizes[label] += size
            if not args.profile_only and selected(jtype, args):
                selected_jobs += 1
                selected_bytes += size
                if writer.count >= args.jobs_per_file:
                    writer.roll()
                writer.add(xml)
        elif elem.tag == "Header":
            pass
        elif not args.profile_only and not args.no_non_jobs:
            tag_lower = elem.tag.lower()
            if not (args.skip_table_defs and ("tabledef" in tag_lower or tag_lower == "dstabledefs")):
                nonjobs.append((elem.tag, xml))

    if writer is not None:
        # Non-job records are small enough to collect in the exports described by the
        # guide. They are appended to the active chunk (or a final one) after streaming.
        # Keep auxiliary records in the active chunk; do not create an empty job-less chunk.
        files = writer.finalize()
        if nonjobs:
            # Metadata records are useful to every analyzer worker. Append them by streaming
            # each completed chunk to a temporary file; do not load a 50-150 MiB chunk in RAM.
            metadata = "".join(xml + "\n" for _tag, xml in nonjobs).encode("utf-8")
            closing = b"</" + root.tag.encode("utf-8") + b">"
            for path in files:
                fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.meta.", dir=str(path.parent))
                tmp = Path(tmp_name)
                try:
                    with path.open("rb") as src, os.fdopen(fd, "wb") as dst:
                        appended = False
                        for line in src:
                            if not appended and line.rstrip().endswith(closing):
                                dst.write(metadata)
                                dst.write(line)
                                appended = True
                            else:
                                dst.write(line)
                    if not appended:
                        raise RuntimeError(f"missing closing {root.tag!r} in {path}")
                    os.replace(tmp, path)
                finally:
                    try:
                        tmp.unlink()
                    except FileNotFoundError:
                        pass

    profile = {
        "source": str(source),
        "sourceBytes": source.stat().st_size,
        "topLevelTags": [
            {"tag": tag, "count": top_counts[tag], "approxBytes": top_sizes[tag],
             "examples": top_examples[tag]}
            for tag in sorted(top_counts)
        ],
        "jobTypes": [
            {"jobType": label, "count": job_type_counts[label], "approxBytes": job_type_sizes[label]}
            for label in sorted(job_type_counts)
        ],
        "selectedJobs": selected_jobs if not args.profile_only else None,
        "selectedApproxBytes": selected_bytes if not args.profile_only else None,
        "chunks": [
            {"path": str(p), "bytes": p.stat().st_size} for p in (writer.files if writer else [])
        ] if not args.profile_only else [],
        "settings": {
            "jobsPerFile": args.jobs_per_file, "onlyJobType": args.only_job_type,
            "excludeJobType": args.exclude_job_type, "skipTableDefs": args.skip_table_defs,
            "includeNonJobs": not args.no_non_jobs,
        },
    }
    if args.profile_json:
        atomic_json(args.profile_json, profile)
    if args.profile_only:
        print(f"Profiled {source} ({source.stat().st_size / 1024 / 1024:.1f} MiB)")
        print("Top-level XML tags:")
        for tag in sorted(top_counts):
            print(f"  {tag:<20} {top_counts[tag]:>8}  {top_sizes[tag] / 1024 / 1024:>9.1f} MiB")
        print("Job types:")
        for label in sorted(job_type_counts):
            print(f"  JobType={label:<7} {job_type_counts[label]:>8}  {job_type_sizes[label] / 1024 / 1024:>9.1f} MiB")
    else:
        print(f"Selected {selected_jobs} jobs ({selected_bytes / 1024 / 1024:.1f} MiB) into {len(files)} chunks")
        for path in files:
            print(f"  {path} ({path.stat().st_size / 1024 / 1024:.1f} MiB)")
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
