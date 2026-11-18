#!/usr/bin/env python3
"""Locate the retail migration checkout when this package is installed as a wheel."""
from __future__ import annotations

import os
from pathlib import Path


def find_migration_root(start: Path | None = None) -> Path:
    """Return the directory containing ``ds2spark.py`` and ``switch_input/``.

    The wheel is repository-aware rather than self-contained: it locates the checkout from
    ``LAKEBRIDGE_MIGRATION_ROOT`` first, then the current directory and its parents. This
    lets the same installed CLI run from either the repository root or ``migration/``.
    """
    env_root = os.environ.get("LAKEBRIDGE_MIGRATION_ROOT")
    if env_root:
        path = Path(env_root).expanduser().resolve()
        if not (path / "ds2spark.py").is_file():
            raise RuntimeError(f"LAKEBRIDGE_MIGRATION_ROOT does not point to migration/: {path}")
        return path

    start = (start or Path.cwd()).resolve()
    for path in (start, *start.parents):
        if (path / "ds2spark.py").is_file() and (path / "switch_input").is_dir():
            return path
        candidate = path / "migration"
        if (candidate / "ds2spark.py").is_file() and (candidate / "switch_input").is_dir():
            return candidate
    raise RuntimeError(
        "cannot locate the retail migration checkout; run from migration/ or the repository "
        "root, or set LAKEBRIDGE_MIGRATION_ROOT"
    )
