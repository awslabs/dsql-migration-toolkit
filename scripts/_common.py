# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared helpers for the command-line scripts (``.env`` parsing + logging +
SQL-identifier validation).

These are the small, byte-identical utilities that ``run_full_load.py`` and
``compare_rows.py`` both need. They are intentionally dependency-free so the
scripts still parse and run from a bare checkout. Each script keeps its own
``cfg`` getter (their env-vs-.env precedence differs on purpose), so only the
truly shared pieces live here.
"""

from __future__ import annotations

import datetime as _dt
import re as _re

# A schema/table/column name can NOT be passed as a bind parameter, so any script
# that builds `... FROM `{schema}`.`{table}`` has to interpolate it. Every such
# name (whether it came from `--table` on the command line, from `.env`, or back
# out of `information_schema`) is therefore checked against this allowlist first,
# so nothing but a plain unqualified identifier can ever reach the SQL text.
_IDENTIFIER_RE = _re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(name: str, kind: str = "identifier") -> str:
    """Return *name* if it is a safe SQL identifier, else raise ``ValueError``.

    Deliberately stricter than MySQL/PostgreSQL themselves (which allow quoted
    names with spaces, dots, hyphens, non-ASCII, ...): these scripts only ever
    address the plain ``[A-Za-z_][A-Za-z0-9_]*`` names the migration test schemas
    use, and rejecting everything else keeps interpolated identifiers injection-free.
    """
    if not _IDENTIFIER_RE.match(name or ""):
        raise ValueError(
            f"Invalid {kind}: {name!r} -- must match [A-Za-z_][A-Za-z0-9_]*")
    return name


def load_dotenv(path: str) -> dict:
    """Minimal ``KEY=VALUE`` parser for a ``.env`` file (no external dependency).

    Ignores blank lines and ``#`` comments, splits on the first ``=``, and strips
    matching single/double quotes around the value. A missing file yields ``{}``.
    """
    values: dict = {}
    try:
        with open(path, encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                values[key.strip()] = val.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return values


def log(msg: str) -> None:
    """Print ``msg`` prefixed with a local ``[HH:MM:SS]`` timestamp, flushed."""
    print(f"[{_dt.datetime.now():%H:%M:%S}] {msg}", flush=True)
