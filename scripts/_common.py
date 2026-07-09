# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared helpers for the command-line scripts (``.env`` parsing + logging).

These are the small, byte-identical utilities that ``run_full_load.py`` and
``compare_rows.py`` both need. They are intentionally dependency-free so the
scripts still parse and run from a bare checkout. Each script keeps its own
``cfg`` getter (their env-vs-.env precedence differs on purpose), so only the
truly shared pieces live here.
"""

from __future__ import annotations

import datetime as _dt


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
