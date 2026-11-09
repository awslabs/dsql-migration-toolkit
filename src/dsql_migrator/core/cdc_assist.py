# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CDC dead-letter summarizer that grounds the AI DBA's DLQ facts.

:func:`_summarize_errors` renders dead-lettered CDC errors as compact, capped
grounding text. The CDC step's AI DBA chat (``_cdc_dlq_facts`` in
``ui/data_migration/_cdc_monitoring.py``) feeds it records whose messages were
already made row-free, so no row value leaves the tool (Property 7). AI stays on
the control plane only: it reads already dead-lettered events and is never on the
data path (capture/stream/apply).
"""

from __future__ import annotations

from typing import Sequence

from dsql_migrator.core.cdc import CdcConnectorError

# Cap how many dead-lettered errors are summarized into the grounding text so an
# unbounded DLQ cannot flood the request.
_MAX_TRIAGE_ERRORS = 100


def _summarize_errors(errors: Sequence[CdcConnectorError]) -> str:
    """Render dead-lettered errors as compact grounding text (capped)."""
    lines = []
    for error in list(errors)[:_MAX_TRIAGE_ERRORS]:
        code = error.error_code or "-"
        lines.append(f"- {error.table} | code={code} | {error.message}")
    return "\n".join(lines) if lines else "(no dead-lettered events)"
