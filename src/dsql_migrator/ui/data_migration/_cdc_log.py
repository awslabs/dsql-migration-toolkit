# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The CDC activity-log anchor (NiceGUI-free).

:func:`_log_cdc_event` appends one CDC lifecycle milestone to the activity log. It was
defined in the package ``__init__``, which ``_cdc_ui`` -- the module ``__init__`` itself
imports at its bottom -- had to import back: the package <-> ``_cdc_ui`` cycle, which only
the bottom placement of that import kept working (dmshared-05). This module is a leaf (it
imports ``core.activity_log`` and nothing of this package), so both sides import it instead.

``__init__`` re-exports the name, so ``from dsql_migrator.ui.data_migration import
_log_cdc_event`` and a patch on ``dm._log_cdc_event`` -- which the connector-transition
logger that stays in ``__init__`` reads as its module global -- behave as before. A patch
must still be made on the module whose code logs: ``_cdc_ui`` for the CDC control render.
"""

from __future__ import annotations

from typing import Optional

from dsql_migrator.core.activity_log import ActivityCategory, ActivityStatus, log_activity


def _log_cdc_event(
    action: str,
    *,
    detail: "Optional[str]" = None,
    status: ActivityStatus = ActivityStatus.STARTED,
    exc: "Optional[BaseException]" = None,
) -> None:
    """Append a discrete CDC lifecycle milestone to the activity log.

    Records control-plane actions (deploy / start / stop / teardown) and connector
    state transitions as one-line events -- the audit trail of WHAT happened to the
    CDC pipeline and WHEN. Continuous progress (replication lag) stays out of the log
    (it would flood the rotated file); it lives in the live monitoring panel.

    Applied row counts are the ONE exception, added deliberately: the per-table
    Inserts/Updates/Deletes existed only on screen, so for a CDC migration the audit
    trail had no record of what the data path actually moved -- half the evidence a
    cut-over approval rests on. They are logged as a THROTTLED roll-up
    (:func:`~dsql_migrator.ui.data_migration._cdc_status.cdc_applied_rollup_due`) plus
    one final cumulative line at Stop, never per poll, so the original "do not flood the
    file" intent still holds.
    """
    log_activity(
        ActivityCategory.CDC, action, status=status, detail=detail,
        # Forwarded so a DEBUG level attaches the (value-free) stacktrace. Without it a
        # failed deploy / start / stop / teardown -- the actions an operator is most likely
        # to be stuck on -- gained nothing from raising the level.
        exc=exc,
    )
