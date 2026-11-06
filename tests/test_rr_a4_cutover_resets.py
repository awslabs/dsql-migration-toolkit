# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""What each ValidationState reset clears of the cut-over outcomes, exactly.

``clear_cutover_outcomes`` (a schema REPLACE), ``set_result`` (a new verdict) and
``clear_outputs`` (a re-run starts) all reset the cut-over outcomes; a stale one has
shown a wrong "go" before (a green "Applied N foreign key(s)" after a REPLACE, an old
identity-sync failure or FK opt-out on a new run -- FIX 4 / FIX 5). They share two
groups through ``_clear_cutover_fk_outcomes_locked`` and
``_clear_verdict_scoped_cutover_locked``. These tests pin, per method, the exact set of
attributes it writes, so the grouping can never quietly widen or narrow a reset.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.models import ValidationMode, ValidationReport
from dsql_migrator.ui.validation import ValidationState

# The foreign-key / DEFAULT-restore outcomes (and the drained attestation): cleared by
# every reset, including the schema REPLACE.
_FK_OUTCOMES = {
    "_cutover_fk_apply",
    "_cutover_fk_apply_job_id",
    "_cutover_fk_apply_progress",
    "proceed_without_foreign_keys",
    "proceed_without_fk_validation",
    "cutover_cdc_drained_confirmed",
    "_cutover_default_restore_outcome",
    # _invalidate_cutover_defaults_locked:
    "_cutover_defaults",
    "_cutover_defaults_generation",
}
# What belongs to one verdict: the cut-over identity sync and the three gate waivers. A
# schema REPLACE keeps them (and the restore job id), a new verdict or a re-run does not.
_VERDICT_SCOPED = {
    "_cutover_identity_sync",
    "_cutover_identity_sync_failed",
    "_cutover_identity_sync_covered",
    "cutover_dlq_waiver",
    "cutover_replica_identity_waiver",
    "cutover_catalog_drift_waiver",
}

_WRITES = {
    "clear_cutover_outcomes": _FK_OUTCOMES,
    "set_result": _FK_OUTCOMES
    | _VERDICT_SCOPED
    | {
        "_result",
        "_error",
        "_progress",
        "_restored",
        "_rechecked_tables",
        "_rechecked_at",
        "_compared_run_start",
        "_compared_overrides",
    },
    "clear_outputs": _FK_OUTCOMES
    | _VERDICT_SCOPED
    | {
        "_result",
        "_error",
        "_recheck_tables",
        "_rechecked_tables",
        "_rechecked_at",
        "_recheck_error",
        "_identity_sync",
        "_identity_sync_failed",
        "interrupted_by_restart",
        "_run_started_wall",
        "_compared_run_start",
        "_compared_overrides",
    },
}


def _call(state: ValidationState, method: str) -> None:
    if method == "set_result":
        state.set_result(ValidationReport(mode=ValidationMode.ROW_COUNT))
    else:
        getattr(state, method)()


@pytest.mark.parametrize("method", sorted(_WRITES))
def test_each_reset_writes_exactly_its_cut_over_groups(method: str) -> None:
    state = ValidationState()
    # Every attribute holds a sentinel, so any write -- even of the value a fresh state
    # holds -- shows up as a changed identity (the lock and the counter stay real).
    for name in list(vars(state)):
        if name not in ("_lock", "_cutover_defaults_generation"):
            setattr(state, name, object())
    before = dict(vars(state))

    _call(state, method)

    after = vars(state)
    written = {name for name in after if after[name] is not before[name]}
    assert sorted(written) == sorted(_WRITES[method])
    assert state._cutover_defaults_generation == 1
    # The two groups land on a fresh state's values.
    fresh = vars(ValidationState())
    for name in (_FK_OUTCOMES | (_VERDICT_SCOPED if method != "clear_cutover_outcomes" else set())):
        if name != "_cutover_defaults_generation":
            assert after[name] == fresh[name], name
