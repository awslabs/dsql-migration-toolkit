# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The session dirty-check covers every persisted field, and a restore picks one port.

``app.py``'s ``_persist_session`` writes a snapshot only when :func:`session_signature`
moves. A field the snapshot captures but the signature leaves out is therefore never
written for a change of that field alone -- the restore then brings back the old value.
That class shipped twice (an edit-only Schema Conversion change, a newly recorded secret
reference) and was open again for the source username, the target database and username,
the tracked CDC connector names and the CDC applied-ops window start.

``_SIGNATURE_COVERAGE`` records, for EVERY :class:`SessionSnapshot` field, how the
dirty-check sees it, and the guard fails as soon as the model gains a field the table
does not name -- so the next persisted field forces a decision instead of drifting.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dsql_migrator.config import SecretValue
from dsql_migrator.core.models import (
    AssessmentReport,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    TargetConnectionConfig,
    TargetInventory,
)
from dsql_migrator.core.session_state_store import SessionSnapshot
from dsql_migrator.ui.data_migration import DataMigrationState
from dsql_migrator.ui.evaluation import EvaluationState
from dsql_migrator.ui.schema_conversion import SchemaConversionState
from dsql_migrator.ui.session import SessionConnectionState
from dsql_migrator.ui.session_persistence import (
    apply_session_snapshot,
    capture_session_snapshot,
    session_signature,
)

# How the dirty-check sees each persisted field. "signature": a term of
# session_signature reads it. "covered by <x>": it only ever changes together with
# <x>, which the signature reads. "not persisted: <why>": nothing to save.
_SIGNATURE_COVERAGE: dict[str, str] = {
    "session_id": "covered by the store key: constant for the session's life",
    "workflow": "signature",
    "inventory": "covered by eval_sig (table/view counts of the one result)",
    "assessment": "covered by eval_sig (set with the inventory as one result)",
    "target_inventory": "covered by eval_sig (target table count, provenance)",
    "target_inventory_endpoint": "signature",
    "target_conflicts": "covered by eval_sig (part of the same result)",
    "generated_node_ids": "signature",
    "ticked_node_ids": "signature",
    "edited_target_ddls": "signature",
    "preserve_foreign_keys": "signature",
    "migration_job_id": "signature",
    "migration_selection": "signature",
    "migration_selection_touched": "signature",
    "migration_active_substep": "signature",
    "migration_prereq_gated_mode": "signature",
    "migration_type": "signature",
    "migration_type_chosen": "signature",
    "cdc_start_mode": "signature",
    "cdc_start_gtid": "signature",
    "cdc_start_binlog_file": "signature",
    "cdc_start_binlog_pos": "signature",
    "cdc_start_fingerprints": "signature",
    "cdc_history_missing": "signature",
    "cdc_start_gtid_bound": "covered by cdc_start_gtid (captured as its truth value)",
    "cdc_lob_exclusions": "signature",
    "cdc_connector_names": "signature",
    "exclusion_semantics": "covered by _exclusion_semantics_sig (the pending flag)",
    "legacy_column_exclusions": "signature",
    "overlay_ledger_in_use": "signature",
    "pending_legacy_candidates": "signature",
    "exclusion_rules_text": "signature",
    "rule_excluded_tables": "signature",
    "exclusion_pending_rules": "signature",
    "rule_reticked_tables": "signature",
    "cdc_deploy_job_id": "signature",
    "cdc_action_kind": "signature",
    "cdc_ops_window_start": "signature",
    "cdc_stack_name": "signature",
    "cdc_infra_inputs": "signature",
    "cdc_dlq_cursor_ms": (
        "covered by _cdc_dlq_sig (saved with the ledger it skips; the cursor itself "
        "moves every poll)"
    ),
    "cdc_dlq_ledger": "signature",
    "cdc_dlq_read_from_ms": "signature",
    "cdc_replica_identity_findings": "signature",
    "cdc_deployed_message_key_columns": "signature",
    "cdc_catalog_drift_findings": "signature",
    "cdc_catalog_drift_digests": "signature",
    "cdc_target_added_columns": "signature",
    "source_secret_id": "signature",
    "full_load_quarantine_accepted": "signature",
    "full_load_accepted_quarantine_rows": "signature",
    "foreign_key_validation": "signature",
    "foreign_key_absent_children": "signature",
    "target_endpoint": "signature",
    "target_region": "signature",
    "target_database": "signature",
    "target_username": "signature",
    "source_type": "signature",
    "source_host": "signature",
    "source_port": "signature",
    "source_database": "signature",
    "source_username": "signature",
    "evaluation_source_label": "covered by eval_sig (the result's label) + the source coordinates",
    "evaluation_source_type": "covered by eval_sig (the result's engine) + the source engine",
    "ai_assist_enabled": "signature",
    "ai_assist_model_id": "signature",
    "ai_assist_region": "signature",
    "aws_profile": "signature",
    "ai_conversation": "covered by _ai_conversation_sig (count, last length, visibility, scope)",
    "workflow_unlocked": "signature",
    "active_view": "signature",
    "validation_report": "covered by _validation_sig (completed_at / rechecked_at stamp it)",
    "validation_completed_at": "signature",
    "validation_rechecked_tables": "covered by _validation_sig (set with rechecked_at)",
    "validation_rechecked_at": "signature",
    "validation_run_started_at": "covered by _validation_sig (set with the report it dates)",
    "validation_compared_from": "covered by _validation_sig (set with completed_at / rechecked_at)",
    "validation_cdc_dlq_waiver": "signature",
    "validation_cdc_replica_identity_waiver": "signature",
    "validation_cdc_catalog_drift_waiver": "signature",
}


def test_every_snapshot_field_has_a_dirty_check_decision() -> None:
    # A field added to SessionSnapshot without an entry here fails, so whoever adds it
    # decides how the dirty-check sees it (most often: add a signature term).
    fields = set(SessionSnapshot.model_fields)
    assert sorted(fields - set(_SIGNATURE_COVERAGE)) == []
    # ...and the table never outlives a removed field.
    assert sorted(set(_SIGNATURE_COVERAGE) - fields) == []
    for name, how in _SIGNATURE_COVERAGE.items():
        assert how == "signature" or how.startswith(("covered by ", "not persisted: ")), name


def _states():
    session = SessionConnectionState()
    eval_state = EvaluationState()
    conv_state = SchemaConversionState()
    migration_state = DataMigrationState()
    session.set_source(
        SourceConnectionConfig(
            source_type=SourceType.MYSQL,
            host="src.example",
            port=3306,
            database="app",
            username="reader",
        ),
        SecretValue("not-a-real-password"),
    )
    session.set_target(
        TargetConnectionConfig(
            cluster_endpoint="abc.dsql.us-east-1.on.aws",
            region="us-east-1",
            database="postgres",
            username="admin",
        )
    )
    return session, eval_state, conv_state, migration_state


def _edit_source_username(session, _migration_state) -> None:  # noqa: ANN001
    session.set_source(
        session.source_config.model_copy(update={"username": "app_reader"}),
        session.source_password,
    )


def _edit_target(field: str, value: str):  # noqa: ANN202
    def _edit(session, _migration_state) -> None:  # noqa: ANN001
        session.set_target(session.target_config.model_copy(update={field: value}))

    return _edit


_ONE_FIELD_EDITS = {
    "source_username": _edit_source_username,
    "target_username": _edit_target("username", "migrator"),
    # Not editable on Connect (fixed at "postgres"); pinned at the state level so the
    # persisted field cannot fall out of the dirty-check.
    "target_database": _edit_target("database", "app"),
    "cdc_connector_names": lambda _s, m: m.set_cdc_connector_names(
        ["dsql-cdc-source", "dsql-cdc-sink"]
    ),
    "cdc_ops_window_start": lambda _s, m: m.set_cdc_ops_window_start(
        datetime(2026, 11, 6, 1, 2, 3, tzinfo=timezone.utc)
    ),
}


@pytest.mark.parametrize("field", sorted(_ONE_FIELD_EDITS))
def test_an_edit_of_only_this_persisted_field_is_saved(field: str) -> None:
    session, eval_state, conv_state, migration_state = _states()
    before_sig = session_signature(session, eval_state, conv_state, migration_state)
    before = getattr(
        capture_session_snapshot("s1", session, eval_state, conv_state, migration_state),
        field,
    )

    _ONE_FIELD_EDITS[field](session, migration_state)

    after = getattr(
        capture_session_snapshot("s1", session, eval_state, conv_state, migration_state),
        field,
    )
    assert after != before  # the snapshot carries the edit...
    # ...so the dirty-check must move, or _persist_session never writes it.
    assert session_signature(session, eval_state, conv_state, migration_state) != before_sig


def test_the_signature_holds_no_credential() -> None:
    # Property 7: the signature lives in process memory next to the snapshot; it carries
    # coordinates (including the usernames now), never the password.
    session, eval_state, conv_state, migration_state = _states()
    signature = session_signature(session, eval_state, conv_state, migration_state)

    def _walk(value):  # noqa: ANN001, ANN202
        if isinstance(value, (tuple, list, frozenset, set)):
            for item in value:
                yield from _walk(item)
        else:
            yield value

    leaves = list(_walk(signature))
    assert "reader" in leaves and "admin" in leaves
    assert not any(isinstance(leaf, SecretValue) for leaf in leaves)
    assert not any("not-a-real-password" in str(leaf) for leaf in leaves)


@pytest.mark.parametrize(
    ("source_type", "port"),
    [("postgres", 5432), ("mysql", 3306), (None, 3306), ("not-an-engine", 3306)],
)
def test_a_restored_source_without_a_port_takes_its_engines_default_on_both_paths(
    source_type: "str | None", port: int
) -> None:
    # The restored connection and the restored report's address agree on the port: a
    # PostgreSQL session saved without one used to come back on 3306 while the report
    # header said 5432. An absent or unknown engine restores as MySQL, as before.
    snapshot = SessionSnapshot(
        session_id="s1",
        source_type=source_type,
        source_host="src.example",
        source_database="app",
        source_username="reader",
        inventory=SourceInventory(tables=[]),
        assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]),
    )
    session = SessionConnectionState()
    eval_state = EvaluationState()
    migration_state = DataMigrationState()
    migration_state.bind_session(session)

    apply_session_snapshot(snapshot, session, eval_state, SchemaConversionState(), migration_state)

    assert session.source_config is not None
    assert session.source_config.port == port
    assert eval_state.result is not None
    assert eval_state.result.source_label == f"src.example:{port}/app"
