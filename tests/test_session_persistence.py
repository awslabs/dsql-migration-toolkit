# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for durable per-session state (reconnect/resume, Property 4).

Covers the SQLite/in-memory session-state stores and the NiceGUI-agnostic
capture/apply/freshness/signature helpers that re-link a reconnecting browser to
its persisted workbench state (workflow progress, evaluation result, generated
objects, and the Full Load job linkage). No NiceGUI is rendered.
"""

from __future__ import annotations

from datetime import datetime, timezone

from dsql_migrator.core.models import (
    AssessmentReport,
    ColumnDef,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    StepStatus,
    TableDef,
    TableSelection,
    TargetInventory,
)
from dsql_migrator.core.session_state_store import (
    InMemorySessionStateStore,
    S3SessionStateStore,
    SessionSnapshot,
    SqliteSessionStateStore,
)
from dsql_migrator.ui.data_migration import DataMigrationState
from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationState
from dsql_migrator.ui.schema_conversion import SchemaConversionState
from dsql_migrator.ui.session import SessionConnectionState
from dsql_migrator.ui.session_persistence import (
    apply_session_snapshot,
    capture_session_snapshot,
    session_is_fresh,
    session_signature,
)
from dsql_migrator.ui.workflow import WorkflowStep, with_status


def _inventory() -> SourceInventory:
    return SourceInventory(
        tables=[
            TableDef(
                name="orders",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            )
        ]
    )


def _populated_states():
    session = SessionConnectionState()
    eval_state = EvaluationState()
    conv_state = SchemaConversionState()
    migration_state = DataMigrationState()

    session.set_workflow(
        with_status(session.workflow, WorkflowStep.EVALUATION, StepStatus.DONE)
    )
    eval_state.set_result(
        EvaluationResult(
            inventory=_inventory(),
            assessment=AssessmentReport.from_items([]),
            target_inventory=TargetInventory(schemas=[]),
            target_conflicts=[],
        )
    )
    conv_state.generated_node_ids = ["table:orders"]
    conv_state.ticked_node_ids = ["table:orders", "table:customers"]
    conv_state.set_edited_target_ddl(
        "migration_demo.orders",
        'CREATE TABLE "migration_demo"."orders" ("c_bool" smallint)',
    )
    migration_state.job_id = "JOB123"
    migration_state.selection = TableSelection(selected_tables=["orders"])
    migration_state.selection_touched = True
    migration_state.active_substep = "full_load"
    return session, eval_state, conv_state, migration_state


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------


def test_start_over_clears_all_per_session_stores() -> None:
    # The "Start over" reset relies on every store dropping the session so the
    # next get_or_create() returns a fresh object. Lock that contract.
    from dsql_migrator.ui.data_migration import DataMigrationStore, MigrationType
    from dsql_migrator.ui.evaluation import EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore

    sid = "reset-me"
    sess_store = SessionStore()
    eval_store = EvaluationStore()
    conv_store = SchemaConversionStore()
    mig_store = DataMigrationStore()

    # Populate distinguishable state.
    s = sess_store.get_or_create(sid)
    s.set_active_view("migration_plan")
    s.set_migration_type("full_load_and_cdc")
    mig_store.get_or_create(sid).set_cdc_infra_inputs({"vpc_id": "vpc-x"})
    conv_store.get_or_create(sid).generated_node_ids = ["table:orders"]

    # Reset: each store drops the session (mirrors _reset_session in app.py).
    for store in (sess_store, eval_store, conv_store, mig_store):
        store.clear(sid)

    # Fresh objects come back with defaults.
    assert sess_store.get_or_create(sid).active_view is None
    assert sess_store.get_or_create(sid).migration_type is MigrationType.FULL_LOAD_ONLY
    assert mig_store.get_or_create(sid).cdc_infra_inputs() == {}
    assert conv_store.get_or_create(sid).generated_node_ids is None


def test_sqlite_session_store_round_trips_a_snapshot(tmp_path) -> None:  # noqa: ANN001
    store = SqliteSessionStateStore(str(tmp_path / "sessions.sqlite"))
    snapshot = SessionSnapshot(session_id="s1", generated_node_ids=["table:orders"])
    store.save(snapshot)

    reopened = SqliteSessionStateStore(str(tmp_path / "sessions.sqlite"))
    loaded = reopened.load("s1")
    assert loaded is not None
    assert loaded.generated_node_ids == ["table:orders"]
    assert reopened.load("missing") is None

    reopened.delete("s1")
    assert reopened.load("s1") is None


def test_sqlite_store_loads_a_snapshot_naming_the_retired_migration_plan_step(
    tmp_path,
) -> None:  # noqa: ANN001
    # THE back-compat guarantee for retiring the Migration plan step. Real persisted
    # snapshots carry workflow.migration_plan, and WorkflowState is extra="forbid" --
    # so if the field were removed, every one of them would fail to validate. The
    # sqlite load had no try/except, so that exception propagated out of build_page and
    # locked the user out of the tool entirely (not merely losing restored progress).
    import json

    store = SqliteSessionStateStore(str(tmp_path / "sessions.sqlite"))
    payload = json.dumps(
        {
            "session_id": "old",
            "workflow": {
                "migration_plan": "DONE",
                "evaluation": "DONE",
                "schema_conversion": "NOT_STARTED",
                "data_migration": "NOT_STARTED",
                "full_load": "NOT_STARTED",
                "cdc": "NOT_STARTED",
                "validation": "NOT_STARTED",
                "cut_over": "NOT_STARTED",
            },
            "migration_type": "full_load_and_cdc",
            "active_view": "migration_plan",
        }
    )
    store._conn.execute(  # noqa: SLF001 - simulate a pre-existing row
        "INSERT INTO sessions (session_id, payload, updated_at) VALUES (?, ?, ?)",
        ("old", payload, "2026-07-01T00:00:00Z"),
    )
    store._conn.commit()  # noqa: SLF001

    loaded = store.load("old")
    assert loaded is not None
    assert loaded.workflow.migration_plan is StepStatus.DONE
    assert loaded.migration_type == "full_load_and_cdc"
    # The parked view survives on the snapshot; app.py redirects it to Evaluation.
    assert loaded.active_view == "migration_plan"


def test_sqlite_store_degrades_on_an_unreadable_snapshot(tmp_path) -> None:  # noqa: ANN001
    # A payload that cannot validate (e.g. written by a newer build naming a field
    # this one does not know, since the model is extra="forbid") must be IGNORED, not
    # raise: the exception used to propagate out of build_page and break the page.
    store = SqliteSessionStateStore(str(tmp_path / "sessions.sqlite"))
    store._conn.execute(  # noqa: SLF001
        "INSERT INTO sessions (session_id, payload, updated_at) VALUES (?, ?, ?)",
        ("bad", '{"session_id": "bad", "a_field_from_the_future": 1}', "2026-07-01Z"),
    )
    store._conn.commit()  # noqa: SLF001
    assert store.load("bad") is None  # degraded, no exception


# ---------------------------------------------------------------------------
# S3-backed store (durable across a Fargate task replacement)
# ---------------------------------------------------------------------------


class _S3Error(Exception):
    """A boto3-shaped S3 error carrying an Error.Code for absence detection."""

    def __init__(self, code: str, msg: str = "") -> None:
        super().__init__(f"{code}: {msg}")
        self.response = {"Error": {"Code": code}}


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakePaginator:
    def __init__(self, objects: dict) -> None:
        self._objects = objects

    def paginate(self, *, Bucket, Prefix=""):  # noqa: N803
        contents = [
            {"Key": key, "LastModified": lm}
            for key, (_, lm) in self._objects.items()
            if key.startswith(Prefix)
        ]
        # Two pages so the store's pagination handling is exercised.
        yield {"Contents": contents[:1]}
        yield {"Contents": contents[1:]}


class _FakeS3:
    """A minimal in-memory S3 double for :class:`S3SessionStateStore` tests."""

    def __init__(self, *, buckets=("b",), put_error=None) -> None:
        self._objects: dict = {}
        self._buckets = set(buckets)
        self._put_error = put_error
        self._clock = 0
        self.created_buckets: list[str] = []

    def head_bucket(self, *, Bucket):  # noqa: N803
        if Bucket not in self._buckets:
            raise _S3Error("404", "Not Found")

    def create_bucket(self, *, Bucket, **_kw):  # noqa: N803
        self._buckets.add(Bucket)
        self.created_buckets.append(Bucket)

    def put_object(self, *, Bucket, Key, Body, **_kw):  # noqa: N803
        if self._put_error is not None:
            raise self._put_error
        self._clock += 1
        last_modified = datetime(2026, 7, 11, 0, 0, 0, self._clock, tzinfo=timezone.utc)
        self._objects[Key] = (bytes(Body), last_modified)

    def get_object(self, *, Bucket, Key):  # noqa: N803
        if Key not in self._objects:
            raise _S3Error("NoSuchKey", "Not Found")
        body, _ = self._objects[Key]
        return {"Body": _Body(body)}

    def delete_object(self, *, Bucket, Key):  # noqa: N803
        self._objects.pop(Key, None)

    def get_paginator(self, _name):
        return _FakePaginator(self._objects)


def test_s3_session_store_round_trips_and_deletes() -> None:
    fake = _FakeS3(buckets=("b",))
    store = S3SessionStateStore("b", s3_client=fake)
    store.save(SessionSnapshot(session_id="s1", generated_node_ids=["table:orders"]))

    loaded = store.load("s1")
    assert loaded is not None
    assert loaded.generated_node_ids == ["table:orders"]
    # A missing session is a normal None (NoSuchKey), not an error.
    assert store.load("missing") is None

    store.delete("s1")
    assert store.load("s1") is None


def test_s3_session_store_creates_bucket_when_absent() -> None:
    # Self-provisioning: if the managed bucket is not there yet, the store creates
    # it (idempotent) on first save -- no separate setup step for the customer.
    fake = _FakeS3(buckets=())
    store = S3SessionStateStore("b", region="ap-northeast-2", s3_client=fake)
    store.save(SessionSnapshot(session_id="s1"))
    assert "b" in fake.created_buckets
    assert store.load("s1") is not None


def test_s3_session_store_prune_keeps_most_recent() -> None:
    fake = _FakeS3(buckets=("b",))
    store = S3SessionStateStore("b", s3_client=fake)
    for sid in ("old1", "old2", "new1"):  # saved in order -> new1 is most recent
        store.save(SessionSnapshot(session_id=sid))

    deleted = store.prune(1)  # keep only the single newest
    assert set(deleted) == {"old1", "old2"}
    assert store.load("new1") is not None
    assert store.load("old1") is None


def test_s3_session_store_save_is_best_effort_on_error() -> None:
    # A transient S3 error on save must be swallowed so the live UI never breaks;
    # persistence merely degrades (a later load finds nothing).
    fake = _FakeS3(buckets=("b",), put_error=_S3Error("SlowDown", "throttled"))
    store = S3SessionStateStore("b", s3_client=fake)
    store.save(SessionSnapshot(session_id="s1"))  # must NOT raise
    assert store.load("s1") is None


def test_s3_session_store_save_swallows_serialization_error() -> None:
    # Even a failure to SERIALIZE the snapshot must not escape save() (the store's
    # "never raised to the caller" contract): serialization is guarded inside the try.
    class _BoomSnapshot:
        session_id = "s1"

        def model_dump_json(self) -> str:
            raise ValueError("cannot serialize")

    fake = _FakeS3(buckets=("b",))
    store = S3SessionStateStore("b", s3_client=fake)
    store.save(_BoomSnapshot())  # must NOT raise
    assert store.load("s1") is None  # nothing was written


def test_config_reads_session_state_bucket_env() -> None:
    from dsql_migrator.config import load_config

    cfg = load_config({"DSQL_MIGRATOR_SESSION_STATE_BUCKET": "my-bucket"})
    assert cfg.session_state_bucket == "my-bucket"
    assert load_config({}).session_state_bucket is None  # default unset


# ---------------------------------------------------------------------------
# capture / apply
# ---------------------------------------------------------------------------


def test_capture_then_apply_restores_full_session() -> None:
    session, eval_state, conv_state, migration_state = _populated_states()
    store = InMemorySessionStateStore()
    store.save(
        capture_session_snapshot(
            "s1", session, eval_state, conv_state, migration_state
        )
    )

    s2 = SessionConnectionState()
    e2 = EvaluationState()
    c2 = SchemaConversionState()
    m2 = DataMigrationState()
    snapshot = store.load("s1")
    assert snapshot is not None
    apply_session_snapshot(snapshot, s2, e2, c2, m2)

    assert getattr(s2.workflow, "evaluation") is StepStatus.DONE
    assert e2.result is not None
    assert [t.name for t in e2.result.inventory.tables] == ["orders"]
    assert c2.generated_node_ids == ["table:orders"]
    assert c2.ticked_node_ids == ["table:orders", "table:customers"]
    assert (
        c2.get_edited_target_ddl("migration_demo.orders")
        == 'CREATE TABLE "migration_demo"."orders" ("c_bool" smallint)'
    )
    assert m2.job_id == "JOB123"
    assert m2.selection.selected_tables == ["orders"]
    assert m2.selection_touched is True
    assert m2.active_substep == "full_load"


def test_ai_conversation_round_trips_through_a_snapshot() -> None:
    # The AI transcript is durable: it survives an app restart (snapshot round-trip),
    # not just a browser refresh. Credential-free content only.
    from dsql_migrator.core.models import AiConversation, AiScope
    from dsql_migrator.ui.session_persistence import session_signature

    session = SessionConnectionState()
    session.ai_conversation = AiConversation(
        messages=[
            {"role": "user", "text": "How do I convert orders?"},
            {"role": "assistant", "text": "Use IDENTITY."},
            {
                "role": "event", "text": "Assessment complete", "status": "success",
                "kind": "assessment",
                "data": {"auto": 2, "manual": 38, "unsupported": 15},
            },
        ],
        active_scope=AiScope(scope_id="general", title="AI assistant"),
        visible=True,
    )
    eval_state = EvaluationState()
    conv_state = SchemaConversionState()
    migration_state = DataMigrationState()
    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )

    s2 = SessionConnectionState()
    apply_session_snapshot(
        snapshot, s2, EvaluationState(), SchemaConversionState(), DataMigrationState()
    )
    # Transcript, event data, active scope, and open state all survive.
    assert [m["text"] for m in s2.ai_conversation.messages] == [
        "How do I convert orders?", "Use IDENTITY.", "Assessment complete"
    ]
    assert s2.ai_conversation.messages[2]["data"] == {
        "auto": 2, "manual": 38, "unsupported": 15
    }
    assert s2.ai_conversation.active_scope.scope_id == "general"
    assert s2.ai_conversation.visible is True
    # The restored transcript is an independent deep copy of the snapshot's.
    s2.ai_conversation.messages.append({"role": "user", "text": "x"})
    assert len(snapshot.ai_conversation.messages) == 3

    # A new chat turn changes the signature, so the persistence hook saves it.
    base = session_signature(session, eval_state, conv_state, migration_state)
    session.ai_conversation.messages.append({"role": "user", "text": "another"})
    assert (
        session_signature(session, eval_state, conv_state, migration_state) != base
    )


def test_cdc_state_round_trips() -> None:
    session, eval_state, conv_state, migration_state = _populated_states()
    migration_state.set_cdc_start_mode("manual")
    migration_state.set_cdc_start_position(
        gtid="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
    )
    migration_state.set_lob_exclusion("app.orders", "notes", True)
    migration_state.set_lob_exclusion("app.orders", "avatar", True)
    migration_state.set_cdc_connector_names(["src", "sink"])

    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert snapshot.cdc_start_gtid == "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
    assert snapshot.cdc_lob_exclusions == ["app.orders:avatar", "app.orders:notes"]
    assert snapshot.cdc_connector_names == ["src", "sink"]

    m2 = DataMigrationState()
    apply_session_snapshot(snapshot, SessionConnectionState(), EvaluationState(),
                           SchemaConversionState(), m2)
    override = m2.cdc_start_override()
    assert override is not None
    assert override.gtid_executed == "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
    assert m2.lob_exclusions() == {"app.orders": {"notes", "avatar"}}
    assert m2.cdc_connector_names == ["src", "sink"]


def test_cdc_deploy_job_id_round_trips_for_reconnect() -> None:
    # Regression: after an app restart the CDC pipeline's stage breakdown vanished
    # because the deploy-job link was not persisted. The job id + which operation
    # it is (kind) must survive a restore so the CDC card keeps rendering stages.
    session, eval_state, conv_state, migration_state = _populated_states()
    migration_state.set_cdc_deploy_job_id("cdc-job-123", kind="start")

    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert snapshot.cdc_deploy_job_id == "cdc-job-123"
    assert snapshot.cdc_action_kind == "start"

    m2 = DataMigrationState()
    apply_session_snapshot(snapshot, SessionConnectionState(), EvaluationState(),
                           SchemaConversionState(), m2)
    assert m2.cdc_deploy_job_id == "cdc-job-123"
    assert m2.cdc_action_kind == "start"


def test_teardown_reconnect_drops_stale_connectors_and_job_link() -> None:
    # Regression: after a completed Stop/Delete, a RECONNECT must NOT restore the
    # finished lifecycle-job link or the (now-gone) connector names -- otherwise the
    # CDC card clung to the old "Infrastructure deleted" log and classified the
    # pipeline off stale connector names, so it never surfaced the "Deploy CDC
    # infrastructure" action. The stack identity IS kept so the fresh AWS phase probe
    # knows which stack to check (absent -> Deploy, infra -> Start).
    for kind in ("delete", "stop"):
        session, eval_state, conv_state, migration_state = _populated_states()
        migration_state.set_cdc_connector_names(["src", "sink"])
        migration_state.set_cdc_deploy_job_id("cdc-teardown-1", kind=kind)
        migration_state.set_cdc_stack_name("mysql-dsql-cdc-seoul-test")

        snapshot = capture_session_snapshot(
            "s1", session, eval_state, conv_state, migration_state
        )
        assert snapshot.cdc_action_kind == kind
        assert snapshot.cdc_deploy_job_id == "cdc-teardown-1"
        assert snapshot.cdc_connector_names == ["src", "sink"]

        m2 = DataMigrationState()
        apply_session_snapshot(snapshot, SessionConnectionState(), EvaluationState(),
                               SchemaConversionState(), m2)
        assert m2.cdc_deploy_job_id is None, kind  # stale finished job NOT restored
        assert m2.cdc_connector_names == [], kind  # stale connectors NOT restored
        assert m2.cdc_stack_name == "mysql-dsql-cdc-seoul-test", kind  # stack id kept


def test_pre_cdc_deploy_job_snapshot_restores_without_link() -> None:
    # Older snapshots (and sessions that never started a CDC op) have no deploy
    # job: the fields default to None and restore leaves the link unset.
    snapshot = SessionSnapshot(session_id="s1")
    assert snapshot.cdc_deploy_job_id is None
    assert snapshot.cdc_action_kind is None
    m2 = DataMigrationState()
    apply_session_snapshot(snapshot, SessionConnectionState(), EvaluationState(),
                           SchemaConversionState(), m2)
    assert m2.cdc_deploy_job_id is None
    assert m2.cdc_action_kind is None


def test_ai_assist_preference_round_trips_for_reconnect() -> None:
    # The AI Assist toggle (+ Bedrock model/region) must survive a restart instead
    # of resetting to off, so a reconnecting user keeps their choice. Non-secret.
    from dsql_migrator.ui.ai_assist import build_ai_assist_config

    session, eval_state, conv_state, migration_state = _populated_states()
    session.set_ai_assist(
        build_ai_assist_config(
            enabled=True, model_id="global.anthropic.claude-opus-4-8", region="us-west-2"
        )
    )

    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert snapshot.ai_assist_enabled is True
    assert snapshot.ai_assist_model_id == "global.anthropic.claude-opus-4-8"
    assert snapshot.ai_assist_region == "us-west-2"

    s2 = SessionConnectionState()
    apply_session_snapshot(snapshot, s2, EvaluationState(),
                           SchemaConversionState(), DataMigrationState())
    assert s2.ai_assist.enabled is True
    assert s2.ai_assist.model_id == "global.anthropic.claude-opus-4-8"
    assert s2.ai_assist.region == "us-west-2"


def test_ai_assist_off_snapshot_restores_as_off() -> None:
    # An AI-off session (the default) restores with the toggle still off.
    snapshot = SessionSnapshot(session_id="s1")
    assert snapshot.ai_assist_enabled is False
    s2 = SessionConnectionState()
    apply_session_snapshot(snapshot, s2, EvaluationState(),
                           SchemaConversionState(), DataMigrationState())
    assert s2.ai_assist.enabled is False


def test_session_signature_changes_on_ai_assist_toggle() -> None:
    from dsql_migrator.ui.ai_assist import build_ai_assist_config

    session, eval_state, conv_state, migration_state = _populated_states()
    sig1 = session_signature(session, eval_state, conv_state, migration_state)
    session.set_ai_assist(build_ai_assist_config(enabled=True))
    sig2 = session_signature(session, eval_state, conv_state, migration_state)
    assert sig2 != sig1


def test_cdc_start_mode_round_trips() -> None:
    session, eval_state, conv_state, migration_state = _populated_states()
    migration_state.set_cdc_start_mode("manual")
    migration_state.set_cdc_start_position(gtid="x:1")
    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert snapshot.cdc_start_mode == "manual"
    m2 = DataMigrationState()
    apply_session_snapshot(snapshot, SessionConnectionState(), EvaluationState(),
                           SchemaConversionState(), m2)
    assert m2.cdc_start_mode() == "manual"
    assert m2.cdc_start_override() is not None


def test_migration_type_chosen_round_trips() -> None:
    # The journey header hides its migration-type banner until the user has actually
    # chosen, so the "chosen" latch has to survive a restart -- otherwise a reconnect
    # would drop the banner on a session that had already picked its type.
    session, eval_state, conv_state, migration_state = _populated_states()
    migration_state.bind_session(session)
    session.set_migration_type("full_load_and_cdc")
    assert session.migration_type_chosen() is True

    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert snapshot.migration_type_chosen is True

    s2, m2 = SessionConnectionState(), DataMigrationState()
    apply_session_snapshot(snapshot, s2, EvaluationState(), SchemaConversionState(), m2)
    assert s2.migration_type_chosen() is True
    assert s2.migration_type.value == "full_load_and_cdc"


def test_restore_does_not_fake_a_choice_that_never_happened() -> None:
    """A session that never chose must NOT come back looking as if it had.

    ``apply_session_snapshot`` replays the persisted type through
    ``set_migration_type``, which latches "the user chose this" -- so without
    re-asserting the persisted flag afterwards, every restore would turn the
    full-load-only DEFAULT into an apparent decision and the banner would reappear on
    Evaluation. Older snapshots (no such field) restore as False for the same reason.
    """
    session, eval_state, conv_state, migration_state = _populated_states()
    assert session.migration_type_chosen() is False  # untouched default

    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert snapshot.migration_type_chosen is False
    assert snapshot.migration_type == "full_load_only"  # the type is still captured

    s2, m2 = SessionConnectionState(), DataMigrationState()
    apply_session_snapshot(snapshot, s2, EvaluationState(), SchemaConversionState(), m2)
    assert s2.migration_type_chosen() is False
    assert s2.migration_type.value == "full_load_only"

    # An OLD snapshot payload lacking the field entirely behaves the same way.
    legacy = snapshot.model_dump()
    legacy.pop("migration_type_chosen")
    from dsql_migrator.core.session_state_store import SessionSnapshot as _Snap

    s3, m3 = SessionConnectionState(), DataMigrationState()
    apply_session_snapshot(
        _Snap.model_validate(legacy), s3, EvaluationState(), SchemaConversionState(), m3
    )
    assert s3.migration_type_chosen() is False


def test_session_signature_changes_when_the_type_is_first_chosen() -> None:
    # Choosing the value that was ALREADY the default still changes what the UI shows
    # (the banner appears), so it must dirty the signature or the flag never persists.
    session, eval_state, conv_state, migration_state = _populated_states()
    migration_state.bind_session(session)
    sig1 = session_signature(session, eval_state, conv_state, migration_state)
    session.set_migration_type("full_load_only")  # same value, but now an explicit choice
    sig2 = session_signature(session, eval_state, conv_state, migration_state)
    assert sig2 != sig1


def test_prereq_gated_mode_round_trips() -> None:
    # The prerequisite REPORTS are deliberately not persisted, so the run-guard
    # excuses an absent report once a run exists. That excuse must be scoped to the
    # mode that actually cleared the gate -- otherwise a Full-load-only run, after a
    # restart, would let a switch to a CDC type inherit its pass and reach Start CDC
    # with the binary-log format never checked. So the gated MODE must survive.
    from dsql_migrator.core.models import MigrationMode

    session, eval_state, conv_state, migration_state = _populated_states()
    migration_state.set_prereq_gated_mode(MigrationMode.FULL_LOAD)
    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert snapshot.migration_prereq_gated_mode == MigrationMode.FULL_LOAD.value

    m2 = DataMigrationState()
    apply_session_snapshot(snapshot, SessionConnectionState(), EvaluationState(),
                           SchemaConversionState(), m2)
    assert m2.prereq_gated_mode is MigrationMode.FULL_LOAD


def test_prereq_gated_mode_absent_or_unknown_restores_as_none() -> None:
    # Back-compat: snapshots written before this field existed (and any unrecognized
    # value) must restore as None, which keeps the lenient "a run exists, don't
    # re-block an absent report" behavior -- a reconnect is never hard-blocked by it.
    session, eval_state, conv_state, migration_state = _populated_states()
    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert snapshot.migration_prereq_gated_mode is None

    m2 = DataMigrationState()
    apply_session_snapshot(snapshot, SessionConnectionState(), EvaluationState(),
                           SchemaConversionState(), m2)
    assert m2.prereq_gated_mode is None

    bogus = snapshot.model_copy(update={"migration_prereq_gated_mode": "NOT_A_MODE"})
    m3 = DataMigrationState()
    apply_session_snapshot(bogus, SessionConnectionState(), EvaluationState(),
                           SchemaConversionState(), m3)
    assert m3.prereq_gated_mode is None


def test_session_signature_changes_on_prereq_gated_mode() -> None:
    # The dirty-checked signature gates the save, so a change that is only the gated
    # mode must still trigger a persist -- otherwise the field silently never lands.
    from dsql_migrator.core.models import MigrationMode

    session, eval_state, conv_state, migration_state = _populated_states()
    sig1 = session_signature(session, eval_state, conv_state, migration_state)
    migration_state.set_prereq_gated_mode(MigrationMode.CDC)
    sig2 = session_signature(session, eval_state, conv_state, migration_state)
    assert sig2 != sig1


def test_cdc_stack_name_and_infra_inputs_round_trip() -> None:
    # A reconnecting session must recover which cdc-stack it owns + the VpcId it
    # deployed with, so re-probing AWS can recover the live phase (not a blank
    # Deploy form). cdc_infra_inputs reads through to the bound session.
    session, eval_state, conv_state, migration_state = _populated_states()
    migration_state.bind_session(session)
    assert migration_state.set_cdc_stack_name("mysql-dsql-cdc-orders") is True
    migration_state.set_cdc_infra_inputs(
        {"vpc_id": "vpc-0abc", "connector_subnet_ids": "subnet-a,subnet-b"}
    )

    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert snapshot.cdc_stack_name == "mysql-dsql-cdc-orders"
    assert snapshot.cdc_infra_inputs["vpc_id"] == "vpc-0abc"

    s2 = SessionConnectionState()
    m2 = DataMigrationState()
    m2.bind_session(s2)
    apply_session_snapshot(snapshot, s2, EvaluationState(), SchemaConversionState(), m2)
    assert m2.cdc_stack_name == "mysql-dsql-cdc-orders"
    assert m2.cdc_infra_inputs()["vpc_id"] == "vpc-0abc"
    assert m2.cdc_infra_inputs()["connector_subnet_ids"] == "subnet-a,subnet-b"


def test_target_connection_and_unlock_round_trip_for_reconnect() -> None:
    # On reconnect the CDC card must be able to re-probe AWS, which needs the
    # (non-secret) target endpoint+region restored, and the workflow must stay
    # unlocked. Source is NOT persisted (it carries a secret).
    from dsql_migrator.core.models import TargetConnectionConfig

    session, eval_state, conv_state, migration_state = _populated_states()
    session.set_target(
        TargetConnectionConfig(
            cluster_endpoint="grto.dsql.us-east-1.on.aws",
            region="us-east-1",
            database="postgres",
            username="admin",
        )
    )
    # Latch unlock as if both connections had been verified this session.
    session.set_source_verified(True)
    session.set_target_verified(True)
    assert session.workflow_unlocked() is True

    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert snapshot.target_endpoint == "grto.dsql.us-east-1.on.aws"
    assert snapshot.target_region == "us-east-1"
    assert snapshot.workflow_unlocked is True

    s2 = SessionConnectionState()
    m2 = DataMigrationState()
    m2.bind_session(s2)
    assert s2.workflow_unlocked() is False
    apply_session_snapshot(snapshot, s2, EvaluationState(), SchemaConversionState(), m2)
    # Target restored (enables the read-only CDC phase probe) ...
    assert s2.target_config is not None
    assert s2.target_config.region == "us-east-1"
    assert s2.target_config.cluster_endpoint == "grto.dsql.us-east-1.on.aws"
    # ... and the workflow is navigable again ...
    assert s2.workflow_unlocked() is True
    # ... but the source secret is NOT restored (re-tested on Connect).
    assert s2.source_config is None
    # target_verified is not blindly restored -- the user re-tests on Connect.
    assert s2.target_verified is False


def test_source_type_hint_round_trips_for_postgres_reconnect() -> None:
    # The source ENGINE kind (non-secret) is persisted so a reconnect pre-selects
    # Postgres on Connect -- but the source connection itself (which carries a secret)
    # is NOT restored. The user re-enters credentials; the engine picker just no longer
    # snaps back to the MySQL default for a PostgreSQL migration.
    session, eval_state, conv_state, migration_state = _populated_states()
    session.set_source(
        SourceConnectionConfig(
            source_type=SourceType.POSTGRES,
            host="pg.example",
            port=5432,
            database="app",
            username="u",
        ),
        None,
    )
    snap = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert snap.source_type == "postgres"

    s2 = SessionConnectionState()
    m2 = DataMigrationState()
    m2.bind_session(s2)
    apply_session_snapshot(snap, s2, EvaluationState(), SchemaConversionState(), m2)
    # The source secret / connection is NOT restored ...
    assert s2.source_config is None
    # ... but the engine hint is, so Connect pre-selects Postgres (not MySQL).
    assert s2.restored_source_type is SourceType.POSTGRES


def test_source_type_hint_absent_on_old_snapshot_defaults_mysql() -> None:
    # Byte-identity for MySQL: a session with NO source set (an older snapshot predates
    # the field, or the source was never reconnected) captures source_type=None and
    # restores with NO engine hint, so Connect keeps its MySQL default. A MySQL source
    # round-trips as "mysql" and restores as the MySQL hint -- neither disturbs the
    # historical default path.
    session, eval_state, conv_state, migration_state = _populated_states()
    # No source set -> nothing to persist.
    snap = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert snap.source_type is None

    s2 = SessionConnectionState()
    m2 = DataMigrationState()
    m2.bind_session(s2)
    apply_session_snapshot(snap, s2, EvaluationState(), SchemaConversionState(), m2)
    assert s2.restored_source_type is None  # MySQL default path unchanged

    # A MySQL source persists its kind as "mysql" and restores as the MySQL hint.
    session.set_source(
        SourceConnectionConfig(source_type=SourceType.MYSQL, host="my.example"), None
    )
    mysql_snap = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert mysql_snap.source_type == "mysql"

    s3 = SessionConnectionState()
    m3 = DataMigrationState()
    m3.bind_session(s3)
    apply_session_snapshot(
        mysql_snap, s3, EvaluationState(), SchemaConversionState(), m3
    )
    assert s3.restored_source_type is SourceType.MYSQL


def test_active_view_round_trips_for_reconnect() -> None:
    # The last-viewed step must be restored so a reconnect reopens it instead of
    # resetting to Connect.
    session, eval_state, conv_state, migration_state = _populated_states()
    session.set_active_view("full_load")
    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert snapshot.active_view == "full_load"

    s2 = SessionConnectionState()
    m2 = DataMigrationState()
    m2.bind_session(s2)
    apply_session_snapshot(snapshot, s2, EvaluationState(), SchemaConversionState(), m2)
    assert s2.active_view == "full_load"


def test_signature_changes_when_active_view_or_stack_changes() -> None:
    from dsql_migrator.ui.session_persistence import session_signature

    session, eval_state, conv_state, migration_state = _populated_states()
    migration_state.bind_session(session)
    base = session_signature(session, eval_state, conv_state, migration_state)

    session.set_active_view("full_load")
    after_view = session_signature(session, eval_state, conv_state, migration_state)
    assert after_view != base  # a view change must trigger a re-save

    migration_state.set_cdc_stack_name("mysql-dsql-cdc-orders")
    after_stack = session_signature(session, eval_state, conv_state, migration_state)
    assert after_stack != after_view  # a stack-name change must trigger a re-save


def test_pre_cdc_infra_snapshot_restores_with_defaults() -> None:
    # Back-compat: a snapshot written before these fields existed restores cleanly
    # (default stack name kept, no infra inputs).
    snapshot = SessionSnapshot(session_id="s1")  # no cdc_stack_name / cdc_infra_inputs
    assert snapshot.cdc_stack_name is None
    assert snapshot.cdc_infra_inputs == {}
    s2 = SessionConnectionState()
    m2 = DataMigrationState()
    m2.bind_session(s2)
    default_name = m2.cdc_stack_name
    apply_session_snapshot(snapshot, s2, EvaluationState(), SchemaConversionState(), m2)
    assert m2.cdc_stack_name == default_name  # unchanged
    assert m2.cdc_infra_inputs() == {}


def test_migration_type_round_trips_to_session() -> None:
    # The migration type is now authoritative on the SESSION (chosen early on the
    # Migration plan step). Capturing it and applying it must land on the session,
    # not only on the (read-through) DataMigrationState.
    from dsql_migrator.ui.data_migration import MigrationType

    session, eval_state, conv_state, migration_state = _populated_states()
    migration_state.bind_session(session)
    migration_state.set_migration_type(MigrationType.FULL_LOAD_AND_CDC)
    assert session.migration_type is MigrationType.FULL_LOAD_AND_CDC

    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert snapshot.migration_type == "full_load_and_cdc"

    s2 = SessionConnectionState()
    m2 = DataMigrationState()
    apply_session_snapshot(snapshot, s2, EvaluationState(), SchemaConversionState(), m2)
    # Authoritative on the session...
    assert s2.migration_type is MigrationType.FULL_LOAD_AND_CDC
    # ...and read-through from the bound migration_state.
    assert m2.migration_type is MigrationType.FULL_LOAD_AND_CDC


def test_old_snapshot_without_cdc_fields_restores_clean() -> None:
    # A snapshot written before CDC persistence (no cdc_* fields) must restore
    # with the CDC state unset, not raise.
    snapshot = SessionSnapshot.model_validate_json('{"session_id": "old"}')
    assert snapshot.cdc_start_gtid is None
    assert snapshot.cdc_lob_exclusions == []
    m2 = DataMigrationState()
    apply_session_snapshot(snapshot, SessionConnectionState(), EvaluationState(),
                           SchemaConversionState(), m2)
    assert m2.cdc_start_override() is None
    assert m2.lob_exclusions() == {}


def test_session_signature_changes_on_cdc_start_position() -> None:
    session, eval_state, conv_state, migration_state = _populated_states()
    sig1 = session_signature(session, eval_state, conv_state, migration_state)
    migration_state.set_cdc_start_position(gtid="x:1")
    sig2 = session_signature(session, eval_state, conv_state, migration_state)
    assert sig2 != sig1


def test_session_signature_changes_on_cdc_deploy_job() -> None:
    # Starting a CDC lifecycle op must dirty the signature so the snapshot (with
    # the deploy-job link) is persisted and survives a reconnect.
    session, eval_state, conv_state, migration_state = _populated_states()
    sig1 = session_signature(session, eval_state, conv_state, migration_state)
    migration_state.set_cdc_deploy_job_id("cdc-job-1", kind="start")
    sig2 = session_signature(session, eval_state, conv_state, migration_state)
    assert sig2 != sig1


# ---------------------------------------------------------------------------
# freshness + signature (restore guard / dirty-check)
# ---------------------------------------------------------------------------


def test_session_is_fresh_only_for_uninitialized_session() -> None:
    session = SessionConnectionState()
    eval_state = EvaluationState()
    migration_state = DataMigrationState()
    assert session_is_fresh(session, eval_state, migration_state) is True

    migration_state.job_id = "JOB1"
    assert session_is_fresh(session, eval_state, migration_state) is False


def test_session_signature_changes_on_linkage_change_not_on_noise() -> None:
    session, eval_state, conv_state, migration_state = _populated_states()
    sig1 = session_signature(session, eval_state, conv_state, migration_state)
    # Re-reading without changes yields the same signature (skip redundant save).
    sig2 = session_signature(session, eval_state, conv_state, migration_state)
    assert sig1 == sig2

    # A linkage change (new selection) changes the signature.
    migration_state.selection = TableSelection(selected_tables=["orders", "items"])
    sig3 = session_signature(session, eval_state, conv_state, migration_state)
    assert sig3 != sig1


def test_session_store_prune_keeps_newest(tmp_path) -> None:  # noqa: ANN001
    store = SqliteSessionStateStore(str(tmp_path / "sessions.sqlite"))
    for i in range(4):
        store.save(SessionSnapshot(session_id=f"s{i}"))

    deleted = store.prune(keep_most_recent=2)

    assert sorted(deleted) == ["s0", "s1"]  # oldest two pruned
    assert store.load("s2") is not None
    assert store.load("s3") is not None
    assert store.load("s0") is None


def test_validation_result_round_trips_and_flags_restored() -> None:
    # A completed validation report must survive capture->restore so a reconnect
    # reopens the result page (not "Re-run"), flagged restored with its finish time.
    from datetime import datetime, timezone

    from dsql_migrator.core.models import (
        TableValidationResult,
        ValidationMode,
        ValidationReport,
    )
    from dsql_migrator.ui.validation import ValidationState

    session, eval_state, conv_state, migration_state = _populated_states()
    vstate = ValidationState()
    report = ValidationReport.build(
        mode=ValidationMode.ROW_COUNT,
        items=[
            TableValidationResult(
                table="orders", source_row_count=10, target_row_count=10,
                row_count_match=True, matched=True,
            )
        ],
    )
    done_at = datetime(2026, 6, 27, 1, 2, 3, tzinfo=timezone.utc)
    vstate._result = report  # simulate a completed run
    vstate._completed_at = done_at

    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state, vstate
    )
    assert snapshot.validation_report is not None
    assert snapshot.validation_completed_at == done_at

    # Fresh process: restore re-hydrates the report, flags it restored.
    v2 = ValidationState()
    apply_session_snapshot(
        snapshot, SessionConnectionState(), EvaluationState(),
        SchemaConversionState(), DataMigrationState(), v2,
    )
    assert v2.result is not None
    assert v2.restored is True
    assert v2.completed_at == done_at
    # Elapsed is not restored (a restored run has no live duration).
    assert v2.elapsed_seconds is None


def test_reconnect_reconciles_stuck_in_progress_validation_to_done() -> None:
    # Regression: if a validation is saved as IN_PROGRESS but WITH a completed report
    # (disconnect right at completion, before the DONE flip persisted), a reconnect
    # must reconcile the step to DONE. Otherwise the shell shows a stuck "In progress"
    # badge over a completed report with a permanently-locked "Re-run validation".
    from datetime import datetime, timezone

    from dsql_migrator.core.models import (
        TableValidationResult,
        ValidationMode,
        ValidationReport,
    )
    from dsql_migrator.ui.validation import ValidationState
    from dsql_migrator.ui.workflow import get_status

    session, eval_state, conv_state, migration_state = _populated_states()
    session.set_workflow(  # the stuck state: IN_PROGRESS ...
        with_status(session.workflow, WorkflowStep.VALIDATION, StepStatus.IN_PROGRESS)
    )
    vstate = ValidationState()
    vstate._result = ValidationReport.build(  # ... but a completed report exists
        mode=ValidationMode.ROW_COUNT,
        items=[
            TableValidationResult(
                table="orders", source_row_count=10, target_row_count=10,
                row_count_match=True, matched=True,
            )
        ],
    )
    vstate._completed_at = datetime(2026, 7, 13, 1, 2, 3, tzinfo=timezone.utc)

    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state, vstate
    )
    assert getattr(snapshot.workflow, "validation") is StepStatus.IN_PROGRESS
    assert snapshot.validation_report is not None

    s2 = SessionConnectionState()
    v2 = ValidationState()
    apply_session_snapshot(
        snapshot, s2, EvaluationState(), SchemaConversionState(),
        DataMigrationState(), v2,
    )
    # Reconciled to DONE -> Re-run enabled + the completed report shows cleanly.
    assert get_status(s2.workflow, WorkflowStep.VALIDATION) is StepStatus.DONE
    assert v2.result is not None


def test_genuine_in_progress_validation_without_report_is_not_marked_done() -> None:
    # Guard the reconcile's precondition: IN_PROGRESS with NO report (a genuinely
    # in-flight run -- clear_outputs() wiped the report at run start) must stay
    # IN_PROGRESS on restore, never flipped to DONE.
    from dsql_migrator.ui.validation import ValidationState
    from dsql_migrator.ui.workflow import get_status

    session, eval_state, conv_state, migration_state = _populated_states()
    session.set_workflow(
        with_status(session.workflow, WorkflowStep.VALIDATION, StepStatus.IN_PROGRESS)
    )
    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state, ValidationState()
    )
    assert snapshot.validation_report is None
    s2 = SessionConnectionState()
    apply_session_snapshot(
        snapshot, s2, EvaluationState(), SchemaConversionState(),
        DataMigrationState(), ValidationState(),
    )
    assert get_status(s2.workflow, WorkflowStep.VALIDATION) is StepStatus.IN_PROGRESS


def test_reconnect_clears_a_schema_apply_left_spinning_by_a_restart() -> None:
    """A restart mid-apply must not leave the step spinning forever.

    Reported from a real session: the UI was restarted while "Applying converted DDL to
    the target..." was running, and after reconnecting the spinner never stopped. The
    apply runs IN-PROCESS and its job id is deliberately never persisted, so a restart
    kills the work AND loses the handle: the step still restores as IN_PROGRESS (which
    renders the spinner), while ``_install_poll_timer`` -- the only thing that finalizes
    the status -- returns immediately because ``job_id is None``. Nothing could ever
    clear it, and the Apply controls stayed locked behind it.

    Reconciled to FAILED rather than DONE: unlike Validation there is no restored report
    proving completion, and the run demonstrably did NOT finish. FAILED is honest and
    re-enables Apply; the apply is idempotent per object, so re-running finishes the
    remainder.
    """
    from dsql_migrator.ui.workflow import get_status

    session, eval_state, conv_state, migration_state = _populated_states()
    session.set_workflow(
        with_status(
            session.workflow, WorkflowStep.SCHEMA_CONVERSION, StepStatus.IN_PROGRESS
        )
    )
    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )
    assert getattr(snapshot.workflow, "schema_conversion") is StepStatus.IN_PROGRESS

    s2 = SessionConnectionState()
    c2 = SchemaConversionState()
    assert c2.job_id is None  # a fresh process has no handle on the dead job

    apply_session_snapshot(
        snapshot, s2, EvaluationState(), c2, DataMigrationState(), None
    )

    assert (
        get_status(s2.workflow, WorkflowStep.SCHEMA_CONVERSION) is StepStatus.FAILED
    )
    # The user is told what happened, that already-created objects are on the target,
    # and that re-running with "Skip if exists" is the safe way to finish.
    assert c2.error is not None
    assert "restarted" in c2.error
    assert "Skip if exists" in c2.error


def test_a_live_schema_apply_is_not_reconciled_away() -> None:
    # Guard the precondition: when a real apply job IS in flight (job_id present, e.g.
    # a same-process reconnect), the step must stay IN_PROGRESS so the spinner and the
    # poll timer keep tracking it -- the reconcile must key off the LOST handle, not
    # merely off the IN_PROGRESS status.
    from dsql_migrator.ui.workflow import get_status

    session, eval_state, conv_state, migration_state = _populated_states()
    session.set_workflow(
        with_status(
            session.workflow, WorkflowStep.SCHEMA_CONVERSION, StepStatus.IN_PROGRESS
        )
    )
    snapshot = capture_session_snapshot(
        "s1", session, eval_state, conv_state, migration_state
    )

    s2 = SessionConnectionState()
    c2 = SchemaConversionState()
    c2.job_id = "apply-job-still-running"

    apply_session_snapshot(
        snapshot, s2, EvaluationState(), c2, DataMigrationState(), None
    )

    assert (
        get_status(s2.workflow, WorkflowStep.SCHEMA_CONVERSION)
        is StepStatus.IN_PROGRESS
    )
    assert c2.error is None


def test_validation_signature_changes_when_result_recorded() -> None:
    from dsql_migrator.core.models import (
        TableValidationResult,
        ValidationMode,
        ValidationReport,
    )
    from dsql_migrator.ui.validation import ValidationState

    session, eval_state, conv_state, migration_state = _populated_states()
    vstate = ValidationState()
    sig_before = session_signature(
        session, eval_state, conv_state, migration_state, vstate
    )
    vstate._result = ValidationReport.build(
        mode=ValidationMode.ROW_COUNT,
        items=[
            TableValidationResult(
                table="orders", source_row_count=1, target_row_count=1,
                row_count_match=True, matched=True,
            )
        ],
    )
    sig_after = session_signature(
        session, eval_state, conv_state, migration_state, vstate
    )
    assert sig_before != sig_after  # recording a result triggers a save
