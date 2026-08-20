# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Connect screen (BYO source/target) and session state.

These tests cover the NiceGUI-agnostic core of the Connect screen:

- Config building/validation from form input.
- Source/target connection testing via injected fakes (no real network).
- The read-only-guarded source engine factory (Property 1).
- Credential confidentiality and session isolation (Property 7 /
  Requirement 9.2): credentials live only in per-session memory and never
  appear in plaintext in reprs, model dumps, or persisted state.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from sqlalchemy.engine import Engine

from dsql_migrator.config import SecretValue
from dsql_migrator.core.introspector import ReadOnlySourceError
from dsql_migrator.core.models import (
    ConnectionResult,
    SourceConnectionConfig,
    TargetConnectionConfig,
)
from dsql_migrator.ui.connect import (
    ENV_CREDENTIAL_CHAIN_LABEL,
    build_source_config,
    build_target_config,
    check_source_connection,
    check_target_connection,
    discover_aws_profiles,
    make_source_engine_factory,
    normalize_profile_selection,
    parse_region_from_endpoint,
    profile_selector_options,
)
from dsql_migrator.ui.session import SessionConnectionState, SessionStore


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeIntrospector:
    """Records the config it was asked to test and returns a canned result."""

    def __init__(self, result: ConnectionResult) -> None:
        self.result = result
        self.tested: list[SourceConnectionConfig] = []

    def test_connection(self, conn: SourceConnectionConfig) -> ConnectionResult:
        self.tested.append(conn)
        return self.result


class _FakeConnector:
    """A fake DSQL connector returning a canned result."""

    def __init__(self, result: ConnectionResult) -> None:
        self.result = result

    def test_connection(self) -> ConnectionResult:
        return self.result


# ---------------------------------------------------------------------------
# Config building / validation
# ---------------------------------------------------------------------------


def test_build_source_config_valid() -> None:
    config = build_source_config(
        host="db.example.com", port=3306, database="app", username="reader"
    )
    assert config.host == "db.example.com"
    assert config.port == 3306
    assert config.database == "app"
    assert config.username == "reader"
    assert config.secret is None


def test_build_source_config_defaults_to_mysql_source_type() -> None:
    from dsql_migrator.core.models import SourceType

    config = build_source_config(host="db.example.com", port=3306)
    assert config.source_type is SourceType.MYSQL


def test_build_source_config_passes_through_postgres_source_type() -> None:
    from dsql_migrator.core.models import SourceType

    config = build_source_config(
        host="pg.example.com", port=5432, source_type=SourceType.POSTGRES
    )
    assert config.source_type is SourceType.POSTGRES
    assert config.port == 5432


def test_build_source_config_blank_username_becomes_none() -> None:
    config = build_source_config(host="h", port=3306, database="app", username="")
    assert config.username is None


def test_build_source_config_rejects_empty_host() -> None:
    with pytest.raises(ValidationError):
        build_source_config(host="", port=3306, database="app")


def test_build_source_config_rejects_out_of_range_port() -> None:
    with pytest.raises(ValidationError):
        build_source_config(host="h", port=70000, database="app")


def test_build_target_config_applies_defaults() -> None:
    config = build_target_config(
        cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
    )
    assert config.database == "postgres"
    assert config.username == "admin"


def test_build_target_config_rejects_empty_endpoint() -> None:
    with pytest.raises(ValidationError):
        build_target_config(cluster_endpoint="", region="us-east-1")


# ---------------------------------------------------------------------------
# Region auto-detection from the DSQL cluster endpoint
# ---------------------------------------------------------------------------


def test_parse_region_from_endpoint_standard() -> None:
    assert (
        parse_region_from_endpoint("abc123.dsql.us-east-1.on.aws") == "us-east-1"
    )


def test_parse_region_from_endpoint_other_regions() -> None:
    assert (
        parse_region_from_endpoint("c.dsql.ap-southeast-2.on.aws")
        == "ap-southeast-2"
    )
    assert (
        parse_region_from_endpoint("c.dsql.eu-central-1.on.aws") == "eu-central-1"
    )


def test_parse_region_from_endpoint_is_case_insensitive() -> None:
    assert parse_region_from_endpoint("ABC.DSQL.US-EAST-1.ON.AWS") == "us-east-1"


def test_parse_region_from_endpoint_trims_whitespace_and_trailing_dot() -> None:
    assert (
        parse_region_from_endpoint("  cluster.dsql.us-west-2.on.aws.  ")
        == "us-west-2"
    )


def test_parse_region_from_endpoint_handles_dsql_subdomain_variant() -> None:
    assert (
        parse_region_from_endpoint("foo.dsql-gamma.eu-west-1.on.aws") == "eu-west-1"
    )


def test_parse_region_from_endpoint_returns_none_when_unrecognized() -> None:
    assert parse_region_from_endpoint("") is None
    assert parse_region_from_endpoint("   ") is None
    assert parse_region_from_endpoint("not-an-endpoint") is None
    assert parse_region_from_endpoint("host.example.com") is None
    # A "dsql" label not followed by a valid region token yields None.
    assert parse_region_from_endpoint("a.dsql.notaregion.on.aws") is None


# ---------------------------------------------------------------------------
# Connection testing (via injected fakes)
# ---------------------------------------------------------------------------


def test_check_source_connection_passes_through_result() -> None:
    config = build_source_config(host="h", port=3306, database="app")
    introspector = _FakeIntrospector(
        ConnectionResult(success=True, detail="Connection successful.")
    )
    result = check_source_connection(
        config, SecretValue("pw"), introspector=introspector
    )
    assert result.success is True
    assert introspector.tested == [config]


def test_check_source_connection_reports_failure() -> None:
    config = build_source_config(host="h", port=3306, database="app")
    introspector = _FakeIntrospector(
        ConnectionResult(success=False, detail="Connection failed: timeout")
    )
    result = check_source_connection(config, None, introspector=introspector)
    assert result.success is False
    assert "failed" in result.detail.lower()


def test_check_target_connection_passes_through_result() -> None:
    config = build_target_config(
        cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
    )
    captured: dict[str, TargetConnectionConfig] = {}

    def factory(conn: TargetConnectionConfig) -> _FakeConnector:
        captured["conn"] = conn
        return _FakeConnector(
            ConnectionResult(success=True, detail="Connection successful.")
        )

    result = check_target_connection(config, connector_factory=factory)
    assert result.success is True
    assert captured["conn"] is config


def test_check_target_connection_threads_aws_profile_to_connector(monkeypatch) -> None:
    # The default (non-injected) path MUST build DsqlConnector with the selected
    # aws_profile so the IAM token is generated under the same identity the rest of
    # the workflow uses (Requirement 9.5/9.7). A profile-less test misleads.
    import dsql_migrator.ui.connect as connect_mod

    captured: dict[str, object] = {}

    class _Conn:
        def __init__(self, config, *, aws_profile=None):
            captured["aws_profile"] = aws_profile

        def test_connection(self):
            return ConnectionResult(success=True, detail="ok")

    monkeypatch.setattr(connect_mod, "DsqlConnector", _Conn)
    config = build_target_config(
        cluster_endpoint="c.dsql.ap-northeast-2.on.aws", region="ap-northeast-2"
    )
    result = check_target_connection(config, "prod-profile")
    assert result.success is True
    assert captured["aws_profile"] == "prod-profile"


# ---------------------------------------------------------------------------
# Source engine factory: read-only guard (Property 1) and no credential leak
# ---------------------------------------------------------------------------


def test_source_engine_factory_returns_engine() -> None:
    factory = make_source_engine_factory(SecretValue("pw"))
    config = build_source_config(host="h", port=3306, database="app", username="u")
    engine = factory(config)
    try:
        assert isinstance(engine, Engine)
    finally:
        engine.dispose()


def test_source_engine_factory_does_not_leak_password_in_url_repr() -> None:
    factory = make_source_engine_factory(SecretValue("super-secret"))
    config = build_source_config(host="h", port=3306, database="app", username="u")
    engine = factory(config)
    try:
        # SQLAlchemy masks the password in URL repr/str.
        assert "super-secret" not in repr(engine.url)
        assert "super-secret" not in str(engine.url)
    finally:
        engine.dispose()


def test_source_engine_factory_builds_expected_url() -> None:
    factory = make_source_engine_factory(SecretValue("pw"))
    config = build_source_config(
        host="db.example.com", port=3307, database="app", username="reader"
    )
    engine = factory(config)
    try:
        url = engine.url
        assert url.drivername == "mysql+pymysql"
        assert url.host == "db.example.com"
        assert url.port == 3307
        assert url.database == "app"
        assert url.username == "reader"
    finally:
        engine.dispose()


def test_read_only_guard_blocks_writes_on_a_runnable_engine() -> None:
    # The guard installed by the factory is the same function exercised here on a
    # runnable SQLite engine: any write/DDL is rejected before execution
    # (Property 1).
    from sqlalchemy import create_engine, text

    from dsql_migrator.core.introspector import install_read_only_guard

    engine = create_engine("sqlite+pysqlite:///:memory:")
    install_read_only_guard(engine)
    try:
        with engine.connect() as conn:
            with pytest.raises(ReadOnlySourceError):
                conn.execute(text("CREATE TABLE t (id INTEGER)"))
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# Session state: confidentiality (Property 7)
# ---------------------------------------------------------------------------


def test_active_view_defaults_none_set_and_cleared() -> None:
    state = SessionConnectionState()
    # Default: no remembered view (a fresh session opens on Connect).
    assert state.active_view is None

    state.set_active_view("data_migration")
    assert state.active_view == "data_migration"

    # clear() wipes the remembered view along with the rest of the session.
    state.clear()
    assert state.active_view is None


def test_schema_conversion_skipped_flag_default_set_and_cleared() -> None:
    state = SessionConnectionState()
    assert state.schema_conversion_skipped is False

    state.set_schema_conversion_skipped(True)
    assert state.schema_conversion_skipped is True

    state.clear()
    assert state.schema_conversion_skipped is False


def test_session_state_keeps_password_in_memory_only() -> None:
    state = SessionConnectionState()
    config = build_source_config(host="h", port=3306, database="app", username="u")
    state.set_source(config, SecretValue("hunter2"))

    assert state.has_source() is True
    assert state.source_password is not None
    assert state.source_password.reveal() == "hunter2"


def test_session_state_repr_never_exposes_password() -> None:
    state = SessionConnectionState()
    config = build_source_config(host="h", port=3306, database="app", username="u")
    state.set_source(config, SecretValue("hunter2"))
    assert "hunter2" not in repr(state)


def test_source_config_dump_contains_no_password() -> None:
    config = build_source_config(host="h", port=3306, database="app", username="u")
    dumped = config.model_dump()
    assert "password" not in dumped
    # Only a (None) secret reference is present, never a plaintext value.
    assert dumped.get("secret") is None


def test_session_state_clear_discards_credentials() -> None:
    state = SessionConnectionState()
    config = build_source_config(host="h", port=3306, database="app")
    state.set_source(config, SecretValue("pw"))
    state.set_target(
        build_target_config(cluster_endpoint="c.aws", region="us-east-1")
    )

    state.clear()

    assert state.has_source() is False
    assert state.has_target() is False
    assert state.source_password is None


# ---------------------------------------------------------------------------
# Connect gate: Next is unlocked only when both connections are verified
# ---------------------------------------------------------------------------


def test_connection_not_ready_by_default() -> None:
    state = SessionConnectionState()
    assert state.source_verified is False
    assert state.target_verified is False
    assert state.connection_ready() is False


def test_connection_ready_requires_both_verified() -> None:
    state = SessionConnectionState()
    state.set_source_verified(True)
    assert state.connection_ready() is False
    state.set_target_verified(True)
    assert state.connection_ready() is True


def test_failed_source_test_relocks_the_gate() -> None:
    state = SessionConnectionState()
    state.set_source_verified(True)
    state.set_target_verified(True)
    assert state.connection_ready() is True
    # A subsequent failed source test must lock Next again.
    state.set_source_verified(False)
    assert state.connection_ready() is False


def test_clear_resets_verification_flags() -> None:
    state = SessionConnectionState()
    state.set_source_verified(True)
    state.set_target_verified(True)
    state.clear()
    assert state.source_verified is False
    assert state.target_verified is False
    assert state.connection_ready() is False


def test_workflow_locked_by_default() -> None:
    state = SessionConnectionState()
    assert state.workflow_unlocked() is False


def test_workflow_unlocks_only_after_both_verified() -> None:
    state = SessionConnectionState()
    state.set_source_verified(True)
    assert state.workflow_unlocked() is False
    state.set_target_verified(True)
    assert state.workflow_unlocked() is True


def test_workflow_unlock_is_sticky_after_invalidation() -> None:
    state = SessionConnectionState()
    state.set_source_verified(True)
    state.set_target_verified(True)
    assert state.workflow_unlocked() is True
    # Editing a verified connection re-locks the Connect screen's Next button
    # (connection_ready) but must NOT eject the user from the workflow.
    state.set_source_verified(False)
    assert state.connection_ready() is False
    assert state.workflow_unlocked() is True


def test_clear_resets_workflow_unlock_latch() -> None:
    state = SessionConnectionState()
    state.set_source_verified(True)
    state.set_target_verified(True)
    state.clear()
    assert state.workflow_unlocked() is False


def test_reset_in_place_keeps_same_object_so_reverify_unlocks() -> None:
    # Regression: after "Start over", the workflow nav captured the session object
    # in its closures at build time. A pop+recreate (clear) orphaned that object --
    # re-verifying the connections updated a NEW instance while the nav guard kept
    # reading the old, still-locked one, so steps never unlocked. reset_in_place
    # must wipe the SAME instance so the captured reference re-latches on re-verify.
    store = SessionStore()
    sid = "s1"
    captured = store.get_or_create(sid)  # what the nav closure holds
    captured.set_source_verified(True)
    captured.set_target_verified(True)
    assert captured.workflow_unlocked() is True

    store.reset_in_place(sid)
    # Same object instance, now wiped.
    assert store.get_or_create(sid) is captured
    assert captured.workflow_unlocked() is False

    # Re-verify on the store's session (Connect re-fetches via get_or_create) and
    # confirm the captured reference the nav guard reads is the one that unlocks.
    fresh = store.get_or_create(sid)
    fresh.set_source_verified(True)
    fresh.set_target_verified(True)
    assert captured.workflow_unlocked() is True


# ---------------------------------------------------------------------------
# Session store: isolation (Property 7)
# ---------------------------------------------------------------------------


def test_session_store_get_or_create_is_idempotent() -> None:
    store = SessionStore()
    first = store.get_or_create("session-a")
    second = store.get_or_create("session-a")
    assert first is second
    assert store.active_session_count() == 1


def test_session_store_isolates_sessions() -> None:
    store = SessionStore()
    state_a = store.get_or_create("session-a")
    state_b = store.get_or_create("session-b")

    state_a.set_source(
        build_source_config(host="a", port=3306, database="app"),
        SecretValue("secret-a"),
    )

    # Session B sees none of session A's credentials.
    assert state_b.has_source() is False
    assert state_b.source_password is None
    assert store.get("session-b") is state_b
    assert state_a is not state_b


def test_session_store_clear_removes_only_target_session() -> None:
    store = SessionStore()
    state_a = store.get_or_create("session-a")
    store.get_or_create("session-b")
    state_a.set_source(
        build_source_config(host="a", port=3306, database="app"),
        SecretValue("secret-a"),
    )

    store.clear("session-a")

    assert store.get("session-a") is None
    assert store.get("session-b") is not None
    assert store.active_session_count() == 1
    # The wiped state no longer holds the credential.
    assert state_a.source_password is None


def test_session_store_clear_unknown_session_is_noop() -> None:
    store = SessionStore()
    store.clear("does-not-exist")
    store.clear(None)
    assert store.active_session_count() == 0


# ---------------------------------------------------------------------------
# Global AWS profile discovery / selection (pure helpers)
# ---------------------------------------------------------------------------


def test_discover_aws_profiles_returns_injected_profiles() -> None:
    profiles = discover_aws_profiles(profile_lister=lambda: ["default", "dev", "prod"])
    assert profiles == ["default", "dev", "prod"]


def test_discover_aws_profiles_empty_is_selector_not_shown_condition() -> None:
    # No local named profiles -> empty list -> the Connect screen hides the
    # selector (default = environment credential chain).
    assert discover_aws_profiles(profile_lister=lambda: []) == []


def test_discover_aws_profiles_filters_blank_and_non_string_names() -> None:
    profiles = discover_aws_profiles(
        profile_lister=lambda: ["dev", "", "  ", None, 123, "prod"]  # type: ignore[list-item]
    )
    assert profiles == ["dev", "prod"]


def test_discover_aws_profiles_degrades_to_empty_on_error() -> None:
    def boom() -> list[str]:
        raise RuntimeError("no AWS config on this machine")

    assert discover_aws_profiles(profile_lister=boom) == []


def test_profile_selector_options_prepends_env_default() -> None:
    options = profile_selector_options(["dev", "prod"])
    assert options == [ENV_CREDENTIAL_CHAIN_LABEL, "dev", "prod"]


def test_normalize_profile_selection_default_maps_to_none() -> None:
    # Selecting the default option means the standard credential chain (None).
    assert normalize_profile_selection(ENV_CREDENTIAL_CHAIN_LABEL) is None
    assert normalize_profile_selection("") is None
    assert normalize_profile_selection(None) is None


def test_normalize_profile_selection_keeps_named_profile() -> None:
    assert normalize_profile_selection("prod") == "prod"


def test_connection_status_badge_verified() -> None:
    from dsql_migrator.ui.connect import connection_status_badge

    label, color = connection_status_badge(True)
    assert label == "Verified"
    assert color == "positive"


def test_connection_status_badge_not_verified() -> None:
    from dsql_migrator.ui.connect import connection_status_badge

    label, color = connection_status_badge(False)
    assert label == "Not verified"
    assert color == "grey"


# ---------------------------------------------------------------------------
# Session state: global AWS profile (non-secret) -- Property 7 / Req 9.8
# ---------------------------------------------------------------------------


def test_session_state_defaults_to_no_aws_profile() -> None:
    state = SessionConnectionState()
    assert state.aws_profile is None


def test_session_state_persists_selected_profile_name() -> None:
    state = SessionConnectionState()
    state.set_aws_profile("prod")
    assert state.aws_profile == "prod"


def test_session_state_set_aws_profile_none_uses_env_chain() -> None:
    state = SessionConnectionState()
    state.set_aws_profile("prod")
    state.set_aws_profile(None)
    assert state.aws_profile is None


def test_session_state_clear_resets_aws_profile() -> None:
    state = SessionConnectionState()
    state.set_aws_profile("prod")
    state.clear()
    assert state.aws_profile is None


def test_session_store_isolates_aws_profile_per_session() -> None:
    store = SessionStore()
    a = store.get_or_create("session-a")
    b = store.get_or_create("session-b")

    a.set_aws_profile("prod")

    assert a.aws_profile == "prod"
    # Session B is unaffected and keeps the default (env credential chain).
    assert b.aws_profile is None
