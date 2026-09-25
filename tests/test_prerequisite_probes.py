# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the session-wired prerequisite probes (UI adapter layer).

Focus on :class:`SessionMskProbe`: it must report MSK / MSK Connect availability
from the target region's real state via read-only ``kafka:ListClustersV2`` /
``kafkaconnect:ListConnectors`` calls, and fail closed (``False``) on any access
error -- without ever reaching AWS in the test (a fake ``boto3.Session`` is
injected). Property 1 (read-only) and Property 7 (no credential values) hold:
the probe only lists, and only the non-secret region/profile flow through.
"""

from __future__ import annotations

from typing import Any

from dsql_migrator.ui.prerequisite_probes import (
    SessionMskProbe,
    UnavailableMskProbe,
)


class _FakeClient:
    """A fake MSK / MSK Connect client returning canned list responses."""

    def __init__(self, responses: dict[str, Any], *, raise_on: str | None = None):
        self._responses = responses
        self._raise_on = raise_on
        self.calls: list[str] = []

    def list_clusters_v2(self, **_kwargs: Any) -> Any:
        self.calls.append("list_clusters_v2")
        if self._raise_on == "list_clusters_v2":
            raise RuntimeError("boom")
        return self._responses.get("list_clusters_v2", {})

    def list_connectors(self, **_kwargs: Any) -> Any:
        self.calls.append("list_connectors")
        if self._raise_on == "list_connectors":
            raise RuntimeError("boom")
        return self._responses.get("list_connectors", {})


class _FakeSession:
    """Fake ``boto3.Session`` that hands out a fixed client and records region."""

    def __init__(self, client: _FakeClient):
        self._client = client
        self.client_calls: list[tuple[str, Any]] = []

    def client(self, service_name: str, **kwargs: Any) -> _FakeClient:
        self.client_calls.append((service_name, kwargs.get("region_name")))
        return self._client


def _probe(client: _FakeClient, region: str = "us-east-1") -> SessionMskProbe:
    return SessionMskProbe(region, session=_FakeSession(client))


# ---------------------------------------------------------------------------
# cluster_available
# ---------------------------------------------------------------------------


def test_cluster_available_true_when_active_cluster_present() -> None:
    client = _FakeClient(
        {"list_clusters_v2": {"ClusterInfoList": [{"ClusterName": "c", "State": "ACTIVE"}]}}
    )
    assert _probe(client).cluster_available() is True
    # Read-only: only the list call was made.
    assert client.calls == ["list_clusters_v2"]


def test_cluster_available_false_when_cluster_not_active() -> None:
    client = _FakeClient(
        {"list_clusters_v2": {"ClusterInfoList": [{"State": "CREATING"}]}}
    )
    assert _probe(client).cluster_available() is False


def test_cluster_available_false_when_no_clusters() -> None:
    client = _FakeClient({"list_clusters_v2": {"ClusterInfoList": []}})
    assert _probe(client).cluster_available() is False


def test_cluster_available_false_on_aws_error() -> None:
    client = _FakeClient({}, raise_on="list_clusters_v2")
    assert _probe(client).cluster_available() is False


def test_cluster_available_passes_region_to_client() -> None:
    client = _FakeClient(
        {"list_clusters_v2": {"ClusterInfoList": [{"State": "ACTIVE"}]}}
    )
    session = _FakeSession(client)
    probe = SessionMskProbe("eu-west-1", session=session)
    probe.cluster_available()
    assert session.client_calls == [("kafka", "eu-west-1")]


# ---------------------------------------------------------------------------
# connect_available
# ---------------------------------------------------------------------------


def test_connect_available_true_when_a_connector_exists() -> None:
    client = _FakeClient(
        {"list_connectors": {"connectors": [{"connectorName": "sink", "connectorState": "RUNNING"}]}}
    )
    assert _probe(client).connect_available() is True
    assert client.calls == ["list_connectors"]


def test_connect_available_false_when_no_connectors() -> None:
    client = _FakeClient({"list_connectors": {"connectors": []}})
    assert _probe(client).connect_available() is False


def test_connect_available_false_on_aws_error() -> None:
    client = _FakeClient({}, raise_on="list_connectors")
    assert _probe(client).connect_available() is False


def test_connect_available_uses_kafkaconnect_client() -> None:
    client = _FakeClient({"list_connectors": {"connectors": [{"connectorName": "x"}]}})
    session = _FakeSession(client)
    SessionMskProbe("us-east-1", session=session).connect_available()
    assert session.client_calls == [("kafkaconnect", "us-east-1")]


# ---------------------------------------------------------------------------
# UnavailableMskProbe fallback
# ---------------------------------------------------------------------------


def test_unavailable_probe_always_false() -> None:
    probe = UnavailableMskProbe()
    assert probe.cluster_available() is False
    assert probe.connect_available() is False


def test_session_target_probe_reads_value_required_columns_via_introspector() -> None:
    """The wiring the columns check depends on: the probe must delegate to the
    live-catalog reader with the connector's connect factory.

    Asserted on the parse tree (not a source substring) so reformatting can't
    satisfy it. Without this delegation the prerequisite check has no data and the
    NOT-NULL-column gap goes unchecked.
    """
    import ast
    import inspect

    from dsql_migrator.ui import prerequisite_probes as probes_mod

    src = inspect.getsource(
        probes_mod.SessionTargetProbe.required_columns_without_default
    )
    tree = ast.parse(src.strip())
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "target_required_columns_without_default"
    ]
    assert calls, "probe must call target_required_columns_without_default"
    call = calls[0]
    # Table name forwarded positionally; connection_factory is the connector's connect.
    assert call.args, "the qualified table name must be forwarded"
    kwargs = {kw.arg: ast.unparse(kw.value) for kw in call.keywords}
    assert kwargs.get("connection_factory") == "self._connector.connect"


# ---------------------------------------------------------------------------
# SessionSourceProbe.variables: bound names, literal statement
# ---------------------------------------------------------------------------


class _FakeRows:
    """Minimal SQLAlchemy result stand-in exposing ``fetchall``."""

    def __init__(self, rows: list[tuple[str, str]]):
        self._rows = rows

    def fetchall(self) -> list[tuple[str, str]]:
        return self._rows


class _RecordingConnection:
    """Records (statement, parameters) and returns canned variable rows.

    ``variables()`` issues TWO queries: ``SHOW GLOBAL VARIABLES`` (returns ``rows``)
    and the RDS-only ``mysql.rds_configuration`` read (returns ``rds_rows`` -- empty
    by default so a non-RDS source adds no retention key).
    """

    def __init__(
        self,
        rows: list[tuple[str, str]],
        rds_rows: list[tuple[Any]] | None = None,
    ):
        self._rows = rows
        self._rds_rows = rds_rows or []
        self.calls: list[tuple[str, Any]] = []

    def __enter__(self) -> "_RecordingConnection":
        return self

    def __exit__(self, *_exc: Any) -> bool:
        return False

    def execute(self, statement: Any, parameters: Any = None) -> _FakeRows:
        self.calls.append((str(statement), parameters))
        if "rds_configuration" in str(statement):
            return _FakeRows(self._rds_rows)
        return _FakeRows(self._rows)


def _source_probe(connection: _RecordingConnection) -> Any:
    from dsql_migrator.core.models import SourceConnectionConfig
    from dsql_migrator.ui.prerequisite_probes import SessionSourceProbe

    probe = SessionSourceProbe(SourceConnectionConfig(host="db.example.com"), None)
    # Replace the engine factory seam: no MySQL is reached in a unit test.
    probe._engine_factory = lambda _config: type(  # noqa: SLF001
        "_Engine", (), {"connect": lambda _self: connection}
    )()
    return probe


def test_variables_binds_the_names_instead_of_formatting_them() -> None:
    """The variable names must travel as bind parameters, not inside the SQL text.

    They are a fixed module constant, so this is not an exploitable injection -- but
    they are VALUES (a schema/table name, by contrast, cannot be bound at all), so
    binding is possible and it keeps the statement a literal. Static analysers flag
    every formatted SQL string, and this is one of the few the tool can simply not
    have.
    """
    from dsql_migrator.ui.prerequisite_probes import _CDC_VARIABLES

    connection = _RecordingConnection([("log_bin", "ON"), ("binlog_format", "ROW")])
    result = _source_probe(connection).variables()
    assert result == {"log_bin": "ON", "binlog_format": "ROW"}

    statement, parameters = connection.calls[0]
    # Every name is passed as a parameter value ...
    assert set(parameters.values()) == set(_CDC_VARIABLES)
    # ... and none of them appears in the statement text itself.
    for name in _CDC_VARIABLES:
        assert name not in statement, f"{name} leaked into the SQL text: {statement}"
    # One placeholder per variable: adding a variable without a placeholder (or vice
    # versa) would silently drop it from the query, so pin the count.
    assert statement.count(":v") == len(_CDC_VARIABLES), statement
    assert len(parameters) == len(_CDC_VARIABLES)


# ---------------------------------------------------------------------------
# SessionSourceProbe.grants: delegates to the source dialect (engine-correct).
# The pre-fix probe hard-coded SHOW GRANTS, which errors -> empty on a PostgreSQL
# source and falsely FAILs the "required privileges" prerequisite, blocking the
# Full Load. The probe must instead ask the dialect for the grant surface.
# ---------------------------------------------------------------------------


class _GrantsDispatchConnection:
    """Records SQL and answers is_superuser / role_table_grants / SHOW GRANTS."""

    def __init__(self, *, super=None, pg_rows=None, mysql_rows=None):
        self._super = super
        self._pg_rows = pg_rows or []
        self._mysql_rows = mysql_rows or []
        self.calls: list[str] = []

    def __enter__(self) -> "_GrantsDispatchConnection":
        return self

    def __exit__(self, *_exc: Any) -> bool:
        return False

    def execute(self, statement: Any, parameters: Any = None) -> Any:
        sql = str(statement)
        self.calls.append(sql)
        upper = sql.upper()
        rows = self._mysql_rows if "SHOW GRANTS" in upper else self._pg_rows
        scalar = self._super
        return type(
            "_R", (), {"fetchall": lambda _self: rows, "scalar": lambda _self: scalar}
        )()


def _typed_source_probe(connection: Any, source_type: Any) -> Any:
    from dsql_migrator.core.models import SourceConnectionConfig
    from dsql_migrator.ui.prerequisite_probes import SessionSourceProbe

    probe = SessionSourceProbe(
        SourceConnectionConfig(host="db.example.com", source_type=source_type), None
    )
    probe._engine_factory = lambda _config: type(  # noqa: SLF001
        "_Engine", (), {"connect": lambda _self: connection}
    )()
    return probe


def test_grants_delegates_to_mysql_dialect_show_grants() -> None:
    from dsql_migrator.core.models import SourceType

    connection = _GrantsDispatchConnection(mysql_rows=[("GRANT SELECT ON db.* TO u",)])
    grants = _typed_source_probe(connection, SourceType.MYSQL).grants()
    assert grants == ["GRANT SELECT ON db.* TO u"]
    assert any("SHOW GRANTS" in call for call in connection.calls)


def test_grants_delegates_to_postgres_dialect_not_show_grants() -> None:
    # For a PostgreSQL source the probe must NOT issue SHOW GRANTS; a superuser yields
    # ALL PRIVILEGES via the PG-specific probe.
    from dsql_migrator.core.models import SourceType

    connection = _GrantsDispatchConnection(super="on")
    grants = _typed_source_probe(connection, SourceType.POSTGRES).grants()
    assert grants == ["ALL PRIVILEGES"]
    assert not any("SHOW GRANTS" in call for call in connection.calls)


def test_variables_folds_in_rds_binlog_retention_when_set() -> None:
    conn = _RecordingConnection([("log_bin", "ON")], rds_rows=[("168",)])
    result = _source_probe(conn).variables()
    assert result["log_bin"] == "ON"
    assert result["rds_binlog_retention_hours"] == "168"


def test_variables_marks_rds_retention_unset_as_zero() -> None:
    # The RDS row exists but its value is NULL -> retention unset -> "0" (risk).
    conn = _RecordingConnection([("log_bin", "ON")], rds_rows=[(None,)])
    assert _source_probe(conn).variables()["rds_binlog_retention_hours"] == "0"


def test_variables_omits_rds_key_on_self_managed() -> None:
    # No mysql.rds_configuration rows (non-RDS / no access) -> no synthetic key.
    result = _source_probe(_RecordingConnection([("log_bin", "ON")])).variables()
    assert "rds_binlog_retention_hours" not in result


# ---------------------------------------------------------------------------
# The oversized-value probe: bounded, engine-correct, honest when it cannot tell
# ---------------------------------------------------------------------------


class _SizeProbeConn:
    """Records statements; answers the EXISTS probe per column."""

    def __init__(
        self,
        oversized: set[str],
        *,
        fail: set[str] | None = None,
        aborts_on_failure: bool = False,
    ) -> None:
        self.statements: list[str] = []
        self.bound: list[dict] = []
        self.rollbacks = 0
        self._oversized = oversized
        self._fail = fail or set()
        # PostgreSQL leaves the transaction ABORTED after a failed statement, so every
        # later one fails until the caller rolls back. Modelling that is what makes the
        # probe's rollback observable (a fake without it answers the rest regardless).
        self._aborts_on_failure = aborts_on_failure
        self._aborted = False

    def rollback(self):  # noqa: ANN201
        self.rollbacks += 1
        self._aborted = False

    def execute(self, statement, params=None):  # noqa: ANN001, ANN201
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        if self._aborted:
            raise RuntimeError(
                "current transaction is aborted, commands ignored until end of "
                "transaction block"
            )
        if "SET " in sql:
            return None
        # The byte limit must arrive as a BOUND parameter. Checking only that ":limit"
        # appears in the text is not enough -- dropping the bindparams() call leaves the
        # placeholder in the SQL and the value nowhere, which a permissive fake would
        # happily answer. Record what was actually bound.
        try:
            self.bound.append(dict(statement.compile().params))
        except Exception:  # noqa: BLE001 - only the assertions care
            self.bound.append({})
        for name in self._fail:
            if f'"{name}"' in sql or f"`{name}`" in sql:
                if self._aborts_on_failure:
                    self._aborted = True
                raise RuntimeError("column read failed")

        class _R:
            def __init__(self, value):
                self._v = value

            def first(self):
                return (self._v,)

        hit = any(f'"{n}"' in sql or f"`{n}`" in sql for n in self._oversized)
        return _R(hit)

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False


def _size_probe(source_type, conn):
    from dsql_migrator.core.models import SourceConnectionConfig
    from dsql_migrator.ui.prerequisite_probes import SessionSourceProbe

    probe = SessionSourceProbe(
        SourceConnectionConfig(
            source_type=source_type, host="h", port=5432, username="u"
        ),
        None,
    )

    class _Engine:
        def connect(self):
            return conn

    probe._engine_factory = lambda _cfg: _Engine()  # type: ignore[assignment]
    return probe


def test_the_size_probe_stops_at_the_first_offending_row_and_is_bounded() -> None:
    """This is the ONLY prerequisite that reads row data, so its cost must be capped.

    ``EXISTS`` lets the engine stop at the first offender, and a statement timeout keeps
    proving the ABSENCE of one from becoming an unbounded scan of the customer's source.
    """
    from dsql_migrator.core.models import SourceType

    conn = _SizeProbeConn({"content"})
    answer = _size_probe(SourceType.POSTGRES, conn).oversized_values(
        "ecommerce.product_media",
        ["content", "notes"],
        limit_bytes=1024 * 1024,
        timeout_seconds=15.0,
    )
    assert answer == {"content": True, "notes": False}
    joined = " ".join(conn.statements)
    assert "SELECT EXISTS" in joined, joined
    assert "octet_length" in joined, joined
    # Bounded, and the limit is a BOUND parameter, not formatted into the SQL.
    assert "statement_timeout = 15000" in joined, joined
    assert ":limit" in joined, joined
    assert "1048576" not in joined, joined
    # ...and the value really is bound, not just a placeholder left dangling.
    assert conn.bound and all(
        b.get("limit") == 1024 * 1024 for b in conn.bound
    ), conn.bound
    # Schema-qualified and quoted for the engine.
    assert '"ecommerce"."product_media"' in joined, joined


def test_the_size_probe_uses_mysql_syntax_for_a_mysql_source() -> None:
    from dsql_migrator.core.models import SourceType

    conn = _SizeProbeConn(set())
    _size_probe(SourceType.MYSQL, conn).oversized_values(
        "shop.docs", ["body"], limit_bytes=1024 * 1024, timeout_seconds=5.0
    )
    joined = " ".join(conn.statements)
    assert "MAX_EXECUTION_TIME = 5000" in joined, joined
    assert "`shop`.`docs`" in joined, joined
    assert "`body`" in joined, joined
    assert "statement_timeout" not in joined, joined


def test_one_column_failing_does_not_lose_the_others() -> None:
    from dsql_migrator.core.models import SourceType

    conn = _SizeProbeConn({"notes"}, fail={"content"})
    answer = _size_probe(SourceType.POSTGRES, conn).oversized_values(
        "s.t", ["content", "notes"], limit_bytes=1024 * 1024, timeout_seconds=5.0
    )
    assert answer == {"notes": True}, answer


def test_a_probe_that_establishes_nothing_returns_none_not_a_clean_answer() -> None:
    """``{}`` would grade as "no value exceeds the limit" -- a false pass."""
    from dsql_migrator.core.models import SourceType

    conn = _SizeProbeConn(set(), fail={"content"})
    answer = _size_probe(SourceType.POSTGRES, conn).oversized_values(
        "s.t", ["content"], limit_bytes=1024 * 1024, timeout_seconds=5.0
    )
    assert answer is None, answer


def test_no_columns_is_an_empty_answer_not_a_query() -> None:
    from dsql_migrator.core.models import SourceType

    conn = _SizeProbeConn(set())
    assert _size_probe(SourceType.POSTGRES, conn).oversized_values(
        "s.t", [], limit_bytes=1024 * 1024, timeout_seconds=5.0
    ) == {}
    assert conn.statements == []


def test_a_failed_column_is_rolled_back_so_the_rest_are_still_measured() -> None:
    """On PostgreSQL the FIRST failure used to lose every later column silently.

    A failed statement leaves the transaction aborted, so the per-column loop's remaining
    statements all failed with ``InFailedSqlTransaction`` -- and an unmeasured column was
    then reported inside a green PASS. Reproduced live on a real PostgreSQL 17: probing
    ``price_band`` (numrange, which ``octet_length`` cannot take) followed by
    ``description`` returned only ``{'description': False}`` WITH the rollback, and nothing
    at all without it.
    """
    from dsql_migrator.core.models import SourceType

    conn = _SizeProbeConn({"notes"}, fail={"content"}, aborts_on_failure=True)
    answer = _size_probe(SourceType.POSTGRES, conn).oversized_values(
        "s.t", ["content", "notes"], limit_bytes=1024 * 1024, timeout_seconds=5.0
    )
    # The failing column is OMITTED (-> reported as not-determined), the next one measured.
    assert answer == {"notes": True}, answer
    assert conn.rollbacks == 1, conn.rollbacks
    # SET LOCAL is scoped to the rolled-back transaction, so the cap must be re-applied or
    # the remaining columns would run unbounded on the customer's source.
    assert sum("statement_timeout" in sql for sql in conn.statements) == 2, conn.statements


def test_json_columns_are_measured_by_casting_because_octet_length_has_no_overload() -> None:
    """``octet_length(jsonb)`` does not exist -- verified on live PostgreSQL 17.

    That is how the whole probe used to break on an ordinary schema: the jsonb column's
    statement failed, aborting the transaction. The cast measures the document's serialized
    bytes, which is what the value costs on the target.
    """
    from dsql_migrator.core.models import SourceType

    conn = _SizeProbeConn(set())
    answer = _size_probe(SourceType.POSTGRES, conn).oversized_values(
        "ecommerce.order_events",
        ["payload", "doc", "notes"],
        limit_bytes=1024 * 1024,
        timeout_seconds=5.0,
        column_types={"payload": "jsonb", "doc": "json", "notes": "text"},
    )
    assert answer == {"payload": False, "doc": False, "notes": False}, answer
    joined = " ".join(conn.statements)
    assert 'octet_length("payload"::text)' in joined, joined
    assert 'octet_length("doc"::text)' in joined, joined
    # A text column needs no cast, and must not grow one.
    assert 'octet_length("notes")' in joined, joined
    assert 'octet_length("notes"::text)' not in joined, joined


def test_each_column_is_probed_against_its_own_type_limit() -> None:
    """A flat 1 MiB overstated an unbounded varchar's headroom 16-fold."""
    from dsql_migrator.core.models import SourceType

    conn = _SizeProbeConn(set())
    _size_probe(SourceType.POSTGRES, conn).oversized_values(
        "s.t",
        ["body", "label"],
        limit_bytes=1024 * 1024,
        timeout_seconds=5.0,
        limits={"body": 1024 * 1024, "label": 65535},
        column_types={"body": "text", "label": "character varying"},
    )
    bound = [b.get("limit") for b in conn.bound]
    assert bound == [1024 * 1024, 65535], bound
