# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read-only prerequisite probes wired to a session's live connections.

The pure prerequisite check functions live in
:mod:`dsql_migrator.core.prerequisites`; they take already-gathered facts. This
module supplies those facts by adapting the session's configured source/target
connections into the read-only :class:`SourceProbe` / :class:`TargetProbe` /
:class:`MskProbe` surfaces the :class:`PrerequisiteChecker` consumes.

Every probe is **read-only** (Property 1): the source adapter runs only
``SELECT 1`` / ``SHOW GLOBAL VARIABLES`` and the dialect's engine-specific grant
probe, and the target adapter only validates connectivity and reads the catalog.
Probe methods are
defensive: any access error is turned into a "not satisfied" result (a failing
:class:`ConnectionResult`, empty grants/variables, or ``False`` existence) so the
corresponding check reports a ``FAIL`` with actionable remediation rather than
crashing the run. Credential/token values never appear in returned detail
strings (Property 7).

MSK availability is probed read-only against the account the sink streams
through: :class:`SessionMskProbe` calls ``kafka:ListClustersV2`` and
``kafkaconnect:ListConnectors`` in the target's region and reports the cluster
(and MSK Connect) as available only when a cluster is ``ACTIVE`` and at least
one connector exists. MSK is NOT a blocking prerequisite -- it is created by the
cdc-stack deploy that follows the CDC step -- so an unavailable result surfaces
as a non-blocking advisory ``WARN`` (see ``check_msk_available``), not a failure.
:class:`UnavailableMskProbe` is an explicit always-unavailable fallback.
"""

from __future__ import annotations

from typing import Optional, Sequence

from sqlalchemy import text

from dsql_migrator.config import SecretValue
from dsql_migrator.core.aws_session import BotoSessionLike, build_session
from dsql_migrator.core.models import (
    ConnectionResult,
    SourceConnectionConfig,
    TargetConnectionConfig,
)
from dsql_migrator.core.prerequisites import PrerequisiteChecker
from dsql_migrator.core.target_connection import DsqlConnector
from dsql_migrator.core.target_introspector import (
    TargetIntrospector,
    target_required_columns_without_default,
)
from dsql_migrator.ui.connect import make_source_engine_factory

# MSK Serverless/provisioned cluster states that mean the broker is usable.
_MSK_CLUSTER_READY_STATES = frozenset({"ACTIVE"})

# MySQL global variables relevant to the CDC binlog/GTID/retention prerequisite
# checks. ``binlog_expire_logs_seconds`` / ``expire_logs_days`` are the self-managed
# binlog-retention signals (RDS retention is read separately from
# ``mysql.rds_configuration`` -- see ``variables``).
_CDC_VARIABLES = (
    "log_bin",
    "binlog_format",
    "binlog_row_image",
    "gtid_mode",
    "binlog_expire_logs_seconds",
    "expire_logs_days",
)


class SessionSourceProbe:
    """Read-only :class:`SourceProbe` over a session's source connection (any engine)."""

    def __init__(
        self,
        source_config: SourceConnectionConfig,
        source_password: Optional[SecretValue],
    ) -> None:
        """Bind the probe to one session's source config + in-memory password."""
        self._config = source_config
        self._engine_factory = make_source_engine_factory(source_password)

    def reachable(self) -> ConnectionResult:
        """Return whether the source is reachable and login succeeds (SELECT 1)."""
        try:
            engine = self._engine_factory(self._config)
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return ConnectionResult(success=True, detail="Connected to the source.")
        except Exception as exc:  # noqa: BLE001 - surfaced as a failing check
            return ConnectionResult(
                success=False, detail=f"Source connection failed: {type(exc).__name__}"
            )

    def grants(self) -> list[str]:
        """Return the source user's privilege grant lines (empty on error).

        Delegates to the source dialect so the grant surface is engine-correct: MySQL
        reads ``SHOW GRANTS``; PostgreSQL (no ``SHOW GRANTS``) derives it from superuser
        status / ``role_table_grants``. A single MySQL statement run against PostgreSQL
        would error to empty and falsely FAIL the privilege prerequisite, blocking the
        Full Load on a perfectly-privileged PG source.
        """
        from dsql_migrator.core.source_dialect import dialect_for

        dialect = dialect_for(self._config.source_type)
        try:
            engine = self._engine_factory(self._config)
            with engine.connect() as connection:
                return dialect.probe_grants(connection)
        except Exception:  # noqa: BLE001 - treated as "no grants visible"
            return []

    def variables(self) -> dict[str, str]:
        """Return the CDC-relevant source variables (empty on error).

        The ``SHOW GLOBAL VARIABLES`` names are BOUND, not formatted into the
        statement. Nothing external can reach this list either way (it is a module
        constant), but these are VALUES -- unlike a schema/table name, which cannot be
        a bind parameter at all -- so binding is possible here, and doing it keeps the
        statement text a plain literal. tests/test_prerequisite_probes.py pins the
        placeholder count to _CDC_VARIABLES, so adding a variable cannot silently drop
        it from the query. The RDS-only ``binlog retention hours`` is read separately
        from ``mysql.rds_configuration`` and folded into the SAME map under the
        synthetic key ``rds_binlog_retention_hours`` (``"0"`` = the RDS row exists but
        is unset -> RDS purges aggressively), so the pure check reads one dict.
        """
        params = {f"v{index}": name for index, name in enumerate(_CDC_VARIABLES)}
        placeholders = ", ".join(f":v{index}" for index in range(len(_CDC_VARIABLES)))
        result: dict[str, str] = {}
        try:
            engine = self._engine_factory(self._config)
            with engine.connect() as connection:
                rows = connection.execute(
                    text(
                        "SHOW GLOBAL VARIABLES WHERE Variable_name "
                        f"IN ({placeholders})"
                    ),
                    params,
                ).fetchall()
            result = {str(row[0]): str(row[1]) for row in rows if len(row) >= 2}
        except Exception:  # noqa: BLE001 - treated as "variables unknown"
            result = {}
        # RDS-specific binlog retention (best-effort, SEPARATE try so a non-RDS source
        # -- where mysql.rds_configuration does not exist -- never wipes the vars above).
        try:
            engine = self._engine_factory(self._config)
            with engine.connect() as connection:
                rrows = connection.execute(
                    text(
                        "SELECT value FROM mysql.rds_configuration "
                        "WHERE name = 'binlog retention hours'"
                    )
                ).fetchall()
            if rrows:  # the row exists => this IS an RDS source
                raw = rrows[0][0]
                # NULL/empty => retention unset => RDS purges aggressively => risk ("0").
                result["rds_binlog_retention_hours"] = (
                    str(raw).strip() if raw is not None and str(raw).strip() else "0"
                )
        except Exception:  # noqa: BLE001 - not RDS / no access -> key absent
            pass
        return result

    def cdc_prerequisites(self, table_names):
        """Return the PostgreSQL CDC readiness facts, or None for a MySQL source.

        Delegates to the source dialect (read-only), mirroring :meth:`grants`: only
        ``PostgresSourceDialect`` gathers the logical-replication facts; MySQL returns
        None (it uses the binlog/GTID variable checks). Any connection failure degrades
        to None so the checks report "unknown" rather than crashing the gate.
        """
        from dsql_migrator.core.source_dialect import dialect_for

        dialect = dialect_for(self._config.source_type)
        try:
            engine = self._engine_factory(self._config)
            with engine.connect() as connection:
                return dialect.probe_cdc_prerequisites(connection, list(table_names))
        except Exception:  # noqa: BLE001 - treated as "facts unknown"
            return None


class SessionTargetProbe:
    """Read-only :class:`TargetProbe` over a session's Aurora DSQL target."""

    def __init__(
        self,
        target_config: TargetConnectionConfig,
        *,
        aws_profile: Optional[str] = None,
    ) -> None:
        """Bind the probe to one session's target config + global AWS profile."""
        self._config = target_config
        self._connector = DsqlConnector(target_config, aws_profile=aws_profile)
        self._introspector = TargetIntrospector()
        self._browsed = False
        self._browse_ok = False

    def reachable(self) -> bool:
        """Return whether the DSQL endpoint accepts a connection (SELECT 1)."""
        return self._connector.test_connection().success

    def iam_auth(self) -> ConnectionResult:
        """Return whether an IAM-token connection + ``SELECT 1`` succeeds."""
        return self._connector.test_connection()

    def relation_exists(self, qualified_name: str) -> bool:
        """Return whether the target relation exists (browse once, then lookup).

        The catalog is browsed lazily on first call and cached; a browse failure
        yields ``False`` so the schema-ready check fails closed with guidance to
        apply DDL first.
        """
        if not self._browsed:
            self._browsed = True
            try:
                self._introspector.browse(self._config)
                self._browse_ok = True
            except Exception:  # noqa: BLE001 - treated as "schema not ready"
                self._browse_ok = False
        if not self._browse_ok:
            return False
        try:
            return self._introspector.object_exists(qualified_name)
        except Exception:  # noqa: BLE001 - treated as "not present"
            return False

    def required_columns_without_default(
        self, qualified_name: str
    ) -> Optional[Sequence[str]]:
        """Return the target's NOT NULL / no-default / non-identity columns.

        Read live from the DSQL catalog (not the browsed inventory, which does not
        carry column defaults). ``None`` on any error or a missing table, so the
        columns check defers to TARGET_SCHEMA_READY instead of false-failing.
        """
        return target_required_columns_without_default(
            qualified_name, connection_factory=self._connector.connect
        )


class SessionMskProbe:
    """Read-only :class:`MskProbe` over the target account's MSK + MSK Connect.

    Probes the region the sink streams into (the target DSQL region) with two
    read-only calls -- ``kafka:ListClustersV2`` and
    ``kafkaconnect:ListConnectors`` -- and reports availability based on what is
    actually deployed: the cluster is available when at least one MSK cluster is
    ``ACTIVE``; MSK Connect is available when at least one connector exists. Every
    access error (missing credentials/permissions, unreachable region, or the
    ``cdc-stack`` simply not deployed) returns ``False``.

    Note: MSK is NOT a blocking prerequisite. The cdc-stack (which creates MSK +
    the connectors) is deployed AFTER the CDC step produces the connector config,
    so a ``False`` here surfaces as a non-blocking advisory ``WARN`` (see
    ``check_msk_available``), confirming presence when a prior deploy exists --
    not failing the run. No credential value is read or logged (Property 7); the
    shared :func:`build_session` honors the single global AWS profile.
    """

    def __init__(
        self,
        region: str,
        *,
        aws_profile: Optional[str] = None,
        session: Optional[BotoSessionLike] = None,
    ) -> None:
        """Bind the probe to the MSK region + global AWS profile.

        ``session`` is an injection seam for tests (a fake ``boto3.Session``);
        when omitted the shared profile-aware session is built lazily on first
        use so constructing the probe never reaches AWS.
        """
        self._region = region
        self._aws_profile = aws_profile
        self._session = session

    def _client(self, service_name: str) -> object:
        session = self._session or build_session(self._aws_profile)
        return session.client(service_name, region_name=self._region)

    def cluster_available(self) -> bool:
        """Return whether at least one MSK cluster is ``ACTIVE`` in the region."""
        try:
            client = self._client("kafka")
            response = client.list_clusters_v2()
            clusters = response.get("ClusterInfoList", []) or []
            return any(
                str(cluster.get("State", "")).upper() in _MSK_CLUSTER_READY_STATES
                for cluster in clusters
            )
        except Exception:  # noqa: BLE001 - treated as "MSK not available"
            return False

    def connect_available(self) -> bool:
        """Return whether at least one MSK Connect connector exists in the region."""
        try:
            client = self._client("kafkaconnect")
            response = client.list_connectors()
            return bool(response.get("connectors", []) or [])
        except Exception:  # noqa: BLE001 - treated as "MSK Connect not available"
            return False


class UnavailableMskProbe:
    """Explicit always-unavailable :class:`MskProbe` fallback.

    Reports MSK and MSK Connect as unavailable, which surfaces as a non-blocking
    advisory ``WARN`` (not a blocking FAIL) -- MSK is created by the later
    cdc-stack deploy, not a prerequisite. Used when no MSK region/credentials are
    configured; :class:`SessionMskProbe` is the default once a target connection
    exists.
    """

    def cluster_available(self) -> bool:
        """Return ``False`` (no MSK cluster provisioned yet)."""
        return False

    def connect_available(self) -> bool:
        """Return ``False`` (no MSK Connect provisioned yet)."""
        return False


def build_prerequisite_checker(
    *,
    source_config: SourceConnectionConfig,
    source_password: Optional[SecretValue],
    target_config: TargetConnectionConfig,
    aws_profile: Optional[str] = None,
) -> PrerequisiteChecker:
    """Wire a :class:`PrerequisiteChecker` from a session's live connections.

    Builds read-only source/target probes from the configured connections and an
    MSK probe that reads the target region's MSK + MSK Connect state
    (:class:`SessionMskProbe`); when AWS is unreachable or the ``cdc-stack`` is
    not deployed the MSK probe reports unavailable, which is a non-blocking
    advisory (MSK is provisioned by the later cdc-stack deploy, not a
    prerequisite). All probes are read-only (Property 1) and never expose
    credential values (Property 7).
    """
    return PrerequisiteChecker(
        source_probe=SessionSourceProbe(source_config, source_password),
        target_probe=SessionTargetProbe(target_config, aws_profile=aws_profile),
        msk_probe=SessionMskProbe(target_config.region, aws_profile=aws_profile),
    )


__all__ = [
    "SessionSourceProbe",
    "SessionTargetProbe",
    "SessionMskProbe",
    "UnavailableMskProbe",
    "build_prerequisite_checker",
]
