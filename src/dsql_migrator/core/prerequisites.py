# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prerequisite checks gating Full Load / CDC (read-only).

Before a Full Load or CDC run, the Data Migration sub-flow always runs a
**gate**: it verifies the migration preconditions for the selected tables and
the source/target environment, and reports each check as PASS/FAIL/WARN/SKIP
with an actionable, credential-free remediation. A ``FAIL`` on any ``required``
check makes :attr:`PrerequisiteReport.can_proceed` ``False`` and blocks that mode
(Property 14).

All checks are **read-only** (Property 1): they only run ``SHOW``/catalog reads
and connection probes, never writes. Inputs are gathered through small,
injectable read-only *probes* (:class:`SourceProbe`, :class:`TargetProbe`,
:class:`MskProbe`) so the checker is unit-testable with fakes and never reaches a
real database or MSK in tests. The per-check logic itself lives in small pure
functions (``check_*``) that take already-gathered facts and return a
:class:`PrerequisiteResult`, so each rule is independently testable.

Mode coverage:

- ``FULL_LOAD`` runs the common checks; the CDC-only checks
  (``BINLOG_ROW_FORMAT`` / ``GTID_MODE`` / ``MSK_AVAILABLE`` /
  ``MSK_CONNECT_AVAILABLE``) are reported as ``SKIP``.
- ``CDC`` runs the common checks plus the CDC-only checks.

Credential confidentiality (Property 7): probe results and remediation strings
never include credential or token values.
"""

from __future__ import annotations

from typing import Optional, Protocol, Sequence

from dsql_migrator.core.models import (
    ConnectionResult,
    MigrationMode,
    PrerequisiteCheckId,
    PrerequisiteCheckRequest,
    PrerequisiteReport,
    PrerequisiteResult,
    PrerequisiteStatus,
    TableDef,
)

# Privileges required on the source MySQL user, by mode. Full Load needs read
# access; CDC additionally needs the replication privileges Debezium uses.
_FULL_LOAD_GRANTS = ("SELECT",)
_CDC_GRANTS = ("SELECT", "REPLICATION CLIENT", "REPLICATION SLAVE")


class SourceProbe(Protocol):
    """Read-only access to the source MySQL used by prerequisite checks."""

    def reachable(self) -> ConnectionResult:
        """Return whether the source is reachable and login succeeds."""

    def grants(self) -> list[str]:
        """Return ``SHOW GRANTS`` lines for the migration user (raw strings)."""

    def variables(self) -> dict[str, str]:
        """Return relevant ``SHOW VARIABLES`` as a name->value map.

        Keys should be the MySQL variable names (e.g. ``log_bin``,
        ``binlog_format``, ``binlog_row_image``, ``gtid_mode``); values are their
        string values (e.g. ``ON`` / ``ROW`` / ``FULL``).
        """


class TargetProbe(Protocol):
    """Read-only access to the target Aurora DSQL used by prerequisite checks."""

    def reachable(self) -> bool:
        """Return whether the DSQL endpoint is reachable (network/TCP)."""

    def iam_auth(self) -> ConnectionResult:
        """Return whether an IAM-token connection + ``SELECT 1`` succeeds."""

    def relation_exists(self, qualified_name: str) -> bool:
        """Return whether the target relation for ``qualified_name`` exists."""


class MskProbe(Protocol):
    """Read-only availability probe for the CDC infrastructure (CDC mode only)."""

    def cluster_available(self) -> bool:
        """Return whether the MSK cluster is reachable."""

    def connect_available(self) -> bool:
        """Return whether MSK Connect (for the Debezium source) is available."""


# ---------------------------------------------------------------------------
# Pure per-check functions (each independently unit-testable)
# ---------------------------------------------------------------------------


def check_source_reachable(result: ConnectionResult) -> PrerequisiteResult:
    """PASS when the source connection check succeeded."""
    ok = result.success
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.SOURCE_REACHABLE,
        title="Source database is reachable",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.FAIL,
        required=True,
        detail="Connected to the source." if ok else "Could not connect to the source.",
        remediation=""
        if ok
        else "Verify host/port/security group and source credentials.",
    )


def check_replication_grants(
    grants: list[str], mode: MigrationMode
) -> PrerequisiteResult:
    """PASS when the source user holds the privileges required for ``mode``.

    Full Load requires ``SELECT``; CDC additionally requires ``REPLICATION
    CLIENT`` and ``REPLICATION SLAVE``. ``ALL PRIVILEGES`` satisfies any
    requirement. Matching is case-insensitive over the raw ``SHOW GRANTS`` text.
    """
    required = _CDC_GRANTS if mode == MigrationMode.CDC else _FULL_LOAD_GRANTS
    blob = " ".join(grants).upper()
    has_all = "ALL PRIVILEGES" in blob
    missing = [
        priv for priv in required if not has_all and priv.upper() not in blob
    ]
    ok = not missing
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.REPLICATION_GRANTS,
        title="Source user has the required privileges",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.FAIL,
        required=True,
        detail="Required privileges present."
        if ok
        else f"Missing privileges: {', '.join(missing)}.",
        remediation=""
        if ok
        else (
            "Use a dedicated least-privilege CDC user granted SELECT, REPLICATION "
            "CLIENT, REPLICATION SLAVE (and RELOAD + LOCK TABLES for the initial "
            "snapshot) rather than an admin account."
            if mode == MigrationMode.CDC
            else "Grant SELECT to a dedicated migration user (avoid an admin account)."
        ),
    )


def check_table_primary_key(table: TableDef) -> PrerequisiteResult:
    """PASS when ``table`` has a primary key (required for keying/upsert)."""
    ok = bool(table.primary_key)
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.TABLE_PRIMARY_KEY,
        title="Table has a primary key",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.FAIL,
        required=True,
        target=table.name,
        detail="Primary key present."
        if ok
        else "No primary key on the selected table.",
        remediation=""
        if ok
        else (
            f"Add a primary key to `{table.name}`; CDC keying and idempotent "
            "upsert require it."
        ),
    )


def check_target_dsql_reachable(reachable: bool) -> PrerequisiteResult:
    """PASS when the DSQL endpoint is reachable."""
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.TARGET_DSQL_REACHABLE,
        title="Target DSQL endpoint is reachable",
        status=PrerequisiteStatus.PASS if reachable else PrerequisiteStatus.FAIL,
        required=True,
        detail="DSQL endpoint reachable." if reachable else "DSQL endpoint unreachable.",
        remediation=""
        if reachable
        else "Check VPC route/NAT/PrivateLink to the DSQL endpoint (port 5432).",
    )


def check_target_iam_auth(result: ConnectionResult) -> PrerequisiteResult:
    """PASS when an IAM-token connection to DSQL succeeds."""
    ok = result.success
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.TARGET_IAM_AUTH,
        title="Target DSQL IAM authentication works",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.FAIL,
        required=True,
        detail="IAM token connection succeeded."
        if ok
        else "IAM token connection failed.",
        remediation=""
        if ok
        else (
            "Attach dsql:DbConnect/DbConnectAdmin to the task or connector "
            "execution role for this cluster."
        ),
    )


def check_target_schema_ready(table: TableDef, exists: bool) -> PrerequisiteResult:
    """PASS when the target table for ``table`` already exists (DDL applied)."""
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.TARGET_SCHEMA_READY,
        title="Target schema is ready for the table",
        status=PrerequisiteStatus.PASS if exists else PrerequisiteStatus.FAIL,
        required=True,
        target=table.name,
        detail="Target table exists." if exists else "Target table does not exist.",
        remediation=""
        if exists
        else f"Apply converted DDL for `{table.name}` in Step 2 (Schema Conversion) first.",
    )


def _on(value: Optional[str]) -> bool:
    """Return True when a MySQL variable value means 'on' (case-insensitive)."""
    return (value or "").strip().upper() in {"ON", "1"}


def check_binlog_row_format(variables: dict[str, str]) -> PrerequisiteResult:
    """PASS when binary logging is on with ROW format and a FULL row image."""
    log_bin = _on(variables.get("log_bin"))
    row_format = (variables.get("binlog_format") or "").strip().upper() == "ROW"
    full_image = (variables.get("binlog_row_image") or "").strip().upper() == "FULL"
    ok = log_bin and row_format and full_image
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.BINLOG_ROW_FORMAT,
        title="Binary log uses ROW format with full row image",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.FAIL,
        required=True,
        detail="log_bin=ON, binlog_format=ROW, binlog_row_image=FULL."
        if ok
        else "Binary logging is not ROW/FULL or is disabled.",
        remediation=""
        if ok
        else (
            "Set binlog_format=ROW and binlog_row_image=FULL on the source "
            "(RDS parameter group)."
        ),
    )


def check_gtid_mode(variables: dict[str, str]) -> PrerequisiteResult:
    """PASS when GTID mode is on; otherwise a non-blocking ``INFO`` recommendation.

    GTID is **not required** for CDC. Debezium and the watermark-based resume
    (:class:`~dsql_migrator.core.cdc.CdcResumePoint`) fall back to the binlog
    ``file:position`` coordinate when GTID is unavailable, so the gapless Full
    Load -> CDC handoff still works. GTID is only *recommended* because it
    survives source failover/replica promotion (where ``file:position`` can
    break). Its absence is therefore reported as ``INFO`` (an optional
    recommendation, ``required=False``) -- NOT a ``WARN`` (which implies something
    is wrong) and NOT a gating ``FAIL`` (Property 14). Customers without GTID
    enabled are not forced to turn it on and are not shown an alarming warning.
    """
    ok = (variables.get("gtid_mode") or "").strip().upper() == "ON"
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.GTID_MODE,
        title="GTID mode is enabled (recommended)",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.INFO,
        required=False,
        detail="gtid_mode=ON."
        if ok
        else "gtid_mode is not ON; CDC will resume from the binlog file:position watermark.",
        remediation=""
        if ok
        else (
            "Optional: enable GTID mode for more robust CDC resume across source "
            "failover/replica promotion. Not required -- CDC otherwise resumes "
            "from the binlog file:position watermark."
        ),
    )


def check_msk_available(available: bool) -> PrerequisiteResult:
    """Report MSK cluster presence as a non-blocking ``INFO`` (not WARN, not FAIL).

    MSK is NOT a CDC prerequisite: in this tool's model the MSK Serverless
    cluster is created when the customer deploys the cdc-stack -- which happens
    AFTER the CDC step produces the connector config, not before. So when the
    cluster is not yet present this is expected, no-action-needed ``INFO`` (not a
    ``WARN`` that implies a problem, and never a required ``FAIL`` that would
    block reaching the CDC step). When the cluster IS already present (e.g. a
    prior deploy) it is reported ``PASS`` as useful confirmation.
    """
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.MSK_AVAILABLE,
        title="MSK cluster is available",
        status=PrerequisiteStatus.PASS if available else PrerequisiteStatus.INFO,
        required=False,
        detail="MSK cluster reachable."
        if available
        else "MSK cluster not deployed yet (created when you deploy the cdc-stack).",
        remediation=""
        if available
        else (
            "Not a prerequisite -- the MSK Serverless cluster is provisioned when "
            "you deploy the cdc-stack, which you do AFTER completing the CDC step "
            "(it produces the connector config to deploy). No action needed here."
        ),
    )


def check_msk_connect_available(available: bool) -> PrerequisiteResult:
    """Report MSK Connect presence as a non-blocking ``INFO`` (not WARN, not FAIL).

    Like :func:`check_msk_available`, MSK Connect (which runs the Debezium source
    and DSQL sink connectors) is created by the cdc-stack deploy that follows the
    CDC step -- so its absence is expected, no-action-needed ``INFO``, not a
    ``WARN`` (no problem here) and not a blocking ``FAIL``. Present (a prior
    deploy) reports ``PASS``.
    """
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.MSK_CONNECT_AVAILABLE,
        title="MSK Connect is available",
        status=PrerequisiteStatus.PASS if available else PrerequisiteStatus.INFO,
        required=False,
        detail="MSK Connect available."
        if available
        else "MSK Connect not deployed yet (created when you deploy the cdc-stack).",
        remediation=""
        if available
        else (
            "Not a prerequisite -- MSK Connect and its connectors are provisioned "
            "when you deploy the cdc-stack after the CDC step. No action needed here."
        ),
    )


def _skipped(check_id: PrerequisiteCheckId, title: str) -> PrerequisiteResult:
    """Build a non-applicable (SKIP) result for a check not run in this mode."""
    return PrerequisiteResult(
        check_id=check_id,
        title=title,
        status=PrerequisiteStatus.SKIP,
        required=True,
        detail="Not applicable for this mode.",
    )


class PrerequisiteChecker:
    """Runs the prerequisite gate for a mode over the selected tables.

    The source/target/MSK access is supplied as injectable read-only probes, so
    the checker performs no writes (Property 1) and is unit-testable with fakes.
    """

    def __init__(
        self,
        *,
        source_probe: SourceProbe,
        target_probe: TargetProbe,
        msk_probe: Optional[MskProbe] = None,
    ) -> None:
        """Create a checker.

        ``source_probe`` and ``target_probe`` are required. ``msk_probe`` is only
        needed for ``CDC`` mode; in ``FULL_LOAD`` the MSK checks are skipped and a
        missing probe is fine. All probes are read-only.
        """
        self._source = source_probe
        self._target = target_probe
        self._msk = msk_probe

    def check(
        self,
        request: PrerequisiteCheckRequest,
        *,
        tables: Sequence[TableDef],
    ) -> PrerequisiteReport:
        """Run all checks for ``request.mode`` over the resolved ``tables``.

        Produces one :class:`PrerequisiteResult` per check (and per selected
        table for ``TABLE_PRIMARY_KEY`` / ``TARGET_SCHEMA_READY``). CDC mode adds
        the binlog/GTID/MSK checks; Full Load reports those as ``SKIP``. Returns a
        :class:`PrerequisiteReport` whose ``can_proceed`` is ``True`` only when no
        required check failed (Property 14). All probe access is read-only.
        """
        results: list[PrerequisiteResult] = []

        # Source-side checks
        results.append(check_source_reachable(self._source.reachable()))
        results.append(check_replication_grants(self._source.grants(), request.mode))

        # Per-table checks
        for table in tables:
            results.append(check_table_primary_key(table))

        # Target-side checks
        results.append(check_target_dsql_reachable(self._target.reachable()))
        results.append(check_target_iam_auth(self._target.iam_auth()))
        for table in tables:
            results.append(
                check_target_schema_ready(
                    table, self._target.relation_exists(table.name)
                )
            )

        # CDC-only checks (SKIP in Full Load).
        if request.mode == MigrationMode.CDC:
            variables = self._source.variables()
            results.append(check_binlog_row_format(variables))
            results.append(check_gtid_mode(variables))
            results.append(
                check_msk_available(
                    self._msk.cluster_available() if self._msk else False
                )
            )
            results.append(
                check_msk_connect_available(
                    self._msk.connect_available() if self._msk else False
                )
            )
        else:
            results.append(
                _skipped(
                    PrerequisiteCheckId.BINLOG_ROW_FORMAT,
                    "Binary log uses ROW format with full row image",
                )
            )
            results.append(
                _skipped(PrerequisiteCheckId.GTID_MODE, "GTID mode is enabled")
            )
            results.append(
                _skipped(PrerequisiteCheckId.MSK_AVAILABLE, "MSK cluster is available")
            )
            results.append(
                _skipped(
                    PrerequisiteCheckId.MSK_CONNECT_AVAILABLE,
                    "MSK Connect is available",
                )
            )

        return PrerequisiteReport.build(request.mode, results)


__all__ = [
    "SourceProbe",
    "TargetProbe",
    "MskProbe",
    "PrerequisiteChecker",
    "check_source_reachable",
    "check_replication_grants",
    "check_table_primary_key",
    "check_target_dsql_reachable",
    "check_target_iam_auth",
    "check_target_schema_ready",
    "check_binlog_row_format",
    "check_gtid_mode",
    "check_msk_available",
    "check_msk_connect_available",
]
