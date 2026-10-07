# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Deploy and manage the whole CDC pipeline lifecycle via the cdc-stack.

The UI owns the entire CDC lifecycle through this module, all expressed as
CloudFormation operations on the canonical cdc-stack (``deploy/cdc-stack/
cdc-stack.yaml``), which stays the single source of truth for the gotcha-laden
connector configuration:

* **Deploy infra** (:func:`run_cdc_infra_deploy`) -- ``create_stack`` with the
  customer's BYO-VPC inputs, ``MskBootstrapServers=""`` and
  ``DeploySink="false"`` so MSK / VPC wiring / plugins / IAM are created but no
  connectors yet (the MSK Serverless cluster takes ~15-20 min).
* **Start CDC** (:func:`run_cdc_start`) -- a SINGLE-pass ``update_stack``: fetch
  the cluster bootstrap brokers, then set ``MskBootstrapServers`` +
  ``DeploySink="true"`` so CloudFormation runs ``CdcStartPrepResource`` (which
  PRE-CREATES the per-table topics, and on a gapless handoff seeds the offset) and
  then creates the source AND sink connectors IN PARALLEL -- both DependsOn only
  the pre-created topics, not each other. Pre-creating the topics removes the
  empty-partition-assignment race (formerly gotcha #11, worked around by a serial
  source-then-sink two-pass) at its source and roughly halves the connector wall
  time. We then wait for both connectors to reach RUNNING.
* **Stop CDC** (:func:`run_cdc_stop`) -- ``update_stack`` setting
  ``MskBootstrapServers=""``; both connectors' template conditions
  (``HasBootstrapServers`` / ``DeploySinkConnector``) go false, so
  CloudFormation deletes just the two connectors and keeps MSK / VPC / plugins /
  IAM for a fast restart. MSK Connect has no pause API, so stop == delete.
* **Delete infra** (:func:`run_cdc_delete`) -- ``delete_stack`` to tear the whole
  stack down (also the recovery path for a create that ended in
  ``ROLLBACK_COMPLETE``).

Why CloudFormation and not ``kafkaconnect:CreateConnector`` directly: the
connector config has 20+ keys plus a Secrets Manager config-provider colon
syntax, MSK Serverless topic-creation rules, a self-managed schema-history
topic, and a separate WorkerConfiguration -- all already encoded correctly in
the template. Re-encoding them in Python would create a second source of truth
(drift). CloudFormation keeps one source of truth and gets automatic rollback +
rich stack-event logs for the step-by-step progress UI.

Mutating operations (``create_stack`` / ``update_stack`` / ``delete_stack``) are
each gated behind an explicit UI confirmation. boto3 clients are built from the
shared profile-aware :func:`build_session`, injectable for tests.

WARNING -- partition quota: every connector create/delete consumes MSK
Serverless partition quota that does not come back (topics survive rollback), so
the Start path is idempotent (it skips the update when a connector is already
RUNNING) to avoid burning quota on retries.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence

from dsql_migrator.config import SecretValue
from dsql_migrator.core import cdc_pg_slot
from dsql_migrator.core.aws_session import BotoSessionLike, build_session
from dsql_migrator.core.cdc import (
    CDC_PLACEHOLDER_PREFIX,
    CDC_STACK_NAME_PREFIX,
    CDC_WATERMARK_PARAM_KEYS,
    CdcInfraParams,
    CdcStackParams,
    apply_column_exclude_chooser,
    choose_rekey_routing,
    column_exclude_seen_refusal,
    build_watermark_params,
    legacy_column_exclude_key_conflicts,
    cdc_expected_connector_names,
    rekey_parameter_size_refusal,
    rekey_routing_log_lines,
)
from dsql_migrator.core.cdc_coords import gtid_set_summary, normalize_gtid_set
from dsql_migrator.core.models import (
    ChunkState,
    MigrationJob,
    SourceConnectionConfig,
    Watermark,
)

# The CdcStackDeployer boto3/CloudFormation wrapper (its data types, the
# build_cdc_stack_deployer factory, and the _parse_unsupported_azs / _stack_absent_error
# helpers it uses) was extracted to cdc_stack_deployer.py for maintainability. Re-exported
# so the orchestration below and the existing test/ui imports
# (`from dsql_migrator.core.cdc_deployer import CdcStackDeployer`) resolve unchanged.
from dsql_migrator.core.cdc_stack_deployer import (  # noqa: F401
    _UNSUPPORTED_AZ_RE,
    _parse_unsupported_azs,
    CdcDeployError,
    CdcStackDeployer,
    CdcStackDiscovery,
    build_cdc_stack_deployer,
)

_LOGGER = logging.getLogger(__name__)


# Each lifecycle operation is a list of ordered stages, surfaced as a
# step-by-step progress log. The chunk_id is the first element, the human label
# the second. Stage ids are unique within an operation.

# Deploy CDC infra: ensure the plugin bucket + upload artifacts, then create_stack
# the MSK / VPC wiring / plugins / IAM (no connectors).
CDC_INFRA_STAGES: tuple[tuple[str, str], ...] = (
    ("ensure_bucket", "Ensuring plugin S3 bucket"),
    ("upload_plugins", "Uploading connector plugins"),
    ("check_existing", "Checking for existing stack"),
    ("validate_params", "Validating infrastructure parameters"),
    ("create_stack", "Submitting stack creation"),
    ("stack_create", "Creating infrastructure (usually ~5 min)"),
    ("infra_ready", "Infrastructure ready"),
)

# Start CDC: a SINGLE-pass update creates BOTH connectors at once. The stack's
# CdcStartPrepResource pre-creates the per-table topics (so the sink no longer hits
# the empty-partition-assignment race and no longer has to wait for the source to
# auto-create them). Source and sink then deploy IN PARALLEL and are waited on
# together in one "connectors_running" stage -- roughly halving the connector wall
# time vs the old source-then-sink two-pass. The per-connector state (source /sink
# still CREATING vs RUNNING) is shown by the live connector-state chips.
CDC_START_STAGES: tuple[tuple[str, str], ...] = (
    ("discover_stack", "Discovering cdc-stack"),
    ("validate_params", "Validating configuration"),
    ("fetch_bootstrap", "Fetching MSK bootstrap brokers"),
    ("submit_connectors", "Starting connectors (topics + source + sink)"),
    ("stack_connectors", "Connectors deploying"),
    ("connectors_running", "Waiting for connectors (source + sink)"),
    ("pipeline_running", "Pipeline running"),
)

# Stop CDC: update MskBootstrapServers="" so CloudFormation deletes both connectors.
CDC_STOP_STAGES: tuple[tuple[str, str], ...] = (
    ("discover_stack", "Discovering cdc-stack"),
    ("submit_stop", "Submitting connector removal"),
    ("stack_stop", "Removing connectors"),
    ("connectors_gone", "CDC stopped"),
)

# Delete CDC infra: delete_stack the whole thing.
CDC_DELETE_STAGES: tuple[tuple[str, str], ...] = (
    ("discover_stack", "Discovering cdc-stack"),
    ("submit_delete", "Submitting stack deletion"),
    ("stack_delete", "Deleting infrastructure"),
    ("cleanup_secret", "Removing the source-credentials secret"),
    ("deleted", "Infrastructure deleted"),
)

# MSK Connect connector states meaning "fully up" vs "terminally failed".
_CONNECTOR_RUNNING = "RUNNING"
_CONNECTOR_FAILED = "FAILED"

# How many CONSECUTIVE connector-state read failures a RUNNING-wait tolerates before
# it gives up and surfaces the cause. A read failure (throttle, transient network,
# credential expiry) must NOT masquerade as "still creating" and stall the wait
# forever with no error -- the observed failure mode where the source connector was
# RUNNING but the deploy never advanced to the sink pass.
_MAX_STATE_READ_FAILURES = 5

# AWS error codes/markers for a NON-self-healing read failure (credentials expired,
# access lost). These will never recover on retry, so the wait fails immediately
# with an actionable cause instead of burning the whole retry budget.
_TERMINAL_READ_ERROR_MARKERS = (
    "ExpiredToken",
    "ExpiredTokenException",
    "InvalidClientTokenId",
    "UnrecognizedClientException",
    "SignatureDoesNotMatch",
    "AccessDenied",
    "AccessDeniedException",
    "UnauthorizedOperation",
)


def _is_terminal_read_error(exc: Exception) -> bool:
    """True when ``exc`` is a credential/authorization failure that will not recover
    on retry (so the RUNNING-wait should fail fast with the cause, not keep polling)."""
    code = ""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = str(response.get("Error", {}).get("Code", ""))
    text = code + " " + str(exc)
    return any(marker in text for marker in _TERMINAL_READ_ERROR_MARKERS)


@dataclass
class CdcDeployLog:
    """An append-only, timestamped progress log for one deploy run.

    Lives on the session state (transient); the work function appends one line
    per meaningful step / stack event and the UI renders them logging-style.
    """

    lines: list[tuple[datetime, str]] = field(default_factory=list)

    def append(self, when: datetime, message: str) -> None:
        self.lines.append((when, message))


# -- Stage progression on a JobManager job ----------------------------------


def _seed_stages(job: MigrationJob, stages: tuple[tuple[str, str], ...]) -> None:
    """Seed one PENDING chunk per stage of an operation (chunk_id = stage name)."""
    job.chunks = [ChunkState(chunk_id=name) for name, _ in stages]


def seed_infra_stages(job: MigrationJob) -> None:
    """Seed PENDING chunks for the Deploy-infra operation."""
    _seed_stages(job, CDC_INFRA_STAGES)


def seed_start_stages(job: MigrationJob) -> None:
    """Seed PENDING chunks for the Start-CDC operation."""
    _seed_stages(job, CDC_START_STAGES)


def seed_stop_stages(job: MigrationJob) -> None:
    """Seed PENDING chunks for the Stop-CDC operation."""
    _seed_stages(job, CDC_STOP_STAGES)


def seed_delete_stages(job: MigrationJob) -> None:
    """Seed PENDING chunks for the Delete-infra operation."""
    _seed_stages(job, CDC_DELETE_STAGES)


def _set_stage(job: MigrationJob, stage: str, status: str) -> None:
    for chunk in job.chunks:
        if chunk.chunk_id == stage:
            chunk.status = status  # type: ignore[assignment]
            return


def _now() -> datetime:
    return datetime.now(timezone.utc)


class _StageDriver:
    """Tiny helper binding a JobManager handle + log to a single operation's stages.

    Centralises the boilerplate every ``run_cdc_*`` shares: seed the chunks, mark
    a stage IN_PROGRESS/DONE/FAILED, append a timestamped log line, and -- on a
    :class:`CdcDeployError` -- log it + bump the job's ``error_count`` before
    re-raising. ``sleep`` is injectable so tests run without real waits.
    """

    def __init__(
        self,
        handle,
        *,
        stages: tuple[tuple[str, str], ...],
        on_log: Callable[[datetime, str], None],
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        import time as _time

        self._handle = handle
        self._stages = stages
        self._on_log = on_log
        self.sleep = sleep or _time.sleep
        handle.update(lambda job: _seed_stages(job, stages))

    @property
    def cancelled(self) -> bool:
        return bool(self._handle.cancelled)

    def log(self, msg: str) -> None:
        self._on_log(_now(), msg)

    def heartbeat(self) -> None:
        """Refresh the job's liveness clock without changing its state.

        CDC lifecycle waits (MSK create ~15-20 min, a connector reaching RUNNING
        up to 45 min) can pass many minutes between stack events, so the
        JobManager's stall watchdog (DEFAULT_STALL_TIMEOUT_SECONDS, 15 min) would
        otherwise reap a perfectly healthy, still-provisioning job. A no-op
        ``handle.update`` refreshes ``last_progress_at`` (apply_update stamps it on
        every mutation), proving liveness each poll so only a genuinely wedged
        operation is ever reaped. Cheap: it mutates nothing and persists the
        unchanged snapshot.
        """
        self._handle.update(lambda job: None)

    def stage(self, name: str, status: str) -> None:
        self._handle.update(lambda job, n=name, s=status: _set_stage(job, n, s))

    def all_done(self) -> None:
        """Mark every stage of this operation DONE (idempotent fast-path)."""
        for name, _ in self._stages:
            self.stage(name, "DONE")

    def fail(self, exc: CdcDeployError) -> None:
        """Log the error and bump the job error count (call before re-raising)."""
        self.log(f"ERROR: {exc}")
        self._handle.update(
            lambda job: setattr(job, "error_count", job.error_count + 1)
        )


# MSK Serverless caps total partitions per cluster. Every connector create/delete
# consumes partitions that are NOT reclaimed (the internal topics survive rollback),
# so repeated Start/iterate cycles can exhaust the quota and wedge the cluster --
# unrecoverable in place. We detect the signature in CloudFormation/MSK event text
# and turn the otherwise-opaque failure into an actionable recovery instruction.
# How often to re-report an unchanged seeder-ENI wait during a delete. The wait has no
# CloudFormation events and routinely runs ~15-20 min, so a change-only line leaves the
# log silent for that whole stretch and the teardown reads as hung. 2 min is frequent
# enough to prove liveness without drowning the stack events (polls are ~30s).
_ENI_REPORT_INTERVAL_SECONDS = 120.0

_PARTITION_QUOTA_GUIDANCE = (
    "Stack operation ended in '{status}' because the MSK Serverless partition "
    "quota is exhausted. Each connector create/delete consumes partitions that "
    "are not reclaimed, so repeated Start/retry cycles can run the cluster out. "
    "This cannot be fixed in place: use Delete CDC infrastructure to tear down the "
    "whole cdc-stack, then deploy again from scratch (CDC resumes from the source "
    "position you set). To avoid recurrence, finalize table selection before "
    "Start so you do not iterate connectors."
)


def _is_partition_quota_message(msg: str) -> bool:
    """True when a stack event signals MSK Serverless partition-quota exhaustion."""
    lowered = (msg or "").lower()
    return "partition" in lowered and (
        "quota" in lowered or "exceeded" in lowered or "limit" in lowered
    )


# One text for both "Debezium has no definition for this table" signatures below (a row
# change, or a replayed CREATE UNIQUE INDEX). It carries SCHEMA_HISTORY_MARK on purpose:
# a schema_only Start that fails with it flags the stack's history as missing.
_UNKNOWN_TABLE_GUIDANCE = (
    "{connector} failed: Debezium met a row change or a CREATE UNIQUE INDEX for a table "
    "that Debezium's schema history for this stack does not hold — the history is "
    "incomplete, or a captured table was dropped or renamed on the source after the "
    "position this connector started reading from. If this stack streamed before (a "
    "restart after Stop CDC), remove the connectors first if the CDC step still shows "
    "them — Stop CDC, or Clean up leftover connector on an incomplete pipeline (a Start or "
    "Retry CDC on connectors that exist keeps their snapshot mode and fails the same way) "
    "— then Start CDC: it rebuilds the history from the source tables as they are now, "
    "after checking the captured tables against the start point. Otherwise, or if that "
    "check names a changed or missing table, give CDC a start point after the change: "
    "Re-run Full Load for all tables with \"Drop & reload\" (Full load + CDC), or choose "
    "Manual, Fetch current position and Use this start point, then reconcile with "
    "\"Drop & reload\" and CHECKSUM. "
    "A reload switches the CDC start point back to Automatic, so choose Manual again "
    "and Use this start point before Start CDC (the fetched point is kept)."
)

# G7: a DDL statement Debezium's MySQL grammar cannot parse (see the table entry below).
_UNPARSEABLE_DDL_NEEDLE = "ddl statement couldn't be parsed"

# The STREAMING variant: the statement was in the binary log. Neither G7 text may carry
# SCHEMA_HISTORY_MARK (that would flip the next resume to recovery) or "Start CDC again"
# (Starting again replays the statement). The sentence after the CHECKSUM step ("A reload
# switches the CDC start point back to Automatic, ...") is
# ui.cdc_start_point_gate.RELOAD_RESETS_MANUAL verbatim -- core may not import ui, so it
# is duplicated here and a test pins the two in sync.
_UNPARSEABLE_DDL_GUIDANCE = (
    "{connector} failed: Debezium could not parse a DDL statement in the source's binary "
    "log and stopped just before it.{statement} Debezium parses every DDL statement on the "
    "source server — for every database and table, not only the captured ones, including "
    "stored routines, triggers, views and accounts — and stops at one its MySQL grammar "
    "does not support rather than skip it, because a skipped statement could leave its "
    "table definitions wrong. The connector resumes from a position before that "
    "statement, so Retry CDC, and Start CDC after Stop CDC, read it and stop again. Stop "
    "CDC, then give CDC a start point after the statement: Re-run Full Load for all "
    "tables with \"Drop & reload\" (Full load + CDC; re-run Step 1 (Evaluation) and Schema "
    "Conversion first if the statement changed a captured table) — its new watermark is "
    "past the statement; or choose Manual, Fetch current position and Use this start "
    "point — changes made between where CDC stopped and that point are then not "
    "replicated, so reload those tables with \"Drop & reload\" (CDC only: correct them on "
    "Aurora DSQL) and validate with CHECKSUM. "
    "A reload switches the CDC start point back to Automatic, so choose Manual again "
    "and Use this start point before Start CDC (the fetched point is kept). "
    "Do not use the \"binlog reader near position\" in the connector log as a start "
    "point: it is where the statement begins. A Start from a new start point reads the "
    "current definition of every table on the server, so if the statement created or "
    "altered a table, that Start can stop on the table's definition the same way, and the "
    "CDC step then says so. A statement that runs again (a schema-migration tool, a "
    "scheduled job) stops CDC each time."
)

# Carried ONLY by the snapshot variant below: history_missing_after_failure keys on it.
DEFINITION_SNAPSHOT_MARK = "could not parse the definition of a table on the source server"

# The SNAPSHOT variant: the statement was a table's current definition (SHOW CREATE TABLE),
# read for every table on the server because store.only.captured.tables.ddl=false. No
# start point helps, so it never offers one.
_UNPARSEABLE_DEFINITION_GUIDANCE = (
    "{connector} failed: Debezium " + DEFINITION_SNAPSHOT_MARK + " while reading the "
    "definitions of every table at Start.{statement} A Start that builds Debezium's schema "
    "from the source — the first Start, a Start from a new start point, and a Start that "
    "rebuilds a missing schema history — reads the definition of EVERY table on the "
    "server, in every database, not only the captured ones, and stops at one its MySQL "
    "grammar does not support. So a different start point, Retry CDC and another Start all "
    "stop the same way. The tool cannot work around this: change that table's definition "
    "on the source to a form the grammar supports, then Stop CDC and Start CDC, which "
    "reads the definitions again. Otherwise CDC cannot run against this source server: "
    "migrate it with Full load only."
)
_UNPARSEABLE_DEFINITION_HEADLINE = "Debezium could not parse a source table's definition"

# G19: GTID positioning on a gtid_mode=ON source (see the table entries below). The sentence
# after the CHECKSUM step is ui.cdc_start_point_gate.RELOAD_RESETS_MANUAL verbatim (pinned).
_ANONYMOUS_GTID_NEEDLE = "cannot replicate anonymous transaction"
_PURGED_GTIDS_NEEDLE = "purged required binary logs"
_PURGED_GTIDS_NEEDLE_57 = "has purged binary logs containing gtids"
_ANONYMOUS_GTID_GUIDANCE = (
    "{connector} failed: the source runs gtid_mode=ON, so Debezium reads the binary log by "
    "GTID, and the binary log after the position this connector started from holds changes "
    "written without a GTID (while gtid_mode was not ON); MySQL cannot send those by GTID. "
    "Retry CDC fails the same way, and so does Stop CDC followed by Start CDC. Stop CDC, "
    "then either set the source's gtid_mode back to ON_PERMISSIVE until cut over — Debezium "
    "then reads by binlog file:position from where it stopped — or give CDC a start point "
    "recorded while gtid_mode is ON: Re-run Full Load for all tables with \"Drop & reload\" "
    "(Full load + CDC), or choose Manual, Fetch current position and Use this start point — "
    "changes made between where CDC stopped and that point are then not replicated, so "
    "reload those tables with \"Drop & reload\" (CDC only: correct them on Aurora DSQL) and "
    "validate with CHECKSUM. "
    "A reload switches the CDC start point back to Automatic, so choose Manual again and "
    "Use this start point before Start CDC (the fetched point is kept)."
)
_PURGED_GTIDS_GUIDANCE = (
    "{connector} failed: the source runs gtid_mode=ON, so Debezium reads the binary log by "
    "GTID, and the source refused the GTID set this connector started from: it has already "
    "purged binary logs holding transactions that set does not contain. That has two "
    "causes. If the binlog retention ran out while CDC was stopped or behind, those changes "
    "can no longer be streamed. If an earlier version of the tool started this pipeline "
    "from a Manual start point (CDC only included), the source may still hold every change "
    "since CDC stopped: only the GTID set that version recorded lacks the transactions the "
    "source had purged at that start. While gtid_mode stays ON, Retry CDC fails the same "
    "way, and so does Stop CDC followed by Start CDC. For a pipeline an earlier version "
    "started from a Manual start point, Stop CDC, set the source's gtid_mode to "
    "ON_PERMISSIVE until cut over (a parameter-group change on RDS and Aurora), then Start "
    "CDC: Debezium resumes from where it stopped by binlog file:position, with no gap (if "
    "the binlog retention also ran out, that Start fails rather than skip changes). "
    "Otherwise, Stop CDC, then give CDC a start point the source still holds: Re-run Full "
    "Load for all tables with \"Drop & reload\" (Full load + CDC; raise the source's binlog "
    "retention first, for example CALL mysql.rds_set_configuration('binlog retention "
    "hours', 168)), or choose Manual, Fetch current position and Use this start point — "
    "changes made between where CDC stopped and that point are then not replicated, so "
    "reload those tables with \"Drop & reload\" (CDC only: correct them on Aurora DSQL) and "
    "validate with CHECKSUM. "
    "A reload switches the CDC start point back to Automatic, so choose Manual again and "
    "Use this start point before Start CDC (the fetched point is kept)."
)


_CONNECTOR_LOG_DIAGNOSES: tuple[tuple[str, str], ...] = (
    (
        "quota exceeded for maximum number of partitions",
        "{connector} failed: the MSK Serverless partition quota is exhausted. Each "
        "connector create/delete consumes partitions that are not reclaimed, so "
        "repeated Start/retry cycles run the cluster out. This cannot be fixed in "
        "place: use Delete CDC infrastructure to tear down the whole cdc-stack, then "
        "deploy again from scratch (CDC resumes from the source position you set). To "
        "avoid recurrence, finalize your table selection before Start so you do not "
        "iterate connectors.",
    ),
    (
        "could not find a 'kafkaclient' entry in the jaas",
        "{connector} failed: the schema-history Kafka client could not authenticate "
        "to MSK (missing IAM JAAS config). This is a cdc-stack template defect, not an "
        "operational issue -- report it; redeploying will not help until the template "
        "is fixed.",
    ),
    (
        "auth_scheme_provider",
        "{connector} failed with an AWS SDK conflict (NoSuchFieldError "
        "AUTH_SCHEME_PROVIDER) in the connector plugin. This is a plugin-packaging "
        "defect (conflicting bundled SDK jars), not an operational issue -- report it; "
        "redeploying the same plugin will not help.",
    ),
    # --- MySQL schema-history replay (Debezium 2.7.4) ---------------------------
    # snapshot.mode=recovery rebuilds the schema history from the source tables AS THEY
    # ARE NOW and then reads OLDER binlog rows with it, so a DDL committed after the start
    # offset fails the task on every start. Above "communications link failure": a tail
    # can also hold transient link errors, and these causes never heal by themselves.
    # Debezium's own text advises "restart connector with schema recovery mode", which is
    # exactly what replays the failure, so the guidance contradicts it explicitly.
    (
        # BinlogStreamingChangeEventSource.validateChangeEventWithTable: "... different
        # column size with internal schema size N, but <op> size M ..." (log) and "Error
        # processing row in <t>, internal schema size N, but row size M ..." (exception).
        "internal schema size",
        "{connector} failed: Debezium read a binlog row whose column count does not match "
        "the definition it rebuilt from the source table as it is now. A captured table "
        "had a column added or dropped (ALTER TABLE) after the position this connector "
        "started reading from — before the first Start from that start point, before a "
        "Start after Delete CDC infrastructure, or shortly before an automatic connector "
        "restart. Starting again, Retry CDC and the \"schema recovery mode\" the log "
        "suggests all replay the same rows and fail again. Stop CDC, then give CDC a "
        "start point after the change: for Full load + CDC, re-run Step 1 (Evaluation), "
        "update the changed table in Schema Conversion and Re-run Full Load for all "
        "tables with \"Drop & reload\" (its new watermark is past the change); or choose "
        "Manual, Fetch current position and Use this start point — changes made before "
        "that point are then not replicated, so reload those tables with \"Drop & "
        "reload\" (CDC only: correct them on Aurora DSQL) and validate with CHECKSUM. "
        "A reload switches the CDC start point back to Automatic, so choose Manual again "
        "and Use this start point before Start CDC (the fetched point is kept).",
    ),
    (
        # AlterTableParserListener: "Trying to change column <c> in <t> table, which does
        # not exist. Query: ..." -- replaying a RENAME/CHANGE COLUMN against a definition
        # rebuilt with the new name. Any table on the server, because
        # store.only.captured.tables.ddl=false makes Debezium track every table.
        "trying to change column",
        "{connector} failed: Debezium could not replay a column rename (ALTER TABLE … "
        "RENAME COLUMN or CHANGE) from the binary log, because the definition it rebuilt "
        "from the source table as it is now already has the new name. A column was "
        "renamed after the position this connector started reading from, on ANY table of "
        "the source server, not only a captured one: Debezium tracks every table's "
        "definition (the connector's CloudWatch log names the table). Starting again, "
        "Retry CDC and recovery mode repeat it. Stop CDC, then give CDC a start point "
        "after the rename: Re-run Full Load for all tables with \"Drop & reload\" (Full "
        "load + CDC; re-run Step 1 and Schema Conversion first if a captured table "
        "changed); or "
        "choose Manual, Fetch current position and Use this start point, then reload or "
        "correct the changes made before it and validate with CHECKSUM. "
        "A reload switches the CDC start point back to Automatic, so choose Manual again "
        "and Use this start point before Start CDC (the fetched point is kept).",
    ),
    (
        # BaseSourceTask: "The db history topic is missing. You may attempt to recover it
        # by reconfiguring the connector to recovery." (schema_only with an offset and no
        # history). With a SPACE: recovery's own INFO line "The db-history topic is missing
        # but we are in recovery snapshot mode" must not match.
        "db history topic is missing",
        "{connector} failed: Debezium's schema history for this stack is missing, so the "
        "connector could not continue from the stream position it recorded. If the CDC "
        "step still shows the connectors, remove them first with Stop CDC (or Clean up "
        "leftover connector on an incomplete pipeline): a Start or Retry CDC on connectors "
        "that exist keeps their snapshot mode and fails the same way. Then Start CDC: it "
        "rebuilds that history from the source tables as they are now "
        "(snapshot.mode=recovery) and continues from the same position, after checking "
        "that the captured tables still match their definitions at the start point — if "
        "one changed, it refuses and names the way forward.",
    ),
    (
        # BinlogStreamingChangeEventSource: "Encountered change event ... for table <t>
        # whose schema isn't known to this connector. One possible cause is an incomplete
        # database schema history topic." (inconsistent.schema.handling.mode=fail).
        "isn't known to this connector",
        _UNKNOWN_TABLE_GUIDANCE,
    ),
    (
        # CreateUniqueIndexParserListener: "Trying to create index on non existing table
        # <t>.Query: <ddl>" -- a CREATE UNIQUE INDEX replayed for a captured table the
        # schema history does not hold: the same cause as the entry above.
        "trying to create index on non existing table",
        _UNKNOWN_TABLE_GUIDANCE,
    ),
    # --- MySQL: a DDL statement Debezium's grammar cannot parse (Debezium 2.7.4) -------
    # The literal is ParsingErrorListener.syntaxError's (verified with javap):
    #   "DDL statement couldn't be parsed. Please open a Jira issue with the statement
    #   '<ddl>'\n<ANTLR detail>"
    # and MultipleParsingExceptions ("Multiple parsing errors") repeats each inner
    # ParsingException in its toString, so the phrase reaches the log for one error or
    # many. BinlogDatabaseSchema.parseDdl parses EVERY DDL on the server -- any database,
    # any table, routines, triggers, views and account statements -- BEFORE the
    # captured-table filter, for streaming and snapshot DDL alike, and rethrows because
    # schema.history.internal.skip.unparseable.ddl is false (the default; the template
    # does not set it). MySqlErrorHandler retries only IOException/SQLException, so the
    # task stops and its offset never passes the statement.
    #
    # The G5 tree-walk entries above ("trying to change column", "trying to create index
    # ...") cannot shadow it (first match wins): AntlrDdlParser.parse walks the tree only
    # when the statement had no syntax error, so one statement raises one kind or the
    # other. It sits ABOVE
    # the link, auth, PostgreSQL and connection-test needles because the tail quotes the
    # statement text, which can contain any of their words.
    #
    # No skip option is offered on purpose: the same setting also swallows the tree-walk
    # ParsingExceptions above, turning those loud stops into a silently stale in-memory
    # schema (shifted or NULL values with no error). The tuple carries the STREAMING text;
    # diagnose_connector_log swaps in _UNPARSEABLE_DEFINITION_GUIDANCE when the failure
    # came from the schema snapshot instead (see _failed_in_schema_snapshot) and fills
    # {statement} with a masked, bounded head of the statement (_ddl_statement_head).
    (
        _UNPARSEABLE_DDL_NEEDLE,
        _UNPARSEABLE_DDL_GUIDANCE,
    ),
    # --- MySQL gtid_mode=ON: GTID positioning (G19) ------------------------------------
    # Debezium 2.7.4 asks MySQL for every transaction NOT in its offset's GTID set when the
    # source's gtid_mode is ON (MySqlConnection.isGtidModeEnabled), and the server refuses
    # in two ways the start-time GTID check cannot always prevent: the binary log after
    # that point holds transactions written without a GTID (a gtid_mode change while CDC
    # was stopped or streaming), or the server purged binary logs the set does not cover
    # (retention ran out, or a set recorded by an earlier version lacks the purged range).
    # Server text, as the binlog client's ServerException repeats it: "Cannot replicate
    # anonymous transaction when AUTO_POSITION = 1, at file ..., position ..."; MySQL 8.0
    # "Cannot replicate because the source purged required binary logs. ..." and MySQL
    # 5.7 "... has purged binary logs containing GTIDs that the replica requires" (older
    # releases name the two servers with other words; both needles avoid them). Below the
    # DDL entry (a quoted statement could contain these words) and above the link, auth
    # and connection-test needles. Neither text carries SCHEMA_HISTORY_MARK or
    # DEFINITION_SNAPSHOT_MARK (a resume must not flip to recovery) nor "Start CDC again"
    # (Starting again fails the same way). Debezium's own WARN "Last recorded offset is no
    # longer available on the server." matches neither.
    (
        _ANONYMOUS_GTID_NEEDLE,
        _ANONYMOUS_GTID_GUIDANCE,
    ),
    (
        _PURGED_GTIDS_NEEDLE,
        _PURGED_GTIDS_GUIDANCE,
    ),
    (
        _PURGED_GTIDS_NEEDLE_57,
        _PURGED_GTIDS_GUIDANCE,
    ),
    (
        "communications link failure",
        "{connector} failed: the Debezium worker could not reach the source MySQL "
        "(TCP connection timed out). Check that the source database is reachable from "
        "the connector's network and that its security group allows inbound on the "
        "MySQL port from the connector. Then Stop CDC if the CDC step still shows the "
        "connectors (or Clean up leftover connector on an incomplete pipeline), and "
        "Start CDC.",
    ),
    (
        "access denied for user",
        "{connector} failed: the source MySQL rejected the connector's credentials "
        "(access denied). Verify the CDC user's username/password and that it has the "
        "required REPLICATION privileges. Then Stop CDC if the CDC step still shows the "
        "connectors (or Clean up leftover connector on an incomplete pipeline), and "
        "Start CDC.",
    ),
    # --- PostgreSQL source ----------------------------------------------------
    # Every needle above is MySQL/MSK-flavoured, so a PostgreSQL source connector that
    # died got the content-free "<connector> entered FAILED state." and the operator had
    # to open the MSK Connect log group themselves.
    (
        "publication autocreation is disabled",
        "{connector} failed: the PostgreSQL CDC publication does not exist on the "
        "source, and the connector is configured not to create one. Only a "
        "'Full load + CDC' run creates it. Delete the connectors (Stop CDC), then Start "
        "CDC again and choose \"Re-snapshot every table instead\" when the dialog offers "
        "it -- that lets the connector create its own publication and snapshot the "
        "tables, which has no replication gap. Do NOT create a replication slot by hand "
        "to resume from: a slot cannot start at a past position, so it would silently "
        "skip every change made since the Full Load.",
    ),
    (
        # PostgresReplicationConnection: "Unable to %s filtered publication %s for %s" and
        # "No table filters found for filtered publication %s", at task start, when
        # publication.autocreate.mode=filtered (a CDC-only re-snapshot Start) must create or
        # update the publication. ABOVE "replication slot": every PostgreSQL start logs
        # "Obtained valid replication slot ..." before it gets here.
        "filtered publication",
        "{connector} failed: it could not create or update its PostgreSQL publication for "
        "the selected tables. Usually a selected table no longer exists on the source "
        "under that schema and name (dropped, renamed or moved to another schema since it "
        "was selected), or the CDC user does not own it — adding a table to a publication "
        "requires owning it (membership in the owning role is enough). The connector's "
        "CloudWatch log names the tables. Every Start fails the same way until the "
        "selection and the source agree. Restore the table or its ownership on the "
        "source, then Stop CDC (or Clean up leftover connector on an incomplete pipeline) "
        "and Start CDC. If it was dropped or renamed on purpose, the table set cannot "
        "change while the CDC infrastructure exists (the table picker is locked): Delete "
        "CDC infrastructure on the CDC step (or, once a Full Load has run or CDC has "
        "streamed, use Start over), then re-run Step 1 (Evaluation), take the table out "
        "of the selection, deploy the infrastructure again and Start CDC.",
    ),
    (
        "replication slot",
        "{connector} failed while setting up its PostgreSQL replication slot. Check that "
        "the source's wal_level is 'logical', that the CDC user has REPLICATION (or is "
        "rds_replication on RDS/Aurora), and that max_replication_slots is not "
        "exhausted; the connector's CloudWatch log names the specific cause.",
    ),
    # LAST RESORT -- must stay BELOW every specific needle above, because the failure modes
    # OVERLAP in the log tail and the first match wins.
    #
    # What this string actually means, from the shipped plugins rather than from its
    # wording: on PostgreSQL `Failed testing connection for {} with user '{}'` is logged by
    # the catch around `checkWalLevel` in PostgresConnectorTask -- the same class that holds
    # "wal_level property must be 'logical'" -- so it fires when the connector CONNECTED
    # and then failed a check, not only when it could not reach the host. On MySQL the same
    # literal comes from BinlogConnector's catch of ANY SQLException around connect() +
    # SELECT version(), which covers a wrong database name, TZ/SSL and auth-plugin errors.
    # So this entry must NOT claim "could not connect": it names the three real causes in
    # likelihood order and defers to the connector's CloudWatch log. (Keyed on this literal
    # and not on pgjdbc's own "The connection attempt failed." because that one could also
    # come from the SINK reaching DSQL; verified this literal has zero hits in
    # dsql-sink-plugin.zip.)
    (
        "failed testing connection",
        "{connector} failed its connection TEST to the source database -- it got far "
        "enough to try, so this is usually configuration rather than the network. On "
        "PostgreSQL the most common cause is wal_level not being 'logical' (on RDS/Aurora "
        "set rds.logical_replication=1 AND REBOOT -- the parameter alone does nothing); on "
        "MySQL it is usually the CDC user's credentials, privileges or database name. If "
        "those are right, check the path: the connectors run in your VPC on the cdc-stack's "
        "own security group, and the stack can only open its OWN egress -- so the SOURCE "
        "database's security group must allow INBOUND on the database port from the "
        "connector subnets. The connector's CloudWatch log names the specific cause.",
    ),
)

# The cause in a few words, per needle, for places a runbook does not fit: a
# Pipeline-health row ("Stopped — <headline>."), a notice header ("... stopped:
# <headline>"), an activity-log action, the AI payload. Always composed mid-sentence, so
# it is a lower-case fragment unless its first word is a proper noun or acronym
# (Debezium, MSK, AWS, PostgreSQL). At most 80 characters, no braces, no trailing period
# (a test pins all four).
_CONNECTOR_LOG_HEADLINES: dict[str, str] = {
    "quota exceeded for maximum number of partitions": (
        "MSK Serverless partition quota exhausted"
    ),
    "could not find a 'kafkaclient' entry in the jaas": (
        "schema-history client could not authenticate to MSK"
    ),
    "auth_scheme_provider": "AWS SDK conflict in the connector plugin",
    "internal schema size": "a captured table's column count changed after the start point",
    "trying to change column": "a column rename after the start point cannot be replayed",
    "db history topic is missing": "Debezium's schema history is missing",
    "isn't known to this connector": (
        "a change arrived for a table Debezium's schema history does not hold"
    ),
    "trying to create index on non existing table": (
        "a change arrived for a table Debezium's schema history does not hold"
    ),
    _UNPARSEABLE_DDL_NEEDLE: "Debezium could not parse a DDL statement in the binary log",
    _ANONYMOUS_GTID_NEEDLE: "the source wrote changes without a GTID after CDC's start point",
    _PURGED_GTIDS_NEEDLE: "the source has purged binary logs CDC's GTID set does not cover",
    _PURGED_GTIDS_NEEDLE_57: "the source has purged binary logs CDC's GTID set does not cover",
    "communications link failure": "the source MySQL could not be reached",
    "access denied for user": "the source MySQL rejected the CDC user's credentials",
    "publication autocreation is disabled": "the PostgreSQL CDC publication does not exist",
    "filtered publication": (
        "the PostgreSQL CDC publication could not be created for the selected tables"
    ),
    "replication slot": "PostgreSQL replication slot setup failed",
    "failed testing connection": "the connection test to the source database failed",
}

# How an UNMATCHED worker log is surfaced. Before this, a tail that matched no needle was
# fetched and then DISCARDED, leaving the bare "<connector> entered FAILED state." -- the
# single worst outcome in the CDC flow, because the operator had waited out a billable
# connector create (up to ``connector_timeout_seconds``) and still had to go read CloudWatch
# to learn anything at all. Repeating the log's last error line (masked) is strictly more
# general than any needle: it covers every failure mode nobody has written a needle for yet.
_UNMATCHED_LOG_HINT = (
    "No known failure signature matched its worker log, so its last error line is repeated "
    "(quoted values hidden): {excerpt} — the MSK Connect log group has the full stack "
    "trace."
)

# Defensive only: the connector's password is a separate `database.password` config (Kafka
# Connect masks Password-typed values), so it is not expected in a log line -- but this
# excerpt is about to reach the UI and the downloadable activity log, and Property 7 says
# credentials never land there. Cheap to scrub, so scrub.
_SECRET_IN_LOG_RE = re.compile(
    r"(?i)\b(password|passwd|pwd|secret|token)\b\s*[=:]\s*\S+"
)
# The same pairs with a quoted value, scrubbed in a log excerpt BEFORE its literals are
# masked: a quote mis-paired by the prose around it can otherwise hide the keyword inside a
# masked region, where the scrub above never sees it.
_SECRET_QUOTED_IN_LOG_RE = re.compile(
    r"""(?i)\b(password|passwd|pwd|secret|token)\b\s*[=:]\s*(?:'[^']*'|"[^"]*"|\S+)"""
)
_LOG_EXCERPT_MAX = 400

# Which worker-log line an excerpt repeats. A task that dies logs Kafka Connect's ERROR
# "Task threw an uncaught and unrecoverable exception", the exception and its frames
# ("\tat ...", "Caused by: ...", "... 9 more"), and then INFO shutdown lines (Debezium's
# "Stopping down connector", the producer close, "Kafka Connect stopped" once the connector
# is deleted) -- so the LAST line is almost never the error. All after any "[Worker-...]" /
# "[timestamp]" prefixes.
_STACK_FRAME_RE = re.compile(r"^\s*(?:\[[^\]]*\]\s*)*(?:at\s+\S|\.\.\.\s*\d+\s+more\b)")
_CAUSED_BY_RE = re.compile(r"^\s*(?:\[[^\]]*\]\s*)*Caused by:")
_EXCEPTION_LINE_RE = re.compile(r"\w(?:Exception|Error):")
_ERROR_LEVEL_RE = re.compile(r"\b(?:ERROR|FATAL)\b")


def _excerpt_line(log_tail: str) -> Optional[str]:
    """The worker-log line that best names the error, or ``None`` for an empty tail. Pure.

    Within the latest task run without INFO/DEBUG/TRACE lines (:func:`_episode_lines`) and
    without stack frames: the LAST ``Caused by:`` line (the root cause of the latest
    trace), else the last exception line (``...Exception:`` / ``...Error:``), else the last
    ERROR/FATAL line, else the last line left; the last non-empty line of the whole tail
    only when nothing is left.
    """
    lines = [ln.strip() for ln in (log_tail or "").splitlines() if ln.strip()]
    if not lines:
        return None
    telling = [
        ln.strip()
        for ln in _episode_lines(log_tail).splitlines()
        if ln.strip() and not _STACK_FRAME_RE.match(ln)
    ]
    for pattern in (_CAUSED_BY_RE, _EXCEPTION_LINE_RE, _ERROR_LEVEL_RE):
        pick = next((ln for ln in reversed(telling) if pattern.search(ln)), None)
        if pick is not None:
            return pick
    return telling[-1] if telling else lines[-1]


def _connector_log_excerpt(log_tail: str) -> Optional[str]:
    """The worker-log line that names the error, masked, scrubbed and length-bounded. Pure.

    Bounded on purpose: a Java stack trace is unbounded and this string flows into a notice
    and the activity log, so it takes ONE line, picked by content (:func:`_excerpt_line`):
    Kafka Connect logs the task's ERROR with its trace and then INFO shutdown lines, so the
    last line is rarely the cause. Credentials are scrubbed, and every quoted literal is
    masked (:func:`_mask_sql_literals`, reading an apostrophe inside a word as prose): a
    server error can quote a row value ("Duplicate entry '<value>' for key ..."), and
    Property 7 keeps row values out of the UI and the activity log.
    """
    line = _excerpt_line(log_tail)
    return _sanitized_log_line(line) if line is not None else None


def _sanitized_log_line(line: str) -> str:
    """One log or stack-event line as the UI may repeat it: credentials scrubbed, quoted
    values masked (prose apostrophes kept), length-bounded. Pure."""
    pre = _SECRET_QUOTED_IN_LOG_RE.sub(r"\1=***", line.strip())
    scrubbed = _SECRET_IN_LOG_RE.sub(r"\1=***", _mask_sql_literals(pre, prose=True))
    if len(scrubbed) > _LOG_EXCERPT_MAX:
        scrubbed = scrubbed[:_LOG_EXCERPT_MAX].rstrip() + " …"
    return scrubbed


def _connector_timeout_message(
    deployer, stack_name: str, connector: str,
    log_tail: Optional[ConnectorLogReader] = None,
) -> str:
    """Timeout text, with the worker log's diagnosis when it explains the stall.

    Diagnose on TIMEOUT too, not only on connectorState=FAILED. MSK Connect exposes no
    task-level state and no last-task exception (the kafkaconnect API has no task
    operation; stateDescription is only on DescribeConnector), so a connector whose only
    task died at startup can sit in CREATING until the budget expires with the cause
    visible ONLY in CloudWatch -- which is exactly the trip the operator should not have
    to make. Falls back to the bare timeout wording when nothing matches.
    """
    timed_out = f"{connector} did not reach RUNNING in time."
    message = _connector_failure_message(deployer, stack_name, connector, log_tail=log_tail)
    bare = f"{connector} entered FAILED state."
    if message == bare:
        return timed_out
    if message.startswith(bare):
        # An unmatched-log excerpt. Keep it -- this is the path the excerpt matters MOST on
        # (a connector whose only task died at startup sits in CREATING until the budget
        # expires, so the state says nothing and the log is the only evidence) -- but drop
        # the "entered FAILED state" framing, because it timed out rather than reporting
        # FAILED. Keyed on the exact prefix, not on `endswith`, which the excerpt broke.
        return f"{timed_out} {message[len(bare):].lstrip()}"
    return f"{timed_out} {message}"


# The deploy-log line run_cdc_start writes for the MySQL source connector's snapshot.mode.
_SNAPSHOT_MODE_LOG = {
    "recovery": (
        "Source connector snapshot.mode=recovery (Debezium rebuilds its schema history "
        "from the source tables as they are now, then reads from the start offset)."
    ),
    "schema_only": (
        "Source connector snapshot.mode=schema_only (no schema rebuild: Debezium replays "
        "its recorded schema history, or reads the current schema on a new connector)."
    ),
}

# The phrase both schema-history diagnoses above carry, so a Start can tell from the error
# it raised that a schema_only resume found the stack's history missing or incomplete.
SCHEMA_HISTORY_MARK = "Debezium's schema history for this stack"


def is_schema_history_failure(message: str) -> bool:
    """True when a Start CDC error is the missing/incomplete schema-history diagnosis. Pure."""
    return SCHEMA_HISTORY_MARK in str(message or "")


def history_missing_after_failure(snapshot_mode: Optional[str], message: str) -> bool:
    """Should a failure with this guidance flag the stack's schema history as missing? Pure.

    ONE rule for the Start CDC error and the Pipeline-health diagnosis, keyed on the
    ``snapshot.mode`` the connectors ran with (MySQL only; ``None`` for PostgreSQL):

    * ``schema_only`` -- the missing/incomplete-history diagnoses (G5).
    * ``recovery`` -- the unparseable-table-definition diagnosis: that snapshot failed while
      reading the table definitions, before any schema event reached the history, while
      the seeded offset is already the start point -- so the next Start of the same seed
      is a resume and would otherwise go ``schema_only`` on an empty or stale history.

    Setting the flag only ever moves the next Start to recovery plus the start-point
    check, which is the safe side.
    """
    text = str(message or "")
    if snapshot_mode == "schema_only":
        return is_schema_history_failure(text)
    if snapshot_mode == "recovery":
        return DEFINITION_SNAPSHOT_MARK in text
    return False


@dataclass(frozen=True)
class ConnectorLogDiagnosis:
    """A known connector failure: a short ``headline`` and the full ``guidance``.

    The headline is the cause in a few words (a Pipeline-health row, an activity-log
    action, the AI payload); the guidance is the runbook. Both are value-free: the only
    source text either can carry is a masked, bounded head of a DDL statement.
    """

    headline: str
    guidance: str


# Where a task RUN begins, per debezium-core 2.7.4's BaseSourceTask (all at INFO, so they
# must be found before quiet lines are dropped): "Starting {} with configuration:" is
# logged only by start(Map), Kafka Connect's first start; "Attempting to restart task."
# and "Successfully restarted task" by Debezium's own restart after an error it retries
# (startIfNeededAndPossible), which never logs the first one. ErrorHandler logs
# "Producer failure" at ERROR even for an error it then retries, so without the restart
# lines an error the task recovered from would outrank the one that finally stopped it.
_TASK_RUN_BOUNDARY_RE = re.compile(
    r"(?i)\bstarting \w+ with configuration:"
    r"|\battempting to restart task\b"
    r"|\bsuccessfully restarted task\b"
)
# A line logged at INFO/DEBUG/TRACE, after any "[Worker-...]" / "[timestamp]" prefixes.
_QUIET_LEVEL_RE = re.compile(r"^\s*(?:\[[^\]]*\]\s*)*(?:INFO|DEBUG|TRACE)\b")


def _episode_lines(log_tail: str) -> str:
    """The part of a worker-log tail that belongs to the latest task run, minus quiet lines.

    Keeps only the lines after the LAST task-run boundary (``_TASK_RUN_BOUNDARY_RE``: Kafka
    Connect's first start, or Debezium's own restart after a retried error), because an
    earlier run's or attempt's error must not label this one; then drops INFO/DEBUG/TRACE
    lines (a PostgreSQL start logs "Obtained valid replication slot ..." at INFO, which
    the broad slot needle would otherwise match on any later failure). Both steps only
    ever fail toward KEEPING lines when the log format differs. Pure.
    """
    lines = (log_tail or "").splitlines()
    last_start = None
    for index, line in enumerate(lines):
        if _TASK_RUN_BOUNDARY_RE.search(line):
            last_start = index
    if last_start is not None:
        lines = lines[last_start + 1:]
    return "\n".join(line for line in lines if not _QUIET_LEVEL_RE.match(line))


# In a log line (``prose``), the word right before an apostrophe that makes it a SQL literal
# anyway: a lone N / X / B (``N'v'``, ``x'ab'``, ``b'1'``) or a ``_charset`` introducer.
_SQL_LITERAL_PREFIX_RE = re.compile(r"(?:^|[^\w])(?:[nNxXbB]|_[A-Za-z0-9]+)$")


def _prose_apostrophe(text: str, k: int) -> bool:
    """Is ``text[k]`` (a ``'``) prose -- a contraction, a possessive, a name like O'Brien --
    rather than a quote? A letter on both sides, and no SQL literal prefix before it."""
    return (
        0 < k < len(text) - 1
        and text[k - 1].isalpha()
        and text[k + 1].isalpha()
        and not _SQL_LITERAL_PREFIX_RE.search(text, max(0, k - 64), k)
    )


def _mask_sql_literals(text: str, *, prose: bool = False) -> str:
    """Replace every quoted SQL literal with ``'?'`` / ``"?"``; keep identifiers. Pure.

    A character scanner, not a regex: backtick identifiers are copied verbatim (a doubled
    backtick escapes one; an unterminated identifier keeps the rest), a single- or
    double-quoted literal with backslash or doubled-quote escapes becomes ``q?q``, and an
    UNTERMINATED literal becomes ``q?`` and ends the scan (the rest of the text is
    dropped, since it is inside the literal). A backslash right before the literal's own
    quote is ambiguous -- an escaped quote, or, under sql_mode NO_BACKSLASH_ESCAPES, the
    end of a complete value such as ``'C:\\'`` -- and guessing wrong would show the NEXT
    literal in clear, so it is treated like an unterminated literal (fail closed). Other
    backslash escapes read the same in both modes. Numbers and keywords stay.

    ``prose`` (a log line, not a statement): an apostrophe with a letter on both sides is
    text -- "couldn't", "the user's", "O'Brien" -- so it neither opens a literal (which
    would pair with the NEXT quote and show the value after it in clear) nor closes one;
    unless the word before it is a SQL literal prefix (``N'..'``, ``x'..'``, ``_binary'..'``),
    which keeps it masked. A DDL statement head keeps the strict scan.
    """
    out: list[str] = []
    i, n = 0, len(text or "")
    while i < n:
        ch = text[i]
        if ch == "`":
            j = i + 1
            while j < n:
                if text[j] == "`":
                    if j + 1 < n and text[j + 1] == "`":
                        j += 2
                        continue
                    break
                j += 1
            if j >= n:
                out.append(text[i:])
                break
            out.append(text[i:j + 1])
            i = j + 1
            continue
        if ch == "'" and prose and _prose_apostrophe(text, i):
            out.append(ch)
            i += 1
            continue
        if ch in ("'", '"'):
            j = i + 1
            closed = False
            while j < n:
                c = text[j]
                if c == "\\":
                    if j + 1 < n and text[j + 1] == ch:
                        break  # ambiguous (see above): fail closed, like unterminated
                    j += 2
                    continue
                if c == ch:
                    if j + 1 < n and text[j + 1] == ch:
                        j += 2
                        continue
                    if ch == "'" and prose and _prose_apostrophe(text, j):
                        j += 1  # an apostrophe inside the value ('O'Brien-5'), not its end
                        continue
                    closed = True
                    break
                j += 1
            if not closed:
                out.append(ch + "?")
                break
            out.append(ch + "?" + ch)
            i = j + 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


# ParsingErrorListener's exact wording before the statement (case as in the bytecode).
_DDL_STATEMENT_MARK = "with the statement '"
_DDL_HEAD_MAX = 160


def _ddl_statement_head(text: str) -> Optional[str]:
    """The masked, bounded beginning of the statement an unparseable-DDL error quotes. Pure.

    Takes the LAST "...with the statement '" in ``text``, keeps only the rest of THAT line
    (the ANTLR detail on the next line can quote a token, and a multi-line statement is
    cut to its first line), drops the one closing quote Debezium adds, masks every quoted
    literal (DEFAULT/COMMENT values, routine bodies, an account's auth string), collapses
    whitespace, scrubs ``password=...``-style pairs and caps the result at
    ``_DDL_HEAD_MAX`` characters. ``None`` when there is no such marker or nothing is left.
    Only this derived string is kept; the caller drops the raw tail.
    """
    at = (text or "").rfind(_DDL_STATEMENT_MARK)
    if at < 0:
        return None
    rest = text[at + len(_DDL_STATEMENT_MARK):]
    line = (rest.splitlines() or [""])[0].rstrip()
    if line.endswith("'"):
        line = line[:-1]
    head = " ".join(_mask_sql_literals(line).split())
    head = _SECRET_IN_LOG_RE.sub(r"\1=***", head)
    if not head:
        return None
    if len(head) > _DDL_HEAD_MAX:
        head = head[:_DDL_HEAD_MAX].rstrip() + " …"
    return head


def _failed_in_schema_snapshot(lowered: str) -> bool:
    """True when an unparseable-DDL tail failed in the schema snapshot, not while streaming.

    The more RECENT marker wins: RelationalSnapshotChangeEventSource logs "Error during
    snapshot"; the binlog reader logs "Error during binlog processing" and "Error
    processing binlog event". Neither present -> streaming (the default). Pure.
    """
    snapshot = lowered.rfind("error during snapshot")
    streaming = max(
        lowered.rfind("error during binlog processing"),
        lowered.rfind("error processing binlog event"),
    )
    return snapshot > streaming


def diagnose_connector_log(
    connector: str, log_tail: str, *, episode_only: bool = False
) -> Optional[ConnectorLogDiagnosis]:
    """Map a connector's worker-log tail to a :class:`ConnectorLogDiagnosis`, or ``None``.

    First match wins over ``_CONNECTOR_LOG_DIAGNOSES`` (lower-cased substring). With
    ``episode_only`` (the Pipeline-health read of a connector that stopped after it
    started) the tail is first reduced to the latest task run without INFO/DEBUG/TRACE
    lines (:func:`_episode_lines`); the Start CDC path reads it whole, unchanged. Pure.
    """
    text = _episode_lines(log_tail) if episode_only else (log_tail or "")
    lowered = text.lower()
    for needle, guidance in _CONNECTOR_LOG_DIAGNOSES:
        if needle not in lowered:
            continue
        headline = _CONNECTOR_LOG_HEADLINES[needle]
        statement = ""
        if needle == _UNPARSEABLE_DDL_NEEDLE:
            head = _ddl_statement_head(text)
            statement = (
                f" It begins \"{head}\" (quoted values are hidden)."
                if head
                else " The connector's CloudWatch log shows the statement."
            )
            if _failed_in_schema_snapshot(lowered):
                guidance = _UNPARSEABLE_DEFINITION_GUIDANCE
                headline = _UNPARSEABLE_DEFINITION_HEADLINE
        return ConnectorLogDiagnosis(
            headline=headline,
            guidance=guidance.format(connector=connector, statement=statement),
        )
    return None


def _diagnose_connector_log(connector: str, log_tail: str) -> Optional[str]:
    """Map a connector's worker-log tail to actionable guidance, or ``None``.

    Returns the guidance for the first known failure signature found in
    ``log_tail`` (the recent worker-log messages). ``None`` when nothing matches,
    so the caller can fall back to the raw connector state. Pure (no AWS). The Start
    CDC path: the whole tail, unscoped (see :func:`diagnose_connector_log`).
    """
    diagnosis = diagnose_connector_log(connector, log_tail)
    return diagnosis.guidance if diagnosis is not None else None


# A worker-log reader: ``(stack_name, connector_name, *, since=None) -> tail`` (``""`` when
# unreadable). With ``since``, only events written from then on (see
# ``cdc_stack_deployer.read_connector_log_tail``).
ConnectorLogReader = Callable[..., str]


def _read_connector_log(
    deployer,
    log_tail: Optional[ConnectorLogReader],
    stack_name: str,
    connector: str,
    *,
    since: Optional[datetime] = None,
) -> str:
    """A connector's worker-log tail, read with ``log_tail`` first, then the deployer's.

    ``log_tail`` is the app's own reader (MySQL Start CDC passes the MSK Connect
    controller's): on Fargate and EC2 the deployer acts as CdcDeployRole, which cannot read
    the connector log group, while the app's own identity can. A reader that raises, or
    returns nothing, falls back to the deployer's (``""`` when neither reads anything).
    ``since`` (this Start's submit time) keeps both reads to events written from then on:
    connector names repeat across Starts and the log group keeps 30 days, so the newest
    stream can be an EARLIER Start's. Without ``log_tail`` this is exactly the deployer's
    read.
    """
    extra = {"since": since} if since is not None else {}
    if log_tail is None:
        return deployer.connector_log_tail(stack_name, connector, **extra)
    try:
        tail = log_tail(stack_name, connector, **extra)
    except Exception:  # noqa: BLE001 - advisory read; fall back to the deployer's
        tail = ""
    if isinstance(tail, str) and tail:
        return tail
    try:
        tail = deployer.connector_log_tail(stack_name, connector, **extra)
    except Exception:  # noqa: BLE001 - advisory read
        return ""
    return tail if isinstance(tail, str) else ""


# CloudFormation logical IDs of the connector resources -> their index in
# ``connector_names_for_log`` (``[source, sink]``).
_CONNECTOR_RESOURCE_INDEX = {
    "DebeziumSourceConnector": 0,
    "PostgresSourceConnector": 0,
    "DsqlSinkConnector": 1,
}


def _failed_resource_in_event(message: str) -> Optional[tuple[str, str]]:
    """``(LogicalId, reason)`` of a resource a stack-event line reports FAILED, or ``None``.

    ``message`` is a :meth:`CdcStackDeployer.poll_events` line, ``"<LogicalId> <STATUS>[ —
    <reason>]"``. A status ending in ``_FAILED`` counts, except a resource CloudFormation
    cancelled because ANOTHER resource failed ("Resource creation cancelled"): that one did
    not fail on its own. Pure.
    """
    parts = (message or "").split(None, 2)
    if len(parts) < 2 or not parts[1].endswith("_FAILED"):
        return None
    if "cancel" in message.lower():
        return None
    reason = parts[2] if len(parts) > 2 else ""
    if reason.startswith("—"):
        reason = reason[1:]
    return parts[0], reason.strip()


def _failed_connector_in_event(
    message: str, connector_names: Sequence[str]
) -> Optional[str]:
    """The connector a stack-event line reports FAILED (:func:`_failed_resource_in_event`),
    or ``None``. Pure."""
    failed = _failed_resource_in_event(message)
    if failed is None:
        return None
    index = _CONNECTOR_RESOURCE_INDEX.get(failed[0])
    if index is None or index >= len(connector_names):
        return None
    return connector_names[index]


def _rollback_failure_message(
    deployer,
    stack_name: str,
    status: str,
    *,
    connector_names: Sequence[str],
    failed: Sequence[str],
    log_tail: ConnectorLogReader,
    failed_resource: Optional[tuple[str, str]] = None,
    since: Optional[datetime] = None,
) -> str:
    """The Start CDC error for a connector stack operation that ended in ``status``.

    Every worker-log read is limited to this Start (``since``, :func:`_read_connector_log`):
    a connector name repeats across Starts and the log group keeps 30 days, so an earlier
    Start's failure must not be reported as this one's. When the stack events reported
    connectors FAILED, only their logs are read -- a connector cancelled because another
    resource failed is not the cause: the first known failure signature wins; otherwise the
    last error line of a failed connector is repeated (masked); otherwise the message says
    no log line was found for it. When only another resource failed (``failed_resource``:
    the offset seeder / topic prep, say), it is named with its CloudFormation reason
    (masked), and no connector log is read. With neither, the connectors' logs since the
    Start are scanned for a known signature before the bare status.
    """
    if not failed and failed_resource is not None:
        resource, reason = failed_resource
        shown = _sanitized_log_line(reason).rstrip(".") if reason else ""
        return (
            f"Stack operation ended in '{status}': {resource} failed"
            + (f" — {shown}" if shown else "")
            + ". No connector reported a failure, so no connector worker log is shown."
        )
    names = [n for n in failed if n in connector_names] if failed else list(connector_names)
    tails: dict[str, str] = {}
    for name in names:
        tails[name] = _read_connector_log(deployer, log_tail, stack_name, name, since=since)
        guidance = _diagnose_connector_log(name, tails[name])
        if guidance:
            return guidance
    for name in failed:
        excerpt = _connector_log_excerpt(tails.get(name, ""))
        if excerpt:
            return (
                f"Stack operation ended in '{status}': {name} failed. "
                + _UNMATCHED_LOG_HINT.format(excerpt=excerpt)
            )
    if failed:
        return (
            f"Stack operation ended in '{status}'. No worker-log line was found for "
            f"{' and '.join(failed)} in the CloudWatch log group "
            f"/msk-connect/{stack_name}-cdc since this Start — the connector may have "
            "failed before writing one, or this app cannot read that log group — so the "
            "cause is not shown here; the stack events above are all that CloudFormation "
            "reported."
        )
    return f"Stack operation ended in '{status}'."


def _wait_stack_settles(
    deployer: CdcStackDeployer,
    stack_name: str,
    *,
    driver: _StageDriver,
    since: datetime,
    timeout: float,
    interval: float,
    vanish_ok: bool = False,
    connector_names_for_log: Optional[Sequence[str]] = None,
    watch_seeder_enis: bool = False,
    log_tail: Optional[ConnectorLogReader] = None,
) -> Optional[str]:
    """Stream stack events to the log and block until the stack settles.

    Returns the final (non ``*_IN_PROGRESS``) StackStatus. When ``vanish_ok`` is
    set (the Delete path), a stack that no longer exists is a success and returns
    ``None``. Otherwise a transient ``None`` from ``stack_status`` just keeps
    polling. Raises :class:`CdcDeployError` on a ROLLBACK/FAILED terminal state or
    on timeout. Honors cancellation by returning the last seen status.

    A connector's stack op (CREATE_FAILED) rolls back with only a generic
    CloudFormation event -- the real cause lives in the worker log. When
    ``connector_names_for_log`` lists the connectors being created, a rollback scans
    EACH one's worker log so a known failure (partition quota, source unreachable, a
    plugin defect, …) becomes actionable guidance instead of an opaque
    "Stack operation ended in 'UPDATE_ROLLBACK_COMPLETE'". Pass the REAL connector
    names (``connector_log_tail`` matches a log stream by substring, so a synthetic
    "src + sink" pseudo-name would match no stream and the diagnosis would be dead on
    exactly this -- the primary -- connector-failure path); the stack rolls back and
    re-raises here before the per-connector RUNNING-waits (which diagnose on real
    names) are ever reached.

    ``log_tail`` (MySQL Start CDC: the app's own log reader, see
    :func:`_read_connector_log`) also records which resources the stack events report
    FAILED, and the rollback message then reads only the failed connectors' logs written
    since ``since`` (this Start), repeats a failed connector's last error line when no
    signature matches or says that none was found, and names a failed non-connector
    resource with its reason (:func:`_rollback_failure_message`). Without it the rollback
    message is exactly as before.
    """
    import time as _time

    deadline = _monotonic_deadline(timeout)
    seen_until = since
    saw_partition_quota = False
    failed_connectors: list[str] = []
    # The first non-connector resource the events report FAILED (the seeder, say).
    failed_resource: Optional[tuple[str, str]] = None
    status_read_failures = 0
    # Seeder-ENI reporting state. ``-2`` is an unused sentinel so the first real
    # reading (including 0) always prints. ``eni_wait_started`` / ``eni_last_logged``
    # drive the periodic re-report: logging ONLY on change left an observed 18m30s gap
    # between "MskCluster DELETE_COMPLETE" and "Seeder network interfaces released.",
    # which reads as a hung teardown -- the very symptom this reporting exists to cure.
    last_eni_count = -2
    eni_wait_started: Optional[float] = None
    eni_last_logged: Optional[float] = None
    while True:
        if driver.cancelled:
            return None
        # Prove liveness each poll: MSK provisioning can pass >15 min between
        # stack events, and without a heartbeat the stall watchdog would reap this
        # healthy job. (A log line below also refreshes liveness, but events can be
        # sparse, so heartbeat unconditionally.)
        driver.heartbeat()
        for ts, msg in deployer.poll_events(stack_name, seen_until):
            driver.log(msg)
            seen_until = ts
            if _is_partition_quota_message(msg):
                saw_partition_quota = True
            if log_tail is not None:
                _failed = _failed_connector_in_event(msg, connector_names_for_log or ())
                if _failed and _failed not in failed_connectors:
                    failed_connectors.append(_failed)
                elif _failed is None and failed_resource is None:
                    _resource = _failed_resource_in_event(msg)
                    if (
                        _resource is not None
                        and _resource[0] != stack_name
                        and _resource[0] not in _CONNECTOR_RESOURCE_INDEX
                    ):
                        failed_resource = _resource
        # Delete only: report the seeder-ENI reclamation that CloudFormation is silent
        # about, so the long tail of the teardown does not look frozen. Reported on
        # CHANGE *and* periodically while the count is unchanged -- the wait routinely
        # runs ~15-20 min with no stack event, so a change-only line leaves a silent gap
        # that reads as a hang (observed: 18m30s between the last CFN event and
        # "released"). The re-report carries elapsed minutes so the operator can see it
        # is progressing rather than stuck.
        if watch_seeder_enis:
            count = deployer.seeder_eni_count(stack_name)
            if count is not None:
                now_mono = _time.monotonic()
                changed = count != last_eni_count
                if count > 0:
                    if eni_wait_started is None:
                        eni_wait_started = now_mono
                    due = (
                        eni_last_logged is None
                        or (now_mono - eni_last_logged) >= _ENI_REPORT_INTERVAL_SECONDS
                    )
                    if changed or due:
                        noun = "interface" if count == 1 else "interfaces"
                        waited = int((now_mono - eni_wait_started) // 60)
                        # Only mention elapsed time once there is some to mention, so
                        # the first line stays clean.
                        elapsed = f" — {waited} min so far" if waited >= 1 else ""
                        driver.log(
                            f"Waiting for AWS to reclaim {count} seeder network "
                            f"{noun}{elapsed} (no CloudFormation event until done)…"
                        )
                        eni_last_logged = now_mono
                elif changed and last_eni_count > 0:
                    # Only announce release if we had previously seen some pending.
                    driver.log("Seeder network interfaces released.")
                    eni_wait_started = None
                    eni_last_logged = None
                last_eni_count = count
        try:
            status = deployer.stack_status_checked(stack_name)
        except Exception as exc:  # noqa: BLE001 - a status READ failure, not a state
            # A read failure must NOT masquerade as "still in progress" and spin the
            # wait to a false "Stack operation timed out." A credential/authorization
            # error will not self-heal -> fail immediately with the cause. This is the
            # observed failure mode: the deployer's single 1 h assumed-role session
            # expires mid-op (connector waits run 45 min each; an AZ-retry
            # delete+recreate passes an hour), every CFN read then raises ExpiredToken,
            # stack_status swallowed it to None, the None was read as "transient, keep
            # polling", and the loop burned the whole timeout before wrongly reporting
            # a timeout on a stack that had actually reached CREATE_COMPLETE. A
            # transient error is tolerated for a few consecutive polls, then surfaced.
            # Mirrors _wait_connector_running's read-failure handling.
            status_read_failures += 1
            cause = str(exc).splitlines()[0]
            terminal = _is_terminal_read_error(exc)
            driver.log(
                f"could not read stack status ({cause}) "
                f"[{status_read_failures}/{_MAX_STATE_READ_FAILURES}]"
            )
            if terminal or status_read_failures >= _MAX_STATE_READ_FAILURES:
                hint = (
                    " Credentials may have expired or access was lost mid-deploy; the "
                    "stack operation may still be running in AWS -- check the "
                    "CloudFormation console, then retry."
                    if terminal else ""
                )
                raise CdcDeployError(
                    f"Could not read the status of stack '{stack_name}': {cause}.{hint}"
                )
            if _deadline_passed(deadline):
                raise CdcDeployError("Stack operation timed out.")
            driver.sleep(interval)
            continue
        status_read_failures = 0  # a successful read clears the transient streak
        if status is None:
            if vanish_ok:
                return None  # stack is gone — delete succeeded
            # genuinely absent but not a delete wait; keep polling
        elif "IN_PROGRESS" not in status:
            if "ROLLBACK" in status or "FAILED" in status:
                if saw_partition_quota:
                    raise CdcDeployError(_PARTITION_QUOTA_GUIDANCE.format(status=status))
                if log_tail is not None:
                    raise CdcDeployError(_rollback_failure_message(
                        deployer, stack_name, status,
                        connector_names=list(connector_names_for_log or ()),
                        failed=failed_connectors,
                        log_tail=log_tail,
                        failed_resource=failed_resource,
                        since=since,
                    ))
                for name in connector_names_for_log or ():
                    # Scan each REAL connector's worker log (a synthetic combined name
                    # matched no log stream, so the diagnosis was always dead here) and
                    # raise the first actionable cause found.
                    tail = deployer.connector_log_tail(stack_name, name)
                    guidance = _diagnose_connector_log(name, tail)
                    if guidance:
                        raise CdcDeployError(guidance)
                raise CdcDeployError(f"Stack operation ended in '{status}'.")
            return status
        if _deadline_passed(deadline):
            raise CdcDeployError("Stack operation timed out.")
        driver.sleep(interval)


# -- The four lifecycle operations (JobManager work functions) ---------------


def _patch_plugin_params(params: CdcInfraParams, upload) -> CdcInfraParams:
    """Return a new CdcInfraParams with plugin S3 params filled from the upload.

    ``build_cdc_infra_params`` emits empty placeholders for PluginBucketArn / the
    two plugin keys / PluginVersion when the tool will provision them; after the
    bucket + upload stages run, this fills those four with the upload result so
    ``create_stack`` gets concrete values. Pure (no AWS).
    """
    updates = {
        "PluginBucketArn": upload.bucket_arn,
        "DsqlSinkPluginS3Key": upload.dsql_sink_key,
        "LambdaSeederS3Key": upload.lambda_seeder_key,
        "PluginVersion": upload.plugin_version,
    }
    # The source plugin key is engine-specific: a MySQL param set carries
    # DebeziumPluginS3Key, a PostgreSQL one carries DebeziumPostgresPluginS3Key.
    # Fill whichever the set already carries; never cross-add the other engine's key
    # (a MySQL stack must not gain a PG plugin param and vice versa). getattr keeps
    # this tolerant of an older PluginUploadResult without the PG field.
    engine_keys = {
        "DebeziumPluginS3Key": upload.debezium_key,
        "DebeziumPostgresPluginS3Key": getattr(upload, "debezium_pg_key", ""),
    }
    new_filled = [
        (k, engine_keys[k] if k in engine_keys else updates.get(k, v))
        for k, v in params.filled
    ]
    present = {k for k, _ in new_filled}
    # Append only the always-present neutral plugin params, not the engine-specific
    # source key (that must match the param set's engine).
    for k, v in updates.items():
        if k not in present:
            new_filled.append((k, v))
    return CdcInfraParams(
        filled=new_filled,
        stack_name=params.stack_name,
        topic_prefix=params.topic_prefix,
    )


def _param_value(params: CdcInfraParams, key: str) -> Optional[str]:
    """Return a filled parameter's value by key, or ``None`` if absent."""
    for k, v in params.filled:
        if k == key:
            return str(v)
    return None


def _with_subnet_param(params: CdcInfraParams, subnet_ids: str) -> CdcInfraParams:
    """Return a copy of ``params`` with ConnectorSubnetIds replaced. Pure (no AWS)."""
    new_filled = [
        (k, subnet_ids if k == "ConnectorSubnetIds" else v) for k, v in params.filled
    ]
    return CdcInfraParams(
        filled=new_filled,
        stack_name=params.stack_name,
        topic_prefix=params.topic_prefix,
    )


def _retry_infra_without_azs(
    driver: "_StageDriver",
    deployer: CdcStackDeployer,
    stack_name: str,
    params: CdcInfraParams,
    *,
    excluded_azs: set[str],
    new_azs: set[str],
    ec2_client: Optional[BotoSessionLike],
    aws_profile: Optional[str],
    region: Optional[str],
    delete_timeout: float,
    interval: float,
) -> CdcInfraParams:
    """Recover from an MSK unsupported-AZ CREATE_FAILED by re-selecting subnets.

    Tears down the rolled-back stack, then re-selects NAT-egress connector subnets
    with ``excluded_azs`` removed and returns ``params`` with the new
    ConnectorSubnetIds. Raises :class:`CdcDeployError` when the VPC can no longer
    yield >=2 supported AZs (so the caller stops rather than looping forever).
    """
    from dsql_migrator.core.ec2_metadata import (
        Ec2MetadataError,
        build_ec2_client,
        select_connector_subnets,
    )

    driver.log(
        "MSK Serverless does not support availability zone(s) "
        f"{', '.join(sorted(new_azs))} in this region; excluding them and "
        "retrying with different subnets."
    )

    vpc_id = _param_value(params, "VpcId")
    if not vpc_id:
        raise CdcDeployError(
            "MSK rejected the connector subnets' availability zone(s) "
            f"({', '.join(sorted(new_azs))}) and no VpcId is available to "
            "re-select subnets. Enter subnet ids in supported AZs manually."
        )

    # Tear down the rolled-back stack — CloudFormation will not create over it.
    driver.log(f"Deleting the rolled-back stack '{stack_name}' before retrying…")
    deployer.delete_stack(stack_name)
    _wait_stack_settles(
        deployer, stack_name, driver=driver, since=_now(),
        timeout=delete_timeout, interval=interval, vanish_ok=True,
    )

    client = ec2_client or build_ec2_client(aws_profile, region)
    try:
        selection = select_connector_subnets(
            client, vpc_id, excluded_azs=excluded_azs
        )
    except Ec2MetadataError as exc:
        raise CdcDeployError(str(exc)) from exc
    if not selection.can_auto_select or not selection.subnet_ids:
        raise CdcDeployError(selection.reason)
    driver.log(selection.reason)
    return _with_subnet_param(params, selection.subnet_ids)


def run_cdc_infra_deploy(
    handle,
    *,
    stack_name: str,
    template_body: str,
    params: CdcInfraParams,
    deployer: CdcStackDeployer,
    on_log: Callable[[datetime, str], None],
    region: Optional[str] = None,
    aws_profile: Optional[str] = None,
    s3_client: Optional[BotoSessionLike] = None,
    sts_client: Optional[BotoSessionLike] = None,
    ec2_client: Optional[BotoSessionLike] = None,
    create_timeout_seconds: float = 1800.0,
    poll_interval_seconds: float = 30.0,
    delete_timeout_seconds: float = 1200.0,
    max_az_retries: int = 3,
    sleep: Callable[[float], None] = None,  # type: ignore[assignment]
) -> None:
    """Deploy CDC infrastructure with ``create_stack`` (no connectors yet).

    Walks :data:`CDC_INFRA_STAGES`: ensure the managed plugin S3 bucket exists and
    upload the connector artifacts THIS SOURCE ENGINE uses (the engine is derived from
    ``params``' ``EngineType``, so the uploaded set cannot disagree with the stack being
    created -- a PostgreSQL stack references neither the MySQL source plugin nor the
    offset-seeder Lambda), then refuse to create over an
    existing stack (directing the user to Start, or to Delete a rolled-back one),
    then create the stack and poll until ``CREATE_COMPLETE``. The MSK Serverless
    cluster takes ~15-20 min, so ``create_timeout_seconds`` defaults to 30 min.

    ``region``/``aws_profile`` (or injected ``s3_client``/``sts_client`` for tests)
    drive the bucket+upload stages; the resulting bucket ARN / keys / version are
    patched into ``params`` before ``create_stack``.
    """
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.core.s3_provision import (
        S3ProvisionError,
        build_s3_client,
        build_sts_client,
        ensure_and_upload_plugins,
    )

    driver = _StageDriver(
        handle, stages=CDC_INFRA_STAGES, on_log=on_log, sleep=sleep
    )
    try:
        # 0a. ensure plugin bucket + 0b. upload plugins (background — ~32-43 MiB,
        #     depending on the source engine).
        driver.stage("ensure_bucket", "IN_PROGRESS")
        if region is None:
            raise CdcDeployError(
                "No AWS region for plugin upload — configure the target connection."
            )
        s3 = s3_client if s3_client is not None else build_s3_client(aws_profile, region)
        sts = sts_client if sts_client is not None else build_sts_client(aws_profile, region)
        driver.stage("ensure_bucket", "DONE")

        driver.stage("upload_plugins", "IN_PROGRESS")
        # Derive the engine from the params we are ABOUT TO SUBMIT rather than take it as
        # an argument: EngineType is exactly what the cdc-stack's IsMySqlSource /
        # IsPostgresSource read, and it is absent on a MySQL set (template Default:
        # mysql). Deriving it HERE makes it structurally impossible for the uploaded
        # artifacts to disagree with the stack being created, and needs no change at
        # either call site (the UI and scripts/run_pg_cdc_e2e.py, neither of which knows
        # to pass an engine -- a defaulted kwarg would have silently skipped the
        # PostgreSQL plugin in the one harness that would otherwise catch it).
        source_engine = (
            SourceType.POSTGRES
            if _param_value(params, "EngineType") == "postgres"
            else SourceType.MYSQL
        )
        try:
            upload = ensure_and_upload_plugins(
                s3, sts, region, on_progress=driver.log, source_engine=source_engine
            )
        except S3ProvisionError as exc:
            driver.stage("upload_plugins", "FAILED")
            raise CdcDeployError(str(exc)) from exc
        params = _patch_plugin_params(params, upload)
        # The cdc-stack template now exceeds CloudFormation's 51,200-byte inline
        # limit, so the deployer stages it in the managed plugin bucket and uses
        # TemplateURL. Reuse the just-provisioned bucket.
        deployer.template_s3_bucket = upload.bucket_name
        driver.stage("upload_plugins", "DONE")

        if driver.cancelled:
            return

        # 1. check existing — never create over a live or rolled-back stack
        driver.stage("check_existing", "IN_PROGRESS")
        driver.log(f"Checking whether cdc-stack '{stack_name}' already exists…")
        existing = deployer.describe_stack_or_none(stack_name)
        if existing is not None:
            status = existing.stack_status
            if "ROLLBACK" in status:
                raise CdcDeployError(
                    f"cdc-stack '{stack_name}' is in '{status}' from a failed "
                    "creation. Delete it (Delete CDC infrastructure) then retry."
                )
            if "IN_PROGRESS" in status:
                raise CdcDeployError(
                    f"cdc-stack '{stack_name}' is '{status}'. Wait for the current "
                    "operation to finish, then retry."
                )
            raise CdcDeployError(
                f"cdc-stack '{stack_name}' already exists ('{status}'). Use Start "
                "CDC to begin replication, or Delete CDC infrastructure first."
            )
        driver.stage("check_existing", "DONE")

        # 2. validate generated params carry no unfilled placeholder
        driver.stage("validate_params", "IN_PROGRESS")
        bad = [k for k, v in params.filled if str(v).startswith(CDC_PLACEHOLDER_PREFIX)]
        if bad:
            raise CdcDeployError(f"Unfilled infrastructure parameters: {bad}")
        driver.stage("validate_params", "DONE")

        if driver.cancelled:
            return

        # 3+4. create_stack, wait for CREATE_COMPLETE, and self-heal the one
        # failure the user cannot pre-empt: MSK Serverless supports only a subset
        # of a region's AZs (no API to list them), so a NAT subnet auto-selected
        # in an unsupported AZ makes MskCluster CREATE_FAILED. On that reason we
        # delete the rolled-back stack, re-select subnets with the AZ excluded,
        # and retry — bounded by max_az_retries so a genuinely stuck deploy stops.
        excluded_azs: set[str] = set()
        attempt = 0
        while True:
            if driver.cancelled:
                return
            attempt += 1

            driver.stage("create_stack", "IN_PROGRESS")
            since = _now()
            deployer.create_stack(stack_name, template_body, params.filled)
            # MEASURED, not guessed. "~15-20 min" was a stale estimate from when this
            # provisioned a PROVISIONED MSK cluster; with MSK Serverless the whole
            # CloudFormation run is dominated by the Lambda + cluster create and finished in
            # 3m34s and 4m07s on two real runs. Telling an operator to expect 15-20 minutes
            # for a 4-minute job is not a harmless over-estimate: they leave the screen, and
            # the deploy that needs them to press Start CDC next sits idle. Phrased as a
            # typical, not a promise -- a different region/VPC can be slower.
            driver.log(
                "Stack creation submitted — provisioning MSK Serverless and the connector "
                "plugins. This usually takes ~5 min."
            )
            driver.stage("create_stack", "DONE")

            driver.stage("stack_create", "IN_PROGRESS")
            try:
                _wait_stack_settles(
                    deployer, stack_name, driver=driver, since=since,
                    timeout=create_timeout_seconds, interval=poll_interval_seconds,
                )
                break
            except CdcDeployError:
                new_azs = deployer.find_unsupported_azs(stack_name) - excluded_azs
                if not new_azs or attempt > max_az_retries:
                    driver.stage("stack_create", "FAILED")
                    raise
                excluded_azs |= new_azs
                try:
                    params = _retry_infra_without_azs(
                        driver, deployer, stack_name, params,
                        excluded_azs=excluded_azs, new_azs=new_azs,
                        ec2_client=ec2_client, aws_profile=aws_profile, region=region,
                        delete_timeout=delete_timeout_seconds,
                        interval=poll_interval_seconds,
                    )
                except CdcDeployError:
                    driver.stage("stack_create", "FAILED")
                    raise
        if driver.cancelled:
            return
        driver.stage("stack_create", "DONE")

        # 5. ready
        driver.stage("infra_ready", "IN_PROGRESS")
        driver.log("Infrastructure ready. Run Start CDC to begin replication.")
        driver.stage("infra_ready", "DONE")
    except CdcDeployError as exc:
        driver.fail(exc)
        raise


# The dev-bastion range an UNCONDITIONAL 9098 ingress admitted on every cdc-stack deployed
# before that rule was removed (ConnectorBastionDiagnosticsIngress). Such a stack keeps the
# rule until a Start updates its template -- and the admission gate runs BEFORE that update.
_LEGACY_BASTION_CIDR = "172.31.0.0/20"
_LEGACY_BASTION_RULE = "ConnectorBastionDiagnosticsIngress"


def _legacy_bastion_may_admit(deployer: object, stack_name: str, own_ip: str) -> bool:
    """Whether a pre-removal stack's legacy bastion rule may admit ``own_ip``: the address
    is in its range and the stack still has (or may have: unreadable) that resource."""
    from dsql_migrator.core.ec2_metadata import cidr_contains

    if not cidr_contains(_LEGACY_BASTION_CIDR, own_ip):
        return False
    probe = getattr(deployer, "stack_has_resource", None)
    if not callable(probe):
        return True  # cannot tell: not a certainty, so do not refuse
    try:
        has = probe(stack_name, _LEGACY_BASTION_RULE)
    except Exception:  # noqa: BLE001 - unreadable: not a certainty
        return True
    return has is not False


def _run_external_seed(
    *,
    stack_name: str,
    bootstrap: str,
    region: str,
    params: CdcStackParams,
    discovery: "CdcStackDiscovery",
    watermark: Optional[Watermark],
    src_name: str,
    driver: "_StageDriver",
    seed_fn: Optional[Callable[..., str]] = None,
    deployer: object = None,
) -> None:
    """Do the CDC Kafka prep in-process for SeedMode=External (the Lambda-free path).

    Reads the same seed inputs the cdc-stack would have handed the in-VPC seeder
    Lambda — the per-table topics + partition plan + DLQ + watermark — from the
    connector params (``params.filled``) and the values carried forward on the
    deployed stack (``discovery.current_parameters``: the partition plan +
    ``MaxMessageBytes`` are set once at create), then calls
    :func:`cdc_kafka_seed.seed_kafka_prep`. Fails LOUDLY (raises
    :class:`CdcDeployError`) if the cluster is unreachable or the optional
    ``cdc-external`` extra is missing — NEVER returns success on a failed seed,
    which would let the connectors start with no seeded offset (a silent gap).

    ``seed_fn`` is the injected test seam (defaults to the real seed function).
    """
    from dsql_migrator.core.cdc import CDC_DEFAULT_DLQ_TOPIC
    from dsql_migrator.core.cdc_kafka_seed import CdcSeedError, seed_kafka_prep

    filled = dict(params.filled)
    current = discovery.current_parameters
    # The 9098 ingress that lets THIS process reach MSK is created at INFRA-DEPLOY time
    # from HostSubnetCidr (cdc-stack.yaml ConnectorHostDiagnosticsIngress, Condition
    # HasHostSubnetCidr) and rides UsePreviousValue into this pass, so it cannot be
    # added now. Judge the DEPLOYED stack, not local config: a session can Start CDC on
    # an ADOPTED stack, or on one created from a different host, and without this the
    # only symptom is a ~60s KafkaTimeoutError for what is really "this stack does not
    # admit you". Refuse ONLY on a certainty -- a NON-EMPTY admitted CIDR that does not
    # contain this address. An empty value or an unknown local address proceeds: the
    # stack may admit this host by a hand-added rule or by SG membership
    # (ConnectorSelfIngress), and the seed's own three-condition failure text still
    # applies. A gate that wrongly refuses a working host is worse than the
    # bug it guards.
    from dsql_migrator.config import load_config
    from dsql_migrator.core.ec2_metadata import cidr_contains, local_ipv4

    admitted = (current.get("HostSubnetCidr") or "").strip()
    own_ip = local_ipv4()
    # An operator who SET DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR to exactly what the stack
    # admits has attested to this host's network, and that attestation outranks an
    # address heuristic. The in-VPC EC2 deployment (its user-data sets the value at
    # boot) and scripts/run_pg_cdc_e2e.py both rely on it, so without this disjunct the
    # gate could refuse a live-verified path -- and a wrong-but-routable own address
    # (bridge-mode ECS, a multi-homed VPN laptop, a multi-ENI instance) is exactly the
    # case the address class guard in ec2_metadata cannot catch.
    try:
        attested = (load_config().cdc_host_subnet_cidr or "").strip()
    except Exception:  # noqa: BLE001 - a config problem must not gate the seed
        attested = ""
    _LOGGER.debug(
        "external-seed admission: admitted=%r own_ip=%r attested=%r",
        admitted,
        own_ip,
        attested,
    )
    if (
        admitted
        and own_ip
        and admitted != attested
        and not cidr_contains(admitted, own_ip)
        and not _legacy_bastion_may_admit(deployer, stack_name, own_ip)
    ):
        raise CdcDeployError(
            f"The cdc-stack '{stack_name}' admits {admitted} on MSK port 9098 and this "
            f"app's address {own_ip} is not in it, so the in-process CDC seed could not "
            "reach MSK. Start CDC from the host whose network the stack admits; or add a "
            "TCP 9098 inbound rule for this app's range to the cdc-stack's "
            f"ConnectorSecurityGroup (CloudFormation -> {stack_name} -> Resources) AND "
            f"set DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR={admitted} on this app to attest "
            "it, then restart the app and Start CDC. HostSubnetCidr itself is fixed "
            "when the stack is created, so changing THAT needs a delete and re-deploy. "
            "No connectors were created."
        )
    # Prefer the freshly-computed connector params; fall back to the values the
    # stack already carries (the partition plan + MaxMessageBytes are set at create
    # and carried forward via UsePreviousValue, so they live on the deployed stack).
    sink_topics_csv = filled.get("SinkTopics") or current.get("SinkTopics", "")
    topic_prefix = filled.get("TopicPrefix") or current.get("TopicPrefix", "")
    dlq_topic = (
        filled.get("DlqTopicName")
        or current.get("DlqTopicName")
        or CDC_DEFAULT_DLQ_TOPIC
    )
    default_partitions = current.get("TopicDefaultPartitions", "1")
    sink_topic_partitions_csv = current.get("SinkTopicPartitions", "")
    max_message_bytes = current.get("MaxMessageBytes") or None
    offset_topic = f"{stack_name}-debezium-source-offsets"
    sink_topics = [t.strip() for t in sink_topics_csv.split(",") if t.strip()]

    driver.log(
        "SeedMode=External: preparing CDC topics + offset in-process over the MSK "
        "IAM bootstrap (Lambda-free)."
    )
    run_seed = seed_fn or seed_kafka_prep
    try:
        outcome = run_seed(
            bootstrap=bootstrap,
            region=region,
            offset_topic=offset_topic,
            offset_partitions=1,
            sink_topics=sink_topics,
            default_partitions=default_partitions,
            sink_topic_partitions_csv=sink_topic_partitions_csv,
            max_message_bytes=max_message_bytes,
            dlq_topic=dlq_topic,
            connector_name=src_name,
            topic_prefix=topic_prefix,
            watermark=watermark,
            # So the operator sees the broker wait instead of a stalled step. A cluster
            # created minutes ago reports ACTIVE before it completes a SASL/IAM handshake,
            # and the seed now waits for that rather than failing the Start on one
            # 30-second bootstrap timeout.
            log=driver.log,
        )
    except CdcSeedError as exc:
        raise CdcDeployError(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - surface any Kafka/network failure loudly
        # Do NOT assert the three static preconditions here. This branch catches ANYTHING
        # the seed raised, and the seed already exhausts a ~14-minute broker wait before it
        # gives up -- that exhaustion path (CdcSeedError, above) is the one entitled to say
        # "the path is the likely cause", and it does. Claiming it here sent a live operator
        # auditing a VPC, a security group and an IAM policy that were all correct: the real
        # cause was an MSK Serverless cluster minutes old whose brokers were not yet
        # accepting a handshake, and the identical code bootstrapped in 0.17s once they were.
        # So: name the failure, say that nothing was created and that a retry is safe, and
        # mention warm-up as the common cause -- without ruling the path out either.
        raise CdcDeployError(
            "In-process CDC seed (SeedMode=External) failed before creating the "
            f"connectors: {exc}. No connectors were created and the infrastructure is "
            "unchanged, so Start CDC can simply be retried — and a retry is usually all "
            "this needs: an MSK Serverless cluster reports ACTIVE before its brokers "
            "accept a SASL/IAM handshake, so a Start run soon after the infrastructure "
            "deploy can hit a cluster that is not ready yet. If retrying keeps failing the "
            "same way, then check the path: this app must run inside the cdc-stack VPC, be "
            "admitted on MSK port 9098, and hold data-plane kafka-cluster IAM."
        ) from exc
    driver.log(f"In-process CDC prep complete (offset seed: {outcome}).")


def _repair_publication_via_partition_root(
    *,
    params: CdcStackParams,
    discovery: "CdcStackDiscovery",
    source_config: Optional[SourceConnectionConfig],
    source_password: Optional[object],
    driver: "_StageDriver",
) -> None:
    """Ensure the CDC publication publishes under the partition ROOT. PostgreSQL only.

    See :func:`~dsql_migrator.core.cdc_pg_slot.ensure_publication_via_partition_root` for
    why this is needed at all (Debezium creates the publication on the ``initial``-snapshot
    routes and cannot set the property) and for what "repaired" does and does not recover.

    NEVER fatal. A Start must not be blocked because the source credentials are absent (they
    are never persisted -- Property 7 -- so a restored session has none) or because the CDC
    user lacks publication ownership. Both are reported as a WARNING naming the one manual
    statement, which is the same contract the Delete path uses for the slot drop: the
    alternative is refusing a Start that would work for every non-partitioned table.
    """
    filled = dict(params.filled)
    current = dict(discovery.current_parameters or {})
    publication = (
        filled.get("PgPublicationName") or current.get("PgPublicationName") or ""
    ).strip()
    if not publication:
        return  # MySQL, or a stack with no PostgreSQL publication to repair
    manual = (
        f'Run this on the source and start again:  ALTER PUBLICATION "{publication}" '
        "SET (publish_via_partition_root = true);"
    )
    if source_config is None or source_password is None:
        driver.log(
            "WARNING: no source credentials in this session, so the CDC publication's "
            "partition-root setting could not be checked. If any captured table is "
            "PARTITIONED, its changes will be silently dropped by the connector. " + manual
        )
        return
    engine = None
    try:
        engine = cdc_pg_slot.build_pg_source_write_engine(source_config, source_password)
        with engine.connect() as connection:
            outcome = cdc_pg_slot.ensure_publication_via_partition_root(
                connection,
                name=publication,
                on_log=lambda message: driver.log(f"source: {message}"),
            )
        if outcome == "unchanged":
            driver.log(
                f'Publication "{publication}" already publishes under the partition root.'
            )
    except Exception as exc:  # noqa: BLE001 - advisory: never fail a Start on this
        driver.log(
            "WARNING: could not set the CDC publication to publish under the partition "
            f"root ({str(exc).splitlines()[0]}). Changes to a PARTITIONED captured table "
            "would be silently dropped by the connector. " + manual
        )
    finally:
        if engine is not None:
            engine.dispose()


def run_cdc_start(
    handle,
    *,
    stack_name: str,
    params: CdcStackParams,
    deployer: CdcStackDeployer,
    on_log: Callable[[datetime, str], None],
    watermark: Optional[Watermark] = None,
    seed_origin: str = "full_load",
    template_body: Optional[str] = None,
    connector_timeout_seconds: float = 2700.0,
    poll_interval_seconds: float = 15.0,
    sleep: Callable[[float], None] = None,  # type: ignore[assignment]
    seed_mode: str = "lambda",
    seed_fn: Optional[Callable[..., str]] = None,
    # PostgreSQL only, and OPTIONAL: without them the publication repair below is skipped
    # with a WARNING rather than failing the Start, exactly as the Delete path treats a
    # missing credential for the slot drop. They are never persisted (Property 7), so a
    # restored session legitimately has none.
    pg_source_config: Optional[SourceConnectionConfig] = None,
    pg_source_password: Optional[object] = None,
    # The app's own connector-log reader (MySQL Start CDC passes the MSK Connect
    # controller's), read before the deployer's: see _read_connector_log.
    log_tail: Optional[ConnectorLogReader] = None,
) -> None:
    """Start CDC with a SINGLE-pass update that creates both connectors at once.

    Walks :data:`CDC_START_STAGES`. Fetches the cluster bootstrap brokers, then a
    single ``update_stack`` sets ``MskBootstrapServers`` + ``DeploySink="true"`` so
    CloudFormation runs ``CdcStartPrepResource`` (pre-creating the per-table topics
    and, on a gapless handoff, seeding the offset) and then creates the source AND
    sink connectors IN PARALLEL (both DependsOn only the pre-created topics, not each
    other). We then wait for both to reach RUNNING. This roughly halves the
    connector wall time vs the old source-then-sink two-pass, and removes the
    empty-partition-assignment race at its source (topics exist before either
    connector starts). The flow is config-aware idempotent: the whole pass is
    skipped only when BOTH connectors are already RUNNING AND the desired connector
    configuration (notably the table set -> ``TableIncludeList`` / ``SinkTopics``)
    matches the deployed stack. When the configuration changed (e.g. a different set
    of tables), the connectors are updated rather than silently kept on the old
    table set; an identical re-start still no-ops (CloudFormation reports no changes)
    and burns no MSK quota.

    ``connector_timeout_seconds`` is the upper bound PER connector wait (applied to
    each connector's RUNNING wait; because both deploy in parallel the total wait is
    ~max(source, sink), not their sum), NOT the whole operation. It defaults to
    2700s (45 min) because MSK Connect connector creation (Fargate provisioning +
    plugin download of our ~70-90 MiB zips + Kafka Connect worker boot + Glue
    Schema Registry connect) can take well past 15-25 min EACH, and a too-short
    budget would time out and red-flag a connector that is still creating normally.
    A generous bound is essentially free: polling exits the instant a connector
    reaches RUNNING/FAILED, so the full timeout is only ever consumed when the
    connector genuinely never settles.

    ``watermark`` is the start offset offered to the seeder -- the Full Load
    consistency point, or an operator's Manual binlog file:position
    (:func:`~dsql_migrator.core.cdc.cdc_seed_watermark`); ``seed_origin`` names which
    (``"full_load"`` / ``"manual"``) for the deploy log. Its binlog coordinates are
    passed as the cdc-stack Watermark* parameters on the SOURCE pass only, so the
    in-VPC seeder Lambda creates + seeds the connect-offsets record before the
    source connector is created (SeedMode=External seeds it in-process). The
    Watermark* params are deliberately kept OUT of the config-changed comparison
    and the shared connector overrides, so a watermark-only change never forces an
    already-RUNNING source connector to be torn down and rebuilt (the seed is a
    create-time concern; rewinding a live connector is exactly what the seeder's
    no-clobber guard prevents anyway). When ``watermark`` is None or lacks binlog
    coordinates the Watermark* params are blanked and the template's SeedOffset
    condition stays false (legacy: start from the current binlog).

    ``log_tail`` (optional) is the app's own reader of a connector's worker log. A failed
    connector create is diagnosed from it first, so the cause is named even when the
    deployer's identity (CdcDeployRole) cannot read the log group. ``None`` keeps every
    failure message exactly as before.
    """
    driver = _StageDriver(
        handle, stages=CDC_START_STAGES, on_log=on_log, sleep=sleep
    )
    src_name, sink_name = cdc_expected_connector_names(stack_name)
    try:
        # 1. discover (raises if absent/unstable/placeholder)
        driver.stage("discover_stack", "IN_PROGRESS")
        driver.log(f"Discovering cdc-stack '{stack_name}'…")
        discovery = deployer.discover_stack(stack_name)
        # The UI judged the exclusion set against the stack value its last probe read; a
        # LIVE value that differs means another session started or redeployed the stack
        # since, and sending this session's set could change capture mid-stream.
        _seen_refusal = column_exclude_seen_refusal(params, discovery.current_parameters)
        if _seen_refusal:
            raise CdcDeployError(_seen_refusal)
        # A stack deployed with the 0.1.561 ColumnExcludeList keeps it byte-for-byte, so
        # a running pipeline is not updated by this release; judged against THIS describe.
        params = apply_column_exclude_chooser(params, discovery.current_parameters)
        # Re-keyed tables (G15): keyed on the SOURCE primary key in the change stream -- so
        # every change to a row stays in order -- only when the DEPLOYED sink plugin reads
        # SinkKeyColumns. PluginVersion changes only on Deploy infra (a Start carries it
        # forward), so it is judged against THIS describe; an older or unknown plugin keeps
        # the composite-key routing every plugin applies, and the log says why.
        _plugin = (discovery.current_parameters or {}).get("PluginVersion")
        _routed = choose_rekey_routing(params, _plugin)
        for _line in rekey_routing_log_lines(params, _routed, _plugin):
            driver.log(_line)
        params = _routed
        driver.stage("discover_stack", "DONE")

        # 2. validate connector params
        driver.stage("validate_params", "IN_PROGRESS")
        bad = [k for k, v in params.filled if str(v).startswith(CDC_PLACEHOLDER_PREFIX)]
        if bad:
            raise CdcDeployError(f"Unfilled values in connector parameters: {bad}")
        # CloudFormation refuses a parameter value over 4,096 bytes, which would fail the
        # update mid-Start: refuse an oversized re-key value before anything is changed.
        _rekey_refusal = rekey_parameter_size_refusal(params)
        if _rekey_refusal:
            raise CdcDeployError(_rekey_refusal)
        # Backstop for the chooser: a KEPT 0.1.561 ColumnExcludeList may still exclude a
        # composite key column, which 0.1.561 refused to start with (Debezium cannot build
        # the record key). Covers Retry CDC and a UI that judged a stale deployed value.
        _legacy_keys = legacy_column_exclude_key_conflicts(params)
        if _legacy_keys:
            raise CdcDeployError(
                "The deployed CDC stack keeps the capture list it was deployed with, which "
                "excludes these composite primary-key columns, so Debezium cannot build "
                f"the record key: {', '.join(_legacy_keys)}. No connectors were changed. "
                "Delete the CDC infrastructure on the CDC step, then deploy it again and "
                "Start CDC: a new stack gets a capture list without key columns."
            )
        driver.stage("validate_params", "DONE")

        if driver.cancelled:
            return

        # 3. fetch bootstrap brokers (re-read each Start; stable per cluster)
        driver.stage("fetch_bootstrap", "IN_PROGRESS")
        cluster_arn = deployer.get_stack_output(stack_name, "MskClusterArn")
        if not cluster_arn:
            raise CdcDeployError(
                "Could not read MskClusterArn from the stack outputs — is the "
                "infrastructure deploy complete?"
            )
        bootstrap = deployer.get_bootstrap_brokers(cluster_arn)
        driver.log("Fetched MSK bootstrap brokers.")
        driver.stage("fetch_bootstrap", "DONE")

        # PostgreSQL only: make the publication publish under the partition ROOT, BEFORE the
        # update below can create the source connector. This is the only point that
        # pre-empts it, and it must pre-empt it: on the routes where the effective
        # snapshot.mode is `initial`, `publication.autocreate.mode` is `filtered` and
        # DEBEZIUM creates the publication -- at PostgreSQL's default
        # publish_via_partition_root=false, which it offers no property to change. pgoutput
        # then names every change after the LEAF partition while `table.include.list` names
        # the collapsed PARENT (the target has no partitioning), so the connector filters out
        # and DISCARDS every change to a partitioned table: RUNNING, empty DLQ, no rows.
        # Live-observed on Aurora PostgreSQL 17.7 -- 7 rows lost with no signal anywhere.
        _repair_publication_via_partition_root(
            params=params,
            discovery=discovery,
            source_config=pg_source_config,
            source_password=pg_source_password,
            driver=driver,
        )

        # connector-control overrides shared by both passes. Watermark* keys are
        # never part of params.filled (they come from the separate watermark arg),
        # so they cannot leak into config_changed; we also defensively exclude them
        # here so a watermark-only change can never bounce a RUNNING source.
        _seed_keys = set(CDC_WATERMARK_PARAM_KEYS)
        connector_overrides = [
            kv
            for kv in params.filled
            if kv[0] not in {"MskBootstrapServers", "DeploySink"} and kv[0] not in _seed_keys
        ]

        # Watermark -> cdc-stack Watermark* params, applied on the SOURCE pass only
        # (a create-time seed concern). Blank/absent watermark -> SeedOffset false.
        watermark_overrides = build_watermark_params(watermark)
        if watermark is not None and watermark.binlog_file:
            # Neutral wording: the seeders never rewind a committed offset (no-clobber),
            # so on a restart the offered offset may legitimately not be applied.
            _origin = {
                "full_load": "Full Load watermark",
                "manual": "Manual start point",
            }.get(seed_origin, seed_origin)
            # The GTID set offered with it: what Debezium starts after on a gtid_mode=ON
            # source (the same normalized value build_watermark_params sends).
            _gtids = normalize_gtid_set(watermark.gtid_executed)
            driver.log(
                f"Offering CDC start offset {watermark.binlog_file}:"
                f"{watermark.binlog_position}"
                + (f" with GTID set {gtid_set_summary(_gtids)}" if _gtids else "")
                + f" ({_origin}) to the seeder; not applied if "
                "the committed offset is already at/past it."
            )
        # MySQL only (the PostgreSQL params carry no SnapshotMode): say which schema path
        # the source connector takes, because it decides how a DDL since the start point
        # is decoded.
        _snapshot_mode = dict(params.filled).get("SnapshotMode")
        if _snapshot_mode in _SNAPSHOT_MODE_LOG:
            driver.log(_SNAPSHOT_MODE_LOG[_snapshot_mode])

        # Has the desired connector configuration (notably the table set ->
        # TableIncludeList / SinkTopics) changed vs the currently-deployed stack?
        # The RUNNING fast-path below skips only when the config is UNCHANGED;
        # when it differs (e.g. a new/different set of tables for Full load + CDC)
        # the connectors MUST be updated, so we never silently keep the old table
        # set. CloudFormation is the safety net for a true no-op (an identical
        # config update reports no changes and burns no MSK partition quota).
        config_changed = any(
            str(discovery.current_parameters.get(key, "")) != str(value)
            for key, value in connector_overrides
        )

        # -- Single pass: create BOTH connectors at once ----------------------
        # CdcStartPrepResource (in the stack) pre-creates the per-table topics, so
        # the sink no longer waits for the source; both connectors deploy in
        # parallel. Best-effort pre-check: a read error only means "cannot confirm
        # RUNNING", so fall through to the (idempotent) pass. Skip the whole pass
        # only when BOTH connectors are already RUNNING with the requested config.
        try:
            src_state = deployer.connector_state(src_name)
        except Exception:  # noqa: BLE001
            src_state = None
        try:
            sink_state = deployer.connector_state(sink_name)
        except Exception:  # noqa: BLE001
            sink_state = None
        both_running = (
            src_state == _CONNECTOR_RUNNING and sink_state == _CONNECTOR_RUNNING
        )
        if both_running and not config_changed:
            driver.log(
                "Both connectors already RUNNING with the requested configuration "
                "— nothing to start."
            )
            driver.stage("submit_connectors", "DONE")
            driver.stage("stack_connectors", "DONE")
            driver.stage("connectors_running", "DONE")
        else:
            if both_running and config_changed:
                driver.log(
                    "Connectors RUNNING but the configuration changed (e.g. a "
                    "different table set) — updating them."
                )
            driver.stage("submit_connectors", "IN_PROGRESS")
            since = _now()
            start_pass = [
                ("MskBootstrapServers", bootstrap),
                ("DeploySink", "true"),
                *watermark_overrides,
                *connector_overrides,
            ]
            # SeedMode=External (Lambda-free): the app does the CDC Kafka prep
            # IN-PROCESS here — pre-create the compacted offset topic + per-table
            # data topics + DLQ topic, and seed the connect-offsets record from the
            # watermark — BEFORE submit_update creates the connectors. This runs
            # only in the else branch (so an idempotent both-running re-Start still
            # skips it, doing zero Kafka I/O), and completes synchronously so the
            # topics provably exist before either connector starts — replacing the
            # in-VPC seeder Lambda's CdcStartPrepResource DependsOn with strict
            # in-process topics-then-connectors ordering. The seed is itself
            # idempotent (topic-already-exists swallowed; offset seed no-clobbered).
            if seed_mode == "external":
                # The template's SeedMode AllowedValues are ["Lambda","External"]
                # (case-sensitive) and SeedByExternal = Equals(SeedMode,"External"),
                # so the value sent to CloudFormation MUST be the capitalized token
                # "External" -- NOT the lowercase config value we compare on above.
                start_pass.append(("SeedMode", "External"))
                _run_external_seed(
                    stack_name=stack_name,
                    bootstrap=bootstrap,
                    region=deployer.region,
                    params=params,
                    discovery=discovery,
                    watermark=watermark,
                    src_name=src_name,
                    driver=driver,
                    seed_fn=seed_fn,
                    deployer=deployer,
                )
            changed = deployer.submit_update(
                stack_name, start_pass, template_body=template_body
            )
            if changed:
                driver.log(
                    "Connector deploy submitted (topics + source + sink in one pass)."
                )
                driver.stage("submit_connectors", "DONE")
                driver.stage("stack_connectors", "IN_PROGRESS")
                try:
                    _wait_stack_settles(
                        deployer, stack_name, driver=driver, since=since,
                        timeout=connector_timeout_seconds, interval=poll_interval_seconds,
                        connector_names_for_log=[src_name, sink_name],
                        log_tail=log_tail,
                    )
                except CdcDeployError:
                    driver.stage("stack_connectors", "FAILED")
                    raise
                driver.stage("stack_connectors", "DONE")
            else:
                driver.log("No connector changes needed.")
                driver.stage("submit_connectors", "DONE")
                driver.stage("stack_connectors", "DONE")
            if driver.cancelled:
                return
            # Both connectors were created by the single stack update and deploy in
            # parallel; wait for each to reach RUNNING under the one connectors_running
            # stage. Sequential polling, so the total wait is ~max(source, sink) --
            # not their sum -- because both are already deploying concurrently. Their
            # worker logs are read without the rollback path's "since this Start" limit:
            # an update that did not change a connector (Retry CDC with the same
            # configuration) leaves a connector FAILED by an earlier run, whose cause was
            # logged before this Start.
            driver.stage("connectors_running", "IN_PROGRESS")
            _wait_connector_running(
                deployer, src_name, "connectors_running", driver,
                connector_timeout_seconds, poll_interval_seconds,
                stack_name=stack_name, log_tail=log_tail,
            )
            _wait_connector_running(
                deployer, sink_name, "connectors_running", driver,
                connector_timeout_seconds, poll_interval_seconds,
                stack_name=stack_name, log_tail=log_tail,
            )
            driver.stage("connectors_running", "DONE")

        # final
        driver.stage("pipeline_running", "IN_PROGRESS")
        driver.log("CDC pipeline is running.")
        driver.stage("pipeline_running", "DONE")
    except CdcDeployError as exc:
        driver.fail(exc)
        raise


def run_cdc_stop(
    handle,
    *,
    stack_name: str,
    deployer: CdcStackDeployer,
    on_log: Callable[[datetime, str], None],
    stop_timeout_seconds: float = 600.0,
    poll_interval_seconds: float = 15.0,
    sleep: Callable[[float], None] = None,  # type: ignore[assignment]
) -> None:
    """Stop CDC by blanking ``MskBootstrapServers`` so CFN deletes both connectors.

    Walks :data:`CDC_STOP_STAGES`. MSK / VPC / plugins / IAM are preserved for a
    fast restart. Idempotent: if the stack already has ``MskBootstrapServers=""``
    (already stopped, or infra-only) no update is submitted.
    """
    driver = _StageDriver(
        handle, stages=CDC_STOP_STAGES, on_log=on_log, sleep=sleep
    )
    try:
        # 1. discover
        driver.stage("discover_stack", "IN_PROGRESS")
        driver.log(f"Discovering cdc-stack '{stack_name}'…")
        discovery = deployer.discover_stack(stack_name)
        already_stopped = (discovery.current_parameters.get("MskBootstrapServers", "") == "")
        driver.stage("discover_stack", "DONE")

        if already_stopped:
            driver.log("CDC is already stopped — no connectors to remove.")
            driver.all_done()
            return

        if driver.cancelled:
            return

        # 2. submit stop (blank the bootstrap → both connector conditions go false)
        driver.stage("submit_stop", "IN_PROGRESS")
        since = _now()
        changed = deployer.submit_update(stack_name, [("MskBootstrapServers", "")])
        if not changed:
            driver.log("No changes — connectors already removed.")
            driver.all_done()
            return
        driver.log("Connector removal submitted.")
        driver.stage("submit_stop", "DONE")

        # 3. wait for the update to settle
        driver.stage("stack_stop", "IN_PROGRESS")
        try:
            _wait_stack_settles(
                deployer, stack_name, driver=driver, since=since,
                timeout=stop_timeout_seconds, interval=poll_interval_seconds,
            )
        except CdcDeployError:
            driver.stage("stack_stop", "FAILED")
            raise
        if driver.cancelled:
            return
        driver.stage("stack_stop", "DONE")

        # 4. done
        driver.stage("connectors_gone", "IN_PROGRESS")
        driver.log("CDC stopped. MSK and infrastructure preserved.")
        driver.stage("connectors_gone", "DONE")
    except CdcDeployError as exc:
        driver.fail(exc)
        raise


def _drop_pg_source_replication(
    driver,
    *,
    stack_name: str,
    stack_params: dict,
    region: Optional[str],
    aws_profile: Optional[str],
    source_credentials: Optional[tuple[str, str]] = None,
    on_source_write: Optional[Callable[[str], None]] = None,
) -> bool:
    """Drop the PostgreSQL CDC replication slot + publication on the source (teardown).

    ``on_source_write`` mirrors every source write/failure into a DURABLE channel (the
    caller's audit log). Provisioning these same objects is audited; destroying them was
    not -- it went only to the in-memory deploy log that the next lifecycle action
    wipes -- so the audit trail recorded the creation of a WAL-pinning slot on the
    customer's production source and nothing about its removal.

    Reconstructs the source connection from the (captured) stack parameters
    (``SourceDbHostname`` / ``SourceDbPort`` / ``PgDatabaseName``) plus the source
    credentials, then drops the slot + publication via the audited, non-read-only-guarded
    PostgreSQL write path (:mod:`dsql_migrator.core.cdc_pg_slot`). The slot/publication
    names are the DEPLOYED ones the connector used (``PgSlotName`` / ``PgPublicationName``
    from the captured params), so a teardown drops exactly what the connector held even if
    the session's stack name drifted; it falls back to the deterministic name. Credentials
    come from the tool-managed secret, or -- for a Secrets-Manager-auth source where the
    tool never created one -- from the stack's own ``SourceSecretArn`` / ``SourceSecretName``.
    ``source_credentials`` are the SESSION's own source username/password, already in this
    process's memory because the operator entered them to connect. They are tried FIRST.
    Without them this function had only one credential path -- reading a secret -- and the
    app's identity is not granted ``secretsmanager:GetSecretValue`` on
    ``mysql-dsql-migrator/cdc/*`` (the TaskRole's policy grants Create/Put/Describe/Restore/
    Delete but not Get), so on a real teardown the drop failed with "Access denied reading
    the secret" while the very next step successfully DELETED that same secret. Using the
    credentials the tool already has needs no IAM at all and is the path that works.

    Best-effort but LOUD: any failure logs a prominent manual-drop reminder (a surviving
    slot pins the source WAL) and returns without failing the already-complete teardown.

    **Returns** whether the slot was dropped, so the caller can keep the credential path
    alive (and report the outcome honestly) when it was not.
    """
    # Prefer the DEPLOYED names the connector actually used; fall back to the derived name.
    slot = (stack_params.get("PgSlotName") or "").strip() or cdc_pg_slot.pg_slot_name(
        stack_name
    )
    publication = (
        stack_params.get("PgPublicationName") or ""
    ).strip() or cdc_pg_slot.pg_publication_name(stack_name)
    host = (stack_params.get("SourceDbHostname") or "").strip()
    database = (stack_params.get("PgDatabaseName") or "").strip()
    try:
        port = int(stack_params.get("SourceDbPort") or 5432)
    except (TypeError, ValueError):
        port = 5432
    manual = (
        f"Drop it manually on the source so it does not pin WAL: "
        f"SELECT pg_drop_replication_slot('{slot}'); DROP PUBLICATION IF EXISTS "
        f'"{publication}";'
    )
    if not host:
        driver.log(
            "WARNING: could not determine the source host to drop the PostgreSQL "
            f"replication slot '{slot}'. {manual}"
        )
        return
    try:
        from dsql_migrator.core.models import SourceConnectionConfig, SourceType
        from dsql_migrator.core.secrets import (
            SecretResolutionError,
            cdc_source_secret_name,
            resolve_source_secret,
        )

        # Credentials, cheapest and most reliable FIRST: the session's own source
        # username/password, already in this process's memory. Reading a secret needs
        # secretsmanager:GetSecretValue, which the app's identity is not granted for the
        # tool-managed CDC secret -- so a teardown that had only that path failed with
        # "Access denied reading the secret" and left the slot pinning WAL.
        if source_credentials and (source_credentials[1] or "").strip():
            username, password = source_credentials
        else:
            # The tool-managed secret is only created for a username/password source. For a
            # Secrets-Manager-auth source it was never created, so fall back to the stack's
            # own source secret (SourceSecretArn/Name) -- the exact creds the connector used.
            try:
                username, password = resolve_source_secret(
                    cdc_source_secret_name(stack_name), aws_profile, region=region
                )
            except SecretResolutionError as _exc:
                fallback = (
                    stack_params.get("SourceSecretArn")
                    or stack_params.get("SourceSecretName")
                    or ""
                ).strip()
                if not fallback:
                    raise
                # NOT "absent": resolve_source_secret turns every boto failure -- including
                # AccessDenied -- into SecretResolutionError, and calling that "absent" told
                # the operator the wrong thing about a secret the very next step then
                # deleted. Report what actually happened.
                driver.log(
                    "Could not read the tool-managed source secret "
                    f"({str(_exc).splitlines()[0]}); trying the stack's own source secret "
                    "to drop the replication slot."
                )
                username, password = resolve_source_secret(
                    fallback, aws_profile, region=region
                )
        source_config = SourceConnectionConfig(
            source_type=SourceType.POSTGRES,
            host=host,
            port=port,
            database=database,
            username=username or "",
        )
        _announce = (
            f"Dropping the PostgreSQL replication slot '{slot}' + publication "
            f"'{publication}' on the source (releases pinned WAL)…"
        )
        driver.log(_announce)
        if on_source_write is not None:
            on_source_write(_announce)

        def _source_log(message: str) -> None:
            driver.log(f"source: {message}")
            if on_source_write is not None:
                on_source_write(message)

        # ``password`` here is a plain str -- it came from ``source_credentials`` or from
        # ``resolve_source_secret``, both of which deal in strings -- but
        # ``build_pg_source_write_engine`` takes a SecretValue and calls ``reveal()`` on it.
        # Passing the str raised `'str' object has no attribute 'reveal'`, which FAILED THE
        # TEARDOWN and left the cdc-stack up with its replication slot still pinning source
        # WAL. It went unseen because a SECOND bug of the same family fired first
        # (``_session_source_credentials`` called ``.strip()`` on the SecretValue), so fixing
        # that one alone would only have swapped one AttributeError for the next.
        # Wrap only a plain str: an already-wrapped SecretValue must pass through, because
        # SecretValue's constructor REFUSES a non-str and double-wrapping raised TypeError --
        # which the teardown's catch-all swallowed into "could not drop the slot", i.e. the
        # same silent failure by another route.
        engine = cdc_pg_slot.build_pg_source_write_engine(
            source_config,
            SecretValue(password) if isinstance(password, str) else password,
        )
        try:
            with engine.connect() as connection:
                cdc_pg_slot.deprovision_pg_replication(
                    connection,
                    slot_name=slot,
                    publication_name=publication,
                    on_log=_source_log,
                )
            _source_log("replication slot + publication dropped")
        finally:
            engine.dispose()
        return True
    except Exception as exc:  # noqa: BLE001 - teardown is already complete; never fatal
        _failure = (
            f"WARNING: could not drop the PostgreSQL replication slot '{slot}': "
            f"{str(exc).splitlines()[0]}. {manual}"
        )
        driver.log(_failure)
        if on_source_write is not None:
            on_source_write(_failure)
        return False


def run_cdc_delete(
    handle,
    *,
    stack_name: str,
    deployer: CdcStackDeployer,
    on_log: Callable[[datetime, str], None],
    region: Optional[str] = None,
    aws_profile: Optional[str] = None,
    cleanup_source_secret: bool = True,
    source_credentials: Optional[tuple[str, str]] = None,
    delete_timeout_seconds: float = 1800.0,
    poll_interval_seconds: float = 30.0,
    sleep: Callable[[float], None] = None,  # type: ignore[assignment]
    on_residue: Optional[Callable[[str], None]] = None,
    on_source_write: Optional[Callable[[str], None]] = None,
) -> None:
    """Delete the entire cdc-stack (full teardown / rollback recovery).

    ``on_residue`` receives one short line per object this teardown could NOT remove
    from the customer's source. It exists because the teardown is deliberately
    non-fatal -- the infrastructure is already gone, so a failed source cleanup must
    not fail the job -- which meant a surviving logical replication slot, the code's
    own "production hazard" (it pins source WAL until the source disk fills), was
    reported ONLY into the in-memory deploy log that the next lifecycle action wipes,
    while the durable audit line said the teardown completed successfully. ``core``
    has no activity-log dependency, so the caller decides how to record it.

    Walks :data:`CDC_DELETE_STAGES`. Uses :meth:`describe_stack_or_none` (not
    ``discover_stack``) so a ``ROLLBACK_COMPLETE`` stack can still be deleted. If
    the stack is already gone the operation succeeds immediately.

    After the stack is gone, the tool-managed source-credentials secret (created
    out-of-band by :func:`ensure_source_secret`, so CloudFormation cannot delete it)
    is scheduled for deletion with a recovery window, so production DB credentials do
    not linger in Secrets Manager. The secret delete uses the app's own credentials
    (``region``/``aws_profile``), NOT the assumed deploy role, mirroring how it was
    created. A cleanup failure does NOT fail the teardown -- the stack is already
    gone -- it logs a manual-deletion reminder. Set ``cleanup_source_secret=False``
    (or omit ``region``) to skip cleanup, e.g. when the source used Secrets Manager
    auth and the tool never created a secret.
    """
    driver = _StageDriver(
        handle, stages=CDC_DELETE_STAGES, on_log=on_log, sleep=sleep
    )
    # Stamped BEFORE any work: the secret cleanup at the end only deletes a secret not
    # written since this moment, so a new deployment that upserted the same name while this
    # teardown was still polling keeps its credentials (see delete_source_secret).
    _teardown_started_at = datetime.now(timezone.utc)
    try:
        # 1. discover (non-raising — a rolled-back stack must still be deletable)
        driver.stage("discover_stack", "IN_PROGRESS")
        driver.log(f"Discovering cdc-stack '{stack_name}'…")
        existing = deployer.describe_stack_or_none(stack_name)
        if existing is None:
            driver.log("Stack does not exist — nothing to delete.")
            # The stack is gone, so its parameters (engine, source host, secret) can no
            # longer be read to drop a PostgreSQL replication slot. If it was deleted
            # out-of-band (e.g. from the CloudFormation console after a DELETE_FAILED), a
            # logical replication slot may survive on the source and pin WAL. Warn so the
            # operator can drop it manually -- the tool cannot reach it from here. (No-op
            # concern for a MySQL source, which has no slot.)
            driver.log(
                "NOTE: if this was a PostgreSQL CDC stack deleted outside the tool, a "
                f"replication slot named '{cdc_pg_slot.pg_slot_name(stack_name)}' may "
                "remain on the source. Drop it there if present so it does not pin WAL: "
                f"SELECT pg_drop_replication_slot('{cdc_pg_slot.pg_slot_name(stack_name)}');"
            )
            driver.all_done()
            return
        # Capture the stack's parameters NOW (while it still exists) so a PostgreSQL
        # teardown can reconstruct the source connection (host/port/db + engine) after
        # the stack is gone, to drop the replication slot + publication on the source.
        stack_params = dict(existing.current_parameters or {})
        # A stack mid-operation cannot be cleanly deleted yet. If a delete is ALREADY
        # underway (DELETE_IN_PROGRESS) just wait for it -- re-submitting is wasteful
        # and CloudFormation ignores it. For any OTHER in-flight operation (a
        # create/update/rollback still running) submitting a delete now races the
        # live operation and CloudFormation may reject it, so stop with a clear
        # wait-and-retry message instead of a blind, possibly-doomed submit. This
        # mirrors the deploy path's IN_PROGRESS guard.
        status = existing.stack_status
        already_deleting = status == "DELETE_IN_PROGRESS"
        # In-flight == any live CloudFormation operation (statuses end in
        # ``_IN_PROGRESS``, e.g. CREATE_/UPDATE_/UPDATE_ROLLBACK_IN_PROGRESS).
        in_flight = bool(status) and status.upper().endswith("_IN_PROGRESS")
        if in_flight and not already_deleting:
            raise CdcDeployError(
                f"cdc-stack '{stack_name}' is '{status}' — an operation is still "
                "running, so it cannot be deleted yet. Wait for it to finish, then "
                "try Delete CDC infrastructure again. (If it stays stuck, delete the "
                f"'{stack_name}' stack in the AWS CloudFormation console.)"
            )
        driver.stage("discover_stack", "DONE")

        if driver.cancelled:
            return

        # 2. submit delete (skip the submit if a deletion is already in flight; just
        #    fall through to wait for it to settle).
        driver.stage("submit_delete", "IN_PROGRESS")
        since = _now()
        if already_deleting:
            driver.log("Stack deletion already in progress — waiting for it to finish.")
        else:
            deployer.delete_stack(stack_name)
            driver.log("Stack deletion submitted.")
        driver.stage("submit_delete", "DONE")

        # 3. wait for the stack to disappear
        driver.stage("stack_delete", "IN_PROGRESS")
        try:
            _wait_stack_settles(
                deployer, stack_name, driver=driver, since=since,
                timeout=delete_timeout_seconds, interval=poll_interval_seconds,
                vanish_ok=True,
                watch_seeder_enis=True,
            )
        except CdcDeployError:
            # DELETE_FAILED is usually the in-VPC offset-seeder Lambda's leftover
            # (detached) ENIs still pinning the connector subnets / SG while AWS
            # reclaims them. Auto-recover: delete those available ENIs and re-issue
            # the delete (retaining anything still stuck), instead of dead-ending the
            # teardown and leaving billable MSK/NAT behind (previously a manual ENI
            # cleanup + delete-stack from the CLI).
            status = deployer.stack_status(stack_name)
            if status == "DELETE_FAILED":
                driver.log(
                    "Stack delete blocked (likely leftover Lambda ENIs); clearing "
                    "detached ENIs and retrying the delete…"
                )
                recovered = deployer.recover_delete_failed(stack_name)
                if recovered == "" or deployer.stack_status(stack_name) is None:
                    driver.log("Stack deletion completed after ENI cleanup.")
                    driver.stage("stack_delete", "DONE")
                else:
                    driver.stage("stack_delete", "FAILED")
                    raise
            else:
                driver.stage("stack_delete", "FAILED")
                raise
        else:
            driver.stage("stack_delete", "DONE")
        if driver.cancelled:
            return

        # 3b. PostgreSQL CDC: drop the logical replication slot + publication on the
        #     source. Runs here -- AFTER the stack (and its connectors) are gone, so the
        #     slot is inactive and droppable -- and BEFORE the source secret is deleted
        #     below, so the drop can still authenticate. A slot left behind pins the
        #     source WAL and can fill the source disk, so this runs regardless of
        #     ``cleanup_source_secret``. NOT a separate stage: it happens under the
        #     existing flow so a MySQL teardown (no slot) is byte-identical. Best-effort
        #     but LOUD -- a surviving slot is a production hazard, so a failure logs a
        #     prominent manual-drop reminder rather than failing the (already-complete)
        #     infra teardown.
        _slot_dropped = True
        if stack_params.get("EngineType") == "postgres":
            _slot_dropped = _drop_pg_source_replication(
                driver,
                stack_name=stack_name,
                stack_params=stack_params,
                region=region,
                aws_profile=aws_profile,
                source_credentials=source_credentials,
                on_source_write=on_source_write,
            )
            if not _slot_dropped and on_residue is not None:
                # Durable, not just the wiped-on-next-action deploy log: this is the
                # one outcome of a teardown that can take the source down later.
                on_residue(
                    "the PostgreSQL logical replication slot "
                    f"'{(stack_params.get('PgSlotName') or '').strip() or cdc_pg_slot.pg_slot_name(stack_name)}' "
                    "is STILL ON THE SOURCE and keeps pinning WAL -- drop it manually"
                )

        # 4. clean up the tool-managed source-credentials secret (best effort).
        #    CloudFormation never owned it, so it must be removed here or it lingers
        #    in Secrets Manager with production DB credentials. A failure here is
        #    logged, not fatal -- the infrastructure is already gone.
        driver.stage("cleanup_secret", "IN_PROGRESS")
        if cleanup_source_secret and region and not _slot_dropped:
            # The slot is still on the source pinning WAL, and this secret holds the only
            # stored credentials that can drop it. Deleting it here destroyed the operator's
            # own recovery path one second after telling them to go and use it -- so keep it,
            # and say why. The credentials are still in Secrets Manager either way; what
            # changes is whether they are scheduled for destruction while a WAL-pinning slot
            # is outstanding.
            from dsql_migrator.core.secrets import cdc_source_secret_name
            driver.log(
                "Keeping the source-credentials secret "
                f"'{cdc_source_secret_name(stack_name)}' in place: the replication slot "
                "could not be dropped, and these are the credentials needed to drop it. "
                "Delete the secret yourself once the slot is gone."
            )
        elif cleanup_source_secret and region:
            from dsql_migrator.core.secrets import (
                SecretProvisionError,
                cdc_source_secret_name,
                delete_source_secret,
            )

            secret_name = cdc_source_secret_name(stack_name)
            try:
                result = delete_source_secret(
                    stack_name=stack_name, aws_profile=aws_profile, region=region,
                    not_modified_since=_teardown_started_at,
                )
                if result == "skipped-modified":
                    driver.log(
                        f"Left the source secret '{secret_name}' in place: it was written "
                        "AFTER this teardown started, so a newer deployment owns it. "
                        "Deleting it would have broken that pipeline's Start CDC."
                    )
                elif result == "skipped-unverified":
                    driver.log(
                        f"Left the source secret '{secret_name}' in place: could not read "
                        "its last-changed time to confirm this teardown owns it. Delete it "
                        "manually in Secrets Manager if no other deployment uses it."
                    )
                elif result == "absent":
                    driver.log(
                        f"No tool-managed source secret '{secret_name}' to remove "
                        "(the source likely used Secrets Manager auth)."
                    )
                else:
                    driver.log(
                        f"Source-credentials secret '{secret_name}' scheduled for "
                        "deletion (7-day recovery window)."
                    )
            except SecretProvisionError as exc:
                driver.log(
                    f"WARNING: could not delete the source secret '{secret_name}': "
                    f"{exc}. Delete it manually in Secrets Manager so the database "
                    "credentials do not linger."
                )
        else:
            driver.log("Skipped source-secret cleanup (no region or disabled).")
        driver.stage("cleanup_secret", "DONE")

        # 5. done
        driver.stage("deleted", "IN_PROGRESS")
        driver.log("CDC infrastructure deleted.")
        driver.stage("deleted", "DONE")
    except CdcDeployError as exc:
        driver.fail(exc)
        raise


def _wait_connector_running(
    deployer, connector, stage_name, driver: _StageDriver, timeout, interval,
    *, stack_name: Optional[str] = None, log_tail: Optional[ConnectorLogReader] = None,
) -> None:
    """Poll one connector until RUNNING; raise on FAILED or timeout.

    ``driver`` supplies cancellation, logging, and the injectable ``sleep``. On a
    FAILED connector the CloudFormation event is only a generic error, so when
    ``stack_name`` is given we read the connector's worker log and, if it matches a
    known failure signature (partition-quota exhaustion, source-DB unreachable, a
    plugin/SDK defect, …), raise with the actionable guidance instead of the opaque
    "entered FAILED state". ``log_tail`` is read first (:func:`_read_connector_log`).
    """
    deadline = _monotonic_deadline(timeout)
    read_failures = 0
    while True:
        if driver.cancelled:
            return
        # Prove liveness each poll so a connector that legitimately takes many
        # minutes to reach RUNNING is not reaped by the stall watchdog.
        driver.heartbeat()
        try:
            state = deployer.connector_state(connector)
        except Exception as exc:  # noqa: BLE001 - a state READ failure, not a state
            # A read failure must NOT masquerade as "still creating" (that stalled the
            # deploy forever with no surfaced cause). A credential/authorization error
            # will not self-heal -> fail immediately with the cause; a transient error
            # is tolerated for a few consecutive polls, then surfaced.
            read_failures += 1
            cause = str(exc).splitlines()[0]
            terminal = _is_terminal_read_error(exc)
            driver.log(
                f"{connector}: could not read state ({cause}) "
                f"[{read_failures}/{_MAX_STATE_READ_FAILURES}]"
            )
            if terminal or read_failures >= _MAX_STATE_READ_FAILURES:
                hint = (
                    " Credentials may have expired or access was lost; retry Start CDC."
                    if terminal else ""
                )
                raise CdcDeployError(
                    f"Could not read {connector} state: {cause}.{hint}"
                )
            if _deadline_passed(deadline):
                raise CdcDeployError(_connector_timeout_message(
                    deployer, stack_name, connector, log_tail=log_tail
                ))
            driver.sleep(interval)
            continue
        read_failures = 0  # a successful read clears the transient-failure streak
        if state == _CONNECTOR_RUNNING:
            driver.log(f"{connector}: RUNNING")
            return
        if state == _CONNECTOR_FAILED:
            raise CdcDeployError(_connector_failure_message(
                deployer, stack_name, connector, log_tail=log_tail
            ))
        driver.log(f"{connector}: {state or 'creating'}…")
        if _deadline_passed(deadline):
            raise CdcDeployError(_connector_timeout_message(
                    deployer, stack_name, connector, log_tail=log_tail
                ))
        driver.sleep(interval)


def _connector_failure_message(
    deployer, stack_name: Optional[str], connector: str,
    log_tail: Optional[ConnectorLogReader] = None,
) -> str:
    """Best-effort actionable message for a FAILED connector (worker-log driven).

    Falls back to a plain "entered FAILED state" when no stack name is available
    or the worker log matches no known signature. ``log_tail`` is read first
    (:func:`_read_connector_log`).
    """
    bare = f"{connector} entered FAILED state."
    if stack_name:
        tail = _read_connector_log(deployer, log_tail, stack_name, connector)
        guidance = _diagnose_connector_log(connector, tail)
        if guidance:
            return guidance
        # Nothing matched -- but the tail was already fetched, so say what it said instead
        # of throwing it away and reporting only the state.
        excerpt = _connector_log_excerpt(tail)
        if excerpt:
            return f"{bare} {_UNMATCHED_LOG_HINT.format(excerpt=excerpt)}"
    return bare


# Monotonic deadline helpers kept tiny + injectable-friendly. Real time is fine
# here (a live deploy genuinely waits); tests pass sleep=lambda _: None and short
# timeouts, or a fake deployer that returns RUNNING immediately.
def _monotonic_deadline(seconds: float) -> float:
    import time as _time

    return _time.monotonic() + seconds


def _deadline_passed(deadline: float) -> bool:
    import time as _time

    return _time.monotonic() >= deadline


__all__ = [
    "CdcDeployError",
    "CdcStackDiscovery",
    "CdcDeployLog",
    "SCHEMA_HISTORY_MARK",
    "DEFINITION_SNAPSHOT_MARK",
    "ConnectorLogDiagnosis",
    "diagnose_connector_log",
    "history_missing_after_failure",
    "is_schema_history_failure",
    "CdcStackDeployer",
    "build_cdc_stack_deployer",
    # stage lists (one per lifecycle operation)
    "CDC_INFRA_STAGES",
    "CDC_START_STAGES",
    "CDC_STOP_STAGES",
    "CDC_DELETE_STAGES",
    # seed helpers
    "seed_infra_stages",
    "seed_start_stages",
    "seed_stop_stages",
    "seed_delete_stages",
    # lifecycle operations (JobManager work functions)
    "run_cdc_infra_deploy",
    "run_cdc_start",
    "run_cdc_stop",
    "run_cdc_delete",
]
