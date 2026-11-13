# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""What a failed CDC connector's worker log means, and how it is said.

The catalogue of known connector failures -- a log signature (a lower-cased substring of
the worker log) -> the short headline and the runbook the operator is given -- plus the
rules that read a log tail safely: which line of it names the error, which part of it
belongs to the latest task run, how a quoted value or a credential is masked before any
of it reaches the UI or the activity log (Property 7), and how much of an unparseable
DDL statement may be shown.

All of it is PURE text work on a tail somebody else read: nothing here calls AWS, reads a
log group or touches a job. :mod:`dsql_migrator.core.cdc_deployer` -- which does all
three -- asks these functions what a tail means, and re-exports every name below so the
UI and the tests keep importing them from there.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from dsql_migrator.core import cdc_pg_slot


# What a Delete and a redeploy then cost, per engine (G25): this text is read on either, and
# nothing here says which. A MySQL Start on the new stack re-reads the binary log from a start
# point -- with CDC only and none, from the source's current position. A PostgreSQL delete
# drops the replication slot, so its way on is the route rule's (cdc_pg_slot.pg_cdc_route).
_QUOTA_REDEPLOY_COST = (
    "On a MySQL source, Start CDC on the new stack re-reads the binary log from the Full Load "
    "watermark or your Manual start point; with CDC only and neither, it begins at the "
    "source's current position, so changes since the last one Aurora DSQL applied are not "
    "replicated. On a PostgreSQL source, deleting drops the replication slot: "
    + cdc_pg_slot.pg_cdc_route(None, started=True, cdc_only=None)
    + "."
)

# G25 (decisions A/B): every connector diagnosis below is read on a stack whose connectors
# were started (running, stopped since, or a Start CloudFormation rolled back), so its CDC
# topics may hold changes no sink applied and its sink resumes from its last commit: a NEW
# start point there needs new topics first -- the Full Load step refuses one, and Start CDC
# refuses a newer one -- and the Manual alternative is on the new stack. The last sentence is
# ui.cdc_start_point_gate.RELOAD_RESETS_MANUAL verbatim (core may not import ui; pinned).
_NEW_START_POINT_ROUTE = (
    "This stack has streamed, so a new start point needs new CDC topics: Delete CDC "
    "infrastructure and deploy it again (the new stack starts with empty topics), then "
    "Re-run Full Load for all tables with \"Drop & reload\" (Full load + CDC; re-run Step 1 "
    "(Evaluation) and Schema Conversion first if a captured table changed), then Start CDC. "
    "On the new stack you can instead choose Manual, Fetch current position and Use this "
    "start point: changes made between where CDC stopped and that point are then not "
    "replicated, so reload those tables with \"Drop & reload\" (CDC only: correct them on "
    "Aurora DSQL) and validate with CHECKSUM. "
    "A reload switches the CDC start point back to Automatic, so choose Manual again and "
    "Use this start point before Start CDC (the fetched point is kept)."
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
    "check names a changed or missing table, give CDC a start point after the change. "
    + _NEW_START_POINT_ROUTE
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
    "statement, so Retry CDC, and Start CDC after Stop CDC, read it and stop again. To "
    "continue, give CDC a start point after the statement. " + _NEW_START_POINT_ROUTE + " "
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
    "recorded while gtid_mode is ON. " + _NEW_START_POINT_ROUTE
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
    "CDC: Debezium resumes from where it stopped by binlog file:position, with no gap while "
    "the CDC topics still retain the changes the sink had not applied when CDC stopped "
    "(their retention period on MSK); if the binlog retention also ran out, that Start "
    "fails rather than skip changes. Otherwise, give CDC a start point the source still "
    "holds (raise the source's binlog retention first, for example CALL "
    "mysql.rds_set_configuration('binlog retention hours', 168)). " + _NEW_START_POINT_ROUTE
)


_CONNECTOR_LOG_DIAGNOSES: tuple[tuple[str, str], ...] = (
    (
        "quota exceeded for maximum number of partitions",
        "{connector} failed: the MSK Serverless partition quota is exhausted. Each "
        "connector create/delete consumes partitions that are not reclaimed, so "
        "repeated Start/retry cycles run the cluster out. This cannot be fixed in "
        "place: use Delete CDC infrastructure to tear down the whole cdc-stack, then "
        "deploy again from scratch. " + _QUOTA_REDEPLOY_COST + " To avoid recurrence, "
        "finalize your table selection before Start so you do not iterate connectors.",
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
        "suggests all replay the same rows and fail again. To continue, give CDC a start "
        "point after the change. " + _NEW_START_POINT_ROUTE,
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
        "Retry CDC and recovery mode repeat it. To continue, give CDC a start point after "
        "the rename. " + _NEW_START_POINT_ROUTE,
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
        # Names no "Re-snapshot every table instead": the Start dialog never offers it on a
        # stack whose connectors were started (G25, _cdc_ui._pg_stack_started), and every
        # stack this failure is read on is one: its connectors still exist (the task died
        # after the stack update; Stop CDC then keeps DeploySink=true), or the Start's update
        # rolled back (any UPDATE_ROLLBACK_*), which removed them. Either way the CDC topics
        # may hold changes no sink applied, which this stack's sink would apply over a Full
        # Load re-run on it, so the one route is new topics first -- the route rule's own
        # words (cdc_pg_slot.pg_cdc_route), as the Start dialog and the job name it (G25).
        # The migration type is not known here, so both types' last steps are named.
        "publication autocreation is disabled",
        "{connector} failed: the PostgreSQL CDC publication does not exist on the "
        "source, and the connector is configured not to create one. Only a "
        "'Full load + CDC' run creates it. If the CDC step still shows the connectors, Stop "
        "CDC first (or Clean up leftover connector on an incomplete pipeline); a Start whose "
        "CloudFormation update rolled back left none to remove. "
        + cdc_pg_slot.pg_cdc_route(
            cdc_pg_slot.PUBLICATION_MISSING_HEADER, started=True, cdc_only=None
        )
        + " Do NOT create a replication slot by hand to resume from: a slot cannot start at "
        "a past position, so it would silently skip every change made since the Full Load.",
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
