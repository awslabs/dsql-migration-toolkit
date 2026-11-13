# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Fix target schema: the dead-letter drift copy and the add-column dialog (extracted from
``_cdc_monitoring.py``).

What the schema-drift banner says about a group of dead-lettered records and how to repair
the table: the drift labels and their "why" (:data:`_DRIFT_LABELS`, :func:`_drift_why`, the
23505 / 07006 / value-fit texts), the source schema re-check's confirming changes
(:func:`_catalog_confirming_changes`, :func:`_catalog_label`), the drift-kind sets, the
runbooks (:func:`_ddl_runbook`, :func:`_truncate_runbook`, :func:`_value_fit_runbook`,
:func:`_key_change_remedy`), the Fix target schema dialog's notices and runbook
(:func:`_fix_refusal_notice`, :func:`_fix_rename_notice`, :func:`_fix_held_notices`,
:func:`_fix_orphan_notice`, :func:`_fix_runbook`), its inputs (:func:`_fix_inputs`) and the
dialog itself (:func:`_open_add_column_dialog`, the one function here that renders).

Its top-level imports are ``core.models`` and ``ui.design`` only; the rest are imported
inside functions, as they were. Nothing from the package or ``_cdc_monitoring`` at the top:
``_cdc_monitoring`` imports this module at its top and re-exports every name defined here,
private ones included, so an import from it resolves unchanged. ``_LOGGER`` is the
package's logger, by name. A re-export does not carry a monkeypatch: a test patches a name
here to reach the reads made here (the dialog's ``render_notice`` and ``section_header``
among them).
"""

from __future__ import annotations

import logging
from typing import NamedTuple, Optional

from dsql_migrator.core.models import SourceType
from dsql_migrator.ui.design import render_notice, section_header

# The package's logger (``dsql_migrator.ui.data_migration``), by name: importing it from the
# package would make this module part of the package <-> ``_cdc_monitoring`` import order.
_LOGGER = logging.getLogger("dsql_migrator.ui.data_migration")


# The 23505 "why", up to its remedy tail. Deliberately does NOT assert a cause: 23505 is
# raised by any unique index, so an out-of-band duplicate raises it too. It names the
# realistic causes -- a primary-key change (or a unique value passed between rows) on any
# table with a secondary unique index, held up to 5 minutes since sink plugin v53 (G23), the
# composite-key cause, and a source that dropped or relaxed the index (CDC replicates no DDL,
# and the source schema re-check compares no index) -- and, above all, that a row may be
# MISSING -- the one thing an unclassified poison row cannot say. The index name, when the
# record carries one, is shown beside the label. The tail follows the migration type
# (:func:`_drift_why`): CDC only has no reload, and there its Delete-and-Deploy aside also
# names what the delete costs (:func:`_fix_engine_sentence`: on MySQL the stream position of
# EVERY captured table goes with it).
_UNIQUE_CONFLICT_WHY_HELD = (
    "Aurora DSQL refused the row because a unique index already holds its values "
    "(SQLSTATE 23505), so the row can be missing or stale on the target. On any table "
    "with a unique index besides the primary key, a change can reach Aurora DSQL before "
    "the earlier change that frees the value: most often an UPDATE of the primary key, "
    "which is replicated as a delete of the old key and an insert of the new key that "
    "can travel on different stream partitions, or a unique value passed from one row to "
    "another. CDC infrastructure deployed by this version holds such a change for up to "
    "5 minutes and dead-letters it only if the conflict outlasts the wait (the change "
    "that frees the value is further behind, an older change of the row replayed after a "
    "restart or a re-read of the stream has its later change further behind on its own "
    "partition, or the target already holds the value from outside the stream); "
    "infrastructure deployed by an earlier version dead-letters it "
    "at once (Delete and Deploy CDC infrastructure to pick up the wait)."
)
_UNIQUE_CONFLICT_WHY_REST = (
    " On a table with "
    "a composite key (Schema Conversion's Composite key), an UPDATE to the leading key "
    "column produces it when the change does not carry the old value (REPLICA IDENTITY "
    "below FULL on a PostgreSQL source) or when the CDC infrastructure was deployed by "
    "an earlier version of this tool; the leading key column must be one the "
    "application never updates. A source that dropped or relaxed this UNIQUE index gives "
    "the same refusal (no source DDL change is replicated): drop it on Aurora DSQL too"
)
_UNIQUE_CONFLICT_WHY = _UNIQUE_CONFLICT_WHY_HELD + _UNIQUE_CONFLICT_WHY_REST
_UNIQUE_CONFLICT_TAIL = (
    ", and re-run Step 1 (Evaluation) before the reload so the reload does not rebuild it. "
    "In each case, reload the table as below and validate it"
)
_UNIQUE_CONFLICT_TAIL_CDC_ONLY = ". In each case, repair the table as below and validate it"

# The 07006 "why", up to its recovery. Neither a schema change nor a key-model conflict: the
# sink cannot BIND the value, so EVERY insert and update for the table dead-letters. Names the
# array cause because it is the one the tool itself creates (Schema Conversion substitutes an
# array to jsonb), and says plainly that the count-based signals cannot see it -- that is the
# whole reason this needed classifying. The recovery follows the migration type.
_UNBINDABLE_WHY = (
    "Aurora DSQL never received the row: the sink could not bind one of its values "
    "(SQLSTATE 07006), so EVERY insert and update for this table is being "
    "dead-lettered for as long as the stream runs. The known cause is a source ARRAY "
    "column: Schema Conversion stores it as jsonb (DSQL has no array type) and the Full "
    "Load converts it correctly, but the change stream delivers it as a list the sink "
    "cannot bind. Row COUNTS cannot see this — an update that never lands changes no "
    "count — so run Validation (step 4), whose checksum does. Recovery: "
)
_UNBINDABLE_RECOVERY = (
    "stop CDC and reload this table with 'Drop & reload' to bring its values current (an "
    "append reload cannot — it never rewrites a primary key the target already has); the "
    "stream cannot catch up on its own until the sink connector is upgraded"
)
# CDC only: the upgrade, then (in _drift_why) what deleting the infrastructure costs, then this.
_UNBINDABLE_RECOVERY_CDC_ONLY = (
    "the stream cannot catch up on its own until the sink connector is upgraded (Delete and "
    "Deploy CDC infrastructure)."
)
_UNBINDABLE_REPAIR_CDC_ONLY = " Repair the rows set aside as below"

# Source-schema-drift kinds (core.cdc.SchemaDriftKind values) -> a human label and
# the reason DSQL rejected the row. Detection-only: CDC never propagates DDL, so a
# source ALTER leaves the target behind and its rows dead-letter. The recovery is
# always operator-driven (the tool never auto-alters the target -- Property 6).
_DRIFT_LABELS: dict[str, tuple[str, str]] = {
    "add-column": (
        "column added or renamed at the source",
        "the source added or renamed a column, and the target table has no column of that "
        "name, so DSQL rejected the new-schema rows (SQLSTATE 42703)",
    ),
    # Neutral on purpose: a 23502 whose record did not carry the column is a dropped column,
    # an excluded one, or (PostgreSQL) an unchanged TOAST value, and one with no evidence at
    # all may be a NULL the source now allows. The banner upgrades the label to "column
    # dropped at the source" only when the source schema re-check confirms it (_catalog_label).
    "drop-column": (
        "a NOT NULL column got no value",
        "Aurora DSQL rejected rows that carry no value for a column it requires, NOT NULL "
        "with no DEFAULT (SQLSTATE 23502): usually the source dropped that column, or now "
        "allows NULL in it, or it is a generated column CDC does not carry",
    ),
    "type-change": (
        "column type changed at the source",
        "the source changed a column's type incompatibly, so DSQL rejected the rows "
        "(SQLSTATE 42804, or 42883 for a delete keyed on that column)",
    ),
    # TARGET-side, and the most destructive: every event for the table is dead-lettered
    # with its offset committed, so the rows are not waiting anywhere to be retried.
    "missing-table": (
        "the TARGET table no longer exists",
        "the table this stream writes to is gone from Aurora DSQL — dropped and "
        "recreated (a Schema Conversion REPLACE) or dropped out of band — so DSQL "
        "rejected every row (SQLSTATE 42P01)",
    ),
    "unique-conflict": (
        "a unique constraint rejected the row",
        _UNIQUE_CONFLICT_WHY + _UNIQUE_CONFLICT_TAIL,
    ),
    "unbindable-value": (
        "the sink cannot write one of this table's columns",
        _UNBINDABLE_WHY + _UNBINDABLE_RECOVERY,
    ),
    # The only kind where the target ends up with EXTRA rows rather than missing ones, so it
    # needs its own header and its own runbook line (an ALTER backfills nothing here). Says
    # plainly that the divergence is permanent without action and that Validation is what
    # measures it, because one dead letter per truncated table sits well under the DLQ badge's
    # 50-record warn threshold and would otherwise pass unnoticed.
    "source-truncate": (
        "a source TRUNCATE was not replicated",
        "the source truncated this table, but Aurora DSQL has no TRUNCATE statement, so there "
        "is no correct apply and the sink quarantined the event rather than guessing. The "
        "target still holds the rows the source dropped and will keep them until you act "
        "(Validation, step 4, reports them as EXTRA rows). The stream itself is unaffected: "
        "later inserts and updates keep flowing",
    ),
    # The sink's own pre-write refusal (no SQLSTATE): Aurora DSQL would have ACCEPTED these
    # values and stored them changed. Not necessarily a DDL change -- a MySQL TINYINT(1) holds
    # up to 127 with none -- so it names the value rule, not a source ALTER, and gets its own
    # header and runbook below.
    "value-does-not-fit": (
        "a value its Aurora DSQL column cannot hold exactly",
        "the sink set these rows aside instead of writing a value this column would change: a "
        "number other than 0 or 1 for a boolean column would be stored as true or false (a "
        "MySQL TINYINT(1) can hold up to 127 with no source change at all), and a fraction, or "
        "more decimal places than an integer or numeric column keeps, would be rounded "
        "(usually after the source type was widened, or a narrower type was chosen in Schema "
        "Conversion). Values that fit keep flowing",
    ),
    # 42P10: every insert and update for the table is lost (deletes, keyed by WHERE, still
    # apply), so the banner is `error` -- but NOT target-side: the table exists, and the
    # dropped-table "Stop CDC and reload these tables" notice would be false here.
    "key-change": (
        "the change records' key no longer matches the Aurora DSQL primary key",
        "Aurora DSQL found no primary key or unique index matching the key these change "
        "records carry (SQLSTATE 42P10), so EVERY insert and update for this table is being "
        "dead-lettered: the source's primary key changed, or the table was re-applied in "
        "Schema Conversion with a different key than the deployed CDC infrastructure writes by",
    ),
    "nullability-change": (
        "a NULL sent for a NOT NULL column",
        "the change records carry NULL in a column the Aurora DSQL table keeps NOT NULL, so "
        "DSQL rejected the rows (SQLSTATE 23502): usually the source column now allows NULL",
    ),
    # 23514. The tool's own ENUM port is the usual CHECK (text + CHECK IN (...)), so a value
    # the source type gained is the realistic cause; a hand-added CHECK is named too.
    # 428C9: target-side, and every insert and update is lost, like key-change.
    "generated-column": (
        "a GENERATED column was sent a value",
        "the Aurora DSQL column is GENERATED (it computes its own value), and the change "
        "records carry one, so DSQL rejected every insert and update for this table "
        "(SQLSTATE 428C9). This is not a source change: the column is GENERATED on the "
        "target but the stream still carries its value -- the capture was set up before "
        "the column became GENERATED (in Schema Conversion or outside it)",
    ),
    "check-violation": (
        "a CHECK constraint rejected the row",
        "a value fails a CHECK constraint on the Aurora DSQL table, so DSQL rejected the rows "
        "(SQLSTATE 23514). Schema Conversion gives every MySQL ENUM column a CHECK listing its "
        "allowed values, so usually the source ENUM gained a value; a CHECK you added in Schema "
        "Conversion or on Aurora DSQL does the same. Rows the CHECK allows keep flowing",
    ),
}

# The same "why" for a PostgreSQL source, which has no TINYINT(1): there a boolean target only
# meets other numbers after a source boolean column was changed to an integer type.
# A MySQL TIME duration (outside 00:00:00..24:00:00) the sink refuses for a time column.
_VALUE_FIT_TIME_WHY = (
    "the sink set these rows aside instead of writing a value this column would change: a "
    "MySQL TIME outside 00:00:00..24:00:00 is a duration, which an Aurora DSQL time column "
    "(a time of day) cannot hold -- give the column the interval type instead. Values that "
    "fit keep flowing"
)


def _value_fit_times_only(group) -> bool:
    """Whether every column a value-fit group names is a time column (a TIME duration)."""

    def _is_time(name: str) -> bool:
        lowered = name.lower()
        if lowered.endswith((" time", " time without time zone", " time with time zone")):
            return True
        head, sep, tail = lowered.rpartition(" time(")
        return bool(head and sep) and tail.endswith(")") and tail[:-1].isdigit()

    columns = [str(c) for c in (getattr(group, "columns", None) or ())]
    return bool(columns) and all(_is_time(c) for c in columns)


_VALUE_FIT_WHY_PG = (
    "the sink set these rows aside instead of writing a value this column would change: a "
    "number other than 0 or 1 for a boolean column would be stored as true or false (after a "
    "source boolean column was changed to an integer type), and a fraction, or more decimal "
    "places than an integer or numeric column keeps, would be rounded (usually after the "
    "source type was widened, or a narrower type was chosen in Schema Conversion). Values "
    "that fit keep flowing"
)

# Aurora DSQL's OWN rejection of a value (22001 / 22003), when the group holds only those: the
# same kind as the sink's refusals (the column needs a type that holds the value), but not the
# sink's words. Never a type-change claim. ``{pg}`` is the PostgreSQL numeric clause.
_VALUE_FIT_SERVER_WHY = (
    "Aurora DSQL rejected a value its column cannot hold: a string longer than its varchar or "
    "char length (SQLSTATE 22001), or a number outside its integer or numeric range (SQLSTATE "
    "22003), usually after the source column was widened or because a narrower type was "
    "chosen in Schema Conversion{pg}. Values that fit keep flowing"
)
_VALUE_FIT_SERVER_PG = (
    " (a numeric with no precision is stored as numeric(18,6) on Aurora DSQL)"
)
# Appended to the sink's why when a group holds both the sink's refusals and DSQL's own.
_VALUE_FIT_SERVER_ALSO = (
    ". Aurora DSQL also rejected a string longer than its varchar or char column (SQLSTATE "
    "22001) or a number outside its integer or numeric range (SQLSTATE 22003)"
)
# The sink's synthetic value-fit code (core.cdc_dlq.VALUE_DOES_NOT_FIT_CODE). Every OTHER code
# a value-fit group can hold is one core.cdc maps to the kind -- Aurora DSQL's own rejection --
# so no second code table is kept here.
_SINK_VALUE_FIT_CODE = "VALUE_DOES_NOT_FIT"

# A drop-column group whose records PROVE the column was not sent (group.columns names it).
_DROP_COLUMN_NOT_SENT_WHY = (
    "the change records do not carry this column, which the Aurora DSQL table requires (NOT "
    "NULL, no DEFAULT), so DSQL rejected the rows (SQLSTATE 23502): usually the source "
    "dropped it, or CDC never carries it (a generated column, or a PostgreSQL type Debezium "
    "has no converter for)"
)
_DROP_COLUMN_TOAST_PG = (
    ", or an update left its large (TOASTed) value unchanged, which PostgreSQL does not resend"
)

# The check-violation "why" for a PostgreSQL source: its enum is a TYPE, and ALTER TYPE ... ADD
# VALUE changes no column definition, so the source schema re-check cannot see it.
_CHECK_WHY_PG = (
    "a value fails a CHECK constraint on the Aurora DSQL table, so DSQL rejected the rows "
    "(SQLSTATE 23514). Schema Conversion gives every PostgreSQL enum column a CHECK listing its "
    "labels, so usually the source enum gained a value (ALTER TYPE ... ADD VALUE, which the "
    "source schema re-check cannot see); a CHECK you added in Schema Conversion or on Aurora "
    "DSQL does the same. Rows the CHECK allows keep flowing"
)


def _drift_why(group, *, is_pg: bool, cdc_only: bool = False) -> str:
    """The banner's "why" for one group: by kind, engine, migration type (CDC only has no
    reload: its target-side remedy is a repair by hand, see the notice below the rows, and its
    Delete-and-Deploy advice names what the delete costs on the engine) and the group's own
    evidence."""
    kind = getattr(group, "kind", "")
    if cdc_only and kind == "unique-conflict":
        return (
            _UNIQUE_CONFLICT_WHY_HELD
            + _fix_engine_sentence(is_pg=is_pg, cdc_only=True)
            + _UNIQUE_CONFLICT_WHY_REST
            + _UNIQUE_CONFLICT_TAIL_CDC_ONLY
        )
    if cdc_only and kind == "unbindable-value":
        return (
            _UNBINDABLE_WHY
            + _UNBINDABLE_RECOVERY_CDC_ONLY
            + _fix_engine_sentence(is_pg=is_pg, cdc_only=True)
            + _UNBINDABLE_REPAIR_CDC_ONLY
        )
    why = _DRIFT_LABELS.get(kind, (kind, "the source schema changed"))[1]
    if kind == "value-does-not-fit":
        codes = set(getattr(group, "codes", None) or ())
        server = codes - {_SINK_VALUE_FIT_CODE}
        if server and _SINK_VALUE_FIT_CODE not in codes:
            return _VALUE_FIT_SERVER_WHY.format(pg=_VALUE_FIT_SERVER_PG if is_pg else "")
        # A MySQL TIME duration refused for a time-of-day column: its own reason, not the
        # boolean / fraction / decimal-places one (and no TINYINT(1) advice).
        if not is_pg and _value_fit_times_only(group):
            return _VALUE_FIT_TIME_WHY + (_VALUE_FIT_SERVER_ALSO if server else "")
        why = _VALUE_FIT_WHY_PG if is_pg else why
        return why + (_VALUE_FIT_SERVER_ALSO if server else "")
    if kind == "drop-column" and getattr(group, "columns", None):
        return _DROP_COLUMN_NOT_SENT_WHY + (_DROP_COLUMN_TOAST_PG if is_pg else "")
    if kind == "check-violation" and is_pg:
        return _CHECK_WHY_PG
    return why


# The dead-letter kinds whose label the source schema re-check (G4) can CONFIRM: a finding of
# the same kind for the same table (and column, when the records name one) upgrades the label
# to the re-check's own ("column dropped at the source", ...). Render-time and label only: the
# kind, the ledger, the gate and the activity marker stay record-level.
_CORROBORATED_KINDS = frozenset({"drop-column", "nullability-change", "key-change"})


def _catalog_confirming_changes(group, migration_state) -> list:
    """The OPEN re-check finding's changes that confirm this group, else ``[]``.

    Reads state only (the throttled background read landed it; no I/O here). A nullability
    finding confirms only a relaxation (NOT NULL -> NULL): a tightening dead-letters nothing.
    """
    kind = getattr(group, "kind", "")
    if kind not in _CORROBORATED_KINDS or migration_state is None:
        return []
    findings = getattr(migration_state, "cdc_catalog_drift_findings", None) or {}
    try:
        finding = findings.get(getattr(group, "table", None))
    except Exception:  # noqa: BLE001 - a label upgrade must never break the banner
        return []
    if finding is None or getattr(finding, "resolved_at", None) is not None:
        return []
    changes = [
        change
        for change in (getattr(finding, "changes", None) or ())
        if getattr(change, "kind", None) == kind
        and (kind != "nullability-change" or getattr(change, "after", None) == "NULL")
    ]
    if not changes:
        return []
    named = set(getattr(group, "columns", None) or ())
    if kind == "key-change" or not named or any(
        getattr(change, "column", None) in named for change in changes
    ):
        return changes
    return []


def _catalog_label(group, migration_state) -> Optional[str]:
    """The re-check's label for this group when an OPEN finding confirms it, else ``None``."""
    if not _catalog_confirming_changes(group, migration_state):
        return None
    from dsql_migrator.ui.cdc_catalog_drift_gate import _LABELS as _CATALOG_LABELS

    return _CATALOG_LABELS.get(getattr(group, "kind", ""))

# Drift kinds that are NOT a source DDL change, so the banner must not call them one.
# ``source-truncate`` is deliberately NOT here: it IS source-side, and this set's header and
# stop-and-reload notice are both about a row going MISSING from the target, which is the
# opposite of what a truncate leaves behind. It gets its own header + runbook line below.
_TARGET_SIDE_DRIFT_KINDS = frozenset(
    {"missing-table", "unique-conflict", "unbindable-value"}
)

# ...and of THOSE, the ones where rows are CERTAINLY being lost right now, not merely at
# risk. `error` rather than `warning` by the design system's own calibration (warning =
# real but non-blocking): an unbindable value loses every write for the table until the
# connector is replaced, and a missing target table loses every event for it. 23505 is
# deliberately NOT here -- there a row only CAN end up missing. ``key-change`` (42P10) loses
# every insert and update too, but is in neither set: its table exists, so it gets its own
# `error` header and the recreate runbook, never the dropped-table notice these sets drive.
_CERTAIN_LOSS_DRIFT_KINDS = frozenset({"missing-table", "unbindable-value"})

# The kinds that are neither a source DDL change nor target-side: a source TRUNCATE (extra
# rows on the target) and a value the sink refused because its column would change it (a
# MySQL TINYINT(1) can hold 2 with no DDL). A banner holding only these -- either or both --
# gets their own header(s) and runbook(s), never the generic "apply the matching change
# (e.g. ALTER TABLE)" one.
_NON_DDL_SOURCE_KINDS = frozenset({"source-truncate", "value-does-not-fit"})

# The kinds whose WHOLE repair is putting the table's rows right, which a CHECKSUM match after
# the table's last dead letter proves done: the target-side kinds (refused rows, restored by a
# reload or a later full-row change) and a source TRUNCATE (rows the source dropped, removed
# by a Drop & reload or a delete -- a match proves the target holds none of them). A
# value-fit or DDL group keeps its banner group once verified: its cause (a column type, a
# schema change) is still there, and no CHECKSUM proves that fixed.
_CHECKSUM_REPAIRED_DRIFT_KINDS = _TARGET_SIDE_DRIFT_KINDS | {"source-truncate"}


# How many names one Fix target schema… sentence lists before it tails off with "+N more".
_FIX_NAME_LIMIT = 6


def _fix_names(names, joiner: str = ", ") -> str:
    """At most :data:`_FIX_NAME_LIMIT` names, then ``(+N more)``."""
    items = [str(n) for n in names]
    shown = joiner.join(items[:_FIX_NAME_LIMIT])
    more = len(items) - _FIX_NAME_LIMIT
    return shown + (f" (+{more} more)" if more > 0 else "")


def _fix_is_cdc_only(migration_type) -> bool:
    return getattr(migration_type, "name", None) == "CDC_ONLY"


# What the dead-letter record list can tell a CDC-only hand repair. The sink renders a key
# value only when it is an integer or a UUID (DsqlSinkTask.isSurrogate); any other, a natural
# key such as an email, is "<withheld>" (Property 7), so such a table's rows cannot be found
# from the list and the banner and the card send that table to Start over.
_DLQ_KEY_VALUES = (
    "names each record's primary key, but shows a key value only when it is an integer or a "
    "UUID (any other is withheld)"
)
# The same, inside a runbook's parenthesis (the card itself then names Start over).
_DLQ_KEY_LISTED = (
    "the dead-letter queue card lists each record's primary key, with its value when that is "
    "an integer or a UUID"
)


def _value_fit_runbook(
    migration_type, names, *, is_pg: bool = False, remap: bool = True
) -> str:
    """The recreate runbook for rows whose column needs another type to hold their values.

    Aurora DSQL has no ``ALTER COLUMN ... TYPE``, so the only way to give a column a type that
    holds the values is to recreate the table -- never an ALTER. ``names`` are the affected
    tables (capped like the Fix sentences): the sink's value-fit refusals, Aurora DSQL's own
    22001/22003 and a type change. ``remap`` False drops the TINYINT(1) clause (no sink
    refusal among them). No engine sentence: nothing here deletes the CDC infrastructure, so
    the replication slot / stream position is kept.
    """
    shown = _fix_names(sorted({str(n) for n in names}))
    # A MySQL TINYINT(1) is the common no-DDL cause; PostgreSQL has no such type.
    remap_clause = (
        ""
        if is_pg or not remap
        else " (for a MySQL TINYINT(1) holding values other than 0 and 1, edit its type to "
        "smallint)"
    )
    if _fix_is_cdc_only(migration_type):
        return (
            "Aurora DSQL cannot change a column's type in place, and CDC only has no Full Load "
            "step to reload from. Stop CDC and give the column a type that holds these values "
            "by hand: add a column of that type, copy the values across (at most 3,000 rows "
            "per transaction), drop the old column and rename the new one (a primary-key "
            "column cannot be dropped: use Start over instead). Then correct the set-aside "
            f"rows on Aurora DSQL ({_DLQ_KEY_LISTED}), "
            f"Start CDC and run Validation (step 4); or use Start over to migrate {shown} "
            "again with that type."
        )
    return (
        "Aurora DSQL cannot change a column's type in place, so recreate the table with a type "
        "that holds these values: let CDC catch up (Stream lag: caught up), Stop CDC, re-run "
        f"Step 1 (Evaluation) if the source column's type changed, re-apply {shown} in Schema "
        "Conversion (Step 2) choosing Replace when asked, with the column given a type that "
        f"holds the values{remap_clause}, reload it on the Full Load step with 'Drop & reload', "
        "then "
        "Start CDC and run Validation (step 4). Until then CDC keeps setting such rows aside, "
        "and Cut over stays held for each table until it matches by CHECKSUM."
    )


def _fix_engine_sentence(*, is_pg: bool, cdc_only: bool) -> str:
    """What deleting and redeploying the CDC infrastructure costs, by engine and type."""
    if is_pg:
        if cdc_only:
            return (
                " On PostgreSQL, deleting the stack drops its replication slot, so the next "
                "Start CDC snapshots the captured tables again."
            )
        return (
            " On PostgreSQL, choose 'Re-snapshot every table instead' in the Start CDC "
            "dialog: deleting the stack drops its replication slot."
        )
    if cdc_only:
        return (
            " On MySQL, deleting the CDC infrastructure also deletes the stream position: "
            "changes made before the new Start are not replicated."
        )
    return (
        " On MySQL, that Start needs a start point newer than this change; the Start CDC "
        "dialog explains the options."
    )


def _fix_refusal_notice(plan, table: str, *, migration_type, is_pg: bool) -> "tuple[str, str]":
    """``(header, body)`` of the ``error`` notice for a table whose record key changed."""
    refusal = plan.refusal
    facts = [f"{old} was renamed to {new}" for old, new in refusal.renamed]
    facts += [f"the source no longer has {k}" for k in refusal.gone]
    facts += [
        f"{k} is now part of the primary key, and Aurora DSQL does not have it"
        for k in refusal.missing
    ]
    if not facts and (refusal.before or refusal.after):

        def _key(columns) -> str:
            return f"({', '.join(columns)})" if columns else "none"

        facts.append(f"the primary key was {_key(refusal.before)}, now {_key(refusal.after)}")
    if len(facts) > _FIX_NAME_LIMIT:
        facts = facts[:_FIX_NAME_LIMIT] + [f"+{len(facts) - _FIX_NAME_LIMIT} more"]
    consequence = (
        "CDC writes and deletes this table's rows by that key, so adding or renaming a "
        "column here would not repair it: every insert and update would still be rejected, "
        "and every delete would silently match no row."
    )
    remedy = _key_change_remedy(
        migration_type, table, is_pg=is_pg, rekeyed=tuple(refusal.rekeyed)
    )
    return (
        f"Fix target schema… cannot repair {table}: its key changed on the source",
        "On the source, " + "; ".join(facts) + ". " + consequence + remedy,
    )


def _key_change_remedy(migration_type, shown: str, *, is_pg: bool, rekeyed) -> str:
    """The recreate remedy for a table whose key changed (a leading space, then sentences).

    Shared by the Fix dialog's key refusal and the dead-letter banner's 42P10 line, so the
    two never disagree. ``shown`` names the table(s). ``rekeyed`` is the table's re-key
    columns (the dialog: those the change touches; the banner: the table's own): non-empty
    adds the Composite-key redeploy clause, an empty tuple adds nothing, ``None`` (not known:
    no state, the deployed stack's key map not read yet, or several tables) adds it
    conditionally.
    """
    if _fix_is_cdc_only(migration_type):
        # Every insert and update was dead-lettered while the key did not match, and a
        # dead-lettered row is never replayed: restoring the key repairs only new writes.
        return (
            " Aurora DSQL cannot change a primary key in place or drop a primary-key column, "
            "and CDC only has no Full Load step to reload from: restore the original key on "
            "the source, or use Start over to migrate the table with its new key. If you "
            "restore the key, the rows set aside meanwhile are not replayed: correct them on "
            f"Aurora DSQL by hand ({_DLQ_KEY_LISTED})."
        )
    remedy = (
        " Aurora DSQL cannot change a primary key in place or drop a primary-key column, "
        "so recreate the table: let CDC catch up (Stream lag: caught up), Stop CDC, "
        f"re-run Step 1 (Evaluation), re-apply {shown} in Schema Conversion (Step 2) "
        "choosing Replace when asked (it recreates the table empty, with the new key), "
        "reload it on the Full Load step with 'Drop & reload', then Start CDC and run "
        "Validation (step 4). Or restore the original key on the source; the rows set "
        "aside meanwhile still need 'Drop & reload'."
    )
    if rekeyed:
        remedy += (
            f" CDC re-keys this table on {_fix_names(rekeyed)} (Schema "
            "Conversion's Composite key), and the deployed CDC infrastructure carries "
            "that key: choose the Composite key again in Schema Conversion, then Delete "
            "CDC infrastructure and deploy it again before Start CDC."
            + _fix_engine_sentence(is_pg=is_pg, cdc_only=False)
        )
    elif rekeyed is None:
        remedy += (
            " A table CDC re-keys (Schema Conversion's Composite key) also needs the "
            "Composite key chosen again in Schema Conversion, then Delete CDC infrastructure "
            "and deploy it again before Start CDC."
            + _fix_engine_sentence(is_pg=is_pg, cdc_only=False)
        )
    return remedy


def _truncate_runbook(migration_type, names: Optional[str] = None) -> str:
    """The repair for a source TRUNCATE (extra rows on the target), by migration type. Pure.

    ``names`` (already capped) names the truncated tables when the banner holds other kinds
    too; a TRUNCATE-only banner says "the table" (its rows name them)."""
    table = names or "the table"
    if _fix_is_cdc_only(migration_type):
        return (
            "CDC cannot replicate a TRUNCATE (Aurora DSQL has no TRUNCATE statement), "
            "and CDC only has no Full Load step to reload from. Stop CDC, delete the "
            f"rows the source no longer has from {table} on Aurora DSQL (at most "
            "3,000 rows per transaction), then Start CDC. Run Validation (step 4) "
            "afterwards: Cut over stays held for each truncated table until it "
            "matches by CHECKSUM."
        )
    return (
        "CDC cannot replicate a TRUNCATE (Aurora DSQL has no TRUNCATE statement). "
        f"Stop CDC, reload {table} on the Full Load step with 'Drop & reload' "
        "(offered only while CDC is stopped; 'Append' only adds missing rows, so "
        "it cannot remove the stale ones), then Start CDC. Run Validation (step 4) "
        "afterwards: Cut over stays held for each truncated table until it "
        "matches by CHECKSUM."
    )


def _banner_rekeyed(tables, migration_state):
    """``rekeyed`` for the banner's key-change remedy (see :func:`_key_change_remedy`).

    ``()`` when the state proves no listed table is re-keyed, the one table's re-key columns
    when it is, else ``None`` (no state, the deployed stack's key map not read yet -- a key
    removed after the deploy is still on the stack -- or several tables, some re-keyed)."""
    if migration_state is None or (
        getattr(migration_state, "cdc_deployed_message_key_columns", None) is None
    ):
        return None
    from dsql_migrator.ui.cdc_catalog_drift_gate import cdc_rekey_columns

    rekey = cdc_rekey_columns(migration_state)  # CPU only; never raises
    hit = [t for t in tables if rekey.get(t)]
    if not hit:
        return ()
    if len(tables) == 1:
        return tuple(sorted(rekey[hit[0]]))
    return None


def _ddl_runbook(drift, migration_type, *, is_pg: bool, migration_state=None) -> str:
    """The banner's runbook when at least one kind is a source schema change. CPU only.

    One lead, then one sentence per kind present, each a repair Aurora DSQL supports: no
    ``ALTER COLUMN ... TYPE`` (a type change and a value its column cannot hold share the
    recreate runbook), no ``SET NOT NULL``, no primary-key change (the recreate remedy), a
    CHECK only re-added ``NOT VALID``, ``DROP NOT NULL`` keeps the values, and ``DROP
    COLUMN`` must come before a reload (Drop & reload refuses while the target still has the
    column). A source TRUNCATE beside them keeps its own repair (no DDL lead removes extra
    rows, and its dead letter names no row to correct by hand). CDC only has no Full Load
    step, so it never names 'Drop & reload'. ``migration_state`` (read only) tells whether a
    key-change table is re-keyed (:func:`_banner_rekeyed`).
    """
    by_kind: dict[str, set] = {}
    for group in drift:
        by_kind.setdefault(getattr(group, "kind", ""), set()).add(str(group.table))
    cdc_only = _fix_is_cdc_only(migration_type)
    # A target-side kind (a 23505, a dropped target table, an unbindable value) is no source
    # DDL: its repair is the stop-and-reload notice the banner renders below, so on its own
    # it gets no DDL lead -- "apply the matching change (e.g. ALTER TABLE)" would send the
    # operator after a schema change that never happened.
    ddl = set(by_kind) - _NON_DDL_SOURCE_KINDS - _TARGET_SIDE_DRIFT_KINDS

    def names(kind: str) -> str:
        return _fix_names(sorted(by_kind[kind]))

    # A GENERATED target column is no source change: its own sentence below is the repair.
    lead = ddl - {"generated-column"}
    if not lead:
        parts = []
    elif lead <= {"type-change", "key-change"}:
        # Neither can be "applied" with an ALTER on Aurora DSQL: their own sentences recreate.
        parts = ["CDC does not replicate DDL."]
    elif cdc_only:
        parts = [
            "CDC does not replicate DDL. Apply the matching change to the target schema (e.g. "
            "ALTER TABLE) to stop new rows being set aside. The rows already set aside are not "
            "replayed, and CDC only has no Full Load step to reload them from: correct them on "
            f"Aurora DSQL by hand ({_DLQ_KEY_LISTED}), "
            "re-run Step 1 (Evaluation) so Validation compares the new definition, then run "
            "Validation (step 4)."
        ]
    else:
        parts = [
            "CDC does not replicate DDL. Apply the matching change to the target "
            "schema (e.g. ALTER TABLE) to stop new rows being set aside — then "
            "backfill the ones already set aside: re-run Step 1 (Evaluation) so the "
            "Full Load sees the changed column list, stop CDC, and reload the table "
            "with 'Drop & reload'. A plain append reload cannot backfill them: it "
            "only inserts primary keys the target is missing, so a row already there "
            "keeps its old values. Run Validation (step 4) afterwards to confirm."
        ]
    drop_not_null = (
        "run ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL on Aurora DSQL: it keeps the "
        "stored values, and Aurora DSQL cannot make the column NOT NULL again."
    )
    if "drop-column" in by_kind:
        toast = ", or an update left its large (TOASTed) value unchanged" if is_pg else ""
        if cdc_only:
            drop = (
                "drop it on Aurora DSQL too (ALTER TABLE ... DROP COLUMN ...; a primary-key "
                "column cannot be dropped: use Start over for that table)."
            )
        else:
            drop = (
                "drop it on Aurora DSQL too (ALTER TABLE ... DROP COLUMN ...) before you "
                "reload: 'Drop & reload' refuses while the target still has a column the new "
                "definition does not, and a primary-key column cannot be dropped (re-apply "
                "that table in Schema Conversion with Replace instead)."
            )
        parts.append(
            f"For {names('drop-column')} (a NOT NULL column got no value): if the source "
            f"dropped that column, {drop} Otherwise (the source column now allows NULL, "
            f"your exclusions keep it out{toast}, or it is a generated column CDC does not "
            f"carry -- Step 1 names those), {drop_not_null}"
        )
    if "nullability-change" in by_kind:
        parts.append(
            f"For {names('nullability-change')} (a NULL sent for a NOT NULL column): if the "
            f"source column now allows NULL, {drop_not_null} If the source column is still "
            "NOT NULL, CDC could not convert that row's value: check the row on the source "
            "(the dead-letter queue card lists its primary key)."
        )
    if "check-violation" in by_kind:
        check = (
            f"For {names('check-violation')} (a CHECK constraint rejected the row): if the "
            "source now allows more values, replace that CHECK on Aurora DSQL, one statement "
            "per transaction: ALTER TABLE ... DROP CONSTRAINT with the constraint the "
            "dead-letter reason names, then ALTER TABLE ... ADD CONSTRAINT with the same name "
            "and CHECK (column IN (...every value the source now allows...)) NOT VALID. Aurora "
            "DSQL adds a CHECK only as NOT VALID; it still checks every new write."
        )
        if not cdc_only:
            check += (
                " The 'Drop & reload' backfill recreates it from the re-run Step 1 with the "
                "new values."
            )
        parts.append(check)
    if "generated-column" in by_kind:
        generated = (
            f"For {names('generated-column')} (a GENERATED column was sent a value): if the "
            "column is meant to be GENERATED, as Schema Conversion has it, Stop CDC and Start "
            "CDC again -- the capture is then updated to leave it out; otherwise make it an "
            "ordinary column on Aurora DSQL with ALTER TABLE ... ALTER COLUMN ... DROP "
            "EXPRESSION (it keeps the stored values). Either way new changes apply again."
        )
        if not cdc_only:
            generated += (
                " Then backfill the rows already set aside: stop CDC and reload the table with "
                "'Drop & reload'. If you made the column ordinary, first remove GENERATED from "
                "its target DDL in Schema Conversion as well, or the reload recreates it "
                "GENERATED; if it stays GENERATED, reload it as it is."
            )
        else:
            generated += (
                " The inserts and updates set aside meanwhile are not replayed: correct them on "
                f"Aurora DSQL by hand ({_DLQ_KEY_LISTED}), then run Validation (step 4)."
            )
        parts.append(generated)
    if "key-change" in by_kind:
        rekeyed = _banner_rekeyed(sorted(by_kind["key-change"]), migration_state)
        parts.append(
            f"For {names('key-change')}:"
            + _key_change_remedy(
                migration_type, names("key-change"), is_pg=is_pg, rekeyed=rekeyed
            )
        )
    recreate = sorted(by_kind.get("type-change", set()) | by_kind.get("value-does-not-fit", set()))
    if recreate:
        parts.append(
            _value_fit_runbook(migration_type, recreate, is_pg=is_pg, remap=_value_fit_remap(drift))
        )
    if "source-truncate" in by_kind:
        parts.append(_truncate_runbook(migration_type, names("source-truncate")))
    return " ".join(parts)


def _value_fit_remap(drift) -> bool:
    """Whether the recreate runbook gives the TINYINT(1) -> smallint advice.

    Only for the sink's own value-fit refusal (a boolean target holding 2), or a group whose
    codes are unknown; a type change or Aurora DSQL's 22001/22003 alone never needs it."""
    return any(
        not getattr(g, "codes", None) or _SINK_VALUE_FIT_CODE in g.codes
        for g in drift
        if getattr(g, "kind", "") == "value-does-not-fit" and not _value_fit_times_only(g)
    )


_FIX_RENAME_EVIDENCE = {
    "inventory": (
        "The Step 1 (Evaluation) inventory has {old} at this position with the same type."
    ),
    "re-check": "The source-schema re-check saw this rename.",
    "case": (
        "Aurora DSQL has {old}, which differs from {new} only in letter case, and the source "
        "no longer has it; CDC writes column names exactly."
    ),
}


def _fix_rename_notice(step, table: str, *, source_type) -> "tuple[str, str]":
    """``(header, body)`` of the ``info`` notice explaining one RENAME statement.

    Inventory and re-check evidence pair a gone column with a new one at the same position
    and type, and a dropped column plus an added one of the same type looks exactly like
    that when the added column lands where the dropped one was (an added column goes at
    the end of the table): the notice says "looks renamed" and how to add the column
    instead. A case-only pair is stated as a rename."""
    from dsql_migrator.core.cdc_schema_evolve import EVIDENCE_CASE, plan_add_columns

    old, new = step.old, step.column
    evidence = _FIX_RENAME_EVIDENCE.get(step.evidence, "").format(old=old, new=new)
    caveat = ""
    if step.evidence != EVIDENCE_CASE:
        adds = plan_add_columns(
            table, [(new, step.source_type)], [], source_type=source_type
        ).steps
        manual = adds[0].ddl if adds else "ALTER TABLE … ADD COLUMN, nullable, with no default"
        caveat = (
            f" The source catalog cannot tell this from {old} dropped and {new} added with "
            "the same type (an added column goes at the end of the table, where a dropped "
            f"last column was). If that is what the source did, Cancel and add {new} by hand "
            f"instead ({manual}): renaming would show the values of {old} as {new}."
        )
    header = (
        f"{old} was renamed to {new} on the source"
        if step.evidence == EVIDENCE_CASE
        else f"{old} looks renamed to {new} on the source"
    )
    return (
        header,
        evidence
        + caveat
        + f" Renaming keeps the values already in the column; adding {new} instead would "
        f"leave them in {old} and the new column empty. Apply it once Stream lag shows caught "
        f"up: a change made on the source before the rename still carries {old}, and one "
        "still queued when this runs is set aside (the backfill below repairs it). Then "
        "re-run Step 1 (Evaluation) so the source-schema re-check and Validation compare the "
        "new name (until then the re-check still reports the rename and Cut over stays "
        "held), and if you edited this table's DDL in Schema Conversion, rename the column "
        "there too.",
    )


def _fix_held_notices(
    plan, table: str, *, migration_type, is_pg: bool
) -> "list[tuple[str, str, Optional[str]]]":
    """``[(header, body, code)]`` of the ``warning`` notices for the held columns, in plan
    order. The new rule-matched, the excluded-gone and the rules-unknown columns collapse
    into one notice each (one remedy covers them all). ``code`` is the statements to run by
    hand, one per line with its ``;`` (Aurora DSQL runs one DDL per transaction), or
    ``None``."""
    from dsql_migrator.core.cdc_schema_evolve import (
        HELD_EXCLUDED_GONE,
        HELD_GENERATED,
        HELD_NAME_TAKEN,
        HELD_RENAMED_EXCLUDED,
        HELD_RULE,
        HELD_RULES_UNKNOWN,
        _quote_ident,
        _quote_qualified,
    )

    cdc_only = _fix_is_cdc_only(migration_type)
    engine = _fix_engine_sentence(is_pg=is_pg, cdc_only=cdc_only)
    qt = _quote_qualified(table)

    def _paren(what: str) -> str:
        """Full load + CDC: what Change & reload does with the column(s)."""
        return "" if cdc_only else f" (Change & reload: {what})"

    def _drop_first(columns, why: str) -> str:
        """Full load + CDC: drop the stale target column(s) before Change & reload, which
        refuses to recreate a table that has a column the refreshed definition lacks."""
        if cdc_only or not columns:
            return ""
        how = (
            f"ALTER TABLE {qt} DROP COLUMN {_quote_ident(columns[0])}"
            if len(columns) == 1
            else f"one ALTER TABLE {qt} DROP COLUMN statement each"
        )
        return (
            f", drop {_fix_names(columns)} on Aurora DSQL by hand ({how}; {why}, and Change "
            "& reload refuses to destroy a column the refreshed definition no longer has)"
        )

    out: list[tuple[str, str, Optional[str]]] = []
    grouped: set[str] = set()
    for held in plan.held:
        c, old = held.column, held.old
        if held.reason == HELD_RULE and not old:
            if HELD_RULE in grouped:
                continue
            grouped.add(HELD_RULE)
            group = [h for h in plan.held if h.reason == HELD_RULE and not h.old]
            if len(group) == 1:
                lead = (
                    f"Your saved rule '{held.rule}' matches {c}"
                    if held.rule
                    else f"A saved exclusion rule matches {c}"
                )
                out.append((
                    f"{c} is not added: your exclusion rules match it",
                    lead + ", which the source added after the rules were saved. CDC's "
                    "capture list holds the column names fixed when the rules were saved, so "
                    f"CDC sends {c}, and every insert and update to {table} that carries it is "
                    "dead-lettered until the list excludes it. Adding it here would copy the "
                    "data the rule keeps out. To keep it out: Stop CDC, Delete CDC "
                    f"infrastructure, re-run Step 1 (Evaluation) so the rule sees {c}, then "
                    "save the rules again as the Exclude tables & columns popup shows"
                    + _paren(
                        f"it creates {c} on Aurora DSQL and leaves it NULL, like every "
                        "excluded column"
                    )
                    + ", deploy CDC again and Start CDC." + engine
                    + f" To copy {c} after all, change the rule in the same steps so it no "
                    "longer matches it.",
                    None,
                ))
                continue
            names = _fix_names([h.column for h in group])
            rules = sorted({str(h.rule) for h in group if h.rule})
            quoted = f" ({_fix_names([f"'{r}'" for r in rules])})" if rules else ""
            out.append((
                f"Not added: your exclusion rules match {names}",
                f"Your saved exclusion rules{quoted} match {names}, which the source added "
                "after the rules were saved. CDC's capture list holds the column names fixed "
                f"when the rules were saved, so CDC sends them, and every insert and update to "
                f"{table} that carries one is dead-lettered until the list excludes them. "
                "Adding them here would copy the data the rules keep out. To keep them out: "
                "Stop CDC, Delete CDC infrastructure, re-run Step 1 (Evaluation) so the rules "
                "see them, then save the rules again as the Exclude tables & columns popup "
                "shows"
                + _paren(
                    "it creates them on Aurora DSQL and leaves them NULL, like every excluded "
                    "column"
                )
                + ", deploy CDC again and Start CDC." + engine
                + " To copy one of them after all, change the rule in the same steps so it no "
                "longer matches that column.",
                None,
            ))
        elif held.reason == HELD_RULE:
            lead = (
                f"Your saved rule '{held.rule}' matches {c}"
                if held.rule
                else f"A saved exclusion rule matches {c}"
            )
            out.append((
                f"{old} is not renamed to {c}: your exclusion rules match {c}",
                lead + f", which the source renamed from {old} after the rules were saved. "
                "CDC's capture list holds the column names fixed when the rules were saved, so "
                f"CDC sends {c}, and every insert and update to {table} that carries it is "
                "dead-lettered until the list excludes it. Adding it here would copy the data "
                "the rule keeps out. To keep it out: Stop CDC, Delete CDC infrastructure"
                + _drop_first([old], "the values it holds are the ones the rule keeps out")
                + f", re-run Step 1 (Evaluation) so the rule sees {c}, then save the rules "
                "again as the Exclude tables & columns popup shows"
                + _paren(
                    f"it creates {c} on Aurora DSQL and leaves it NULL, like every excluded "
                    "column"
                )
                + ", deploy CDC again and Start CDC." + engine
                + f" To copy {c} after all, change the rule in the same steps so it no longer "
                "matches it" + ("" if cdc_only else f", and do not drop {old}") + ".",
                None,
            ))
        elif held.reason == HELD_RENAMED_EXCLUDED:
            out.append((
                f"{c} is not added: it is the excluded column {old}, renamed on the source",
                f"You excluded {old}, and the source renamed it to {c}. CDC's capture list "
                f"names {old}, so CDC now sends {c} with its values, and every insert and "
                f"update to {table} that carries it is dead-lettered until the list excludes "
                f"{c}. Adding or renaming the column here would copy the values you excluded "
                "into Aurora DSQL. To keep it out: Stop CDC, Delete CDC infrastructure"
                + _drop_first([old], "it holds only NULLs")
                + f", re-run Step 1 (Evaluation), change the rule that excludes {old} so it "
                f"matches {c}, save the rules as the Exclude tables & columns popup shows"
                + _paren(
                    f"it creates {c} on Aurora DSQL and leaves it NULL, like every excluded "
                    "column"
                )
                + ", deploy CDC again and Start CDC." + engine
                + " Or rename the column back on the source.",
                None,
            ))
        elif held.reason == HELD_EXCLUDED_GONE:
            if HELD_EXCLUDED_GONE in grouped:
                continue
            grouped.add(HELD_EXCLUDED_GONE)
            group = [h for h in plan.held if h.reason == HELD_EXCLUDED_GONE]
            others = list(held.others)
            one = len(others) == 1
            header = (
                f"Not added: the excluded column {others[0]} is no longer on the source"
                if one
                else "Not added: the excluded columns "
                f"{_fix_names(others)} are no longer on the source"
            )
            ddls = [h.ddl for h in group if h.ddl]
            out.append((
                header,
                f"If the source renamed {'it' if one else 'one of them'} to "
                f"{_fix_names([h.column for h in group], ' or ')}, adding that column would "
                "copy the values you excluded into Aurora DSQL, and the tool cannot tell a "
                "rename from a drop here. If it was renamed, keep it out: Stop CDC, Delete CDC "
                "infrastructure"
                + _drop_first(others, "it holds only NULLs" if one else "they hold only NULLs")
                + ", re-run Step 1 (Evaluation), change your exclusion rule so it matches the "
                "new name, save the rules as the Exclude tables & columns popup shows"
                + _paren("it creates the column on Aurora DSQL and leaves it NULL")
                + ", deploy CDC again and Start CDC." + engine
                + (
                    " If it was dropped, run the statement below on Aurora DSQL yourself."
                    if len(ddls) == 1
                    else " If it was dropped, run each statement below on Aurora DSQL "
                    "yourself, on its own: Aurora DSQL allows one DDL per transaction."
                ),
                "\n".join(f"{ddl};" for ddl in ddls) or None,
            ))
        elif held.reason == HELD_NAME_TAKEN:
            qold = _quote_ident(old or "")
            lead = (
                f"The source renamed {old} to {c}, and Aurora DSQL has both ({c} may be the "
                "empty column an earlier Fix target schema… added)."
            )
            if cdc_only:
                body = lead + (
                    " CDC only has no Full Load step to reload from: copy the values from "
                    f"{old} into {c} on Aurora DSQL yourself (at most 3,000 rows per "
                    f"transaction), drop {old}, then re-run Step 1 (Evaluation)."
                )
            else:
                body = lead + (
                    f" To realign the table: drop {old} on Aurora DSQL by hand (ALTER TABLE "
                    f"{qt} DROP COLUMN {qold}), re-run Step 1 (Evaluation), Stop CDC, reload "
                    f"the table with 'Drop & reload' (it copies {c} for every row), then Start "
                    "CDC."
                )
            out.append((f"{old} is not renamed to {c}: Aurora DSQL already has {c}", body, None))
        elif held.reason == HELD_GENERATED:
            out.append((
                f"{c} is not added: the source computes it",
                "It is a generated column on the source, and CDC may not send its values, so "
                "a plain column added here could stay empty. If you need it on Aurora DSQL, "
                "add it by hand (ALTER TABLE … ADD COLUMN, nullable, with no default).",
                None,
            ))
        elif held.reason == HELD_RULES_UNKNOWN:
            if HELD_RULES_UNKNOWN in grouped:
                continue
            grouped.add(HELD_RULES_UNKNOWN)
            group = [h for h in plan.held if h.reason == HELD_RULES_UNKNOWN]
            names = _fix_names(
                [f"{h.column} (renamed from {h.old})" if h.old else h.column for h in group]
            )
            backfill = (
                "correct the rows already set aside on Aurora DSQL by hand (CDC only has no "
                "Full Load step to reload from)"
                if cdc_only
                else "backfill as for any added column: re-run Step 1 (Evaluation), Stop CDC, "
                "reload the table with 'Drop & reload', Start CDC"
            )
            out.append((
                "Nothing is added or renamed: the exclusion rules could not be checked",
                "Fix target schema… could not check this migration's saved exclusion rules "
                f"against {table}, and must not add or rename a column they match, so it "
                f"holds {names}. Trying again gives the same result (the app log has the "
                "error). Open Exclude tables & columns and check whether a saved rule matches "
                "these columns. If none does, change Aurora DSQL by hand, one statement per "
                "transaction (ALTER TABLE … ADD COLUMN, nullable, with no default; or ALTER "
                "TABLE … RENAME COLUMN old TO new for a renamed column), then " + backfill
                + ". If one does, keep it out: Stop CDC, Delete CDC infrastructure, re-run "
                "Step 1 (Evaluation), save the rules again as the popup shows, deploy CDC "
                "again and Start CDC." + engine,
                None,
            ))
    return out


def _fix_orphan_notice(plan, table: str) -> "Optional[tuple[str, str]]":
    """``(header, body)`` of the ``warning`` beside an ADD: target columns the source lacks
    (one may be the old name of a rename the tool could not confirm)."""
    from dsql_migrator.core.cdc_schema_evolve import (
        AddColumnStep,
        _quote_ident,
        _quote_qualified,
    )

    adds = [s for s in plan.steps if isinstance(s, AddColumnStep)]
    orphans = list(plan.orphans)
    if not orphans or not adds:
        return None
    one = len(orphans) == 1
    header = (
        f"Aurora DSQL also has {orphans[0]}, which the source table does not have"
        if one
        else f"Aurora DSQL also has columns the source table does not have: {_fix_names(orphans)}"
    )
    manual = ""
    if one and len(adds) == 1:
        manual = (
            f" (ALTER TABLE {_quote_qualified(table)} RENAME COLUMN "
            f"{_quote_ident(orphans[0])} TO {_quote_ident(adds[0].column)})"
        )
    return (
        header,
        f"If the source renamed {'it' if one else 'one of them'} to a column above, do not "
        "apply this: adding the new name leaves every existing row's value in the old column "
        "and the new one empty. Rename it on Aurora DSQL by hand instead" + manual + ". The "
        f"tool could not confirm a rename: the Step 1 (Evaluation) inventory does not have "
        f"{'it' if one else 'them'} at that position with that type, and the source-schema "
        f"re-check did not report one. If {'it was' if one else 'they were'} dropped on the "
        f"source, or {'is' if one else 'are'} meant to exist on Aurora DSQL only, apply as "
        "shown.",
    )


# The ADD runbook (unchanged since ADD COLUMN-only Fix): measured, a plain per-table Reload
# repairs NEITHER half of this. Its SELECT list comes from the Step 1 inventory (so the new
# column is not even read), and it appends with SKIP_EXISTING (so a row already on the
# target is never rewritten). Same shape as the oversized-value runbook in
# assessment_strategist.
_FIX_RESUMES = (
    "CDC resumes applying new changes for this table on its own, but the "
    "rows already dead-lettered are NOT replayed"
)
_FIX_ADD_STEPS = (
    ". Backfilling them takes "
    "four steps, in this order: (1) re-run Step 1 (Evaluation) — the Full "
    "Load reads the column list captured there, which predates this source "
    "change, so without a refresh it would not copy the new column at all; "
    "(2) stop CDC; (3) reload this table on the Full Load step choosing "
    "'Drop & reload' — a plain append reload only inserts primary keys the "
    "target is missing, so it cannot fill the new column on rows that are "
    "already there; (4) start CDC again. Then run Validation (step 4) to "
    "confirm the table matches."
)
_FIX_ADD_RUNBOOK = _FIX_RESUMES + _FIX_ADD_STEPS


def _fix_still_sent(plan) -> "list[str]":
    """The held columns CDC still sends and Aurora DSQL still lacks after this plan: every
    insert and update that carries one stays dead-lettered whatever the plan applies."""
    from dsql_migrator.core.cdc_schema_evolve import (
        HELD_EXCLUDED_GONE,
        HELD_RENAMED_EXCLUDED,
        HELD_RULE,
    )

    sent = (HELD_RULE, HELD_RENAMED_EXCLUDED, HELD_EXCLUDED_GONE)
    return [h.column for h in plan.held if h.reason in sent]


def _fix_runbook(plan, *, migration_type) -> Optional[str]:
    """The ``info`` backfill runbook for the plan, or ``None`` (a refusal, or held only)."""
    from dsql_migrator.core.cdc_schema_evolve import AddColumnStep, RenameColumnStep

    if plan.refusal is not None:
        return None
    still_sent = _fix_still_sent(plan) if plan.steps else []
    lead = _FIX_RESUMES
    if still_sent:
        lead = (
            f"Inserts and updates to this table that carry {_fix_names(still_sent)} are still "
            "set aside after this (the warning above says why and what to do): applying it "
            "only stops rows failing on the columns it adds or renames. The rows already "
            "dead-lettered are NOT replayed"
        )
    has_rename = any(isinstance(s, RenameColumnStep) for s in plan.steps)
    if has_rename and _fix_is_cdc_only(migration_type):
        # Before the ADD branch: CDC only has no Full Load step, whatever else is added.
        return lead + (
            ", and CDC only has no Full Load step to reload them from: correct them on "
            "Aurora DSQL by hand, re-run Step 1 (Evaluation) so Validation compares the new "
            "name, then run Validation (step 4) to confirm the table matches."
        )
    if any(isinstance(s, AddColumnStep) for s in plan.steps) or (
        not plan.steps and not plan.held
    ):
        return lead + _FIX_ADD_STEPS
    if has_rename:
        return lead + (
            ". Backfilling them takes four steps, in this order: (1) re-run Step 1 "
            "(Evaluation), so the Full Load reads the new column name; (2) stop CDC; (3) "
            "reload this table on the Full Load step choosing 'Drop & reload' — a plain "
            "append reload only inserts primary keys the target is missing, so it cannot "
            "repair rows that are already there; (4) start CDC again. Then run Validation "
            "(step 4) to confirm the table matches."
        )
    return None


class _FixInputs(NamedTuple):
    """What the Fix plan needs from the migration, gathered on the loop (CPU only)."""

    baseline: object = None  # the Step 1 inventory's TableDef, or None
    excluded: tuple = ()  # effective stored exclusions
    rules_known: bool = False  # False: hold every add and rename (fail closed)
    rules_text: Optional[str] = None  # the SAVED rule text (never the pending lines)
    rekey: tuple = ()  # current + deployed message.key.columns
    seen_renames: tuple = ()  # ((old, new, old type), ...) the re-check recorded
    migration_type: object = None


def _fix_inputs(migration_state, table: str, session=None) -> _FixInputs:
    """The Fix plan's inputs for ``table`` from ``migration_state``. CPU only; never raises.

    Without a migration state (or on any error) the rules count as unknown, so the plan
    adds and renames nothing; the key refusal still runs from the live source and target
    primary keys."""
    migration_type = getattr(migration_state, "migration_type", None) or getattr(
        session, "migration_type", None
    )
    if migration_state is None:
        return _FixInputs(migration_type=migration_type)
    try:
        from dsql_migrator.ui.cdc_catalog_drift_gate import _raw_baseline, cdc_rekey_columns
        from dsql_migrator.ui.data_migration._models import effective_lob_exclusions

        inventory = getattr(_raw_baseline(migration_state), "inventory", None)
        baseline = next(
            (t for t in (getattr(inventory, "tables", None) or ()) if t.name == table), None
        )
        try:
            effective = effective_lob_exclusions(migration_state, inventory) or {}
        except Exception:  # noqa: BLE001 - the stored set (never fewer exclusions)
            effective = migration_state.lob_exclusions() or {}
        rules = getattr(migration_state, "exclusion_rules_text", None)
        finding = (getattr(migration_state, "cdc_catalog_drift_findings", None) or {}).get(table)
        seen = tuple(
            (str(c.column), str(c.after), str(c.before or ""))
            for c in (getattr(finding, "changes", None) or ())
            if getattr(c, "kind", "") == "rename-column" and c.column and c.after
        )
        return _FixInputs(
            baseline=baseline,
            excluded=tuple(sorted(str(c) for c in (effective.get(table) or ()))),
            rules_known=callable(rules),
            rules_text=rules() if callable(rules) else None,
            rekey=tuple(sorted(cdc_rekey_columns(migration_state).get(table, ()))),
            seen_renames=seen,
            migration_type=migration_type,
        )
    except Exception:  # noqa: BLE001 - fail closed: nothing is added or renamed
        _LOGGER.warning("Fix target schema inputs could not be read", exc_info=True)
        return _FixInputs(migration_type=migration_type)


async def _open_add_column_dialog(
    ui, session, table: str, on_refresh=None, on_applied=None, migration_state=None
) -> None:
    """Offer the operator the exact DDL that realigns the target with a source ADD / RENAME
    COLUMN -- or explain why it must not be changed here.

    Both entry points (the dead-letter banner's and the source-schema re-check's Fix target
    schema…) open this, and it uses the ONE plan the re-check's ``fixable`` comes from
    (``cdc_schema_evolve.plan_target_fix``): it reads the source table's live definition
    (one bounded read-only catalog statement, then the engine is disposed) and the target's
    column names and primary key, plans, shows the statements verbatim for approval, and
    only then applies them -- one DDL per transaction, as Aurora DSQL requires. A table whose
    record key changed is refused (``error``) with the recreate steps; a column the
    exclusions keep out, a generated one, or a rename whose new name is taken is held with
    what to do (``warning``). Without ``migration_state`` the saved rules cannot be checked,
    so nothing is added or renamed.

    The reads and the apply are blocking I/O, so each is offloaded with ``run.io_bound``;
    the render path itself stays I/O-free.

    ``on_applied(table, {column: source catalog type})`` (optional) is told the columns
    that were ADDED -- never a failed, skipped or renamed one (a recorded rename would read
    as a dropped old column) -- before ``on_refresh``, so the source-schema re-check counts
    them as part of its baseline.
    """
    from nicegui import run

    from dsql_migrator.core import target_introspector
    from dsql_migrator.core.cdc_schema_evolve import (
        AddColumnStep,
        RenameColumnStep,
        apply_add_columns,
        plan_target_fix,
        read_source_table,
        read_target_columns,
    )
    from dsql_migrator.core.exclusion_rules import rule_matched_columns
    from dsql_migrator.core.target_connection import DsqlConnector
    from dsql_migrator.ui.connect import make_source_engine_factory
    from dsql_migrator.ui.data_migration._cdc_status import (
        _SOURCE_CATALOG_READ_TIMEOUT_SECONDS,
    )

    source_config = getattr(session, "source_config", None)
    target_config = getattr(session, "target_config", None)
    if source_config is None or target_config is None:
        ui.notify(  # type: ignore[attr-defined]
            "Connect the source and target first (Step 1).", type="warning"
        )
        return

    connector = DsqlConnector(
        target_config, aws_profile=getattr(session, "aws_profile", None)
    )
    inputs = _fix_inputs(migration_state, table, session)
    source_type = source_config.source_type
    is_pg = source_type is SourceType.POSTGRES

    def _build_plan():
        engine = make_source_engine_factory(
            getattr(session, "source_password", None),
            read_timeout_seconds=_SOURCE_CATALOG_READ_TIMEOUT_SECONDS,
        )(source_config)
        try:
            with engine.connect() as connection:
                live = read_source_table(connection, table, source_type=source_type)
        finally:
            engine.dispose()
        if live is None or not live.columns:
            # An empty source read is NOT "the target is already current" -- the
            # drifting table demonstrably exists on the source (it is dead-lettering
            # rows). Reading zero columns means the name did not resolve (renamed /
            # dropped on the source, or a topic->table mapping mismatch), so report
            # that explicitly rather than let the empty diff read as "nothing to do".
            raise ValueError(
                f"could not read source columns for '{table}' -- the table may have "
                "been renamed or dropped on the source, or its CDC topic maps to a "
                "different name. Verify the source table before adding columns."
            )
        target = connector.connect()
        try:
            target_columns = read_target_columns(target, table)
        finally:
            target.close()
        target_pk = target_introspector.target_primary_key_columns(
            table, connection_factory=connector.connect
        )
        rule_matches = None
        if inputs.rules_known:
            try:
                rule_matches = rule_matched_columns(
                    inputs.rules_text,
                    table,
                    [(c.name, c.type) for c in live.columns],
                    sorted(live.primary_key),
                    source_type=source_type,
                )
            except Exception:  # noqa: BLE001 - fail closed: hold every add and rename
                _LOGGER.warning(
                    "exclusion rules could not be checked for Fix target schema",
                    exc_info=True,
                )
        return plan_target_fix(
            table,
            live,
            target_columns,
            baseline=inputs.baseline,
            seen_renames=inputs.seen_renames,
            record_key=target_pk or (),
            rekey_columns=inputs.rekey,
            excluded=inputs.excluded,
            rule_matches=rule_matches,
            source_type=source_type,
        )

    try:
        plan = await run.io_bound(_build_plan)
    except Exception as exc:  # noqa: BLE001 - surfaced, never crashes the monitor
        ui.notify(f"Could not read the schemas: {exc}", type="negative")  # type: ignore[attr-defined]
        return

    if not plan.steps and not plan.skipped and not plan.held and plan.refusal is None:
        # The target already matches: the DLQ rows are still set aside, so point at
        # the backfill rather than implying there is nothing left to do.
        ui.notify(  # type: ignore[attr-defined]
            f"{table}: the target already has every source column. To backfill the "
            "dead-lettered rows: re-run Step 1 (Evaluation), stop CDC, then reload this "
            "table choosing 'Drop & reload'.",
            type="info",
        )
        return

    migration_type = inputs.migration_type
    renames = [s for s in plan.steps if isinstance(s, RenameColumnStep)]
    adds = [s for s in plan.steps if isinstance(s, AddColumnStep)]
    # "Add missing columns" only when the dialog adds one; a rename, a refusal or a plan
    # that holds every column is titled after the button that opened it.
    title = (
        f"Add missing columns to {table}"
        if adds and not renames and plan.refusal is None
        else f"Fix target schema: {table}"
    )
    with ui.dialog() as dialog, ui.card().classes("gap-2").style("min-width: 560px"):  # type: ignore[attr-defined]
        section_header(ui, icon="schema", title=title)
        if plan.refusal is not None:
            header, body = _fix_refusal_notice(
                plan, table, migration_type=migration_type, is_pg=is_pg
            )
            render_notice(ui, tone="error", header=header, body=body)
            # No statement, no runbook, no Apply: only Cancel.
            with ui.row().classes("w-full justify-end gap-2"):  # type: ignore[attr-defined]
                ui.button("Cancel", on_click=dialog.close).props("flat no-caps")  # type: ignore[attr-defined]
        else:
            if plan.steps:
                if renames:
                    intro = (
                        "These statements will run on the target, one per transaction. A renamed "
                        "column keeps its values, and new change events write to its new name."
                    )
                    if adds:
                        intro += (
                            " Each added column is NULLable with no default: existing rows read "
                            "NULL until you backfill them, and new change events carry the real "
                            "value."
                        )
                else:
                    intro = (
                        "These statements will run on the target, one per transaction. Each "
                        "column is added NULLable with no default: existing rows read NULL "
                        "until you backfill them, and new change events carry the real value."
                    )
                ui.label(intro).classes("text-xs text-gray-700")  # type: ignore[attr-defined]
                ui.code(plan.ddl_text, language="sql").classes("w-full text-xs")  # type: ignore[attr-defined]
                for step in adds:
                    if step.warning:
                        render_notice(
                            ui,
                            tone="warning",
                            header=f"{step.column} ({step.source_type})",
                            body=step.warning,
                        )
                for step in renames:
                    header, body = _fix_rename_notice(step, table, source_type=source_type)
                    render_notice(ui, tone="info", header=header, body=body)
            for header, body, code in _fix_held_notices(
                plan, table, migration_type=migration_type, is_pg=is_pg
            ):
                render_notice(ui, tone="warning", header=header, body=body)
                if code:
                    ui.code(code, language="sql").classes("w-full text-xs")  # type: ignore[attr-defined]
            for skipped in plan.skipped:
                render_notice(
                    ui,
                    tone="warning",
                    header=f"{skipped.column} is not included",
                    body=(
                        f"source type {skipped.source_type}: {skipped.reason}. Add this "
                        "column by hand if you need it."
                    ),
                )
            orphan = _fix_orphan_notice(plan, table)
            if orphan is not None:
                render_notice(ui, tone="warning", header=orphan[0], body=orphan[1])
            runbook = _fix_runbook(plan, migration_type=migration_type)
            if runbook is not None:
                render_notice(
                    ui,
                    tone="info",
                    header="After applying: how to backfill the rows already set aside",
                    body=runbook,
                )

            async def _apply() -> None:
                dialog.close()  # type: ignore[attr-defined]
                try:
                    outcomes = await run.io_bound(
                        apply_add_columns, plan, connector.connect
                    )
                except Exception as exc:  # noqa: BLE001
                    ui.notify(f"Apply failed: {exc}", type="negative")  # type: ignore[attr-defined]
                    return
                applied = [o for o in outcomes if o.applied]
                failed = [o for o in outcomes if not o.applied]
                # Only ADDED columns are recorded: a renamed one recorded here would make the
                # re-check report the old name as a dropped column.
                add_types = {step.column: step.source_type for step in adds}
                added = {o.column: add_types[o.column] for o in applied if o.column in add_types}
                if on_applied is not None and added:
                    try:
                        on_applied(table, added)
                    except Exception:  # noqa: BLE001 - recording must never hide the outcome
                        _LOGGER.warning("could not record the added columns", exc_info=True)
                from dsql_migrator.core.activity_log import (
                    ActivityCategory,
                    ActivityStatus,
                    log_activity,
                )

                renamed_columns = {step.column for step in renames}
                for outcome in outcomes:
                    log_activity(
                        ActivityCategory.CDC,
                        (
                            "rename column on target (schema drift)"
                            if outcome.column in renamed_columns
                            else "add column to target (schema drift)"
                        ),
                        status=(
                            ActivityStatus.SUCCESS if outcome.applied
                            else ActivityStatus.FAILURE
                        ),
                        target=f"{table}.{outcome.column}",
                        detail=outcome.error or outcome.ddl,
                    )
                # A held column CDC still sends keeps every insert and update that carries it
                # dead-lettered, whatever was applied: say so, never "New changes apply again".
                still_sent = _fix_still_sent(plan)
                resumed, tone = "New changes apply again", "positive"
                if still_sent:
                    resumed = (
                        f"Inserts and updates that carry {_fix_names(still_sent)} are still "
                        "set aside (Fix target schema… explains why)"
                    )
                    tone = "warning"
                if not renames:
                    if failed:
                        ui.notify(  # type: ignore[attr-defined]
                            f"Added {len(applied)} column(s); {failed[0].column} failed: "
                            f"{failed[0].error}",
                            type="negative",
                        )
                    else:
                        ui.notify(  # type: ignore[attr-defined]
                            f"Added {len(applied)} column(s) to {table}. {resumed}; to "
                            "backfill the rows already set aside, re-run Step 1 "
                            "(Evaluation), stop CDC, then reload this table choosing "
                            "'Drop & reload'.",
                            type=tone,
                        )
                elif failed:
                    ui.notify(  # type: ignore[attr-defined]
                        f"Applied {len(applied)} statement(s); {failed[0].column} failed: "
                        f"{failed[0].error}",
                        type="negative",
                    )
                elif _fix_is_cdc_only(migration_type):
                    ui.notify(  # type: ignore[attr-defined]
                        f"Applied {len(applied)} statement(s) to {table}. {resumed}; re-run "
                        "Step 1 (Evaluation), and correct the rows already set aside on "
                        "Aurora DSQL by hand: they are not replayed.",
                        type=tone,
                    )
                else:
                    ui.notify(  # type: ignore[attr-defined]
                        f"Applied {len(applied)} statement(s) to {table}. {resumed}; re-run "
                        "Step 1 (Evaluation), then backfill the rows already set aside: stop "
                        "CDC and reload this table choosing 'Drop & reload'.",
                        type=tone,
                    )
                if on_refresh is not None:
                    on_refresh()

            with ui.row().classes("w-full justify-end gap-2"):  # type: ignore[attr-defined]
                ui.button("Cancel", on_click=dialog.close).props("flat no-caps")  # type: ignore[attr-defined]
                if plan.steps:
                    ui.button(  # type: ignore[attr-defined]
                        f"Apply {len(plan.steps)} statement(s)", on_click=_apply
                    ).props("no-caps color=primary")
    dialog.open()  # type: ignore[attr-defined]
