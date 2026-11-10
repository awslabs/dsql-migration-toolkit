# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Foreign-key pass for the Full Load and Cut over (NiceGUI-free).

:func:`apply_preserved_foreign_keys` re-creates the preserved foreign keys on Aurora DSQL:
it adds every key first (``ALTER TABLE ... ADD CONSTRAINT ... NOT VALID``), then has Aurora
DSQL validate the rows already loaded (``ALTER TABLE ASYNC ... VALIDATE CONSTRAINT``). A
key whose parent side is incomplete by design is counted for orphans before its ADD, the
pre-gate (:func:`_pregate_orphans`, :func:`_count_orphans`). The Full Load's post-load pass
(``BatchedTableMigrator.apply_foreign_keys``) and Cut over, once the CDC stream has
drained, both run it. Beside it: the key selection both share (:func:`foreign_keys_in_scope`,
:func:`foreign_keys_pointing_in`, :func:`foreign_keys_to_apply`,
:func:`foreign_keys_counted_before_add`), the Full Load's count of the keys it still has to
apply (:func:`pending_foreign_key_count`; Cut over counts its own over the same selection,
``ui/cutover.py``'s ``_cutover_pending_foreign_keys``, and lists them with
``_fk_validation.pending_foreign_keys``), and the constraint and index name readers
(:func:`_named_foreign_key_ddls`, :func:`_stored_index_name`).

It was split out of ``_full_load_engine``, which re-exports every name defined here
(private ones too; ``_LOGGER`` it defines itself, the same logger), so ``from
dsql_migrator.ui.data_migration._full_load_engine import ...`` of any of them keeps
resolving. A re-export does not carry a monkeypatch: a test patches a name the pass reads
in this module. This module must not import the engine at its top level, because the
engine imports it. So ``DataMigrator``, which only an annotation names, is imported for
type checking only: that annotation (:func:`pending_foreign_key_count`'s) does not
evaluate at run time (``typing.get_type_hints`` raises NameError), and nothing evaluates
it. The pass logs through the engine's logger, as before the split; and ``_error_code``,
which both modules call, moved here with the pass.
"""

from __future__ import annotations

import logging
import re
import time as _time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Callable, Collection, Iterable, Mapping, Optional, Sequence

from dsql_migrator.core.activity_log import ActivityCategory, ActivityStatus, log_activity
from dsql_migrator.core.converter import TableConversion
from dsql_migrator.core.models import ForeignKeyDef
from dsql_migrator.core.schema_applier import apply_foreign_key, validate_foreign_key
from dsql_migrator.core.target_introspector import pg_fold_unquoted, pg_stored_name
from dsql_migrator.core.validation_sql import build_orphan_count_sql

if TYPE_CHECKING:
    from dsql_migrator.ui.data_migration._full_load_engine import DataMigrator

# The engine's logger, not this module's: the pass's log lines keep the logger name they
# had before the pass moved here.
_LOGGER = logging.getLogger("dsql_migrator.ui.data_migration._full_load_engine")


def _error_code(exc: BaseException) -> Optional[str]:
    """Return a SQLSTATE-like error code from ``exc`` when available.

    psycopg/DB-API errors expose ``sqlstate``; anything else yields ``None`` so
    the record still captures the message without an code.
    """
    code = getattr(exc, "sqlstate", None)
    return str(code) if code else None


# No key is counted before its ADD unless the caller names it (read-only default).
_NO_COUNTED_KEYS: "Mapping[tuple[str, str], str]" = MappingProxyType({})


@dataclass(frozen=True)
class ForeignKeyValidationSubmission:
    """What one foreign-key pass handed Aurora DSQL to validate (G142), for the caller to
    record (``DataMigrationState.record_fk_validation``) and watch.

    ``in_place``: each ``(child table, constraint)`` the pass found or put in place, in pass
    order. ``jobs``: the validation job id of each key that has one running -- submitted by
    this pass, or the recorded one still running (a key is never validated twice at once:
    Aurora DSQL does not de-duplicate VALIDATE jobs). ``already_valid``: the keys whose
    VALIDATE returned no job (the constraint was valid already). ``unsubmitted``: each key
    whose VALIDATE could not be submitted, with the reason (value-free). ``absent_children``:
    the unselected children pointing in that the pass found absent from the target and left
    out (:func:`foreign_keys_to_apply`), so Cut over's Finish never waits for their keys.
    """

    in_place: "tuple[tuple[str, str], ...]" = ()
    jobs: "Mapping[tuple[str, str], str]" = field(default_factory=dict)
    already_valid: frozenset = frozenset()
    unsubmitted: "Mapping[tuple[str, str], str]" = field(default_factory=dict)
    absent_children: frozenset = frozenset()


def apply_preserved_foreign_keys(
    table_conversions: Mapping[str, TableConversion],
    connection_factory: Callable[[], Any],
    *,
    # Liveness reporter, called at each real unit boundary (per FK, per orphan-count page,
    # per poll of a still-building parent index, per VALIDATE submitted). This pass runs
    # entirely AFTER the Full Load progress drain has been joined, so without it the whole
    # pass is one silence gap measured against the 900 s stall window: up to 300 s per
    # distinct still-building parent index, plus an O(rows) orphan count for each key that
    # is counted before its ADD. A fully successful load was once reaped as "an
    # unresponsive source/target connection", which _mark_done cannot undo. The cut-over
    # action passes its job's heartbeat too.
    heartbeat: Optional[Callable[[], None]] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
    # Progress reporter, called ``(settled, total)`` once per FK that finishes (applied,
    # skipped or failed) -- DISTINCT from ``heartbeat``, which fires many times per FK
    # (every orphan-count page and every index poll) and so cannot be counted. Without
    # this the step's card sat at "0 of N done" for the whole pass, which is exactly the
    # "it looks stuck, click it again" the explicit action was meant to end.
    on_progress: Optional[Callable[[int, int], None]] = None,
    # WHERE this pass was run from, for the audit line only. The pass itself is identical
    # -- but for a CDC migration it runs ONLY at cut over (the post-load pass is a no-op
    # there), and logging that under FULL_LOAD with no marker made the single most
    # important pre-repoint fact read as something that happened back in the load. The
    # identity-sequence sync standing right beside it on the same screen already logs
    # "(cut-over)" under VALIDATION, so the two halves of one operator action were
    # recorded differently. Still ONE log call -- only its category and label vary.
    origin: str = "post-load",
    # Unselected children kept for a key INTO the migration (foreign_keys_pointing_in):
    # when one is not on the target there is nothing to re-create, so it is left out
    # (foreign_keys_to_apply, the selection Cut over's Finish gate shares) instead of
    # failing its orphan count.
    skip_if_absent: "frozenset[str]" = frozenset(),
    # ``(child, constraint) -> why`` for the keys whose parent side is incomplete on Aurora
    # DSQL by design (foreign_keys_counted_before_add: the referenced column is excluded,
    # the parent table is not migrated, or the child is not): those keep the orphan count
    # BEFORE their ADD and are left out while it finds a row, so a key the migration cannot
    # satisfy never starts refusing the application's writes (G142).
    check_before_add: "Mapping[tuple[str, str], str]" = _NO_COUNTED_KEYS,
    # The VALIDATE job each key already has (the persisted record), so a key whose job is
    # still running is not submitted a second time.
    validation_jobs: "Optional[Mapping[tuple[str, str], str]]" = None,
    # Called once, after the keys are added and their validation submitted.
    on_validation: "Optional[Callable[[ForeignKeyValidationSubmission], None]]" = None,
) -> tuple[int, int, int]:
    """Re-create preserved foreign keys: add every key first, have Aurora DSQL check after.

    Shared by the Full Load run-level post-pass
    (:meth:`BatchedTableMigrator.apply_foreign_keys`) and the CDC **cut-over** action
    (which runs it once the stream has drained). G142 -- live-verified on Aurora DSQL:

    * **Add first.** Each preserved FK is added as its own single-DDL ``ALTER TABLE ... ADD
      CONSTRAINT ... NOT VALID`` (OCC 40001 retry + reconnect; 42710 means it is already
      there, which is success), after its parent's unique index is valid. ``NOT VALID``
      reads no row -- it succeeds in well under a second with orphans present -- and from
      its commit on Aurora DSQL refuses every write that would add an orphan (child and
      parent side alike), so the whole step takes seconds inside the write freeze and no
      child table is too large for it. No orphan count runs before the ADD -- except for
      the keys in ``check_before_add``, whose parent side is incomplete on Aurora DSQL by
      design: those are counted first (concurrently, page by page, :func:`_count_orphans`)
      and left out with an actionable note while a child row references a missing parent.
    * **Check after.** Right after the adds, every key in place gets ``ALTER TABLE ASYNC
      ... VALIDATE CONSTRAINT`` (:func:`validate_foreign_key`), a server-side job that
      checks the rows already loaded; its id is handed to ``on_validation`` for the caller
      to record and watch (``_fk_validation``). The verdict is Aurora DSQL's: ``sys.jobs``
      by job id and ``pg_constraint.convalidated``. A key whose recorded job is still
      running keeps it. A failed submission is reported, never raised.

    Best-effort per FK: a failure is logged and skipped, never raised. Returns
    ``(applied, skipped, failed)``: keys in place, keys left out by their count, keys that
    could not be added.
    """
    # Pair each rendered ADD-CONSTRAINT DDL with its FK metadata by CONSTRAINT NAME,
    # not by list position. The deterministic conversion builds both lists in
    # table.foreign_keys order (aligned), but an edited Schema-Conversion script
    # re-parses foreign_key_ddls from only the FK lines the user kept -- in their
    # edited order -- while preserved_foreign_keys stays the full deterministic list.
    # A positional zip would then pair a DDL with the WRONG FK's metadata, so the
    # orphan count would check the wrong columns and VALIDATE the wrong constraint.
    # Matching on the constraint name (the DDL's own key) keeps the count and the
    # VALIDATE target aligned to the constraint actually being added.
    pending: list[tuple[str, Optional[str], Optional[ForeignKeyDef], str]] = []
    # One memo per PASS: the same parent index is commonly referenced by several FKs.
    index_state_cache: dict = {}
    _beat = heartbeat if callable(heartbeat) else (lambda: None)
    _stopped = should_cancel if callable(should_cancel) else (lambda: False)

    def _record(submission: ForeignKeyValidationSubmission) -> None:
        if not callable(on_validation):
            return
        try:
            on_validation(submission)
        except Exception:  # noqa: BLE001 - the caller's record must not undo the pass
            _LOGGER.warning("Recording the foreign-key validation failed", exc_info=True)

    absent = _absent_optional_children(table_conversions, connection_factory, skip_if_absent)
    table_conversions = foreign_keys_to_apply(table_conversions, skip_if_absent, absent)
    for table_name, conv in table_conversions.items():
        by_name = {fk.name: fk for fk in conv.preserved_foreign_keys}
        # A key added without a name is added under the name PostgreSQL would give it,
        # written into its statement: validated like every key, and 42710 on a re-apply.
        for constraint_name, add_ddl in _named_foreign_key_ddls(conv.foreign_key_ddls):
            fk = by_name.get(constraint_name) if constraint_name is not None else None
            pending.append((table_name, constraint_name, fk, add_ddl))

    _total = len(pending)

    def _report(settled: int) -> None:
        """Publish ``(settled, total)``; cosmetic, so a reporter error never breaks the pass."""
        if not callable(on_progress):
            return
        try:
            on_progress(settled, _total)
        except Exception:  # noqa: BLE001 - progress is display only
            _LOGGER.debug("Foreign-key progress reporter raised", exc_info=True)

    # Publish the real total up front. The caller's own pre-count (pending_foreign_key_count)
    # is computed separately, so this is what keeps the card's denominator equal to the number
    # of FKs the pass will actually work through -- none, when every key was left out.
    _report(0)
    if not pending:
        # Nothing to add; the record still learns what was left out (Finish waits for none
        # of those keys) and that no key is in place as this pass knows it.
        _record(ForeignKeyValidationSubmission(absent_children=absent))
        return (0, 0, 0)

    # A STARTED line, so an INTERRUPTED pass leaves evidence. The roll-up below is emitted
    # after the loop, so a pass killed part-way -- the Fargate task replaced, or the
    # cut-over caller's connection factory raising on an expired IAM token -- wrote NOTHING
    # to the audit trail while the target had already gained enforced constraints. Cheap:
    # the `if not pending` return above means a run with no foreign keys still logs nothing.
    _cutover_origin = origin == "cut-over"
    # The category of EVERY line of the pass, the per-FK ones included: at cut over they
    # were filed under FULL_LOAD while the pass's own lines sat under VALIDATION, so a
    # reader filtering the Validation step saw "1 skipped" with no line naming which
    # constraint or why.
    _category = (
        ActivityCategory.VALIDATION if _cutover_origin else ActivityCategory.FULL_LOAD
    )
    log_activity(
        _category,
        "apply foreign keys (cut-over)" if _cutover_origin else "apply foreign keys",
        status=ActivityStatus.STARTED,
        detail=f"Foreign-key pass starting: {_total} constraint(s) to add.",
    )

    # Only the keys whose parent side is incomplete by design are counted before their ADD,
    # concurrently (the DDL stays SERIAL: one DDL per transaction on DSQL, 0.06-0.2 s per FK).
    _counted_first = {
        index
        for index, (table_name, constraint_name, fk, _ddl) in enumerate(pending)
        if fk is not None
        and constraint_name is not None
        and (table_name, constraint_name) in check_before_add
    }
    _counted_order = sorted(_counted_first)
    _by_position = (
        _pregate_orphans(
            [pending[index] for index in _counted_order],
            connection_factory, beat=_beat, stopped=_stopped,
        )
        if _counted_order
        else {}
    )
    # _pregate_orphans keys its answers by position in the list it was given.
    _pregated = {
        index: _by_position[position]
        for position, index in enumerate(_counted_order)
        if position in _by_position
    }
    applied = skipped = failed = 0
    in_place: "list[tuple[str, Optional[str]]]" = []
    try:
        for _fk_index, (table_name, constraint_name, fk, add_ddl) in enumerate(pending):
            # One FK is a real unit of work: report liveness so a long pass is not reaped,
            # and honour a stop so Stop is not held for the rest of the pass. The stop is a
            # no-op for the cut-over caller, which supplies no ``should_cancel``.
            _beat()
            # Exactly ``_fk_index`` FKs have finished by the time iteration ``_fk_index``
            # starts, so reporting here is accurate, not lagging -- and it needs no
            # re-indentation of the body below, whose many ``continue`` paths would each
            # have to remember to report.
            _report(_fk_index)
            if _stopped():
                _LOGGER.info(
                    "Foreign-key pass stopped by request with %d of %d applied.",
                    applied, len(pending),
                )
                break
            target = f"{table_name}.{constraint_name or '?'}"
            if _fk_index in _counted_first:
                try:
                    # The concurrent count when it produced a number; anything else (missing,
                    # or an exception) falls through to one serial count on its own
                    # connection, so the verdict and its error handling are unchanged.
                    _pre = _pregated.get(_fk_index)
                    orphans = (
                        _pre
                        if isinstance(_pre, int)
                        else _count_orphans(connection_factory, table_name, fk, on_page=_beat)
                    )
                except Exception as exc:  # noqa: BLE001 - cannot verify -> do not risk an ADD
                    # The count replaced its connection after every drop and replayed the
                    # page a 40001 / XX000 interrupted (G139), so what reaches here is a
                    # connection that could not be opened, a key read or page that outlasted
                    # its retries, or a real query error.
                    failed += 1
                    _LOGGER.warning("Orphan pre-check failed for FK %s", target, exc_info=True)
                    log_activity(
                        _category,
                        "foreign key not applied",
                        status=ActivityStatus.FAILURE,
                        target=target,
                        detail=(
                            "orphan pre-check failed; verify referential integrity and "
                            "apply this foreign key manually"
                        ),
                        exc=exc,
                    )
                    continue
                if orphans:
                    skipped += 1
                    log_activity(
                        _category,
                        "foreign key not applied",
                        status=ActivityStatus.FAILURE,
                        target=target,
                        detail=(
                            f"{orphans} child row(s) reference a missing parent, so Aurora "
                            "DSQL cannot enforce this foreign key. Resolve the orphan rows "
                            "(often an un-replicated source cascade) and re-apply. It was "
                            "counted before it was added because "
                            f"{_counted_first_reason(check_before_add, table_name, constraint_name)}"
                            ": added, it would refuse the application's writes that name a "
                            "parent row Aurora DSQL does not hold."
                        ),
                    )
                    continue
            index_state: Optional[str] = None
            index_waited = 0.0
            try:
                # A table Full Load RECREATED gets its secondary indexes only after the
                # load, via CREATE INDEX ASYNC (returns immediately, builds in the
                # background). If this FK's parent gets its uniqueness from one of those,
                # adding the constraint now races the build and fails 42830 -- so wait for
                # the index to become valid first. "absent" returns at once (waiting would
                # be pointless) and the ADD then fails with an actionable reason.
                index_state, index_waited = _wait_for_referenced_unique_index(
                    fk, connection_factory, cache=index_state_cache,
                    # Up to 300 s per distinct parent index, so the poll is the only unit
                    # boundary inside it -- four stuck parents alone is 1200 s of silence.
                    on_poll=_beat,
                )
                if index_waited > 0:
                    # A wait ACTUALLY happened, so say so: this is the only evidence that
                    # the index-build race was handled rather than merely won. Silent when
                    # the index was already valid (the normal case) -- no noise.
                    log_activity(
                        _category,
                        "waited for referenced unique index",
                        status=ActivityStatus.INFO,
                        target=target,
                        detail=(
                            f"the referenced table's unique index was still building "
                            f"(CREATE INDEX ASYNC); waited {index_waited:.1f}s, then it "
                            f"was {_index_state_phrase(index_state)}"
                        ),
                    )
                # DSQL only accepts ADD CONSTRAINT ... NOT VALID (renders that way); it
                # reads no row and enforces every NEW write immediately. 42710 (already
                # there) is success, so a re-run is a no-op.
                apply_foreign_key(add_ddl, connection_factory=connection_factory)
                applied += 1
                in_place.append((table_name, constraint_name))
            except Exception as exc:  # noqa: BLE001 - non-blocking: data is already loaded
                failed += 1
                _LOGGER.warning("Could not apply FK %s", target, exc_info=True)
                log_activity(
                    _category,
                    "foreign key not applied",
                    status=ActivityStatus.FAILURE,
                    target=target,
                    detail=_fk_failure_detail(
                        exc, add_ddl, index_state, waited_seconds=index_waited
                    ),
                    exc=exc,
                )
    finally:
        # Final count, from the buckets themselves: every settled FK increments exactly one
        # of applied/skipped/failed, so this is right both for a completed pass (== total)
        # and for one cut short by Stop (fewer, which is what the card should then show).
        _report(applied + skipped + failed)
    # Check after: one async VALIDATE per key now in place, so Aurora DSQL checks the rows
    # already loaded while the keys enforce every new write. A stopped pass still submits
    # them for the keys it added (a server-side job, nothing to wait for here).
    submission = _submit_foreign_key_validation(
        in_place, connection_factory, validation_jobs, beat=_beat
    )
    for (table_name, constraint_name), reason in submission.unsubmitted.items():
        log_activity(
            _category,
            "foreign key validation not submitted",
            status=ActivityStatus.WARNING,
            target=f"{table_name}.{constraint_name}",
            detail=(
                "the key is in place and refuses every new write that would break it, but "
                f"Aurora DSQL's check of the rows already loaded could not be started "
                f"({reason}); use Validate again"
            ),
        )
    _record(replace(submission, absent_children=absent))
    # THE single audit event for a foreign-key pass, emitted HERE because every caller
    # shares this function -- the Full Load post-pass, the Data Migration action, and the
    # CUT-OVER action, which logs nothing of its own (so moving this to the caller, as the
    # obvious de-duplication, would have deleted cut over's only record of the pass).
    # There used to be a second line from _apply_foreign_keys 0.3 ms later, with a different
    # action name, a different status and different wording ("orphaned rows", capitalised,
    # full stop) -- one event reported twice, contradicting itself. Unreachable for a run
    # with no foreign keys: the `if not pending` return above fires first, so the caller's
    # "no noise" intent holds without a second condition.
    _at_cutover = origin == "cut-over"
    # FKs the loop never reached -- it `break`s on a stop request, and previously the
    # roll-up then reported "3 applied, 0 skipped, 0 failed." as a SUCCESS for a 15-FK
    # schema, with the 12 un-attempted constraints appearing only in _LOGGER.info (the app
    # stream, not the audit file). An operator reading the log saw a completed pass.
    _unattempted = max(0, _total - (applied + skipped + failed))
    # SUCCESS only when every constraint is actually ENFORCED on the target. An
    # orphan-skipped FK was deliberately not added, so referential integrity is NOT in
    # place for it; reporting that as SUCCESS is the reassurance that hides it. WARNING,
    # not FAILURE: nothing broke, and the remedy (clean the orphans, re-run the
    # idempotent pass) is the operator's.
    _status = (
        ActivityStatus.FAILURE
        if failed
        else ActivityStatus.WARNING
        if (skipped or _unattempted or submission.unsubmitted)
        else ActivityStatus.SUCCESS
    )
    _submitted = len(submission.jobs)
    log_activity(
        # Cut over is the Validation step's screen, and the sync beside it logs there too,
        # so a reader filtering that step sees the whole hand-off in one place.
        _category,
        "apply foreign keys (cut-over)" if _at_cutover else "apply foreign keys",
        status=_status,
        detail=(
            # Not "post-load" unconditionally: the same pass runs at cut over for a CDC
            # migration, where it is the ONLY thing that ever creates these constraints.
            f"Foreign-key pass: {applied} applied, {skipped} skipped (orphan rows), "
            f"{failed} failed."
            + (
                f" {_unattempted} of {_total} NOT ATTEMPTED (pass stopped early) — those "
                "constraints are not enforced; re-run the (idempotent) pass."
                if _unattempted
                else ""
            )
            + (
                f" The {skipped} skipped constraint(s) are NOT enforced on the target: "
                "clear the orphan rows and re-run the pass."
                if skipped
                else ""
            )
            + (
                f" The {applied} applied enforce every new write now; Aurora DSQL validates "
                f"the rows already loaded in the background ({_submitted} validation "
                f"job(s) running, {len(submission.already_valid)} already valid"
                + (
                    f", {len(submission.unsubmitted)} not submitted"
                    if submission.unsubmitted
                    else ""
                )
                + ")."
                if applied
                else ""
            )
            + (
                " Run at cut over, before repointing — for a CDC migration this is the "
                "only pass that creates the deferred constraints."
                if _at_cutover
                else ""
            )
        ),
    )
    return (applied, skipped, failed)


def _counted_first_reason(check_before_add, table_name: str, constraint_name) -> str:
    """Why a key was counted before its ADD (``check_before_add`` maps it to a reason)."""
    getter = getattr(check_before_add, "get", None)
    reason = getter((table_name, constraint_name)) if callable(getter) else None
    return reason or "its parent side is not fully on Aurora DSQL by design"


def _submit_foreign_key_validation(
    in_place: "Sequence[tuple[str, Optional[str]]]",
    connection_factory: Callable[[], Any],
    recorded: "Optional[Mapping[tuple[str, str], str]]",
    *,
    beat: Callable[[], None],
) -> ForeignKeyValidationSubmission:
    """Submit ``ALTER TABLE ASYNC ... VALIDATE CONSTRAINT`` for each key in place that is
    not validated yet (G142).

    One catalog read first (:func:`foreign_key_validation_states`): a key already
    ``convalidated`` needs no job, and a key whose recorded job is still ``submitted`` /
    ``processing`` (read BY that job id from ``sys.jobs``) keeps it -- Aurora DSQL does not
    de-duplicate VALIDATE jobs, so a second click would queue another full read of the
    child. A key the read cannot answer is submitted (a VALIDATE of a valid key returns no
    job: success). A key without a parsed constraint name cannot be validated and is left
    out.
    """
    from dsql_migrator.core.target_introspector import foreign_key_validation_states

    keys = [
        (table_name, constraint_name)
        for table_name, constraint_name in in_place
        if constraint_name is not None
    ]
    known = {key: str(job) for key, job in (recorded or {}).items() if key in keys and job}
    try:
        reads = (
            foreign_key_validation_states(keys, known, connection_factory=connection_factory)
            if keys
            else {}
        )
    except Exception:  # noqa: BLE001 - unknown: submit, never skip on a guess
        reads = {}
    valid = {
        key for key, read in reads.items() if read is not None and read.present and read.validated
    }
    running = {
        key: known[key]
        for key, read in reads.items()
        if key in known
        and read is not None
        and read.present
        and not read.validated
        and read.job_status in ("submitted", "processing")
    }
    jobs: "dict[tuple[str, str], str]" = {}
    already_valid: "set[tuple[str, str]]" = set()
    unsubmitted: "dict[tuple[str, str], str]" = {}
    for key in keys:
        beat()
        if key in valid:
            already_valid.add(key)
            continue
        if key in running:
            jobs[key] = running[key]
            continue
        table_name, constraint_name = key
        try:
            job_id = validate_foreign_key(
                table_name, constraint_name, connection_factory=connection_factory
            )
        except Exception as exc:  # noqa: BLE001 - reported per key; the key still enforces
            _LOGGER.warning(
                "VALIDATE CONSTRAINT not submitted for %s.%s", table_name, constraint_name,
                exc_info=True,
            )
            sqlstate = _error_code(exc)
            unsubmitted[key] = (
                f"{type(exc).__name__}, SQLSTATE {sqlstate}" if sqlstate else type(exc).__name__
            )
            continue
        if job_id:
            jobs[key] = job_id
        else:
            already_valid.add(key)
    return ForeignKeyValidationSubmission(
        in_place=tuple(keys),
        jobs=jobs,
        already_valid=frozenset(already_valid),
        unsubmitted=unsubmitted,
    )


def foreign_keys_blocking_replace(
    table_conversions: Mapping[str, TableConversion],
    replace_targets: "Iterable[str]",
) -> list[tuple[str, str]]:
    """Return ``(child_table, constraint)`` FKs that would block a table REPLACE. Pure.

    ``DROP TABLE orders`` is refused while any other table's foreign key still references
    it (``cannot drop table orders because other objects depend on it / constraint
    fk_order_items_orders on table order_items depends on table orders``). A
    ``Full load only`` run creates exactly those FKs in its post-load pass, so choosing
    **Drop & reload** afterwards hit a wall built by the tool itself -- and the error text
    blamed a view and told the user to pick the dependent object in the object browser,
    which is impossible for a foreign key (it is not an item there). So the recreate path
    removes them first and the existing post-load pass puts them back, exactly as the
    dependent-VIEW pre-drop/recreate pair already does. (On a CDC-bearing run that pass
    defers to cut over instead, so the drop stays in effect for the rest of the run --
    correct, because a live stream must not have enforced FKs.)

    Derived from THIS migration's own conversion (the same ``foreign_key_ddls`` the apply
    pass uses), so everything returned is by definition ours to drop and to re-create --
    no catalog read, and a constraint the user made by hand is never touched.

    Only FKs whose PARENT is being replaced are returned. An FK that merely lives ON a
    replaced table needs no action: ``DROP TABLE`` takes its own constraints with it, and
    the post-load pass re-adds them.
    """
    targets = {str(name) for name in replace_targets}
    if not targets:
        return []
    blocking: list[tuple[str, str]] = []
    for table_name, conv in table_conversions.items():
        by_name = {fk.name: fk for fk in conv.preserved_foreign_keys}
        for add_ddl in conv.foreign_key_ddls:
            constraint_name = _constraint_name_from_ddl(add_ddl)
            fk = by_name.get(constraint_name) if constraint_name else None
            if fk is None or not constraint_name:
                continue
            if fk.referenced_table in targets:
                pair = (table_name, constraint_name)
                if pair not in blocking:
                    blocking.append(pair)
    return sorted(blocking)


def foreign_keys_in_scope(table_conversions, tables):
    """The conversions whose foreign keys are the MIGRATION's. Pure.

    A migrated (selected) table keeps all its keys. An UNSELECTED table keeps only its keys
    that reference a migrated table: those are exactly the ones this migration may have
    dropped -- a Drop & reload of the parent pre-drops them, the CDC FK precondition removes
    them -- each promising they are re-created by 'Apply foreign keys'. A key between two
    unselected tables is out of scope (the migration never touches either). ``tables``
    empty or ``None`` (no selection recorded) keeps every table -- the previous behaviour.
    """
    keep = set(tables or ())
    if not keep:
        return dict(table_conversions or {})
    scoped = {}
    for name, conv in (table_conversions or {}).items():
        if name in keep:
            scoped[name] = conv
            continue
        by_name = {fk.name: fk for fk in getattr(conv, "preserved_foreign_keys", ()) or ()}
        pointing_in = [
            ddl
            for ddl in getattr(conv, "foreign_key_ddls", ()) or ()
            if getattr(by_name.get(_constraint_name_from_ddl(ddl) or ""), "referenced_table", None)
            in keep
        ]
        if pointing_in:
            scoped[name] = conv.model_copy(update={"foreign_key_ddls": pointing_in})
    return scoped


def foreign_keys_pointing_in(scoped_conversions, tables) -> frozenset:
    """The unselected children :func:`foreign_keys_in_scope` kept for a key into the
    migration: they may legitimately be absent on the target (nothing to re-create)."""
    keep = set(tables or ())
    if not keep:
        return frozenset()
    return frozenset(n for n in (scoped_conversions or {}) if n not in keep)


def foreign_keys_to_apply(scoped_conversions, optional, absent=frozenset()) -> dict:
    """The conversions whose foreign keys a pass adds: ``scoped_conversions``
    (:func:`foreign_keys_in_scope`) less each ``optional`` child
    (:func:`foreign_keys_pointing_in`) that is ``absent`` from the target, where there is
    nothing to re-create. Pure.

    The ONE selection :func:`apply_preserved_foreign_keys` and Cut over's Finish gate share
    (G142): the pass with the children it found absent, the gate with those the last pass
    recorded -- so Finish never waits for a key the pass leaves out by design."""
    gone = set(optional or ()) & set(absent or ())
    return {name: conv for name, conv in (scoped_conversions or {}).items() if name not in gone}


def foreign_keys_counted_before_add(
    scoped_conversions, tables, excluded_columns=None
) -> "dict[tuple[str, str], str]":
    """``(child, constraint) -> why`` for the keys whose parent side is incomplete on Aurora
    DSQL BY DESIGN, so the pass keeps counting their orphans before it adds them (G142). Pure.

    Every other key is added first (``NOT VALID``, seconds) and validated by Aurora DSQL
    afterwards: its rows came over with their parents, so a failed validation is the
    exception to explain. These three are different -- the migration itself leaves parent
    rows off Aurora DSQL, so adding the key would start refusing the application's writes
    that name them:

    * the referenced column is EXCLUDED on the parent (a B3 entry kept once the exclusion
      set was baked in): it is NULL there, so every child row with a value is an orphan;
    * the parent table is not one of the migration's ``tables`` (an unticked parent): its
      rows on Aurora DSQL are whatever an earlier load left, if any;
    * the child table is not one of them (an unselected child kept for a key into the
      migration, :func:`foreign_keys_pointing_in`): a table the migration does not load.

    ``tables`` empty or ``None`` (no selection recorded) migrates every table, so only the
    first applies. ``excluded_columns`` is the EFFECTIVE exclusion set (table -> columns).
    """
    keep = set(tables or ())
    excluded = {
        str(table): set(columns or ()) for table, columns in (excluded_columns or {}).items()
    }
    counted: "dict[tuple[str, str], str]" = {}
    for child, conv in (scoped_conversions or {}).items():
        by_name = {fk.name: fk for fk in getattr(conv, "preserved_foreign_keys", ()) or ()}
        for ddl in getattr(conv, "foreign_key_ddls", ()) or ():
            name = _constraint_name_from_ddl(ddl)
            fk = by_name.get(name) if name else None
            if fk is None:
                continue
            gone = sorted(set(fk.referenced_columns) & excluded.get(fk.referenced_table, set()))
            if gone:
                counted[(child, name)] = (
                    f"its referenced column {fk.referenced_table}.{gone[0]} is excluded from "
                    "this migration (NULL on Aurora DSQL)"
                )
            elif keep and fk.referenced_table not in keep:
                counted[(child, name)] = (
                    f"its parent table {fk.referenced_table} is not part of this migration"
                )
            elif keep and child not in keep:
                counted[(child, name)] = f"its table {child} is not part of this migration"
    return counted


def _target_table_present(connection_factory, table_name: str) -> bool:
    """Whether ``table_name`` exists on the target (one catalog read); True when unknown."""
    from dsql_migrator.core.target_introspector import read_overlay_column_facts

    try:
        conn = connection_factory()
    except Exception:  # noqa: BLE001 - unknown: let the pass decide as before
        return True
    try:
        exists, _facts = read_overlay_column_facts(conn, table_name, [])
        return bool(exists)
    except Exception:  # noqa: BLE001 - unknown
        return True
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


def _absent_optional_children(table_conversions, connection_factory, optional) -> frozenset:
    """The OPTIONAL children of ``table_conversions`` (keys into the migration from an
    unselected table) that are not on the target, one catalog read each. A migrated table
    is never among them -- an absent one still fails loudly in the pass."""
    wanted = set(optional or ())
    return frozenset(
        name
        for name in (table_conversions or {})
        if name in wanted and not _target_table_present(connection_factory, name)
    )


def pending_foreign_key_count(migrator: DataMigrator, tables=None, absent=()) -> int:
    """How many preserved foreign keys this run still has to apply (no DB access).

    Counts the SAME rendered ``foreign_key_ddls`` :func:`apply_preserved_foreign_keys`
    would iterate, so the Data Migration step's "Apply foreign keys" action shows the same
    number the action will work through -- over the run's ``tables``
    (:func:`foreign_keys_in_scope`; ``None``: every table), less each optional child
    (``fk_optional_tables``) the last pass found ``absent``: the pass's and Cut over's own
    selection (:func:`foreign_keys_to_apply`), so a clean pass leaves none outstanding.
    Zero when FK preservation is off, when the source has none, or for a CDC migration
    (those are applied at cut over instead).
    """
    inputs = getattr(migrator, "_inputs", None)
    if inputs is None:
        return 0
    if (
        getattr(inputs, "is_cdc_migration", False)
        or getattr(inputs, "cdc_coexisting", False)
        or getattr(inputs, "cdc_stack_name", None)
    ):
        return 0
    return sum(
        len(conv.foreign_key_ddls or ())
        for conv in foreign_keys_to_apply(
            foreign_keys_in_scope(getattr(inputs, "table_conversions", None) or {}, tables),
            getattr(inputs, "fk_optional_tables", None) or (),
            absent,
        ).values()
    )


def preserved_foreign_key_names(
    table_conversions: Mapping[str, TableConversion],
) -> dict[str, list[str]]:
    """Return table -> the FK constraint names THIS migration renders. Pure.

    Derived from the very same ``foreign_key_ddls`` that
    :func:`apply_preserved_foreign_keys` applies (named with
    :func:`_named_foreign_key_ddls`, as the pass names them), so the two can never
    disagree about which constraints belong to this migration. That matters because it is
    the ONLY safe ownership test for the CDC precondition: an enforced FK on the target has
    to be removed before streaming (the sink dead-letters 23503 permanently), but the tool
    must never drop a constraint the user created and it cannot put back -- and
    ``pg_constraint.convalidated`` cannot tell them apart.

    Tables with FK preservation disabled (or no FKs) simply do not appear.
    """
    names: dict[str, list[str]] = {}
    for table_name, conv in table_conversions.items():
        for constraint_name, _ddl in _named_foreign_key_ddls(conv.foreign_key_ddls):
            if constraint_name:
                names.setdefault(table_name, []).append(constraint_name)
    return names


# Matches ``ADD CONSTRAINT <name> FOREIGN KEY`` and captures <name>, either a
# double-quoted identifier (as rendered) or a bare word (if a user typed it unquoted).
_FK_CONSTRAINT_NAME_RE = re.compile(
    r'ADD\s+CONSTRAINT\s+("(?:[^"]|"")+"|[^\s("]+)\s+FOREIGN\s+KEY',
    re.IGNORECASE,
)


# SQLSTATE 42830 (invalid_foreign_key): "there is no unique constraint matching given
# keys for referenced table". DSQL raises the SAME code whether the required unique index
# is MISSING or merely still BUILDING, so it is only retryable in the second case --
# distinguished via pg_index.indisvalid (see unique_index_state).
_NO_MATCHING_UNIQUE_SQLSTATE = "42830"

# How long to wait for a post-load ``CREATE INDEX ASYNC`` to become valid before giving up
# on the FK that needs it. Generous because the build scales with the table: a composite-PK
# parent's single-column unique index on a large table can take a while. Bounded so a
# genuinely broken schema cannot hang the run (and "absent" fails immediately anyway).
_INDEX_WAIT_BUDGET_SECONDS = 300.0
_INDEX_WAIT_POLL_SECONDS = 2.0


def _index_state_phrase(state: Optional[str]) -> str:
    """Render a unique-index state for an OPERATOR, never as a Python literal.

    The activity log is downloaded and pasted into runbooks, so "then it was None" (the
    catalog was unreadable) is both meaningless to a reader and indistinguishable from a
    real state. Each phrase also says what it implies for the foreign key.
    """
    return {
        "valid": "ready",
        "building": "still building when the wait budget ran out",
        "absent": "absent (no such unique index)",
        None: "unknown (the target catalog could not be read)",
    }.get(state, str(state))


def _wait_for_referenced_unique_index(
    fk: "Optional[ForeignKeyDef]",
    connection_factory: Callable[[], Any],
    *,
    budget_seconds: float = _INDEX_WAIT_BUDGET_SECONDS,
    poll_seconds: float = _INDEX_WAIT_POLL_SECONDS,
    sleep: Callable[[float], None] = _time.sleep,
    monotonic: Callable[[], float] = _time.monotonic,
    # Per-FK-pass memo of (referenced table, columns) -> final state. The wait is per FK
    # but the INDEX is shared: N foreign keys onto one stuck parent each paid the full
    # budget (N x 300s) and each emitted a near-identical log line. Populated by this
    # function; the caller supplies one dict per pass.
    cache: Optional[dict] = None,
    # Called once per poll of a still-building index, so the caller can report job
    # liveness during a wait that is deliberately long (see apply_preserved_foreign_keys).
    on_poll: Optional[Callable[[], None]] = None,
) -> tuple[Optional[str], float]:
    """Wait while the FK's referenced unique index is still building. Returns its state.

    THE RACE this closes: a table Full Load had to RECREATE (e.g. a composite-PK re-key)
    gets its secondary indexes only AFTER the load, as ``CREATE INDEX ASYNC`` -- which
    returns immediately while DSQL builds in the background. The FK post-pass then runs
    within seconds, and an FK whose parent's single-column uniqueness comes ONLY from one
    of those async indexes fails 42830. Nothing waited for the index, so the migration
    finished "successfully" with referential integrity silently missing -- and being a
    race, the same procedure could pass on one run and drop an FK on the next.

    Returns ``(state, waited_seconds)``: ``"valid"`` (safe to add), ``"absent"`` (waiting
    is pointless -- fail fast), ``"building"`` (still not ready when the budget ran out),
    or ``None`` (catalog unreadable). Only ``building`` consumes the budget.

    ``waited_seconds`` exists so the wait is OBSERVABLE. Without it a successful run left
    no trace at all, so "the wait held the FK back until the index was ready" and "the
    race happened to be won" were indistinguishable in the field -- the gap between the
    last table load and the FK pass was ~5.4s either way, and neither the activity log nor
    CloudWatch had an entry. The caller logs it (only when a wait actually occurred, so a
    normal run stays quiet) and folds it into the failure detail when the budget runs out.

    It measures the POLLING WAIT ONLY -- the first probe's own round trip is excluded, so
    "did not wait" reports exactly ``0.0`` rather than that probe's latency. The caller
    keys its log line on this being non-zero, so anything else makes it log for every FK.
    """
    if fk is None or not fk.referenced_columns:
        return None, 0.0
    from dsql_migrator.core.target_introspector import unique_index_state

    def _probe() -> Optional[str]:
        return unique_index_state(
            fk.referenced_table, fk.referenced_columns,
            connection_factory=connection_factory,
        )

    key = (fk.referenced_table, tuple(fk.referenced_columns))
    if cache is not None and key in cache:
        # Already settled for this parent index in this pass: no probe, no second wait,
        # and no duplicate log line (the caller keys that on a non-zero wait).
        return cache[key], 0.0

    state = _probe()
    if state != "building":
        if cache is not None:
            cache[key] = state
        # Nothing was waited for. The timer must NOT start before this first probe: that
        # probe is a DSQL round trip (IAM token + TLS), so its own latency landed in
        # waited_seconds and made it non-zero for EVERY foreign key. The caller's
        # `index_waited > 0` guard then logged a line per FK reading "the index was still
        # building ... waited 0.0s" -- self-contradictory, and the opposite of the silence
        # this log promised for the normal case.
        return state, 0.0
    started = monotonic()
    deadline = started + max(0.0, budget_seconds)
    while monotonic() < deadline:
        sleep(poll_seconds)
        if callable(on_poll):
            on_poll()
        fresh = _probe()
        if fresh is None:
            # UNKNOWN is not an answer. ``unique_index_state`` swallows every exception
            # and returns None, so one transient connect/read failure mid-wait used to
            # end the loop (``while state == "building"`` went false) after a single poll
            # of a 300s budget -- and the ADD CONSTRAINT then raced the very index build
            # this helper exists to wait for. We already know this index WAS building, so
            # keep polling within the remaining budget and keep that as the last known
            # state; the operator-facing reason stays actionable instead of degrading to
            # a bare "None".
            continue
        state = fresh
        if state != "building":
            break
    if cache is not None:
        cache[key] = state
    return state, max(0.0, monotonic() - started)


def _fk_failure_detail(
    exc: BaseException,
    add_ddl: object,
    index_state: Optional[str],
    *,
    waited_seconds: float = 0.0,
) -> str:
    """Compose an ACTIONABLE activity-log detail for an FK that could not be applied.

    The old copy was the fixed string "could not be created automatically; apply it
    manually", with the real exception going only to the module logger -- so the activity
    log (the artifact an operator downloads and pastes into a runbook) recorded no reason,
    and there was no way to know WHAT to apply manually. This records the SQLSTATE, the
    driver's own message, why it happened when we can tell, and the exact statement to run.
    """
    sqlstate = _error_code(exc)
    message = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
    if sqlstate == _NO_MATCHING_UNIQUE_SQLSTATE and index_state == "building":
        # Name the elapsed wait: it is what separates "the index build is slow, retry
        # shortly" from "there is no such index" for an operator reading the log.
        cause = (
            "the referenced table's UNIQUE index was still being built "
            f"(CREATE INDEX ASYNC) after waiting {waited_seconds:.1f}s (the wait "
            "budget); re-run this statement once it is valid"
        )
    elif sqlstate == _NO_MATCHING_UNIQUE_SQLSTATE and index_state == "absent":
        cause = (
            "the referenced table has no UNIQUE index on the referenced column(s), so "
            "DSQL cannot enforce this foreign key; create one first"
        )
    else:
        cause = "apply it manually"
    statement = add_ddl if isinstance(add_ddl, str) else str(add_ddl)
    return (
        f"{cause}. SQLSTATE={sqlstate or 'unknown'}: {message}. Statement: {statement}"
    )


def _constraint_name_from_ddl(add_ddl: object) -> Optional[str]:
    """Return the constraint name from an ``ADD CONSTRAINT ... FOREIGN KEY`` DDL, as
    PostgreSQL / Aurora DSQL stores it: a quoted name as written, an unquoted one folded to
    lower case (``FK_Orders_Cust`` is stored, read and validated as ``fk_orders_cust``).

    The constraint name is the reliable key that pairs a rendered FK DDL with its
    metadata (for the orphan count) and with the ``VALIDATE CONSTRAINT`` target --
    more robust than positional pairing, which breaks when a user deletes/reorders FK
    lines in the edited Schema-Conversion script. Returns ``None`` if the statement is
    not a recognizable ADD-CONSTRAINT-FOREIGN-KEY (the caller then applies it without a
    count rather than mis-keying another FK's metadata); a key added without a name is
    named by :func:`_named_foreign_key_ddls`.
    """
    match = _FK_CONSTRAINT_NAME_RE.search(str(add_ddl))
    if not match:
        return None
    token = match.group(1)
    if len(token) >= 2 and token.startswith('"') and token.endswith('"'):
        return token[1:-1].replace('""', '"')
    return pg_fold_unquoted(token)


# ``ALTER TABLE [IF EXISTS] [ONLY] [<schema>.]<table> ADD FOREIGN KEY (<columns>)``: a key
# added without a name (typed in Schema Conversion). Captures the table, the ``ADD`` and the
# column list.
_FK_IDENT = r'"(?:[^"]|"")+"|[^\s(),."]+'
_UNNAMED_FK_RE = re.compile(
    rf"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?(?:(?:{_FK_IDENT})\s*\.\s*)?"
    rf"({_FK_IDENT})\s+(ADD\s+)FOREIGN\s+KEY\s*"
    rf"\(\s*((?:{_FK_IDENT})(?:\s*,\s*(?:{_FK_IDENT}))*)\s*\)",
    re.IGNORECASE,
)


def _stored_identifier(token: str) -> str:
    """One identifier as PostgreSQL stores it: quoted as written, unquoted folded to lower
    case, cut to 63 bytes."""
    token = token.strip()
    if len(token) >= 2 and token.startswith('"') and token.endswith('"'):
        return pg_stored_name(token[1:-1].replace('""', '"'))
    return pg_stored_name(pg_fold_unquoted(token))


# ``CREATE [UNIQUE] INDEX [ASYNC] [IF NOT EXISTS] <name> ON ...``: the head, then the index
# name token (matched apart, so ``CREATE INDEX ASYNC ON ...`` never reads ASYNC as a name).
_INDEX_HEAD_RE = re.compile(
    r"^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:ASYNC\s+)?(?:IF\s+NOT\s+EXISTS\s+)?",
    re.IGNORECASE,
)
_INDEX_NAME_RE = re.compile(rf"({_FK_IDENT})\s+ON\b", re.IGNORECASE)


def _stored_index_name(ddl: object) -> Optional[str]:
    """The name a ``CREATE INDEX`` DDL gives its index, as the catalog stores it (a quoted
    name as written, an unquoted one folded, cut to 63 bytes), or ``None`` when the
    statement names none it can read (P4a: such a DDL is submitted, never skipped)."""
    text = str(ddl or "")
    head = _INDEX_HEAD_RE.match(text)
    match = _INDEX_NAME_RE.match(text, head.end()) if head else None
    if match is None or match.group(1).upper() == "ON":
        return None
    return _stored_identifier(match.group(1))


def _pg_foreign_key_name(table: str, columns: "Sequence[str]", taken: "Collection[str]") -> str:
    """The name PostgreSQL gives a foreign key added without one (ChooseConstraintName):
    ``<table>_<col1>[_<col2>...]_fkey``, the longer of the table name and the column part
    cut first so the whole fits in 63 bytes (each on a character boundary), and ``fkey1``,
    ``fkey2``, ... in place of ``fkey`` while the name is ``taken``."""
    first = table.encode("utf-8")
    second = "_".join(columns).encode("utf-8")
    number = 0
    while True:
        label = f"fkey{number or ''}"
        keep_first, keep_second = len(first), len(second)
        while keep_first + keep_second > 63 - 2 - len(label):  # two underscores
            if keep_first > keep_second:
                keep_first -= 1
            else:
                keep_second -= 1
        name = "_".join((
            first[:keep_first].decode("utf-8", errors="ignore"),
            second[:keep_second].decode("utf-8", errors="ignore"),
            label,
        ))
        if name not in taken:
            return name
        number += 1


def _named_foreign_key_ddls(ddls) -> "list[tuple[Optional[str], object]]":
    """``(constraint, statement)`` for each of ONE table's foreign-key DDLs, in order, as the
    pass runs them. Pure.

    A key the DDL names keeps that name (:func:`_constraint_name_from_ddl`). One added
    without a name (``ALTER TABLE ... ADD FOREIGN KEY``, typed in Schema Conversion) gets
    the name PostgreSQL would give it (:func:`_pg_foreign_key_name`, past the names the
    table's other keys hold), written into its statement as ``ADD CONSTRAINT "<name>"`` --
    so it is validated and held like every key, and a re-apply meets 42710 instead of
    adding a second copy. ``None``: a statement whose key cannot be read (run as written).
    The pass, Cut over's Finish, the keys CDC can break and the CDC ownership test read a
    key's name here, so they name the same key.
    """
    statements = list(ddls or ())
    names = [_constraint_name_from_ddl(statement) for statement in statements]
    taken = {pg_stored_name(name) for name in names if name}
    named: "list[tuple[Optional[str], object]]" = []
    for statement, name in zip(statements, names):
        text = str(statement)
        unnamed = None if name is not None else _UNNAMED_FK_RE.search(text)
        if unnamed is not None:
            name = _pg_foreign_key_name(
                _stored_identifier(unnamed.group(1)),
                [_stored_identifier(column) for column in re.findall(_FK_IDENT, unnamed.group(3))],
                taken,
            )
            taken.add(name)
            quoted = '"' + name.replace('"', '""') + '"'
            statement = (
                f"{text[:unnamed.start(2)]}ADD CONSTRAINT {quoted} {text[unnamed.end(2):]}"
            )
        named.append((name, statement))
    return named


# Rows scanned per keyset page in the orphan count's paged path. Bounded so a
# billion-row child never runs one orphan scan past DSQL's ~300s transaction limit.
_ORPHAN_PAGE_SIZE = 5000


# How many orphan counts run at once. A count is READ-ONLY, and each worker counts on its
# own connection, so they are independent -- but the target is a live cluster and the pass
# may run at CUT OVER alongside a draining CDC sink, so the fan-out stays small and bounded
# rather than one connection per foreign key.
#
# Since G142 only the keys whose parent side is incomplete by design are counted BEFORE their
# ADD (foreign_keys_counted_before_add); every other key is added NOT VALID at once and
# Aurora DSQL validates the loaded rows itself -- a failed validation is observable by its
# job id (sys.jobs) and from pg_constraint.convalidated, live-verified -- and its rows are
# counted only to explain a failure (_fk_validation).
#
# 16 IS MEASURED, not chosen: a live sweep over a schema's 15 FKs / 15.5M child rows gave
#   serial 1680.5s | 8-way 461.2s (3.64x) | 16-way 414.0s (4.06x) | 24-way 415.7s (4.04x)
# so it saturates at 16 and 24 is fractionally worse -- the cluster doing the anti-join is
# the limit, not client concurrency. Every arm applied all 15 with zero failures.
_FK_PREGATE_WORKERS = 16


def _pregate_orphans(
    pending: "Sequence[tuple]",
    connection_factory: Callable[[], Any],
    *,
    beat: Callable[[], None],
    stopped: Callable[[], bool],
) -> dict:
    """Count orphans for every FK in ``pending`` CONCURRENTLY (the keys counted before their
    ADD, G142). Returns ``{index in pending: count | Exception}``.

    Read-only and independent per FK, each on its own connection, which the count opens
    from ``connection_factory`` and closes (one psycopg connection cannot serve concurrent
    queries). Best-effort: an entry that fails or is missing simply falls back to the
    caller's existing serial pre-check, so this can only make the pass faster, never change
    its verdict.
    """
    out: dict = {}
    if not pending:
        return out

    def one(index: int, table_name: str, fk) -> None:
        if stopped():
            return
        try:
            out[index] = _count_orphans(connection_factory, table_name, fk, on_page=beat)
        except Exception as exc:  # noqa: BLE001 - recorded; the caller retries serially
            out[index] = exc
        beat()

    jobs = [
        (i, table_name, fk)
        for i, (table_name, _cname, fk, _ddl) in enumerate(pending)
        if fk is not None
    ]
    if not jobs:
        return out
    workers = max(1, min(_FK_PREGATE_WORKERS, len(jobs)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(one, *job) for job in jobs]
        for future in futures:
            try:
                future.result()
            except Exception:  # noqa: BLE001 - `one` already records its own failure
                pass
    return out


def _count_orphans(
    connection_factory: Callable[[], Any],
    child_table: str,
    fk: ForeignKeyDef,
    page_size: int = _ORPHAN_PAGE_SIZE,
    on_page: Optional[Callable[[], None]] = None,
    *,
    # The reconnect wait and the replay backoff (``time.sleep``); injectable for tests.
    sleep: Optional[Callable[[float], None]] = None,
    # Told once where the first orphan is: the cursor of the first page holding one (a
    # tuple of key values, ``None`` for the first page), so a listing query can read that
    # one window (G142). Not called when the count finds none or the child has no key.
    on_orphans: Optional[Callable[[Optional[tuple]], None]] = None,
) -> int:
    """Count child rows whose foreign key points to a missing parent (target).

    Read-only. Run before the ``ADD CONSTRAINT`` only for a key whose parent side is
    incomplete on Aurora DSQL by design (G142: every other key is added ``NOT VALID``
    first), and after a failed Aurora DSQL validation, to explain it with a per-FK count
    (:mod:`~dsql_migrator.ui.data_migration._fk_validation`). Reuses the Validation
    orphan queries (:mod:`~dsql_migrator.core.validation_sql`).

    The child is paged by the primary key the LIVE target enforces, read once first on
    the count's own connection (G138): a single column by bounded keyset pages, a
    composite key by one range of the key per key prefix (G143,
    :func:`~dsql_migrator.core.validator.target_orphan_count_composite`), so a very large
    child never runs the orphan scan in one transaction (a single ``COUNT(*) ... NOT
    EXISTS`` would exceed DSQL's ~300s transaction limit) -- exactly as the count/checksum
    keyset pagers do -- and never by a key the target does not hold unique (the source's,
    after a re-key), whose page boundaries could skip a duplicate and undercount. Only a
    child with no primary key keeps the single scan
    (:func:`~dsql_migrator.core.validation_sql.build_orphan_count_sql`); a key that cannot
    be read raises, which leaves the foreign key to be applied by hand.

    An interrupted count RESUMES; it does not start over (G139). It runs on its own
    connection from ``connection_factory`` behind Validation's reconnecting proxy
    (:class:`~dsql_migrator.core.validator._ReconnectingTargetConnection`): a connection
    Aurora DSQL closes (its 60-minute connection limit, a dropped network, or a session the
    server ends with 57P01 / 57P02 / 57P03, G141) is replaced
    and only the statement it cut off runs again, and an XX000 runs it again on the same
    connection. Each page is its own statement with its own cursor (``WHERE pk > :last``,
    or the composite key's prefix ranges), so that replay re-reads one page. A 40001 (on
    Aurora DSQL even a read can raise OC001/40001 while the target is written
    concurrently), or an XX000 that outlasted the proxy's budget, replays the same
    statement -- the key read, one page or the single scan -- through
    :func:`_orphan_replay`. Every budget is per statement and starts again after each one
    that succeeds, so any number of drops and conflicts over an hours-long count is
    survivable, while a page that keeps failing still fails the count (the caller then
    reports the pre-check failed, never zero orphans). Before, a conflict re-ran the whole
    count from the key read and the first page, and a drop failed it, so a child too large
    to count within one connection's 60 minutes (about 25-33M rows at the measured 7-9k
    rows/s) failed every attempt.
    """
    from dsql_migrator.core.target_introspector import read_target_primary_key
    from dsql_migrator.core.validator import (
        _ReconnectingTargetConnection,
        target_orphan_count_composite,
        target_orphan_count_keyset,
    )

    connection = _ReconnectingTargetConnection(connection_factory, sleep=sleep)
    try:
        key = _orphan_replay(
            connection, lambda: read_target_primary_key(connection, child_table), sleep=sleep
        )
        if len(key) == 1:
            # Validation's pager (VAL-3); each page through _orphan_page_row, so a conflict
            # replays that page from its own bound, never the pages already counted.
            return target_orphan_count_keyset(
                connection, child_table, fk, key[0], page_size, on_page=on_page,
                page_row=lambda statement, parameters: _orphan_page_row(
                    connection, statement, parameters, sleep=sleep
                ),
                on_orphans=on_orphans,
            )
        if key:
            return target_orphan_count_composite(
                connection, child_table, fk, key, page_size, on_page=on_page,
                page_row=lambda statement, parameters: _orphan_page_row(
                    connection, statement, parameters, sleep=sleep
                ),
                on_orphans=on_orphans,
            )
        # The single scan is one statement: its only page, so one liveness report.
        if on_page is not None:
            on_page()
        row = _orphan_page_row(
            connection, build_orphan_count_sql(child_table, fk), sleep=sleep
        )
        return int(row[0]) if row and row[0] is not None else 0
    finally:
        connection.close()


def _orphan_replay(
    connection: Any,
    read: Callable[[], Any],
    *,
    sleep: Optional[Callable[[float], None]] = None,
) -> Any:
    """Run ONE read of the orphan count -- the key read, a page, the single scan -- and
    return its result, replaying only it.

    A 40001 conflict or an XX000 rolls back (a conflict can leave the transaction aborted;
    a no-op on an autocommit connection) and runs the SAME read again, with the OCC
    backoff and jitter, up to :data:`~dsql_migrator.core.occ.DEFAULT_MAX_ATTEMPTS` times;
    the last error is then raised. A dropped connection is the reconnecting proxy's: it
    replaces the connection and replays the statement itself. Any other error is raised at
    once.
    """
    from dsql_migrator.core.occ import is_occ_conflict, with_occ_retry
    from dsql_migrator.core.target_connection import is_server_unavailable

    replay = False

    def _attempt() -> Any:
        nonlocal replay
        if replay:
            try:
                connection.rollback()
            except Exception:  # noqa: BLE001 - best-effort; the replay reports the state
                pass
        replay = True
        return read()

    return with_occ_retry(
        retryable=lambda exc: is_occ_conflict(exc) or is_server_unavailable(exc),
        sleep=sleep or _time.sleep,
    )(_attempt)()


def _orphan_page_row(
    connection: Any,
    statement: Any,
    parameters: Optional[Mapping[str, Any]] = None,
    *,
    sleep: Optional[Callable[[float], None]] = None,
) -> Any:
    """Run ONE orphan-count statement (a page) on its own cursor and return its row,
    replaying only it (:func:`_orphan_replay`)."""

    def _read() -> Any:
        cursor = connection.cursor()
        try:
            if parameters is None:
                cursor.execute(statement)
            else:
                cursor.execute(statement, parameters)
            return cursor.fetchone()
        finally:
            try:
                cursor.close()
            except Exception:  # noqa: BLE001 - best-effort close
                pass

    return _orphan_replay(connection, _read, sleep=sleep)
