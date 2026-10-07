# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The MySQL CDC GTID start check (G19): on a ``gtid_mode=ON`` source, refuse a Start that
cannot begin exactly at its start point. Pure (verdict + copy); the read lives in
``data_migration._cdc_status.read_source_gtid_state_once``.

Debezium 2.7.4 positions by GTID only when the source's ``gtid_mode`` is exactly ``ON``
(``MySqlConnection.isGtidModeEnabled``). It then asks MySQL for every transaction NOT in the
GTID set of its offset and ignores the offset's binlog file:position, and an offset with no
GTID set makes it ask for everything the source still retains. So on an ON source the GTID
set seeded with the start point IS the start point:

- a start point with no GTID set would re-read the whole retained binary log (G1);
- one recorded while ``gtid_mode`` was not ON cannot be streamed by GTID: MySQL stops on the
  changes written without a GTID, or skips them with no error (G2);
- a set this source never executed, or one the source purged binary logs after, cannot be
  streamed either (G4).

A resume continues the stack's committed offset, so it is judged by the stack's OWN record
of the start offset its last Start offered (its ``Watermark*`` parameters, which Stop
carries forward), not by whatever start point the session holds now: a stack that was
offered a binlog position with no GTID set streamed by file:position (or was started by an
earlier version) and holds no GTID set to continue from (G1, resume variant).

With ``gtid_mode`` OFF, OFF_PERMISSIVE or ON_PERMISSIVE Debezium positions by binlog
file:position, exactly as before this check existed, so nothing is refused there. When the
mode cannot be read (no source connection, a failed read), only a start point WITHOUT a GTID
set, or one recorded while the mode was not ON, is refused (G3): otherwise Debezium uses
whichever coordinate the mode requires, and a resume continues from what Debezium committed.
On every mode, a Manual GTID set that is not a GTID set at all is refused before anything is
read: Debezium would write it into every offset it commits and fail on it at the next task
start.

Start CDC runs the check in the Start dialog and again on the job's worker thread before
anything changes (Retry CDC skips the dialog), never for a stack whose connectors exist.
Value-free (Property 7): positions, GTID sets, modes and exception class names only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dsql_migrator.core.activity_log import ActivityStatus
from dsql_migrator.core.cdc_coords import (
    gtid_mode_is_on,
    gtid_set_summary,
    normalize_gtid_set,
)
from dsql_migrator.ui.cdc_start_point_gate import (
    PROBLEM_NO_SOURCE,
    PROBLEM_READ_FAILED,
    RELOAD_RESETS_MANUAL,
    point_coord,
    point_label,
)

PROBLEM_CHECK_FAILED = "check-failed"

KIND_PROCEED = "proceed"
KIND_NO_GTID = "no-gtid"
KIND_MODE_CHANGED = "mode-changed"
KIND_UNREADABLE = "unreadable"
KIND_NOT_USABLE = "not-usable"
KIND_MALFORMED = "malformed"

# Why a GTID set is not usable (KIND_NOT_USABLE).
REASON_NOT_EXECUTED = "not-executed"
REASON_PURGED = "purged"
REASON_CHECK_FAILED = "check-failed"

# How a start that proceeds positions (the deploy log + activity line).
POSITION_GTID = "gtid"
POSITION_GTID_RESUME = "gtid-resume"
POSITION_BINLOG = "binlog"
POSITION_BINLOG_RESUME = "binlog-resume"
POSITION_UNKNOWN = "unknown"
POSITION_UNKNOWN_RESUME = "unknown-resume"

G1_HEADER = "The CDC start point has no GTID set"
G2_HEADER = "The source's gtid_mode changed since the CDC start point"
G3_HEADER = "Cannot read the source's gtid_mode"
G4_HEADER = "The source cannot stream from the start point's GTID set"
# G4 when the check itself failed: it could not be checked, not "cannot be used".
G4_CHECK_HEADER = "Cannot check the start point's GTID set against the source"
MALFORMED_HEADER = "The CDC start point's GTID set is not a valid GTID set"

BLOCK_TOOLTIP = (
    "The CDC start point cannot be used on this gtid_mode=ON source — see the notice above."
)
G3_TOOLTIP = "The source's gtid_mode could not be read — see the notice above."
G4_CHECK_TOOLTIP = (
    "The start point's GTID set could not be checked against the source — see the notice "
    "above."
)
MALFORMED_TOOLTIP = "The CDC start point's GTID set is not valid — see the notice above."

ACTION_BLOCKED = "start blocked: CDC start point's GTID position"
ACTION_POSITION = "CDC start position"

# How a resume names where it continues from (no start point is seeded).
_STOPPED_AT = "where this stack stopped"
# How the copy names a resume judged by the stack's own record of its last start offset
# (its Watermark* parameters) rather than by the session's start point.
STACK_POINT_LABEL = "the start offset this stack was last started from"

_HEADERS = {
    KIND_NO_GTID: (G1_HEADER, "fingerprint"),
    KIND_MODE_CHANGED: (G2_HEADER, "sync_problem"),
    KIND_UNREADABLE: (G3_HEADER, "link_off"),
    KIND_NOT_USABLE: (G4_HEADER, "history_toggle_off"),
    KIND_MALFORMED: (MALFORMED_HEADER, "rule"),
}

_LEAD = (
    "This source runs gtid_mode=ON, so Debezium starts streaming after the GTID set "
    "recorded with the start point, not at its binlog file:position."
)
_NO_CHANGE = " No connectors were changed."


@dataclass(frozen=True)
class GtidSourceState:
    """What one read of the source said (value-free).

    ``mode``: ``@@GLOBAL.gtid_mode`` as read, ``None`` when unread. ``within_executed`` /
    ``covers_purged``: ``GTID_SUBSET(set, @@GLOBAL.gtid_executed)`` and
    ``GTID_SUBSET(@@GLOBAL.gtid_purged, set)``, ``None`` when not checked. ``problem``:
    ``no-source-connection`` / ``read-failed`` (the mode is unknown) or ``check-failed``
    (the mode was read, the GTID set check failed), with the exception's class name.
    """

    mode: Optional[str] = None
    within_executed: Optional[bool] = None
    covers_purged: Optional[bool] = None
    problem: Optional[str] = None
    error_class: Optional[str] = None


@dataclass(frozen=True)
class GtidStartVerdict:
    """The outcome of one GTID start check (value-free).

    ``kind``: ``proceed`` (``positioning`` says how Debezium positions) or a refusal --
    ``no-gtid`` (G1), ``mode-changed`` (G2), ``unreadable`` (G3, ``reason`` is the read
    problem), ``not-usable`` (G4, ``reason`` is ``not-executed`` / ``purged`` /
    ``check-failed``) or ``malformed`` (a Manual GTID set that is not one). ``mode`` /
    ``captured_mode``: the source's mode now and the one recorded with the start point
    (upper-cased; ``None`` when unknown). ``resumes``: the start continues the stack's
    committed offset. ``stack_point``: ``binlog F:P`` of the start offset the stack's last
    Start offered, set when a resume was refused on that record rather than on the
    session's start point (the copy names it, see :func:`gtid_notice_point`).
    """

    kind: str
    positioning: Optional[str] = None
    reason: Optional[str] = None
    mode: Optional[str] = None
    captured_mode: Optional[str] = None
    resumes: bool = False
    error_class: Optional[str] = None
    stack_point: Optional[str] = None

    @property
    def blocks(self) -> bool:
        """Whether Start CDC must refuse."""
        return self.kind != KIND_PROCEED


def _seedable(seed: object) -> bool:
    return bool(getattr(seed, "binlog_file", None)) and getattr(
        seed, "binlog_position", None
    ) is not None


def gtid_start_check_due(seed: object, facts: object) -> bool:
    """Must Start CDC run the GTID start check for ``seed``? Pure.

    Not for a stack whose connectors exist (``facts.live_mode``: Retry CDC, a live
    re-Start -- they keep what they run). Otherwise for a resume of the committed offset
    and for a seedable start point (binlog file:position); a GTID set alone seeds nothing.
    """
    if facts is not None and getattr(facts, "live_mode", None):
        return False
    if facts is not None and getattr(facts, "resumes", False):
        return True
    return seed is not None and _seedable(seed)


def _upper(value: Optional[str]) -> Optional[str]:
    text = (value or "").strip().upper()
    return text or None


def mysql_gtid_start_verdict(
    state: Optional[GtidSourceState],
    *,
    seed_gtids: Optional[str],
    seed_gtid_mode: Optional[str],
    seedable: bool,
    resumes: bool,
    stack_gtids: Optional[str] = None,
    stack_point: Optional[str] = None,
    seed_is_stack_point: bool = False,
    seed_set_malformed: bool = False,
) -> GtidStartVerdict:
    """The GTID start verdict for one MySQL Start (rules R0-R6, in order). Pure.

    ``seed_gtids``: the start point's GTID set (normalized; empty when it has none).
    ``seed_gtid_mode``: the source's ``gtid_mode`` recorded with it (a Full Load watermark
    of this version; ``None`` for a Manual point or an older watermark). ``seedable``: the
    start point has a binlog file:position. ``resumes``: the start continues the stack's
    committed offset. ``stack_gtids``: the stack's own record of the start offset its last
    Start offered -- that offer's GTID set (its ``WatermarkGtids``, normalized; ``""`` when
    it offered a binlog position with none), or ``None`` when the stack records no start
    offset; ``stack_point`` names that offer (``binlog F:P``) and ``seed_is_stack_point``
    says the session's start point is that very position. ``seed_set_malformed``: the
    start point's GTID set is a Manual entry that is not a GTID set.

    0. A seeded start whose Manual GTID set is not a GTID set: refused on every mode.
    1. Mode unread: a resume proceeds, and so does a seed with a GTID set (Debezium uses
       whichever the mode requires); a seed without one is refused (G3), and so is one
       recorded while the mode was not ON (if the source is ON now, its set does not
       describe the point: a silent skip is possible).
    2. Mode not ON (OFF, OFF_PERMISSIVE, ON_PERMISSIVE): proceed by binlog file:position.
    3. ON, and the start point recorded while the mode was not ON: refused (G2) -- for a
       resume only when that point is what the stack was last started from (or the stack
       records nothing); otherwise rule 4 judges the stack's own record.
    4. ON and a resume, judged by the stack's own record when it has one: an offer with a
       GTID set continues after the GTID set Debezium committed, and so does an empty set
       recorded while ON at that very point; an offer with a binlog position and no GTID
       set is refused (G1, resume variant) -- that stack streamed by file:position or was
       started by an earlier version, so it holds no GTID set to continue from. With no
       record, the session's start point is judged the same way.
    5. ON and a seed with no GTID set (and no ON recorded with it): refused (G1).
    6. ON and a seed with a GTID set (or an empty one recorded while ON): it must be within
       the source's ``gtid_executed`` and cover its ``gtid_purged`` (G4 otherwise, also
       when the check failed or could not run).
    """
    mode = _upper(getattr(state, "mode", None)) if state is not None else None
    cap = _upper(seed_gtid_mode)
    problem = getattr(state, "problem", None) if state is not None else PROBLEM_NO_SOURCE
    error_class = getattr(state, "error_class", None) if state is not None else None
    has_set = bool(normalize_gtid_set(seed_gtids))
    # The stack's own record (None: it records no start offset).
    record = normalize_gtid_set(stack_gtids) if stack_gtids is not None else None

    def _verdict(kind: str, **extra) -> GtidStartVerdict:
        return GtidStartVerdict(
            kind=kind, mode=mode, captured_mode=cap, resumes=resumes, **extra
        )

    # R0: a Manual GTID set that is not one would be seeded into the offset whatever the
    # mode (Debezium cannot read it back at the next task start), so nothing is read first.
    if seed_set_malformed and seedable and not resumes:
        return _verdict(KIND_MALFORMED)
    # R1: the mode is unknown.
    if mode is None or problem in (PROBLEM_NO_SOURCE, PROBLEM_READ_FAILED):
        if resumes:
            return _verdict(
                KIND_PROCEED, positioning=POSITION_UNKNOWN_RESUME,
                reason=problem or PROBLEM_READ_FAILED, error_class=error_class,
            )
        if not seedable or (has_set and cap in (None, "ON")):
            return _verdict(
                KIND_PROCEED, positioning=POSITION_UNKNOWN if seedable else None,
                reason=problem or PROBLEM_READ_FAILED, error_class=error_class,
            )
        return _verdict(
            KIND_UNREADABLE, reason=problem or PROBLEM_READ_FAILED, error_class=error_class
        )
    # R2: not ON -> binlog file:position, as before.
    if not gtid_mode_is_on(mode):
        return _verdict(
            KIND_PROCEED,
            positioning=POSITION_BINLOG_RESUME if resumes else POSITION_BINLOG,
        )
    # R3: ON now, recorded while not ON. A resume continues the stack's committed offset,
    # so the recorded mode speaks for it only when the stack was last started from this
    # very point (or records no start offset at all).
    if cap is not None and cap != "ON":
        if not resumes or record is None or seed_is_stack_point:
            return _verdict(KIND_MODE_CHANGED)
    # R4: ON, a resume -- judged by what the stack itself was last offered, when it records
    # it (Stop carries Watermark* forward), not by the session's point: a session may hold
    # no point (an attached stack, a GTID set alone) or one edited since.
    if resumes:
        if record is not None:
            if record or (seed_is_stack_point and cap == "ON"):
                return _verdict(KIND_PROCEED, positioning=POSITION_GTID_RESUME)
            return _verdict(
                KIND_NO_GTID, stack_point=None if seed_is_stack_point else stack_point
            )
        if seedable and not has_set and cap is None:
            return _verdict(KIND_NO_GTID)
        return _verdict(KIND_PROCEED, positioning=POSITION_GTID_RESUME)
    if not seedable:
        return _verdict(KIND_PROCEED)
    # R5: ON, a seed with no GTID set and no ON recorded with it.
    if not has_set and cap is None:
        return _verdict(KIND_NO_GTID)
    # R6: ON, a seed with a GTID set (or an empty one recorded while ON).
    if problem == PROBLEM_CHECK_FAILED:
        return _verdict(KIND_NOT_USABLE, reason=REASON_CHECK_FAILED, error_class=error_class)
    within = getattr(state, "within_executed", None)
    covers = getattr(state, "covers_purged", None)
    if within is False:
        return _verdict(KIND_NOT_USABLE, reason=REASON_NOT_EXECUTED)
    if covers is False:
        return _verdict(KIND_NOT_USABLE, reason=REASON_PURGED)
    if within is None or covers is None:
        return _verdict(KIND_NOT_USABLE, reason=REASON_CHECK_FAILED, error_class=error_class)
    return _verdict(KIND_PROCEED, positioning=POSITION_GTID)


def _capitalized(text: str) -> str:
    return text[:1].upper() + text[1:]


def _cdc_only(migration_type: object) -> bool:
    value = getattr(migration_type, "value", migration_type)
    return str(value or "") == "cdc_only"


def _keep(start: str) -> str:
    return (
        "To keep this start point, set the source's gtid_mode back to ON_PERMISSIVE until "
        "cut over (a parameter-group change on RDS and Aurora): CDC then streams from "
        f"{start} by binlog file:position."
    )


def _new_point(since: str, migration_type: object) -> str:
    if _cdc_only(migration_type):
        return (
            "Or give CDC a new start point: choose Manual, then Fetch current position and "
            f"Use this start point. Changes made between {since} and the new point are then "
            "not replicated: correct them on Aurora DSQL and validate with CHECKSUM."
        )
    return (
        "Or give CDC a new start point: Re-run Full Load for all tables with \"Drop & "
        "reload\" — its new watermark records the GTID set with the position (Reload "
        "specific tables and Retry keep this one) — or choose Manual, then Fetch current "
        "position and Use this start point: changes to the captured tables between "
        f"{since} and that point are then not replicated, so reload them with \"Drop & "
        f"reload\" and validate with CHECKSUM. {RELOAD_RESETS_MANUAL}"
    )


def _since_clause(coord: str, migration_type: object) -> str:
    if _cdc_only(migration_type):
        return (
            f"changes made between {coord} and the new point are then not replicated: "
            "correct them on Aurora DSQL and validate with CHECKSUM"
        )
    return (
        f"changes to the captured tables between {coord} and the new point are then not "
        "replicated, so reload them with \"Drop & reload\" and validate with CHECKSUM"
    )


def _reload_suffix(migration_type: object) -> str:
    return "" if _cdc_only(migration_type) else f" {RELOAD_RESETS_MANUAL}"


def _no_gtid_body(
    verdict: GtidStartVerdict, point: str, coord: str, origin: str, migration_type: object
) -> str:
    if verdict.resumes:
        return (
            f"{_capitalized(point)} has no GTID set, and the source's gtid_mode is ON now. A "
            "stack started from such a point streamed by binlog file:position — while "
            "gtid_mode was not ON — or was started by an earlier version of the tool, so "
            "the position it recorded may hold no GTID set to continue from, and continuing "
            "by GTID could re-read changes or skip some with no error. "
            f"{_keep(_STOPPED_AT)} {_new_point(_STOPPED_AT, migration_type)}"
        )
    if origin == "manual":
        return (
            f"{_LEAD} {_capitalized(point)} has no GTID set — it was entered without one, "
            "fetched while the source's gtid_mode was not ON, or restored from a session "
            "an earlier version of the tool saved (which did not keep a GTID set with its "
            "position) — so Debezium would read "
            "the source's whole retained binary log again from its beginning and re-apply "
            "changes made before it. Choose Fetch current position and Use this start "
            "point: it records the binlog file:position and the GTID set together "
            f"({_since_clause(coord, migration_type)}). If the source's gtid_mode was ON at "
            f"{coord}, you can instead enter the GTID set it had executed at exactly that "
            "position (for example the one your dump recorded) and Use this start point. "
            f"{_keep(coord)}{_reload_suffix(migration_type)}"
        )
    return (
        f"{_LEAD} {_capitalized(point)} has no GTID set: the source reported none when it "
        "was captured, because its gtid_mode was not ON then (or no transaction had a GTID "
        "yet). Debezium would therefore read the source's whole retained binary log again "
        "from its beginning and re-apply changes the Full Load already copied. "
        f"{_keep(coord)} {_new_point(coord, migration_type)}"
    )


def _mode_changed_body(
    verdict: GtidStartVerdict, point: str, coord: str, migration_type: object
) -> str:
    captured = verdict.captured_mode or "not ON"
    lead = (
        f"{_capitalized(point)} was recorded while the source's gtid_mode was {captured}, "
        "and it is ON now."
    )
    if verdict.resumes:
        return (
            f"{lead} This stack streamed from that point by binlog file:position, so the "
            "position it recorded holds no GTID set to continue from, and continuing by "
            "GTID would re-read changes or skip some with no error. "
            f"{_keep(_STOPPED_AT)} {_new_point(_STOPPED_AT, migration_type)}"
        )
    return (
        f"{lead} With gtid_mode=ON Debezium starts from a GTID set, and the changes the "
        "source wrote without a GTID after that point cannot be streamed that way: MySQL "
        "stops the connector on them, or skips them with no error. "
        f"{_keep(coord)} {_new_point(coord, migration_type)}"
    )


def _unreadable_body(verdict: GtidStartVerdict, point: str) -> str:
    if verdict.reason == PROBLEM_NO_SOURCE:
        reason = (
            "this session has no source connection (the source password is kept in memory "
            "only, so an app restart clears it)."
        )
        exit_ = "Test the source connection on the Connect step, then Start CDC again."
    else:
        reason = f"reading it failed ({verdict.error_class or 'no value returned'})."
        exit_ = (
            "Check that the source database is reachable from the tool, then Start CDC "
            "again."
        )
    if verdict.captured_mode is not None and verdict.captured_mode != "ON":
        return (
            f"{_capitalized(point)} was recorded while the source's gtid_mode was "
            f"{verdict.captured_mode}, so Start CDC first checks that the source's "
            "gtid_mode is still not ON — with gtid_mode=ON, Debezium could not start "
            "exactly at it: it would re-read changes, or skip some with no error — and "
            f"{reason} {exit_}"
        )
    return (
        f"{_capitalized(point)} has no GTID set, so Start CDC first checks that the "
        "source's gtid_mode is not ON — with gtid_mode=ON, Debezium would read the "
        "source's whole retained binary log again from its beginning — and "
        f"{reason} {exit_}"
    )


def _check_failed_body(verdict: GtidStartVerdict, origin: str) -> str:
    # The check could not run (a dropped connection, a read timeout): nothing says the start
    # point is unusable, so the way forward is to check again, never a new start point.
    return (
        f"{_LEAD} Checking that GTID set against the source failed "
        f"({verdict.error_class or 'no result'}), so Start CDC cannot confirm that the source "
        "can stream from it."
        + (
            " If you entered the GTID set by hand, check that it is a valid MySQL GTID set."
            if origin == "manual"
            else ""
        )
        + " Check that the source database is reachable from the tool, then Start CDC again."
    )


def _malformed_body(point: str, coord: str, migration_type: object) -> str:
    return (
        f"The GTID set entered with {point} is not a GTID set Debezium can read — "
        "<server UUID>:<interval>, comma-separated, for example "
        "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100 — usually because quotes, a ';' or a "
        "SET statement were copied with it. Debezium writes the start point's GTID set into "
        "every offset it commits and cannot read this one back, so on any gtid_mode CDC "
        "would stop at the connector's next restart. Correct the GTID set, clear it if the "
        "source's gtid_mode is not ON, or choose Fetch current position and Use this start "
        "point: it records the binlog file:position and the GTID set together "
        f"({_since_clause(coord, migration_type)}).{_reload_suffix(migration_type)}"
    )


def _not_usable_body(
    verdict: GtidStartVerdict, point: str, coord: str, origin: str, migration_type: object
) -> str:
    if verdict.reason == REASON_CHECK_FAILED:
        return _check_failed_body(verdict, origin)
    if verdict.reason == REASON_NOT_EXECUTED:
        cause = (
            f"The GTID set recorded with {point} names transactions this source has not "
            "executed — it was read from another server (a source restored, cloned or "
            "switched over since)"
            + (", or entered incorrectly" if origin == "manual" else "")
            + " — so streaming from it would fail, or skip those transactions if they ever "
            "ran."
        )
    else:
        cause = (
            "The source has already purged binary logs that hold changes made after that "
            "GTID set, so those changes can no longer be streamed."
        )
    purged = verdict.reason == REASON_PURGED
    if origin == "manual":
        remedy = (
            " " + ("Choose" if purged else "Correct the GTID set you entered, or choose")
            + " Fetch current position and Use this start point: it records the GTID set "
            f"together with the position ({_since_clause(coord, migration_type)})."
            + _reload_suffix(migration_type)
        )
    elif _cdc_only(migration_type):
        remedy = (
            " Give CDC a new start point: choose Manual, then Fetch current position and "
            f"Use this start point. Changes made between {coord} and the new point are then "
            "not replicated: correct them on Aurora DSQL and validate with CHECKSUM."
        )
    else:
        remedy = (
            " Re-run Full Load for all tables with \"Drop & reload\" for a new start point"
            + (
                " (raise the source's binlog retention first, for example CALL "
                "mysql.rds_set_configuration('binlog retention hours', 168))"
                if purged
                else ""
            )
            + ", or choose Manual, then Fetch current position and Use this start point: "
            f"changes to the captured tables between {coord} and that point are then not "
            "replicated, so reload them with \"Drop & reload\" and validate with CHECKSUM. "
            + RELOAD_RESETS_MANUAL
        )
    return f"{_LEAD} {cause}{remedy}"


def gtid_start_notice(
    verdict: Optional[GtidStartVerdict],
    *,
    point_label: str,
    coord: str,
    origin: str,
    migration_type: object,
) -> Optional[tuple[str, str, str, str]]:
    """``(tone, icon, header, body)`` for a refusal (the Start dialog, the job), or None.

    ``point_label`` / ``coord`` name the start point as the start-point check does
    (:func:`~dsql_migrator.ui.cdc_start_point_gate.point_label` /
    :func:`~dsql_migrator.ui.cdc_start_point_gate.point_coord`); ``origin`` is the seed's
    (``"full_load"`` / ``"manual"``). Pure.
    """
    if verdict is None or not verdict.blocks:
        return None
    header, icon = _header_icon(verdict)
    if verdict.kind == KIND_NO_GTID:
        body = _no_gtid_body(verdict, point_label, coord, origin, migration_type)
    elif verdict.kind == KIND_MODE_CHANGED:
        body = _mode_changed_body(verdict, point_label, coord, migration_type)
    elif verdict.kind == KIND_UNREADABLE:
        body = _unreadable_body(verdict, point_label)
    elif verdict.kind == KIND_MALFORMED:
        body = _malformed_body(point_label, coord, migration_type)
    else:
        body = _not_usable_body(verdict, point_label, coord, origin, migration_type)
    return "error", icon, header, body + _NO_CHANGE


def _header_icon(verdict: GtidStartVerdict) -> tuple[str, str]:
    if verdict.kind == KIND_NOT_USABLE and verdict.reason == REASON_CHECK_FAILED:
        return G4_CHECK_HEADER, "link_off"
    return _HEADERS[verdict.kind]


def gtid_notice_point(
    verdict: Optional[GtidStartVerdict], *, seed: object, origin: str
) -> tuple[str, str]:
    """``(point_label, coord)`` the refusal copy and its activity line name. Pure.

    The session's start point (:func:`point_label` / :func:`point_coord`), except for a
    resume refused on the stack's own record of its last start offset
    (``verdict.stack_point``): that offer is named, since the session may hold no point.
    """
    stack_point = getattr(verdict, "stack_point", None) if verdict is not None else None
    if stack_point:
        return f"{STACK_POINT_LABEL} ({stack_point})", stack_point
    return point_label(seed, origin), point_coord(seed)


def gtid_start_tooltip(verdict: Optional[GtidStartVerdict]) -> Optional[str]:
    """The disabled Start button's tooltip for a refusal, else None. Pure."""
    if verdict is None or not verdict.blocks:
        return None
    if verdict.kind == KIND_UNREADABLE:
        return G3_TOOLTIP
    if verdict.kind == KIND_MALFORMED:
        return MALFORMED_TOOLTIP
    if verdict.kind == KIND_NOT_USABLE and verdict.reason == REASON_CHECK_FAILED:
        return G4_CHECK_TOOLTIP
    return BLOCK_TOOLTIP


def _why_unread(verdict: GtidStartVerdict) -> str:
    if verdict.reason == PROBLEM_NO_SOURCE:
        return "this session has no source connection"
    return f"the read failed: {verdict.error_class or 'no value returned'}"


def gtid_positioning_line(
    verdict: Optional[GtidStartVerdict], *, coord: str, seed_gtids: Optional[str]
) -> Optional[str]:
    """How a start that proceeds positions, for the deploy log and the activity log. Pure.

    ``None`` for a refusal or when nothing is positioned (no seed, no resume).
    """
    if verdict is None or verdict.blocks or verdict.positioning is None:
        return None
    summary = gtid_set_summary(seed_gtids)
    mode = verdict.mode or ""
    if verdict.positioning == POSITION_GTID:
        return (
            "Source gtid_mode=ON: Debezium starts after the start point's GTID set "
            f"{summary or '(empty)'} ({coord})."
        )
    if verdict.positioning == POSITION_GTID_RESUME:
        return "Source gtid_mode=ON: Debezium continues after the GTID set it last committed."
    if verdict.positioning == POSITION_BINLOG:
        return f"Source gtid_mode={mode}: Debezium starts at {coord} by binlog file:position."
    if verdict.positioning == POSITION_BINLOG_RESUME:
        return (
            f"Source gtid_mode={mode}: Debezium continues from the binlog file:position it "
            "last committed."
        )
    if verdict.positioning == POSITION_UNKNOWN:
        return (
            f"The source's gtid_mode could not be read ({_why_unread(verdict)}): the start "
            f"offset carries {coord} and the GTID set {summary or '(empty)'}; Debezium uses "
            "whichever the source's gtid_mode requires."
        )
    return (
        f"The source's gtid_mode could not be read ({_why_unread(verdict)}): Debezium "
        "continues from the position it last committed."
    )


def gtid_start_activity(
    verdict: Optional[GtidStartVerdict], *, coord: str, seed_gtids: Optional[str] = None
) -> Optional[tuple[str, ActivityStatus, str]]:
    """``(action, status, detail)`` for the activity log, or None. Pure; positions, sets and
    modes only."""
    if verdict is None:
        return None
    if verdict.blocks:
        header, _icon = _header_icon(verdict)
        return ACTION_BLOCKED, ActivityStatus.FAILURE, f"{header}: {coord}"
    line = gtid_positioning_line(verdict, coord=coord, seed_gtids=seed_gtids)
    if line is None:
        return None
    return ACTION_POSITION, ActivityStatus.INFO, line


__all__ = [
    "ACTION_BLOCKED",
    "ACTION_POSITION",
    "BLOCK_TOOLTIP",
    "G1_HEADER",
    "G2_HEADER",
    "G3_HEADER",
    "G3_TOOLTIP",
    "G4_CHECK_HEADER",
    "G4_CHECK_TOOLTIP",
    "G4_HEADER",
    "GtidSourceState",
    "GtidStartVerdict",
    "KIND_MALFORMED",
    "KIND_MODE_CHANGED",
    "KIND_NOT_USABLE",
    "KIND_NO_GTID",
    "KIND_PROCEED",
    "KIND_UNREADABLE",
    "MALFORMED_HEADER",
    "MALFORMED_TOOLTIP",
    "POSITION_BINLOG",
    "POSITION_BINLOG_RESUME",
    "POSITION_GTID",
    "POSITION_GTID_RESUME",
    "POSITION_UNKNOWN",
    "POSITION_UNKNOWN_RESUME",
    "PROBLEM_CHECK_FAILED",
    "PROBLEM_NO_SOURCE",
    "PROBLEM_READ_FAILED",
    "REASON_CHECK_FAILED",
    "REASON_NOT_EXECUTED",
    "REASON_PURGED",
    "STACK_POINT_LABEL",
    "gtid_notice_point",
    "gtid_positioning_line",
    "gtid_set_summary",
    "gtid_start_activity",
    "gtid_start_check_due",
    "gtid_start_notice",
    "gtid_start_tooltip",
    "mysql_gtid_start_verdict",
    "point_coord",
    "point_label",
]
