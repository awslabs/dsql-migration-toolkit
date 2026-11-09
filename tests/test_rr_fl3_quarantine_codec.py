# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""FL-3: the Full Load's ``quarantined row pk[...]: reason`` message has ONE codec.

A row the loader permanently drops is recorded in the error log as
``quarantined row pk[<key>]: <reason>``. The step's quarantine count, the per-table card,
the failure/quarantine split and the AI DBA's ``list_failed_full_load_tables`` all key on
that prefix, so a writer and a reader spelling it differently would silently stop counting
dropped rows. It was written in three places (the thread path's attribute-shaped
``QuarantineRecord`` and the two process paths' dict-shaped records) and parsed in six.

Now ``_models`` owns it -- ``QUARANTINE_PREFIX``, ``quarantine_message``,
``is_quarantine_message`` and ``parse_quarantine_message`` -- and the engine's one writer,
``_record_quarantined_rows``, takes both record shapes.

Pinned here:

* golden bytes: every Full Load path writes exactly the message (and the
  ``row quarantined`` activity entry, error code, table and chunk) it wrote before the
  refactor, for each record shape, including keys with brackets, commas and quotes,
  non-ASCII text, long values (no writer truncates) and missing fields;
* every reader extracts exactly what it extracted before -- the two parsers deliberately
  keep their own split points (the primary key ends at the FIRST ``]``, the reason starts
  after the FIRST ``]: ``), which disagree only when a key's text holds a ``]``;
* the codec round-trips, and recognises exactly the messages the readers did;
* structure: no module but the codec spells the prefix, and nothing but the one writer
  builds the message.

Doubles only: no source, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import ast
import json
import pathlib
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import dsql_migrator.ui.data_migration._full_load_engine as engine
import dsql_migrator.ui.data_migration._full_load_ui as flu
from dsql_migrator.core.batched_import import QuarantineRecord
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.models import ChunkState, DataErrorRecord, MigrationJob
from dsql_migrator.ui.data_migration._full_load_engine import TableLoadResult
from dsql_migrator.ui.data_migration._models import (
    QUARANTINE_PREFIX,
    is_quarantine_message,
    parse_quarantine_message,
    quarantine_message,
)

import tests.test_g137_mysql_reader_shards as g137
from tests.test_g150_zero_dates import _Handle, _ResultMigrator, _dated_table
from tests.test_p4a_deferred_indexes import _indexes_after_load, prig  # noqa: F401

_SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"
_LITERAL = "quarantined row pk["

# ---------------------------------------------------------------------------
# The golden corpus: (primary key, reason, error code) -> the exact message bytes
# ---------------------------------------------------------------------------

_LONG_PK = "id=" + "9" * 5000
_LONG_REASON = "r" * 3000

# Each expected message is spelled out by hand (not built with the codec under test).
GOLDEN = [
    pytest.param(
        "id=3", "datatype limit greater than 1048576 bytes not supported for bytea", "54000",
        "quarantined row pk[id=3]: datatype limit greater than 1048576 bytes not supported "
        "for bytea",
        id="plain"),
    pytest.param(
        "tenant=1, id=<withheld>", "value too large", None,
        "quarantined row pk[tenant=1, id=<withheld>]: value too large",
        id="composite-withheld-no-code"),
    pytest.param(
        'a]b=1, "c[d"=2, \'e\'=None', "x]: y 'q' \"z\"", "42804",
        'quarantined row pk[a]b=1, "c[d"=2, \'e\'=None]: x]: y \'q\' "z"',
        id="brackets-commas-quotes"),
    pytest.param(
        "x]: y=1", "why", "22P02",
        "quarantined row pk[x]: y=1]: why",
        id="key-holds-the-separator"),
    pytest.param(
        "주문번호=7, ü=None", "값이 너무 큽니다 — ✓", "54000",
        "quarantined row pk[주문번호=7, ü=None]: 값이 너무 큽니다 — ✓",
        id="non-ascii"),
    pytest.param(
        _LONG_PK, _LONG_REASON, "54000",
        "quarantined row pk[id=" + "9" * 5000 + "]: " + "r" * 3000,
        id="long-untruncated"),
    pytest.param(
        "", "", None,
        "quarantined row pk[]: ",
        id="empty"),
    pytest.param(
        "id=1", "  padded reason \n", "54000",
        "quarantined row pk[id=1]:   padded reason \n",
        id="whitespace-kept"),
]


def _golden_values():
    return [p.values for p in GOLDEN]


def _activity_detail(pk, reason, code) -> str:
    # ``_log_quarantined_row``'s wording, unchanged by FL-3: pinned so the one writer is
    # seen to hand it the same key, reason and code each path did.
    return (f"row pk[{pk}] PERMANENTLY DROPPED: {reason}. "
            f"{engine._quarantine_recovery_advice(code)}")


def _capture_activity(monkeypatch) -> list:
    lines: list = []
    monkeypatch.setattr(engine, "log_activity", lambda _cat, action, **kw: lines.append(
        (action, kw.get("target"), kw.get("error_code"), kw.get("detail"))))
    return lines


def _quarantine_lines(lines) -> list:
    return [(target, code, detail) for action, target, code, detail in lines
            if action == "row quarantined"]


# ---------------------------------------------------------------------------
# 1. Golden bytes: every writer, every record shape
# ---------------------------------------------------------------------------


def test_the_thread_path_writes_the_golden_message_for_attribute_records(monkeypatch) -> None:
    lines = _capture_activity(monkeypatch)
    values = _golden_values()
    records = tuple(
        QuarantineRecord(table="orders", primary_key=pk, error_code=code, message=reason)
        for pk, reason, code, _expected in values
    )
    log = ErrorLogStore()
    outcome = engine._migrate_one_table(
        _Handle(), "job-1", _dated_table("orders"),
        _ResultMigrator(TableLoadResult(rows_loaded=2, rows_quarantined=len(records),
                                        quarantine_records=records)),
        log,
    )
    assert outcome is engine._TableLoadOutcome.QUARANTINED
    written = log.records("job-1")
    assert [r.message for r in written] == [expected for *_rest, expected in values]
    assert [(r.table, r.chunk_id, r.error_code) for r in written] == [
        ("orders", "orders", code) for _pk, _reason, code, _expected in values]
    assert _quarantine_lines(lines) == [
        ("orders", code, _activity_detail(pk, reason, code))
        for pk, reason, code, _expected in values]


def test_the_thread_path_reads_an_attribute_record_without_an_error_code(monkeypatch) -> None:
    # ``getattr(record, "error_code", None)``: a record type without the field is a None code,
    # and a None key / reason renders as the text "None" (an f-string, never a crash).
    lines = _capture_activity(monkeypatch)
    log = ErrorLogStore()
    engine._migrate_one_table(
        _Handle(), "job-1", _dated_table("orders"),
        _ResultMigrator(TableLoadResult(
            rows_loaded=1, rows_quarantined=2,
            quarantine_records=(SimpleNamespace(primary_key="id=5", message="too big"),
                                SimpleNamespace(primary_key=None, message=None)))),
        log,
    )
    assert [(r.message, r.error_code) for r in log.records("job-1")] == [
        ("quarantined row pk[id=5]: too big", None),
        ("quarantined row pk[None]: None", None),
    ]
    assert _quarantine_lines(lines) == [
        ("orders", None, _activity_detail("id=5", "too big", None)),
        ("orders", None, _activity_detail(None, None, None)),
    ]


def test_the_thread_path_still_requires_the_attribute_fields() -> None:
    # The attribute shape reads ``.primary_key`` / ``.message`` directly: a record without
    # them raises as before (it is not silently written as "None"), and nothing is written.
    log = ErrorLogStore()
    with pytest.raises(AttributeError, match="primary_key"):
        engine._migrate_one_table(
            _Handle(), "job-1", _dated_table("orders"),
            _ResultMigrator(TableLoadResult(
                rows_loaded=1, rows_quarantined=1,
                quarantine_records=(SimpleNamespace(message="m"),))),
            log,
        )
    assert not [r for r in log.records("job-1") if r.message.startswith(_LITERAL)]


def _dict_records(values) -> tuple:
    return tuple({"primary_key": pk, "message": reason, "error_code": code}
                 for pk, reason, code, _expected in values)


def test_the_whole_table_process_path_writes_the_golden_message_for_dict_records(
        prig, monkeypatch) -> None:  # noqa: F811
    values = _golden_values()
    prig.whole.update({g137.EVENTS: {"quarantine": _dict_records(values)}})
    lines = _capture_activity(monkeypatch)
    r = prig([g137._table(g137.EVENTS)], counts={g137.EVENTS: 5})
    assert [e for e in prig.events if e[0] == "unit"] == [("unit", g137.EVENTS, None)]
    written = r.log.records("j")
    assert [w.message for w in written] == [expected for *_rest, expected in values]
    assert [(w.table, w.chunk_id, w.error_code) for w in written] == [
        (g137.EVENTS, g137.EVENTS, code) for _pk, _reason, code, _expected in values]
    assert _quarantine_lines(lines) == [
        (g137.EVENTS, code, _activity_detail(pk, reason, code))
        for pk, reason, code, _expected in values]
    assert r.chunks[g137.EVENTS].rows_quarantined == len(values)


def test_a_dict_record_with_missing_keys_writes_none_on_both_process_paths(
        prig, monkeypatch) -> None:  # noqa: F811
    prig.whole.update({g137.EVENTS: {"quarantine": ({}, {"primary_key": "id=1"})}})
    lines = _capture_activity(monkeypatch)
    r = prig([g137._table(g137.EVENTS)], counts={g137.EVENTS: 5})
    assert [(w.message, w.error_code) for w in r.log.records("j")] == [
        ("quarantined row pk[None]: None", None),
        ("quarantined row pk[id=1]: None", None),
    ]
    assert _quarantine_lines(lines) == [
        (g137.EVENTS, None, _activity_detail(None, None, None)),
        (g137.EVENTS, None, _activity_detail("id=1", None, None)),
    ]


def _shard_rig(prig, monkeypatch, per_shard, statuses=None):  # noqa: F811
    """The shard path: four PK ranges of app.events, shard ``i`` returning ``per_shard[i]``."""
    statuses = statuses or {}

    def _shard(args):
        prig.events.append(("unit", args.table.name, args.shard_index))
        return engine._TableWorkerResult(
            table_name=args.table.name, status=statuses.get(args.shard_index, "DONE"),
            rows_loaded=10, shard_index=args.shard_index,
            quarantine_records=tuple(per_shard.get(args.shard_index, ())),
        )

    monkeypatch.setattr(engine, "_migrate_shard_in_process", _shard)
    lines = _capture_activity(monkeypatch)
    r = prig([g137._table(g137.EVENTS)], replace=[g137.EVENTS],
             ranges={g137.EVENTS: g137._FOUR})
    assert [e for e in prig.events if e[0] == "unit"] == [
        ("unit", g137.EVENTS, i) for i in range(4)]
    return r, lines


def test_the_shard_path_writes_the_golden_message_for_dict_records(
        prig, monkeypatch) -> None:  # noqa: F811
    values = _golden_values()
    records = _dict_records(values)
    # Spread over the shards, in order: shard 0 gets the first two, and so on.
    per_shard = {i: records[2 * i: 2 * i + 2] for i in range(4)}
    r, lines = _shard_rig(prig, monkeypatch, per_shard)
    # The shards' results are handled in the order they come back, so the records are
    # compared as a set of (message, code) -- each paired with its own record's code.
    written = r.log.records("j")
    assert sorted((w.message, w.error_code or "") for w in written) == sorted(
        (expected, code or "") for _pk, _reason, code, expected in values)
    assert {(w.table, w.chunk_id) for w in written} == {(g137.EVENTS, g137.EVENTS)}
    assert sorted(_quarantine_lines(lines), key=repr) == sorted(
        [(g137.EVENTS, code, _activity_detail(pk, reason, code))
         for pk, reason, code, _expected in values], key=repr)
    assert r.chunks[g137.EVENTS].rows_quarantined == len(values)


def test_the_shard_path_keeps_every_shards_records_when_a_sibling_failed(
        prig, monkeypatch) -> None:  # noqa: F811
    per_shard = {0: ({"primary_key": "id=1", "message": "too big", "error_code": "54000"},),
                 2: ({"primary_key": "id=600", "message": "bad", "error_code": "42804"},)}
    r, lines = _shard_rig(prig, monkeypatch, per_shard, statuses={1: "FAILED"})
    quarantined = sorted((w.message, w.error_code) for w in r.log.records("j")
                         if w.message.startswith(_LITERAL))
    assert quarantined == [("quarantined row pk[id=1]: too big", "54000"),
                           ("quarantined row pk[id=600]: bad", "42804")]
    assert sorted(_quarantine_lines(lines)) == [
        (g137.EVENTS, "42804", _activity_detail("id=600", "bad", "42804")),
        (g137.EVENTS, "54000", _activity_detail("id=1", "too big", "54000")),
    ]
    assert r.chunks[g137.EVENTS].status == "FAILED"


# ---------------------------------------------------------------------------
# 2. Every reader's extraction, on the golden messages and the edge cases
# ---------------------------------------------------------------------------

# message -> (the per-table card's primary key, its reason, recognised as a quarantine)
READS = [
    ("quarantined row pk[id=3]: datatype limit greater than 1048576 bytes",
     "id=3", "datatype limit greater than 1048576 bytes", True),
    ("quarantined row pk[tenant=1, id=<withheld>]: value too large",
     "tenant=1, id=<withheld>", "value too large", True),
    # The two parsers split differently when the key holds a "]": the key ends at the
    # FIRST "]", the reason starts after the FIRST "]: ". Kept as it was (reported).
    ('quarantined row pk[a]b=1, "c[d"=2, \'e\'=None]: x]: y \'q\' "z"',
     "a", "x]: y 'q' \"z\"", True),
    ("quarantined row pk[x]: y=1]: why", "x", "y=1]: why", True),
    ("quarantined row pk[주문번호=7, ü=None]: 값이 너무 큽니다 — ✓",
     "주문번호=7, ü=None", "값이 너무 큽니다 — ✓", True),
    ("quarantined row pk[id=" + "9" * 5000 + "]: " + "r" * 3000,
     "id=" + "9" * 5000, "r" * 3000, True),
    # An empty key is no key; the reason is stripped.
    ("quarantined row pk[]: ", None, "", True),
    ("quarantined row pk[id=1]:   padded reason \n", "id=1", "padded reason", True),
    ("quarantined row pk[None]: None", "None", "None", True),
    # Prefixed but malformed: still counted as a quarantine, no key, shown verbatim.
    ("quarantined row pk[id=3 oops", None, "quarantined row pk[id=3 oops", True),
    ("quarantined row pk[id=3] no colon", "id=3", "quarantined row pk[id=3] no colon", True),
    # Not a quarantine at all.
    ("InternalError_: server unavailable", None, "InternalError_: server unavailable", False),
    ("  quarantined row pk[id=1]: indented ", None,
     "quarantined row pk[id=1]: indented", False),
    ("Quarantined row pk[id=1]: capital", None, "Quarantined row pk[id=1]: capital", False),
    ("quarantined row pk(id=1): wrong bracket", None,
     "quarantined row pk(id=1): wrong bracket", False),
    ("index not created: ix_a", None, "index not created: ix_a", False),
    ("", None, "", False),
]


@pytest.mark.parametrize("message, pk, reason, _is_q", READS)
def test_the_card_parses_the_primary_key_and_reason_as_before(message, pk, reason, _is_q):
    assert flu._parse_quarantined_pk(message) == pk
    assert flu._quarantined_reason(message) == reason


def test_the_card_parsers_take_none() -> None:
    assert flu._parse_quarantined_pk(None) is None
    assert flu._quarantined_reason(None) == ""


def test_the_card_groups_the_golden_messages_per_table() -> None:
    entries = [("app.a", m) for m, *_rest in READS[:3]] + [("app.b", READS[11][0])]
    assert flu._group_quarantine_entries(entries) == [
        ("app.a", ["id=3", "tenant=1, id=<withheld>", "a"],
         ["datatype limit greater than 1048576 bytes", "value too large",
          "x]: y 'q' \"z\""]),
        ("app.b", [], ["InternalError_: server unavailable"]),
    ]


def _log_with(messages, *, job_id="j", table_of=lambda i: f"app.t{i}") -> ErrorLogStore:
    log = ErrorLogStore()
    for i, message in enumerate(messages):
        log.record(job_id, DataErrorRecord(
            table=table_of(i), chunk_id=table_of(i), error_code=None, message=message,
            occurred_at=datetime(2026, 11, 9, 0, 0, i, tzinfo=timezone.utc)))
    return log


# An error-log record needs a non-empty message, so the logged readers take the rest.
LOGGED = [r for r in READS if r[0]]
_RECOGNISED = sum(1 for *_rest, is_q in LOGGED if is_q)


def test_the_step_counts_the_recognised_messages_when_the_chunks_carry_none() -> None:
    messages = [m for m, *_rest in LOGGED]
    log = _log_with(messages)
    job = MigrationJob(job_id="j", chunks=[ChunkState(chunk_id=f"app.t{i}")
                                           for i in range(len(messages))])
    assert flu._quarantined_row_count(job, log) == _RECOGNISED
    # The scoped view counts only its tables' records.
    wanted = [f"app.t{i}" for i, (*_rest, is_q) in enumerate(LOGGED) if is_q][:3]
    assert flu._quarantined_row_count(job, log, tables=wanted) == 3


def test_the_run_counts_the_recognised_messages_across_its_lineage() -> None:
    messages = [m for m, *_rest in LOGGED]
    log = _log_with(messages)
    assert engine._count_quarantined_rows(log, "j") == _RECOGNISED
    # A retry: app.t0's records belong to the prior run.
    log.record("prior", DataErrorRecord(table="app.t0", chunk_id="app.t0", error_code=None,
                                        message=READS[0][0],
                                        occurred_at=datetime.now(timezone.utc)))
    assert engine._count_quarantined_rows(log, "j", {"app.t0": "prior"}) == _RECOGNISED


def test_the_panel_splits_quarantines_from_failures_as_before(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._models import build_full_load_table_rows
    from tests.test_ui_data_migration import _RecordingUi

    class _Ui(_RecordingUi):
        def linear_progress(self, *a, **k):
            return self._El(self)

    cards: list = []
    notices: list = []
    monkeypatch.setattr(flu, "_quarantine_detail_row", lambda _ui, **kw: cards.append(
        (kw["table"], list(kw["primary_keys"]), list(kw["reasons"]))))
    monkeypatch.setattr(flu, "render_notice", lambda _ui, **kw: notices.append(kw["header"]))
    # Each table's LATEST message decides whether its row is a failure or a quarantine.
    latest = {"app.q": READS[2][0], "app.m": READS[9][0], "app.f": READS[11][0],
              "app.i": READS[12][0]}
    job = MigrationJob(job_id="j", status="FAILED", progress_pct=100.0, chunks=[
        ChunkState(chunk_id=t, status="DONE" if t != "app.f" else "FAILED", attempts=1)
        for t in latest])
    rows = build_full_load_table_rows(job, None, latest)
    records = [("app.q", READS[0][0]), ("app.q", READS[2][0]), ("app.m", READS[9][0]),
               ("app.f", READS[11][0]), ("app.i", READS[12][0])]
    flu._render_full_load_progress(_Ui(), job, rows, quarantine_records=records)
    # Two real failures: app.f, and app.i whose message is indented (not the prefix).
    assert "Failure details (2)" in notices
    assert cards == [
        ("app.q", ["id=3", "a"],
         ["datatype limit greater than 1048576 bytes", "x]: y 'q' \"z\""]),
        ("app.m", [], ["quarantined row pk[id=3 oops"]),
    ]
    # Without per-row records the panel falls back to each quarantined table's latest one.
    cards.clear()
    flu._render_full_load_progress(_Ui(), job, rows)
    assert cards == [("app.q", ["a"], ["x]: y 'q' \"z\""]),
                     ("app.m", [], ["quarantined row pk[id=3 oops"])]


class _Obj:
    def __init__(self, **kw) -> None:
        self.__dict__.update(kw)


class _Store:
    def __init__(self, obj=None) -> None:
        self._obj = obj

    def get_or_create(self, _sid):
        return self._obj

    def get(self, _sid):
        return None


def test_the_ai_dba_lists_the_tables_whose_latest_message_is_a_quarantine() -> None:
    from dsql_migrator.ui.ai_tools import build_ai_tool_executor

    names = [f"app.t{i}" for i in range(len(LOGGED))]
    log = _log_with([m for m, *_rest in LOGGED])
    job = MigrationJob(job_id="j", chunks=[ChunkState(chunk_id=n, status="DONE")
                                           for n in names]
                       + [ChunkState(chunk_id="app.z", status="DONE", rows_quarantined=2)])
    execute = build_ai_tool_executor(
        session_id="s1",
        session_store=_Store(_Obj(target_config=None, target_verified=False)),
        evaluation_store=_Store(None),
        schema_conversion_store=_Store(_Obj(generated_node_ids=[], apply_results=[])),
        validation_store=_Store(_Obj(result=None)),
        data_migration_store=_Store(_Obj(job_id="j", error_log=log)),
        job_manager=_Obj(get_status=lambda _jid: job),
        full_load_rate_eta=lambda *_a, **_k: (None, None),
    )
    payload = json.loads(execute("list_failed_full_load_tables", {}))
    assert payload["status"] == "ok", payload
    expected = sorted([n for n, (*_rest, is_q) in zip(names, LOGGED) if is_q] + ["app.z"])
    assert [q["table"] for q in payload["quarantined_tables"]] == expected
    assert {q["table"]: q["rows_quarantined"] for q in payload["quarantined_tables"]}["app.z"] == 2


# ---------------------------------------------------------------------------
# 3. The codec: encode, recognise, parse
# ---------------------------------------------------------------------------


def test_the_prefix_is_the_one_every_message_has_carried() -> None:
    assert QUARANTINE_PREFIX == _LITERAL


@pytest.mark.parametrize("pk, reason, _code, expected", GOLDEN)
def test_the_codec_writes_the_golden_bytes(pk, reason, _code, expected) -> None:
    assert quarantine_message(pk, reason) == expected
    assert quarantine_message(pk, reason).encode("utf-8") == expected.encode("utf-8")


def test_the_codec_interpolates_any_object_as_an_f_string_does() -> None:
    assert quarantine_message(None, None) == "quarantined row pk[None]: None"
    assert quarantine_message(7, 3.5) == "quarantined row pk[7]: 3.5"
    assert quarantine_message(True, ValueError("boom")) == "quarantined row pk[True]: boom"


@pytest.mark.parametrize("pk, reason", [
    ("id=3", "datatype limit greater than 1048576 bytes"),
    ("tenant=1, id=<withheld>", "value too large"),
    ("주문번호=7", "값이 너무 큽니다 ✓"),
    ('"c[d"=2, \'e\'=None', "reason with ]: inside and 'quotes'"),
    (_LONG_PK, _LONG_REASON),
    ("id=1", "  kept as written \n"),
    ("id=1", ""),
])
def test_a_key_without_a_closing_bracket_round_trips(pk, reason) -> None:
    message = quarantine_message(pk, reason)
    assert is_quarantine_message(message)
    assert parse_quarantine_message(message) == (pk, reason)


def test_a_key_holding_a_bracket_parses_as_the_card_always_did() -> None:
    # The reader difference FL-3 keeps (and reports): the key ends at the first "]",
    # the reason begins after the first "]: ". Both are today's observable results.
    assert parse_quarantine_message(quarantine_message("a]b=1", "why")) == ("a", "why")
    assert parse_quarantine_message(quarantine_message("x]: y=1", "why")) == ("x", "y=1]: why")
    assert parse_quarantine_message(quarantine_message("", "why")) == (None, "why")


@pytest.mark.parametrize("message, pk, reason, is_q", READS)
def test_the_codec_matches_every_reader_on_the_corpus(message, pk, reason, is_q) -> None:
    assert is_quarantine_message(message) is is_q
    parsed = parse_quarantine_message(message)
    assert (parsed is not None) is is_q
    assert (parsed[0] if parsed else None) == pk
    if parsed is not None and parsed[1] is not None:
        assert parsed[1].strip() == reason
    else:
        assert message.strip() == reason


@pytest.mark.parametrize("value", [None, 0, False, "", [], b"quarantined row pk[1]: x",
                                   ValueError("quarantined row pk[1]: x")])
def test_recognition_reads_any_object_as_str(value) -> None:
    # As the readers did (``str(x).startswith(prefix)``): an exception carrying the message
    # is one; bytes (``b'...'`` as str) and falsy values are not.
    assert is_quarantine_message(value) is str(value).startswith(_LITERAL)
    assert (parse_quarantine_message(value) is not None) is str(value or "").startswith(
        _LITERAL)


# ---------------------------------------------------------------------------
# 4. Structure: the prefix is spelled once, the message built by one writer
# ---------------------------------------------------------------------------


def _modules():
    for path in sorted(_SRC.rglob("*.py")):
        yield path, ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _string_literals(tree) -> list:
    """Every str constant that is code (f-string parts included), not a docstring."""
    statements = {id(node.value) for node in ast.walk(tree)
                  if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)}
    return [node for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
            and id(node) not in statements]


def test_no_module_but_the_codec_spells_the_prefix() -> None:
    spelled = {
        str(path.relative_to(_SRC)): [n.lineno for n in _string_literals(tree)
                                      if _LITERAL in n.value]
        for path, tree in _modules()
    }
    spelled = {path: lines for path, lines in spelled.items() if lines}
    assert list(spelled) == ["ui/data_migration/_models.py"], spelled
    assert len(spelled["ui/data_migration/_models.py"]) == 1


def _calls_by_function(name: str) -> dict:
    """``{module:function: count}`` of the calls to ``name`` in src, by innermost function."""
    found: dict = {}

    def _visit(node, path, owner):
        for child in ast.iter_child_nodes(node):
            inner = (child.name if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                     else owner)
            if isinstance(child, ast.Call):
                called = getattr(child.func, "id", None) or getattr(child.func, "attr", None)
                if called == name:
                    key = f"{path.relative_to(_SRC)}:{owner}"
                    found[key] = found.get(key, 0) + 1
            _visit(child, path, inner)

    for path, tree in _modules():
        _visit(tree, path, "<module>")
    return found


_ENGINE = "ui/data_migration/_full_load_engine.py"


def test_only_the_one_writer_builds_the_message_and_its_audit_line() -> None:
    assert _calls_by_function("quarantine_message") == {
        f"{_ENGINE}:_record_quarantined_rows": 1}
    assert _calls_by_function("_log_quarantined_row") == {
        f"{_ENGINE}:_record_quarantined_rows": 1}


def test_every_load_path_goes_through_the_one_writer() -> None:
    # The thread path (_migrate_one_table) and, in the process pool's result loop, the
    # shard aggregation and the whole-table result: three calls, nothing else.
    calls = _calls_by_function("_record_quarantined_rows")
    assert sorted(calls.items()) == sorted({
        f"{_ENGINE}:_migrate_one_table": 1,
        f"{_ENGINE}:_migrate_tables_in_parallel": 2,
    }.items()), calls
