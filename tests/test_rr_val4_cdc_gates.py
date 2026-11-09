# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""VAL-4: Validation builds its three CDC gates in one place.

The dead-letter, REPLICA IDENTITY and source-schema gates (``cdc_dlq_gate_for``,
``replica_identity_gate_for``, ``catalog_drift_gate_for``) were built by 17 copied calls in
``ui/validation.py``; the tables a CHECKSUM is forced for were split by reason (dead-lettered
first, then REPLICA IDENTITY, then a source schema change) by three copies; and the held
REPLICA IDENTITY and source-schema gates were rendered by two copied functions.

These pin, per call site, what each gate is built with and in what order (a recorder wraps
the three builders where ``ui/validation.py`` looks them up), and the strings the sites
produce: the run's start and verdict lines (the start partition reads the snapshot taken when
the run starts, the verdict's the state after the CHECKSUM pass), the Options' per-reason
CHECKSUM lists and hint, the report's gates and notices, the Cut over click's re-checks and
its acknowledgement line, and the two held-gate renders. Then the helpers themselves.
Doubles only: no source, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import itertools
from datetime import timedelta
from types import SimpleNamespace

import pytest

import dsql_migrator.ui.validation as uiv
from dsql_migrator.core.models import ValidationMode

_KIND = {
    "cdc_dlq_gate_for": "dlq",
    "replica_identity_gate_for": "ri",
    "catalog_drift_gate_for": "g4",
}


@pytest.fixture
def built(monkeypatch):
    """Every gate ``ui/validation.py`` builds: ``(kind, positional count, cdc_in_use,
    release, summary given, check)`` in build order (``check`` as the builder sees it)."""
    calls: list = []
    for name, kind in _KIND.items():
        real = getattr(uiv, name)

        def _recording(*args, _real=real, _kind=kind, **kwargs):
            check = kwargs.get("check_reads", kwargs.get("check_probe", True))
            calls.append((_kind, len(args), kwargs["cdc_in_use"], kwargs["release"],
                          kwargs["summary"] is not None, check))
            return _real(*args, **kwargs)

        monkeypatch.setattr(uiv, name, _recording)
    return calls


def _ri_finding():
    from tests.test_cdc_replica_identity_gate import _finding

    return _finding()


def _g4_finding():
    from tests.test_cdc_catalog_drift_gate import _finding

    return _finding()


def _screen(monkeypatch, *, ledger=("orders",), ri=(), g4=(), ui=None):
    from tests.test_ui_validation import _g1_screen, _ModeAwareValidator

    fake = _ModeAwareValidator()
    s = _g1_screen(monkeypatch, fake, ledger_tables=ledger, ui=ui, cdc=True)
    s.mstate.cdc_replica_identity_findings = {t: _ri_finding() for t in ri}
    s.mstate.cdc_catalog_drift_findings = {t: _g4_finding() for t in g4}
    s.validator = fake
    return s


def _detail(s, action):
    (entry,) = [e for e in s.logged if e["action"] == action]
    return entry["detail"]


# ---------------------------------------------------------------------------
# The run: its start partition (the run-start snapshot), its verdict's gates
# ---------------------------------------------------------------------------

_VERDICT = (True, "clean", True, False)  # cdc_in_use, release, summary given, check
_START = "ROW_COUNT comparison of 2 table(s), source vs target"
_DONE = "ROW_COUNT verdict: MATCH — 2/2 table(s) matched, 0 mismatched"


@pytest.mark.parametrize(
    ("ledger", "ri", "g4", "started", "completed"),
    [
        (("orders",), (), ("customers",),
         _START + "; tables with dead-lettered CDC changes are also compared by CHECKSUM: "
         "orders; tables whose source schema changed are also compared by CHECKSUM: customers",
         _DONE + "; CHECKSUM pass for tables with dead-lettered CDC changes: orders; CHECKSUM "
         "pass for tables whose source schema changed: customers; cut over held: 1 table(s) "
         "with CDC dead letters lack a CHECKSUM match after their last one: orders (1); and "
         "the CDC dead-letter history can't be confirmed — one CHECKSUM run of every table "
         "clears it; cut over held: source schema changed on customers"),
        ((), ("orders",), ("orders", "customers"),
         _START + "; tables whose REPLICA IDENTITY dropped below FULL are also compared by "
         "CHECKSUM: orders; tables whose source schema changed are also compared by CHECKSUM: "
         "customers",
         _DONE + "; CHECKSUM pass for tables whose REPLICA IDENTITY dropped below FULL: "
         "orders; CHECKSUM pass for tables whose source schema changed: customers; cut over "
         "held: REPLICA IDENTITY below FULL on orders; cut over held: source schema changed "
         "on customers, orders"),
    ],
)
def test_the_run_partitions_and_builds_its_verdict_gates_as_before(
        monkeypatch, built, ledger, ri, g4, started, completed) -> None:
    s = _screen(monkeypatch, ledger=ledger, ri=ri, g4=g4)
    s.vstate.mode = ValidationMode.ROW_COUNT
    s.run()
    assert _detail(s, "validation started") == started
    assert _detail(s, "validation completed") == completed
    # The verdict line's three gates, each on its own, as of the verdict, no read / probe.
    assert built == [("dlq", 2, *_VERDICT), ("ri", 3, *_VERDICT), ("g4", 3, *_VERDICT)]
    # The CHECKSUM pass: every forced table, once.
    assert sorted(s.validator.calls[1]["tables"]) == sorted(set(ledger) | set(ri) | set(g4))


def test_each_reason_lists_its_tables_in_the_runs_order(monkeypatch) -> None:
    s = _screen(monkeypatch, ledger=(), ri=(), g4=("orders", "customers"))
    s.vstate.mode = ValidationMode.ROW_COUNT
    s.run()
    assert _detail(s, "validation started") == (
        _START + "; tables whose source schema changed are also compared by CHECKSUM: "
        "orders, customers")
    assert _detail(s, "validation completed").startswith(
        _DONE + "; CHECKSUM pass for tables whose source schema changed: orders, customers;")


def test_the_start_partition_reads_the_run_start_snapshot(monkeypatch, built) -> None:
    # A dead letter folded while the run compares is in the CHECKSUM pass and the verdict's
    # partition, but not in the start line, which was written from the snapshot before it.
    from dsql_migrator.core.cdc import CdcConnectorError

    s = _screen(monkeypatch, ledger=(), ri=(), g4=("customers",))
    s.vstate.mode = ValidationMode.ROW_COUNT

    def _fold(n):
        if n == 1:
            s.mstate.fold_cdc_dlq_ledger(
                [CdcConnectorError(table="customers", message="DLQ offset=9: late")])

    s.validator.on_call = _fold
    s.run()
    assert _detail(s, "validation started") == (
        _START + "; tables whose source schema changed are also compared by CHECKSUM: "
        "customers")
    done = _detail(s, "validation completed")
    # Each table under one reason: dead-lettered first.
    assert "; CHECKSUM pass for tables with dead-lettered CDC changes: customers;" in done
    assert "CHECKSUM pass for tables whose source schema changed" not in done


# ---------------------------------------------------------------------------
# The Validation screen: Options' per-reason lists, the hint, the read, the report gates
# ---------------------------------------------------------------------------


def _record_screen(monkeypatch):
    """Record what the screen hands ``_render_options`` / ``_render_result`` and each
    notice it renders (tone, header)."""
    seen: dict = {"notices": []}
    real_options, real_result = uiv._render_options, uiv._render_result
    real_notice = uiv.render_notice

    def _options(*a, **k):
        seen["options"] = tuple(k[key] for key in (
            "cdc_checksum_tables", "replica_identity_checksum_tables",
            "catalog_drift_checksum_tables", "cdc_history_hint"))
        return real_options(*a, **k)

    def _result(*a, **k):
        seen["result"] = tuple(
            type(k[key]).__name__ for key in
            ("cdc_gate", "replica_identity_gate", "catalog_drift_gate"))
        return real_result(*a, **k)

    def _notice(ui, *, tone, header, body="", **k):
        seen["notices"].append((tone, header))
        return real_notice(ui, tone=tone, header=header, body=body, **k)

    monkeypatch.setattr(uiv, "_render_options", _options)
    monkeypatch.setattr(uiv, "_render_result", _result)
    monkeypatch.setattr(uiv, "render_notice", _notice)
    return seen


_HINT = (
    "Cut over also needs one CHECKSUM run of every table (with Fast sweep off), because the "
    "pipeline was attached while already running, so the dead-letter history can't be "
    "confirmed."
)


@pytest.mark.parametrize(
    ("ledger", "ri", "g4", "options", "held"),
    [
        (("orders",), ("orders", "customers"), ("customers",),
         (("orders",), ("customers",), (), _HINT),
         [("warning", "Tables match — cut over waits for a CHECKSUM re-check of 1 table(s)"),
          ("error", "Tables match — cut over is held: REPLICA IDENTITY on 2 re-keyed "
                    "table(s) is below FULL"),
          ("error", "Tables match — cut over is held: the source schema changed on 1 "
                    "table(s)")]),
        ((), ("customers",), ("orders", "customers"),
         ((), ("customers",), ("orders",), ""),
         [("error", "Tables match — cut over is held: REPLICA IDENTITY on 1 re-keyed "
                    "table(s) is below FULL"),
          ("error", "Tables match — cut over is held: the source schema changed on 2 "
                    "table(s)")]),
        ((), (), (), ((), (), (), ""), [("success", "Ready for cut-over")]),
    ],
)
def test_the_screen_builds_its_gates_and_lists_as_before(
        monkeypatch, built, ledger, ri, g4, options, held) -> None:
    from tests.test_ui_validation import _ScreenUi

    s = _screen(monkeypatch, ledger=ledger, ri=ri, g4=g4, ui=_ScreenUi())
    seen = _record_screen(monkeypatch)
    s.content(lambda: None)  # no report yet
    assert seen["options"] == options
    assert built == [
        ("dlq", 2, True, "blocked", False, False),  # the Options hint: no report
        ("dlq", 2, True, "clean", False, True),     # the background read: always "clean"
    ]
    s.vstate.mode = ValidationMode.ROW_COUNT
    s.run()
    built.clear()
    seen["notices"].clear()
    s.content(lambda: None)
    assert seen["result"] == ("CdcDlqGate", "ReplicaIdentityGate", "CatalogDriftGate")
    assert built == [  # the report's three gates, no read / probe state
        ("dlq", 2, True, "clean", True, False),
        ("ri", 3, True, "clean", True, False),
        ("g4", 3, True, "clean", True, False),
    ]
    tones = [n for n in seen["notices"] if n[1].startswith(("Tables match", "Ready"))]
    assert tones == held


def test_a_gate_that_cannot_be_built_leaves_the_report_its_other_gates(monkeypatch) -> None:
    from tests.test_ui_validation import _ScreenUi

    s = _screen(monkeypatch, ui=_ScreenUi())
    s.vstate.mode = ValidationMode.ROW_COUNT
    s.run()
    seen = _record_screen(monkeypatch)

    def _broken(*_a, **_k):
        raise RuntimeError("boom")

    monkeypatch.setattr(uiv, "replica_identity_gate_for", _broken)
    s.content(lambda: None)
    assert seen["result"] == ("CdcDlqGate", "NoneType", "CatalogDriftGate")


def test_a_gate_that_cannot_be_built_still_logs_the_verdict(monkeypatch) -> None:
    s = _screen(monkeypatch, ledger=(), g4=("customers",))
    s.vstate.mode = ValidationMode.ROW_COUNT

    def _broken(*_a, **_k):
        raise RuntimeError("boom")

    monkeypatch.setattr(uiv, "cdc_dlq_gate_for", _broken)
    s.run()
    # The dead-letter clause is dropped; the source-schema clause still holds the line.
    assert _detail(s, "validation completed") == (
        _DONE + "; CHECKSUM pass for tables whose source schema changed: customers; cut "
        "over held: source schema changed on customers")


# ---------------------------------------------------------------------------
# Cut over: the click's re-checks, one gate at a time, and the acknowledgement line
# ---------------------------------------------------------------------------

_CLICK = (True, "clean", True, True)


def _cutover(monkeypatch):
    from tests.test_cdc_dlq_gate import _Cutover, _item

    s = _Cutover(monkeypatch)
    s.validate(_item(), mode=ValidationMode.CHECKSUM)
    s.landed()
    return s


def test_the_cut_over_click_rechecks_each_gate_then_logs_the_three(monkeypatch, built) -> None:
    s = _cutover(monkeypatch)
    s.render()
    assert built == [("dlq", 2, *_CLICK), ("ri", 3, *_CLICK), ("g4", 3, *_CLICK)]
    built.clear()
    s.click("I've cut over to DSQL")
    # The click: dead-letter, then REPLICA IDENTITY, then source schema, each from the
    # current state; then the acknowledgement line's three, with the release it signed off.
    assert built == [("dlq", 2, *_CLICK), ("ri", 3, *_CLICK), ("g4", 3, *_CLICK)] * 2
    (ack,) = s.acknowledged()
    assert ack["detail"] == (
        "operator acknowledged cut-over to Aurora DSQL (clean match); foreign keys: none in "
        "scope; identity sequences not synced; CDC dead letters: none recorded"
    )


def test_a_held_dead_letter_stops_the_click_before_the_other_gates(monkeypatch, built) -> None:
    s = _cutover(monkeypatch)
    s.render()
    built.clear()
    s.dead_letter("app.t", ago=timedelta(seconds=1))  # after the validation: held
    s.click("I've cut over to DSQL")
    assert built == [("dlq", 2, *_CLICK)]
    assert s.acknowledged() == []


# ---------------------------------------------------------------------------
# The held REPLICA IDENTITY / source-schema gate renders
# ---------------------------------------------------------------------------


class _RenderUi:
    def __init__(self):
        self.events: list = []

    class _Ctx:
        def __init__(self, ui):
            self.ui = ui

        def classes(self, *_a, **_k):
            return self

        def __enter__(self):
            self.ui.events.append(("row",))
            return self

        def __exit__(self, *_exc):
            return False

    class _Button:
        def __init__(self, ui, text, icon, on_click):
            self.ui, self.text = ui, text
            ui.events.append(("button", text, icon, on_click))

        def props(self, value):
            self.ui.events.append(("props", self.text, value))
            return self

    def row(self):
        return self._Ctx(self)

    def button(self, text, *, icon=None, on_click=None):
        return self._Button(self, text, icon, on_click)


_NOTICES = {
    "ri": ("ri_weak_notice", "ri_pending_notice", "ri_must_recheck_notice",
           "ri_waivable_notice"),
    "g4": ("g4_open_notice", "g4_pending_notice", "g4_must_recheck_notice",
           "g4_waivable_notice"),
}
_RENDER = {"ri": "_render_replica_identity_gate", "g4": "_render_catalog_drift_gate"}


def _expected(kind, gate, running, with_dict, waive, check):
    """The rule both renders follow (the old code's, written out)."""
    out = []
    for slot, tone in zip(("open", "pending", "must", "waivable"),
                          ("error", "info", "warning", "warning")):
        if gate.parts[slot]:
            out.append(("notice", tone, f"{kind} {slot} header", f"{kind} {slot} body"))
    show_check = gate.parts["open"]
    if not (show_check or gate.can_waive):
        return out
    out.append(("row",))
    if gate.can_waive:
        out += [("button", "Continue without this check", "rule", waive),
                ("props", "Continue without this check", "no-caps color=warning outline")]
    if show_check:
        out += [("button", "Check again", "refresh", check),
                ("props", "Check again", "no-caps color=primary")]
        if running or not gate.has_source_connection or (
                kind == "g4" and gate.probe_state == uiv.G4_PROBE_NO_BASELINE):
            out.append(("props", "Check again", "disable"))
        if with_dict:
            out.append(("captured",))
    return out


@pytest.mark.parametrize("kind", ["ri", "g4"])
def test_the_held_probe_gate_renders_follow_their_rule(monkeypatch, kind) -> None:
    events: list = []
    for name, slot in zip(_NOTICES[kind], ("open", "pending", "must", "waivable")):
        monkeypatch.setattr(
            uiv, name,
            lambda gate, _slot=slot: (f"{kind} {_slot} header", f"{kind} {_slot} body")
            if gate.parts[_slot] else None)
    monkeypatch.setattr(uiv, "render_notice",
                        lambda _ui, *, tone, header, body="", **_k:
                        events.append(("notice", tone, header, body)))
    render = getattr(uiv, _RENDER[kind])
    flags = itertools.product(*([(False, True)] * 8))
    for opened, pending, must, waivable, can_waive, source, running, with_dict in flags:
        for state in (None, uiv.G4_PROBE_NO_BASELINE):
            gate = SimpleNamespace(
                parts={"open": opened, "pending": pending, "must": must, "waivable": waivable},
                can_waive=can_waive, has_source_connection=source, probe_state=state)
            ui = _RenderUi()
            ui.events = events
            del events[:]
            holder = {} if with_dict else None
            waive, check = object(), object()
            render(ui, gate, on_waive=waive, on_check_again=check, check_running=running,
                   check_button=holder)
            got = list(events)
            if with_dict and "button" in holder:
                assert holder["button"].text == "Check again"
                got.append(("captured",))
            assert got == _expected(kind, gate, running, with_dict, waive, check), (
                kind, gate, running, with_dict)


@pytest.mark.parametrize(
    ("ledger", "ri", "g4", "lists"),
    [
        (("orders",), ("orders", "customers"), ("customers",), (("orders",), ("customers",), ())),
        ((), (), ("orders", "customers"), ((), (), ("customers", "orders"))),
    ],
)
def test_an_unresolvable_next_run_lists_every_forced_table_sorted(
        monkeypatch, ledger, ri, g4, lists) -> None:
    from tests.test_ui_validation import _ScreenUi

    s = _screen(monkeypatch, ledger=ledger, ri=ri, g4=g4, ui=_ScreenUi())
    seen = _record_screen(monkeypatch)
    resolved: list = []
    real = uiv.resolve_validation_tables

    def _unresolvable(*a, **k):
        resolved.append(1)
        if len(resolved) > 1:  # the scope card's own read succeeds; the next run's fails
            raise RuntimeError("no scope")
        return real(*a, **k)

    monkeypatch.setattr(uiv, "resolve_validation_tables", _unresolvable)
    s.content(lambda: None)
    assert seen["options"][:3] == lists


# ---------------------------------------------------------------------------
# The helpers (VAL-4) and that every site goes through them
# ---------------------------------------------------------------------------


def test_forced_checksum_partition_names_each_table_once_by_its_first_reason() -> None:
    snapshot = ({"a": 1, "b": 1}, {"b": 1, "c": 1, "d": 1}, {"c": 1, "e": 1, "a": 1})
    names = (n for n in ["e", "d", "c", "b", "a", "z"])  # any iterable, read once
    assert uiv.forced_checksum_partition(names, snapshot) == (["b", "a"], ["d", "c"], ["e"])
    assert uiv.forced_checksum_partition([], snapshot) == ([], [], [])
    assert uiv.forced_checksum_partition(["a", "a"], ({"a": 1}, {}, {})) == (["a", "a"], [], [])


def test_the_snapshot_reads_the_three_finding_sets_as_they_are() -> None:
    ms = SimpleNamespace(cdc_dlq_ledger={"a": 1}, cdc_replica_identity_findings=None)
    assert uiv._forced_checksum_snapshot(ms) == ({"a": 1}, {}, {})
    assert uiv._forced_checksum_snapshot(None) == ({}, {}, {})


@pytest.fixture
def fake_gates(monkeypatch):
    """The three builders, ``_cdc_in_use`` and ``cutover_release_state`` as recorders."""
    rec = SimpleNamespace(calls=[], in_use=[], released=[], fail=set())

    def _fake(kind):
        def _gate(*args, **kwargs):
            rec.calls.append((kind, args, kwargs))
            if kind in rec.fail:
                raise RuntimeError(kind)
            return f"{kind}-gate"
        return _gate

    monkeypatch.setattr(uiv, "cdc_dlq_gate_for", _fake("dlq"))
    monkeypatch.setattr(uiv, "replica_identity_gate_for", _fake("ri"))
    monkeypatch.setattr(uiv, "catalog_drift_gate_for", _fake("g4"))
    monkeypatch.setattr(uiv, "_cdc_in_use", lambda s: rec.in_use.append(s) or True)
    monkeypatch.setattr(
        uiv, "cutover_release_state",
        lambda summary, *, gap_accepted: rec.released.append((summary, gap_accepted))
        or "acceptable")
    return rec


def test_cdc_gates_builds_the_three_in_order_each_on_its_own(fake_gates) -> None:
    ms, vs, session = object(), SimpleNamespace(accept_explained_gap=True), object()
    assert uiv._cdc_gates(ms, vs, session, summary="S", check=False) == (
        "dlq-gate", "ri-gate", "g4-gate")
    assert fake_gates.calls == [
        ("dlq", (ms, vs), dict(cdc_in_use=True, release="acceptable", summary="S",
                               check_reads=False)),
        ("ri", (ms, vs, session), dict(cdc_in_use=True, release="acceptable", summary="S",
                                       check_probe=False)),
        ("g4", (ms, vs, session), dict(cdc_in_use=True, release="acceptable", summary="S",
                                       check_probe=False)),
    ]
    # cdc_in_use and the release are read for each gate, as each copied call did.
    assert fake_gates.in_use == [session] * 3
    assert fake_gates.released == [("S", True)] * 3


def test_cdc_gates_takes_a_given_release_and_checks_by_default(fake_gates) -> None:
    vs = SimpleNamespace(accept_explained_gap=False)
    assert uiv._cdc_gates(None, vs, None, summary=None, release="clean",
                          kinds=("g4", "dlq")) == ("g4-gate", "dlq-gate")
    assert [(k, kw["release"], kw.get("check_probe", kw.get("check_reads")))
            for k, _a, kw in fake_gates.calls] == [("g4", "clean", True), ("dlq", "clean", True)]
    assert fake_gates.released == []


def test_cdc_gates_isolated_leaves_none_for_a_gate_that_cannot_be_built(fake_gates) -> None:
    vs = SimpleNamespace(accept_explained_gap=False)
    fake_gates.fail.add("ri")
    assert uiv._cdc_gates(None, vs, None, summary=None, isolated=True) == (
        "dlq-gate", None, "g4-gate")
    with pytest.raises(RuntimeError, match="ri"):
        uiv._cdc_gates(None, vs, None, summary=None)
    with pytest.raises(ValueError):
        uiv._cdc_gates(None, vs, None, summary=None, kinds=("nope",))


def _loads_by_function(tree, names):
    """``{enclosing function path: count}`` of every load of ``names``."""
    import ast

    found: dict = {}

    def _walk(node, path):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                _walk(child, path + (child.name,))
                continue
            if isinstance(child, ast.Name) and child.id in names:
                key = ".".join(path)
                found[key] = found.get(key, 0) + 1
            _walk(child, path)

    _walk(tree, ())
    return found


def test_every_gate_is_built_through_cdc_gates_but_the_cut_over_block() -> None:
    import ast
    import inspect

    loads = _loads_by_function(ast.parse(inspect.getsource(uiv)), set(_KIND))
    # The cut-over render block's three gates wait for VAL-1 (the Cut over move).
    assert loads == {"_cdc_gates._build": 3, "build_cutover_screen.content": 3}


def test_every_forced_checksum_split_goes_through_the_partition() -> None:
    import ast
    import inspect

    loads = _loads_by_function(ast.parse(inspect.getsource(uiv)),
                               {"forced_checksum_partition"})
    assert loads == {
        "build_validation_screen.runner.work": 2,  # the run's start line, its verdict line
        "build_validation_screen._forced_checksum_scopes": 1,  # the Options' lists
    }
    source = inspect.getsource(uiv)
    for gone in ("_ledger_at_start", "_ledger_done", "_cdc_checksum_scope",
                 "_validation_dlq_gate", "_validation_ri_gate", "_validation_g4_gate"):
        assert gone not in source, gone


def test_the_two_held_gate_renders_are_one() -> None:
    import ast
    import inspect
    import textwrap

    for name in ("_render_replica_identity_gate", "_render_catalog_drift_gate"):
        tree = ast.parse(textwrap.dedent(inspect.getsource(getattr(uiv, name))))
        calls = [n.func.id for n in ast.walk(tree)
                 if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)]
        assert calls == ["_render_probe_gate"], name
