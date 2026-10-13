# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G26 repair R-FL: the Full Load copy and AI grounding name the buttons the step draws.

The Full Load step's recovery row draws "Retry unfinished tables (N)" beside "Re-run all
tables"; no "Retry failed tables" button exists. Yet the Re-run all tables tooltip said "Use
'Retry failed tables' to resume only the unfinished ones instead", and the Full Load error
chat's grounding (``_FULL_LOAD_RECOVERY_CONTEXT``) told the model to use 'Retry failed tables'
and, for a failed table, "the per-table 'Reload' button" -- which only a quarantined table's
card draws (a failed table's row has AI Assist alone) -- so the assistant quoted buttons the
screen does not have. The same grounding said "CDC only carries changes forward", which reads
as the "CDC only" migration type, and Validation's grounding rendered "from a LSN watermark"
for a PostgreSQL source.

The step is rendered through the NiceGUI double (the real ``_render_full_load_step`` and
``_render_full_load_progress``). No AWS, no source, no Aurora DSQL, no model call.
"""

from __future__ import annotations

import ast
import inspect
import re
from datetime import datetime, timezone

import pytest

from dsql_migrator import config
from dsql_migrator.core import assessment_strategist
from dsql_migrator.core.assessment_strategist import (
    _FULL_LOAD_RECOVERY_CONTEXT,
    _validation_recovery_context,
    build_cdc_error_chat_system,
    build_cutover_chat_system,
    build_full_load_error_chat_system,
    build_general_chat_system,
    build_validation_chat_system,
)
from dsql_migrator.core.models import StepStatus
from dsql_migrator.ui.data_migration import MigrationType, _full_load_engine, _full_load_ui

from tests.test_g25_fl_r6 import MY, NEVER_STARTED, PG, _btn, _failed_b, _render, _state

FLC = MigrationType.FULL_LOAD_AND_CDC
FLO = MigrationType.FULL_LOAD_ONLY
ENGINES = ("MySQL", "PostgreSQL")

_STALE = "Retry failed tables"
# The Re-run all tables tooltip when nothing holds the action.
_RERUN_ALL_TIP = (
    "Re-runs ALL selected tables from a fresh snapshot (new watermark). Use 'Retry "
    "unfinished tables' to resume only the unfinished ones instead."
)


def _bullet(text: str, lead: str) -> str:
    (line,) = [ln for ln in text.splitlines() if ln.startswith(lead)]
    return line


def _step(monkeypatch, engine, mtype):
    """The real Full Load step after a run whose app.b failed (app.a loaded), on a migration
    whose new-start-point gate holds nothing: Full load only, or Full load + CDC on a stack
    the real probe read as never started."""
    state = _state(monkeypatch, NEVER_STARTED if mtype is FLC else None, mtype=mtype)
    ui, _rec = _render(monkeypatch, state=state, job=_failed_b(), engine=engine,
                       status=StepStatus.FAILED)
    return ui


# ---------------------------------------------------------------------------
# The Re-run all tables tooltip
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", [MY, PG], ids=["mysql", "postgresql"])
@pytest.mark.parametrize("mtype", [FLO, FLC], ids=["full_load_only", "full_load_and_cdc"])
def test_re_run_all_tables_names_the_retry_button_its_row_draws(monkeypatch, engine, mtype):
    # FAILS WITHOUT THE FIX: the tooltip sent the operator to 'Retry failed tables', a button
    # the row does not have.
    ui = _step(monkeypatch, engine, mtype)
    rerun = _btn(ui, "Re-run all tables")
    assert rerun.enabled is True and rerun.tips == [_RERUN_ALL_TIP]
    # The button the tooltip names is the one beside it.
    (named,) = re.findall(r"'([^']+)'", rerun.tips[0])
    retry = _btn(ui, named)
    assert retry.text == "Retry unfinished tables (1)" and retry.enabled is True
    shown = [*ui.texts, *ui.buttons, *(tip for b in ui.btns for tip in b.tips)]
    assert not [t for t in shown if _STALE in t]


@pytest.mark.parametrize(
    "module",
    [_full_load_ui, assessment_strategist, _full_load_engine, config],
    ids=["full_load_ui", "strategist", "full_load_engine", "config"],
)
def test_no_string_in_the_module_names_the_retired_label(module) -> None:
    # FAILS WITHOUT THE FIX: the tooltip (_full_load_ui), two grounding bullets plus the chat
    # builder's docstring (assessment_strategist), the 'Load failed' job error in both of
    # incomplete_load_message's branches, an anchor-lost table's recorded reason (which its
    # AI Assist chat takes as authoritative facts) and the re-read activity line
    # (_full_load_engine), and two Settings help texts (config). Comments may keep the old
    # name; strings are copy or describe it.
    tree = ast.parse(inspect.getsource(module))
    hits = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and _STALE in " ".join(node.value.split())
    ]
    assert hits == []


# ---------------------------------------------------------------------------
# The Full Load error chat's grounding
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ENGINES)
def test_the_full_load_error_grounding_names_the_retry_button_the_step_draws(
    monkeypatch, engine
) -> None:
    # FAILS WITHOUT THE FIX: both recovery bullets named 'Retry failed tables', which the
    # assistant then quoted as a button.
    system = build_full_load_error_chat_system(
        "app.b", "InternalError_: server unavailable", source_engine=engine
    )
    assert _STALE not in system
    rerun = _bullet(system, "- Because the load is idempotent and resumable")
    transient = _bullet(system, "- A transient error")
    for line in (rerun, transient):
        assert "'Retry unfinished tables'" in line, line
    # Its checklist is how one failed table is re-run on its own; still no duplicates.
    assert "untick the others to re-run Full Load for just this one" in rerun
    assert "no duplicates are created" in rerun
    # The name it gives is the button the Full Load step draws for a failed table.
    ui = _step(monkeypatch, MY if engine == "MySQL" else PG, FLO)
    assert _btn(ui, "Retry unfinished tables").text == "Retry unfinished tables (1)"


class _RowUi:
    """Records each button with the table whose row or card drew it: the last table name the
    panel printed as a badge (a failure row) or a label (a quarantine card)."""

    def __init__(self, tables) -> None:
        self._tables, self._current = set(tables), None
        self.buttons: list = []

    class _El:
        def __init__(self, ui) -> None:
            self._ui = ui

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def __getattr__(self, _name):
            return lambda *_a, **_k: self

    def _text(self, text="", *_a, **_k):
        if str(text) in self._tables:
            self._current = str(text)
        return self._El(self)

    label = badge = _text

    def button(self, text="", *_a, **_k):
        self.buttons.append((self._current, str(text)))
        return self._El(self)

    def __getattr__(self, _name):
        return lambda *_a, **_k: _RowUi._El(self)


def _panel_buttons() -> list:
    """The real per-table panel after a run where app.b FAILED and app.q loaded with a
    quarantined row: ``[(table, button label)]``."""
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.models import ChunkState, DataErrorRecord, MigrationJob
    from dsql_migrator.ui.data_migration._cdc_status import (
        full_load_error_records_for,
        full_load_error_summary_for,
        full_load_latest_messages_for,
    )
    from dsql_migrator.ui.data_migration._models import build_full_load_table_rows

    log = ErrorLogStore()
    for table, message in (
        ("app.b", "InternalError_: server unavailable"),
        ("app.q", "quarantined row pk[id=3]: datatype limit greater than 1048576 bytes"),
    ):
        log.record("j1", DataErrorRecord(table=table, chunk_id=table, error_code=None,
                                         message=message,
                                         occurred_at=datetime.now(timezone.utc)))
    job = MigrationJob(job_id="j1", status="FAILED", progress_pct=100.0, chunks=[
        ChunkState(chunk_id="app.b", status="FAILED", attempts=1),
        ChunkState(chunk_id="app.q", status="DONE", rows_loaded=2, rows_quarantined=1,
                   attempts=1),
    ])
    rows = build_full_load_table_rows(
        job, full_load_error_summary_for(log, job), full_load_latest_messages_for(log, job)
    )
    ui = _RowUi({"app.b", "app.q"})
    _full_load_ui._render_full_load_progress(
        ui, job, rows, reload_table=lambda _n: None, connections_ready=True,
        ai_error_opener=lambda *_a, **_k: None,
        quarantine_records=[
            (str(r.table), str(r.message)) for r in full_load_error_records_for(log, job)
        ],
    )
    return ui.buttons


def test_the_grounding_gives_the_per_table_reload_to_a_quarantined_table_only() -> None:
    # FAILS WITHOUT THE FIX: "use the per-table 'Reload' button" and "just Reload that table"
    # for a FAILED table. The step draws a per-table Reload only on a quarantined table's
    # card; a failed table's row has AI Assist alone (Retry unfinished tables scopes it).
    reload_lines = [ln for ln in _FULL_LOAD_RECOVERY_CONTEXT.splitlines() if "Reload" in ln]
    assert reload_lines, _FULL_LOAD_RECOVERY_CONTEXT
    for line in reload_lines:
        assert line.startswith("- 'quarantined row' entries are NOT failures"), line
    assert "that table's card has its own 'Reload' button" in reload_lines[0]
    drawn = _panel_buttons()
    assert ("app.q", "Reload") in drawn, drawn
    assert ("app.b", "AI Assist") in drawn and ("app.b", "Reload") not in drawn, drawn


@pytest.mark.parametrize("engine", ENGINES)
def test_the_standing_gap_trap_does_not_read_as_the_cdc_only_type(engine) -> None:
    # FAILS WITHOUT THE FIX: "CDC only carries changes forward" read as the CDC only
    # migration type (the wording G26 already fixed in Validation's grounding).
    system = build_full_load_error_chat_system("app.b", "boom", source_engine=engine)
    trap = _bullet(system, "- STANDING-GAP TRAP")
    assert "CDC only carries" not in system
    assert "CDC carries only the changes going forward from the Full Load watermark" in trap
    assert "it does NOT backfill history" in trap


# ---------------------------------------------------------------------------
# Validation's grounding: "an LSN"
# ---------------------------------------------------------------------------


def test_a_postgresql_grounding_says_an_lsn_watermark() -> None:
    # FAILS WITHOUT THE FIX: Validation's grounding rendered "from a LSN watermark".
    pg = "PostgreSQL"
    assert "from an LSN watermark captured at the snapshot" in _validation_recovery_context(pg)
    assert "from a binlog watermark captured at the snapshot" in _validation_recovery_context(
        "MySQL"
    )
    for system in (
        *(
            build_validation_chat_system("facts", scope=scope, source_engine=pg, table="app.b")
            for scope in ("table", "run", "readiness")
        ),
        build_full_load_error_chat_system("app.b", "boom", source_engine=pg),
        build_cdc_error_chat_system("facts", source_engine=pg),
        build_cutover_chat_system("facts", source_engine=pg),
        build_general_chat_system(source_engine=pg),
    ):
        assert not re.search(r"\ba LSN\b", system)
