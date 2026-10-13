# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G26 item [3]: Validation's AI grounding gives the steps of the "How to recover" card.

The card's "Diagnose with AI" chat (and the per-table and readiness chats) is grounded by
:func:`~dsql_migrator.core.assessment_strategist._validation_recovery_context`. It said "the
idempotent loader makes a re-run safe: re-running Full Load (then CDC) backfills missing rows
without creating duplicates ... This is the recommended fix for a standing gap" and named
Drop & reload only for a CDC stack that has streamed. The card names Drop & reload for every
CDC type (rule E): CDC then starts from the new start point, so a re-run with the dialog's
default Append keeps the rows already on the target without the updates and deletes made
before it. A ROW_COUNT validation does not catch those stale rows. The validation facts carry
neither the migration type nor the CDC stack state, so the grounding names every case: Full
load only (no CDC step, rule F), a stack that has streamed or is not read yet, and a stack
that never streamed. The idempotent append is the fix only for missing rows under the
ORIGINAL start point (Retry unfinished tables, Reload specific tables).

Text checks, plus the card rendered through the NiceGUI double. No AWS, no source, no
Aurora DSQL, no model call.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.assessment_strategist import (
    _validation_recovery_context,
    build_validation_chat_system,
)
from dsql_migrator.ui.data_migration import MigrationType

from tests.test_g25_fl_r7 import _in_order, _runbook, _steps

FLC = MigrationType.FULL_LOAD_AND_CDC
FLO = MigrationType.FULL_LOAD_ONLY
CDC_ONLY = MigrationType.CDC_ONLY

ENGINES = ("MySQL", "PostgreSQL")

# The lead of each case's steps in the grounding (one nested bullet each).
_FLO = "  - Full load only:"
_STREAMED = "has streamed (or rolled back a Start)"
_NEVER = "never streamed"

# Every (migration type, CDC stack state) the card distinguishes, and the grounding's case for it.
_CASES = [
    ("flo_never", FLO, {}, _FLO),
    ("flo_streamed", FLO, {"cdc_stack_streamed": True}, _FLO),
    ("flo_unread", FLO, {"cdc_state_unread": True}, _FLO),
    ("flc_never", FLC, {}, _NEVER),
    ("flc_streamed", FLC, {"cdc_stack_streamed": True}, _STREAMED),
    ("flc_unread", FLC, {"cdc_state_unread": True}, _STREAMED),
    ("cdc_only_never", CDC_ONLY, {}, _NEVER),
    ("cdc_only_streamed", CDC_ONLY, {"cdc_stack_streamed": True}, _STREAMED),
    ("cdc_only_unread", CDC_ONLY, {"cdc_state_unread": True}, _STREAMED),
]

# The CDC actions a case's steps can name, compared in order between the card and the grounding.
_CDC_ACTIONS = ("Stop CDC", "Delete CDC infrastructure", "Start CDC")


def _clause(text: str, lead: str) -> str:
    """The single nested bullet (one case's steps) that carries ``lead``."""
    lines = [ln for ln in text.splitlines() if ln.startswith("  - ") and lead in ln]
    assert len(lines) == 1, (lead, lines)
    return lines[0]


def _rule(text: str) -> str:
    """The bullet that says what a re-run before CDC from a new start point is."""
    lead = "- Before CDC from a new start point"
    lines = [ln for ln in text.splitlines() if ln.startswith(lead)]
    assert len(lines) == 1, lines
    return lines[0]


def _actions(text: str) -> list:
    """The CDC actions ``text`` names, in the order it names them."""
    return sorted((a for a in _CDC_ACTIONS if a in text), key=text.index)


# ---------------------------------------------------------------------------
# The steps per migration type and CDC stack state
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ENGINES)
def test_full_load_only_reruns_and_revalidates_with_no_cdc_step(engine) -> None:
    # FAILS WITHOUT THE FIX: no Full load only steps; "re-running Full Load (then CDC)".
    flo = _clause(_validation_recovery_context(engine), _FLO)
    assert _in_order(flo, "Re-run Full Load", "Re-run validation")
    # The card's own words for this type: the re-run fills missing rows, no duplicates...
    assert "fills missing rows" in flo and "never creates duplicates" in flo
    # ...and a fresh copy after source writes is a Drop & reload (rule E), as the Cut over
    # runbook says.
    assert _in_order(flo, "took writes", "'Drop & reload'", "applies no update or delete")
    # Rule F: Full load only has no CDC step; CDC needs the type switched first.
    assert "has no CDC step" in flo and "switch the migration type first" in flo
    for word in ("Start CDC", "Stop CDC", "Delete CDC infrastructure", "Continue to CDC"):
        assert word not in flo, word


@pytest.mark.parametrize("engine", ENGINES)
def test_a_stack_that_has_streamed_or_is_unread_gets_new_topics_then_a_drop_and_reload(
    engine,
) -> None:
    # FAILS WITHOUT THE FIX: the order was a sentence after "This is the recommended fix",
    # with no unread state, no CDC only type switch and no Re-run validation.
    streamed = _clause(_validation_recovery_context(engine), _STREAMED)
    assert "Full load + CDC or CDC only" in streamed
    # A CDC state not read yet (after an app restart) gets this order too, as on the card.
    assert "not read yet" in streamed and "after an app restart" in streamed
    assert _in_order(
        streamed,
        "Stop CDC if it is running",
        "Delete CDC infrastructure and deploy it again",
        "with CDC only set the migration type to Full load + CDC",
        "Re-run Full Load with Drop & reload",
        "then Start CDC",
        "new Full Load's start point",
        "Re-run validation",
    )
    assert "empty CDC topics" in streamed and "refuses a new start point" in streamed
    assert "Append" not in streamed and "idempotent" not in streamed


@pytest.mark.parametrize("engine", ENGINES)
def test_a_stack_that_never_streamed_is_a_drop_and_reload_with_no_new_topics(engine) -> None:
    # FAILS WITHOUT THE FIX (item [3]): Drop & reload was named only for a stack that has
    # streamed, so for one that never did the grounding's fix was the plain re-run.
    never = _clause(_validation_recovery_context(engine), _NEVER)
    # Also the case right after the streamed steps' redeploy, as the card says.
    assert "right after that redeploy" in never
    assert _in_order(
        never,
        "with CDC only, set the migration type to Full load + CDC first",
        "Re-run Full Load choosing 'Drop & reload'",
        "then Start CDC",
        "Re-run validation",
    )
    assert "Stop CDC" not in never and "Delete CDC infrastructure" not in never
    assert "Append" not in never and "idempotent" not in never


@pytest.mark.parametrize("engine", ENGINES)
def test_it_never_recommends_an_append_rerun_before_cdc_from_a_new_start_point(engine) -> None:
    text = _validation_recovery_context(engine)
    # FAILS WITHOUT THE FIX: the grounding recommended exactly that.
    assert "makes a re-run safe" not in text
    assert "re-running Full Load (then CDC) backfills" not in text
    assert "recommended fix for a standing gap" not in text
    rule = _rule(text)
    # Any CDC type, streamed or never streamed: a Drop & reload, never the default Append.
    assert "any CDC type" in rule and "streamed or never streamed" in rule
    assert _in_order(rule, "'Drop & reload'", "NEVER a plain re-run", "default Append",
                     "updates and deletes", "never recommend a plain re-run")
    # The idempotent append is the fix only for missing rows under the ORIGINAL start point.
    assert _in_order(rule, "never recommend", "idempotent",
                     "only for missing rows under the ORIGINAL start point",
                     "Retry unfinished tables", "Reload specific tables",
                     "keeps the original start point and needs no redeploy")
    # Every idempotent / duplicate-free claim is in the Full load only steps or that sentence.
    for line in text.splitlines():
        if "idempotent" in line or "duplicates" in line:
            assert line.startswith(_FLO) or "ORIGINAL start point" in line, line


@pytest.mark.parametrize("engine", ENGINES)
def test_extra_rows_name_the_per_table_drop_and_reload_that_keeps_the_start_point(
    engine,
) -> None:
    # FAILS WITHOUT THE FIX: no remedy, so the plain re-run above was the one offered, and
    # an append never removes a row.
    text = _validation_recovery_context(engine)
    extra = next(ln for ln in text.splitlines() if "MORE rows than source" in ln)
    assert "append re-run does not clear them" in extra and "never removes one" in extra
    assert _in_order(extra, "Reload specific tables", "with CDC stopped", "'Drop & reload'",
                     "keeps the original start point")
    # CDC only switches the type first; with no Full Load record there is no table to reload.
    assert _in_order(extra, "with CDC only", "Full load + CDC", "no Full Load in this session",
                     "backfill steps above")


def test_cdc_only_in_the_grounding_always_names_the_migration_type() -> None:
    # "CDC only carries changes GOING FORWARD" read as the CDC only type once the grounding
    # names that type's steps.
    for engine in ENGINES:
        text = _validation_recovery_context(engine)
        assert "CDC only carries" not in text
        assert "CDC carries only the changes GOING FORWARD" in text


# ---------------------------------------------------------------------------
# The grounding mirrors the card
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mtype, kwargs, lead", [c[1:] for c in _CASES], ids=[c[0] for c in _CASES]
)
def test_the_grounding_names_the_cards_actions_in_the_cards_order(mtype, kwargs, lead) -> None:
    card = " ".join(_steps(_runbook(migration_type=mtype, **kwargs)))
    clause = _clause(_validation_recovery_context(), lead)
    # The same CDC actions, in the same order (none for Full load only).
    assert _actions(card) == _actions(clause), (card, clause)
    assert "Re-run validation" in card and "Re-run validation" in clause
    if mtype is FLO:
        assert _actions(clause) == []
        return
    # A CDC type's re-run is a Drop & reload, before Start CDC, on both.
    for text in (card, clause):
        assert _in_order(text, "Re-run Full Load", "Drop & reload", "Start CDC"), text
    # CDC only sets the type to Full load + CDC before its re-run, on both.
    if mtype is CDC_ONLY:
        assert _in_order(card, "Set the migration type to Full load + CDC", "Re-run Full Load")
        assert _in_order(clause, "with CDC only", "set the migration type to Full load + CDC",
                         "Re-run Full Load")
    else:
        assert "Set the migration type" not in card


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("scope", ["table", "run", "readiness"])
def test_every_validation_chat_scope_carries_the_steps(engine, scope) -> None:
    system = build_validation_chat_system(
        "Matched: 7; mismatched: 1", scope=scope, source_engine=engine, table="shop.orders"
    )
    assert _validation_recovery_context(engine) in system
    for lead in (_FLO, _STREAMED, _NEVER):
        _clause(system, lead)
    _rule(system)
    if engine == "PostgreSQL":
        assert "MySQL" not in system
