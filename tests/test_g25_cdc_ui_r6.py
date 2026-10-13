# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 6, package CDC: the Stop CDC dialog and the MySQL resume cards (findings 2, 9).

Stop CDC deletes the source and the sink connectors in parallel, with no drain: on the next
Start the sink resumes from its last commit, so the changes it had not applied at the Stop
reach Aurora DSQL only from the CDC topics -- a restart is gapless only while the topics still
retain them. The Stop dialog promised "no gap" unconditionally on both engines (MySQL also
"nothing re-applied"), and so did MySQL's "Ready to resume CDC" card. Now every one states the
condition, and PostgreSQL names the route rule's way out past that period
(``cdc_pg_slot.pg_cdc_route``).

Each test FAILS on the code before this round. Fakes only: no AWS.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dsql_migrator.core import cdc_pg_slot as pg
from dsql_migrator.core.models import SourceType
from dsql_migrator.ui.data_migration._models import MigrationType

_CONDITION = (
    "Changes the sink has not applied when CDC stops are applied from the CDC topics on the "
    "next Start, so a restart is gapless only while the topics still retain them"
)
_CARD_CONDITION = (
    "Changes the sink had not applied when CDC stopped are applied from the CDC topics, so this "
    "is gapless only while the topics still retain them (their retention period on MSK)."
)


def _stop_dialog(state, session=None) -> str:
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_ui_data_migration import _RecordingUi

    ui = _RecordingUi()
    _cdc_ui._open_cdc_stop_dialog(ui, state, lambda: None, session=session)
    return " ".join(ui.texts)


def _pg_session():
    return SimpleNamespace(
        source_config=SimpleNamespace(source_type=SourceType.POSTGRES, database="app"),
        source_password=None, target_config=None, aws_profile=None,
    )


@pytest.mark.parametrize("cdc_only", [False, True], ids=["full-load-and-cdc", "cdc-only"])
def test_the_postgres_stop_dialog_states_the_topics_condition_and_the_route(cdc_only) -> None:
    # FAILS WITHOUT THE FIX: "a source connector that has streamed continues from where
    # streaming stopped: no gap, and no Full Load or start point needed again" -- left stopped
    # past the topics' retention, the sink's unapplied changes are gone and skipped.
    from dsql_migrator.ui.data_migration import DataMigrationState

    state = DataMigrationState()
    state.migration_type = MigrationType.CDC_ONLY if cdc_only else MigrationType.FULL_LOAD_AND_CDC
    body = _stop_dialog(state, _pg_session())
    route = pg.pg_cdc_route(None, started=True, cdc_only=cdc_only)
    assert (
        f"{_CONDITION}: once their retention period on MSK has passed, a Start would skip "
        f"them, so instead {route}. Restart as often as you like — but do not LEAVE it stopped."
    ) in body
    assert "no gap" not in body
    # The route is the route rule's: new topics before any Full Load re-run, no Full Load
    # step with CDC only.
    if cdc_only:
        assert "Full Load step" not in body
    else:
        assert body.index("Delete CDC infrastructure and deploy it again") < body.index(
            "Re-run Full Load")


def test_the_mysql_stop_dialog_states_the_topics_condition() -> None:
    # FAILS WITHOUT THE FIX: "no gap, nothing re-applied" -- the sink re-applies from its last
    # commit, and what it had not applied comes only from the topics.
    from dsql_migrator.ui.data_migration import DataMigrationState

    body = _stop_dialog(DataMigrationState())
    assert f"{_CONDITION} (their retention period on MSK)." in body
    assert "no gap" not in body and "nothing re-applied" not in body
    assert "no Full Load or start point needed again" in body  # the resume itself stands


def test_the_mysql_resume_cards_state_the_topics_condition() -> None:
    # FAILS WITHOUT THE FIX: "Ready to resume CDC ... no gap, no re-load" and the start-point
    # card's "... and nothing re-applied".
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_cdc_restart_start_point import _CardUi, _JM, _WM, _job, _state as _mysql
    from tests.test_ui_data_migration import _RecordingUi

    deployed = ("mysql-bin-changelog.000007", "108063011")
    start, card = _RecordingUi(), _CardUi()
    _cdc_ui._render_cdc_start_button(
        start, _mysql(deployed=deployed), _JM(_job(_WM)), lambda: None, inventory=None,
        session=None,
    )
    _cdc_ui._render_cdc_source_config_card(
        card, _mysql(deployed=deployed), _JM(_job(_WM)), lambda: None, inventory=None,
        migration_type=MigrationType.FULL_LOAD_AND_CDC, session=None,
    )
    for ui, header in ((start, "Ready to resume CDC"),
                       (card, "Resuming from the last streamed position")):
        blob = " ".join(ui.texts)
        assert header in blob
        assert _CARD_CONDITION in blob, blob
        assert "no gap" not in blob and "nothing re-applied" not in blob, blob
