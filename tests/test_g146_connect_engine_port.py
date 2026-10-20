# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G146: the Connect page's Port follows the source engine the operator picked.

The engine tile was seeded from the operator's current pick, but the Port prefill was not:
after picking PostgreSQL and reloading the page (or opening a second tab) the PostgreSQL
tile stayed selected while Port went back to MySQL's 3306 (found in the G145 review). Both
now resolve the engine the same way: the pick, then the tested config, then the restored
snapshot's engine, then MySQL.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.models import SourceConnectionConfig, SourceType
from tests.test_ui_connect_ai_status import _render


def _port(ui) -> object:
    return next(el for el in ui.els if el.text == "Port" or el.label == "Port").value


@pytest.mark.parametrize("pick, port", [(SourceType.POSTGRES, 5432), (SourceType.MYSQL, 3306)])
def test_a_rebuilt_page_prefills_the_picked_engines_port(pick, port) -> None:
    ui, _state = _render(lambda state: state.set_source_engine_choice(pick))
    assert _port(ui) == port


def test_a_restored_postgresql_session_still_gets_its_port() -> None:
    def restored(state) -> None:
        state.restored_source_type = SourceType.POSTGRES

    ui, _state = _render(restored)
    assert _port(ui) == 5432


def test_a_tested_config_keeps_its_own_port() -> None:
    def tested(state) -> None:
        state.set_source(
            SourceConnectionConfig(source_type=SourceType.POSTGRES, host="h", port=6543, username="u"),
            None,
        )

    ui, _state = _render(tested)
    assert _port(ui) == 6543
