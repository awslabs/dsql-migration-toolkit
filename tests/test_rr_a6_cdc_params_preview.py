# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The CDC connector preview and Start CDC build ONE parameter set (review item A6b).

The connector-configuration preview's downloadable cdc-stack parameter file is built "so the
preview cannot disagree with Start" -- but it was a second copy of Start's build that did not
pass the composite record key (``message_key_columns``) to ``dispatch_source_config``. For a
re-keyed table the file therefore carried an empty ``MessageKeyColumns`` while Start CDC sends
the key (``core.cdc.build_cdc_stack_params``). Both now call one builder,
``_cdc_start_connector_params``; only the column-exclusion value stays the caller's choice
(the preview shows what Start sends, ``_capture.sent``; Start sends the exact form,
``_capture.value``, which it re-chooses against the live stack). Offline: the preview renders
into a recording UI double and Start's job body runs with ``run_cdc_start`` captured.
"""

from __future__ import annotations

from types import SimpleNamespace

from dsql_migrator.core.models import (
    ColumnDef,
    SourceInventory,
    SourceType,
    TableDef,
    TableSelection,
)
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType
from tests.test_cdc_restart_start_point import _WM, _CardUi
from tests.test_ui_data_migration import _run_start_cdc_capture

_KEY = ["tenant_id", "id"]


def _inventory() -> SourceInventory:
    return SourceInventory(
        tables=[
            TableDef(
                name="orders",
                columns=[
                    ColumnDef(name="tenant_id", mysql_type="int", nullable=False),
                    ColumnDef(name="id", mysql_type="int", nullable=False),
                ],
                primary_key=["id"],
            ),
            TableDef(
                name="customers",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            ),
        ]
    )


def _session() -> SimpleNamespace:
    # The same session _run_start_cdc_capture drives Start with (MySQL; a target with a region).
    return SimpleNamespace(
        target_config=SimpleNamespace(
            region="us-east-1",
            cluster_endpoint="ep.dsql.amazonaws.com",
            database="postgres",
            username="admin",
        ),
        aws_profile=None,
        source_password=None,
        source_config=SimpleNamespace(source_type=SourceType.MYSQL, database="app", port=3306),
        source_secret_id=None,
    )


class _FullLoadJobs:
    def get_status(self, job_id):
        assert job_id == "fl-job"
        return SimpleNamespace(job_id="fl-job", watermark=_WM, status="DONE")


def _rekeyed_state() -> DataMigrationState:
    state = DataMigrationState()
    state.set_selection(TableSelection(selected_tables=["orders", "customers"]))
    # orders is re-keyed (Schema Conversion prepended tenant_id to its key); customers is not.
    state.set_cdc_message_key_columns({"orders": list(_KEY)})
    state.job_id = "fl-job"
    return state


def test_the_preview_parameter_file_is_the_set_start_cdc_sends_for_a_rekeyed_table(
    monkeypatch,
) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui

    built: list = []
    real = _cdc_ui.dispatch_cdc_stack_params

    def _spy(*args, **kwargs):
        params = real(*args, **kwargs)
        built.append(params)
        return params

    monkeypatch.setattr(_cdc_ui, "dispatch_cdc_stack_params", _spy)
    # One Settings value for both (Start's capture helper stubs the host config it reads).
    monkeypatch.setattr(_cdc_ui, "_sink_mcu_count", lambda: 2)
    shown: list = []
    monkeypatch.setattr(_cdc_ui, "_render_cdc_params_file", lambda _ui, params: shown.append(params))
    state = _rekeyed_state()
    inventory = _inventory()

    # The connector-configuration preview (Step 3's CDC start card).
    _cdc_ui._render_cdc_source_config_card(
        _CardUi(), state, _FullLoadJobs(), lambda: None, inventory=inventory,
        migration_type=MigrationType.FULL_LOAD_AND_CDC, session=_session(),
    )
    (preview,) = shown
    # Start CDC on the same state (its job body runs; run_cdc_start is captured).
    run = _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.MYSQL, job_watermark=_WM, state=state,
        inventory=inventory,
    )
    assert run["submitted"], run["notices"]
    sent = run["kwargs"]["params"]
    assert len(built) == 2 and built[0] is preview

    # FAILED WITHOUT THE FIX: the preview's file left MessageKeyColumns empty.
    assert dict(sent.filled)["MessageKeyColumns"] == r"orders:tenant_id,id"
    assert dict(preview.filled)["MessageKeyColumns"] == dict(sent.filled)["MessageKeyColumns"]
    # ...and the whole file is the set Start CDC builds: the same values, the same placeholders.
    assert dict(preview.filled) == dict(built[1].filled)
    assert preview.placeholders == built[1].placeholders


def test_the_preview_and_start_build_through_one_builder() -> None:
    # One assembly of the source config, the sink config and the cdc-stack parameters, so a
    # parameter added to Start reaches the preview too; each caller keeps its own exclusion form.
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    preview = inspect.getsource(_cdc_ui._render_cdc_source_config_card)
    start = inspect.getsource(_cdc_ui._start_cdc_deploy)
    for src in (preview, start):
        assert src.count("_cdc_start_connector_params(") == 1
        assert "dispatch_source_config(" not in src
        assert "dispatch_cdc_stack_params(" not in src
        assert "build_sink_config(" not in src
    assert "split_column_exclude_value(_capture.sent)" in preview
    assert "split_column_exclude_value(_capture.value)" in start
