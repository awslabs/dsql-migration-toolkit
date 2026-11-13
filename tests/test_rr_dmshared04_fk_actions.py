# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""dmshared-04: the Full Load step's foreign-key handlers are make_fk_actions', not nested
definitions of build_data_migration_screen's render closure."""

import ast
import inspect
from types import SimpleNamespace

import dsql_migrator.ui.data_migration as dm

MOVED = (
    "_fk_apply_inputs",
    "fk_pending_count",
    "cancel_foreign_keys",
    "_fk_target_connect",
    "_fk_defs",
    "watch_foreign_key_validation",
    "_fk_revalidating",
    "validate_foreign_keys_again",
    "apply_foreign_keys_now",
    "_fk_apply_running",
)


def _defs(func) -> "set[str]":
    tree = ast.parse(inspect.getsource(func))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def test_the_handlers_are_defined_in_the_factory_and_not_in_the_screen() -> None:
    screen, factory = _defs(dm.build_data_migration_screen), _defs(dm.make_fk_actions)
    assert set(MOVED) <= factory
    assert not set(MOVED) & screen
    # The index-build watch (P4b) is not an FK action and stays in the screen, which takes
    # its DSQL connection factory off the bundle.
    assert "watch_index_builds" in screen and "watch_index_builds" not in factory
    assert "connection_factory=_fk.target_connect()" in inspect.getsource(
        dm.build_data_migration_screen
    )


def test_the_screen_builds_the_bundle_once_per_render_and_wires_it_to_the_card() -> None:
    src = inspect.getsource(dm.build_data_migration_screen)
    assert src.count("_fk = make_fk_actions(") == 1
    assert "count_memo=_fk_count_memo," in src  # the total kept across renders
    for kwarg in (
        "apply_foreign_keys=_fk.apply_now,",
        "foreign_keys_pending=_fk.pending_count,",
        "foreign_keys_running=_fk.running,",
        "cancel_foreign_keys=_fk.cancel,",
        "validate_foreign_keys_again=_fk.validate_again,",
        "foreign_keys_revalidating=_fk.revalidating,",
        "watch_foreign_keys=_fk.progress_watch,",
    ):
        assert kwarg in src


def _actions(*, target=None, job_manager=None, memo=None):
    state = dm.DataMigrationState()
    return state, dm.make_fk_actions(
        SimpleNamespace(),
        state,
        job_manager or SimpleNamespace(),
        SimpleNamespace(target_config=target, aws_profile=None),
        SimpleNamespace(preserve_foreign_keys=True, edited_target_ddls={}),
        inventory=lambda: None,
        migrator_factory=lambda _inputs: None,
        refresh=lambda: None,
        count_memo={} if memo is None else memo,
    )


def test_every_member_is_a_provider() -> None:
    _state, fk = _actions()
    assert all(
        callable(getattr(fk, field)) for field in dm.FkActions.__dataclass_fields__
    )


def test_no_target_connection_means_no_connection_factory() -> None:
    """What the FK and index-build watches read to decide they cannot run yet."""
    _state, fk = _actions()
    assert fk.target_connect() is None
    from dsql_migrator.core.models import TargetConnectionConfig

    _state, with_target = _actions(
        target=TargetConnectionConfig(
            cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
        )
    )
    assert callable(with_target.target_connect())  # built, never connected here


def test_stop_cancels_the_passs_own_job_and_refreshes() -> None:
    cancelled: list = []
    refreshed: list = []
    state = dm.DataMigrationState()
    fk = dm.make_fk_actions(
        SimpleNamespace(),
        state,
        SimpleNamespace(request_cancel=cancelled.append),
        SimpleNamespace(target_config=None, aws_profile=None),
        SimpleNamespace(preserve_foreign_keys=True, edited_target_ddls={}),
        inventory=lambda: None,
        migrator_factory=lambda _inputs: None,
        refresh=lambda: refreshed.append(1),
        count_memo={},
    )
    fk.cancel()
    assert cancelled == [] and refreshed == [1]  # nothing to cancel, still re-renders
    state.set_fk_apply_job("fk-job")
    fk.cancel()
    assert cancelled == ["fk-job"] and refreshed == [1, 1]


def test_the_count_is_advisory_and_never_breaks_the_panel() -> None:
    """A raise anywhere under it (a restored session with no source_config) reads as 0,
    and nothing is memoised for it."""
    memo: dict = {}
    _state, fk = _actions(memo=memo)
    assert fk.pending_count() == 0 and memo == {}
