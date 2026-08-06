# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Step 2 (Schema Conversion) screen's NiceGUI-agnostic logic.

These cover the parts of the Schema Conversion screen that do not touch NiceGUI:

- Object tree assembly from the source inventory (Requirement 10.1).
- Source DDL vs converted target DDL preview pairing (Requirement 10.2) and the
  existence/conflict annotation (Requirement 10.3).
- The apply orchestration's Property 12 safety semantics: a destructive REPLACE
  is never applied without explicit confirmation, each statement is applied as a
  single DDL, OC001 conflicts are idempotently retried, per-object results are
  reported, and the apply path never touches the source (Requirements 10.4-10.8).
- Job-status -> step-status mapping and per-session state/store isolation.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.converter import (
    ConversionNoteKind,
    ConversionWarning,
    SchemaConversionResult,
    SchemaConverter,
    TableConversion,
)
from dsql_migrator.core.models import (
    ApplyResult,
    ApplyStatus,
    Classification,
    ColumnDef,
    ForeignKeyDef,
    IndexDef,
    SourceInventory,
    StepStatus,
    TableDef,
    TargetConnectionConfig,
    TargetInventory,
    TargetObjectKind,
    TargetRelation,
    TargetSchemaNode,
    ViewDef,
)
from dsql_migrator.core.occ import OCC_SQLSTATE
from dsql_migrator.ui.schema_conversion import (
    TABLE_PREFIX,
    TARGET_PREFIX,
    TRIGGER_PREFIX,
    ROUTINE_PREFIX,
    VIEW_PREFIX,
    ApplyMode,
    ApplyObject,
    ApplyOutcome,
    DsqlSchemaApplier,
    ObjectApplyError,
    ObjectApplyResult,
    ObjectApplyStatus,
    OccRetryingSchemaApplier,
    SchemaConversionState,
    SchemaConversionStore,
    DdlPreview,
    _apply_should_replace,
    _object_header_summary,
    _render_copy_ddl_button,
    _render_editable_target,
    applied_table_conversions,
    build_apply_objects,
    build_object_tree,
    build_table_preview,
    build_target_object_tree,
    ddl_equivalent,
    generate_previews,
    job_status_to_step_status,
    override_apply_objects,
    preview_for_selection,    render_source_table_ddl,
    render_target_ddl,
    replace_confirmation_message,
    run_schema_apply,
    schema_apply_is_complete,
    selected_object_names,
    split_sql_statements,
)
from dsql_migrator.ui.schema_conversion import (
    _render_pk_strategy_picker,
    build_composite_conversion,
    composite_leading_candidates,
    composite_leading_from_ddl,
    default_composite_leading,
)
from dsql_migrator.ui.schema_conversion import (
    CDC_APPLY_BLOCK_BODY,
    CDC_APPLY_BLOCK_HEADER,
    _cdc_apply_is_blocked,
)


# ---------------------------------------------------------------------------
# Fixtures / fakes
# ---------------------------------------------------------------------------


def _inventory() -> SourceInventory:
    return SourceInventory(
        tables=[
            TableDef(
                name="orders",
                columns=[
                    ColumnDef(name="id", mysql_type="int", nullable=False),
                    ColumnDef(name="customer_id", mysql_type="int", nullable=False),
                ],
                primary_key=["id"],
                indexes=[IndexDef(name="idx_customer", columns=["customer_id"])],
                foreign_keys=[
                    ForeignKeyDef(
                        name="fk_customer",
                        columns=["customer_id"],
                        referenced_table="customers",
                        referenced_columns=["id"],
                    )
                ],
            ),
            TableDef(
                name="customers",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            ),
        ],
        views=[ViewDef(name="active_orders", definition="SELECT * FROM orders")],
    )


class _FakeApplier:
    """Records the objects it was asked to apply and returns canned outcomes."""

    def __init__(self, outcome: ApplyOutcome = ApplyOutcome.CREATED) -> None:
        self.outcome = outcome
        self.calls: list[tuple[str, tuple[str, ...], ApplyMode]] = []

    def apply_object(self, object_name, ddls, on_conflict):
        self.calls.append((object_name, tuple(ddls), on_conflict))
        return self.outcome


class _FakeExecutor:
    """A target-only DDL executor recording every statement it runs.

    ``exists`` lists object names already present on the target. ``fail_once_on``
    maps a statement substring to a remaining count of OC001 conflicts to raise
    before succeeding (to exercise idempotent retry).
    """

    def __init__(
        self,
        *,
        exists: tuple[str, ...] = (),
        fail_once_on: dict[str, int] | None = None,
        hard_fail_on: str | None = None,
    ) -> None:
        self.exists = set(exists)
        self.fail_remaining = dict(fail_once_on or {})
        self.hard_fail_on = hard_fail_on
        self.executed: list[str] = []
        self.dropped: list[str] = []
        self.exists_calls: list[str] = []

    def object_exists(self, object_name: str) -> bool:
        self.exists_calls.append(object_name)
        return object_name in self.exists

    def execute_ddl(self, ddl: str) -> None:
        if self.hard_fail_on is not None and self.hard_fail_on in ddl:
            raise RuntimeError("permission denied")
        for needle, remaining in list(self.fail_remaining.items()):
            if needle in ddl and remaining > 0:
                self.fail_remaining[needle] = remaining - 1
                raise _OccConflict()
        self.executed.append(ddl)

    def drop_object(self, object_name: str) -> None:
        self.dropped.append(object_name)


class _OccConflict(Exception):
    """A fake OC001 (SQLSTATE 40001) serialization conflict."""

    sqlstate = OCC_SQLSTATE


def _no_sleep(_seconds: float) -> None:
    """Sleep stub so retry tests never wait."""


def _no_jitter() -> float:
    """Deterministic jitter stub."""
    return 0.0


def _converted():
    return SchemaConverter().convert(_inventory())


# ---------------------------------------------------------------------------
# Object tree assembly (Requirement 10.1)
# ---------------------------------------------------------------------------


def test_build_object_tree_groups_tables_and_views() -> None:
    tree = build_object_tree(_inventory(), schema_label="app")

    assert len(tree) == 1
    schema = tree[0]
    assert schema["id"] == "schema:app"
    categories = {child["id"]: child for child in schema["children"]}
    assert "category:tables:app" in categories
    assert "category:views:app" in categories

    table_ids = {node["id"] for node in categories["category:tables:app"]["children"]}
    assert table_ids == {f"{TABLE_PREFIX}orders", f"{TABLE_PREFIX}customers"}
    view_ids = {node["id"] for node in categories["category:views:app"]["children"]}
    assert view_ids == {f"{VIEW_PREFIX}active_orders"}


def test_build_object_tree_groups_by_schema_for_qualified_names() -> None:
    # Cluster-wide introspection qualifies names as "database.object"; the tree
    # groups them under separate schema nodes with short object labels.
    inventory = SourceInventory(
        tables=[
            TableDef(name="shop.orders", primary_key=["id"]),
            TableDef(name="billing.invoices", primary_key=["id"]),
        ]
    )
    tree = build_object_tree(inventory)
    schema_ids = {node["id"] for node in tree}
    assert schema_ids == {"schema:shop", "schema:billing"}
    shop = next(n for n in tree if n["id"] == "schema:shop")
    shop_tables = shop["children"][0]["children"]
    assert shop_tables[0]["id"] == f"{TABLE_PREFIX}shop.orders"
    # The label shows the short (unqualified) object name.
    assert shop_tables[0]["label"] == "orders"


def test_generate_previews_only_for_ticked_tables_and_views() -> None:
    inventory = _inventory()
    result = SchemaConverter().convert(inventory)
    node_ids = [
        "schema:app",  # ignored (not an object leaf)
        "category:tables:app",  # ignored
        f"{TABLE_PREFIX}orders",  # -> table preview
        f"{VIEW_PREFIX}active_orders",  # -> view preview
        f"{TRIGGER_PREFIX}some_trigger",  # ignored (not previewable)
    ]
    previews = generate_previews(node_ids, inventory, result)
    names = [preview.object_name for preview in previews]
    assert names == ["orders", "active_orders"]


def test_build_object_tree_table_leaves_carry_pk_indicator_metadata() -> None:
    # Each table leaf carries has_pk + a "header": "table" hook so the source
    # browser's header-table slot can show a PK indicator. Views (and other
    # object kinds) are not tables and do not carry these keys.
    inventory = SourceInventory(
        tables=[
            TableDef(name="orders", primary_key=["id"]),
            TableDef(name="audit_log", primary_key=[]),  # no PK
        ],
        views=[ViewDef(name="active_orders", definition="SELECT 1")],
    )
    tree = build_object_tree(inventory, schema_label="app")
    categories = {child["id"]: child for child in tree[0]["children"]}
    tables = {n["id"]: n for n in categories["category:tables:app"]["children"]}
    assert tables[f"{TABLE_PREFIX}orders"]["has_pk"] is True
    assert tables[f"{TABLE_PREFIX}orders"]["header"] == "table"
    assert tables[f"{TABLE_PREFIX}audit_log"]["has_pk"] is False
    # A view leaf is not a table -> no PK hook on it.
    views = categories["category:views:app"]["children"]
    assert all("header" not in v and "has_pk" not in v for v in views)


def test_build_object_tree_annotates_existing_target_objects() -> None:
    tree = build_object_tree(_inventory(), existing_objects=["orders"])
    tables = tree[0]["children"][0]["children"]
    orders = next(node for node in tables if node["id"] == f"{TABLE_PREFIX}orders")
    customers = next(
        node for node in tables if node["id"] == f"{TABLE_PREFIX}customers"
    )
    assert "exists on target" in orders["label"]
    assert "exists on target" not in customers["label"]


# ---------------------------------------------------------------------------
# Source vs target DDL diff (Requirement 10.2)
# ---------------------------------------------------------------------------


class _CopyUi:
    """Minimal NiceGUI double capturing the copy button's click handler + effects."""

    class _Clipboard:
        def __init__(self, fail: bool = False) -> None:
            self.written: list[str] = []
            self._fail = fail

        def write(self, text: str) -> None:
            if self._fail:
                raise RuntimeError("clipboard unavailable")
            self.written.append(text)

    class _Btn:
        def props(self, *_a, **_k):
            return self

        def tooltip(self, *_a, **_k):
            return self

    def __init__(self, *, clipboard_fails: bool = False) -> None:
        self.clipboard = _CopyUi._Clipboard(fail=clipboard_fails)
        self.on_click = None
        self.notifications: list[tuple[str, str]] = []

    def button(self, *, on_click=None, **_k):
        self.on_click = on_click
        return _CopyUi._Btn()

    def notify(self, message, *, type="info", **_k):  # noqa: A002 - mirror ui.notify
        self.notifications.append((message, type))


def test_copy_ddl_button_writes_text_and_confirms() -> None:
    ui = _CopyUi()
    _render_copy_ddl_button(ui, "CREATE TABLE t (id INT);", label="Target DDL")
    assert ui.on_click is not None
    ui.on_click()  # simulate the click
    assert ui.clipboard.written == ["CREATE TABLE t (id INT);"]
    assert ui.notifications == [("Target DDL copied.", "positive")]


def test_copy_ddl_button_reads_a_callable_at_click_time() -> None:
    """The editor's Copy must reflect the user's typing, not the DDL at build time.

    In Edit mode the header captured ``current`` -- the DDL when the editor was built -- so
    after typing, Copy handed back the pre-edit text (with a positive "copied" toast) while
    Apply sent the edited text: copy and apply disagreeing on the same button row. A callable
    read at click time fixes it.
    """
    buffer = {"ddl": "CREATE TABLE t (id INT);"}
    ui = _CopyUi()
    _render_copy_ddl_button(ui, lambda: buffer["ddl"], label="Target DDL")

    # The user types; the buffer changes AFTER the button was rendered.
    buffer["ddl"] = "CREATE TABLE t (id INT); -- edited"
    ui.on_click()
    assert ui.clipboard.written == ["CREATE TABLE t (id INT); -- edited"], ui.clipboard.written


def test_edit_mode_copy_passes_a_live_buffer_reader_not_a_string() -> None:
    """The edit header must hand Copy a callable over the buffer, not a build-time string.

    Pairs with test_copy_ddl_button_reads_a_callable_at_click_time (which proves the button
    honours a callable); this proves the edit branch actually passes one.
    """
    import inspect

    from dsql_migrator.ui import schema_conversion

    editing = inspect.getsource(schema_conversion._render_editable_target).split(
        "else:", 1
    )[1]
    assert "copy_ddl=lambda:" in editing, editing[:600]
    assert "get_edited_target_ddl(preview.object_name)" in editing
    # NOT the stale build-time string.
    assert "copy_ddl=current" not in editing


def test_copy_ddl_button_falls_back_when_clipboard_unavailable() -> None:
    ui = _CopyUi(clipboard_fails=True)
    _render_copy_ddl_button(ui, "CREATE TABLE t (id INT);", label="Source DDL")
    ui.on_click()
    # No crash; a calm info toast tells the user to copy from the block instead.
    assert ui.clipboard.written == []
    assert ui.notifications[0][1] == "info"
    assert "source ddl" in ui.notifications[0][0].lower()


# ---------------------------------------------------------------------------
# DDL preview pairing (Requirements 10.2, 10.3)
# ---------------------------------------------------------------------------


def test_render_source_table_ddl_includes_pk_index_and_fk() -> None:
    table = _inventory().tables[0]
    ddl = render_source_table_ddl(table)
    assert "CREATE TABLE `orders`" in ddl
    assert "PRIMARY KEY (`id`)" in ddl
    assert "KEY `idx_customer`" in ddl
    assert "FOREIGN KEY (`customer_id`)" in ddl


def test_render_target_ddl_emits_async_index_and_no_foreign_key() -> None:
    conversion = _converted().tables[0]
    target = render_target_ddl(conversion)
    assert "CREATE INDEX ASYNC" in target
    # DSQL removes foreign keys, so the converted target DDL must not declare one.
    assert "FOREIGN KEY" not in target


def test_build_table_preview_pairs_source_and_target() -> None:
    table = _inventory().tables[0]
    conversion = _converted().tables[0]
    preview = build_table_preview(table, conversion, exists_on_target=True)
    assert preview.object_name == "orders"
    assert "CREATE TABLE `orders`" in preview.source_ddl
    assert '"orders"' in preview.target_ddl
    assert preview.exists_on_target is True
    # The removed foreign key is surfaced as a conversion warning (Property 6).
    assert any("Foreign key" in w.message for w in preview.warnings)


def test_preview_for_selection_resolves_table_and_existence() -> None:
    inventory = _inventory()
    result = _converted()

    class _Existing:
        def object_exists(self, name: str) -> bool:
            return name == "orders"

    preview = preview_for_selection(
        f"{TABLE_PREFIX}orders", inventory, result, existence_checker=_Existing()
    )
    assert preview is not None
    assert preview.object_name == "orders"
    assert preview.exists_on_target is True


def test_preview_for_selection_handles_view_and_unknown_nodes() -> None:
    inventory = _inventory()
    result = _converted()

    view_preview = preview_for_selection(
        f"{VIEW_PREFIX}active_orders", inventory, result
    )
    assert view_preview is not None
    # Views are now auto-converted to a PostgreSQL CREATE VIEW (editable/applyable).
    assert "CREATE VIEW" in view_preview.target_ddl
    assert view_preview.exists_on_target is None

    assert preview_for_selection(None, inventory, result) is None
    assert preview_for_selection("category:tables", inventory, result) is None


# ---------------------------------------------------------------------------
# Apply orchestration -- Property 12 safety semantics
# ---------------------------------------------------------------------------


def test_build_apply_objects_one_per_table_with_single_ddls() -> None:
    objects = build_apply_objects(_converted())
    names = [obj.object_name for obj in objects]
    # Tables first, then auto-converted views (applied after the tables).
    assert names == ["orders", "customers", "active_orders"]
    orders = objects[0]
    # CREATE TABLE plus one CREATE INDEX ASYNC, each a single statement.
    assert len(orders.ddls) == 2
    assert orders.ddls[0].strip().lower().startswith("create table")
    assert "CREATE INDEX ASYNC" in orders.ddls[1]
    # The view is a single CREATE VIEW statement.
    view_obj = objects[2]
    assert len(view_obj.ddls) == 1
    assert view_obj.ddls[0].strip().lower().startswith("create view")


def test_override_apply_objects_uses_edited_ddl_split_into_statements() -> None:
    objects = build_apply_objects(_converted())
    edited = {
        "orders": 'CREATE TABLE "orders" (id uuid PRIMARY KEY);\n'
        'CREATE INDEX ASYNC ix ON "orders" (id);'
    }
    overridden = override_apply_objects(objects, edited)

    # Names and order are preserved; only "orders" is replaced by the edit.
    assert [o.object_name for o in overridden] == ["orders", "customers", "active_orders"]
    orders = overridden[0]
    assert orders.ddls == (
        'CREATE TABLE "orders" (id uuid PRIMARY KEY)',
        'CREATE INDEX ASYNC ix ON "orders" (id)',
    )
    # The untouched object keeps its deterministic DDL.
    assert overridden[1].ddls == objects[1].ddls


def test_override_apply_objects_ignores_objects_without_edits() -> None:
    objects = build_apply_objects(_converted())
    assert override_apply_objects(objects, {}) == objects


def test_override_apply_objects_blank_edit_keeps_generated_ddl() -> None:
    objects = build_apply_objects(_converted())
    # A blank/whitespace edit must not silently apply nothing for the object.
    overridden = override_apply_objects(objects, {"orders": "   \n  "})
    assert overridden[0].ddls == objects[0].ddls


def test_selected_object_names_strips_leaf_prefixes_and_ignores_groups() -> None:
    node_ids = [
        "schema:app",  # ignored (group)
        "category:tables:app",  # ignored (group)
        f"{TABLE_PREFIX}shop.orders",
        f"{VIEW_PREFIX}active_orders",
        f"{TRIGGER_PREFIX}audit_trg",
        f"{ROUTINE_PREFIX}calc_proc",
    ]
    assert selected_object_names(node_ids) == {
        "shop.orders",
        "active_orders",
        "audit_trg",
        "calc_proc",
    }


def test_selected_object_names_empty_for_no_object_leaves() -> None:
    assert selected_object_names(["schema:app", "category:views:app"]) == set()


def test_ddl_equivalent_ignores_trailing_semicolons_and_whitespace() -> None:
    assert ddl_equivalent(
        'CREATE TABLE "t" (id uuid);',
        '  CREATE TABLE "t" (id uuid)  ',
    )
    # Multi-statement scripts compare statement-by-statement.
    assert ddl_equivalent(
        'CREATE TABLE "t" (id uuid);\nCREATE INDEX ASYNC ix ON "t" (id);',
        'CREATE TABLE "t" (id uuid) ;  CREATE INDEX ASYNC ix ON "t" (id)',
    )


def test_ddl_equivalent_detects_real_differences() -> None:
    assert not ddl_equivalent(
        'CREATE TABLE "t" (id uuid PRIMARY KEY);',
        'CREATE TABLE "t" (id bigint PRIMARY KEY);',
    )


class _FakeCoreApplier:
    """Records per-statement apply calls and returns canned core ApplyResults."""

    def __init__(self, results: list[ApplyResult]) -> None:
        self._results = list(results)
        self.calls: list[tuple[str, object, bool]] = []
        self.dropped: list[str] = []

    def apply(self, target_ddl, on_conflict, *, confirmed=False):  # noqa: ANN001
        self.calls.append((target_ddl, on_conflict, confirmed))
        return self._results.pop(0)

    def drop(self, target_ddl) -> None:  # noqa: ANN001
        self.dropped.append(target_ddl)


def _target() -> TargetConnectionConfig:
    return TargetConnectionConfig(cluster_endpoint="c.dsql.aws", region="us-east-1")


def test_dsql_applier_reports_created_when_any_statement_created() -> None:
    core = _FakeCoreApplier(
        [
            ApplyResult(object_name="orders", status=ApplyStatus.CREATED),
            ApplyResult(object_name="orders", status=ApplyStatus.CREATED),
        ]
    )
    applier = DsqlSchemaApplier(
        _target(), core_factory=lambda target, profile: core
    )

    outcome = applier.apply_object(
        "orders", ("CREATE TABLE ...", "CREATE INDEX ASYNC ..."), ApplyMode.SKIP_IF_EXISTS
    )

    assert outcome is ApplyOutcome.CREATED
    # Each statement applied individually, with confirmation forwarded as True.
    assert len(core.calls) == 2
    assert all(call[2] is True for call in core.calls)


def test_dsql_applier_reports_skipped_when_all_statements_skipped() -> None:
    core = _FakeCoreApplier(
        [ApplyResult(object_name="orders", status=ApplyStatus.SKIPPED)]
    )
    applier = DsqlSchemaApplier(
        _target(), core_factory=lambda target, profile: core
    )

    outcome = applier.apply_object(
        "orders", ("CREATE TABLE ...",), ApplyMode.SKIP_IF_EXISTS
    )

    assert outcome is ApplyOutcome.SKIPPED


def test_dsql_applier_raises_on_failed_statement() -> None:
    core = _FakeCoreApplier(
        [
            ApplyResult(
                object_name="orders",
                status=ApplyStatus.FAILED,
                detail="target rejected the DDL",
            )
        ]
    )
    applier = DsqlSchemaApplier(
        _target(), core_factory=lambda target, profile: core
    )

    with pytest.raises(RuntimeError, match="target rejected the DDL"):
        applier.apply_object(
            "orders", ("CREATE TABLE ...",), ApplyMode.SKIP_IF_EXISTS
        )


def test_dsql_applier_maps_replace_mode_to_core_enum() -> None:
    core = _FakeCoreApplier(
        [ApplyResult(object_name="orders", status=ApplyStatus.CREATED)]
    )
    applier = DsqlSchemaApplier(
        _target(), core_factory=lambda target, profile: core
    )

    applier.apply_object("orders", ("CREATE TABLE ...",), ApplyMode.REPLACE)

    # The UI ApplyMode is mapped to the core ApplyMode by value.
    assert core.calls[0][1].value == ApplyMode.REPLACE.value


def test_dsql_applier_drop_delegates_to_core_applier() -> None:
    core = _FakeCoreApplier([])
    applier = DsqlSchemaApplier(
        _target(), core_factory=lambda target, profile: core
    )

    applier.drop("CREATE VIEW v AS SELECT 1")

    assert core.dropped == ["CREATE VIEW v AS SELECT 1"]


_CREATE_CATEGORIES = 'CREATE TABLE "categories" ("id" uuid PRIMARY KEY)'
_CREATE_IDX_PARENT = (
    'CREATE INDEX ASYNC "idx_cat_parent" ON "categories" ("parent_id")'
)
_CREATE_IDX_NAME = 'CREATE INDEX ASYNC "idx_cat_name" ON "categories" ("name")'


def test_dsql_applier_reports_each_statement_on_failure() -> None:
    """A failure carries every statement's result, not only the first failure."""
    core = _FakeCoreApplier(
        [
            ApplyResult(object_name="categories", status=ApplyStatus.CREATED),
            ApplyResult(
                object_name="idx_cat_parent",
                status=ApplyStatus.FAILED,
                detail='Apply failed: relation "idx_cat_parent" already exists',
            ),
        ]
    )
    applier = DsqlSchemaApplier(_target(), core_factory=lambda target, profile: core)

    with pytest.raises(ObjectApplyError) as excinfo:
        applier.apply_object(
            "categories",
            (_CREATE_CATEGORIES, _CREATE_IDX_PARENT),
            ApplyMode.SKIP_IF_EXISTS,
        )

    error = excinfo.value
    assert [s.status for s in error.statements] == [
        ObjectApplyStatus.CREATED,
        ObjectApplyStatus.FAILED,
    ]
    summary = str(error)
    assert "TABLE categories: CREATED" in summary
    assert "INDEX idx_cat_parent: FAILED" in summary
    # The redundant "Apply failed:" prefix is stripped from the reason.
    assert 'relation "idx_cat_parent" already exists' in summary
    assert "Apply failed:" not in summary
    # Both statements were attempted (the table succeeded, the index failed).
    assert len(core.calls) == 2


def test_dsql_applier_aborts_indexes_when_table_fails() -> None:
    """A failed structural statement aborts its dependent index statements."""
    core = _FakeCoreApplier(
        [
            ApplyResult(
                object_name="categories",
                status=ApplyStatus.FAILED,
                detail="Apply failed: boom",
            )
        ]
    )
    applier = DsqlSchemaApplier(_target(), core_factory=lambda target, profile: core)

    with pytest.raises(ObjectApplyError) as excinfo:
        applier.apply_object(
            "categories",
            (_CREATE_CATEGORIES, _CREATE_IDX_PARENT),
            ApplyMode.SKIP_IF_EXISTS,
        )

    statuses = [s.status for s in excinfo.value.statements]
    assert statuses == [ObjectApplyStatus.FAILED, ObjectApplyStatus.FAILED]
    # The index DDL was never sent to the core applier (table is a prerequisite).
    assert len(core.calls) == 1
    assert "prerequisite" in excinfo.value.statements[1].detail.lower()


def test_dsql_applier_continues_after_index_failure() -> None:
    """An index failure does not stop the remaining sibling indexes."""
    core = _FakeCoreApplier(
        [
            ApplyResult(object_name="categories", status=ApplyStatus.CREATED),
            ApplyResult(
                object_name="idx_cat_parent",
                status=ApplyStatus.FAILED,
                detail="Apply failed: bad index",
            ),
            ApplyResult(object_name="idx_cat_name", status=ApplyStatus.CREATED),
        ]
    )
    applier = DsqlSchemaApplier(_target(), core_factory=lambda target, profile: core)

    with pytest.raises(ObjectApplyError) as excinfo:
        applier.apply_object(
            "categories",
            (_CREATE_CATEGORIES, _CREATE_IDX_PARENT, _CREATE_IDX_NAME),
            ApplyMode.SKIP_IF_EXISTS,
        )

    statuses = [s.status for s in excinfo.value.statements]
    assert statuses == [
        ObjectApplyStatus.CREATED,
        ObjectApplyStatus.FAILED,
        ObjectApplyStatus.CREATED,
    ]
    # All three statements were attempted.
    assert len(core.calls) == 3


def test_run_schema_apply_surfaces_per_statement_detail() -> None:
    """run_schema_apply reports the per-statement summary, no RuntimeError prefix."""
    core = _FakeCoreApplier(
        [
            ApplyResult(object_name="categories", status=ApplyStatus.SKIPPED),
            ApplyResult(
                object_name="idx_cat_parent",
                status=ApplyStatus.FAILED,
                detail='Apply failed: relation "idx_cat_parent" already exists',
            ),
        ]
    )
    applier = DsqlSchemaApplier(_target(), core_factory=lambda target, profile: core)
    objects = [
        ApplyObject(
            object_name="categories",
            ddls=(_CREATE_CATEGORIES, _CREATE_IDX_PARENT),
        )
    ]

    results = run_schema_apply(
        objects, applier=applier, mode=ApplyMode.SKIP_IF_EXISTS, confirmed=False
    )

    assert len(results) == 1
    result = results[0]
    assert result.object_name == "categories"
    assert result.status is ObjectApplyStatus.FAILED
    assert "TABLE categories: SKIPPED" in result.detail
    assert "INDEX idx_cat_parent: FAILED" in result.detail
    assert not result.detail.startswith("RuntimeError")


def test_edited_target_ddl_state_set_get_clear() -> None:
    state = SchemaConversionState()
    assert state.get_edited_target_ddl("orders") is None

    state.set_edited_target_ddl("orders", "CREATE TABLE x (id uuid PRIMARY KEY);")
    assert state.get_edited_target_ddl("orders") is not None
    assert "orders" in state.edited_target_ddls

    # A blank edit clears the override (revert to generated DDL).
    state.set_edited_target_ddl("orders", "   ")
    assert state.get_edited_target_ddl("orders") is None

    state.set_edited_target_ddl("orders", "CREATE TABLE x (id uuid PRIMARY KEY);")
    state.clear_edited_target_ddl("orders")
    assert state.get_edited_target_ddl("orders") is None


def test_reset_generation_clears_all_prior_analysis() -> None:
    state = SchemaConversionState()
    state.generated_node_ids = ["table:orders"]
    state.set_edited_target_ddl("orders", "CREATE TABLE x (id uuid PRIMARY KEY);")
    state.replace_confirmed = True
    state.job_id = "job-1"
    state.set_error("boom")

    state.reset_generation()

    # The Reset all button must wipe the generated DDL, edits, confirmation,
    # job linkage, and prior results/error so the screen is ready to run fresh.
    assert state.generated_node_ids is None
    assert state.edited_target_ddls == {}
    assert state.all_suggestions() == []
    assert state.replace_confirmed is False
    assert state.job_id is None
    assert state.error is None
    assert state.apply_results is None


def test_run_schema_apply_reports_per_object_created() -> None:
    applier = _FakeApplier(ApplyOutcome.CREATED)
    objects = build_apply_objects(_converted())
    results = run_schema_apply(
        objects, applier=applier, mode=ApplyMode.SKIP_IF_EXISTS, confirmed=False
    )

    assert [r.status for r in results] == [
        ObjectApplyStatus.CREATED,
        ObjectApplyStatus.CREATED,
        ObjectApplyStatus.CREATED,
    ]
    assert {r.object_name for r in results} == {"orders", "customers", "active_orders"}
    assert len(applier.calls) == 3


def test_run_schema_apply_skips_when_applier_returns_skipped() -> None:
    applier = _FakeApplier(ApplyOutcome.SKIPPED)
    results = run_schema_apply(
        build_apply_objects(_converted()),
        applier=applier,
        mode=ApplyMode.SKIP_IF_EXISTS,
        confirmed=False,
    )
    assert all(r.status is ObjectApplyStatus.SKIPPED for r in results)


def test_unsupported_table_is_skipped_not_failed() -> None:
    # A table the converter could not auto-convert (e.g. MySQL spatial types) has a
    # comment placeholder as its target_ddl, not a CREATE. It must be reported
    # SKIPPED with the redesign reason and never sent to the applier (which would
    # otherwise raise a cryptic "target DDL must be a CREATE ..." SchemaApplyError).
    reason = (
        "Table app.typetest_spatial could not be auto-converted: it uses MySQL "
        "spatial column(s) (geom point) that Aurora DSQL does not support. "
        "Redesign it and reimplement manually on Aurora DSQL."
    )
    result = SchemaConversionResult(
        tables=[
            TableConversion(
                table="app.orders",
                target_ddl='CREATE TABLE "app"."orders" (id uuid PRIMARY KEY)',
            ),
            TableConversion(
                table="app.typetest_spatial",
                target_ddl=(
                    "-- Could not auto-convert table app.typetest_spatial for "
                    "Aurora DSQL; it uses MySQL spatial column(s) (geom point). "
                    "Redesign the table and reimplement it manually."
                ),
                warnings=[
                    ConversionWarning(
                        object_name="app.typetest_spatial",
                        classification=Classification.UNSUPPORTED,
                        message=reason,
                    )
                ],
            ),
        ]
    )

    objects = build_apply_objects(result)
    by_name = {o.object_name: o for o in objects}
    assert by_name["app.orders"].skip_reason is None
    assert by_name["app.typetest_spatial"].skip_reason == reason
    assert by_name["app.typetest_spatial"].ddls == ()

    applier = _FakeApplier(ApplyOutcome.CREATED)
    results = run_schema_apply(
        objects, applier=applier, mode=ApplyMode.SKIP_IF_EXISTS, confirmed=False
    )
    by_res = {r.object_name: r for r in results}
    # The unsupported table is SKIPPED with the reason; the normal one is CREATED.
    assert by_res["app.typetest_spatial"].status is ObjectApplyStatus.SKIPPED
    assert by_res["app.typetest_spatial"].detail == reason
    assert by_res["app.orders"].status is ObjectApplyStatus.CREATED
    # The applier was never asked to apply the unsupported table.
    assert [call[0] for call in applier.calls] == ["app.orders"]


def test_replace_requires_confirmation_and_never_calls_applier() -> None:
    """Property 12: a destructive REPLACE must not apply without confirmation."""
    applier = _FakeApplier(ApplyOutcome.CREATED)
    objects = build_apply_objects(_converted())

    results = run_schema_apply(
        objects, applier=applier, mode=ApplyMode.REPLACE, confirmed=False
    )

    assert applier.calls == []  # nothing was applied
    assert all(r.status is ObjectApplyStatus.SKIPPED for r in results)
    assert all("not confirmed" in r.detail for r in results)


# ---------------------------------------------------------------------------
# schema_apply_is_complete -- DONE-readiness (per-object inline apply)
# ---------------------------------------------------------------------------


def _res(name: str, status: ObjectApplyStatus) -> ObjectApplyResult:
    return ObjectApplyResult(object_name=name, status=status)


def test_schema_apply_complete_when_all_skipped() -> None:
    # The reported bug: every selected table already exists on target -> all
    # SKIPPED. The schema IS ready, so the step must be DONE (Next unlocks).
    names = ["orders", "customers"]
    results = [
        _res("orders", ObjectApplyStatus.SKIPPED),
        _res("customers", ObjectApplyStatus.SKIPPED),
    ]
    assert schema_apply_is_complete(names, results) is True


def test_schema_apply_complete_with_mixed_created_and_skipped() -> None:
    names = ["orders", "customers"]
    results = [
        _res("orders", ObjectApplyStatus.CREATED),
        _res("customers", ObjectApplyStatus.SKIPPED),
    ]
    assert schema_apply_is_complete(names, results) is True


def test_schema_apply_incomplete_when_any_failed() -> None:
    names = ["orders", "customers"]
    results = [
        _res("orders", ObjectApplyStatus.CREATED),
        _res("customers", ObjectApplyStatus.FAILED),
    ]
    assert schema_apply_is_complete(names, results) is False


def test_schema_apply_incomplete_when_object_has_no_result_yet() -> None:
    # Partial one-by-one apply: only 'orders' applied so far -> not DONE.
    names = ["orders", "customers"]
    results = [_res("orders", ObjectApplyStatus.SKIPPED)]
    assert schema_apply_is_complete(names, results) is False


def test_schema_apply_incomplete_with_no_applicable_or_no_results() -> None:
    assert schema_apply_is_complete([], [_res("x", ObjectApplyStatus.SKIPPED)]) is False
    assert schema_apply_is_complete(["orders"], None) is False
    assert schema_apply_is_complete(["orders"], []) is False


def test_confirmed_replace_applies_objects() -> None:
    applier = _FakeApplier(ApplyOutcome.CREATED)
    objects = build_apply_objects(_converted())

    results = run_schema_apply(
        objects, applier=applier, mode=ApplyMode.REPLACE, confirmed=True
    )

    assert len(applier.calls) == 3
    assert all(call[2] is ApplyMode.REPLACE for call in applier.calls)
    assert all(r.status is ObjectApplyStatus.CREATED for r in results)


class _FakeApplierWithDrop(_FakeApplier):
    """A fake applier that also records the REPLACE pre-pass view drops."""

    def __init__(self, outcome: ApplyOutcome = ApplyOutcome.CREATED) -> None:
        super().__init__(outcome)
        self.dropped: list[str] = []

    def drop(self, target_ddl: str) -> None:
        self.dropped.append(target_ddl)


def test_confirmed_replace_drops_dependent_views_before_recreating_tables() -> None:
    # Regression: re-running REPLACE failed because dropping a table whose view
    # (created by an earlier apply) still selected from it raised "other objects
    # depend on it". The pre-pass drops the apply set's views first.
    applier = _FakeApplierWithDrop(ApplyOutcome.CREATED)
    objects = build_apply_objects(_converted())  # orders, customers, active_orders

    results = run_schema_apply(
        objects, applier=applier, mode=ApplyMode.REPLACE, confirmed=True
    )

    # The view's CREATE VIEW was dropped (DROP happens before the table recreate).
    assert len(applier.dropped) == 1
    assert applier.dropped[0].strip().lower().startswith("create view")
    # All three objects were still applied (the view is recreated by its own unit).
    assert len(applier.calls) == 3
    assert all(r.status is ObjectApplyStatus.CREATED for r in results)


def test_predrop_skipped_for_skip_mode_and_unconfirmed_replace() -> None:
    objects = build_apply_objects(_converted())

    skip = _FakeApplierWithDrop(ApplyOutcome.SKIPPED)
    run_schema_apply(objects, applier=skip, mode=ApplyMode.SKIP_IF_EXISTS, confirmed=False)
    assert skip.dropped == []  # SKIP mode never drops anything

    unconfirmed = _FakeApplierWithDrop(ApplyOutcome.CREATED)
    run_schema_apply(objects, applier=unconfirmed, mode=ApplyMode.REPLACE, confirmed=False)
    assert unconfirmed.dropped == []  # unconfirmed REPLACE applies nothing
    assert unconfirmed.calls == []


def test_confirmed_replace_without_drop_seam_is_a_noop_prepass() -> None:
    # A test double (or any applier) without a ``drop`` method must still work:
    # the pre-pass is simply skipped, not an error.
    applier = _FakeApplier(ApplyOutcome.CREATED)  # no .drop attribute
    objects = build_apply_objects(_converted())

    results = run_schema_apply(
        objects, applier=applier, mode=ApplyMode.REPLACE, confirmed=True
    )
    assert all(r.status is ObjectApplyStatus.CREATED for r in results)


def test_replace_confirmation_message_lists_existing_objects() -> None:
    message = replace_confirmation_message(["orders", "customers"])
    # Names are listed (sorted) and the destructive nature is stated.
    assert "customers, orders" in message
    assert "DROP" in message
    assert "destructive" in message


def test_replace_confirmation_message_generic_when_none_known() -> None:
    message = replace_confirmation_message([])
    assert "any target object that already exists" in message
    assert "destructive" in message


def test_apply_failure_is_isolated_per_object() -> None:
    class _OneFails:
        def apply_object(self, object_name, ddls, on_conflict):
            if object_name == "orders":
                raise RuntimeError("boom on orders")
            return ApplyOutcome.CREATED

    results = run_schema_apply(
        build_apply_objects(_converted()),
        applier=_OneFails(),
        mode=ApplyMode.SKIP_IF_EXISTS,
        confirmed=False,
    )
    by_name = {r.object_name: r for r in results}
    assert by_name["orders"].status is ObjectApplyStatus.FAILED
    assert "boom on orders" in by_name["orders"].detail
    assert by_name["customers"].status is ObjectApplyStatus.CREATED


def test_apply_path_only_writes_target_ddl_never_source() -> None:
    """Property 12 / Property 1: the apply path applies only converted target DDL.

    The orchestration takes no source handle at all; here we further assert that
    the only SQL ever handed to the target executor is the converted target DDL
    and never the reconstructed source (MySQL) DDL.
    """
    inventory = _inventory()
    result = SchemaConverter().convert(inventory)
    source_ddls = {render_source_table_ddl(t) for t in inventory.tables}

    executor = _FakeExecutor()
    applier = OccRetryingSchemaApplier(
        executor, sleep=_no_sleep, jitter=_no_jitter
    )

    run_schema_apply(
        build_apply_objects(result),
        applier=applier,
        mode=ApplyMode.SKIP_IF_EXISTS,
        confirmed=False,
    )

    assert executor.executed  # something was applied
    assert executor.dropped == []  # nothing existed, nothing dropped
    for statement in executor.executed:
        assert statement not in source_ddls
        assert "FOREIGN KEY" not in statement  # FKs only exist in source DDL


# ---------------------------------------------------------------------------
# OccRetryingSchemaApplier -- single-DDL txn, SKIP/REPLACE, OC001 retry
# ---------------------------------------------------------------------------


def test_applier_applies_each_statement_as_single_ddl() -> None:
    executor = _FakeExecutor()
    applier = OccRetryingSchemaApplier(executor, sleep=_no_sleep, jitter=_no_jitter)
    obj = build_apply_objects(_converted())[0]  # orders: table + 1 index

    outcome = applier.apply_object(obj.object_name, obj.ddls, ApplyMode.SKIP_IF_EXISTS)

    assert outcome is ApplyOutcome.CREATED
    # Each statement executed individually (one DDL per transaction).
    assert len(executor.executed) == len(obj.ddls)


def test_applier_skips_existing_object_without_writing() -> None:
    executor = _FakeExecutor(exists=("orders",))
    applier = OccRetryingSchemaApplier(executor, sleep=_no_sleep, jitter=_no_jitter)
    obj = build_apply_objects(_converted())[0]

    outcome = applier.apply_object(obj.object_name, obj.ddls, ApplyMode.SKIP_IF_EXISTS)

    assert outcome is ApplyOutcome.SKIPPED
    assert executor.executed == []
    assert executor.dropped == []


def test_applier_replace_drops_existing_then_recreates() -> None:
    executor = _FakeExecutor(exists=("orders",))
    applier = OccRetryingSchemaApplier(executor, sleep=_no_sleep, jitter=_no_jitter)
    obj = build_apply_objects(_converted())[0]

    outcome = applier.apply_object(obj.object_name, obj.ddls, ApplyMode.REPLACE)

    assert outcome is ApplyOutcome.CREATED
    assert executor.dropped == ["orders"]
    assert len(executor.executed) == len(obj.ddls)


def test_applier_idempotently_retries_oc001_conflict() -> None:
    """Property 5/12: an OC001 conflict is retried until the idempotent op wins."""
    obj = build_apply_objects(_converted())[1]  # customers: single CREATE TABLE
    create_sql = obj.ddls[0]
    executor = _FakeExecutor(fail_once_on={create_sql[:20]: 2})
    applier = OccRetryingSchemaApplier(executor, sleep=_no_sleep, jitter=_no_jitter)

    outcome = applier.apply_object(obj.object_name, obj.ddls, ApplyMode.SKIP_IF_EXISTS)

    assert outcome is ApplyOutcome.CREATED
    assert executor.executed == [create_sql]  # eventually succeeded once


def test_applier_propagates_non_occ_failure() -> None:
    obj = build_apply_objects(_converted())[1]
    executor = _FakeExecutor(hard_fail_on="CREATE TABLE")
    applier = OccRetryingSchemaApplier(executor, sleep=_no_sleep, jitter=_no_jitter)

    with pytest.raises(RuntimeError, match="permission denied"):
        applier.apply_object(obj.object_name, obj.ddls, ApplyMode.SKIP_IF_EXISTS)


def test_applier_failure_surfaces_as_failed_result_in_orchestration() -> None:
    executor = _FakeExecutor(hard_fail_on="CREATE TABLE")
    applier = OccRetryingSchemaApplier(executor, sleep=_no_sleep, jitter=_no_jitter)

    results = run_schema_apply(
        build_apply_objects(_converted()),
        applier=applier,
        mode=ApplyMode.SKIP_IF_EXISTS,
        confirmed=False,
    )
    # The CREATE TABLE objects fail and surface as FAILED per object (the view's
    # CREATE VIEW does not hit the CREATE TABLE failure, so it is unaffected).
    by_name = {r.object_name: r for r in results}
    assert by_name["orders"].status is ObjectApplyStatus.FAILED
    assert by_name["customers"].status is ObjectApplyStatus.FAILED
    assert "permission denied" in by_name["orders"].detail
    assert "permission denied" in by_name["customers"].detail


# ---------------------------------------------------------------------------
# Statement splitting
# ---------------------------------------------------------------------------


def test_split_sql_statements_drops_empty_fragments() -> None:
    assert split_sql_statements(" SELECT 1; ; SELECT 2 ;") == ["SELECT 1", "SELECT 2"]
    assert split_sql_statements("   ") == []


# ---------------------------------------------------------------------------
# Job-status mapping + per-session state/store
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("job_status", "expected"),
    [
        ("DONE", StepStatus.DONE),
        ("FAILED", StepStatus.FAILED),
        ("PENDING", None),
        ("RUNNING", None),
    ],
)
def test_job_status_to_step_status(job_status: str, expected) -> None:
    assert job_status_to_step_status(job_status) is expected


def _apply_result() -> ObjectApplyResult:
    """Build a minimal CREATED result for state tests."""
    return ObjectApplyResult(object_name="orders", status=ObjectApplyStatus.CREATED)


def test_state_apply_results_and_error_handoff() -> None:
    state = SchemaConversionState()
    assert state.apply_results is None
    assert state.error is None

    state.set_apply_results([_apply_result()])
    assert state.apply_results is not None
    assert state.error is None

    state.set_error("boom")
    assert state.error == "boom"
    state.clear_outputs()
    assert state.apply_results is None
    assert state.error is None


def test_state_set_results_clears_prior_error() -> None:
    state = SchemaConversionState()
    state.set_error("previous failure")
    state.set_apply_results([_apply_result()])
    assert state.error is None


def test_state_merge_apply_results_upserts_and_keeps_others() -> None:
    # "Retry failed" re-applies a subset and merges: the retried object's result
    # is updated while the other objects' results are preserved.
    state = SchemaConversionState()
    state.set_apply_results(
        [
            ObjectApplyResult("orders", ObjectApplyStatus.FAILED, "boom"),
            ObjectApplyResult("customers", ObjectApplyStatus.CREATED),
        ]
    )
    state.merge_apply_results(
        [ObjectApplyResult("orders", ObjectApplyStatus.CREATED)]
    )
    by_name = {r.object_name: r for r in (state.apply_results or [])}
    assert by_name["orders"].status is ObjectApplyStatus.CREATED
    assert by_name["customers"].status is ObjectApplyStatus.CREATED


def test_state_pending_target_refresh_defaults_false() -> None:
    assert SchemaConversionState().pending_target_refresh is False


def test_store_is_isolated_per_session() -> None:
    store = SchemaConversionStore()
    a = store.get_or_create("session-a")
    b = store.get_or_create("session-b")
    assert a is not b
    assert store.get_or_create("session-a") is a

    a.selected_node_id = "table:orders"
    assert b.selected_node_id is None


def test_store_clear_removes_only_target_session() -> None:
    store = SchemaConversionStore()
    store.get_or_create("session-a")
    store.get_or_create("session-b")

    store.clear("session-a")
    assert store.get("session-a") is None
    assert store.get("session-b") is not None
    store.clear("missing")
    store.clear(None)


# ---------------------------------------------------------------------------
# Target object browser tree (side-by-side with the source browser)
# ---------------------------------------------------------------------------


def _target_inventory() -> TargetInventory:
    return TargetInventory(
        schemas=[
            TargetSchemaNode(
                name="public",
                tables=[
                    TargetRelation(
                        schema_name="public", name="orders", kind=TargetObjectKind.TABLE
                    )
                ],
                views=[
                    TargetRelation(
                        schema_name="public",
                        name="active_orders",
                        kind=TargetObjectKind.VIEW,
                    )
                ],
            )
        ]
    )


def test_build_target_object_tree_groups_by_schema() -> None:
    tree = build_target_object_tree(_target_inventory())
    assert len(tree) == 1
    schema = tree[0]
    assert schema["id"] == f"{TARGET_PREFIX}schema:public"
    categories = schema["children"]
    table_nodes = categories[0]["children"]
    view_nodes = categories[1]["children"]
    assert table_nodes[0]["id"] == f"{TARGET_PREFIX}table:public.orders"
    assert table_nodes[0]["label"] == "orders"
    assert view_nodes[0]["id"] == f"{TARGET_PREFIX}view:public.active_orders"


def test_build_target_object_tree_empty_when_no_inventory() -> None:
    assert build_target_object_tree(None) == []
    assert build_target_object_tree(TargetInventory()) == []


# ---------------------------------------------------------------------------
# Generated-object header summary (expansion caption) + expand/collapse state
# ---------------------------------------------------------------------------


def test_object_header_summary_combines_all_status_parts() -> None:
    table = _inventory().tables[0]  # orders: has a removed-FK conversion warning
    conversion = _converted().tables[0]
    preview = build_table_preview(table, conversion, exists_on_target=True)

    summary = _object_header_summary(
        preview,
        edited=True,
        applied=ObjectApplyResult(
            object_name="orders", status=ObjectApplyStatus.CREATED
        ),
    )

    assert "exists on target" in summary
    assert "warning" in summary  # at least one conversion warning surfaced
    assert "edited" in summary
    assert "applied: CREATED" in summary
    assert " · " in summary  # parts are joined with a separator


def test_object_header_summary_surfaces_unsupported_severity() -> None:
    # An object the converter could not auto-convert (UNSUPPORTED warning) is
    # labeled "Unsupported" in the collapsed header, not just "1 warning", so the
    # user can tell it needs manual redesign at a glance.
    preview = DdlPreview(
        object_name="app.typetest_spatial",
        source_ddl="CREATE TABLE `typetest_spatial` (`geom` point)",
        target_ddl=(
            "-- Could not auto-convert table app.typetest_spatial for Aurora DSQL; "
            "it uses MySQL spatial column(s) (geom point). Redesign it manually."
        ),
        warnings=(
            ConversionWarning(
                object_name="app.typetest_spatial",
                classification=Classification.UNSUPPORTED,
                message="MySQL spatial columns are unsupported; redesign manually.",
            ),
        ),
    )
    summary = _object_header_summary(preview, edited=False, applied=None)
    assert "Unsupported" in summary
    assert "1 warning" in summary


def test_object_header_summary_empty_when_nothing_noteworthy() -> None:
    table = _inventory().tables[1]  # customers: PK only, no warnings
    conversion = _converted().tables[1]
    preview = build_table_preview(table, conversion)  # exists_on_target=None

    assert _object_header_summary(preview, edited=False, applied=None) == ""


def test_schema_conversion_state_expand_all_defaults_collapsed() -> None:
    assert SchemaConversionState().expand_all is False


# ---------------------------------------------------------------------------
# Apply result detail disambiguates CREATED vs SKIPPED (Requirement 10.7).
# Answers "was it dropped and recreated, or created fresh, or skipped?"
# ---------------------------------------------------------------------------


def test_apply_detail_created_in_skip_mode_states_no_drop() -> None:
    applier = _FakeApplier(ApplyOutcome.CREATED)
    results = run_schema_apply(
        build_apply_objects(_converted()),
        applier=applier,
        mode=ApplyMode.SKIP_IF_EXISTS,
        confirmed=False,
    )
    assert all(r.status is ObjectApplyStatus.CREATED for r in results)
    for r in results:
        detail = r.detail.lower()
        assert "created new" in detail
        # SKIP mode must make clear nothing was dropped/replaced.
        assert "never drops" in detail


def test_apply_detail_skipped_states_left_unchanged() -> None:
    applier = _FakeApplier(ApplyOutcome.SKIPPED)
    results = run_schema_apply(
        build_apply_objects(_converted()),
        applier=applier,
        mode=ApplyMode.SKIP_IF_EXISTS,
        confirmed=False,
    )
    assert all(r.status is ObjectApplyStatus.SKIPPED for r in results)
    assert all("left unchanged" in r.detail.lower() for r in results)


def test_apply_detail_replace_created_states_dropped_and_recreated() -> None:
    applier = _FakeApplier(ApplyOutcome.CREATED)
    results = run_schema_apply(
        build_apply_objects(_converted()),
        applier=applier,
        mode=ApplyMode.REPLACE,
        confirmed=True,
    )
    assert all(r.status is ObjectApplyStatus.CREATED for r in results)
    assert all("dropped and recreated" in r.detail.lower() for r in results)


# ---------------------------------------------------------------------------
# Regression: a re-ensured CREATE SCHEMA must not make a re-applied qualified
# table look CREATED when the table/indexes were skipped (idempotent re-apply).
# ---------------------------------------------------------------------------

_QUALIFIED_TABLE_DDLS = (
    "CREATE SCHEMA IF NOT EXISTS app",
    "CREATE TABLE app.orders (id uuid PRIMARY KEY)",
    'CREATE INDEX ASYNC "idx_orders_id" ON app.orders (id)',
)


def test_dsql_applier_skipped_when_only_schema_created_on_reapply() -> None:
    """Re-apply of an existing qualified table: schema re-ensured (always CREATED)
    must not override the table/index SKIPPED into a misleading object CREATED."""
    core = _FakeCoreApplier(
        [
            ApplyResult(object_name="app", status=ApplyStatus.CREATED),
            ApplyResult(object_name="app.orders", status=ApplyStatus.SKIPPED),
            ApplyResult(object_name="idx_orders_id", status=ApplyStatus.SKIPPED),
        ]
    )
    applier = DsqlSchemaApplier(_target(), core_factory=lambda target, profile: core)

    outcome = applier.apply_object(
        "app.orders", _QUALIFIED_TABLE_DDLS, ApplyMode.SKIP_IF_EXISTS
    )

    assert outcome is ApplyOutcome.SKIPPED


def test_dsql_applier_created_when_qualified_table_actually_created() -> None:
    """First apply: schema ensured (CREATED) plus the table created -> CREATED."""
    core = _FakeCoreApplier(
        [
            ApplyResult(object_name="app", status=ApplyStatus.CREATED),
            ApplyResult(object_name="app.orders", status=ApplyStatus.CREATED),
            ApplyResult(object_name="idx_orders_id", status=ApplyStatus.CREATED),
        ]
    )
    applier = DsqlSchemaApplier(_target(), core_factory=lambda target, profile: core)

    outcome = applier.apply_object(
        "app.orders", _QUALIFIED_TABLE_DDLS, ApplyMode.SKIP_IF_EXISTS
    )

    assert outcome is ApplyOutcome.CREATED




def test_applied_table_conversions_overlays_edits_and_falls_back() -> None:
    # #1/#3 source of truth: a user-edited target DDL is parsed into the applied
    # TableConversion (CREATE TABLE -> target_ddl, CREATE INDEX -> index_ddls); an
    # unedited table keeps the deterministic conversion unchanged.
    result = _converted()  # tables: orders, customers
    edited = {
        "orders": (
            'CREATE TABLE "orders" ("id" uuid PRIMARY KEY, "qty" smallint);\n'
            'CREATE INDEX ASYNC ix_orders ON "orders" ("qty");'
        )
    }
    applied = applied_table_conversions(result, edited)

    orders = applied["orders"]
    assert orders.target_ddl.lower().startswith("create table")
    assert "smallint" in orders.target_ddl
    assert any("INDEX" in ddl.upper() for ddl in orders.index_ddls)

    deterministic_customers = next(t for t in result.tables if t.table == "customers")
    assert applied["customers"] is deterministic_customers


def test_apply_should_replace_routes_edits_to_replace() -> None:
    from dsql_migrator.ui.schema_conversion import _apply_should_replace

    # Global REPLACE always replaces.
    assert _apply_should_replace(apply_mode=ApplyMode.REPLACE, edited=False)
    # Any EDITED object replaces: SKIP would skip an existing one and silently drop
    # the edit; REPLACE's DROP IF EXISTS safely handles a not-yet-existing object.
    assert _apply_should_replace(apply_mode=ApplyMode.SKIP_IF_EXISTS, edited=True)
    # A non-edited object in SKIP mode is not replaced (idempotent skip/create).
    assert not _apply_should_replace(apply_mode=ApplyMode.SKIP_IF_EXISTS, edited=False)


# ---------------------------------------------------------------------------
# Per-table Primary-key strategy picker (Phase 2: opt-in composite key)
# ---------------------------------------------------------------------------


def _pk_table(name: str = "orders") -> TableDef:
    return TableDef(
        name=name,
        columns=[
            ColumnDef(name="id", mysql_type="BIGINT", nullable=False),
            ColumnDef(name="customer_id", mysql_type="BIGINT", nullable=False),
            ColumnDef(name="region", mysql_type="VARCHAR(20)", nullable=False),
            ColumnDef(name="note", mysql_type="TEXT", nullable=True),
        ],
        primary_key=["id"],
        auto_increment_column="id",
    )


def test_composite_leading_candidates_excludes_nullable_and_pk() -> None:
    # Only NOT NULL non-PK columns can lead a composite key (matches the converter
    # validation), so the dropdown never offers an invalid choice.
    assert composite_leading_candidates(_pk_table()) == ["customer_id", "region"]


def test_default_composite_leading_is_first_candidate_or_none() -> None:
    assert default_composite_leading(_pk_table()) == "customer_id"
    # A table with no eligible column returns None (composite can't be offered).
    no_lead = TableDef(
        name="t",
        columns=[
            ColumnDef(name="id", mysql_type="INT", nullable=False),
            ColumnDef(name="maybe", mysql_type="INT", nullable=True),
        ],
        primary_key=["id"],
    )
    assert default_composite_leading(no_lead) is None


def test_composite_leading_from_ddl_round_trips_the_stored_choice() -> None:
    # The picker keeps NO separate state: it infers the leading column back out of
    # the stored (edited) target DDL. build -> render -> infer must round-trip.
    table = _pk_table()
    conversion = build_composite_conversion(SchemaConverter(), table, "customer_id")
    stored = render_target_ddl(conversion)
    assert composite_leading_from_ddl(table, stored) == "customer_id"


def test_composite_leading_from_ddl_none_for_unchanged_key() -> None:
    table = _pk_table()
    deterministic = SchemaConverter().convert_table(table)
    stored = render_target_ddl(deterministic)
    # Unchanged key -> not composite -> the picker renders as "Keep source PK".
    assert composite_leading_from_ddl(table, stored) is None


def test_build_composite_conversion_emits_unique_index_on_original_key() -> None:
    conversion = build_composite_conversion(SchemaConverter(), _pk_table(), "customer_id")
    script = render_target_ddl(conversion)
    assert 'PRIMARY KEY ("customer_id", "id")' in script
    assert "CREATE UNIQUE INDEX ASYNC" in script and '("id")' in script


class _PickerEl:
    """Chainable element double that captures a toggle/select's on_change handler."""

    def __init__(self, recorder, kind, on_change=None):
        self._recorder = recorder
        self.kind = kind
        self.on_change = on_change
        self.disabled = False

    def classes(self, *_a, **_k):
        return self

    def props(self, value="", *_a, **_k):
        if "disable" in str(value):
            self.disabled = True
        return self

    def tooltip(self, *_a, **_k):
        return self

    def on(self, event, handler=None, *_a, **_k):
        # The PK picker uses Cloudscape radio TILES (ui/design.radio_tiles), which
        # register their selection as a click on the tile card -- not a toggle.
        if event == "click" and handler is not None:
            self._recorder.tile_clicks.append(handler)
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _PickerUi:
    """Minimal NiceGUI double capturing the picker's tiles + select + notices."""

    def __init__(self):
        self.tile_clicks: list = []   # tile click handlers, in render order
        self.toggle_el = None
        self.select_el = None
        self.select_options = None
        self.select_value = None
        self.texts: list[str] = []    # every emitted label/badge string, in order

    def card(self, *_a, **_k):
        return _PickerEl(self, "card")

    def badge(self, text="", *_a, **_k):
        if text:
            self.texts.append(str(text))
        return _PickerEl(self, "badge")

    def row(self, *_a, **_k):
        return _PickerEl(self, "row")

    def column(self, *_a, **_k):
        return _PickerEl(self, "column")

    def label(self, text="", *_a, **_k):
        # render_notice()/inline_hint() render their header/body/text via ui.label,
        # so recording label text lets a test assert the caveat wording was shown.
        if text:
            self.texts.append(str(text))
        return _PickerEl(self, "label")

    def icon(self, *_a, **_k):
        return _PickerEl(self, "icon")

    def body(self) -> str:
        return "\n".join(self.texts)

    def space(self, *_a, **_k):
        return _PickerEl(self, "space")

    def html(self, *_a, **_k):
        return _PickerEl(self, "html")

    def toggle(self, options=None, *, value=None, on_change=None, **_k):
        self.toggle_el = _PickerEl(self, "toggle", on_change=on_change)
        return self.toggle_el

    def select(self, options=None, *, value=None, on_change=None, label=None, **_k):
        self.select_options = options
        self.select_value = value
        self.select_el = _PickerEl(self, "select", on_change=on_change)
        return self.select_el


def _event(value):
    return type("Evt", (), {"value": value})()


def test_pk_picker_selecting_composite_stores_composite_ddl() -> None:
    ui = _PickerUi()
    state = SchemaConversionState()
    table = _pk_table()
    refreshed: list[bool] = []
    _render_pk_strategy_picker(ui, table, state, lambda: refreshed.append(True))

    # _pk_table() is a single-column AUTO_INCREMENT key, so all three tiles render in
    # order: Keep source PK, Server-generated (IDENTITY), Composite key.
    assert len(ui.tile_clicks) == 3
    ui.tile_clicks[2]()  # the "Composite key" tile

    stored = state.get_edited_target_ddl("orders")
    assert stored is not None
    assert composite_leading_from_ddl(table, stored) == "customer_id"  # default lead
    assert refreshed  # the screen re-rendered


def test_pk_picker_leading_dropdown_switches_leading_column() -> None:
    ui = _PickerUi()
    state = SchemaConversionState()
    table = _pk_table()
    # Pre-seed a composite choice so the dropdown renders.
    state.set_edited_target_ddl(
        "orders", render_target_ddl(build_composite_conversion(SchemaConverter(), table, "customer_id"))
    )
    _render_pk_strategy_picker(ui, table, state, lambda: None)

    # The dropdown offers exactly the eligible columns, preset to the current lead.
    assert ui.select_options == ["customer_id", "region"]
    assert ui.select_value == "customer_id"
    # Switch the leading column to "region".
    ui.select_el.on_change(_event("region"))
    assert composite_leading_from_ddl(table, state.get_edited_target_ddl("orders")) == "region"


def test_pk_picker_switching_back_to_keep_clears_override() -> None:
    ui = _PickerUi()
    state = SchemaConversionState()
    table = _pk_table()
    state.set_edited_target_ddl(
        "orders", render_target_ddl(build_composite_conversion(SchemaConverter(), table, "customer_id"))
    )
    _render_pk_strategy_picker(ui, table, state, lambda: None)
    ui.tile_clicks[0]()  # the "Keep source PK" tile
    # Reverting to Keep source PK drops the composite override entirely.
    assert state.get_edited_target_ddl("orders") is None


def test_pk_picker_disables_composite_when_no_eligible_leading() -> None:
    ui = _PickerUi()
    state = SchemaConversionState()
    table = TableDef(
        name="t",
        columns=[
            ColumnDef(name="id", mysql_type="INT", nullable=False),
            ColumnDef(name="maybe", mysql_type="INT", nullable=True),
        ],
        primary_key=["id"],
    )
    _render_pk_strategy_picker(ui, table, state, lambda: None)
    # No NOT NULL non-PK column -> the tiles render but the group is locked, so no
    # click handler is wired and no dropdown/notice appears.
    assert ui.tile_clicks == []
    assert ui.select_el is None


def test_pk_picker_not_rendered_for_table_without_primary_key() -> None:
    ui = _PickerUi()
    state = SchemaConversionState()
    keyless = TableDef(
        name="k",
        columns=[ColumnDef(name="v", mysql_type="INT", nullable=False)],
        primary_key=[],
    )
    _render_pk_strategy_picker(ui, keyless, state, lambda: None)
    # No PK -> no hot-partition concern -> the picker renders nothing.
    assert ui.tile_clicks == []
    assert ui.select_el is None


# ---------------------------------------------------------------------------
# T1: Server-generated (IDENTITY) primary-key strategy in the picker
# ---------------------------------------------------------------------------


def _int_auto_inc_table(name: str = "orders") -> TableDef:
    """Single-column INT AUTO_INCREMENT PK -- the shape the IDENTITY tile targets."""
    return TableDef(
        name=name,
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="total", mysql_type="decimal(10,2)", nullable=True),
        ],
        primary_key=["id"],
        auto_increment_column="id",
    )


def test_build_identity_conversion_emits_cached_bigint_identity() -> None:
    # The IDENTITY tile bakes this DDL; it must be the BIGINT cached-identity form
    # (Aurora DSQL rejects an INT identity, and the CACHE clause is what spreads the
    # key across nodes). BY DEFAULT (not ALWAYS) so Full Load can still insert ids.
    from dsql_migrator.ui.schema_conversion import build_identity_conversion

    script = render_target_ddl(build_identity_conversion(SchemaConverter(), _int_auto_inc_table()))
    assert '"id" BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)' in script
    # It is the KEY, unchanged in name/position -- not a composite rewrite.
    assert 'PRIMARY KEY ("id")' in script


def test_identity_from_ddl_round_trips_and_is_false_for_other_strategies() -> None:
    # The picker keeps no separate state: build -> render -> infer must round-trip, and
    # the identity detector must NOT fire on the Keep or Composite scripts.
    from dsql_migrator.ui.schema_conversion import (
        build_identity_conversion,
        identity_from_ddl,
    )

    table = _int_auto_inc_table()
    identity = render_target_ddl(build_identity_conversion(SchemaConverter(), table))
    assert identity_from_ddl(table, identity) is True

    keep = render_target_ddl(SchemaConverter().convert_table(table))
    assert identity_from_ddl(table, keep) is False

    # A composite table has a customer_id to lead with; its script is not an identity.
    composite_table = _pk_table()
    composite = render_target_ddl(
        build_composite_conversion(SchemaConverter(), composite_table, "customer_id")
    )
    assert identity_from_ddl(composite_table, composite) is False

    # "GENERATED" alone must NOT count: a generated (computed) column on the key line
    # carries GENERATED without being an identity, so the detector requires BOTH
    # "GENERATED" and "AS IDENTITY" (guards against dropping the second half).
    generated_not_identity = (
        'CREATE TABLE "orders" (\n'
        '  "id" BIGINT NOT NULL GENERATED ALWAYS AS (1) STORED,\n'
        '  PRIMARY KEY ("id")\n'
        ")"
    )
    assert identity_from_ddl(table, generated_not_identity) is False


def test_identity_eligible_only_for_single_column_auto_increment_key() -> None:
    from dsql_migrator.ui.schema_conversion import _identity_eligible

    # Single-column AUTO_INCREMENT PK: eligible.
    assert _identity_eligible(_int_auto_inc_table()) is True

    # AUTO_INCREMENT column that is only PART of a composite PK: NOT eligible (the
    # identity strategy rewrites a single key column, not a composite key).
    composite_key = TableDef(
        name="line_items",
        columns=[
            ColumnDef(name="order_id", mysql_type="int", nullable=False),
            ColumnDef(name="line_no", mysql_type="int", nullable=False),
        ],
        primary_key=["order_id", "line_no"],
        auto_increment_column="line_no",
    )
    assert _identity_eligible(composite_key) is False

    # No AUTO_INCREMENT column at all: NOT eligible.
    plain = TableDef(
        name="ref",
        columns=[ColumnDef(name="code", mysql_type="varchar(10)", nullable=False)],
        primary_key=["code"],
    )
    assert _identity_eligible(plain) is False


def test_pk_picker_offers_identity_tile_for_auto_increment_and_stores_identity_ddl() -> None:
    from dsql_migrator.ui.schema_conversion import identity_from_ddl

    ui = _PickerUi()
    state = SchemaConversionState()
    table = _int_auto_inc_table()
    refreshed: list[bool] = []
    _render_pk_strategy_picker(ui, table, state, lambda: refreshed.append(True))

    # _int_auto_inc_table() has no NOT NULL non-key column, so no composite tile:
    # the tiles are Keep source PK, Server-generated (IDENTITY).
    assert len(ui.tile_clicks) == 2
    ui.tile_clicks[1]()  # the IDENTITY tile

    stored = state.get_edited_target_ddl("orders")
    assert stored is not None
    assert identity_from_ddl(table, stored) is True
    # It stores the identity form, not a composite rewrite.
    assert '"id" BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)' in stored
    assert refreshed


def test_pk_picker_renders_as_identity_when_identity_ddl_is_stored() -> None:
    # Pre-seeding the identity DDL makes the picker render with IDENTITY selected and
    # its info caveat, and it does NOT show the composite dropdown.
    from dsql_migrator.ui.schema_conversion import build_identity_conversion

    ui = _PickerUi()
    state = SchemaConversionState()
    table = _int_auto_inc_table()
    state.set_edited_target_ddl(
        "orders", render_target_ddl(build_identity_conversion(SchemaConverter(), table))
    )
    _render_pk_strategy_picker(ui, table, state, lambda: None)

    # The identity caveat is shown, and the composite dropdown is not.
    assert "gaps and loose ordering" in ui.body()
    assert ui.select_el is None


def test_pk_picker_switching_identity_back_to_keep_clears_override() -> None:
    from dsql_migrator.ui.schema_conversion import build_identity_conversion

    ui = _PickerUi()
    state = SchemaConversionState()
    table = _int_auto_inc_table()
    state.set_edited_target_ddl(
        "orders", render_target_ddl(build_identity_conversion(SchemaConverter(), table))
    )
    _render_pk_strategy_picker(ui, table, state, lambda: None)
    ui.tile_clicks[0]()  # the "Keep source PK" tile
    # Reverting to Keep drops the identity override entirely.
    assert state.get_edited_target_ddl("orders") is None


def test_pk_picker_hides_identity_tile_for_composite_key_table() -> None:
    # A composite source key is not a single-column AUTO_INCREMENT key, and here has no
    # eligible composite-leading column either, so ONLY the Keep tile renders (locked).
    ui = _PickerUi()
    state = SchemaConversionState()
    composite_key = TableDef(
        name="line_items",
        columns=[
            ColumnDef(name="order_id", mysql_type="int", nullable=False),
            ColumnDef(name="line_no", mysql_type="int", nullable=False),
        ],
        primary_key=["order_id", "line_no"],
        auto_increment_column="line_no",
    )
    _render_pk_strategy_picker(ui, composite_key, state, lambda: None)
    # Only Keep applies -> group locked -> no click handlers, no dropdown.
    assert ui.tile_clicks == []
    assert ui.select_el is None


def test_pk_picker_uuid_tile_is_never_offered() -> None:
    # UUID retypes the key to uuid, which the tool's Full Load cannot populate from the
    # source's integer ids -- so it must not appear as a picker tile on any table shape.
    for table in (_int_auto_inc_table(), _pk_table()):
        ui = _PickerUi()
        state = SchemaConversionState()
        _render_pk_strategy_picker(ui, table, state, lambda: None)
        assert "uuid" not in ui.body().lower()


# ---------------------------------------------------------------------------
# Per-object "Apply to target" button — busy-state feedback
# ---------------------------------------------------------------------------


class _EditableUi:
    """NiceGUI double for _render_editable_target.

    Every element is a chainable, context-manager node; the Apply button records
    the sequence of props(add / remove=...) calls so a test can assert the busy
    state (loading + disable) is set for the duration of the apply and cleared
    afterwards. on_click(cb) stores the click callback so the test can invoke it.
    """

    class _Node:
        def __init__(self, ui: "_EditableUi", kind: str) -> None:
            self._ui = ui
            self.kind = kind
            self.click_cb = None
            self.prop_events: list[tuple[str, object]] = []

        def classes(self, *_a, **_k):
            return self

        def props(self, add: str = "", *, remove: str = ""):
            if add:
                self.prop_events.append(("add", add))
            if remove:
                self.prop_events.append(("remove", remove))
            return self

        def style(self, *_a, **_k):
            return self

        def tooltip(self, *_a, **_k):
            return self

        def on_click(self, cb):
            self.click_cb = cb
            return self

        def on(self, *_a, **_k):
            return self

        def clear(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    def __init__(self) -> None:
        self.buttons: list["_EditableUi._Node"] = []

    def _node(self, kind: str) -> "_EditableUi._Node":
        return _EditableUi._Node(self, kind)

    def button(self, *_a, **_k):
        node = self._node("button")
        self.buttons.append(node)
        return node

    # Everything else the render path touches is a no-op chainable node.
    def __getattr__(self, name):  # noqa: ANN001
        def _factory(*_a, **_k):
            return self._node(name)

        return _factory


def _editable_preview() -> DdlPreview:
    return DdlPreview(
        object_name="orders",
        source_ddl="CREATE TABLE `orders` (`id` int)",
        target_ddl='CREATE TABLE "orders" ("id" integer)',
    )


def test_apply_button_shows_busy_state_during_apply() -> None:
    import asyncio

    ui = _EditableUi()
    calls: list[str] = []

    async def _on_apply(name: str) -> None:
        # While the apply runs, the button must be in the busy state.
        apply_btn = ui.buttons[-1]
        # Busy state = disabled (never the forbidden Quasar `loading` prop).
        assert ("add", "disable") in apply_btn.prop_events
        assert ("remove", "disable") not in apply_btn.prop_events
        calls.append(name)

    _render_editable_target(
        ui, _editable_preview(), SchemaConversionState(), on_apply_object=_on_apply
    )
    apply_btn = ui.buttons[-1]
    assert apply_btn.click_cb is not None

    asyncio.run(apply_btn.click_cb())

    assert calls == ["orders"]
    # After the apply completes the busy state is cleared (added, then removed).
    assert ("add", "disable") in apply_btn.prop_events
    assert ("remove", "disable") in apply_btn.prop_events
    assert apply_btn.prop_events.index(
        ("add", "disable")
    ) < apply_btn.prop_events.index(("remove", "disable"))


def test_apply_button_clears_busy_state_even_when_apply_raises() -> None:
    import asyncio

    ui = _EditableUi()

    async def _boom(_name: str) -> None:
        raise RuntimeError("apply failed")

    _render_editable_target(
        ui, _editable_preview(), SchemaConversionState(), on_apply_object=_boom
    )
    apply_btn = ui.buttons[-1]

    with pytest.raises(RuntimeError, match="apply failed"):
        asyncio.run(apply_btn.click_cb())

    # The finally block still clears the busy state so the button is usable again.
    assert ("remove", "disable") in apply_btn.prop_events


# ---------------------------------------------------------------------------
# _InventoryExistenceChecker (inventory-backed existence for per-object apply)
# ---------------------------------------------------------------------------


def test_inventory_existence_checker_detects_existing_tables() -> None:
    from dsql_migrator.ui.schema_conversion import _InventoryExistenceChecker

    class _Schema:
        def __init__(self, name, tables, views=()):
            self.name = name
            self.tables = tables
            self.views = views

    class _Rel:
        def __init__(self, name):
            self.name = name

    class _Inv:
        def __init__(self, schemas):
            self.schemas = schemas

    inv = _Inv([
        _Schema("public", [_Rel("orders"), _Rel("customers")], [_Rel("v_summary")]),
    ])
    checker = _InventoryExistenceChecker(inv)
    assert checker.object_exists("orders") is True
    assert checker.object_exists("ORDERS") is True  # case-insensitive
    assert checker.object_exists("public.orders") is True
    assert checker.object_exists("v_summary") is True
    assert checker.object_exists("nonexistent") is False


def test_inventory_existence_checker_handles_empty_inventory() -> None:
    from dsql_migrator.ui.schema_conversion import _InventoryExistenceChecker

    class _Inv:
        schemas = []

    checker = _InventoryExistenceChecker(_Inv())
    assert checker.object_exists("anything") is False


# ---------------------------------------------------------------------------
# CDC-live apply block (Step 2 must not apply schema while CDC is streaming)
# ---------------------------------------------------------------------------


def test_cdc_apply_is_blocked_none_probe_is_not_blocked() -> None:
    # No probe wired (tests / data-migration state not connected): apply is free.
    assert _cdc_apply_is_blocked(None) is False


def test_cdc_apply_is_blocked_reflects_probe_result() -> None:
    assert _cdc_apply_is_blocked(lambda: True) is True
    assert _cdc_apply_is_blocked(lambda: False) is False


def test_cdc_apply_is_blocked_coerces_truthy_to_bool() -> None:
    # A truthy/falsey non-bool probe result is normalised to a real bool.
    assert _cdc_apply_is_blocked(lambda: "streaming") is True
    assert _cdc_apply_is_blocked(lambda: 0) is False


def test_cdc_apply_is_blocked_swallows_probe_errors() -> None:
    # A status probe must never break the page: a raising probe reads as "not
    # blocked" so the UI stays usable rather than erroring on render.
    def _boom() -> bool:
        raise RuntimeError("status read failed")

    assert _cdc_apply_is_blocked(_boom) is False


def test_cdc_apply_block_message_is_actionable() -> None:
    # The single-sourced block message points to the two real paths forward
    # (Skip to continue, or stop CDC in Data Migration to change the schema) and
    # explains why applying now is unsafe (DDL is not replicated).
    assert "Skip conversion & continue" in CDC_APPLY_BLOCK_BODY
    assert "Data Migration" in CDC_APPLY_BLOCK_BODY
    assert "DDL is not replicated" in CDC_APPLY_BLOCK_BODY
    assert "CDC is streaming to the target" in CDC_APPLY_BLOCK_HEADER


# ---------------------------------------------------------------------------
# Conversion notes — recommendations are separated from real conversion gaps
# ---------------------------------------------------------------------------


class _NotesUi:
    """NiceGUI double for the notes block. ``cards`` pairs each tinted card surface with the badges rendered inside it, so a test can assert which note got which surface/colour."""

    def __init__(self):
        self.texts: list[str] = []
        self.badges: list[str] = []
        self.icons: list[str] = []
        self.tooltips: list[str] = []
        self.classes: list[str] = []
        self.cards: list[dict] = []

    class _El:
        def __init__(self, rec, badge=None):
            self._rec = rec
            self._badge = badge

        def classes(self, value="", *_a, **_k):
            if value:
                self._rec.classes.append(str(value))
                if "rounded-md" in str(value) and "border" in str(value):
                    self._rec.cards.append({"surface": str(value), "badges": []})
            return self

        def props(self, value="", *_a, **_k):
            if self._badge is not None and "color=" in str(value):
                self._badge["color"] = str(value).split("color=", 1)[1].split()[0]
                if self._rec.cards:
                    self._rec.cards[-1]["badges"].append(self._badge)
            return self

        def tooltip(self, text="", *_a, **_k):
            self._rec.tooltips.append(str(text))
            return self

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def label(self, text="", *_a, **_k):
        if text:
            self.texts.append(str(text))
        return self._El(self)

    def badge(self, text="", *_a, **_k):
        self.badges.append(str(text))
        return self._El(self, badge={"text": str(text), "color": None})

    def icon(self, name="", *_a, **_k):
        if name:
            self.icons.append(str(name))
        return self._El(self)

    def row(self, *_a, **_k):
        return self._El(self)

    def column(self, *_a, **_k):
        return self._El(self)


def _loss_note(message="Foreign keys (fk) are not supported and were removed."):
    return ConversionWarning(
        object_name="t", classification=Classification.MANUAL, message=message
    )


def _recommendation_note(message="The integer key was kept; consider a UUID key."):
    return ConversionWarning(
        object_name="t",
        column_name="id",
        classification=Classification.MANUAL,
        kind=ConversionNoteKind.RECOMMENDATION,
        message=message,
    )


def test_split_conversion_notes_defaults_to_loss() -> None:
    # LOSS is what every note historically meant, so anything that does not opt into
    # RECOMMENDATION -- including a payload restored from an older snapshot -- keeps
    # its current (warning) treatment.
    from dsql_migrator.ui.schema_conversion import split_conversion_notes

    losses, recs = split_conversion_notes([_loss_note(), _recommendation_note()])
    assert len(losses) == 1 and len(recs) == 1
    assert losses[0].kind is ConversionNoteKind.LOSS  # implicit default
    assert split_conversion_notes([]) == ([], [])


def test_conversion_notes_render_recommendations_in_their_own_section() -> None:
    """A recommendation must not wear the amber MANUAL warning badge.

    A kept AUTO_INCREMENT key converts perfectly and works -- switching to a
    UUID/random key is throughput advice, not a defect. It used to sit under
    "Conversion warnings" with the same badge as a removed foreign key, which
    overstated it.
    """
    from dsql_migrator.ui.schema_conversion import _render_conversion_warnings

    ui = _NotesUi()
    _render_conversion_warnings(ui, [_recommendation_note(), _loss_note()])

    assert "Conversion warnings" in ui.texts
    assert "Recommendations" in ui.texts
    # The advice gets the calm RECOMMENDED badge; the real gap keeps its severity.
    assert "RECOMMENDED" in ui.badges
    assert "MANUAL" in ui.badges


def test_conversion_notes_omit_the_warnings_heading_when_only_advice() -> None:
    # A table whose ONLY note is advice must not show a "Conversion warnings" heading
    # at all -- there is nothing wrong with it.
    from dsql_migrator.ui.schema_conversion import _render_conversion_warnings

    ui = _NotesUi()
    _render_conversion_warnings(ui, [_recommendation_note()])
    assert "Conversion warnings" not in ui.texts
    assert "Recommendations" in ui.texts
    assert "MANUAL" not in ui.badges


def test_recommendations_explanation_is_a_tooltip_not_standing_text() -> None:
    # The "optional, not problems to fix" wording belongs in a help tooltip: the badge
    # and heading already carry it, and this block repeats per object, so a permanent
    # paragraph is noise.
    from dsql_migrator.ui.schema_conversion import _render_conversion_warnings

    ui = _NotesUi()
    _render_conversion_warnings(ui, [_recommendation_note()])
    assert "help_outline" in ui.icons
    assert any("not problems to fix" in t for t in ui.tooltips)
    # It must NOT be rendered as a visible label.
    assert not any("not problems to fix" in t for t in ui.texts)


# ---------------------------------------------------------------------------
# DDL diff rendering — AWS code-surface treatment
# ---------------------------------------------------------------------------


class _DiffCellUi:
    """Double recording a diff cell's emitted labels and the classes applied."""

    def __init__(self):
        self.labels: list[str] = []
        self.classes: list[str] = []

    class _El:
        def __init__(self, rec):
            self._rec = rec

        def classes(self, value="", *_a, **_k):
            if value:
                self._rec.classes.append(value)
            return self

        def props(self, *_a, **_k):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def label(self, text="", *_a, **_k):
        self.labels.append(str(text))
        return self._El(self)

    def row(self, *_a, **_k):
        return self._El(self)


# ---------------------------------------------------------------------------
# Object browser — layout + apply lock
# ---------------------------------------------------------------------------


def _browser_fn_source() -> str:
    import inspect

    from dsql_migrator.ui import schema_conversion as sc

    return inspect.getsource(sc._render_browser_and_preview)


def test_bulk_select_buttons_live_in_the_source_header() -> None:
    """Select all / Unselect all belong on the "Source (MySQL)" header row.

    On their own row below the header they pushed the source filter box down, so the
    source and target panels started at different y-positions and the side-by-side
    comparison read as misaligned.
    """
    src = _browser_fn_source()
    header_at = src.index('"Source (MySQL)"')
    select_all_at = src.index('"Select all"')
    filter_at = src.index('placeholder="Filter objects by name"')
    # The bulk buttons come AFTER the header label and BEFORE the filter input...
    assert header_at < select_all_at < filter_at
    # ...and inside the same header row, i.e. no separate full-width row is opened
    # between the header label and the buttons.
    between = src[header_at:select_all_at]
    assert 'classes("items-center gap-1 w-full no-wrap")' not in between


def test_pk_legend_is_below_the_tree_so_both_panels_align() -> None:
    # The legend used to sit between the filter and the tree, pushing the SOURCE tree
    # down while the target tree started right after its filter.
    src = _browser_fn_source()
    filter_at = src.index('placeholder="Filter objects by name"')
    tree_at = src.index("tree = ui.tree(")
    legend_at = src.index('"Table has a primary key"')
    assert filter_at < tree_at < legend_at, (
        "the PK legend must render after the tree, not between filter and tree"
    )


def test_object_browser_takes_an_apply_in_progress_flag() -> None:
    import inspect

    from dsql_migrator.ui import schema_conversion as sc

    params = inspect.signature(sc._render_browser_and_preview).parameters
    assert "apply_in_progress" in params
    assert params["apply_in_progress"].default is False  # opt-in, never locks by accident


def test_apply_in_progress_locks_every_selection_control() -> None:
    """A running apply must freeze the selection AND the DDL it is executing.

    The worker was handed a fixed object list at start, so re-ticking cannot change
    what it writes -- it would only desynchronize the screen from the target. Worse,
    "Generate DDL" or "Reset all" mid-run would swap or discard the DDL under the
    in-flight apply.
    """
    src = _browser_fn_source()
    # Each control is guarded by the flag.
    for control in ("select_all_btn", "unselect_all_btn", "src_filter", "tree", "gen_btn"):
        assert control in src, control
    # The tree, filter and bulk buttons are disabled under the flag.
    assert src.count("if apply_in_progress:") >= 4
    # Quasar's q-tree has NO `disable` prop, so props("disable") is silently ignored
    # and the tree would stay fully clickable. It must be blocked with
    # pointer-events-none (the same way the Data Migration table picker does it).
    assert 'tree.classes("pointer-events-none opacity-70")' in src
    assert 'tree.props("disable")' not in src, (
        "q-tree ignores the disable prop -- use pointer-events-none"
    )
    # q-input DOES accept disable, so the filter can use the prop.
    assert 'src_filter.props("disable")' in src
    # Generate and Reset are both blocked (they would change the applied DDL).
    assert "gen_btn.disable()" in src
    assert "reset_btn.disable()" in src
    # And the user is told why, rather than facing silently dead controls.
    assert "Selection is locked while the schema apply runs" in src


def test_apply_in_progress_is_wired_from_the_step_status() -> None:
    # The flag must come from the real in-progress signal, not be hardcoded.
    import inspect

    from dsql_migrator.ui import schema_conversion as sc

    screen_src = inspect.getsource(sc.build_schema_conversion_screen)
    assert "apply_in_progress=status is StepStatus.IN_PROGRESS" in screen_src


# ---------------------------------------------------------------------------
# View source DDL — readable, and diffable against the pretty-printed target
# ---------------------------------------------------------------------------


_RAW_VIEW = (
    "CREATE ALGORITHM=UNDEFINED DEFINER=`dalyoung`@`%` SQL SECURITY DEFINER VIEW "
    "`ecommerce_demo`.`customer_order_summary` AS select `c`.`customer_id` AS "
    "`customer_id`,`co`.`country_name` AS `country_name`,count(distinct "
    "`o`.`order_id`) AS `order_count` from ((`ecommerce_demo`.`customers` `c` join "
    "`ecommerce_demo`.`countries` `co` on((`co`.`country_id` = `c`.`country_id`))) "
    "left join `ecommerce_demo`.`orders` `o` on((`o`.`customer_id` = "
    "`c`.`customer_id`))) group by `c`.`customer_id`,`co`.`country_name`"
)


def test_view_source_ddl_is_pretty_printed() -> None:
    """MySQL returns SHOW CREATE VIEW on ONE line; the diff needs it formatted.

    Shown raw it was an unreadable wall of text -- and unusable in the side-by-side
    view, where the target side IS pretty-printed, so a one-line source could never
    align with it.
    """
    from dsql_migrator.ui.schema_conversion import render_source_view_ddl

    out = render_source_view_ddl(
        ViewDef(name="ecommerce_demo.customer_order_summary", definition=_RAW_VIEW)
    )
    assert len(out.splitlines()) > 5, "the one-line definition must be broken up"
    assert "CREATE VIEW" in out
    # The SELECT list is on its own lines, not run together.
    assert "SELECT\n" in out


def test_view_source_ddl_drops_server_metadata() -> None:
    # ALGORITHM/DEFINER/SQL SECURITY is server bookkeeping irrelevant to the
    # conversion -- and sqlglot mangles DEFINER's backticks into double quotes when it
    # round-trips them, which would show the user invalid MySQL.
    from dsql_migrator.ui.schema_conversion import render_source_view_ddl

    out = render_source_view_ddl(ViewDef(name="v", definition=_RAW_VIEW))
    assert "DEFINER" not in out
    assert "ALGORITHM" not in out
    assert "SQL SECURITY" not in out
    assert '"dalyoung"' not in out  # the mangled form must never appear


def test_view_source_ddl_keeps_unparseable_definitions_verbatim() -> None:
    # An unparseable definition is exactly when the user needs the source as-is.
    from dsql_migrator.ui.schema_conversion import render_source_view_ddl

    junk = "CREATE VIEW x AS SELECT ((( not sql at all %%%"
    assert render_source_view_ddl(ViewDef(name="x", definition=junk)) == junk


def test_view_source_ddl_wraps_a_bare_select_body() -> None:
    # Some introspection paths return only the SELECT body; the CREATE VIEW header
    # must survive pretty-printing or the source loses the object's identity.
    from dsql_migrator.ui.schema_conversion import render_source_view_ddl

    out = render_source_view_ddl(ViewDef(name="s.v", definition="select 1 as a"))
    assert out.startswith("CREATE VIEW s.v")
    assert "SELECT" in out


def test_view_source_ddl_reports_a_missing_definition() -> None:
    from dsql_migrator.ui.schema_conversion import render_source_view_ddl

    out = render_source_view_ddl(ViewDef(name="s.v", definition=""))
    assert "unavailable" in out.lower()
    assert "s.v" in out


class _DdlPaneUi:
    """Double recording what the DDL comparison renders (codemirror calls, labels)."""

    def __init__(self):
        self.labels: list[str] = []
        self.editors: list[dict] = []
        self.css: list[str] = []
        self.clicks: list = []
        self.dialogs_opened: int = 0

    class _El:
        def __init__(self, rec):
            self._rec = rec

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def __getattr__(self, _name):
            return lambda *_a, **_k: self

    def codemirror(self, value="", **kwargs):
        entry = {"value": value, **kwargs, "props": [], "classes": []}
        self.editors.append(entry)
        return self._PropEl(self, entry)

    class _PropEl:
        """Element double recording ``.props()`` and ``.classes()`` on its editor."""

        def __init__(self, rec, entry):
            self._rec = rec
            self._entry = entry

        def props(self, value="", *_a, **_k):
            if value:
                self._entry["props"].append(str(value))
            return self

        def classes(self, value="", *_a, **_k):
            if value:
                self._entry["classes"].append(str(value))
            return self

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def __getattr__(self, _name):
            return lambda *_a, **_k: self

    def label(self, text="", *_a, **_k):
        self.labels.append(str(text))
        return self._El(self)

    def add_css(self, css="", *_a, **_k):
        self.css.append(str(css))

    def button(self, *_a, on_click=None, **_k):
        # Record every button's click handler so a test can invoke expand and see what it
        # does, instead of only grepping the source for the button's existence.
        if on_click is not None:
            self.clicks.append(on_click)
        return self._El(self)

    class _Dialog:
        def __init__(self, rec):
            self._rec = rec

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def open(self):
            self._rec.dialogs_opened += 1

        def close(self, *_a, **_k):
            return None

        def __getattr__(self, _name):
            return lambda *_a, **_k: self

    def dialog(self, *_a, **_k):
        return _DdlPaneUi._Dialog(self)

    def __getattr__(self, _name):
        return lambda *_a, **_k: self._El(self)


def test_ddl_comparison_renders_each_side_in_its_own_dialect() -> None:
    """Each pane is a real editor highlighted in ITS dialect, not one generic code block.

    The hand-built diff table this replaces had no syntax highlighting at all, and wrapped
    long lines with ``break-all`` -- an ENUM list came out split mid-token across two visual
    rows, which also pushed the two sides out of vertical alignment.
    """
    from dsql_migrator.ui.schema_conversion import _render_ddl_diff

    ui = _DdlPaneUi()
    _render_ddl_diff(ui, "CREATE TABLE `t` (`id` int)", 'CREATE TABLE "t" ("id" INT)')

    assert len(ui.editors) == 2, ui.editors
    source, target = ui.editors
    assert source["language"] == "MySQL", source
    assert target["language"] == "PostgreSQL", target
    assert source["value"] == "CREATE TABLE `t` (`id` int)"
    assert target["value"] == 'CREATE TABLE "t" ("id" INT)'
    # Both sides are still named, so the panes are never ambiguous.
    assert "Source — MySQL" in ui.labels
    assert "Target — Aurora DSQL" in ui.labels


def test_ddl_panes_do_not_wrap_lines() -> None:
    # One logical line stays one line and the editor scrolls horizontally, as a Markdown
    # fence and every editor do. Wrapping is what split ``'cancelled')`` mid-token before.
    from dsql_migrator.ui.schema_conversion import _render_ddl_diff

    ui = _DdlPaneUi()
    _render_ddl_diff(ui, "a", "b")
    for editor in ui.editors:
        assert editor["line_wrapping"] is False, editor


def test_ddl_pane_height_css_targets_the_editor_not_the_wrapper() -> None:
    """CodeMirror renders its own DOM, so a height on the wrapper does not reach it.

    A ``max-height`` on the outer element left ``.cm-editor`` at its default 256px and the
    taller pane was cut off mid-statement with no scrollbar to reveal it.
    """
    from dsql_migrator.ui.schema_conversion import _DDL_PANE_CSS, _render_ddl_diff

    # The INLINE pane rules must exist in their own right, not be satisfied by the unrelated
    # .ddl-expanded rules that carry the same substrings. Deleting the .ddl-pane block sent
    # the comparison panes back to CodeMirror's 256px default with no scroller cap -- the
    # clipping bug these rules fix -- yet a whole-sheet substring search still passed.
    pane_rules = [
        line for line in _DDL_PANE_CSS.splitlines()
        if line.strip().startswith(".ddl-pane")
    ]
    assert any(".ddl-pane .cm-editor" in r for r in pane_rules), _DDL_PANE_CSS
    scroller = next((r for r in pane_rules if ".ddl-pane .cm-scroller" in r), "")
    assert scroller, _DDL_PANE_CSS
    assert "max-height" in scroller and "overflow: auto" in scroller, scroller

    # And the inline panes actually carry the ddl-pane class, or the rules apply to nothing.
    ui = _DdlPaneUi()
    _render_ddl_diff(ui, "a", "b")
    assert ui.editors, "expected both panes to render"
    for editor in ui.editors:
        assert any("ddl-pane" in c for c in editor["classes"]), editor
    assert _DDL_PANE_CSS in ui.css, "the pane stylesheet was not injected"


def test_ddl_comparison_panes_are_not_editable() -> None:
    """The comparison is read-only; editing has its own mode behind the Edit button.

    ``readonly`` is the obvious guess and NiceGUI's CodeMirror silently ignores it -- the
    pane stayed editable (verified in a browser: contenteditable=true, typing worked), so a
    user could change the DDL here, watch it vanish on the next re-render, and have
    "Apply to target" still send the unedited version. ``disable`` reconfigures CodeMirror's
    ``editable`` compartment and actually blocks input.
    """
    from dsql_migrator.ui.schema_conversion import _render_ddl_diff

    ui = _DdlPaneUi()
    _render_ddl_diff(ui, "CREATE TABLE `t` (`id` int)", 'CREATE TABLE "t" ("id" INT)')

    assert ui.editors, "expected both panes to render"
    for editor in ui.editors:
        props = " ".join(editor["props"])
        assert "disable" in props, editor
        # A no-op prop must not stand in for the one that works.
        assert "readonly" not in props, editor
        # A read-only pane never writes back, so it takes no change handler.
        assert editor.get("on_change") is None, editor


def test_ddl_comparison_shows_the_edited_ddl_not_the_generated_one() -> None:
    """The comparison must show the edited DDL, since that is what Apply sends.

    Rendered, not grepped: an earlier version asserted the two source lines exist, so a later
    line that clobbered ``current`` back to the generated DDL passed while the saved edit
    vanished from view. This checks the target pane actually receives the edit.
    """
    from dsql_migrator.ui.schema_conversion import _render_editable_target

    edited = 'CREATE TABLE "t" ("id" INT); -- my edit'
    ui = _editable_target_ui()
    _render_editable_target(ui, _StubPreview(), _StubConvState(edited=edited))

    # Read-only mode renders two editors: source (left) and the effective target (right).
    targets = [e["value"] for e in ui.editors if e.get("language") == "PostgreSQL"]
    assert targets == [edited], (targets, edited)
    # The generated DDL is NOT what is shown when an edit exists.
    assert _StubPreview.target_ddl not in targets, targets


def _editable_target_ui():
    """A ``_DdlPaneUi`` that also records buttons, so both modes can be inspected."""

    class _Ui(_DdlPaneUi):
        def __init__(self):
            super().__init__()
            self.buttons: list[str] = []
            self.badges: list[str] = []

        def button(self, text="", *_a, on_click=None, **_k):
            if text:
                self.buttons.append(str(text))
            if on_click is not None:
                self.clicks.append(on_click)
            return self._El(self)

        def badge(self, text="", *_a, **_k):
            if text:
                self.badges.append(str(text))
            return self._El(self)

    return _Ui()


class _StubConvState:
    def __init__(self, edited=None):
        self._edited = edited

    def get_edited_target_ddl(self, _name):
        return self._edited

    def set_edited_target_ddl(self, _name, _value):
        pass

    def clear_edited_target_ddl(self, _name):
        pass

    def get_apply_result(self, _name):
        return None


class _StubPreview:
    object_name = "ecommerce.orders"
    source_ddl = "CREATE TABLE `orders` (`id` int)"
    target_ddl = 'CREATE TABLE "orders" ("id" INT)'


def test_edit_mode_labels_the_pane_as_the_target() -> None:
    """Entering Edit must not leave an unlabeled code box.

    The editor used to render bare: both headers disappeared, so nothing said it is the
    TARGET being changed. Since the source pane is read-only by design, mistaking one for
    the other is a plausible misread of a screen whose whole point is source-vs-target.
    """
    from dsql_migrator.ui.schema_conversion import _render_editable_target

    ui = _editable_target_ui()
    _render_editable_target(ui, _StubPreview(), _StubConvState())
    # Read-only mode names both sides.
    assert "Source — MySQL" in ui.labels
    assert "Target — Aurora DSQL" in ui.labels

    # The editing branch is only reachable through the Edit button's handler, which a
    # double cannot click, so assert on that branch's source.
    import inspect

    from dsql_migrator.ui import schema_conversion

    src = inspect.getsource(schema_conversion._render_editable_target)
    editing_branch = src.split("else:", 1)[1]
    assert "_render_ddl_header(" in editing_branch, editing_branch[:400]
    assert 'title="Target — Aurora DSQL"' in editing_branch
    # The Editing badge rides on that header band, not loose below the editor.
    assert 'trailing=lambda: ui.badge("Editing")' in editing_branch


def test_edit_mode_editor_matches_the_target_pane_treatment() -> None:
    # Same dialect highlighting and no wrapping as the read-only target pane, so switching
    # into Edit does not change how the SQL reads.
    import inspect

    from dsql_migrator.ui import schema_conversion

    editing = inspect.getsource(schema_conversion._render_editable_target).split(
        "else:", 1
    )[1]
    assert 'language="PostgreSQL"' in editing, editing[:400]
    assert "line_wrapping=False" in editing
    assert "ddl-pane" in editing
    # It IS editable -- unlike the comparison panes, this one writes to the buffer.
    assert "on_change=on_edit" in editing
    assert '.props("disable")' not in editing


def test_each_ddl_pane_offers_an_expand_to_a_content_sized_dialog() -> None:
    """A split view gives each pane half the window, which the DDL often outgrows.

    Measured against a real source: 14 of 18 tables had a line too long for a half-width
    pane and 4 exceeded its height. Both scroll, but reading a 144-character CHECK
    constraint through a half-width porthole is what makes an operator copy the DDL out to
    an editor instead of reviewing it here.
    """
    import inspect

    from dsql_migrator.ui import schema_conversion

    src = inspect.getsource(schema_conversion._render_ddl_diff)
    # Both panes opt in, each with its own dialect so the expanded view highlights the same
    # way the pane did.
    assert 'expand_language="MySQL"' in src, src
    assert 'expand_language="PostgreSQL"' in src, src

    expand = inspect.getsource(schema_conversion._render_expand_ddl_button)
    # Sized to the CONTENT, over the page rather than replacing it. A maximized dialog
    # covered a 1440x900 display to show a panel that needs ~1060x800 at its widest
    # (144-char line, 29 lines measured across a real source), and losing the page behind it
    # costs the context the comparison sat in.
    assert '.props("maximized")' not in expand, expand
    assert "width: min(1100px, 92vw)" in expand, expand
    # Read-only, like the pane it expands.
    assert '.props("disable")' in expand, expand
    # Its own height class, or it would inherit the 26rem cap it exists to escape. Check
    # the class actually applied, not prose: the docstring mentions ``ddl-pane`` by name.
    assert '.classes("w-full ddl-expanded")' in expand, expand
    assert '.classes("w-full ddl-pane")' not in expand, expand


def test_expanded_ddl_css_sizes_the_wrapper_and_the_scroller() -> None:
    """Sizing only the scroller leaves the wrapper at CodeMirror's 256px default.

    That produced a clipped editor under a tall blank band -- the same trap as the pane,
    one element further out.
    """
    from dsql_migrator.ui.schema_conversion import _DDL_PANE_CSS

    assert ".ddl-expanded {" in _DDL_PANE_CSS, _DDL_PANE_CSS
    assert ".ddl-expanded .cm-editor" in _DDL_PANE_CSS
    assert ".ddl-expanded .cm-scroller" in _DDL_PANE_CSS
    # ``height: auto`` on the WRAPPER is what lets the dialog track the DDL: CodeMirror
    # falls back to a fixed 256px otherwise, which pinned a 29-line DDL to the same height
    # as a 4-line one. Check that rule specifically -- ``.cm-editor`` carries the same
    # declaration, so searching the whole sheet for the string would pass without it.
    wrapper_rule = next(
        line for line in _DDL_PANE_CSS.splitlines()
        if line.strip().startswith(".ddl-expanded {")
    )
    assert "height: auto" in wrapper_rule, wrapper_rule
    # A cap still keeps a huge DDL on screen, and it is taller than the inline pane.
    assert "min(44rem, 74vh)" in _DDL_PANE_CSS
    assert "26rem" in _DDL_PANE_CSS  # the pane cap is still there for the inline view


def test_edit_mode_offers_no_expand() -> None:
    # The dialog is read-only; offering it beside a live editor would invite edits into a
    # copy that is discarded on close. In Edit the pane is already full width anyway.
    import inspect

    from dsql_migrator.ui import schema_conversion

    editing = inspect.getsource(schema_conversion._render_editable_target).split(
        "else:", 1
    )[1]
    assert "expand_language" not in editing, editing[:500]


def test_conversion_note_cards_match_the_evaluation_finding_treatment() -> None:
    """A note is one of this app's cards, not an outlined table cell.

    The rows carried a bare ``border``, which renders Tailwind's default near-black: it read
    as a table and put a harder line around a recommendation than Evaluation puts around an
    UNSUPPORTED finding. Both screens now use the same tinted surface with a matching *-200
    border.
    """
    from dsql_migrator.ui.schema_conversion import _render_conversion_warnings

    ui = _NotesUi()
    _render_conversion_warnings(ui, [_loss_note(), _recommendation_note()])

    # No uncolored border anywhere -- a bare `border` is Tailwind's near-black default.
    assert not any(
        "border" in c.split() and not any(t.startswith("border-") for t in c.split())
        for c in ui.classes
    ), ui.classes

    # WHICH note gets WHICH surface, not merely that both surfaces appear. A LOSS is the
    # neutral card, a RECOMMENDATION the calm sky one -- the pair Evaluation uses. Asserting
    # only that both strings exist let a severity inversion (LOSS painted sky, advice gray)
    # pass; this pins each card to its note.
    by_badge = {}
    for card in ui.cards:
        assert "rounded-md" in card["surface"], card  # not the tighter `rounded`
        for badge in card["badges"]:
            by_badge[badge["text"]] = card["surface"]
    # The loss note badges MANUAL; the recommendation badges RECOMMENDED.
    assert "border-gray-200 bg-gray-50" in by_badge.get("MANUAL", ""), ui.cards
    assert "border-sky-200 bg-sky-50" in by_badge.get("RECOMMENDED", ""), ui.cards


def test_conversion_note_badge_colors_track_severity() -> None:
    """A LOSS badge is amber/red; a RECOMMENDATION badge is calm info -- never inverted.

    The only prior assertion was ``"RECOMMENDED" in ui.badges`` (text), so flipping the
    advisory badge to ``negative`` (red advice) passed. This checks the colour each note's
    badge actually carries.
    """
    from dsql_migrator.ui.schema_conversion import _render_conversion_warnings

    ui = _NotesUi()
    _render_conversion_warnings(ui, [_loss_note(), _recommendation_note()])
    color_of = {
        b["text"]: b["color"] for card in ui.cards for b in card["badges"]
    }
    # A real gap is warning-toned; advice is the calm info tone, not a severity colour.
    assert color_of.get("MANUAL") == "warning", color_of
    assert color_of.get("RECOMMENDED") == "info", color_of


def test_conversion_note_surfaces_come_from_the_same_tokens_evaluation_uses() -> None:
    # Pin the pairing itself: if Evaluation restyles its cards, this assertion is what
    # surfaces that Schema Conversion was left behind.
    import inspect

    from dsql_migrator.ui import evaluation, schema_conversion

    notes = inspect.getsource(schema_conversion._render_conversion_warnings)
    concern = inspect.getsource(evaluation._render_assessment_item)
    for surface in ("border-gray-200 bg-gray-50", "border-sky-200 bg-sky-50"):
        assert surface in notes, surface
        assert surface in concern, surface


def test_expand_button_opens_a_dialog_with_an_expanded_editor() -> None:
    """The expand affordance must actually open something.

    Every prior assertion about expand was an inspect.getsource() grep, so deleting
    dialog.open(), inverting the render guard, or making the handler raise all left the suite
    green. This calls the button's handler directly and checks a dialog opened and rendered
    the DDL in the EXPANDED editor surface (not the inline-capped one).
    """
    from dsql_migrator.ui.schema_conversion import _render_expand_ddl_button

    ui = _DdlPaneUi()
    _render_expand_ddl_button(
        ui, 'CREATE TABLE "t" ("id" INT)', title="Target — Aurora DSQL",
        language="PostgreSQL",
    )
    # The button registered a click handler; nothing has opened yet.
    assert ui.clicks, "expand button has no click handler"
    assert ui.dialogs_opened == 0
    assert ui.editors == []

    ui.clicks[-1]()  # click Expand

    assert ui.dialogs_opened == 1, "expand click did not open a dialog"
    # The dialog rendered the DDL in an editor, in the expanded surface, read-only.
    assert len(ui.editors) == 1, ui.editors
    editor = ui.editors[0]
    assert editor["value"] == 'CREATE TABLE "t" ("id" INT)'
    assert editor["language"] == "PostgreSQL"
    assert "disable" in editor["props"], editor


def test_ddl_diff_wires_an_expand_handler_to_each_pane() -> None:
    # Both comparison panes get an expand handler (over the copy handlers), so neither side
    # is left without the affordance.
    from dsql_migrator.ui.schema_conversion import _render_ddl_diff

    ui = _DdlPaneUi()
    _render_ddl_diff(ui, "CREATE TABLE `t` (`id` int)", 'CREATE TABLE "t" ("id" INT)')
    # Clicking every registered handler must open exactly two dialogs -- one per pane's
    # expand button (the copy handlers open none).
    for click in ui.clicks:
        click()
    assert ui.dialogs_opened == 2, (ui.dialogs_opened, len(ui.clicks))


# ---------------------------------------------------------------------------
# Apply controls: the bulk action must read as the action ON the Generated DDL
# list above it, not as a separate feature one card away.
# ---------------------------------------------------------------------------


class _ApplyControlsUi:
    """Records label text, button labels and select options for the apply card."""

    def __init__(self) -> None:
        self.texts: list[str] = []
        self.buttons: list[str] = []

    class _El:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def __getattr__(self, _name):
            return lambda *_a, **_k: self

    def label(self, text="", *_a, **_k):
        if text:
            self.texts.append(str(text))
        return self._El()

    def button(self, text="", *_a, **_k):
        if text:
            self.buttons.append(str(text))
        return self._El()

    def __getattr__(self, _name):
        return lambda *_a, **_k: _ApplyControlsUi._El()


def _render_apply_card(count: int, *, in_progress: bool = False):
    from dsql_migrator.ui.schema_conversion import (
        SchemaConversionState,
        _render_apply_controls,
    )

    ui = _ApplyControlsUi()
    _render_apply_controls(
        ui,
        SchemaConversionState(),
        lambda: None,
        on_apply_all=lambda: None,
        table_count=count,
        in_progress=in_progress,
    )
    return ui


def test_apply_card_names_the_generated_ddl_it_applies() -> None:
    """The header and button must name their OBJECT, and the count must tie them to the
    list above.

    Reported in review: the bulk apply sits in a separate card below the Generated DDL
    list, so it read as a different feature -- and its title was the literal string
    "Apply to target", identical to each row's per-object button label. The copy even
    had to point back with "...in the Generated DDL list above" twice, which is the tell
    that the layout wasn't carrying the relationship.
    """
    ui = _render_apply_card(7)

    assert "Apply generated DDL to target" in ui.texts
    # Not the bare, scope-ambiguous old title.
    assert "Apply to target" not in ui.texts
    # The count is the concrete tie to the list (it moves with the selection).
    assert any("7 objects from the Generated DDL list above" in t for t in ui.texts)
    assert "Apply all 7 generated objects to target" in ui.buttons
    # Still says it is not the whole schema.
    assert any("not the whole schema" in t for t in ui.texts)


def test_apply_card_uses_singular_and_drops_the_redundant_pointer() -> None:
    # With exactly one object in scope, "use its Apply to target button instead" would
    # tell the user to do what the button already does, so it is omitted.
    ui = _render_apply_card(1)

    assert "Apply all 1 generated object to target" in ui.buttons
    assert any("the 1 object from the Generated DDL list" in t for t in ui.texts)
    assert not any("To apply just one object instead" in t for t in ui.texts)


def test_apply_card_keeps_the_single_object_pointer_for_a_multi_object_scope() -> None:
    ui = _render_apply_card(7)
    assert any("To apply just one object instead" in t for t in ui.texts)


def test_apply_card_button_shows_progress_label_while_applying() -> None:
    # The label stays visible while applying (Quasar's `loading` would blank it).
    ui = _render_apply_card(7, in_progress=True)
    assert "Applying…" in ui.buttons
    assert not any(b.startswith("Apply all") for b in ui.buttons)


# ---------------------------------------------------------------------------
# Object browser: expansion must survive a re-render (Generate DDL / apply / poll)
# ---------------------------------------------------------------------------


def test_object_browser_trees_restore_their_expansion_after_a_rerender() -> None:
    """Pressing "Generate DDL for selected" collapsed the whole tree.

    The tree is rebuilt on every render (Generate, an apply, the progress poll) and
    NiceGUI's tree keeps open/closed state CLIENT-side, so anything not restored in Python
    snaps shut -- hiding the very tables the operator had just drilled into and ticked.
    Ticks were already carried across; expansion was the missing half. The target pane has
    the same problem: it is what the operator compares against while working.
    """
    import ast
    import inspect

    from dsql_migrator.ui import schema_conversion as module

    src = inspect.getsource(module)
    tree = ast.parse(src)

    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "tree"
    ]
    assert len(calls) == 2, f"expected the source + target trees, found {len(calls)}"
    # BOTH trees record their open nodes...
    for call in calls:
        kwargs = {kw.arg for kw in call.keywords if kw.arg}
        assert "on_expand" in kwargs, (
            f"ui.tree at line {call.lineno} does not record expansion"
        )
    # ...and both restore them on the next render.
    expands = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "expand"
    ]
    assert len(expands) == 2, f"expected two .expand() restores, found {len(expands)}"


def test_expansion_state_defaults_empty_and_survives_clear() -> None:
    """Fresh state starts collapsed; "Clear" discards ANALYSIS, not navigation.

    reset_generation() drops the generated DDL, edits and AI suggestions -- but where the
    operator had navigated to in the browser is not analysis, and collapsing the tree on
    Clear would repeat the bug this fixes.
    """
    from dsql_migrator.ui.schema_conversion import SchemaConversionState

    state = SchemaConversionState()
    assert state.expanded_node_ids == []
    assert state.target_expanded_node_ids == []

    state.expanded_node_ids = ["ecommerce", "ecommerce.orders"]
    state.target_expanded_node_ids = ["public"]
    state.generated_node_ids = ["ecommerce.orders"]
    state.reset_generation()
    assert state.generated_node_ids is None  # analysis discarded
    assert state.expanded_node_ids == ["ecommerce", "ecommerce.orders"]
    assert state.target_expanded_node_ids == ["public"]


# ---------------------------------------------------------------------------
# One vocabulary across screens: the tree must not rename Evaluation's objects
# ---------------------------------------------------------------------------


def test_object_tree_uses_evaluations_vocabulary_for_routines() -> None:
    """Reported from a workshop: two screens named the same objects differently.

    Evaluation reports "Stored procedures" / "Functions" (assessor.KIND_LABELS); the
    conversion tree lumped both under "Routines (n)", so attendees could not match one
    screen's list to the other's. The introspector already distinguishes PROCEDURE from
    FUNCTION, so the tree was discarding information it had.
    """
    from dsql_migrator.core.assessor import kind_label
    from dsql_migrator.core.models import (
        ObjectRef,
        ObjectType,
        SourceInventory,
        TableDef,
    )
    from dsql_migrator.ui.schema_conversion import build_object_tree

    inventory = SourceInventory(
        tables=[TableDef(name="ecommerce.orders", columns=[], primary_key=["id"])],
        routines=[
            ObjectRef(name="ecommerce.sp_reorder", object_type=ObjectType.PROCEDURE),
            ObjectRef(name="ecommerce.sp_audit", object_type=ObjectType.PROCEDURE),
            ObjectRef(name="ecommerce.fn_total", object_type=ObjectType.FUNCTION),
        ],
    )
    tree = build_object_tree(inventory)
    labels = [cat["label"] for cat in tree[0]["children"]]

    # The headings are Evaluation's, and come from the shared mapping rather than being
    # restated here -- restating them is how the two screens drifted apart.
    assert f"{kind_label('PROCEDURE')} (2)" in labels
    assert f"{kind_label('FUNCTION')} (1)" in labels
    # ...and the generic heading is gone when every routine has a kind.
    assert not any(label.startswith("Routines") for label in labels)


def test_object_tree_keeps_routine_node_ids_stable_across_the_split() -> None:
    """The node ID must stay ``routine:<name>`` whatever the kind.

    IDs are parsed back via _OBJECT_NODE_PREFIXES and are what the ticked / generated sets
    persist across renders -- so re-keying them by kind would silently invalidate a
    restored selection (and the DDL-generation scope with it). Only the DISPLAY grouping
    changed.
    """
    from dsql_migrator.core.models import ObjectRef, ObjectType, SourceInventory
    from dsql_migrator.ui.schema_conversion import ROUTINE_PREFIX, build_object_tree

    inventory = SourceInventory(
        routines=[
            ObjectRef(name="ecommerce.sp_x", object_type=ObjectType.PROCEDURE),
            ObjectRef(name="ecommerce.fn_y", object_type=ObjectType.FUNCTION),
        ],
    )
    ids = [
        leaf["id"]
        for cat in build_object_tree(inventory)[0]["children"]
        for leaf in cat.get("children", [])
    ]
    assert sorted(ids) == [
        f"{ROUTINE_PREFIX}ecommerce.fn_y",
        f"{ROUTINE_PREFIX}ecommerce.sp_x",
    ]


def test_object_tree_still_groups_an_untyped_routine() -> None:
    """An untyped routine keeps the generic heading rather than being mislabelled.

    ``ObjectType.ROUTINE`` is the introspector's fallback when MySQL's ROUTINE_TYPE is
    neither PROCEDURE nor FUNCTION; forcing it into one of the named buckets would state
    something the catalog did not say.
    """
    from dsql_migrator.core.assessor import kind_label
    from dsql_migrator.core.models import ObjectRef, ObjectType, SourceInventory
    from dsql_migrator.ui.schema_conversion import build_object_tree

    inventory = SourceInventory(
        routines=[
            ObjectRef(name="ecommerce.mystery", object_type=ObjectType.ROUTINE),
        ],
    )
    labels = [cat["label"] for cat in build_object_tree(inventory)[0]["children"]]
    assert f"{kind_label('ROUTINE')} (1)" in labels
    assert not any(label.startswith("Stored procedures") for label in labels)
    assert not any(label.startswith("Functions") for label in labels)


# ---------------------------------------------------------------------------
# The source DDL must show what the target cannot reproduce
# ---------------------------------------------------------------------------


def _auto_inc_table():
    from dsql_migrator.core.models import ColumnDef, TableDef

    return TableDef(
        name="ecommerce.orders",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(
                name="created_at",
                mysql_type="datetime",
                nullable=True,
                default="CURRENT_TIMESTAMP",
                default_is_expression=True,
            ),
            ColumnDef(
                name="updated_at",
                mysql_type="datetime",
                nullable=True,
                default="CURRENT_TIMESTAMP",
                default_is_expression=True,
                auto_update_timestamp=True,
            ),
        ],
        primary_key=["id"],
        auto_increment_column="id",
    )


def test_source_ddl_shows_auto_increment_and_on_update() -> None:
    """Both are behaviour DSQL cannot reproduce, and the diff exists to show that.

    Evaluation already flags them (its AUTO_INCREMENT recommendation and the
    ON_UPDATE_TIMESTAMP MANUAL rule), but the reconstructed source DDL omitted both -- so
    the side-by-side claimed nothing was lost, and an operator following up on that finding
    could not locate the column. Each needs an application change: inserts must supply the
    key, and updated_at must be written explicitly (DSQL has no ON UPDATE and no triggers).
    """
    from dsql_migrator.ui.schema_conversion import render_source_table_ddl

    ddl = render_source_table_ddl(_auto_inc_table())
    assert "`id` int NOT NULL AUTO_INCREMENT" in ddl
    assert "`updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" in ddl
    # A plain timestamp column must NOT gain the clause.
    created = next(line for line in ddl.splitlines() if "created_at" in line)
    assert "ON UPDATE" not in created
    # Only the auto-increment column is marked.
    updated = next(line for line in ddl.splitlines() if "updated_at" in line)
    assert "AUTO_INCREMENT" not in updated


def test_conversion_path_must_not_emit_auto_increment() -> None:
    """The DISPLAY renderer and the CONVERSION builder must stay separate.

    ``converter._build_source_ddl``'s output is PARSED to produce the target, and sqlglot
    transpiles MySQL ``AUTO_INCREMENT`` into ``GENERATED BY DEFAULT AS IDENTITY``. So
    adding the clause there -- the tempting "one-line fix" -- would silently turn every
    default (KEEP_INTEGER) conversion into an identity column, and an INT one, which
    Aurora DSQL rejects outright (identity must be BIGINT with CACHE stated). This test
    pins that boundary: the target of a default conversion stays a plain integer key.
    """
    from dsql_migrator.core.converter import SchemaConverter, SchemaConvertOptions

    conversion = SchemaConverter().convert_table(
        _auto_inc_table(), SchemaConvertOptions()
    )
    target = conversion.target_ddl
    assert "AUTO_INCREMENT" not in target
    assert "IDENTITY" not in target, (
        "the KEEP_INTEGER default must not produce an identity column"
    )
    assert '"id" INT NOT NULL' in target
    # ...and the ON UPDATE clause must not leak into the target either: it is a syntax
    # error there, which is why _strip_on_update exists.
    assert "ON UPDATE" not in target


def test_sqlglot_would_convert_auto_increment_to_identity() -> None:
    """Documents WHY the two renderers are separate, against the real library.

    If sqlglot's behaviour ever changes, this is the assumption that would have silently
    broken -- so it is asserted rather than left as a comment.
    """
    import sqlglot

    parsed = sqlglot.parse_one(
        "CREATE TABLE `t` (`id` int NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`))",
        read="mysql",
    )
    postgres = parsed.sql(dialect="postgres")
    assert "GENERATED BY DEFAULT AS IDENTITY" in postgres
    # And it stays INT -- the type Aurora DSQL refuses for an identity column.
    assert '"id" INT GENERATED BY DEFAULT AS IDENTITY' in postgres


def test_source_ddl_shows_every_unreproducible_source_feature() -> None:
    """Audit result: each captured field that the target cannot reproduce is now visible.

    Comparing ColumnDef/TableDef/IndexDef against what the renderer emitted found six
    omissions, and Evaluation already flagged EVERY one of them (AUTO_INCREMENT,
    ON_UPDATE_TIMESTAMP, CI_COLLATION, GENERATED_COLUMN, PARTITIONED_TABLE, SPATIAL_TYPE).
    So the tool told the operator about each, then showed a diff implying nothing changed.
    """
    from dsql_migrator.core.models import ColumnDef, IndexDef, TableDef
    from dsql_migrator.ui.schema_conversion import render_source_table_ddl

    table = TableDef(
        name="ecommerce.products",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(
                name="name",
                mysql_type="varchar(200)",
                nullable=False,
                collation="utf8mb4_0900_ai_ci",
            ),
            ColumnDef(name="body", mysql_type="text", nullable=True),
            ColumnDef(
                name="margin", mysql_type="decimal(10,2)", nullable=True, generated=True
            ),
        ],
        primary_key=["id"],
        indexes=[
            IndexDef(name="idx_name", columns=["name"], unique=True, index_type="BTREE"),
            IndexDef(
                name="ft_body", columns=["body"], unique=False, index_type="FULLTEXT"
            ),
        ],
        auto_increment_column="id",
        partitioned=True,
    )
    ddl = render_source_table_ddl(table)

    # A case-INSENSITIVE collation silently changes query results on DSQL, so it shows.
    assert "COLLATE utf8mb4_0900_ai_ci" in ddl
    # FULLTEXT has no DSQL equivalent; rendering it as a plain KEY understated the loss.
    assert "FULLTEXT KEY `ft_body`" in ddl
    # BTREE is MySQL's default and adds nothing to the diff, so it stays off.
    assert "BTREE" not in ddl
    assert "UNIQUE KEY `idx_name`" in ddl
    # Generated columns and partitioning are MARKED, not invented: introspection captures
    # only the boolean, never the expression / PARTITION BY body. Reconstructing syntax we
    # never read would make the source DDL lie in a new way.
    assert "GENERATED column" in ddl and "not captured" in ddl
    assert "PARTITION BY" in ddl and "not captured" in ddl


def test_source_ddl_omits_markers_for_a_plain_table() -> None:
    """No noise on an ordinary table: every marker is conditional.

    A schema with none of these features must render exactly as before, or the diff gains
    clutter that trains the reader to skip it.
    """
    from dsql_migrator.core.models import ColumnDef, IndexDef, TableDef
    from dsql_migrator.ui.schema_conversion import render_source_table_ddl

    plain = TableDef(
        name="ecommerce.simple",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="label", mysql_type="varchar(50)", nullable=True),
        ],
        primary_key=["id"],
        indexes=[IndexDef(name="idx_label", columns=["label"], unique=False)],
    )
    ddl = render_source_table_ddl(plain)
    for token in (
        "AUTO_INCREMENT",
        "ON UPDATE",
        "COLLATE",
        "GENERATED",
        "PARTITION",
        "FULLTEXT",
        "SPATIAL",
    ):
        assert token not in ddl, f"{token} leaked into a plain table's DDL:\n{ddl}"
    assert "KEY `idx_label`" in ddl


def test_source_ddl_quotes_literal_defaults_but_not_expressions() -> None:
    """The rendered DDL was invalid SQL: ``DEFAULT pending`` instead of ``'pending'``.

    ``information_schema.COLUMN_DEFAULT`` stores the value UNQUOTED, and it was emitted
    raw -- so the pane's "Copy Source DDL" button handed over text MySQL cannot run (a bare
    ``pending`` reads as a column reference). Present since the initial release.

    ``default_is_expression`` is the discriminator, the same one the converter keys on: it
    comes from EXTRA's DEFAULT_GENERATED flag, the only thing that can distinguish the
    literal string 'CURRENT_TIMESTAMP' from the function call. Verified against the live
    source: the reconstructed orders DDL now executes on MySQL and round-trips identically.
    """
    from dsql_migrator.core.models import ColumnDef, TableDef
    from dsql_migrator.ui.schema_conversion import render_source_table_ddl

    table = TableDef(
        name="ecommerce.orders",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            # An expression: emitted verbatim, or the target would default to a string.
            ColumnDef(
                name="created_at",
                mysql_type="datetime",
                nullable=True,
                default="CURRENT_TIMESTAMP",
                default_is_expression=True,
            ),
            # A literal that LOOKS like an expression -- the case the flag exists for.
            ColumnDef(
                name="label",
                mysql_type="varchar(30)",
                nullable=True,
                default="CURRENT_TIMESTAMP",
                default_is_expression=False,
            ),
            ColumnDef(
                name="status", mysql_type="varchar(20)", nullable=True, default="pending"
            ),
            # An embedded apostrophe must be escaped, not left to break the statement.
            ColumnDef(
                name="owner", mysql_type="varchar(30)", nullable=True, default="O'Brien"
            ),
        ],
        primary_key=["id"],
    )
    lines = {
        line.strip().split("`")[1]: line
        for line in render_source_table_ddl(table).splitlines()
        if "DEFAULT" in line
    }
    assert "DEFAULT CURRENT_TIMESTAMP" in lines["created_at"]
    assert "DEFAULT 'CURRENT_TIMESTAMP'" not in lines["created_at"]
    # ...and the identically-valued LITERAL is quoted.
    assert "DEFAULT 'CURRENT_TIMESTAMP'" in lines["label"]
    assert "DEFAULT 'pending'" in lines["status"]
    assert "DEFAULT 'O''Brien'" in lines["owner"]


def test_source_default_rendering_matches_the_converters_discriminator() -> None:
    """Display and conversion must decide quoting from the SAME signal.

    Two places deciding "is this an expression?" by different means is how they drift; the
    converter's ``_column_default_sql`` reads ``default_is_expression``, so the renderer
    must not fall back to a shape heuristic (which cannot tell a literal 'NOW()' from the
    call).
    """
    import inspect

    from dsql_migrator.ui import schema_conversion as module

    src = inspect.getsource(module._render_source_default)
    assert "default_is_expression" in src
    # No guessing from the value's shape.
    for heuristic in ("upper()", "startswith(", "endswith(", "CURRENT_TIMESTAMP\""):
        assert heuristic not in src, f"shape heuristic leaked in: {heuristic}"


def test_generate_rereads_the_target_before_committing_the_scope() -> None:
    """The reported defect: "already exists on target" survived deleting the target.

    The verdict comes from the cached TargetInventory snapshot (no SQL), filled only by
    Evaluation's browse or the manual "Refresh target" button -- so a target emptied
    since then still pushed the user toward REPLACE (destructive) for objects that were
    gone. Generate must re-read the catalog first, and must do so BEFORE it commits the
    scope, or the diffs render against the stale snapshot anyway.
    """
    import asyncio

    from dsql_migrator.ui.schema_conversion import (
        SchemaConversionState,
        generate_selected_ddl,
    )

    calls: list[str] = []
    state = SchemaConversionState()
    state.ticked_node_ids = ["table:ecommerce.orders"]

    seen: dict = {}

    async def _sync() -> None:
        calls.append("sync")
        # RECORD the scope rather than asserting here: generate_selected_ddl swallows
        # exceptions from this hook (a refresh failure must not block generation), so an
        # inline assert would be caught and the ordering check would silently pass.
        seen["scope_at_sync"] = state.generated_node_ids

    asyncio.run(
        generate_selected_ddl(state, lambda: calls.append("refresh"), sync_target=_sync)
    )

    assert calls == ["sync", "refresh"]
    assert seen["scope_at_sync"] is None, (
        "the target must be re-read BEFORE the generation scope is committed, "
        "or the diffs render against the stale snapshot anyway"
    )
    assert state.generated_node_ids == ["table:ecommerce.orders"]


def test_generate_still_produces_ddl_when_the_target_reread_fails() -> None:
    """A refresh failure must not block generation.

    The diff is the point, and a stale verdict cannot misroute the apply (the apply path
    browses the target again and decides from that live read). Failing closed here would
    turn a transient AWS blip into "the button does nothing".
    """
    import asyncio

    from dsql_migrator.ui.schema_conversion import (
        SchemaConversionState,
        generate_selected_ddl,
    )

    state = SchemaConversionState()
    state.ticked_node_ids = ["table:ecommerce.orders"]
    refreshed: list[str] = []

    async def _boom() -> None:
        raise RuntimeError("DSQL unreachable")

    asyncio.run(
        generate_selected_ddl(
            state, lambda: refreshed.append("refresh"), sync_target=_boom
        )
    )

    assert state.generated_node_ids == ["table:ecommerce.orders"]
    assert refreshed == ["refresh"], "the screen must still re-render"


def test_generate_works_without_a_target_sync_hook() -> None:
    # Tests (and any caller with no target configured) pass no hook; generation must
    # still commit the scope rather than raising.
    import asyncio

    from dsql_migrator.ui.schema_conversion import (
        SchemaConversionState,
        generate_selected_ddl,
    )

    state = SchemaConversionState()
    state.ticked_node_ids = ["table:a"]
    asyncio.run(generate_selected_ddl(state, lambda: None))
    assert state.generated_node_ids == ["table:a"]


def test_generate_accepts_a_synchronous_sync_hook() -> None:
    # The hook is awaited only when awaitable, so a plain callable must work too.
    import asyncio

    from dsql_migrator.ui.schema_conversion import (
        SchemaConversionState,
        generate_selected_ddl,
    )

    calls: list[str] = []
    state = SchemaConversionState()
    state.ticked_node_ids = ["table:a"]
    asyncio.run(
        generate_selected_ddl(
            state, lambda: None, sync_target=lambda: calls.append("sync")
        )
    )
    assert calls == ["sync"]
    assert state.generated_node_ids == ["table:a"]


def test_silent_target_sync_does_not_toast_or_double_render() -> None:
    """The Generate-time re-read must be silent; the manual button still announces.

    Generate renders once after committing its own state, so a toast plus an extra
    re-render from the refresh helper is noise mid-action.
    """
    import inspect

    from dsql_migrator.ui import schema_conversion as sc

    src = inspect.getsource(sc.build_schema_conversion_screen)
    # The announce flag gates BOTH the toast and the helper's own refresh().
    assert "async def refresh_target(*, announce: bool = True)" in src
    gated = src[src.index("if announce:"):]
    assert "Target browser refreshed." in gated.split("\n\n")[0]
    # Generate is wired to the silent variant; the button keeps the announcing one.
    assert "on_sync_target=lambda: refresh_target(announce=False)" in src
    assert "on_refresh_target=refresh_target," in src
