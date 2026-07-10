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
    diff_ddl_lines,
    DiffKind,
    DiffRow,
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


def test_copy_ddl_button_falls_back_when_clipboard_unavailable() -> None:
    ui = _CopyUi(clipboard_fails=True)
    _render_copy_ddl_button(ui, "CREATE TABLE t (id INT);", label="Source DDL")
    ui.on_click()
    # No crash; a calm info toast tells the user to copy from the block instead.
    assert ui.clipboard.written == []
    assert ui.notifications[0][1] == "info"
    assert "source ddl" in ui.notifications[0][0].lower()


def test_diff_ddl_lines_classifies_equal_and_replace() -> None:
    rows = diff_ddl_lines("a\nb\nc", "a\nB\nd")
    assert rows == [
        DiffRow("a", "a", DiffKind.EQUAL),
        DiffRow("b", "B", DiffKind.REPLACE),
        DiffRow("c", "d", DiffKind.REPLACE),
    ]


def test_diff_ddl_lines_handles_pure_insert_and_delete() -> None:
    inserted = diff_ddl_lines("x", "x\ny")
    assert inserted == [
        DiffRow("x", "x", DiffKind.EQUAL),
        DiffRow(None, "y", DiffKind.INSERT),
    ]
    deleted = diff_ddl_lines("x\ny", "x")
    assert deleted == [
        DiffRow("x", "x", DiffKind.EQUAL),
        DiffRow("y", None, DiffKind.DELETE),
    ]


def test_diff_ddl_lines_removed_foreign_key_only_on_source_side() -> None:
    # DSQL removes foreign keys: the FK line must appear on the source side only
    # and never on the target side of any aligned row (Requirement 3.3 / 10.2).
    table = _inventory().tables[0]
    conversion = _converted().tables[0]
    preview = build_table_preview(table, conversion)

    rows = diff_ddl_lines(preview.source_ddl, preview.target_ddl)

    assert any(r.left and "FOREIGN KEY" in r.left for r in rows)
    assert all("FOREIGN KEY" not in (r.right or "") for r in rows)


def test_diff_ddl_lines_async_index_added_on_target_side() -> None:
    # The converted secondary index is emitted as CREATE INDEX ASYNC on the
    # target side and has no source-side counterpart (insert or replace).
    table = _inventory().tables[0]
    conversion = _converted().tables[0]
    preview = build_table_preview(table, conversion)

    rows = diff_ddl_lines(preview.source_ddl, preview.target_ddl)
    async_rows = [r for r in rows if r.right and "CREATE INDEX ASYNC" in r.right]

    assert async_rows
    assert all(r.kind in (DiffKind.INSERT, DiffKind.REPLACE) for r in async_rows)


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

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _PickerUi:
    """Minimal NiceGUI double capturing the picker's toggle + select + notices."""

    def __init__(self):
        self.toggle_el = None
        self.select_el = None
        self.select_options = None
        self.select_value = None
        self.notices: list[tuple[str, str]] = []  # (tone, header)
        self.hints: list[str] = []

    def card(self, *_a, **_k):
        return _PickerEl(self, "card")

    def row(self, *_a, **_k):
        return _PickerEl(self, "row")

    def column(self, *_a, **_k):
        return _PickerEl(self, "column")

    def label(self, *_a, **_k):
        return _PickerEl(self, "label")

    def icon(self, *_a, **_k):
        return _PickerEl(self, "icon")

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

    # Simulate choosing "Composite key" in the segmented control.
    assert ui.toggle_el is not None
    ui.toggle_el.on_change(_event("COMPOSITE"))

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
    ui.toggle_el.on_change(_event("KEEP"))
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
    # No NOT NULL non-PK column -> composite offered but the control is disabled,
    # and no dropdown/notice is rendered.
    assert ui.toggle_el is not None and ui.toggle_el.disabled
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
    assert ui.toggle_el is None


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
