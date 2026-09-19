# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the schema-apply orchestration's destructive REPLACE pre-passes.

Companion to ``test_ui_schema_conversion.py`` (which covers the screen). These cover
``schema_conversion_apply``'s own units: which tables a REPLACE will recreate, the foreign-key
pre-drop that clears the way for them, and the confirmation text that has to disclose it.
"""


# --------------------------------------------------------------------------- #
# A confirmed REPLACE must pre-drop the foreign keys THIS migration owns
#
# Observed live on 0.1.469: with 6 preserved foreign keys applied, editing two primary keys
# and re-applying all 7 objects as Replace reported "created: 6, failed: 1" --
# ecommerce.categories could not be recreated because ecommerce.products.fk_products_category
# still referenced it. The apply pre-dropped the selection's VIEWS but not its foreign keys,
# while Full Load's "drop & reload" path had always pre-dropped them. It was pure ordering:
# products (alphabetically after categories) was recreated without the FK, and re-clicking
# categories then succeeded in 4s.
# --------------------------------------------------------------------------- #


def test_replace_table_names_selects_only_tables() -> None:
    from dsql_migrator.ui.schema_conversion_apply import ApplyObject, replace_table_names

    objects = [
        ApplyObject(
            object_name="ecommerce.categories",
            ddls=(
                'CREATE SCHEMA IF NOT EXISTS "ecommerce"',
                'CREATE TABLE "ecommerce"."categories" ("id" integer PRIMARY KEY)',
                'CREATE INDEX ASYNC "ix_c" ON "ecommerce"."categories" ("id")',
            ),
        ),
        ApplyObject(
            object_name="ecommerce.summary",
            ddls=('CREATE VIEW "ecommerce"."summary" AS SELECT 1',),
        ),
        ApplyObject(object_name="placeholder", ddls=("not a ddl at all",)),
    ]
    # The conversion keys are plain qualified names, so a display label ("TABLE categories")
    # would never match -- the selector must use the DDL parser, not the label helper.
    assert replace_table_names(objects) == ["ecommerce.categories"]


def test_predrop_owned_foreign_keys_drops_each_and_reports_them() -> None:
    from dsql_migrator.ui.schema_conversion_apply import predrop_owned_foreign_keys

    dropped: list[tuple[str, str]] = []
    logged: list[tuple[str, str]] = []
    pairs = [("ecommerce.products", "fk_products_category"),
             ("ecommerce.inventory", "fk_inventory_product")]
    out = predrop_owned_foreign_keys(
        pairs,
        lambda t, c: dropped.append((t, c)),
        on_dropped=lambda t, c: logged.append((t, c)),
    )
    assert dropped == pairs
    assert logged == pairs
    assert out == pairs


def test_predrop_owned_foreign_keys_is_best_effort_per_constraint() -> None:
    # A drop error must not abort the pre-pass or mask the per-object failure that follows:
    # the apply still reports the real error for the object that could not be recreated.
    from dsql_migrator.ui.schema_conversion_apply import predrop_owned_foreign_keys

    seen: list[str] = []

    def _drop(table_name, constraint_name):
        seen.append(constraint_name)
        if constraint_name == "fk_bad":
            raise RuntimeError("permission denied")

    failed: list[tuple[str, str, str]] = []
    out = predrop_owned_foreign_keys(
        [("t1", "fk_bad"), ("t2", "fk_good")], _drop,
        on_failed=lambda t, c, exc: failed.append((t, c, type(exc).__name__)),
    )
    assert seen == ["fk_bad", "fk_good"], "a failure must not stop the remaining drops"
    assert out == [("t2", "fk_good")], "only the ones actually dropped are reported"
    # It must be REPORTED, not swallowed: silently skipping left the operator with the
    # recreate failure and a hint telling them to re-run, which would fail again for the
    # same unreported reason -- an infinite retry with no diagnosis.
    assert failed == [("t1", "fk_bad", "RuntimeError")], failed


def test_a_reporting_error_does_not_break_the_predrop() -> None:
    from dsql_migrator.ui.schema_conversion_apply import predrop_owned_foreign_keys

    def _boom(_t, _c):
        raise RuntimeError("activity log unavailable")

    assert predrop_owned_foreign_keys(
        [("t", "fk")], lambda _t, _c: None, on_dropped=_boom
    ) == [("t", "fk")]


def test_the_fk_predrop_runs_for_every_caller_under_the_confirmed_replace_gate() -> None:
    """The pre-drop belongs INSIDE run_schema_apply, beside its view sibling.

    THE DEFECT the first version of this fix had: the pre-drop was bolted onto the BULK
    caller, but run_schema_apply has a second one -- the per-object "Apply to target" button
    -- and an EDITED object forces REPLACE even in global SKIP mode, so the single-table path
    is the likeliest route straight after a key edit. It reproduced the recreate failure
    verbatim with no pre-drop attempted, and a test that greps the screen for a call site was
    satisfied by the bulk path alone. So assert on run_schema_apply's own behaviour.
    """
    from dsql_migrator.ui.schema_conversion_apply import (
        ApplyMode,
        ApplyObject,
        run_schema_apply,
    )

    class _Applier:
        def apply_object(self, object_name, ddls, on_conflict):
            from dsql_migrator.ui.schema_conversion_apply import ApplyOutcome

            return ApplyOutcome.CREATED

    objects = [
        ApplyObject(
            object_name="ecommerce.categories",
            ddls=('CREATE TABLE "ecommerce"."categories" ("id" integer PRIMARY KEY)',),
        )
    ]
    calls: list[str] = []

    def _predrop(objs):
        calls.append("predrop")

    # Confirmed REPLACE -> the seam runs, once, before any object is applied.
    run_schema_apply(
        objects, applier=_Applier(), mode=ApplyMode.REPLACE, confirmed=True,
        on_object_start=lambda _n: calls.append("apply"),
        predrop_foreign_keys=_predrop,
    )
    assert calls == ["predrop", "apply"], calls

    # UNCONFIRMED replace applies nothing (Property 12), so it must drop nothing either --
    # that would be destruction the operator never approved.
    calls.clear()
    run_schema_apply(
        objects, applier=_Applier(), mode=ApplyMode.REPLACE, confirmed=False,
        on_object_start=lambda _n: calls.append("apply"),
        predrop_foreign_keys=_predrop,
    )
    assert "predrop" not in calls, calls

    # SKIP_IF_EXISTS never recreates a table, so nothing blocks and nothing is dropped.
    calls.clear()
    run_schema_apply(
        objects, applier=_Applier(), mode=ApplyMode.SKIP_IF_EXISTS, confirmed=True,
        on_object_start=lambda _n: calls.append("apply"),
        predrop_foreign_keys=_predrop,
    )
    assert "predrop" not in calls, calls


def test_a_failing_predrop_seam_never_fails_the_apply() -> None:
    # The apply must still report the real per-object outcome; the pre-pass is advisory.
    from dsql_migrator.ui.schema_conversion_apply import (
        ApplyMode,
        ApplyObject,
        ApplyOutcome,
        ObjectApplyStatus,
        run_schema_apply,
    )

    class _Applier:
        def apply_object(self, object_name, ddls, on_conflict):
            return ApplyOutcome.CREATED

    def _boom(_objs):
        raise RuntimeError("no target connection")

    results = run_schema_apply(
        [ApplyObject(object_name="t", ddls=('CREATE TABLE "t" ("id" integer PRIMARY KEY)',))],
        applier=_Applier(), mode=ApplyMode.REPLACE, confirmed=True,
        predrop_foreign_keys=_boom,
    )
    assert [r.status for r in results] == [ObjectApplyStatus.CREATED]


def test_both_apply_call_sites_pass_the_predrop_seam() -> None:
    # Structural companion: the behaviour above is only reached if BOTH callers wire it.
    import inspect

    import dsql_migrator.ui.schema_conversion as sc

    src = inspect.getsource(sc.build_schema_conversion_screen)
    calls = src.count("predrop_foreign_keys=")
    assert calls >= 2, (
        f"only {calls} apply call site(s) pass the pre-drop seam; the per-object "
        '"Apply to target" path reproduced the defect when it was left out'
    )
    # And it must use the same pure selector as the Full Load reload path, so the two paths
    # agree on which constraints are the tool's to drop.
    body = src[src.index("def _predrop_replace_blocking_foreign_keys"):]
    body = body[:body.index("def _submit_apply")]
    assert "foreign_keys_blocking_replace" in body, body
    # The selector must NOT read the live preserve-foreign-keys toggle: unticking it empties
    # foreign_key_ddls without removing anything from the target, so the pre-drop would
    # select nothing exactly when constraints were still live and still blocking.
    assert "preserve_foreign_keys=True" in body, body
    assert "conv_state.preserve_foreign_keys" not in body, body


def test_the_replace_dialog_discloses_the_foreign_keys_it_will_drop() -> None:
    """The pre-drop touches tables the operator did NOT select, so the dialog must say so.

    Property 12 keeps destructive work under the operator's control, and the dialog is where
    that control is exercised. It listed only the SELECTED objects, while the pre-drop removes
    constraints from OTHER tables (the children referencing them) -- and nothing re-creates
    them afterwards. A confirmation that understates its own effect is not informed consent.
    """
    from dsql_migrator.ui.schema_conversion_apply import replace_confirmation_message

    plain = replace_confirmation_message(["ecommerce.categories"])
    assert "ecommerce.categories" in plain
    assert "foreign" not in plain, "nothing to disclose -> unchanged message"

    disclosed = replace_confirmation_message(
        ["ecommerce.categories"],
        [("ecommerce.products", "fk_products_category")],
    )
    assert "ecommerce.categories" in disclosed
    assert "ecommerce.products.fk_products_category" in disclosed, disclosed
    assert "did not select" in disclosed, disclosed
    # And it must not imply the tool puts them back.
    assert "Nothing re-creates" in disclosed, disclosed
    assert "Apply foreign keys" in disclosed, disclosed


def test_the_disclosure_is_grammatical_for_one_and_for_many() -> None:
    from dsql_migrator.ui.schema_conversion_apply import replace_confirmation_message

    one = replace_confirmation_message(["t"], [("c1", "fk_a")])
    many = replace_confirmation_message(["t"], [("c1", "fk_a"), ("c2", "fk_b")])
    assert "the foreign key c1.fk_a" in one and "a table you did not select" in one, one
    assert "the foreign keys c1.fk_a, c2.fk_b" in many, many
    assert "tables you did not select" in many, many
    for text in (one, many):
        # The plural bug this family keeps producing: a pronoun placeholder left singular.
        assert " them is " not in text and " it are " not in text, text
