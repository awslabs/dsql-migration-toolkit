# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CI-09: PostgreSQL's identifier rules have one home, the stdlib-only ``core/pg_names.py``.

``pg_fold_unquoted`` (an UNQUOTED name folds ASCII A-Z only), ``pg_stored_name`` (the
catalog stores a name cut to 63 bytes on a character boundary) and their 63-byte limit lived
in ``core/target_introspector.py`` and were written again in five modules: the foreign key's
``SET NULL (cols)`` reader (``models``), ``converter.fold_identifier`` and ``_PG_NAMEDATALEN``,
the Query Converter's ``_ASCII_FOLD`` (nine folds), the mixed-case note (``assessor``) and the
CDC gate's own 63-byte cut (``cdc.cdc_blocking_foreign_keys``). Each copy is exactly the
canonical rule for every ``str`` (proven below), so each now calls ``core.pg_names``;
``target_introspector`` re-exports all three names (the E2E harness and ``scripts/`` import
``pg_stored_name`` from there) and ``schema_applier`` imports them at its top level instead
of seven lazy imports from the introspector.

Pinned here:

* parity: over a corpus of names (empty, ASCII and non-ASCII capitals, the characters just
  outside A-Z, the Kelvin sign and dotted I that ``str.lower`` would fold, 63 / 64 bytes,
  a cut through a 2- and a 4-byte character, a lone surrogate), every copy gives exactly the
  canonical answer -- through its own public entry point where it has one -- and the
  answers themselves are spelled out by hand;
* the re-exports are the very same objects;
* structure: none of the modules whose copies were replaced spells the PostgreSQL fold or
  the 63-byte cut again (``cdc.py`` keeps ``_cdc_ascii_fold``, deliberately: it is
  Debezium's rule, which happens to agree), and ``pg_names`` imports nothing but the
  standard library.

Pure: no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import ast
import re
import pathlib

import pytest
from sqlglot import exp

from dsql_migrator.core import assessor, cdc, converter, models, query_converter, schema_applier
from dsql_migrator.core import target_introspector as ti
from dsql_migrator.core.models import (
    ColumnDef,
    ForeignKeyDef,
    SourceInventory,
    TableDef,
)

_SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"

# name -> (folded, stored): spelled out by hand.
_GOLDEN = {
    "": ("", ""),
    "a": ("a", "a"),
    "ABC": ("abc", "ABC"),
    "MiXeD_Case": ("mixed_case", "MiXeD_Case"),
    "É_État": ("É_État", "É_État"),           # non-ASCII capitals stay
    "ÀZ": ("Àz", "ÀZ"),
    "@[\\]^_`{": ("@[\\]^_`{", "@[\\]^_`{"),   # just outside A-Z / a-z
    "İstanbul": ("İstanbul", "İstanbul"),      # str.lower would give 'i̇stanbul'
    "\u212aelvin": ("\u212aelvin", "\u212aelvin"),  # KELVIN SIGN: str.lower gives 'k'
    "ß": ("ß", "ß"),
    "a" * 63: ("a" * 63, "a" * 63),
    "A" * 64: ("a" * 64, "A" * 63),
    "é" * 32: ("é" * 32, "é" * 31),            # 64 bytes: the 32nd is cut through
    "ab" + "é" * 31: ("ab" + "é" * 31, "ab" + "é" * 30),
    "X" * 62 + "é": ("x" * 62 + "é", "X" * 62),
    "😀" * 16: ("😀" * 16, "😀" * 15),          # 4-byte characters
    "x" * 61 + "😀": ("x" * 61 + "😀", "x" * 61),
}
_LONE_SURROGATE = "A\ud800b"  # folds; has no UTF-8 form, so it has no stored form either


def _fold_copies(name: str) -> "dict[str, str]":
    """Each fold copy's answer for ``name``, through its own entry point."""
    out = {
        "converter.fold_identifier": converter.fold_identifier(name),
        "query_converter._key": query_converter._key(exp.Identifier(this=name, quoted=False)),
        "schema_applier._ascii_fold": schema_applier._ascii_fold(name),
    }
    if name and not any(ch in name for ch in ' \t(),"'):
        # The SET NULL (cols) list as pg_get_constraintdef prints it: unquoted, then quoted.
        key = ForeignKeyDef(
            name="fk", columns=["a"], referenced_table="p", referenced_columns=["b"],
            on_delete=f'SET NULL ({name}, "{name}")',
        )
        listed = key.on_delete_set_columns
        out["models.on_delete_set_columns"] = listed[0]
        assert listed[1] == name  # a quoted name is kept as written
    return out


@pytest.mark.parametrize("name", [*_GOLDEN, _LONE_SURROGATE], ids=repr)
def test_every_fold_copy_is_the_canonical_fold(name: str) -> None:
    canonical = ti.pg_fold_unquoted(name)
    if name in _GOLDEN:
        assert canonical == _GOLDEN[name][0]
    for copy, answer in _fold_copies(name).items():
        assert answer == canonical, copy
        assert type(answer) is str, copy


@pytest.mark.parametrize("name", list(_GOLDEN), ids=repr)
def test_the_stored_name_is_unchanged(name: str) -> None:
    assert ti.pg_stored_name(name) == _GOLDEN[name][1]
    assert len(ti.pg_stored_name(name).encode("utf-8")) <= 63


def test_a_name_with_no_utf8_form_has_no_stored_form() -> None:
    with pytest.raises(UnicodeEncodeError):
        ti.pg_stored_name(_LONE_SURROGATE)
    with pytest.raises(UnicodeEncodeError):
        cdc.cdc_blocking_foreign_keys({"t": ["x"]}, {"t": [_LONE_SURROGATE]})


def test_the_cdc_gate_owns_a_constraint_by_its_stored_name() -> None:
    preserved = [name for name in _GOLDEN if name] + ["fk_short"]
    stored = [_GOLDEN[name][1] for name in _GOLDEN if name]
    target = {
        "app.t": stored + ["fk_short", "someone_elses", "A" * 62],  # a cut that is not ours
        "app.u": None,
    }
    result = cdc.cdc_blocking_foreign_keys(target, {"app.t": preserved})
    assert result.ours == sorted(("app.t", name) for name in {*stored, "fk_short"})
    assert result.foreign == [("app.t", "A" * 62), ("app.t", "someone_elses")]
    assert result.unknown_tables == ["app.u"]
    # The long form is ours as well (a catalog that reports names in full).
    full = cdc.cdc_blocking_foreign_keys({"app.t": ["A" * 64]}, {"app.t": ["A" * 64]})
    assert full.ours == [("app.t", "A" * 64)] and full.foreign == []


def test_the_mixed_case_note_groups_names_by_their_fold() -> None:
    def _table(name: str, column: str = "id") -> TableDef:
        return TableDef(
            name=name, columns=[ColumnDef(name=column, mysql_type="int")], primary_key=[column]
        )

    inventory = SourceInventory(tables=[
        _table("Shop.Orders", "Id"), _table("shop.orders"),
        _table("shop.\u00c9TAT"), _table("shop.\u00e9tat"),       # ÉTAT / état
        _table("shop.\u212aelvin"), _table("shop.kelvin"),         # Kelvin sign / kelvin
    ])
    (item,) = assessor.check_mixed_case_identifiers(inventory)
    # Folded as Aurora DSQL folds (ASCII A-Z only): only the ASCII pair collides; str.lower
    # would also join ÉTAT / état and the Kelvin sign / kelvin. (The Kelvin sign is no ASCII
    # capital either, so that name is not listed at all.)
    assert item.risk.endswith(
        " These names differ only by case, so they cannot both be reached unquoted: "
        "Shop.Orders / shop.orders."
    )
    assert item.risk.startswith(
        "1 database name(s), 2 table/view name(s), 1 column name(s) contain upper-case "
        "letters (for example Shop, Shop.Orders, shop.\u00c9TAT, Shop.Orders.Id)."
    )


def test_the_converters_limit_is_63() -> None:
    assert converter._PG_NAMEDATALEN == 63


def test_schema_apply_reads_stored_names_as_before() -> None:
    long = "Ab" + "é" * 30 + "cd"  # 64 bytes: stored as its first 63
    stored = "Ab" + "é" * 30 + "c"
    table = schema_applier._ParsedObject("TABLE", ["Shop", long])
    assert table.stored_parts == ("Shop", stored)
    index = schema_applier._ParsedObject("INDEX", [long], on_table=["Shop", long])
    assert index.stored_parts == ("Shop", stored)
    assert index.stored_on_table_name == f"Shop.{stored}"
    names = schema_applier.converted_names(
        [f"Shop.{long}"], {f"Shop.{long}": {long: "ix_new"}}, table_names=["Shop.T"]
    )
    assert names.views == frozenset({("Shop", stored)})
    assert names.relations == {("Shop", "T"): "table", ("Shop", stored): "view"}
    assert names.renamed_indexes == {("Shop", "ix_new"): long}
    assert names.legacy_indexes == {("Shop", stored): (f"Shop.{stored}", "ix_new")}
    # A REPLACE finds the copy an earlier version left under the FOLDED view name.
    view = schema_applier._ParsedObject("VIEW", ["Shop", "V_Orders"])
    found = schema_applier._legacy_folded_view(
        view, lambda parts: ("VIEW", None) if parts == ("shop", "v_orders") else None)
    assert found is not None and found.parts == ["shop", "v_orders"]


# ---------------------------------------------------------------------------
# Re-exports and structure
# ---------------------------------------------------------------------------


def test_the_introspector_re_exports_the_very_same_rules() -> None:
    from dsql_migrator.core import pg_names

    assert ti.pg_fold_unquoted is pg_names.pg_fold_unquoted
    assert ti.pg_stored_name is pg_names.pg_stored_name
    assert ti._PG_NAME_MAX_BYTES == pg_names._PG_NAME_MAX_BYTES == 63
    assert "pg_stored_name" in ti.__all__
    for module in (models, converter, query_converter, assessor, schema_applier):
        assert module.pg_fold_unquoted is pg_names.pg_fold_unquoted, module.__name__
    for module in (cdc, schema_applier):
        assert module.pg_stored_name is pg_names.pg_stored_name, module.__name__


def _tree(module: str) -> ast.Module:
    return ast.parse((_SRC / "core" / module).read_text(encoding="utf-8"))


def test_pg_names_is_standard_library_only() -> None:
    imports = [
        node for node in _tree("pg_names.py").body if isinstance(node, (ast.Import, ast.ImportFrom))
    ]
    assert [ast.unparse(node) for node in imports] == ["from __future__ import annotations"]


_COPY_MODULES = (
    "models.py", "converter.py", "query_converter.py", "assessor.py", "cdc.py",
    "schema_applier.py", "target_introspector.py",
    "sql_text.py",  # the converter's SQL text scan, split out of it (CONV-4)
)


@pytest.mark.parametrize("module", _COPY_MODULES)
def test_no_copy_of_the_rules_comes_back(module: str) -> None:
    text = (_SRC / "core" / module).read_text(encoding="utf-8")
    # The fold, as a translate table (however the call is wrapped) or as the per-character
    # join. cdc.py's _cdc_ascii_fold table is Debezium's rule, kept on purpose.
    flat = re.sub(r"\s+", "", text)
    allowed = 1 if module == "cdc.py" else 0
    assert flat.count('maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ"') <= allowed
    assert 'ch.lower() if "A" <= ch <= "Z"' not in text
    # The 63-byte cut.
    assert "[:63]" not in text and "[:_PG_" not in text
    tree = _tree(module)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            assert node.name not in ("pg_fold_unquoted", "pg_stored_name"), module
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            assert node.value.value != 63, (module, node.lineno)
        if (
            module == "schema_applier.py"  # its lazy imports from the introspector are gone
            and isinstance(node, ast.ImportFrom)
            and node.module == "dsql_migrator.core.target_introspector"
        ):
            names = {alias.name for alias in node.names}
            assert not names & {"pg_fold_unquoted", "pg_stored_name"}, node.lineno


def test_the_converters_limit_and_fold_are_the_shared_ones() -> None:
    (assign,) = [
        node for node in _tree("converter.py").body
        if isinstance(node, ast.Assign)
        and any(getattr(t, "id", "") == "_PG_NAMEDATALEN" for t in node.targets)
    ]
    assert ast.unparse(assign.value) == "_PG_NAME_MAX_BYTES"
    (fold,) = [
        node for node in _tree("converter.py").body
        if isinstance(node, ast.FunctionDef) and node.name == "fold_identifier"
    ]
    assert ast.unparse(fold.body[-1]) == "return pg_fold_unquoted(name)"
