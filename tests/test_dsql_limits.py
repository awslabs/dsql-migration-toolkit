"""B4: each Aurora DSQL type limit that several modules enforce is defined once.

The numeric limit was a literal 38 in three modules, and when the service raised it to 1000
every copy had to be found by hand; the MySQL spatial set and the 8-column key limit were each
kept twice (the GEOMCOLLECTION fix had to edit both sets). The modules keep their own private
names, which tests and callers read, as aliases of the one definition -- so these tests pin
the alias in the source: an equal literal would pass a value check and drift again.
"""

from __future__ import annotations

import ast
import pathlib

from dsql_migrator.core import (
    assessor,
    converter,
    converter_postgres,
    dsql_limits,
    prerequisites,
)


def _module_level_value(module, name: str) -> ast.expr:
    tree = ast.parse(pathlib.Path(module.__file__).read_text())
    values = [
        node.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == name for t in node.targets)
    ]
    assert len(values) == 1, (module.__name__, name, len(values))
    return values[0]


def test_the_limits_keep_their_documented_values() -> None:
    assert dsql_limits.DSQL_NUMERIC_MAX_PRECISION == 1000
    assert dsql_limits.DSQL_NUMERIC_MAX_SCALE == 1000
    assert dsql_limits.DSQL_MAX_VARCHAR_LENGTH == 65535
    assert dsql_limits.DSQL_MAX_CHAR_LENGTH == 4096


def test_each_module_aliases_the_one_definition_instead_of_restating_it() -> None:
    aliases = {
        (assessor, "_MAX_NUMERIC_PRECISION"): "DSQL_NUMERIC_MAX_PRECISION",
        (converter, "_DSQL_NUMERIC_MAX_PRECISION"): "DSQL_NUMERIC_MAX_PRECISION",
        (converter, "_DSQL_NUMERIC_MAX_SCALE"): "DSQL_NUMERIC_MAX_SCALE",
        (converter_postgres, "_DSQL_NUMERIC_MAX_PRECISION"): "DSQL_NUMERIC_MAX_PRECISION",
        (converter_postgres, "_DSQL_NUMERIC_MAX_SCALE"): "DSQL_NUMERIC_MAX_SCALE",
        (converter_postgres, "_DSQL_MAX_VARCHAR_LENGTH"): "DSQL_MAX_VARCHAR_LENGTH",
        (converter_postgres, "_DSQL_MAX_CHAR_LENGTH"): "DSQL_MAX_CHAR_LENGTH",
        # The converter reads these two from the assessor, which defines them.
        (converter, "_SPATIAL_TYPES"): "_SPATIAL_TYPE_BASES",
        (converter, "_DSQL_MAX_PK_COLUMNS"): "_MAX_KEY_COLUMNS",
    }
    for (module, name), source in aliases.items():
        value = _module_level_value(module, name)
        assert isinstance(value, ast.Name) and value.id == source, (
            module.__name__, name, ast.dump(value)
        )
        origin = dsql_limits if hasattr(dsql_limits, source) else assessor
        assert getattr(module, name) is getattr(origin, source), (module.__name__, name)


def test_the_value_ceiling_table_reads_the_character_limits() -> None:
    table = _module_level_value(prerequisites, "_DSQL_VALUE_LIMIT_BY_BASE_TYPE")
    assert isinstance(table, ast.Dict)
    entries = {ast.literal_eval(key): value for key, value in zip(table.keys, table.values)}
    expected = {
        "character varying": "DSQL_MAX_VARCHAR_LENGTH",
        "varchar": "DSQL_MAX_VARCHAR_LENGTH",
        "character": "DSQL_MAX_CHAR_LENGTH",
        "char": "DSQL_MAX_CHAR_LENGTH",
        "bpchar": "DSQL_MAX_CHAR_LENGTH",
    }
    for base, source in expected.items():
        value = entries[base]
        assert isinstance(value, ast.Name) and value.id == source, (base, ast.dump(value))
        assert prerequisites.dsql_value_limit_bytes(base) == getattr(dsql_limits, source)
