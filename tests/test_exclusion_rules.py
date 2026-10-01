"""P3-C01: the exclusion rule language -- tokenizer, parser, name tokens, E1 syntax errors.

Pure core (``core.exclusion_rules``): no matching yet, nothing calls the parser. Grammar and
examples: the plan note, "Rule language" (GRAMMAR, ERRORS E1).
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.exclusion_rules import (
    ParsedRule,
    parse_rules,
    rule_part_token,
    split_rule_comment,
    type_name_token,
)


def _one(text):
    rules, errors = parse_rules(text)
    assert not errors, errors
    assert list(rules) == [1]
    return rules[1]


def _err(text):
    rules, errors = parse_rules(text)
    assert not rules, rules
    assert list(errors) == [1], errors
    (error,) = errors[1]
    assert error.code == "E1" and error.line == 1
    return error


# --- every grammar form -------------------------------------------------------------


def test_the_plan_examples_parse():
    examples = {
        "exclude table shop.audit_*": ("exclude", "table", ("shop", "audit_*")),
        "include table shop.audit_keep": ("include", "table", ("shop", "audit_keep")),
        "exclude shop.product_media.content": ("exclude", "column", ("shop", "product_media", "content")),
        "exclude shop.*.password_hash": ("exclude", "column", ("shop", "*", "password_hash")),
        "exclude *.*.ssn": ("exclude", "column", ("*", "*", "ssn")),
        "include shop.pages.body": ("include", "column", ("shop", "pages", "body")),
        'exclude "my schema"."order.items"."weird*col"': (
            "exclude", "column", ("my schema", "order.items", "weird*col")),
    }
    for line, (action, kind, parts) in examples.items():
        rule = _one(line)
        assert (rule.action, rule.kind, tuple(p.text for p in rule.parts)) == (action, kind, parts), line
    typed = _one("exclude type:longtext,longblob")
    assert (typed.kind, [t.text for t in typed.type_names], typed.parts) == ("type", ["longtext", "longblob"], ())


def test_wildcards_only_in_bare_parts_and_quoting_is_literal():
    rule = _one('exclude "my schema"."order.items"."weird*col"')
    assert all(p.quoted for p in rule.parts) and not any(p.wildcard for p in rule.parts)
    bare = _one("exclude shop.*.password_hash")
    assert [p.wildcard for p in bare.parts] == [False, True, False]


def test_keywords_are_case_insensitive_and_identifiers_keep_their_case():
    assert _one("EXCLUDE Table Shop.Orders").kind == "table"
    rule = _one("Include TYPE:LongText")
    assert (rule.action, rule.kind, rule.type_names[0].text) == ("include", "type", "LongText")
    assert [p.text for p in _one("exclude Shop.Orders.Notes").parts] == ["Shop", "Orders", "Notes"]


def test_quoted_names_double_quote_escape_and_hash_inside_quotes():
    rule = _one('exclude "a""b"."t#1".c  # a real comment')
    assert [p.text for p in rule.parts] == ['a"b', "t#1", "c"]
    assert rule.comment == " a real comment"


def test_blank_and_comment_only_lines_produce_nothing():
    rules, errors = parse_rules("\n   \n# just a note\n  # indented note\nexclude s.t.c\n")
    assert list(rules) == [5] and not errors


def test_split_rule_comment_respects_quotes():
    assert split_rule_comment('exclude "a#b".t.c # note') == ('exclude "a#b".t.c ', " note")
    assert split_rule_comment("exclude s.t.c") == ("exclude s.t.c", "")
    assert split_rule_comment('exclude "unbalanced # x') == ('exclude "unbalanced # x', "")


def test_spans_point_at_the_source_text_of_every_name():
    line = 'exclude  "my schema".t_1."weird*col"'
    rule = _one(line)
    assert [line[slice(*p.span)] for p in rule.parts] == ['"my schema"', "t_1", '"weird*col"']
    line = 'include type:longtext,"character varying",text[]'
    rule = _one(line)
    assert [line[slice(*t.span)] for t in rule.type_names] == ["longtext", '"character varying"', "text[]"]


def test_line_numbers_are_one_based_across_lines():
    rules, errors = parse_rules("exclude a.b.c\n\nbogus a.b.c\ninclude table a.b\r\n")
    assert sorted(rules) == [1, 4] and sorted(errors) == [3]


# --- E1 syntax errors: exact messages and one-click fixes ---------------------------


def test_e1_unknown_first_word_suggests_the_keyword():
    error = _err("exlcude s.t.c")
    assert error.message == 'Unknown first word "exlcude": a rule starts with exclude or include.'
    assert [(s.label, s.line_text) for s in error.suggestions] == [('Use "exclude"', "exclude s.t.c")]
    assert _err("frobnicate s.t.c").suggestions == ()


def test_e1_missing_subject():
    error = _err("exclude")
    assert error.message == (
        'Missing what to exclude: write "exclude schema.table.column", '
        '"exclude table schema.table" or "exclude type:<type>".'
    )
    assert _err("include   ").message.startswith("Missing what to include:")


def test_e1_unbalanced_quote():
    error = _err('exclude "my schema.t.c')
    assert error.message == 'Unbalanced quote: close the quoted name with ".'
    assert error.suggestions == ()


def test_e1_two_parts_without_the_table_keyword():
    error = _err("exclude shop.orders")
    assert error.message == (
        'Two parts: write "exclude table shop.orders" for the table, or '
        "shop.orders.<column> for a column."
    )
    assert [(s.label, s.line_text) for s in error.suggestions] == [
        ('Use "exclude table shop.orders"', "exclude table shop.orders")]


def test_e1_table_with_three_parts_or_one():
    error = _err("exclude table shop.orders.notes")
    assert error.message == '"table" takes schema.table (two parts); shop.orders.notes names a column.'
    assert [(s.label, s.line_text) for s in error.suggestions] == [
        ('Use "exclude shop.orders.notes"', "exclude shop.orders.notes")]
    one = _err("exclude table orders")
    assert one.message == '"table" takes schema.table (two parts).' and one.suggestions == ()


def test_e1_wrong_part_counts_and_empty_parts():
    assert _err("exclude notes").message == "A column rule names schema.table.column (three parts)."
    assert _err("exclude a.b.c.d").message == (
        '4 parts: a name has at most three parts (schema.table.column); quote a name that '
        'contains ".".'
    )
    assert _err("exclude shop..c").message == 'An empty name part: remove the extra ".".'
    assert _err("exclude shop.t.").message == 'An empty name part: remove the extra ".".'


def test_e1_type_names():
    dotted = _err("exclude type:app.status")
    assert dotted.message == (
        '"app.status" contains "." -- quote a schema-qualified type: type:"app.status".')
    assert [(s.label, s.line_text) for s in dotted.suggestions] == [
        ('Use type:"app.status"', 'exclude type:"app.status"')]
    assert dotted.name_index == 0
    second = _err('include type:text,app.status')
    assert second.name_index == 1
    assert second.suggestions[0].line_text == 'include type:text,"app.status"'
    assert _err("exclude type:").message == "Empty type list: write type:<type>[,<type>...]."
    assert _err("exclude type:text,").message == "A trailing comma: remove it or add a type."
    assert _err("exclude type:text,,bytea").message == "An empty type name: remove the extra \",\"."


def test_e1_wildcard_and_spaces_in_a_type_list():
    star = _err("exclude type:long*")
    assert star.message == (
        'No wildcard in type names: quote it to match a type literally named "long*".')
    assert star.suggestions[0].line_text == 'exclude type:"long*"'
    spaced = _err("exclude type: text , bytea")
    assert spaced.message == "No spaces in a type list."
    assert [(s.label, s.line_text) for s in spaced.suggestions] == [
        ("Remove the spaces", "exclude type:text,bytea")]


def test_e1_trailing_text_after_the_rule():
    error = _err("exclude s.t.c extra words")
    assert error.message == 'Unexpected text after the rule: "extra words". Use # for a comment.'
    assert [(s.label, s.line_text) for s in error.suggestions] == [
        ("Make it a comment", "exclude s.t.c # extra words")]
    assert _err('exclude "a"x.t.c').message == 'Unexpected character after a quoted name: "x".'
    kept = _err("exclude s.t.c extra # why")
    assert kept.suggestions[0].line_text == "exclude s.t.c # extra # why"
    assert _one(kept.suggestions[0].line_text).comment == " extra # why"
    spaced_name = _err('exclude table "a  b".t.c')
    assert spaced_name.suggestions[0].line_text == 'exclude "a  b".t.c'


def test_errors_carry_the_line_and_never_a_rule():
    rules, errors = parse_rules("exclude s.t.c\nexclude shop.orders\ninclude type:")
    assert list(rules) == [1] and sorted(errors) == [2, 3]
    assert all(e.line == n for n, es in errors.items() for e in es)


# --- tokens: what the popup shows can be pasted back -------------------------------


def test_type_name_token_examples():
    assert [type_name_token(k) for k in (
        "character varying", "app.status", "MyType", "text[]", "my(x)", 'a"b', "a,b", "long*")] == [
        '"character varying"', '"app.status"', "MyType", "text[]", "my(x)", '"a""b"', '"a,b"', '"long*"']


@pytest.mark.parametrize("schema,table,column", [
    ("shop", "orders", "notes"),
    ('we"ird', "or.ders", "n#o,t es"),
    ("café", "주문", "*"),
    ("table", "type:x", " lead"),
    ("", "t", "c"),
])
def test_rule_part_token_round_trips_through_the_parser(schema, table, column):
    line = "exclude " + ".".join(rule_part_token(n) for n in (schema, table, column))
    rule = _one(line)
    assert tuple(p.text for p in rule.parts) == (schema, table, column)
    assert not any(p.wildcard for p in rule.parts)


def test_parsed_rule_keeps_its_rule_text():
    rule = _one("exclude s.t.c   # why")
    assert isinstance(rule, ParsedRule) and rule.rule_text == "exclude s.t.c   " and rule.line == 1


def test_rule_part_token_round_trip_fuzz():
    """Deterministic fuzz over the characters the grammar treats specially."""
    import random

    rnd = random.Random(1562)
    alphabet = ['a', 'Z', '_', '*', '.', '"', '#', ',', ' ', '\t', ':', 'é', '주', '$', '(', ')', '[', ']']
    for _ in range(3000):
        names = ["".join(rnd.choice(alphabet) for _ in range(rnd.randint(0, 6))) for _ in range(3)]
        line = rnd.choice(["exclude ", "include ", "EXCLUDE "]) + ".".join(rule_part_token(n) for n in names)
        rules, errors = parse_rules(line)
        assert not errors, (line, errors)
        assert tuple(p.text for p in rules[1].parts) == tuple(names), line
        assert not any(p.wildcard for p in rules[1].parts)
    for _ in range(2000):
        key = "".join(rnd.choice(alphabet) for _ in range(rnd.randint(1, 8)))
        rules, errors = parse_rules("exclude type:" + type_name_token(key))
        assert not errors, (key, errors)
        assert rules[1].type_names[0].text == key


# --- P3-C02: the type normalizer, typed-name resolution, suggestion hints -----------

from dsql_migrator.core.exclusion_rules import (  # noqa: E402
    hint_for,
    reduce_typed_name,
    resolve_type_name,
    rule_type_name,
)
from dsql_migrator.core.models import ColumnDef, SourceType  # noqa: E402

MY, PG = SourceType.MYSQL, SourceType.POSTGRES


def _col(t):
    return ColumnDef(name="c", mysql_type=t)


@pytest.mark.parametrize("declared,key", [
    ("timestamp(3) without time zone", "timestamp without time zone"),
    ("timestamp without time zone", "timestamp without time zone"),
    ("numeric(5,-2)", "numeric"),
    ("numeric(10,2)", "numeric"),
    ("character varying(255)", "character varying"),
    ("bit varying(8)[]", "bit varying[]"),
    ("text[]", "text[]"),
    ('"MyType"', "MyType"),
    ('"MYTYPE"', "MYTYPE"),
    ('app."Mood"[]', "app.Mood[]"),
    ("app.status", "app.status"),
    ('"my(x)"', "my(x)"),
    ('"char"', "char"),
    ('"a""b"', 'a"b'),
    ("  double   precision ", "double precision"),
    ("bpchar", "bpchar"),
    ("character(3)", "character"),
])
def test_rule_type_name_postgres(declared, key):
    assert rule_type_name(_col(declared), PG) == key


@pytest.mark.parametrize("declared,key", [
    ("int unsigned", "int"), ("decimal(10,2) unsigned", "decimal"), ("LONGTEXT", "longtext"),
    ("varchar(255)", "varchar"), ("enum('a','b')", "enum"),
])
def test_rule_type_name_mysql(declared, key):
    assert rule_type_name(_col(declared), MY) == key


def test_resolve_type_name_three_steps():
    my_keys = {"longtext", "text", "int"}
    assert resolve_type_name("LONGTEXT", my_keys, MY) == "longtext"
    assert resolve_type_name("TEXT", my_keys, MY) == "text"
    assert resolve_type_name("int(11) unsigned", my_keys, MY) == "int"
    pg_keys = {"MyType", "MYTYPE", "my(x)", "character varying", "text", "timestamp without time zone"}
    assert resolve_type_name("MyType", pg_keys, PG) == "MyType"
    assert resolve_type_name("MYTYPE", pg_keys, PG) == "MYTYPE"
    assert resolve_type_name("mytype", pg_keys, PG) is None  # case twins never merge
    assert resolve_type_name("my(x)", pg_keys, PG) == "my(x)"  # step 1 before the typmod strip
    assert resolve_type_name("TEXT", pg_keys, PG) == "text"  # step 3: lower(reduced)
    assert resolve_type_name("timestamp(6) without time zone", pg_keys, PG) == "timestamp without time zone"
    assert resolve_type_name("varchar", pg_keys, PG) is None  # no alias table
    assert reduce_typed_name("Character Varying(20)", PG) == "Character Varying"


def test_hint_for_suggests_only_present_targets():
    assert hint_for("character varying", MY, {"varchar"}) == "varchar"  # the space is not cut
    assert hint_for("NVARCHAR(40)", MY, {"varchar"}) == "varchar"
    assert hint_for("integer", MY, {"bigint"}) is None  # target absent: withheld
    assert hint_for("varchar", PG, {"character varying"}) == "character varying"
    assert hint_for("int4[]", PG, {"integer[]"}) == "integer[]"
    assert hint_for("longtext", PG, {"text"}) == "text"
    assert hint_for("bpchar", PG, {"character"}) == "character"
    assert hint_for("bool", MY, {"tinyint"}) is None  # a hint never broadens a match


def test_the_matcher_never_reads_the_hint_map(monkeypatch):
    import dsql_migrator.core.exclusion_rules as er

    monkeypatch.setitem(er._TYPE_SUGGESTION_HINTS, PG, {"varchar": "text"})
    assert resolve_type_name("varchar", {"text"}, PG) is None


def test_every_key_typed_back_from_its_token_resolves_to_itself():
    keys = {"text", "character varying", "MyType", "MYTYPE", "app.status", "app.Mood[]", "my(x)",
            "char", 'a"b', "a,b", "timestamp without time zone", "text[]"}
    for key in keys:
        rules, errors = parse_rules("exclude type:" + type_name_token(key))
        assert not errors, (key, errors)
        assert resolve_type_name(rules[1].type_names[0].text, keys, PG) == key, key
