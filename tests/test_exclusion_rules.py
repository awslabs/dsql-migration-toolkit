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
    ('"integer"', "integer"),  # a public."integer" domain shares the built-in's key
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


# --- P3-C03: the resolver -- matching, include-wins precedence, table provenance ----

from dsql_migrator.core.exclusion_rules import (  # noqa: E402
    LINE_NO_EFFECT,
    LINE_ONLY_UNSELECTED,
    resolve_exclusion_rules,
)
from dsql_migrator.core.models import TableDef  # noqa: E402


def _t(name, *cols, types=None):
    types = types or {}
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="bigint")]
        + [ColumnDef(name=c, mysql_type=types.get(c, "text")) for c in cols],
        primary_key=["id"],
    )


TABLES = [
    _t("shop.users", "email", "password_hash", "notes",
       types={"email": "varchar(255)", "password_hash": "varchar(64)"}),
    _t("shop.orders", "notes", "memo", types={"memo": "longtext"}),
    _t("shop.product_media", "content", "caption",
       types={"content": "longblob", "caption": "varchar(100)"}),
    _t("shop.audit_2023", "payload", types={"payload": "longtext"}),
    _t("shop.audit_2024", "payload", types={"payload": "longtext"}),
    _t("shop.audit_keep", "payload", types={"payload": "longtext"}),
    _t("shop.pages", "body", types={"body": "longtext"}),
    _t("shop.a_b", "x"),
    _t("shop.aXb", "x"),
    _t("shop.misc", "pct%", "pctX", "q?", "qX"),
    _t("my schema.order.items", "weird*col", "weirdXcol"),
    _t("crm.contacts", "ssn"),
]
ALL = [t.name for t in TABLES]


def _res(text, *, selection=ALL, prev=(), lock=None, reticks=()):
    return resolve_exclusion_rules(
        text, migratable=TABLES, source_type=MY, effective_selection=selection,
        rule_excluded_prev=prev, selection_lock_reason=lock, retick_overrides=reticks,
    )


def _cols(res):
    return {(t, c) for t, cs in res.columns.items() for c in cs}


def test_star_matches_within_one_part_and_other_characters_are_literal():
    assert _cols(_res("exclude shop.*.notes")) == {("shop.users", "notes"), ("shop.orders", "notes")}
    assert _cols(_res("exclude shop.audit_*.payload")) == {
        ("shop.audit_2023", "payload"), ("shop.audit_2024", "payload"), ("shop.audit_keep", "payload")}
    assert _cols(_res("exclude shop.a_b.x")) == {("shop.a_b", "x")}  # '_' is literal
    assert _cols(_res("exclude shop.misc.pct%")) == {("shop.misc", "pct%")}
    assert _cols(_res("exclude shop.misc.q?")) == {("shop.misc", "q?")}
    assert _cols(_res("exclude *.*.ssn")) == {("crm.contacts", "ssn")}
    assert _cols(_res("exclude shop.users.email*")) == {("shop.users", "email")}  # '*' matches none
    # the table splits at its FIRST '.', and '*' never reaches across a part
    assert _cols(_res("exclude my*.order*.weird*col")) == {
        ("my schema.order.items", "weird*col"), ("my schema.order.items", "weirdXcol")}
    assert _cols(_res('exclude "my schema"."order.items"."weird*col"')) == {
        ("my schema.order.items", "weird*col")}
    nothing = _res("exclude *.*.users")
    assert not nothing.columns and [e.code for e in nothing.errors[1]] == ["E2"]


def test_identifiers_are_case_sensitive():
    res = _res("exclude shop.users.Email\nexclude Shop.users.email")
    assert not res.columns and sorted(res.errors) == [1, 2]
    assert all(e.code == "E2" for es in res.errors.values() for e in es)
    assert not res.can_save


def test_type_rules_resolve_through_the_normalizer():
    res = _res("exclude type:LONGTEXT,longblob")
    assert ("shop.product_media", "content") in _cols(res) and ("shop.pages", "body") in _cols(res)
    assert all(c in ("payload", "memo", "body", "content") for _t2, cs in res.columns.items() for c in cs)
    two = _res("exclude type:text,nosuch,alsonot")
    assert [(e.code, e.name_index) for e in two.errors[1]] == [("E2", 1), ("E2", 2)]
    assert ("shop.users", "notes") in _cols(two)  # the resolved name still matches


def _key(res):
    return (res.columns, res.tables_to_unselect, res.tables_to_reselect, res.selected_after,
            res.rule_excluded_after)


def test_order_independent_and_idempotent():
    import random

    lines = [
        "exclude table shop.audit_*", "include table shop.audit_keep", "exclude shop.*.notes",
        "include shop.orders.notes", "exclude type:longtext", "include shop.pages.body",
        "exclude *.*.ssn", "include type:varchar", "exclude shop.users.password_hash",
    ]
    base = _res("\n".join(lines))
    rnd = random.Random(7)
    for _ in range(40):
        shuffled = lines[:]
        rnd.shuffle(shuffled)
        res = _res("\n".join(shuffled))
        assert _key(res) == _key(base)
        by_text = {(shuffled[c.include_line - 1], shuffled[c.exclude_line - 1], c.items)
                   for c in res.cancellations}
        assert by_text == {(lines[c.include_line - 1], lines[c.exclude_line - 1], c.items)
                           for c in base.cancellations}
    assert _key(_res("\n".join(lines))) == _key(base)
    # saving the result and resolving the same rules again changes nothing
    again = _res("\n".join(lines), selection=base.selected_after, prev=base.rule_excluded_after)
    assert again.selected_after is None and not again.tables_to_unselect
    assert again.columns == base.columns and again.rule_excluded_after == base.rule_excluded_after


def test_include_never_adds_a_table_and_a_table_include_never_cancels_a_column():
    unticked = [n for n in ALL if n != "shop.pages"]
    res = _res("include table shop.pages", selection=unticked)
    assert res.selected_after is None and not res.tables_to_reselect
    assert res.line_status[1].codes == (LINE_NO_EFFECT,)
    res = _res("exclude shop.pages.body\ninclude table shop.pages")
    assert _cols(res) == {("shop.pages", "body")}
    assert res.line_status[2].codes == (LINE_NO_EFFECT,)


def test_a_type_include_cancels_a_column_exclude_and_names_both_lines():
    text = "\n".join([
        "exclude shop.*.notes", "exclude shop.users.password_hash", "# a note", "",
        "include type:varchar",
    ])
    res = _res(text)
    assert ("shop.users", "password_hash") not in _cols(res)
    (cancel,) = res.cancellations
    assert (cancel.include_line, cancel.exclude_line, cancel.kind, cancel.items) == (
        5, 2, "column", (("shop.users", "password_hash"),))
    assert res.kept_by[("shop.users", "password_hash")] == (5,)
    assert res.excluded_by[("shop.users", "notes")] == (1,)
    assert res.line_status[2].kept_by == ((5, 1),) and res.line_status[2].matched == 0
    assert res.line_status[5].keeps == 1 and res.line_status[5].codes == ()


def test_table_provenance():
    first = _res("exclude table shop.audit_*\ninclude table shop.audit_keep")
    assert first.tables_to_unselect == ("shop.audit_2023", "shop.audit_2024")
    assert first.selected_after == tuple(n for n in ALL if n not in first.tables_to_unselect)
    assert first.rule_excluded_after == {"shop.audit_2023", "shop.audit_2024"}
    assert first.line_status[1].unselects == ("shop.audit_2023", "shop.audit_2024")
    assert first.line_status[1].matched == 2 and first.line_status[1].kept_by == ((2, 1),)
    saved, prov = first.selected_after, first.rule_excluded_after
    # the operator then unticks shop.pages in the tree; deleting the rule re-selects only
    # what the rules removed
    unticked = tuple(n for n in saved if n != "shop.pages")
    gone = _res("", selection=unticked, prev=prov)
    assert gone.tables_to_reselect == ("shop.audit_2023", "shop.audit_2024")
    assert "shop.pages" not in gone.selected_after and gone.rule_excluded_after == frozenset()
    # an include re-selects
    inc = _res("exclude table shop.audit_*\ninclude table shop.audit_2024", selection=saved, prev=prov)
    assert inc.tables_to_reselect == ("shop.audit_2024",) and inc.line_status[2].reselects == (
        "shop.audit_2024",)
    assert inc.rule_excluded_after == {"shop.audit_2023", "shop.audit_keep"}
    # a provenance entry for a table no longer migratable is kept, never invented
    stale = _res("exclude table shop.audit_*\ninclude table shop.audit_keep", selection=saved,
                 prev=prov | {"old.gone"})
    assert stale.selected_after is None and stale.rule_excluded_after == prov | {"old.gone"}


def test_retick_override_note():
    reticked = [n for n in ALL if n != "shop.audit_2023"]
    res = _res("\n\n\n\n\nexclude table shop.audit_*\ninclude table shop.audit_keep",
               selection=reticked, prev={"shop.audit_2023"}, reticks={"shop.audit_2024"})
    assert res.tables_to_unselect == ("shop.audit_2024",)
    assert ("L6 matches shop.audit_2024, ticked in the tree after your last Save: Save will "
            "unselect it again") in res.line_status[6].notes


def test_e3_only_when_a_locked_selection_would_change():
    lock = "Locked — a Full Load has already run for this table set. Use 'Start over' to migrate a different set of tables."
    res = _res("exclude shop.users.notes\nexclude table shop.audit_*", lock=lock)
    assert list(res.errors) == [2] and res.errors[2][0].code == "E3"
    assert "Start over" in res.errors[2][0].message and "shop.audit_2023" in res.errors[2][0].message
    assert not res.can_save
    applied = [n for n in ALL if n not in ("shop.audit_2023", "shop.audit_2024", "shop.audit_keep")]
    prov = {"shop.audit_2023", "shop.audit_2024", "shop.audit_keep"}
    same = _res("exclude table shop.audit_*\nexclude shop.users.notes", selection=applied,
                prev=prov, lock=lock)
    assert same.can_save and same.selected_after is None
    assert "No effect while the table set is locked." in same.line_status[1].notes
    reinc = _res("exclude table shop.audit_*\ninclude table shop.audit_keep", selection=applied,
                 prev=prov, lock=lock)
    assert [e.code for e in reinc.errors[2]] == ["E3"] and 1 not in reinc.errors
    removed = _res("exclude shop.users.notes", selection=applied, prev=prov, lock=lock)
    assert not removed.errors and [e.code for e in removed.global_errors] == ["E3"]
    assert "shop.audit_keep" in removed.global_errors[0].message and not removed.can_save


def test_reselect_lock_raises_per_table_e3():
    # G16 (FAILS WITHOUT THE FIX: no such parameter): with NO selection lock (Full load +
    # CDC before CDC starts), only the re-selections the per-table lock refuses are E3.
    applied = [n for n in ALL if n not in ("shop.audit_2023", "shop.audit_2024", "shop.audit_keep")]
    prov = {"shop.audit_2023", "shop.audit_2024", "shop.audit_keep"}
    asked: list = []

    def lock(names):
        asked.append(list(names))
        # shop.users too: the unselect below must never be judged by this lock.
        blocked = [n for n in names if n in ("shop.audit_keep", "shop.users")]
        return (blocked, "Not in the snapshot.") if blocked else None

    def res(text, reselect_lock=lock):
        return resolve_exclusion_rules(
            text, migratable=TABLES, source_type=MY, effective_selection=applied,
            rule_excluded_prev=prov, reselect_lock=reselect_lock)

    # A deleted rule re-selects all three: only the refused one is a global E3.
    deleted = res("exclude shop.users.notes")
    assert not deleted.errors and not deleted.can_save
    (e3,) = deleted.global_errors
    assert e3.code == "E3" and e3.message == (
        "Save would re-select shop.audit_keep: no table rule unselects it any more. "
        "Not in the snapshot.")
    # An include line re-selecting it is a line E3 (the global check sees it explained).
    inc = res("exclude table shop.audit_*\ninclude table shop.audit_keep")
    assert [e.code for e in inc.errors[2]] == ["E3"] and not inc.global_errors
    assert inc.errors[2][0].message == (
        "This rule would select shop.audit_keep again. Not in the snapshot.")
    # Re-selecting an allowed table, or unselecting one, is never refused.
    ok = res("exclude table shop.audit_*\ninclude table shop.audit_2024")
    assert ok.can_save and ok.tables_to_reselect == ("shop.audit_2024",)
    unsel = res("exclude table shop.audit_*\nexclude table shop.users")
    assert unsel.can_save and unsel.tables_to_unselect == ("shop.users",)
    # The binary lock wins over it (the lock is not even asked), and None is today's path.
    asked.clear()
    locked = resolve_exclusion_rules(
        "exclude shop.users.notes", migratable=TABLES, source_type=MY,
        effective_selection=applied, rule_excluded_prev=prov,
        selection_lock_reason="Locked.", reselect_lock=lock)
    assert asked == [] and "Locked." in locked.global_errors[0].message
    for text in ("exclude shop.users.notes",
                 "exclude table shop.audit_*\ninclude table shop.audit_keep"):
        plain = res(text, reselect_lock=None)
        today = resolve_exclusion_rules(
            text, migratable=TABLES, source_type=MY, effective_selection=applied,
            rule_excluded_prev=prov)
        assert plain == today and plain.can_save


def test_an_empty_result_is_refused():
    res = _res("exclude table *.*")
    assert [e.code for e in res.global_errors] == ["E5"] and not res.can_save
    assert res.selected_after == ()


def test_column_only_rules_leave_the_selection_untouched():
    res = _res("exclude shop.*.notes\nexclude type:longblob", selection=ALL[:5])
    assert res.selected_after is None and res.rule_excluded_after == frozenset()
    assert not res.tables_to_unselect and not res.tables_to_reselect
    assert set(res.columns) == {"shop.users", "shop.orders", "shop.product_media"}


def test_per_line_status():
    unticked = [n for n in ALL if n != "shop.pages"]
    res = _res("\n".join([
        "exclude shop.*.notes",               # L1 matches 2
        "include shop.orders.notes",          # L2 keeps 1
        "exclude shop.pages.body",            # L3 only an unselected table: a warning, not E2
        "include shop.users.email",           # L4 cancels nothing
        "exclude table shop.audit_2023",      # L5
        "exclude shop.audit_2023.payload",    # L6 inert: L5 unselects the table in this Save
        "exclude shop.nosuch.x",              # L7 E2
    ]), selection=unticked)
    st = res.line_status
    assert (st[1].matched, st[1].kept_by, st[1].codes) == (1, ((2, 1),), ())
    assert (st[2].keeps, st[2].codes) == (1, ())
    assert (st[3].matched, st[3].codes) == (0, (LINE_ONLY_UNSELECTED,))
    assert 3 not in res.errors
    assert st[4].codes == (LINE_NO_EFFECT,)
    assert st[5].unselects == ("shop.audit_2023",)
    assert st[6].codes == (LINE_ONLY_UNSELECTED,)
    assert "Inert on shop.audit_2023: a table rule unselects it in this Save." in st[6].notes
    assert [e.code for e in res.errors[7]] == ["E2"] and 7 not in st
    assert sorted(st) == [1, 2, 3, 4, 5, 6]


# --- P3-C04: E2 "did you mean", hint notes, per-type counts --------------------------


def _pg(*types_counts, name="public.t"):
    cols, n = [], 0
    for declared, count in types_counts:
        for _ in range(count):
            n += 1
            cols.append(ColumnDef(name=f"c{n}", mysql_type=declared))
    return TableDef(name=name, columns=[ColumnDef(name="id", mysql_type="bigint")] + cols,
                    primary_key=["id"])


def _resolve(text, tables, source=PG):
    return resolve_exclusion_rules(
        text, migratable=tables, source_type=source, effective_selection=[t.name for t in tables])


PG_TYPES = [_pg(("character varying(20)", 1), ("character varying(20)[]", 1), ("integer", 1),
                ("timestamp(6) without time zone", 1), ("timestamp with time zone", 1),
                ("boolean", 1), ("character(3)", 1), ("text", 1))]


@pytest.mark.parametrize("typed,token", [
    ("varchar", '"character varying"'), ("int", "integer"),
    ("timestamp", '"timestamp without time zone"'), ("timestamptz", '"timestamp with time zone"'),
    ("bool", "boolean"), ("char", "character"), ("varchar[]", '"character varying[]"'),
])
def test_pg_hint_suggestions_rewrite_to_a_resolving_line(typed, token):
    res = _resolve(f"exclude type:{typed}", PG_TYPES)
    (error,) = res.errors[1]
    assert error.code == "E2" and error.name_index == 0 and not res.can_save
    (fix,) = error.suggestions
    assert fix.line_text == f"exclude type:{token}" and fix.label == f"Use type:{token}"
    fixed = _resolve(fix.line_text, PG_TYPES)
    assert fixed.can_save and fixed.columns


def test_type_names_are_judged_one_at_a_time():
    res = _resolve("exclude type:text,varchar", PG_TYPES)
    (error,) = res.errors[1]
    assert error.name_index == 1
    assert error.suggestions[0].line_text == 'exclude type:text,"character varying"'
    two = _resolve("exclude type:longtext,varchar  # keep", PG_TYPES)
    first, second = two.errors[1]
    assert (first.name_index, second.name_index) == (0, 1)
    assert first.suggestions[0].line_text == "exclude type:text,varchar  # keep"
    assert second.suggestions[0].line_text == 'exclude type:longtext,"character varying"  # keep'


def test_a_withheld_hint_lists_the_types_here():
    tables = [_pg(("text", 10), ("character varying(5)", 9), ("bigint", 7), ("boolean", 7),
                  ("numeric(4,2)", 6), ("date", 5), ("bytea", 4), ("jsonb", 3), ("uuid", 2),
                  ("real", 1))]
    (error,) = _resolve("exclude type:int", tables).errors[1]
    assert error.suggestions == ()
    assert error.message == (
        'No column of type int in any migratable table. Types here: text (10), '
        '"character varying" (9), bigint (8), boolean (7), numeric (6), date (5), bytea (4), '
        "jsonb (3) and 2 more."
    )


def test_the_internal_char_type_note():
    tables = [_pg(('"char"', 2), ("character(5)", 3))]
    res = _resolve("exclude type:char", tables)
    assert res.can_save and sum(len(c) for c in res.columns.values()) == 2
    assert res.line_status[1].notes == (
        "type:char matched char (2 columns). character is a different type here (3 columns): "
        "use type:character for those.",)
    assert res.line_status[1].type_counts == (("char", 2, 1),)
    assert _resolve("exclude type:char,character", tables).line_status[1].notes == ()


def test_bpchar_is_the_same_type_as_character():
    tables = [_pg(("bpchar", 2), ("character(5)", 3))]
    bp = _resolve("exclude type:bpchar", tables)
    assert bp.line_status[1].notes == (
        "type:bpchar matched bpchar (2 columns: character declared without a length). The 3 "
        "character columns are the same type declared with a length: add type:character to "
        "cover them.",)
    ch = _resolve("exclude type:character", tables)
    assert ch.line_status[1].notes == (
        "type:character matched character (3 columns). 2 columns are the same type declared "
        "without a length, printed bpchar: add type:bpchar to cover them.",)
    (error,) = _resolve("exclude type:bpchar", [_pg(("character(5)", 3))]).errors[1]
    assert [s.line_text for s in error.suggestions] == ["exclude type:character"]


def test_mysql_type_suggestions_and_direct_matches():
    tables = [_pg(("int(11)", 1), ("varchar(40)", 1), ("longtext", 1), ("double", 1),
                  ("tinyint(1)", 1), name="shop.t")]
    (error,) = _resolve("exclude type:integer", tables, MY).errors[1]
    assert [s.line_text for s in error.suggestions] == ["exclude type:int"]
    (error,) = _resolve('exclude type:"character varying"', tables, MY).errors[1]
    assert [s.line_text for s in error.suggestions] == ["exclude type:varchar"]
    assert _resolve('exclude type:LONGTEXT,"double precision"', tables, MY).can_save
    (error,) = _resolve("exclude type:bool", tables, MY).errors[1]
    assert error.suggestions == () and "Types here: " in error.message


def test_case_first_type_suggestion():
    tables = [_pg(('"MyType"', 1), ("text", 1))]
    (error,) = _resolve("exclude type:mytype", tables).errors[1]
    assert error.message.endswith("Type names are case-sensitive: did you mean MyType?")
    assert [s.line_text for s in error.suggestions] == ["exclude type:MyType"]


def test_type_counts_cover_the_tables_in_scope():
    tables = [_pg(("longtext", 2), name="shop.a"), _pg(("longtext", 1), name="shop.b"),
              _pg(("longtext", 5), name="shop.c")]
    res = resolve_exclusion_rules(
        "exclude type:longtext", migratable=tables, source_type=MY,
        effective_selection=["shop.a", "shop.b"])
    assert res.line_status[1].type_counts == (("longtext", 3, 2),)


def test_name_misses_name_the_first_failing_level():
    res = _res("exclude shop.prodcut_media.*")
    (error,) = res.errors[1]
    assert error.message == 'No table "prodcut_media" in schema shop.'
    assert [s.line_text for s in error.suggestions] == ["exclude shop.product_media.*"]
    assert error.suggestions[0].label == 'Use "exclude shop.product_media.*"'
    (error,) = _res("exclude shop.orders.Notes").errors[1]
    assert error.message == (
        'No column "Notes" in shop.orders. Names are case-sensitive: did you mean notes?')
    assert [s.line_text for s in error.suggestions] == ["exclude shop.orders.notes"]
    (error,) = _res("exclude table shpo.orders").errors[1]
    assert error.message == 'No schema "shpo".'
    assert [s.line_text for s in error.suggestions] == ["exclude table shop.orders"]
    (error,) = _res("exclude shop.*.nosuchcolumn").errors[1]
    assert error.message.startswith('No column "nosuchcolumn" in the ') and error.suggestions == ()
    (error,) = _res("exclude shop.zz*.x").errors[1]
    assert error.message == 'No table matching "zz*" in schema shop.' and error.suggestions == ()
    (error,) = _res('exclude "my schema"."order.itemz".x').errors[1]
    assert [s.line_text for s in error.suggestions] == ['exclude "my schema"."order.items".x']


# --- P3-C06: eligibility, E4, 42703, target changes, review items, Save diff ---------

from types import SimpleNamespace  # noqa: E402

from dsql_migrator.core.column_exclusion import build_exclusion_context  # noqa: E402
from dsql_migrator.core.exclusion_rules import LINE_ONLY_BLOCKED  # noqa: E402
from dsql_migrator.core.models import ForeignKeyDef, SourceInventory  # noqa: E402

_CUST_DDL = ('CREATE TABLE "shop"."customers" ("id" BIGINT NOT NULL, "code" TEXT NOT NULL, '
             '"name" TEXT DEFAULT \'n/a\', PRIMARY KEY ("id"))')
_ORD_DDL = ('CREATE TABLE "shop"."orders" ("region" TEXT NOT NULL, "id" BIGINT NOT NULL, '
            '"cust_code" TEXT, "price" NUMERIC NOT NULL DEFAULT 0, "total" NUMERIC GENERATED '
            'ALWAYS AS (price * 2) STORED, "note" TEXT, PRIMARY KEY ("region", "id"))')
_FK = ('ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_cust" FOREIGN KEY ("cust_code") '
       'REFERENCES "shop"."customers" ("code") NOT VALID')


def _shop(ddls=None, odd_names=False):
    customers = TableDef(name="shop.customers", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="code", mysql_type="text"),
        ColumnDef(name="name", mysql_type="text")])
    orders = TableDef(name="shop.orders", primary_key=["id"], columns=[
        ColumnDef(name="region", mysql_type="text"), ColumnDef(name="id", mysql_type="bigint"),
        ColumnDef(name="cust_code", mysql_type="text"), ColumnDef(name="price", mysql_type="numeric"),
        ColumnDef(name="total", mysql_type="numeric", generated=True,
                  generated_expression="(price * (2)::numeric)"),
        ColumnDef(name="note", mysql_type="text")],
        foreign_keys=[ForeignKeyDef(name="fk_cust", columns=["cust_code"],
                                    referenced_table="shop.customers", referenced_columns=["code"])])
    tables = [customers, orders]
    if odd_names:
        tables.append(TableDef(name="shop.we:ird", primary_key=["id"], columns=[
            ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="x", mysql_type="text")]))
        tables.append(TableDef(name="shop.plain", primary_key=["id"], columns=[
            ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="a:b", mysql_type="text"),
            ColumnDef(name="c,d", mysql_type="text")]))
    ddls = ddls if ddls is not None else {"shop.customers": _CUST_DDL, "shop.orders": _ORD_DDL}
    applied = {
        name: SimpleNamespace(target_ddl=ddl, foreign_key_ddls=[_FK] if name == "shop.orders" else [],
                              preserved_foreign_keys=next(t for t in tables if t.name == name).foreign_keys,
                              index_ddls=[])
        for name, ddl in ddls.items()
    }
    return tables, applied


def _full(text, *, tables=None, applied=None, selection=None, live=None, stored=None,
          legacy=None, calls=None, prev=()):
    if tables is None:
        tables, applied = _shop()
    inventory = SourceInventory(tables=tables)

    def factory(selected, focus):
        if calls is not None:
            calls.append((tuple(selected), tuple(sorted(focus))))
        return build_exclusion_context(inventory, applied, selected_tables=selected,
                                       focus_tables=focus, warnings=True)

    return resolve_exclusion_rules(
        text, migratable=tables, source_type=MY,
        effective_selection=selection if selection is not None else [t.name for t in tables],
        rule_excluded_prev=prev, context_factory=factory, applied_conversions=applied,
        live_columns=live, legacy=legacy, stored_columns=stored)


def test_blocked_columns_are_reported_and_never_stored():
    calls = []
    res = _full("exclude shop.customers.code\nexclude shop.orders.region\nexclude shop.orders.total\n"
                "exclude *.*.id", calls=calls)
    assert res.columns == {} and res.can_save
    assert len(calls) == 1 and calls[0][0] == ("shop.customers", "shop.orders")
    labels = {(b.table, b.column): b.labels for b in res.blocked}
    assert labels == {
        ("shop.customers", "code"): ("referenced by a foreign key",),
        ("shop.orders", "region"): ("target key",),
        ("shop.orders", "total"): ("computed",),
        ("shop.customers", "id"): ("primary key",),
        ("shop.orders", "id"): ("primary key",),
    }
    st = res.line_status
    assert all(LINE_ONLY_BLOCKED in st[n].codes for n in (1, 2, 3, 4))
    assert (st[4].blocked, st[4].blocked_labels) == (2, ("primary key",))
    assert next(b for b in res.blocked if b.column == "code").lines == (1,)


def test_b3_is_freed_when_this_save_unselects_the_only_referencing_child():
    calls = []
    res = _full("exclude shop.customers.code\nexclude table shop.orders", calls=calls)
    assert res.columns == {"shop.customers": frozenset({"code"})}
    assert calls[0][0] == ("shop.customers",)


def test_colon_in_a_table_name_is_e4_but_not_in_a_column_name_and_commas_are_fine():
    tables, applied = _shop(odd_names=True)
    res = _full("exclude shop.we:ird.x", tables=tables, applied=applied)
    (error,) = res.errors[1]
    assert error.code == "E4" and '":"' in error.message and not res.can_save
    ok = _full('exclude shop.plain."a:b"\nexclude shop.plain."c,d"', tables=tables, applied=applied)
    assert ok.can_save and ok.columns == {"shop.plain": frozenset({"a:b", "c,d"})}
    # a table rule on it is representable: the selection stores the name as is
    assert _full("exclude table shop.we:ird", tables=tables, applied=applied).can_save


def test_42703_blocks_only_a_certain_failure_and_offers_a_rule():
    ddls = {"shop.customers": _CUST_DDL.replace(', "name" TEXT DEFAULT \'n/a\'', ""),
            "shop.orders": _ORD_DDL}
    tables, applied = _shop(ddls=ddls)
    res = _full("", tables=tables, applied=applied)
    (error,) = res.global_errors
    assert error.code == "E4" and "shop.customers.name" in error.message and "42703" in error.message
    assert [s.line_text for s in error.suggestions] == ["exclude shop.customers.name"]
    assert not res.can_save
    assert _full("exclude shop.customers.name", tables=tables, applied=applied).can_save
    live = {"shop.customers": frozenset({"id", "code", "name"})}
    assert _full("", tables=tables, applied=applied, live=live).can_save


def test_target_changes_match_the_start_dialog_builder():
    from dsql_migrator.core.schema_applier import build_overlay_statements, ddl_column_facts

    res = _full("exclude shop.orders.price\nexclude shop.orders.note\nexclude shop.customers.name")
    by_table = {c.table: c for c in res.target_changes}
    expected = build_overlay_statements(
        "shop.orders", ddl_column_facts(_ORD_DDL, ["note", "price"]))
    assert by_table["shop.orders"].statements == tuple(s.text() for s in expected)
    assert [s.split(" ALTER COLUMN ")[1] for s in by_table["shop.orders"].statements] == [
        '"price" DROP NOT NULL', '"price" DROP DEFAULT']
    assert by_table["shop.orders"].restores == (("price", "0"),)
    assert by_table["shop.customers"].restores == (("name", "'n/a'"),)


def test_an_unreadable_ddl_says_so_never_nothing_to_do():
    tables, applied = _shop(ddls={"shop.customers": "not a create table", "shop.orders": _ORD_DDL})
    res = _full("exclude shop.customers.name", tables=tables, applied=applied)
    (change,) = [c for c in res.target_changes if c.table == "shop.customers"]
    assert change.statements == () and change.note == (
        "Cannot read the Aurora DSQL DDL of shop.customers: the changes are decided when the "
        "migration starts.")


def test_retained_legacy_entries_stay_excluded_without_an_overlay():
    res = _full("exclude shop.orders.price\nexclude shop.orders.note",
                legacy={"shop.orders": {"price"}})
    assert res.columns == {"shop.orders": frozenset({"price", "note"})}
    assert res.retained_legacy == {"shop.orders": frozenset({"price"})}
    assert all(c.table != "shop.orders" or not c.statements for c in res.target_changes)


def test_review_items_and_the_save_diff():
    res = _full(
        "exclude shop.orders.note\nexclude shop.orders.price\ninclude shop.orders.price\n"
        "exclude shop.orders.id\nexclude table shop.customers",
        stored={"shop.orders": {"cust_code"}})
    kinds = {(i.kind, i.table, i.column) for i in res.items}
    assert kinds == {
        ("unselected", "shop.customers", None), ("excluded", "shop.orders", "note"),
        ("kept", "shop.orders", "price"), ("blocked", "shop.orders", "id"),
    }
    kept = next(i for i in res.items if i.kind == "kept")
    assert kept.lines == (3,) and kept.why == ("Kept by include on L3.",)
    unselected = next(i for i in res.items if i.kind == "unselected")
    assert unselected.why[0].startswith("Not loaded by this migration")
    assert res.diff.added == {"shop.orders": ("note",)}
    assert res.diff.removed == {"shop.orders": ("cust_code",)}
    assert res.diff.text == "+1 column, -1; unselects 1 table, re-selects 0"
    assert ("L3 include shop.orders.price re-includes shop.orders.price excluded by L2"
            in res.diff.lines)


def test_column_only_diff_text_and_a_warning_flag():
    res = _full("exclude shop.orders.price", stored={})
    assert res.diff.text == "+1 column, -0"
    (item,) = [i for i in res.items if i.kind == "excluded"]
    assert item.warning and any("NOT NULL" in w for w in item.why)


def test_each_ddl_is_read_once_per_resolve_and_not_again(monkeypatch):
    import sqlglot

    names = [f"s.t{i:04d}" for i in range(300)]
    tables = [TableDef(name=n, primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="x", mysql_type="text")])
        for n in names]
    applied = {n: SimpleNamespace(
        target_ddl=f'CREATE TABLE "s"."{n[2:]}" ("id" BIGINT NOT NULL, "x" TEXT NOT NULL, PRIMARY KEY ("id"))',
        foreign_key_ddls=[], preserved_foreign_keys=[], index_ddls=[]) for n in names}
    real = sqlglot.parse_one
    seen: dict = {}

    def counting(sql, *a, **k):
        seen[sql] = seen.get(sql, 0) + 1
        return real(sql, *a, **k)

    monkeypatch.setattr(sqlglot, "parse_one", counting)
    first = _full("exclude *.*.x", tables=tables, applied=applied)
    assert sum(len(c) for c in first.columns.values()) == 300
    per_ddl = max(seen.values())
    seen.clear()
    _full("exclude *.*.x", tables=tables, applied=applied)
    assert seen == {}, "a repeated resolve must not re-parse any DDL"
    # one parse per cached reader (key facts: pk, identity, generated; constraints; UNIQUE
    # members; generated expressions; column names; the overlay preview's generated set),
    # never a repeat
    assert per_ddl <= 8


# --- review fixes (C01-C04 adversarial review) ---------------------------------------


def _fix(text):
    rules, errors = parse_rules(text)
    assert not rules and list(errors) == [1], (rules, errors)
    (error,) = errors[1]
    return error


PG_MULTI = [_pg(("character(3)", 1), ("character varying(255)", 1), ("character varying(9)[]", 1),
                ("timestamp without time zone", 1), ("timestamp with time zone", 1),
                ("bit(3)", 1), ("bit varying(8)", 1), ("double precision", 1), ("text", 1),
                ('"MyType"', 1), ('"MyType"[]', 1), ('app."Mood"', 1), ('app."Mood"[]', 1),
                ("numeric(10,2)", 1))]


def _only_key(res):
    keys = {rule_type_name(c, PG) for t in PG_MULTI for c in t.columns
            if c.name in res.columns.get("public.t", ())}
    return keys


@pytest.mark.parametrize("typed,fixed,key", [
    ("character varying", '"character varying"', "character varying"),
    ("character varying(255)", '"character varying(255)"', "character varying"),
    ("timestamp with time zone", '"timestamp with time zone"', "timestamp with time zone"),
    ("bit varying", '"bit varying"', "bit varying"),
    ("double precision", '"double precision"', "double precision"),
])
def test_an_unquoted_multi_word_type_is_fixed_by_quoting_never_by_a_comment(typed, fixed, key):
    error = _fix(f"exclude type:{typed}  # why")
    (fix,) = error.suggestions
    assert fix.line_text == f"exclude type:{fixed}  # why"
    res = _resolve(fix.line_text, PG_MULTI)
    assert res.can_save and _only_key(res) == {key}


def test_multi_word_fix_keeps_the_other_names():
    error = _fix("exclude type:character varying,text")
    assert error.suggestions[0].line_text == 'exclude type:"character varying",text'
    error = _fix("exclude type:text,character varying")
    assert error.name_index == 1
    assert error.suggestions[0].line_text == 'exclude type:text,"character varying"'


def test_mysql_multi_word_type_reaches_the_varchar_hint():
    tables = [_pg(("char(2)", 1), ("varchar(255)", 1), name="shop.t")]
    fix = _fix("exclude type:character varying").suggestions[0].line_text
    (error,) = _resolve(fix, tables, MY).errors[1]
    assert [s.line_text for s in error.suggestions] == ["exclude type:varchar"]


@pytest.mark.parametrize("typed,fixed,key", [
    ('app."Mood"[]', '"app.Mood[]"', "app.Mood[]"),
    ('"MyType"[]', "MyType[]", "MyType[]"),
    ('"character varying"[]', '"character varying[]"', "character varying[]"),
    ('"app"."Mood"', '"app.Mood"', "app.Mood"),
])
def test_a_format_type_spelling_gets_one_fix_to_its_key(typed, fixed, key):
    error = _fix(f"exclude type:{typed} # c")
    (fix,) = error.suggestions
    assert fix.line_text == f"exclude type:{fixed} # c"
    res = _resolve(fix.line_text, PG_MULTI)
    assert res.can_save and _only_key(res) == {key}


def test_type_list_fixes_keep_the_comment():
    assert _fix("exclude type:app.status # PII").suggestions[0].line_text == (
        'exclude type:"app.status" # PII')
    assert _fix("exclude type:long* # c").suggestions[0].line_text == 'exclude type:"long*" # c'
    assert _fix("exclude type: text , bytea # c").suggestions[0].line_text == (
        "exclude type:text,bytea # c")
    assert _fix('exclude type:"type:x" , y').suggestions[0].line_text == 'exclude type:"type:x",y'


def test_a_typmod_with_a_comma_is_one_name():
    res = _resolve("exclude type:numeric(10,2),text", PG_MULTI)
    assert res.can_save and [t.text for t in res.rules[1].type_names] == ["numeric(10,2)", "text"]
    assert _resolve("exclude type:numeric(5,-2)", PG_MULTI).can_save
    my = [_pg(("decimal(10,2)", 1), name="shop.t")]
    assert _resolve("exclude type:decimal(10,2)", my, MY).can_save
    (error,) = _resolve("exclude type:numeric(10,2)", my, MY).errors[1]
    assert [s.line_text for s in error.suggestions] == ["exclude type:decimal"]
    assert _fix("exclude type:numeric(10").message == 'Unclosed "(" in a type name.'
    spaced = _fix("exclude type:numeric(10, 2), text")
    assert _resolve(spaced.suggestions[0].line_text, PG_MULTI).can_save


def test_first_word_fixes_never_lose_the_subject():
    assert [s.line_text for s in _fix("excludeshop.t.c").suggestions] == ["exclude shop.t.c"]
    assert [s.line_text for s in _fix("exclude.a.b.c").suggestions] == ["exclude a.b.c"]
    assert [s.line_text for s in _fix("exlcude s.t.c").suggestions] == ["exclude s.t.c"]
    assert _fix("frobnicate.x s.t.c").suggestions == ()


@pytest.mark.parametrize("line,fixed", [
    ("exclude s.t.c", "exclude s.t.c"),
    ("exclude　s.t.c", "exclude s.t.c"),
    ("exclude table shop.audit", "exclude table shop.audit"),
    ("exclude s.t.c # x", "exclude s.t.c # x"),
])
def test_a_non_standard_space_is_named_and_fixed(line, fixed):
    error = _fix(line)
    assert error.message.startswith("A non-standard space (U+")
    assert [s.line_text for s in error.suggestions] == [fixed]
    assert parse_rules(fixed)[1] == {}


def test_a_quoted_non_standard_space_is_a_name():
    rule = _one('exclude s.t."a b"')
    assert rule.parts[2].text == "a b"
    assert rule_part_token("a b") == '"a b"'


@pytest.mark.parametrize("line", ["exclude table", "exclude table ", "exclude table\t",
                                  "exclude table # c"])
def test_a_bare_table_keyword_says_what_it_takes(line):
    error = _fix(line)
    assert error.message == '"table" takes schema.table (two parts).' and error.suggestions == ()


def test_table_dot_names_stay_column_refs():
    assert _one("exclude table.x.y").kind == "column"


def test_names_with_line_breaks_are_not_writable_and_never_suggested():
    from dsql_migrator.core.exclusion_rules import rule_name_writable

    assert not rule_name_writable("a\nb") and not rule_name_writable("x\r")
    assert rule_part_token("x\r") == '"x\r"'
    tables = [_t("shop.notes", "long_note\n")]
    res = resolve_exclusion_rules("exclude shop.notes.long_note", migratable=tables,
                                  source_type=MY, effective_selection=["shop.notes"])
    (error,) = res.errors[1]
    assert all("\n" not in s.line_text and "\r" not in s.line_text for s in error.suggestions)


def test_array_bpchar_and_character_are_the_same_type():
    tables = [_pg(("bpchar[]", 2), ("character(5)[]", 3))]
    bp = _resolve("exclude type:bpchar[]", tables).line_status[1].notes
    assert len(bp) == 1 and "a different type" not in bp[0] and "type:character[]" in bp[0]
    ch = _resolve("exclude type:character[]", tables).line_status[1].notes
    assert len(ch) == 1 and "printed bpchar[]: add type:bpchar[] to cover them." in ch[0]
    assert _resolve("exclude type:bpchar[],character[]", tables).line_status[1].notes == ()


def test_one_note_per_key_and_singular_wording():
    tables = [_pg(('"char"', 1), ("character(5)", 1))]
    (note,) = _resolve("exclude type:char,CHAR", tables).line_status[1].notes
    assert note == ("type:char matched char (1 column). character is a different type here "
                    "(1 column): use type:character for it.")
    single = [_pg(("bpchar", 1), ("character(5)", 1))]
    (bp,) = _resolve("exclude type:bpchar", single).line_status[1].notes
    assert "The character column is the same type declared with a length: add type:character to cover it." in bp
    (ch,) = _resolve("exclude type:character,CHARACTER", single).line_status[1].notes
    assert ch.endswith("printed bpchar: add type:bpchar to cover it.")


def test_interval_field_qualifiers_are_one_type():
    for declared in ("interval", "interval(3)", "interval year to month", "interval day to second(3)"):
        assert rule_type_name(_col(declared), PG) == "interval", declared
    assert rule_type_name(_col("interval second(6)[]"), PG) == "interval[]"
    assert rule_type_name(_col('"interval year"'), PG) == "interval year"
    tables = [_pg(("interval", 1), ("interval year to month", 1), ("interval day to second(3)", 1))]
    res = _resolve("exclude type:interval", tables)
    assert res.line_status[1].type_counts == (("interval", 3, 1),)


def test_type_difflib_ignores_case():
    tables = [_pg(("integer", 1), ("bytea", 1), ("jsonb", 1), ("text", 1))]
    (error,) = _resolve("exclude type:INTEGR", tables).errors[1]
    assert [s.line_text for s in error.suggestions] == ["exclude type:integer"]


def test_type_counts_agree_with_matches():
    res = _res("exclude type:longtext\ninclude shop.pages.body")
    st = res.line_status[1]
    assert st.matched == 4 and st.type_counts == (("longtext", 4, 4),) and st.kept_by == ((2, 1),)
    multi = _res("exclude type:longtext,longblob\ninclude shop.product_media.content")
    st = multi.line_status[1]
    assert sum(n for _k, n, _t in st.type_counts) == st.matched


def test_no_types_here_list_when_nothing_has_a_type():
    (error,) = resolve_exclusion_rules("exclude type:text", migratable=[], source_type=PG,
                                       effective_selection=[]).errors[1]
    assert "Types here" not in error.message


def test_every_e1_fix_makes_progress():
    """A fix never repeats its own error and never drops a name part (property)."""
    import random

    rnd = random.Random(99)
    pieces = ["exclude", "include", "exlcude", "table", "type:", "s", "t", "c", ".", " ", ",",
              '"', "*", "#", "(", ")", "[]", "\u00a0", "\u3000", "\t", "character", "varying",
              "x", "excludes", "EXCLUDE", "app", '""']
    for _ in range(3000):
        line = "".join(rnd.choice(pieces) for _ in range(rnd.randint(1, 9)))
        _rules, errors = parse_rules(line)
        for error in errors.get(1, ()):
            for fix in error.suggestions:
                _r2, again = parse_rules(fix.line_text)
                assert all(e.message != error.message for e in again.get(1, ())), (line, fix)


# --- P3-C08: rule text for sessions without it ---------------------------------------

from dsql_migrator.core.exclusion_rules import (  # noqa: E402
    LEGACY_COMMENT,
    synthesis_gaps,
    synthesize_rule_text,
)


def test_synthesized_text_resolves_to_exactly_the_stored_set():
    stored = {"shop.users": {"notes", "email"}, "shop.orders": {"memo"},
              "my schema.order.items": {"weird*col"}}
    text = synthesize_rule_text(stored)
    assert text.splitlines() == [
        'exclude "my schema"."order.items"."weird*col"',
        "exclude shop.orders.memo",
        "exclude shop.users.email",
        "exclude shop.users.notes",
    ]
    res = _res(text)
    assert res.can_save and {t: set(c) for t, c in res.columns.items()} == stored
    assert res.selected_after is None and res.rule_excluded_after == frozenset()
    assert synthesis_gaps(res, stored, TABLES) == ()


def test_comments_say_where_an_entry_came_from():
    stored = {"shop.users": {"notes", "email"}}
    legacy = {"shop.users": {"notes"}}
    assert synthesize_rule_text(stored, legacy).splitlines() == [
        "exclude shop.users.email", f"exclude shop.users.notes  {LEGACY_COMMENT}"]
    assert synthesize_rule_text(stored, legacy, adopted_from="dsql-cdc-x").splitlines() == [
        "exclude shop.users.email  # adopted from dsql-cdc-x",
        "exclude shop.users.notes  # adopted from dsql-cdc-x, omit-only"]


@pytest.mark.parametrize("schema,table,column", [
    ("my.schema", 't"q', "c d"), ("s", "*", "x*"), ("types", "table", " lead"),
])
def test_odd_names_round_trip_through_synthesis(schema, table, column):
    name = f"{schema}.{table}"
    tables = [_t(name, column)]
    stored = {name: {column}}
    res = resolve_exclusion_rules(synthesize_rule_text(stored), migratable=tables,
                                  source_type=MY, effective_selection=[name])
    assert res.can_save and {t: set(c) for t, c in res.columns.items()} == stored


def test_gaps_are_reported_never_dropped_silently():
    tables, applied = _shop()
    stored = {"shop.customers": {"code", "name"}, "shop.orders": {"note", "gone"},
              "old.table": {"x"}, "shop.lines": {"a\nb"}}
    text = synthesize_rule_text(stored)
    assert "a\nb" not in text
    selection = ["shop.customers", "shop.orders"]
    res = _full(text, tables=tables, applied=applied, selection=selection)
    gaps = {(g.table, g.column): g.reason for g in synthesis_gaps(res, stored, tables)}
    assert gaps == {
        ("shop.customers", "code"): "Now blocked (referenced by a foreign key).",
        ("shop.orders", "gone"): "The column is no longer in the source.",
        ("old.table", "x"): "Its table is not migrated any more.",
        ("shop.lines", "a\nb"): "Its name cannot be written as a rule.",
    }
    unselected = _full(text, tables=tables, applied=applied, selection=["shop.customers"])
    gaps = {(g.table, g.column): g.reason for g in synthesis_gaps(unselected, stored, tables)}
    assert gaps[("shop.orders", "note")] == "Its table is not selected."


# --- review fixes (C05-C11 review) ----------------------------------------------------


def test_a_legacy_entry_gets_no_overlay_story():
    res = _full("exclude shop.orders.price", legacy={"shop.orders": {"price"}})
    (item,) = [i for i in res.items if i.kind == "excluded"]
    assert item.legacy and not any(
        s in w for w in item.why for s in ("NOT NULL will be dropped", "restored at Cut over",
                                           "computed from NULL"))
    assert any("Excluded before v0.1.562" in w for w in item.why)
    plain = _full("exclude shop.orders.price")
    (item,) = [i for i in plain.items if i.kind == "excluded"]
    assert not item.legacy and any("NOT NULL will be dropped" in w for w in item.why)


def test_42703_never_offers_a_line_an_include_cancels():
    ddls = {"shop.customers": _CUST_DDL.replace(', "name" TEXT DEFAULT \'n/a\'', ""),
            "shop.orders": _ORD_DDL}
    tables, applied = _shop(ddls=ddls)
    res = _full("exclude shop.*.name\ninclude shop.customers.name", tables=tables, applied=applied)
    (error,) = res.global_errors
    assert error.suggestions == () and "kept by include on L2" in error.message
    only = _full("include shop.customers.name", tables=tables, applied=applied)
    (error,) = only.global_errors
    assert error.suggestions == () and "kept by include on L1" in error.message


def test_a_colon_table_gets_one_e4_per_line_and_a_whole_table_rule_for_42703():
    tables, applied = _shop(odd_names=True)
    weird = next(t for t in tables if t.name == "shop.we:ird")
    weird.columns.append(ColumnDef(name="y", mysql_type="text"))
    res = _full("exclude shop.we:ird.*", tables=tables, applied=applied)
    assert [e.code for e in res.errors[1]] == ["E4"]
    assert "matches 2 of its columns" in res.errors[1][0].message
    assert "exclude table shop.we:ird" in res.errors[1][0].message


def test_columns_on_a_reticked_table_are_reported_not_stored():
    res = _res("exclude table shop.audit_*\nexclude *.*.payload",
               selection=[n for n in ALL if n != "shop.audit_2023"],
               prev={"shop.audit_2023"}, reticks={"shop.audit_2024"})
    assert ("shop.audit_2024", "payload") in res.retick_columns
    assert "shop.audit_2024" not in res.columns



# --- final check: fixes never silently narrow a rule ---------------------------------------


def test_builtin_collision_domain_named_integer_shares_the_integer_key():
    # Documented in rule_type_name: CREATE DOMAIN public."integer" prints "integer".
    res = _resolve("exclude type:integer", [_pg(('"integer"', 1), ("integer", 1))])
    assert dict(res.columns) == {"public.t": frozenset({"c1", "c2"})}
    assert res.line_status[1].type_counts == (("integer", 2, 1),)


def test_a_space_separated_type_list_is_e2_with_the_comma_fix_never_a_silent_first_type():
    table = TableDef(name="shop.media", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="doc", mysql_type="longtext"),
        ColumnDef(name="img", mysql_type="longblob")])
    # The quoting fix of the unquoted form leads here; on MySQL the space cut used to
    # match only longtext and leave img migrated.
    res = _resolve('exclude type:"longtext longblob"', [table], MY)
    (err,) = res.errors[1]
    assert err.code == "E2" and "separate types with a comma" in err.message
    (fix,) = err.suggestions
    assert fix.line_text == "exclude type:longtext,longblob"
    fixed = _resolve(fix.line_text, [table], MY)
    assert dict(fixed.columns) == {"shop.media": frozenset({"doc", "img"})}
    # A real multi-word type is untouched (precision is not a type here).
    dbl = TableDef(name="shop.n", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="x", mysql_type="double")])
    assert dict(_resolve('exclude type:"double precision"', [dbl], MY).columns) == {
        "shop.n": frozenset({"x"})}
    # PG exact multi-word keys still win.
    pg = _pg(("character varying(20)", 1), ("text", 1))
    assert dict(_resolve('exclude type:"character varying"', [pg]).columns) == {
        "public.t": frozenset({"c1"})}


@pytest.mark.parametrize("text", [
    "exclude shop.users.email,phone", "exclude shop.users.email, phone  # why",
    "exclude table shop.a,shop.b", "exclude shop.users.,phone"])
def test_a_comma_list_of_names_gets_no_comment_fix(text):
    _rules, errors = parse_rules(text)
    (err,) = errors[1]
    assert err.code == "E1" and err.message.startswith("One rule per line")
    assert err.suggestions == ()



@pytest.mark.parametrize("text,fixed", [
    ("exclude shop.media.doc,", "exclude shop.media.doc"),
    ("exclude shop.media.doc, # note", "exclude shop.media.doc  # note"),
    ("exclude table shop.media ,", "exclude table shop.media"),
])
def test_a_lone_trailing_comma_is_removed_not_called_a_list(text, fixed):
    _rules, errors = parse_rules(text)
    (err,) = errors[1]
    assert err.message == "A trailing comma: remove it."
    (fix,) = err.suggestions
    assert fix.line_text == fixed and not parse_rules(fix.line_text)[1]



@pytest.mark.parametrize("typed", ['"longtext longblob mediumblob"', '"longtext mediumtext"'])
def test_a_mysql_lob_list_with_an_absent_type_is_still_the_comma_e2(typed):
    # Final regression check: an absent type in the list must not fall back to the space
    # cut (only longtext matched, longblob silently migrated).
    table = TableDef(name="shop.media", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="doc", mysql_type="longtext"),
        ColumnDef(name="img", mysql_type="longblob")])
    res = _resolve(f"exclude type:{typed}", [table], MY)
    (err,) = res.errors[1]
    assert err.code == "E2" and "separate types with a comma" in err.message
    assert dict(res.columns) == {}
    # MySQL attribute words keep their meaning.
    for legit in ('"int unsigned"', '"double precision"'):
        t2 = TableDef(name="shop.n", primary_key=["id"], columns=[
            ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="x", mysql_type="int unsigned"),
            ColumnDef(name="y", mysql_type="double")])
        assert not _resolve(f"exclude type:{legit}", [t2], MY).errors, legit



def test_a_misspelled_spaced_mysql_type_is_not_called_a_list():
    # a48ed96 review: "medium text" is one misspelled type -- its fix is mediumtext, never
    # a comma list that ends up excluding every text column.
    table = TableDef(name="shop.media", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="doc", mysql_type="mediumtext"),
        ColumnDef(name="body", mysql_type="text")])
    res = _resolve('exclude type:"medium text"', [table], MY)
    (err,) = res.errors[1]
    assert "separate types with a comma" not in err.message
    assert [s.line_text for s in err.suggestions] == ["exclude type:mediumtext"]


@pytest.mark.parametrize("text,message", [
    ("exclude ,", 'A name is missing before ",": write the name and remove the comma.'),
    ("exclude table ,", 'A name is missing before ",": write the name and remove the comma.'),
    ("exclude , # c", 'A name is missing before ",": write the name and remove the comma.'),
    ("exclude shop.t.,", 'An empty name part: remove the extra ".".'),
    ("exclude , shop.t.c", "One rule per line: a comma list works only after type:. Put each "
                           "name on its own line."),
])
def test_an_empty_name_before_a_comma_names_what_is_missing(text, message):
    _rules, errors = parse_rules(text)
    (err,) = errors[1]
    assert err.message == message


def test_a_full_mysql_enum_spelling_with_a_spaced_value_still_matches():
    # a48ed96 review: a LOB word inside an enum value is not a type list.
    table = TableDef(name="shop.media", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="bigint"),
        ColumnDef(name="fmt", mysql_type="enum('rich text editor','markdown')"),
        ColumnDef(name="body", mysql_type="text")])
    res = _resolve('exclude type:"enum(\'rich text editor\',\'markdown\')"', [table], MY)
    assert not res.errors and dict(res.columns) == {"shop.media": frozenset({"fmt"})}


# --- rule_matched_columns (G6: Fix target schema… never adds a rule-matched column) ----------

_USERS_LIVE = [("id", "int"), ("pw_hash", "varchar(255)"), ("pw_salt", "varchar(64)"),
               ("bio", "longtext"), ("nick", "varchar(32)")]


def _matched(text, table="shop.users", columns=_USERS_LIVE, pk=("id",), source_type=None):
    from dsql_migrator.core.exclusion_rules import rule_matched_columns
    from dsql_migrator.core.models import SourceType

    return rule_matched_columns(
        text, table, columns, pk, source_type=source_type or SourceType.MYSQL
    )


def test_rule_matched_columns_wildcard_type_and_include_wins() -> None:
    # Resolved against the LIVE columns: pw_hash/bio may have been added after the Save.
    text = ("exclude shop.users.pw_*\nexclude type:longtext\nexclude shop.orders.secret\n"
            "include shop.users.pw_salt")
    assert _matched(text) == {
        "pw_hash": "exclude shop.users.pw_*", "bio": "exclude type:longtext",
    }


def test_rule_matched_columns_postgres_type_rule() -> None:
    from dsql_migrator.core.models import SourceType

    got = _matched("exclude type:jsonb", table="app.t",
                   columns=[("id", "integer"), ("doc", "jsonb"), ("n", "text")],
                   source_type=SourceType.POSTGRES)
    assert got == {"doc": "exclude type:jsonb"}


def test_rule_matched_columns_keep_column_matches_under_a_table_rule() -> None:
    # A table line that would unselect the captured table must not hide its column rules
    # (the resolver re-ticks the table, and its column matches come back without a line).
    text = "exclude table shop.users\nexclude shop.users.pw_*"
    assert not parse_rules(text)[1]  # a valid table rule, not the E1 two-part line
    assert _matched(text) == {"pw_hash": None, "pw_salt": None}


def test_rule_matched_columns_text_is_the_matching_line_without_its_comment() -> None:
    got = _matched("  exclude shop.users.bio   # free text\n")
    assert got == {"bio": "exclude shop.users.bio"}


def test_rule_matched_columns_none_empty_or_other_tables_match_nothing() -> None:
    assert _matched(None) == {} and _matched("") == {} and _matched("  \n ") == {}
    assert _matched("exclude shop.orders.secret") == {}
    assert _matched("# only a comment") == {}
