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
