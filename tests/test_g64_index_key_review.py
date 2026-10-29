"""G64: the review of G44-G55 -- keys and indexes Aurora DSQL cannot build, statement
splitting, and what the index notes say."""

from __future__ import annotations

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import (
    SchemaConverter,
    rename_index_script,
    validate_composite_leading_column,
)
from dsql_migrator.core.models import (
    ColumnDef,
    ConversionNoteKind,
    IndexDef,
    SourceInventory,
    SourceType,
    TableDef,
)
from dsql_migrator.ui.schema_conversion_apply import (
    composite_leading_candidates,
    split_sql_statements,
)

_PG = SourceType.POSTGRES
_MY = SourceType.MYSQL


def _col(name: str, type_: str, nullable: bool = True) -> ColumnDef:
    return ColumnDef(name=name, mysql_type=type_, nullable=nullable)


def _assess(table: TableDef, engine=_PG):
    (item,) = CompatibilityAssessor(source_type=engine).assess(SourceInventory(tables=[table])).items
    return item


def _convert(table: TableDef, engine=_PG):
    (converted,) = SchemaConverter(source_type=engine).convert(SourceInventory(tables=[table])).tables
    return converted


def test_a_composite_key_never_leads_with_a_column_dsql_cannot_index() -> None:
    """An interval or time with time zone leader made a PRIMARY KEY the CREATE TABLE was
    refused for, with no note; jsonb/bytea were blocked only by a misleading size reason."""
    pg = TableDef(
        name="app.events",
        columns=[
            _col("id", "bigint", False), _col("span", "interval", False),
            _col("at", "time with time zone", False), _col("doc", "jsonb", False),
            _col("raw", "bytea", False), _col("tenant", "bigint", False),
        ],
        primary_key=["id"],
    )
    assert composite_leading_candidates(pg, is_postgres=True) == ["tenant"]
    error = validate_composite_leading_column(pg, "span", is_postgres=True)
    assert error and "becomes interval on Aurora DSQL, a type it cannot index" in error
    mysql = TableDef(
        name="app.events",
        columns=[
            _col("id", "bigint", False), _col("doc", "json", False),
            _col("raw", "blob", False), _col("tenant", "bigint", False),
        ],
        primary_key=["id"],
    )
    assert composite_leading_candidates(mysql) == ["tenant"]
    assert "becomes json" in validate_composite_leading_column(mysql, "doc")


def test_an_indexed_time_duration_is_told_to_become_text() -> None:
    table = TableDef(
        name="app.calls",
        columns=[_col("id", "int", False), _col("dur", "time", False), _col("idle", "time")],
        primary_key=["id"],
        indexes=[IndexDef(name="ix_calls_dur", columns=["dur"])],
    )
    item = _assess(table, _MY)
    (concern,) = [c for c in item.concerns if c.rule_id == "TIME_TYPE"]
    assert "to text for dur, which an index or the primary key uses" in concern.recommendation
    assert "cannot index an interval" in concern.recommendation


def test_the_source_pane_shows_a_partial_or_hash_index_as_the_source_has_it() -> None:
    from dsql_migrator.ui.schema_conversion import render_source_table_ddl

    table = TableDef(
        name="public.users",
        columns=[_col("id", "bigint", False), _col("email", "text"), _col("deleted_at", "timestamp")],
        primary_key=["id"],
        indexes=[
            IndexDef(name="ux_live", columns=["email"], unique=True, where="(deleted_at IS NULL)"),
            IndexDef(name="ix_hash", columns=["email"], method="hash"),
        ],
    )
    ddl = render_source_table_ddl(table, source_type=_PG)
    assert 'CREATE UNIQUE INDEX "ux_live" ON "public"."users" ("email") WHERE (deleted_at IS NULL)' in ddl
    assert 'CREATE INDEX "ix_hash" ON "public"."users" USING hash ("email")' in ddl


def test_a_semicolon_inside_a_literal_or_comment_does_not_split_a_statement() -> None:
    script = (
        'CREATE TABLE "public"."users" ("sep" TEXT);\n\n'
        'CREATE INDEX ASYNC "ix_users_sep" ON "public"."users" ("sep") WHERE (sep <> \';\'::text);\n'
        "-- a comment; with a semicolon\n"
        'CREATE INDEX ASYNC "ix_b" ON "public"."users" ("sep") WHERE (sep <> $$;$$);'
    )
    statements = split_sql_statements(script)
    assert len(statements) == 3
    assert statements[1].endswith("WHERE (sep <> ';'::text)")
    renamed = rename_index_script(script, {"ix_users_sep": "ix_users_sep_2", "ix_b": "ix_b_2"})
    assert '"ix_users_sep_2"' in renamed and '"ix_b_2"' in renamed
    assert "WHERE (sep <> ';'::text)" in renamed


def test_the_key_size_note_lists_only_emitted_indexes() -> None:
    for engine in (_MY, _PG):
        table = TableDef(
            name="app.people",
            columns=[_col("id", "bigint", False), _col("name", "varchar(400)"), _col("email", "varchar(100)")],
            primary_key=["id"],
            indexes=[IndexDef(name="ix_name_lower_email", columns=["name"])],
            expression_indexes=["ix_name_lower_email"],
        )
        messages = " ".join(w.message for w in _convert(table, engine).warnings)
        assert "ix_name_lower_email at up to" not in messages
        assert "NOT emitted" in messages


def test_a_hash_index_over_long_values_stays_a_loss() -> None:
    """A hash index stores only the hash -- the usual reason to pick one for a long URL;
    the btree that replaces it caps keys at 1 KiB, yet G52 graded the change advice."""
    links = TableDef(
        name="app.links",
        columns=[_col("id", "bigint", False), _col("url", "character varying(8000)")],
        primary_key=["id"],
        indexes=[IndexDef(name="ix_links_url", columns=["url"], method="hash")],
    )
    (note,) = [w for w in _convert(links).warnings if "btree-only" in w.message]
    assert note.kind is ConversionNoteKind.LOSS
    assert "hash index(es) ix_links_url become btree" in note.message
    # The index is on the table before its rows load, so the Full Load's INSERT of such a
    # value is what fails -- not a post-load index build.
    assert "the Full Load's too, since the index is on the table before its rows load" in (
        note.message)
    assert "post-load" not in note.message
    item = _assess(links)
    (concern,) = [c for c in item.concerns if c.rule_id == "PG_INDEX_NOT_PRESERVED"]
    assert concern.note_kind is ConversionNoteKind.LOSS
    assert item.classification.value == "MANUAL"


def test_an_all_expression_index_note_covers_a_unique_one() -> None:
    table = TableDef(
        name="app.users",
        columns=[_col("id", "bigint", False), _col("email", "text")],
        primary_key=["id"],
        expression_indexes=["ux_users_lower_email"],
    )
    messages = " ".join(w.message for w in _convert(table).warnings)
    assert "If ux_users_lower_email is UNIQUE on the source" in messages


def test_no_primary_key_never_offers_a_partial_or_expression_unique_index() -> None:
    table = TableDef(
        name="app.accounts",
        columns=[_col("email", "text", False), _col("tenant_id", "bigint", False),
                 _col("deleted_at", "timestamp")],
        indexes=[
            IndexDef(name="ux_live_email", columns=["email"], unique=True,
                     where="(deleted_at IS NULL)"),
            IndexDef(name="ux_tenant_email", columns=["tenant_id"], unique=True),
        ],
        expression_indexes=["ux_tenant_email"],
    )
    (concern,) = [c for c in _assess(table).concerns if c.rule_id == "NO_PRIMARY_KEY"]
    assert "already identifies each row" not in concern.recommendation


def test_an_array_primary_key_is_unsupported_at_both_steps() -> None:
    """Re-typed to jsonb, which Aurora DSQL cannot index: the CREATE TABLE is rejected, yet
    both steps said MANUAL and Evaluation that 'the table applies'."""
    table = TableDef(
        name="app.tagged",
        columns=[ColumnDef(name="tags", mysql_type="text[]", type_kind="array", nullable=False)],
        primary_key=["tags"],
    )
    converted = _convert(table)
    assert any(
        w.classification.value == "UNSUPPORTED" and "tags text[] (now jsonb)" in w.message
        for w in converted.warnings
    )
    item = _assess(table)
    assert item.classification.value == "UNSUPPORTED"
    (concern,) = [c for c in item.concerns if c.rule_id == "PG_UNSUPPORTED_TYPE"]
    assert "The table applies" not in concern.risk
