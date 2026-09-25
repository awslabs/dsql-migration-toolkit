# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL-source assessment rules (``assessor_postgres``) -- Phase 1.

The source-neutral, target-DSQL STRUCTURAL rule set. Rules that inspect a MySQL TYPE
STRING or a MySQL feature are excluded so they never misfire on a PostgreSQL source, as
are the MySQL-binlog CDC-cascade rule and the MySQL-function view rule. Rules whose
condition is a STRUCTURAL field the PG inventory populates are kept -- with PG-worded
variants where the MySQL prose would have been wrong.
"""

from dsql_migrator.core.assessor import CompatibilityAssessor, default_rules
from dsql_migrator.core.assessor_postgres import default_rules as pg_default_rules
from dsql_migrator.core.models import (
    ColumnDef,
    SourceInventory,
    SourceType,
    TableDef,
)

_EXPECTED_PG_RULE_IDS = {
    "PG_UNSUPPORTED_TYPE",  # PG-specific: DSQL-unsupported column types
    "PG_UNSUPPORTED_RELATION",  # PG-specific: materialized views / foreign tables
    "FK_PRESERVED",
    "CHECK_CONSTRAINT_DROPPED",
    "TRIGGER_UNSUPPORTED",
    "PROC_PLPGSQL",
    "EVENT_UNSUPPORTED",
    "NO_PRIMARY_KEY",
    "PARTITIONED_TABLE",
    "TOO_MANY_COLUMNS",
    "TOO_MANY_INDEXES",
    "TOO_MANY_KEY_COLUMNS",
    # Conditions read from STRUCTURAL fields the PG inventory populates, so they are
    # engine-neutral and belong here; only the MySQL PROSE had to be replaced (the first
    # two by PG-worded variants carrying the same rule id).
    "GENERATED_COLUMN",
    "AUTO_INCREMENT",
    "NUMERIC_PRECISION",
    # Same id, but it needed a PG-worded rule AND a PG type set: the 1 MiB cap is a
    # TARGET limit that applies to PG text/bytea/json/jsonb identically, while the shared
    # rule matches MySQL type NAMES -- so registering that one would have found nothing.
    "OVERSIZED_LOB",
    # PG-only: a non-PK serial/identity column. Schema Conversion warned about it while
    # Evaluation said nothing, the same contradiction the identity-KEY rule closed.
    "NON_KEY_SEQUENCE",
}

_EXCLUDED_MYSQL_RULE_IDS = {
    "CI_COLLATION",
    "SPATIAL_TYPE",
    "ENUM_SET_TYPE",
    "TINYINT_BOOLEAN",
    "BIT_TYPE",
    "YEAR_TYPE",
    "ON_UPDATE_TIMESTAMP",
    "UNSUPPORTED_INDEX_TYPE",
    "FK_CASCADE_CDC_GAP",  # MySQL-binlog framed
    "VIEW_UNSUPPORTED_SQL",  # MySQL app-query linter
}

# The MySQL classes whose rule id a PG source now also reports, but NEVER via the MySQL
# class itself: their text names MySQL features the source does not have.
_MYSQL_ONLY_RULE_CLASSES = {
    "GeneratedColumnRule",
    "AutoIncrementRule",
    "OversizedLobRule",
}


def test_pg_default_rules_are_exactly_the_structural_source_neutral_set() -> None:
    ids = [r.rule_id for r in pg_default_rules()]
    assert set(ids) == _EXPECTED_PG_RULE_IDS
    assert len(ids) == len(set(ids))  # no duplicates
    for excluded in _EXCLUDED_MYSQL_RULE_IDS:
        assert excluded not in ids
    # The two shared ids must come from the PG-worded variants, never from the MySQL
    # class: reusing the MySQL class would put "AUTO_INCREMENT column" / "MySQL
    # generated columns" in front of a PostgreSQL operator.
    names = {type(r).__name__ for r in pg_default_rules()}
    assert not (names & _MYSQL_ONLY_RULE_CLASSES)
    assert {
        "PgGeneratedColumnRule",
        "PgIdentityKeyRule",
        "PgOversizedLobRule",
    } <= names


def test_assessor_default_rules_postgres_delegates_to_the_pg_module() -> None:
    assert [type(r).__name__ for r in default_rules(SourceType.POSTGRES)] == [
        type(r).__name__ for r in pg_default_rules()
    ]


def test_compatibility_assessor_uses_pg_rules_for_a_pg_source() -> None:
    a = CompatibilityAssessor(source_type=SourceType.POSTGRES)
    ids = {r.rule_id for r in a._rules}  # white-box: the selected rule set
    assert ids == _EXPECTED_PG_RULE_IDS
    # A MySQL default assessor still uses the full MySQL set (unchanged).
    m = CompatibilityAssessor()
    assert "ENUM_SET_TYPE" in {r.rule_id for r in m._rules}


def test_pg_source_flags_missing_pk_but_not_a_mysql_enum_lookalike() -> None:
    # A PostgreSQL table with no PK must be flagged NO_PRIMARY_KEY. A PostgreSQL enum
    # (DSQL-unsupported) IS flagged -- but by the PG rule PG_UNSUPPORTED_TYPE, NEVER by the
    # MySQL ENUM_SET_TYPE rule (which is not run for a PG source).
    table = TableDef(
        name="shop.widgets",
        columns=[ColumnDef(name="kind", mysql_type="mood")],  # a user-defined enum type
        primary_key=[],
    )
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=[table])
    )
    # Collect the primary rule_id AND every concern's rule_id (an object aggregates all).
    rule_ids: set[str] = set()
    for item in report.items:
        rule_ids.add(item.rule_id)
        rule_ids.update(c.rule_id for c in (item.concerns or []))
    assert "NO_PRIMARY_KEY" in rule_ids
    assert "PG_UNSUPPORTED_TYPE" in rule_ids  # the enum is flagged by the PG type rule
    assert "ENUM_SET_TYPE" not in rule_ids  # ... never by the MySQL ENUM rule


def test_pg_unsupported_type_rule_flags_dsql_unsupported_columns() -> None:
    # Evaluation-time mirror of the Schema Conversion unsupported-type warning: only the
    # DSQL-unsupported columns are named, classified UNSUPPORTED, with a remodel target.
    from dsql_migrator.core.assessor_postgres import UnsupportedPostgresTypeRule
    from dsql_migrator.core.models import Classification, EffortLevel

    table = TableDef(
        name="app.events",
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),     # supported
            ColumnDef(name="tags", mysql_type="text[]"),   # array -> unsupported
            ColumnDef(name="ip", mysql_type="inet"),       # network -> unsupported
            ColumnDef(name="note", mysql_type="text"),     # supported
        ],
        primary_key=["id"],
    )
    findings = UnsupportedPostgresTypeRule().evaluate(SourceInventory(tables=[table]))
    assert len(findings) == 1
    f = findings[0]
    assert f.rule_id == "PG_UNSUPPORTED_TYPE"
    assert f.classification is Classification.UNSUPPORTED
    assert f.effort is EffortLevel.MEDIUM
    assert "tags" in f.risk and "ip" in f.risk  # the unsupported columns are named
    assert "note" not in f.risk  # a supported column is not
    assert "jsonb" in f.recommendation  # names the faithful remodel targets


def test_pg_unsupported_type_rule_ignores_fully_supported_tables() -> None:
    from dsql_migrator.core.assessor_postgres import UnsupportedPostgresTypeRule

    table = TableDef(
        name="app.ok",
        columns=[
            ColumnDef(name="id", mysql_type="uuid"),
            ColumnDef(name="j", mysql_type="jsonb"),
            ColumnDef(name="t", mysql_type="timestamp with time zone"),
        ],
        primary_key=["id"],
    )
    assert UnsupportedPostgresTypeRule().evaluate(SourceInventory(tables=[table])) == []


def test_multi_schema_pg_source_is_not_flagged_as_multiple_databases() -> None:
    # A PostgreSQL source is schema-qualified (schema.table); its multiple schemas are
    # one database that DSQL supports and the tool migrates schema-qualified. The
    # MySQL-only MULTIPLE_DATABASES finding must NOT fire (it did, with MySQL
    # consolidation guidance, for essentially every multi-schema PG source).
    from dsql_migrator.core.assessor import default_inventory_rules

    ids = {rule.__name__ for rule in default_inventory_rules(SourceType.POSTGRES)}
    assert "check_multiple_source_databases" not in ids
    assert "check_table_count" in ids  # the table-count limit still applies

    inventory = SourceInventory(
        tables=[
            TableDef(name="public.users", columns=[ColumnDef(name="id", mysql_type="bigint")], primary_key=["id"]),
            TableDef(name="sales.orders", columns=[ColumnDef(name="id", mysql_type="bigint")], primary_key=["id"]),
        ]
    )
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(inventory)
    rule_ids = {item.rule_id for item in report.items}
    assert "MULTIPLE_DATABASES" not in rule_ids
    # A MySQL source with the same two-schema shape DOES still get the finding.
    mysql_report = CompatibilityAssessor().assess(inventory)
    assert "MULTIPLE_DATABASES" in {item.rule_id for item in mysql_report.items}


def test_pg_evaluation_flags_collected_triggers_and_routines() -> None:
    # Fix #2: once enrich populates inventory.triggers/routines for a PG source, the PG
    # rule set (which wires TriggerRule/ProcedureRule) must flag them UNSUPPORTED -- they
    # were always empty before, so these rules never fired for PostgreSQL.
    from dsql_migrator.core.models import Classification, ObjectRef, ObjectType

    inventory = SourceInventory(
        tables=[
            TableDef(name="shop.orders", columns=[ColumnDef(name="id", mysql_type="bigint")], primary_key=["id"])
        ],
        triggers=[ObjectRef(name="shop.audit_ins", object_type=ObjectType.TRIGGER)],
        routines=[
            ObjectRef(name="shop.calc_total", object_type=ObjectType.FUNCTION),
            ObjectRef(name="shop.do_thing", object_type=ObjectType.PROCEDURE),
        ],
    )
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(inventory)
    by_name = {item.object_name: item for item in report.items}
    assert by_name["shop.audit_ins"].rule_id == "TRIGGER_UNSUPPORTED"
    assert by_name["shop.audit_ins"].classification is Classification.UNSUPPORTED
    assert by_name["shop.calc_total"].rule_id == "PROC_PLPGSQL"
    assert by_name["shop.calc_total"].classification is Classification.UNSUPPORTED
    assert by_name["shop.do_thing"].classification is Classification.UNSUPPORTED


def test_pg_evaluation_flags_materialized_views_and_foreign_tables() -> None:
    # Fix #4: matviews / foreign tables are carried as ViewDefs flagged unsupported_kind.
    # The PG rule set must flag them UNSUPPORTED (DSQL has neither) while leaving a plain
    # view unflagged (AUTO for the PG rule set, which has no view-content rule).
    from dsql_migrator.core.models import Classification, ViewDef

    inventory = SourceInventory(
        tables=[
            TableDef(name="shop.orders", columns=[ColumnDef(name="id", mysql_type="bigint")], primary_key=["id"])
        ],
        views=[
            ViewDef(name="shop.daily_totals", unsupported_kind="materialized view"),
            ViewDef(name="shop.ext_orders", unsupported_kind="foreign table"),
            ViewDef(name="shop.plain", definition="SELECT 1"),
        ],
    )
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(inventory)
    by_name = {item.object_name: item for item in report.items}
    assert by_name["shop.daily_totals"].rule_id == "PG_UNSUPPORTED_RELATION"
    assert by_name["shop.daily_totals"].classification is Classification.UNSUPPORTED
    assert by_name["shop.ext_orders"].rule_id == "PG_UNSUPPORTED_RELATION"
    assert by_name["shop.ext_orders"].classification is Classification.UNSUPPORTED
    assert by_name["shop.plain"].classification is Classification.AUTO


def test_html_report_title_reflects_the_postgres_source_engine() -> None:
    from dsql_migrator.core.assessor import render_html_report

    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=[TableDef(name="public.t", columns=[ColumnDef(name="id", mysql_type="bigint")], primary_key=["id"])])
    )
    pg_html = render_html_report(report, source_type=SourceType.POSTGRES)
    assert "PostgreSQL to Aurora DSQL Compatibility Assessment" in pg_html
    assert "MySQL to Aurora DSQL" not in pg_html
    # Default (MySQL) keeps the original title.
    assert "MySQL to Aurora DSQL Compatibility Assessment" in render_html_report(report)


def test_pg_generated_column_rule_uses_pg_wording_not_the_mysql_text() -> None:
    # The condition is the engine-neutral `generated` flag, which PG introspection sets
    # from attgenerated -- previously the whole rule was skipped for PG because the MySQL
    # rule's PROSE was wrong, so Evaluation said "compatible" about a column Schema
    # Conversion warns is a permanent drift risk.
    from dsql_migrator.core.assessor import GeneratedColumnRule
    from dsql_migrator.core.assessor_postgres import PgGeneratedColumnRule
    from dsql_migrator.core.models import Classification

    table = TableDef(
        name="shop.orders",
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),
            ColumnDef(name="total", mysql_type="numeric(12,2)", generated=True),
        ],
        primary_key=["id"],
    )
    inventory = SourceInventory(tables=[table])
    findings = PgGeneratedColumnRule().evaluate(inventory)
    assert len(findings) == 1
    f = findings[0]
    assert f.rule_id == "GENERATED_COLUMN"
    assert f.classification is Classification.MANUAL
    assert "total" in f.risk
    assert "PostgreSQL generated" in f.risk
    # Neither the MySQL naming nor its "recreate as a PostgreSQL GENERATED column"
    # advice, which is meaningless when the source already is PostgreSQL.
    assert "MySQL" not in f.risk
    assert "GENERATED column" not in f.recommendation
    # The MySQL rule fires on the same condition -- which is exactly why its text must
    # not be the one a PG operator sees.
    assert "MySQL generated" in GeneratedColumnRule().evaluate(inventory)[0].risk


def test_pg_identity_key_rule_never_says_auto_increment() -> None:
    from dsql_migrator.core.assessor_postgres import PgIdentityKeyRule
    from dsql_migrator.core.models import ConversionNoteKind

    table = TableDef(
        name="shop.orders",
        columns=[ColumnDef(name="id", mysql_type="bigint")],
        primary_key=["id"],
        auto_increment_column="id",
    )
    findings = PgIdentityKeyRule().evaluate(SourceInventory(tables=[table]))
    assert len(findings) == 1
    f = findings[0]
    assert f.rule_id == "AUTO_INCREMENT"  # same id, PG wording
    assert "AUTO_INCREMENT" not in f.risk
    assert "serial / identity" in f.risk
    # Throughput advice, not a loss -- same calibration as the MySQL rule.
    assert f.note_kind is ConversionNoteKind.RECOMMENDATION


def test_pg_source_flags_an_over_precision_numeric_at_evaluation() -> None:
    from dsql_migrator.core.assessor import _MAX_NUMERIC_PRECISION

    # numeric(40,10) is WITHIN DSQL's documented maximum (precision 1000), so it is not
    # flagged at all. This test asserted the old 38 limit, which is exactly how a stale
    # quota kept a migratable column graded UNSUPPORTED. Use a precision beyond the real
    # documented maximum instead, anchored on the constant.
    # (was: numeric(40,10) exceeds DSQL's 38-digit maximum and Schema Conversion CLAMPS it, so
    # Evaluation -- the go/no-go artifact -- must not report the table as AUTO.
    table = TableDef(
        name="shop.items",
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),
            ColumnDef(
                name="storage_cost_usd",
                mysql_type=f"numeric({_MAX_NUMERIC_PRECISION + 1},10)",
            ),
        ],
        primary_key=["id"],
    )
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=[table])
    )
    rule_ids: set[str] = set()
    for item in report.items:
        rule_ids.add(item.rule_id)
        rule_ids.update(c.rule_id for c in (item.concerns or []))
    assert "NUMERIC_PRECISION" in rule_ids


def test_pg_oversized_lob_rule_covers_what_the_manual_documents() -> None:
    """The 1 MiB per-value cap is a TARGET limit, so it applies to PostgreSQL identically --
    but the shared rule matches MySQL type NAMES, so registering it for PG would have found
    nothing forever. The manual names `OVERSIZED_LOB` as the flag for exactly this, and
    names PostgreSQL explicitly, so a PG source reading AUTO left that signal empty."""
    from dsql_migrator.core.assessor_postgres import PgOversizedLobRule
    from dsql_migrator.core.models import Classification, EffortLevel

    table = TableDef(
        name="shop.media",
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),
            ColumnDef(name="content", mysql_type="bytea"),
            ColumnDef(name="body", mysql_type="text"),
            ColumnDef(name="doc", mysql_type="jsonb"),
            ColumnDef(name="meta", mysql_type="json"),
            ColumnDef(name="unbounded", mysql_type="character varying"),
            ColumnDef(name="code", mysql_type="character varying(50)"),  # bounded
            ColumnDef(name="qty", mysql_type="integer"),
        ],
        primary_key=["id"],
    )
    findings = PgOversizedLobRule().evaluate(SourceInventory(tables=[table]))
    # TWO findings: the grading is split by what the TYPE declares, so a bytea/json/jsonb
    # column is never averaged in with an idiomatic short string. Grading them together
    # went wrong in both directions -- one LOSS made 18 tables of ordinary `text` read as
    # per-table manual work, one RECOMMENDATION made a bytea holding 1,114,112 bytes read
    # "Ready" while MySQL's identical longblob read "Moderate effort".
    from dsql_migrator.core.models import ConversionNoteKind

    assert len(findings) == 2
    by_kind = {f.note_kind: f for f in findings}
    loss = by_kind[ConversionNoteKind.LOSS]
    advice = by_kind[ConversionNoteKind.RECOMMENDATION]
    for f in findings:
        assert f.rule_id == "OVERSIZED_LOB"
        assert f.classification is Classification.MANUAL
        assert f.effort is EffortLevel.MEDIUM
        # A LENGTH-BOUNDED varchar cannot exceed 1 MiB, and a plain integer is irrelevant.
        assert "code" not in f.risk
        assert "qty" not in f.risk
    # Deliberately-large types -> LOSS, matching MySQL's mediumblob/longblob grade.
    for named in ("content", "doc", "meta"):
        assert named in loss.risk, named
    # Merely-unbounded character types -> advice.
    for named in ("body", "unbounded"):
        assert named in advice.risk, named
    assert "content" not in advice.risk
    f = loss
    assert "qty" not in f.risk
    # PG-worded: no MySQL LOB/TEXT type names.
    assert "MySQL" not in f.risk


def test_pg_oversized_lob_rule_ignores_a_table_with_only_bounded_types() -> None:
    from dsql_migrator.core.assessor_postgres import PgOversizedLobRule

    table = TableDef(
        name="shop.ok",
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),
            ColumnDef(name="name", mysql_type="character varying(200)"),
            ColumnDef(name="price", mysql_type="numeric(12,2)"),
        ],
        primary_key=["id"],
    )
    assert PgOversizedLobRule().evaluate(SourceInventory(tables=[table])) == []


def test_json_and_jsonb_are_in_the_pg_oversized_set_not_exempt() -> None:
    """Pins the CORRECTED reasoning. An earlier comment excluded json/jsonb as "stored
    differently and not hit by the text 1 MiB cap the same way", justified by a compression
    measurement. The DSQL docs say the 1 MiB limit applies to bytea/text/json/jsonb, that
    text/varchar/bpchar are auto-compressed TOO (so compression cannot discriminate -- if it
    justified dropping json it would equally justify dropping text, emptying the set), and
    that for json/jsonb the limit applies to the COMPRESSED size, which relocates the cap
    rather than removing it. A documented quota outranks a probe."""
    from dsql_migrator.core.assessor import (
        _PG_OVERSIZED_LOB_BASES,
        is_pg_oversized_lob_type,
    )

    assert {"text", "bytea", "json", "jsonb"} <= _PG_OVERSIZED_LOB_BASES
    for spelling in ("text", "bytea", "json", "jsonb", "character varying", "varchar"):
        assert is_pg_oversized_lob_type(spelling), spelling
    # A length limit is what makes a column safe, not the type family.
    for spelling in ("character varying(50)", "varchar(10)", "integer", "numeric(12,2)"):
        assert not is_pg_oversized_lob_type(spelling), spelling


def test_pg_extension_finding_keeps_the_signal_the_object_filter_removes() -> None:
    """Filtering extension-owned objects out of the inventory is right -- they are not the
    user's to re-create -- but on its own it would hide a REAL incompatibility: Aurora DSQL
    provides no user extensions, so application SQL calling one must change. One item per
    extension carries that in a line or two instead of hundreds of per-object rows."""
    from dsql_migrator.core.assessor import (
        check_postgres_extensions,
        default_inventory_rules,
    )
    from dsql_migrator.core.models import Classification, EffortLevel

    assert check_postgres_extensions in default_inventory_rules(SourceType.POSTGRES)
    # MySQL has no extensions, so the rule is not in its list at all.
    assert check_postgres_extensions not in default_inventory_rules(SourceType.MYSQL)

    inventory = SourceInventory(
        tables=[
            TableDef(
                name="app.orders",
                columns=[ColumnDef(name="id", mysql_type="bigint")],
                primary_key=["id"],
            )
        ],
        extensions=["pgcrypto (public)", "postgis (gis)"],
    )
    items = check_postgres_extensions(inventory)
    assert [i.object_name for i in items] == ["pgcrypto (public)", "postgis (gis)"]
    for item in items:
        assert item.rule_id == "PG_EXTENSION_UNSUPPORTED"
        # MANUAL/MEDIUM, not UNSUPPORTED/SIGNIFICANT: the target is buildable, the app's
        # calls have to change. Rating it SIGNIFICANT would re-create the score distortion
        # the object filter just removed.
        assert item.classification is Classification.MANUAL
        assert item.effort is EffortLevel.MEDIUM
        assert "no user extensions" in item.risk
        # Says the objects were excluded, so a reader does not go looking for them.
        assert "excluded from this migration" in item.risk

    # No extensions -> no items (a PG source without extensions reads clean).
    assert check_postgres_extensions(SourceInventory(tables=[])) == []


def test_a_pg_source_with_an_extension_no_longer_reads_as_significant_effort() -> None:
    """End-to-end over the real assessor: the extension contributes ONE MANUAL item, not a
    per-object flood, so the effort rollup reflects the user's actual work."""
    inventory = SourceInventory(
        tables=[
            TableDef(
                name="app.orders",
                columns=[ColumnDef(name="id", mysql_type="bigint")],
                primary_key=["id"],
            )
        ],
        extensions=["pgcrypto (public)"],
    )
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(inventory)
    rule_ids = [item.rule_id for item in report.items]
    assert rule_ids.count("PG_EXTENSION_UNSUPPORTED") == 1
    assert "PROC_PLPGSQL" not in rule_ids  # no phantom routines


def test_pg_database_collation_finding_covers_what_the_column_capture_cannot() -> None:
    """S-1: the per-column capture ignores the collation named ``default`` -- which is not a
    collation but "this database's default" -- so the ORDINARY case was invisible: a stock
    en_US.utf8 source whose every text column inherits it, migrating onto a DSQL target that
    runs C. Measured on a live cluster: DSQL datcollate='C' gives 'Bob' < 'alice' TRUE and
    order A,B,a,b; en_US.utf8 gives FALSE and a,A,b,B."""
    from dsql_migrator.core.assessor import (
        check_postgres_database_collation,
        default_inventory_rules,
    )
    from dsql_migrator.core.models import Classification, EffortLevel

    assert check_postgres_database_collation in default_inventory_rules(
        SourceType.POSTGRES
    )
    assert check_postgres_database_collation not in default_inventory_rules(
        SourceType.MYSQL
    )

    items = check_postgres_database_collation(
        SourceInventory(tables=[], database_collation="en_US.utf8")
    )
    assert len(items) == 1
    item = items[0]
    assert item.rule_id == "PG_DATABASE_COLLATION"
    assert item.classification is Classification.MANUAL
    assert item.effort is EffortLevel.MEDIUM
    assert "en_US.utf8" in item.risk
    assert "ORDER BY" in item.risk
    # Must NOT overstate it: a deterministic collation change moves ordering and range
    # behaviour, not equality or UNIQUE.
    assert "Equality and UNIQUE enforcement are unchanged" in item.risk

    # C / POSIX is byte ordering -- what the target already does, so no item.
    for same in ("C", "POSIX", "c"):
        assert check_postgres_database_collation(
            SourceInventory(tables=[], database_collation=same)
        ) == []
    # Unreadable / MySQL -> no item.
    assert check_postgres_database_collation(SourceInventory(tables=[])) == []


def test_check_limited_text_enum_is_not_an_oversized_lob_risk() -> None:
    """Measured in a workshop Evaluation: `products.status`, `orders.status` and
    `product_media.media_type` -- each a text column a CHECK limits to 3-5 literals -- were
    reported as "no length limit ... can exceed 1 MiB", and the table's one GENUINE oversized
    column (a bytea holding 1,114,112 bytes) was listed at the same grade. The tool already
    reads those CHECKs in the SAME report, so this needs no new probe."""
    from dsql_migrator.core.assessor import (
        pg_columns_bounded_by_check,
        pg_oversized_lob_column_names,
    )
    from dsql_migrator.core.assessor_postgres import PgOversizedLobRule
    from dsql_migrator.core.models import CheckConstraintDef

    table = TableDef(
        name="ecommerce.product_media",
        columns=[
            ColumnDef(name="id", mysql_type="integer"),
            # PostgreSQL stores an IN-list as `= ANY (ARRAY[...::text])`, which is what the
            # tool captures -- so the detection has to parse, not pattern-match the source.
            ColumnDef(name="media_type", mysql_type="text"),
            ColumnDef(name="content", mysql_type="bytea"),
            ColumnDef(name="full_description", mysql_type="text"),
            ColumnDef(name="notes", mysql_type="text"),
        ],
        primary_key=["id"],
        check_constraints=[
            CheckConstraintDef(
                name="product_media_media_type_check",
                expression=(
                    "media_type = ANY (ARRAY['image'::text, 'video'::text, 'doc'::text])"
                ),
            ),
            # Mentions a column but bounds nothing -- must NOT exclude it.
            CheckConstraintDef(
                name="product_media_notes_check", expression="notes <> ''::text"
            ),
        ],
    )
    assert pg_columns_bounded_by_check(table) == {"media_type"}
    at_risk = pg_oversized_lob_column_names(table)
    assert "content" in at_risk  # the genuine one survives
    assert "full_description" in at_risk
    assert "notes" in at_risk  # `<> ''` bounds nothing
    assert "media_type" not in at_risk

    finding = PgOversizedLobRule().evaluate(SourceInventory(tables=[table]))[0]
    assert "content (bytea)" in finding.risk
    assert "media_type" not in finding.risk


def test_a_table_whose_only_text_column_is_check_limited_is_clean() -> None:
    """The workshop's `orders`: its only unbounded-typed columns were CHECK-limited enums, so
    the table should raise no oversized-LOB finding at all."""
    from dsql_migrator.core.assessor_postgres import PgOversizedLobRule
    from dsql_migrator.core.models import CheckConstraintDef

    table = TableDef(
        name="ecommerce.orders",
        columns=[
            ColumnDef(name="id", mysql_type="integer"),
            ColumnDef(name="status", mysql_type="text"),
        ],
        primary_key=["id"],
        check_constraints=[
            CheckConstraintDef(
                name="orders_status_check",
                expression=(
                    "status = ANY (ARRAY['pending'::text, 'paid'::text, "
                    "'shipped'::text, 'delivered'::text, 'cancelled'::text])"
                ),
            )
        ],
    )
    assert PgOversizedLobRule().evaluate(SourceInventory(tables=[table])) == []


def test_identity_finding_states_who_generates_the_key_not_only_throughput() -> None:
    """A workshop Evaluation listed 7 AUTO_INCREMENT recommendations as throughput advice
    only. Under the DEFAULT conversion the target column is a plain integer with no identity,
    so the APPLICATION must supply the value on every insert -- an app-code change. Schema
    Conversion says so ("IMPORTANT: Aurora DSQL will NOT auto-generate this key"); Evaluation,
    the go/no-go artifact, said "works as-is ... optional, for throughput only".
    Engine-independent: the MySQL rule had the same wording."""
    from dsql_migrator.core.assessor import AutoIncrementRule
    from dsql_migrator.core.assessor_postgres import PgIdentityKeyRule

    pg_table = TableDef(
        name="ecommerce.orders",
        columns=[ColumnDef(name="id", mysql_type="integer")],
        primary_key=["id"],
        auto_increment_column="id",
    )
    for rule in (PgIdentityKeyRule(), AutoIncrementRule()):
        finding = rule.evaluate(SourceInventory(tables=[pg_table]))[0]
        text = finding.risk + " " + finding.recommendation
        assert "will NOT generate it" in finding.risk, type(rule).__name__
        assert "APPLICATION must supply" in finding.risk, type(rule).__name__
        assert "Server-generated (IDENTITY)" in finding.recommendation
        # Still calibrated as advice, not a failure (the v0.1.151 correction): the
        # THROUGHPUT half stays explicitly optional, and the throughput mechanism is kept.
        assert "Optional, for throughput only" in finding.recommendation
        assert "primary-key order" in text
        assert "cause hot partitions" not in text.lower()


def test_or_and_not_checks_do_not_suppress_a_real_lob_risk() -> None:
    """Both verified against real PostgreSQL. Searching the whole parsed tree for an EQ node
    let two shapes hide a genuine risk: an OR means a row can satisfy the CHECK via the other
    branch (so the column may hold anything), and a NOT permits everything EXCEPT the listed
    values. Only a top-level AND-conjunct bounds a column."""
    from dsql_migrator.core.assessor import (
        pg_columns_bounded_by_check,
        pg_oversized_lob_column_names,
    )
    from dsql_migrator.core.models import CheckConstraintDef

    table = TableDef(
        name="app.t",
        columns=[
            ColumnDef(name="id", mysql_type="integer"),
            ColumnDef(name="status", mysql_type="text"),
            ColumnDef(name="other", mysql_type="text"),
            ColumnDef(name="bounded", mysql_type="text"),
        ],
        primary_key=["id"],
        check_constraints=[
            CheckConstraintDef(
                name="or_check",
                expression=(
                    "(status = ANY (ARRAY['a'::text, 'b'::text])) OR id IS NOT NULL"
                ),
            ),
            CheckConstraintDef(
                name="not_check",
                expression="NOT (other = ANY (ARRAY['x'::text, 'y'::text]))",
            ),
            # A genuine top-level conjunct still bounds its column, including inside an AND.
            CheckConstraintDef(
                name="and_check",
                expression=(
                    "id > 0 AND bounded = ANY (ARRAY['s'::text, 'm'::text])"
                ),
            ),
        ],
    )
    assert pg_columns_bounded_by_check(table) == {"bounded"}
    at_risk = pg_oversized_lob_column_names(table)
    assert "status" in at_risk  # the OR branch leaves it unbounded
    assert "other" in at_risk  # the negation permits everything else
    assert "bounded" not in at_risk


def test_a_not_valid_check_bounds_nothing() -> None:
    """A NOT VALID constraint was never checked against the rows ALREADY stored, so a
    multi-megabyte value written before it was added is still there -- and Full Load reads
    exactly those rows. Treating it as a bound hid the value that would fail to load."""
    from dsql_migrator.core.assessor import (
        pg_columns_bounded_by_check,
        pg_oversized_lob_column_names,
    )
    from dsql_migrator.core.models import CheckConstraintDef

    def _table(not_valid: bool) -> TableDef:
        return TableDef(
            name="app.t",
            columns=[
                ColumnDef(name="id", mysql_type="integer"),
                ColumnDef(name="blob_col", mysql_type="text"),
            ],
            primary_key=["id"],
            check_constraints=[
                CheckConstraintDef(
                    name="c",
                    expression="blob_col = ANY (ARRAY['s'::text, 'm'::text])",
                    not_valid=not_valid,
                )
            ],
        )

    assert pg_columns_bounded_by_check(_table(not_valid=False)) == {"blob_col"}
    assert pg_columns_bounded_by_check(_table(not_valid=True)) == set()
    assert "blob_col" in pg_oversized_lob_column_names(_table(not_valid=True))


def test_non_key_sequence_rule_matches_what_schema_conversion_warns() -> None:
    """Evaluation said nothing about a NON-primary-key serial/identity column while Schema
    Conversion warned about it -- the same contradiction the identity-KEY rule closed, one
    column over. Graded a LOSS, unlike the key rule: for the key the operator gets a CHOICE
    at Schema Conversion, for a non-key column the generation is simply gone."""
    from dsql_migrator.core.assessor_postgres import PgNonKeySequenceRule
    from dsql_migrator.core.models import Classification, ConversionNoteKind

    table = TableDef(
        name="shop.invoices",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False, identity=True),
            ColumnDef(name="invoice_no", mysql_type="integer", nullable=False, identity=True),
            ColumnDef(
                name="legacy_no", mysql_type="bigint", nullable=False,
                default="nextval('invoices_legacy_no_seq'::regclass)",
            ),
            ColumnDef(name="note", mysql_type="text"),
        ],
        primary_key=["id"],
        auto_increment_column="id",
    )
    findings = PgNonKeySequenceRule().evaluate(SourceInventory(tables=[table]))
    assert len(findings) == 1
    f = findings[0]
    assert f.rule_id == "NON_KEY_SEQUENCE"
    assert f.classification is Classification.MANUAL
    assert f.note_kind is ConversionNoteKind.LOSS
    # BOTH spellings, and never the primary key.
    assert "invoice_no" in f.risk and "legacy_no" in f.risk
    assert "id" not in f.risk.replace("identity", "").replace("IDENTITY", "")
    # The fixture's columns are all NOT NULL -- which is what `serial` and
    # `GENERATED AS IDENTITY` actually produce -- so the outcome is a REJECTED insert, not a
    # NULL. This test previously asserted `"writes NULL" in f.risk` on exactly these
    # NOT NULL columns, i.e. it pinned the wrong outcome (the same shape as the v0.1.438
    # `lc_numeric` regression, where the test asserted the bug). Verified against a live
    # PostgreSQL catalog: the tool's own DDL emits `INT NOT NULL` with no default and an
    # omitting INSERT fails with SQLSTATE 23502, writing zero rows.
    assert "REJECTED on Aurora DSQL" in f.risk, f.risk
    assert "23502" in f.risk, f.risk
    assert "writes NULL" not in f.risk, f.risk
    # The mechanism is named per column, so the operator can see which spelling it is.
    assert "invoice_no (GENERATED AS IDENTITY)" in f.risk, f.risk
    assert "legacy_no (serial)" in f.risk, f.risk
    # The remedy must not read as deferrable for a NOT NULL column.
    assert "BEFORE cut over" in f.recommendation, f.recommendation

    # A NULLABLE sequence-defaulted column (a hand-attached DEFAULT nextval(...) without
    # NOT NULL) is the case where NULL really is the outcome -- both must be expressible,
    # and in one finding they must stay told apart.
    mixed = TableDef(
        name="shop.mixed",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False, identity=True),
            ColumnDef(name="strict_no", mysql_type="integer", nullable=False, identity=True),
            ColumnDef(
                name="opt_no", mysql_type="bigint", nullable=True,
                default="nextval('mixed_opt_no_seq'::regclass)",
            ),
        ],
        primary_key=["id"],
        auto_increment_column="id",
    )
    m = PgNonKeySequenceRule().evaluate(SourceInventory(tables=[mixed]))[0]
    assert "REJECTED on Aurora DSQL" in m.risk, m.risk
    assert "writes NULL instead of the next number" in m.risk, m.risk
    assert "strict_no" in m.risk.split("nullable, so")[0], "the NOT NULL one is grouped first"
    assert "2 column(s)" in m.risk, m.risk

    # A table with no non-key sequence column raises nothing.
    assert PgNonKeySequenceRule().evaluate(
        SourceInventory(tables=[TableDef(
            name="t", columns=[ColumnDef(name="id", mysql_type="bigint", identity=True)],
            primary_key=["id"], auto_increment_column="id")])
    ) == []


# ---------------------------------------------------------------------------
# The unsupported-type finding must say WHICH KIND each identifier is
# ---------------------------------------------------------------------------


def _orders_with_enum():
    return TableDef(
        name="ecommerce.orders",
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),
            # A user-defined enum's type name is SCHEMA-QUALIFIED, which is exactly the
            # shape this tool uses for tables.
            ColumnDef(name="status", mysql_type="ecommerce.order_status"),
        ],
        primary_key=["id"],
    )


def test_a_qualified_type_name_is_not_presented_as_a_table_reference() -> None:
    """The reported confusion: "why is another table mentioned in orders' finding?"

    ``status (ecommerce.order_status)`` read as ``column (schema.table)`` because that is
    the form the tool uses for tables everywhere else -- so an operator concluded a
    different TABLE was involved. Naming the kind is what removes the ambiguity;
    parentheses alone cannot.
    """
    from dsql_migrator.core.assessor_postgres import UnsupportedPostgresTypeRule

    f = UnsupportedPostgresTypeRule().evaluate(
        SourceInventory(tables=[_orders_with_enum()])
    )[0]
    assert 'column "status" of type ecommerce.order_status' in f.risk, f.risk
    # The bare parenthesised form is what was misread, so it must be gone.
    assert "status (ecommerce.order_status)" not in f.risk, f.risk
    # Only the offending column is counted.
    assert "1 column(s)" in f.risk, f.risk


def test_the_recommendation_names_the_target_for_THIS_type() -> None:
    """The per-type reason was computed and thrown away for a generic catalogue.

    The operator was handed eight mappings and left to match them against their own
    columns, while Schema Conversion -- the same function, the same column -- said exactly
    what to do. Two steps, one column, different answers.
    """
    from dsql_migrator.core.assessor_postgres import UnsupportedPostgresTypeRule
    from dsql_migrator.core.converter_postgres import unsupported_dsql_reason

    table = TableDef(
        name="ecommerce.orders",
        columns=[
            ColumnDef(name="status", mysql_type="ecommerce.order_status"),
            ColumnDef(name="tags", mysql_type="text[]"),
            ColumnDef(name="price", mysql_type="money"),
        ],
        primary_key=["status"],
    )
    f = UnsupportedPostgresTypeRule().evaluate(SourceInventory(tables=[table]))[0]
    # Each column's own reason, verbatim from the single source of truth -- so Evaluation
    # and Schema Conversion cannot drift.
    for col, typ in (
        ("status", "ecommerce.order_status"),
        ("tags", "text[]"),
        ("price", "money"),
    ):
        assert f"{col}: {unsupported_dsql_reason(typ)}" in f.recommendation, col
    # money -> numeric specifically, not a list the operator has to search.
    assert "numeric" in f.recommendation
    # The old generic catalogue enumerated every mapping regardless of the columns; a
    # table with no xml column must not be told about xml.
    assert "xml" not in f.recommendation, f.recommendation


def test_a_very_wide_table_cannot_produce_an_unbounded_message() -> None:
    """One finding is rendered into a UI card, a text report and an HTML report."""
    from dsql_migrator.core.assessor_postgres import (
        _MAX_DETAILED_COLUMNS,
        UnsupportedPostgresTypeRule,
    )

    table = TableDef(
        name="app.wide",
        columns=[ColumnDef(name=f"c{i}", mysql_type="money") for i in range(40)],
        primary_key=["c0"],
    )
    f = UnsupportedPostgresTypeRule().evaluate(SourceInventory(tables=[table]))[0]
    assert "40 column(s)" in f.risk, "the count must stay truthful"
    # Bounded per-column reasons, and the remainder is NAMED, not silently dropped.
    assert f.recommendation.count("Aurora DSQL does not support") == _MAX_DETAILED_COLUMNS
    assert f"{40 - _MAX_DETAILED_COLUMNS} more column(s)" in f.recommendation
    assert "c39" in f.recommendation, "the summarised columns are still named"


def test_the_oversized_lob_finding_points_at_the_check_that_answers_it() -> None:
    """It used to say "Check the largest value in each column" with no way to do so.

    Nothing in the tool could measure a value's size -- pg_column_size / octet_length
    appear nowhere -- so the finding's own instruction was homework. It now names the
    prerequisite check that probes it.
    """
    from dsql_migrator.core.assessor_postgres import PgOversizedLobRule

    table = TableDef(
        name="ecommerce.product_media",
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),
            ColumnDef(name="content", mysql_type="bytea"),
        ],
        primary_key=["id"],
    )
    findings = PgOversizedLobRule().evaluate(SourceInventory(tables=[table]))
    assert findings, "the oversized-LOB rule no longer fires for a bytea column"
    joined = " ".join(f.recommendation for f in findings)
    assert "prerequisite checks" in joined, joined
    assert "ALREADY exceeds 1 MiB" in joined, joined
