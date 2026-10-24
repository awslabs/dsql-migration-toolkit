# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G188 (FX-3): a MySQL view that reads another view types that view's columns.

The converter typed a view's columns from the tables it reads only (``table_type_index``):
a column of another view was unknown. G157's time zone rewrites need the kind, so under a
source zone that is not UTC a view over ``v_base AS SELECT id, ts FROM ev`` (ts a TIMESTAMP)
compared ``v_base.ts`` with the zone's wall clock -- PostgreSQL then reads that wall clock in
the SESSION's zone, UTC on Aurora DSQL -- and ``ts > NOW() - INTERVAL 1 DAY`` lost the rows of
the 9 hours before the boundary (v0.1.574 wrote LOCALTIMESTAMP there, which is right in any
session); ``HOUR(ts)`` and ``DATE(ts) = CURDATE()`` read UTC. The manual promised the rows
MySQL returned whatever the session's zone.

Now ``mysql_type_index`` adds each view another view reads, typed as its own (cached)
conversion types it -- a column it passes on as that column, a computed one by what it
computes -- after the views it reads, for Schema Conversion and Evaluation alike. Live: MySQL
8.0.46 in Asia/Seoul against PostgreSQL 17.11 in a UTC session (commit message); doubles here.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dsql_migrator.core.assessor import MySqlViewConversionRule
from dsql_migrator.core.converter import SchemaConverter, mysql_type_index
from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef, ViewDef

_CLOCK = "DATE_TRUNC('second', ( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ))"
_DAY_AGO_INSTANT = f"( ( {_CLOCK} - INTERVAL '1 DAY' ) AT TIME ZONE 'Asia/Seoul' )"
_DAY_AGO_WALL = f"( {_CLOCK} - INTERVAL '1 DAY' )"
_SEOUL = {"time_zone": "Asia/Seoul", "system_time_zone": "UTC"}


def _ev() -> TableDef:
    return TableDef(
        name="tzv.ev",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="ts", mysql_type="timestamp"),
            ColumnDef(name="dt", mysql_type="datetime"),
            ColumnDef(name="b", mysql_type="blob"),
        ],
        primary_key=["id"],
    )


def _view(name: str, body: str, columns: str = "") -> ViewDef:
    """As SHOW CREATE VIEW stores it in single-database mode (own tables bare)."""
    return ViewDef(
        name=f"tzv.{name}",
        definition=(
            "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW "
            f"`{name}`{columns} AS {body}"
        ),
    )


_BASE = _view(
    "v_base",
    "select `ev`.`id` AS `id`,`ev`.`ts` AS `ts`,`ev`.`dt` AS `dt`,"
    "(`ev`.`ts` + interval 1 hour) AS `tsp`,`ev`.`b` AS `b` from `ev`",
)
_OVER = {
    "v_recent": "select `v_base`.`id` AS `id` from `v_base` where (`v_base`.`ts` > (now() - interval 1 day))",
    "v_recent_dt": "select `v_base`.`id` AS `id` from `v_base` where (`v_base`.`dt` > (now() - interval 1 day))",
    "v_tsp": "select `v_base`.`id` AS `id` from `v_base` where (`v_base`.`tsp` > (now() - interval 1 day))",
    "v_hour": "select `v_base`.`id` AS `id`,hour(`v_base`.`ts`) AS `h` from `v_base`",
    "v_today": "select `v_base`.`id` AS `id` from `v_base` where (cast(`v_base`.`ts` as date) = curdate())",
    "v_join": "select `b`.`id` AS `id` from (`v_base` `b` join `ev` `e` on((`e`.`id` = `b`.`id`))) "
              "where (`b`.`ts` > (now() - interval 1 day))",
}


def _converted(views: "list[ViewDef]") -> "dict[str, str]":
    inventory = SourceInventory(tables=[_ev()], views=views, server_settings=_SEOUL)
    result = SchemaConverter(source_type=SourceType.MYSQL).convert(inventory)
    return {v.view: " ".join(v.target_ddl.split()) for v in result.views}


def test_a_timestamp_read_through_another_view_is_compared_as_the_instant() -> None:
    ddl = _converted([_BASE, *(_view(name, body) for name, body in _OVER.items())])
    assert f'( "v_base"."ts" > {_DAY_AGO_INSTANT} )' in ddl["tzv.v_recent"]
    assert f'( "b"."ts" > {_DAY_AGO_INSTANT} )' in ddl["tzv.v_join"]
    # A value the other view computes from one is typed by what it computes.
    assert f'( "v_base"."tsp" > {_DAY_AGO_INSTANT} )' in ddl["tzv.v_tsp"]
    # A DATETIME through the view is the zone's wall clock, compared as such.
    assert f'( "v_base"."dt" > {_DAY_AGO_WALL} )' in ddl["tzv.v_recent_dt"]
    # Its date and hour are read in the zone, as over the table.
    assert "EXTRACT(HOUR FROM ( \"v_base\".\"ts\" AT TIME ZONE 'Asia/Seoul' ))" in ddl["tzv.v_hour"]
    assert "CAST(( \"v_base\".\"ts\" AT TIME ZONE 'Asia/Seoul' ) AS DATE)" in ddl["tzv.v_today"]


def test_a_chain_is_typed_in_dependency_order_whatever_the_inventory_order() -> None:
    """v_top reads v_mid, which reads v_base and names its columns (`mid_ts`); listed
    top-first."""
    mid = _view("v_mid", "select `v_base`.`id` AS `id`,`v_base`.`ts` AS `ts` from `v_base`",
                columns=" (`mid_id`,`mid_ts`)")
    top = _view("v_top", "select `v_mid`.`mid_id` AS `id` from `v_mid` where (`v_mid`.`mid_ts` > (now() - interval 1 day))")
    ddl = _converted([top, mid, _BASE])
    assert f'( "v_mid"."mid_ts" > {_DAY_AGO_INSTANT} )' in ddl["tzv.v_top"]
    index = mysql_type_index([_ev()], [top, mid, _BASE])
    assert dict(index["tzv.v_mid"][1]) == {"mid_id": "int", "mid_ts": "timestamp"}
    assert dict(index["tzv.v_base"][1])["tsp"].startswith("timestamp")
    # A view no view reads gets no entry; nor does one whose body does not convert.
    assert "tzv.v_top" not in index
    broken = _view("v_broken", "select from where")
    reader = _view("v_reader", "select `v_broken`.`x` AS `x` from `v_broken`")
    assert "tzv.v_broken" not in mysql_type_index([_ev()], [broken, reader])


def test_a_quotient_another_view_passes_on_is_shown_at_its_scale_where_mysql_merges_it() -> None:
    """MySQL merges v_q into the query that reads it, so CONCAT shows the quotient at its
    scale (0.3333, live on 8.0.46; untyped it read 0.333333333); v_m (GROUP BY) is
    materialized, its quotient already stored at that scale."""
    table = TableDef(
        name="qv.t",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="a", mysql_type="int"), ColumnDef(name="b", mysql_type="int")],
        primary_key=["id"],
    )
    views = [
        ViewDef(name="qv.v_q", definition="select `t`.`id` AS `id`,(`t`.`a` / `t`.`b`) AS `q` from `t`"),
        ViewDef(name="qv.v_m", definition="select `t`.`id` AS `id`,(sum(`t`.`a`) / sum(`t`.`b`)) AS `q` "
                                          "from `t` group by `t`.`id`"),
        ViewDef(name="qv.out_q", definition="select concat(`v`.`q`,'') AS `s` from `v_q` `v`"),
        ViewDef(name="qv.out_m", definition="select concat(`v`.`q`,'') AS `s` from `v_m` `v`"),
    ]
    result = SchemaConverter(source_type=SourceType.MYSQL).convert(SourceInventory(tables=[table], views=views))
    ddl = {v.view: " ".join(v.target_ddl.split()) for v in result.views}
    assert 'CAST(ROUND("v"."q", 4) AS TEXT)' in ddl["qv.out_q"]
    assert 'CAST("v"."q" AS TEXT)' in ddl["qv.out_m"] and "ROUND(" not in ddl["qv.out_m"]


def test_evaluation_types_a_view_over_a_view_as_schema_conversion_does() -> None:
    """LENGTH of a blob another view passes on is its bytes in both steps; untyped, both
    noted it (and wrote the length of its hex text)."""
    lengths = _view("v_len", "select length(`v_base`.`b`) AS `l` from `v_base`")
    ddl = _converted([_BASE, lengths])
    assert 'OCTET_LENGTH("v_base"."b")' in ddl["tzv.v_len"]
    inventory = SourceInventory(tables=[_ev()], views=[_BASE, lengths], server_settings=_SEOUL)
    assert MySqlViewConversionRule().evaluate(inventory) == []


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"
_CLAIMS = {
    "en": ("also when `ts` is read through another view",
           "another view's column by what that view's body selects",
           "a column another view, a derived table or a CTE computes from what cannot be typed"),
    "ko": ("(`ts`를 다른 뷰를 거쳐 읽어도 마찬가지입니다)",
           "다른 뷰의 컬럼은 그 뷰의 본문이 고르는 것으로",
           "뷰나 파생 테이블, CTE가 타입을 알 수 없는 값으로 계산하는 컬럼"),
    "ja": ("(`ts` を別のビュー経由で読む場合も同じです)",
           "別のビューの列はそのビューの本体が選ぶもので",
           "別のビューや派生テーブル、CTE が型のわからない値から計算する列"),
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_another_views_columns_are_typed(language: str) -> None:
    text = " ".join(
        (_MANUAL / language / "02-evaluation-and-schema-conversion.md").read_text(encoding="utf-8").split()
    )
    for claim in _CLAIMS[language]:
        assert claim in text, (language, claim)
