# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CDC captures exactly the selected tables, letter case included, on a stack whose first
Start CDC runs on this version (ExactTableIncludeList).

Debezium compiles each table.include.list entry case-insensitively and matches it against the
whole table name (the bundled debezium-core 2.7.4: Selectors / Strings.setOfRegex with
CASE_INSENSITIVE, an anchored match), so capturing shop.users also captured an unselected
shop.Users: on MySQL with lower_case_table_names=0, also SHOP.users in another database; on
PostgreSQL, a quoted mixed-case name, where the connector's filtered publication (the routes
that snapshot) took the table too. Its changes went to a Kafka topic of their own that the
sink never reads.

The cdc-stack template prefixes each entry with "(?-i)" when ExactTableIncludeList is "true",
and Start CDC sends that only to a stack whose parameters show no Start yet: a started or
stopped pipeline's capture never changes, and a stack started before the parameter existed
resolves to its "false" default -- the earlier connector configuration, byte-identical.
Doubles only: no AWS, no source, no Kafka.
"""

from __future__ import annotations

import pathlib
import re

import pytest

from dsql_migrator.core.cdc import CDC_EXACT_TABLE_INCLUDE_PARAM, cdc_stack_params_started
from dsql_migrator.core.cdc_deployer import run_cdc_start

from tests.test_cdc_deploy_stages import (
    SINK,
    SRC,
    STACK,
    _FakeDeployer,
    _FakeHandle,
    _connector_overrides,
    _start_params,
    _watermark,
)

yaml = pytest.importorskip("yaml")

_TEMPLATE = (
    pathlib.Path(__file__).resolve().parents[1] / "deploy" / "cdc-stack" / "cdc-stack.yaml"
)
_SOURCES = ("DebeziumSourceConnector", "PostgresSourceConnector")
_LEGACY = {"Fn::Join": [",", {"Ref": "TableIncludeList"}]}
_EXACT = {"Fn::Join": ["", ["(?-i)", {"Fn::Join": [",(?-i)", {"Ref": "TableIncludeList"}]}]]}
_LOG = (
    "The source connector captures exactly the selected tables, letter case included "
    "(ExactTableIncludeList): this stack has not been started before."
)


def _template() -> dict:
    # Long-form intrinsics only (Fn::If, Fn::Join, Ref), so the safe loader reads it.
    return yaml.safe_load(_TEMPLATE.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# 1. The template: the switch and the expression in both source connectors
# ---------------------------------------------------------------------------


def test_the_template_declares_the_switch_and_both_source_connectors_use_it() -> None:
    # FAILS WITHOUT THE FIX: no such parameter, and table.include.list was the bare Fn::Join.
    doc = _template()
    param = doc["Parameters"][CDC_EXACT_TABLE_INCLUDE_PARAM]
    assert param["Type"] == "String" and param["Default"] == "false"
    assert param["AllowedValues"] == ["true", "false"]
    assert doc["Conditions"]["UseExactTableIncludeList"] == {
        "Fn::Equals": [{"Ref": "ExactTableIncludeList"}, "true"]
    }
    for name in _SOURCES:
        config = doc["Resources"][name]["Properties"]["ConnectorConfiguration"]
        assert config["table.include.list"] == {
            "Fn::If": ["UseExactTableIncludeList", _EXACT, _LEGACY]
        }, name


def _resolve(node, doc: dict, params: dict):
    """``node`` with the intrinsics these expressions use (Ref, Fn::Join, Fn::If and the
    condition's Fn::Equals) resolved against ``params``; a CommaDelimitedList is a list."""
    if isinstance(node, list):
        return [_resolve(item, doc, params) for item in node]
    if not isinstance(node, dict):
        return node
    ((fn, arg),) = node.items()
    if fn == "Ref":
        return params[arg]
    if fn == "Fn::Join":
        delimiter, values = arg
        return delimiter.join(_resolve(values, doc, params))
    if fn == "Fn::Equals":
        left, right = _resolve(arg, doc, params)
        return left == right
    if fn == "Fn::If":
        condition, when_true, when_false = arg
        chosen = when_true if _resolve(doc["Conditions"][condition], doc, params) else when_false
        return _resolve(chosen, doc, params)
    raise AssertionError(f"unexpected intrinsic {fn}")


def _captures(include_list: str, table_ids: list[str]) -> list[str]:
    """The ``table_ids`` a ``table.include.list`` admits, matched the way Debezium 2.7.4 does:
    split on commas, each entry trimmed, an anchored match, case-insensitive for ASCII unless
    the entry starts with "(?-i)" (as core/cdc.py infer_column_exclusions matches
    column.exclude.list). Python's re cannot turn a global flag off with "(?-i)", so the
    prefix is stripped instead. The same results came from Debezium 2.7.4's own filter
    (RelationalTableFilters / Selectors) run on the bundled jars, for MySQL and PostgreSQL
    table ids alike."""
    patterns = []
    for entry in (part.strip() for part in include_list.split(",")):
        if entry.startswith("(?-i)"):
            patterns.append(re.compile(entry[len("(?-i)"):]))
        else:
            patterns.append(re.compile(entry, re.IGNORECASE | re.ASCII))
    return [table for table in table_ids if any(p.fullmatch(table) for p in patterns)]


_TABLES = ["shop.users", "shop.orders", "shop.Users", "SHOP.users", "shop.users2", "xshop.users"]


@pytest.mark.parametrize("name", _SOURCES)
def test_exact_capture_admits_only_the_selected_tables_and_the_default_is_unchanged(name) -> None:
    doc = _template()
    expression = doc["Resources"][name]["Properties"]["ConnectorConfiguration"][
        "table.include.list"
    ]
    selected = {"TableIncludeList": ["shop.users", "shop.orders"]}
    legacy = _resolve(expression, doc, {**selected, "ExactTableIncludeList": "false"})
    exact = _resolve(expression, doc, {**selected, "ExactTableIncludeList": "true"})
    # The default is the earlier Fn::Join [",", Ref TableIncludeList], byte-identical.
    assert legacy == "shop.users,shop.orders"
    assert exact == "(?-i)shop.users,(?-i)shop.orders"
    # Ignoring letter case, the earlier value also captured shop.Users and SHOP.users ...
    assert _captures(legacy, _TABLES) == ["shop.users", "shop.orders", "shop.Users", "SHOP.users"]
    # ... the exact one only the selected tables; either is anchored.
    assert _captures(exact, _TABLES) == ["shop.users", "shop.orders"]
    # A bare "(?-i)" matches nothing.
    bare = _resolve(expression, doc, {"TableIncludeList": [""], "ExactTableIncludeList": "true"})
    assert bare == "(?-i)" and _captures(bare, _TABLES) == []


# ---------------------------------------------------------------------------
# 2. Start CDC sends it only to a stack whose parameters show no Start yet
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "current, started",
    [
        (None, False),
        ({}, False),
        ({"DeploySink": "false", "MskBootstrapServers": ""}, False),
        ({"DeploySink": "false", "MskBootstrapServers": "  "}, False),
        ({"DeploySink": "true", "MskBootstrapServers": ""}, True),  # stopped: Stop keeps it
        ({"DeploySink": "TRUE"}, True),
        ({"DeploySink": "false", "MskBootstrapServers": "b-1:9098"}, True),
    ],
)
def test_the_stack_parameters_show_a_start(current, started) -> None:
    assert cdc_stack_params_started(current) is started


def _start(discovery_params, *, connector_states=None, template_body="BUNDLED-TEMPLATE"):
    deployer = _FakeDeployer(
        connector_states=connector_states
        or {SRC: ["CREATING", "RUNNING"], SINK: ["CREATING", "RUNNING"]},
        discovery_params=discovery_params,
    )
    logs: list[str] = []
    run_cdc_start(
        _FakeHandle(), stack_name=STACK, params=_start_params(), deployer=deployer,
        on_log=lambda ts, msg: logs.append(msg), sleep=lambda _s: None,
        watermark=_watermark(), template_body=template_body,
        connector_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    return deployer, logs


@pytest.mark.parametrize(
    "discovery",
    [
        {},
        # Deployed and never started, by an earlier version (no such parameter) ...
        {"DeploySink": "false", "MskBootstrapServers": ""},
        # ... or by this one.
        {"DeploySink": "false", "MskBootstrapServers": "", "ExactTableIncludeList": "false"},
    ],
)
def test_a_first_start_sends_exact_capture(discovery) -> None:
    # FAILS WITHOUT THE FIX: the Start sent no ExactTableIncludeList.
    deployer, logs = _start(discovery)
    assert len(deployer.updates) == 1
    assert dict(deployer.updates[0])[CDC_EXACT_TABLE_INCLUDE_PARAM] == "true"
    assert _LOG in logs


@pytest.mark.parametrize(
    "discovery, running",
    [
        # Stopped (Stop keeps DeploySink), started by 0.1.580 or earlier (no such parameter) ...
        ({"DeploySink": "true", "MskBootstrapServers": ""}, False),
        # ... or by this version, which kept it either way.
        ({"DeploySink": "true", "MskBootstrapServers": "", "ExactTableIncludeList": "false"}, False),
        ({"DeploySink": "true", "MskBootstrapServers": "", "ExactTableIncludeList": "true"}, False),
        # Running, with another table set: the connectors are updated, their capture rule not.
        (
            {
                "DeploySink": "true", "MskBootstrapServers": "b-1:9098",
                "TableIncludeList": "cdc_demo.something_else",
            },
            True,
        ),
    ],
)
def test_a_started_stack_keeps_its_capture(discovery, running) -> None:
    states = {SRC: ["RUNNING"], SINK: ["RUNNING"]} if running else None
    deployer, logs = _start(discovery, connector_states=states)
    assert len(deployer.updates) == 1
    # Not sent: UsePreviousValue keeps the stack's value, or its "false" default.
    assert CDC_EXACT_TABLE_INCLUDE_PARAM not in dict(deployer.updates[0])
    assert not any(CDC_EXACT_TABLE_INCLUDE_PARAM in m for m in logs)


def test_both_connectors_running_with_the_requested_configuration_sends_nothing() -> None:
    discovery = {**_connector_overrides(), "DeploySink": "true", "MskBootstrapServers": "b-1:9098"}
    deployer, logs = _start(discovery, connector_states={SRC: ["RUNNING"], SINK: ["RUNNING"]})
    assert deployer.updates == []
    assert not any(CDC_EXACT_TABLE_INCLUDE_PARAM in m for m in logs)


@pytest.mark.parametrize(
    "discovery, template_body",
    [
        # The UI's PostgreSQL Start: always the bundled template.
        ({}, "BUNDLED-TEMPLATE"),
        # scripts/run_pg_cdc_e2e.py: no template, a stack the repo template deployed.
        (
            {"DeploySink": "false", "MskBootstrapServers": "", "ExactTableIncludeList": "false"},
            None,
        ),
    ],
)
def test_the_external_seed_path_sends_it_too(discovery, template_body) -> None:
    # Every PostgreSQL Start seeds in-process (SeedMode=External): the same pass carries it.
    deployer = _FakeDeployer(
        connector_states={SRC: ["CREATING", "RUNNING"], SINK: ["CREATING", "RUNNING"]},
        discovery_params=discovery,
    )
    seeded: list[dict] = []

    def _seed_fn(**kwargs):
        seeded.append(kwargs)
        return "true"

    run_cdc_start(
        _FakeHandle(), stack_name=STACK, params=_start_params(), deployer=deployer,
        on_log=lambda ts, msg: None, sleep=lambda _s: None, template_body=template_body,
        connector_timeout_seconds=5.0, poll_interval_seconds=0.0,
        seed_mode="external", seed_fn=_seed_fn,
    )
    assert len(seeded) == 1 and len(deployer.updates) == 1
    sent = dict(deployer.updates[0])
    assert sent[CDC_EXACT_TABLE_INCLUDE_PARAM] == "true" and sent["SeedMode"] == "External"


def test_without_the_bundled_template_only_a_stack_that_declares_it_gets_it() -> None:
    # UsePreviousTemplate: an earlier version's template has no ExactTableIncludeList, so
    # sending it would fail the update -- skipped. A stack that declares it gets it.
    older, logs = _start({"DeploySink": "false", "MskBootstrapServers": ""}, template_body=None)
    assert CDC_EXACT_TABLE_INCLUDE_PARAM not in dict(older.updates[0])
    assert _LOG not in logs
    newer, logs = _start({"ExactTableIncludeList": "false"}, template_body=None)
    assert dict(newer.updates[0])[CDC_EXACT_TABLE_INCLUDE_PARAM] == "true"
    assert _LOG in logs


# ---------------------------------------------------------------------------
# 3. The manual says it, in English, Korean and Japanese alike
# ---------------------------------------------------------------------------

_DOC_HEADLINE = {
    "en": (
        "**A CDC stack first started by version 0.1.580 or earlier also captures each table "
        "whose name differs from a selected one only in letter case.**"
    ),
    "ko": (
        "**버전 0.1.580 이하가 처음 시작한 CDC 스택은 선택한 테이블과 이름이 대소문자만 다른 "
        "테이블도 캡처함.**"
    ),
    "ja": (
        "**バージョン 0.1.580 以前が最初に開始した CDC スタックは、選択したテーブルと名前が"
        "大文字小文字だけ異なるテーブルもキャプチャします。**"
    ),
}


@pytest.mark.parametrize("lang", ["en", "ko", "ja"])
def test_the_manual_names_the_stacks_that_capture_a_case_twin(lang) -> None:
    # FAILS WITHOUT THE FIX: chapter 6 did not say it.
    from tests.test_g25_docs import _flat, _page

    part = _page(lang, "06-limitations.md").split(_flat(lang, "## 6.2"), 1)[1].split(
        _flat(lang, "## 6.3"), 1)[0]
    start = part.index(_flat(lang, _DOC_HEADLINE[lang]))
    bullet = part[start:part.index(_flat(lang, "\n- **"), start)]
    # The twin in another MySQL database, and the PostgreSQL routes it is limited to.
    for phrase in (
        "`lower_case_table_names`", "`shop.Users`", "`SHOP.users`", "0.1.580",
        "**Re-snapshot every table instead**",
    ):
        assert _flat(lang, phrase) in bullet, (lang, phrase)
