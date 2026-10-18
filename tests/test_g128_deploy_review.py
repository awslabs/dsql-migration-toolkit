# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G128: a CDC table list too long for one CloudFormation parameter is refused before
anything is uploaded or loaded; a stack on an older sink plugin says how to get the bundled
one; the deployment docs match the templates.

The per-table lists (TableIncludeList, SinkTopics, SinkTopicPartitions, TopicGroupInclude2 /
4) are each ONE stack parameter, and CloudFormation caps a parameter value at 4,096 bytes:
a capture of about 100-160 tables failed CreateStack / UpdateStack with a bare
ValidationError -- after the plugins were uploaded, and on Full load + CDC possibly after
the whole Full Load. Doubles only: no AWS, no source, no Aurora DSQL.
"""

from __future__ import annotations

import asyncio
import re
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

from dsql_migrator.core.cdc import (
    CDC_PARAMETER_BYTE_LIMIT,
    CDC_TABLE_LIST_PARAMS,
    DebeziumSourceConfig,
    SinkConnectorConfig,
    build_cdc_infra_params,
    build_cdc_stack_params,
    list_parameter_size_refusal,
    plugin_version_is_older,
)
from dsql_migrator.core.cdc_deployer import CdcDeployError
from dsql_migrator.core.cdc_postgres import (
    PostgresSourceConfig,
    build_pg_cdc_infra_params,
    build_pg_cdc_stack_params,
)
from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef
from dsql_migrator.core.s3_provision import PLUGIN_VERSION
from dsql_migrator.core.table_selection import TableSelection
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType
from dsql_migrator.ui.data_migration._models import (
    CDC_TABLE_LIST_HEADER,
    CDC_TABLE_LIST_WORST_CASE_HEADER,
    cdc_table_list_refusal,
)

from tests.test_cdc_deploy_stages import (
    _FakeDeployer,
    _FakeHandle,
    _FakeS3,
    _logs,
    _run_infra,
    _start_with,
    _statuses,
)
from tests.test_ui_data_migration import _RecordingUi, _StubJobManager

REPO = Path(__file__).resolve().parent.parent
_BASES = ("customers", "orders", "order_items", "products", "inventory_movements", "payments")


def _names(n, db="shop"):
    """Realistic qualified names (``shop.order_items_017``: 15-30 bytes)."""
    return [f"{db}.{_BASES[i % len(_BASES)]}_{i:03d}" for i in range(n)]


def _band(n=165):
    """15-byte names: SinkTopics fits 4,096 bytes for 165 of them (3,959 B), the partition
    map that lists every topic does not (4,289 B)."""
    return [f"shop.table_{i:04d}" for i in range(n)]


def _skewed(names):
    # A few big tables, the rest small: what makes the deploy size partitions by table.
    return {name: (50_000_000 if i < 5 else 10_000) for i, name in enumerate(names)}


def _sink(names):
    return SinkConnectorConfig(name="sink", topics=names, dlq_topic="dsql-sink-dlq")


def _pg_source(names):
    return PostgresSourceConfig(
        name="pg-source", table_include_list=names, database_name="app",
        slot_name="s", publication_name="p",
    )


_INFRA_KW = dict(
    vpc_id="vpc-1", connector_subnet_ids="subnet-a,subnet-b", plugin_bucket_arn="",
    debezium_plugin_s3_key="", dsql_sink_plugin_s3_key="", source_db_hostname="db.host",
    source_secret_arn="arn:sec", source_secret_name="my/secret", dsql_cluster_arn="arn:dsql",
    target_endpoint="ep.on.aws",
)


def _infra(engine, names, counts=None):
    if engine == "mysql":
        source = DebeziumSourceConfig(name="mysql-source", table_include_list=names)
        return build_cdc_infra_params(
            source, _sink(names), row_counts_by_table=counts, **_INFRA_KW
        )
    return build_pg_cdc_infra_params(
        _pg_source(names), _sink(names), row_counts_by_table=counts, **_INFRA_KW
    )


def _start(engine, names):
    if engine == "mysql":
        source = DebeziumSourceConfig(name="mysql-source", table_include_list=names)
        return build_cdc_stack_params(source, _sink(names), target_endpoint="ep.on.aws")
    return build_pg_cdc_stack_params(_pg_source(names), _sink(names), target_endpoint="ep.on.aws")


def _size(params, key):
    return len(dict(params.filled)[key].encode("utf-8"))


# ---------------------------------------------------------------------------
# 1. The shared pure helper
# ---------------------------------------------------------------------------


def test_the_helper_refuses_only_a_table_list_over_the_parameter_limit() -> None:
    assert list_parameter_size_refusal([]) is None
    at_limit = [(key, "x" * CDC_PARAMETER_BYTE_LIMIT) for key in CDC_TABLE_LIST_PARAMS]
    assert list_parameter_size_refusal(at_limit) is None
    # Every other parameter is not this helper's (ColumnExcludeList and the re-key lists
    # have their own refusals, with their own remedies).
    assert list_parameter_size_refusal(
        [("ColumnExcludeList", "x" * 5000), ("MessageKeyColumns", "y" * 5000)]
    ) is None
    refusal = list_parameter_size_refusal(
        [("TableIncludeList", "t" * 4097), ("SinkTopics", "s" * 4500),
         ("TopicGroupInclude4", None)]
    )
    # The largest one is named, with its size and the remedy.
    assert refusal == (
        "The captured tables need 4,500 bytes in the CDC configuration (SinkTopics), over "
        "CloudFormation's 4,096-byte limit for one parameter. Capture fewer tables in this "
        "CDC pipeline, or use Full load only."
    )
    # Bytes, not characters.
    assert "(SinkTopicPartitions)" in list_parameter_size_refusal(
        [("SinkTopicPartitions", "é" * 2049)]
    )


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_300_tables_are_refused_by_parameter_and_size(engine) -> None:
    db = "shop" if engine == "mysql" else "public"
    names = _names(300, db)
    infra = _infra(engine, names, _skewed(names))
    # The size plan lists every topic with its count: the longest list.
    plan = _size(infra, "SinkTopicPartitions")
    assert plan > _size(infra, "SinkTopics") > _size(infra, "TableIncludeList") > 4096
    assert f"need {plan:,} bytes in the CDC configuration (SinkTopicPartitions)" in (
        list_parameter_size_refusal(infra.filled)
    )
    start = _start(engine, names)
    topics = _size(start, "SinkTopics")
    assert f"need {topics:,} bytes in the CDC configuration (SinkTopics)" in (
        list_parameter_size_refusal(start.filled)
    )
    # 100 such tables fit, plan included.
    small = names[:100]
    assert list_parameter_size_refusal(_infra(engine, small, _skewed(small)).filled) is None
    assert list_parameter_size_refusal(_start(engine, small).filled) is None


# ---------------------------------------------------------------------------
# 2. The backstops: Deploy CDC infrastructure and Start CDC
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_deploy_refuses_before_the_bucket_or_any_upload(engine) -> None:
    # FAILS WITHOUT THE FIX: the plugins were uploaded and CreateStack received the lists.
    names = _names(300, "shop" if engine == "mysql" else "public")
    handle = _FakeHandle()
    logs, on_log = _logs()
    s3 = _FakeS3(absent=True)
    deployer = _FakeDeployer(stack_statuses=("CREATE_IN_PROGRESS", "CREATE_COMPLETE"))
    with pytest.raises(CdcDeployError) as exc:
        _run_infra(
            handle, deployer, on_log, s3_client=s3, stack_name="dsql-cdc-shop",
            template_body="T", params=_infra(engine, names, _skewed(names)),
            create_timeout_seconds=5.0, poll_interval_seconds=0.0,
        )
    assert "(SinkTopicPartitions), over CloudFormation's 4,096-byte limit" in str(exc.value)
    assert "Capture fewer tables in this CDC pipeline, or use Full load only." in str(exc.value)
    assert s3.calls == [] and deployer.calls == [] and deployer.created == []
    assert set(_statuses(handle).values()) == {"PENDING"}
    assert handle.job.error_count == 1
    assert any(m.startswith("ERROR: The captured tables need") for m in logs)


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_start_refuses_before_any_update_on_a_stack_deployed_with_fewer_tables(engine) -> None:
    # FAILS WITHOUT THE FIX: UpdateStack received SinkTopics over 4,096 bytes.
    names = _names(300, "shop" if engine == "mysql" else "public")
    deployed = dict(_start(engine, names[:20]).filled)
    deployer = _FakeDeployer(discovery_params=deployed)
    with pytest.raises(CdcDeployError) as exc:
        _start_with(_start(engine, names), deployer)
    assert "(SinkTopics), over CloudFormation's 4,096-byte limit" in str(exc.value)
    assert deployer.updates == []
    assert "submit_update" not in deployer.calls
    assert "get_bootstrap_brokers" not in deployer.calls


# ---------------------------------------------------------------------------
# 3. The UI judgement: what Deploy / Start send, built as the builders build it
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_the_ui_judges_exactly_what_the_builders_send(engine) -> None:
    names = _names(300, "shop" if engine == "mysql" else "public")
    counts = _skewed(names)
    assert cdc_table_list_refusal(names, deploy=True, row_counts_by_table=counts) == (
        list_parameter_size_refusal(_infra(engine, names, counts).filled)
    )
    assert cdc_table_list_refusal(names, deploy=False) == (
        list_parameter_size_refusal(_start(engine, names).filled)
    )
    small = names[:100]
    assert cdc_table_list_refusal(small, deploy=True) is None
    assert cdc_table_list_refusal(small, deploy=False) is None


def test_without_row_estimates_the_deploy_is_judged_as_the_builders_send_it() -> None:
    names = _band()
    # Start sends only TableIncludeList and SinkTopics, and they fit.
    assert cdc_table_list_refusal(names, deploy=False) is None
    # G131: no estimates is the uniform plan, which sends no partition map -- as the builders
    # do. The largest plan is the worst case a warning names (cdc_partition_map_worst_case),
    # and the Deploy click reads the estimates before it judges.
    assert cdc_table_list_refusal(names, deploy=True) is None
    largest = cdc_table_list_refusal(names, deploy=True, row_counts_by_table=_skewed(names))
    assert "need 4,289 bytes in the CDC configuration (SinkTopicPartitions)" in largest
    # Even estimates: the uniform plan sends no partition map, so nothing is refused.
    even = {name: 1000 for name in names}
    assert cdc_table_list_refusal(names, deploy=True, row_counts_by_table=even) is None


# ---------------------------------------------------------------------------
# 4. Where the UI holds it: Deploy (button + click), Start (button + click), Start Full Load
# ---------------------------------------------------------------------------


def _inventory(names):
    return SourceInventory(tables=[
        TableDef(name=n, columns=[ColumnDef(name="id", mysql_type="int")], primary_key=["id"])
        for n in names
    ])


def _state(names, mtype=MigrationType.FULL_LOAD_AND_CDC, phase="absent"):
    state = DataMigrationState()
    state.migration_type = mtype
    state._session = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.MYSQL))
    state.set_selection(TableSelection(selected_tables=list(names)))
    if phase is not None:
        state.set_cdc_stack_phase(phase, status="CREATE_COMPLETE" if phase == "infra" else None)
    return state


def test_the_deploy_button_is_held_with_the_parameter_and_the_picker_remedy() -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    # G131: without recorded estimates the button holds only the lists every deploy sends
    # (the band of _band() is a warning there now -- test_g131_table_list_warning).
    names = _names(300)
    state = _state(names)
    notice = _deploy_block_notice(state, _StubJobManager({}), _inventory(names), None)
    assert notice is not None and notice[0] == CDC_TABLE_LIST_HEADER
    assert "(SinkTopics)" in notice[1]
    assert notice[1].endswith("Deselect tables in the table picker.")
    # Attach sends no parameter: not held.
    assert _deploy_block_notice(
        state, _StubJobManager({}), _inventory(names), None,
        include_missing=False, include_capture=False,
    ) is None
    # A capture that fits is not held.
    assert _deploy_block_notice(
        _state(names[:100]), _StubJobManager({}), _inventory(names[:100]), None
    ) is None


def test_the_deploy_click_refuses_before_any_aws_call(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui

    # G131: lists every deploy sends over the limit are refused before even the source's
    # row estimates are read (a plan judged on read estimates: test_g131_table_list_warning).
    names = _names(300)
    io_calls: list = []

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            io_calls.append(fn)
            raise AssertionError("no AWS / io_bound call may run before the refusal")

    fake = types.ModuleType("nicegui")
    fake.run = _Run
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    notes: list = []
    submitted: list = []
    jobs = _StubJobManager({})
    jobs.submit = lambda work: submitted.append(work) or "job"
    deploy_ui = SimpleNamespace(notify=lambda text="", **k: notes.append((text, k.get("type"))))
    asyncio.run(_cdc_ui._start_cdc_infra_deploy(
        deploy_ui, _state(names), jobs, lambda: None, inventory=_inventory(names),
        session=SimpleNamespace(target_config=SimpleNamespace(region="us-east-1")),
    ))
    ((text, tone),) = notes
    assert tone == "negative" and "(SinkTopics)" in text
    assert text.endswith("Deselect tables in the table picker.")
    assert io_calls == [] and submitted == []


def test_the_start_button_is_disabled_with_the_refusal(monkeypatch) -> None:
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import _cdc_ui

    names = _names(300)
    notices: list = []
    real = _cdc_ui.render_notice
    monkeypatch.setattr(
        _cdc_ui, "render_notice", lambda ui, **kw: notices.append(kw) or real(ui, **kw)
    )
    state = _state(names, phase="infra")
    state.set_prereq_gated_mode(MigrationMode.CDC)  # the CDC checks passed (they gate first)
    ui = _RecordingUi()
    _cdc_ui._render_cdc_start_button(
        ui, state, _StubJobManager({}), lambda: None, inventory=_inventory(names), session=None,
    )
    (hit,) = [n for n in notices if n.get("header") == CDC_TABLE_LIST_HEADER]
    assert hit["tone"] == "error" and "(SinkTopics)" in hit["body"]
    (start,) = [b for b in ui.buttons if b.text == "Start CDC"]
    assert "disable" in start.props_seen
    assert hit["body"] in ui.tooltips


def test_start_cdc_and_retry_cdc_refuse_before_any_parameter_or_job(monkeypatch) -> None:
    # Retry CDC skips the dialog and the button: the handler refuses on its own.
    from dsql_migrator.ui.data_migration import _cdc_ui

    names = _names(300)
    notices: list = []
    monkeypatch.setattr(_cdc_ui, "render_notice", lambda ui, **kw: notices.append(kw))
    submitted: list = []
    jobs = _StubJobManager({})
    jobs.submit = lambda work: submitted.append(work) or "job"
    _cdc_ui._start_cdc_deploy(
        SimpleNamespace(notify=lambda *a, **k: None), _state(names, phase="infra"), jobs,
        lambda: None, inventory=_inventory(names),
        session=SimpleNamespace(source_config=None, target_config=None, aws_profile=None,
                                restored_source_type=None, source_password=None),
    )
    (hit,) = [n for n in notices if n.get("header") == CDC_TABLE_LIST_HEADER]
    assert "(SinkTopics), over CloudFormation's 4,096-byte limit" in hit["body"]
    assert submitted == []


def test_start_full_load_is_warned_not_held_before_the_load_for_full_load_and_cdc() -> None:
    # G131: the load may take every table and the set be narrowed before CDC starts (G16),
    # so before the load this is a warning on the Full Load card, never a hold; Deploy and
    # Start CDC hold the capture they send (test_g131_table_list_warning).
    from dsql_migrator.ui.data_migration import (
        data_migration_exclusion_block,
        pre_load_table_list_warning,
    )

    names = _band()
    jobs = _StubJobManager({})
    assert data_migration_exclusion_block(_state(names), jobs, None, _inventory(names)) is None
    # (header, body): only the deploy's worst case does not fit, so it is said as possible.
    header, warning = pre_load_table_list_warning(_state(names), jobs, None, _inventory(names))
    assert header == CDC_TABLE_LIST_WORST_CASE_HEADER
    assert "(SinkTopicPartitions)" in warning
    assert warning.endswith("Deselect tables in the table picker.")
    # Full load only captures nothing.
    assert pre_load_table_list_warning(
        _state(names, MigrationType.FULL_LOAD_ONLY), jobs, None, _inventory(names)
    ) is None
    # A stack deployed before the load already owns its partition plan: only the two lists
    # Start CDC sends are judged, and they fit.
    assert pre_load_table_list_warning(
        _state(names, phase="infra"), jobs, None, _inventory(names)
    ) is None
    many = _names(300)
    assert data_migration_exclusion_block(
        _state(many, phase="infra"), jobs, None, _inventory(many)
    ) is None
    warned = pre_load_table_list_warning(
        _state(many, phase="infra"), jobs, None, _inventory(many)
    )
    assert warned[0] == CDC_TABLE_LIST_HEADER and "(SinkTopics)" in warned[1]


# ---------------------------------------------------------------------------
# 5. A stack on an older sink plugin says how to get the bundled one
# ---------------------------------------------------------------------------


def test_plugin_version_is_older() -> None:
    assert plugin_version_is_older("v53", "v54")
    assert plugin_version_is_older(" v9 ", "v54")
    assert not plugin_version_is_older("v54", "v54")
    assert not plugin_version_is_older("v55", "v54")
    # Unknown is not called older.
    for deployed in (None, "", "latest", "54", "V53"):
        assert not plugin_version_is_older(deployed, "v54"), deployed
    assert not plugin_version_is_older(PLUGIN_VERSION, PLUGIN_VERSION)


def test_the_phase_probe_records_the_deployed_plugin_version(monkeypatch) -> None:
    import dsql_migrator.core.cdc_deployer as deployer_mod
    from dsql_migrator.ui.data_migration._cdc_status import _probe_cdc_stack_phase

    def _probe(params):
        class _Deployer:
            def describe_stack_or_none(self, name):
                if params is None:
                    return None
                return SimpleNamespace(stack_status="UPDATE_COMPLETE", current_parameters=params,
                                       is_stable=True, stack_name=name)

            def list_cdc_stacks(self):
                return []

        monkeypatch.setattr(deployer_mod, "build_cdc_stack_deployer", lambda *a, **k: _Deployer())
        state = DataMigrationState()
        state.set_cdc_deployed_plugin_version("stale")
        _probe_cdc_stack_phase(state, SimpleNamespace(
            target_config=SimpleNamespace(region="us-east-1"), aws_profile=None))
        return state.cdc_deployed_plugin_version

    assert _probe({"PluginVersion": "v53", "MskBootstrapServers": ""}) == "v53"
    assert _probe({"TableIncludeList": "shop.docs"}) is None  # no such parameter
    assert _probe(None) is None                                # no stack


def test_the_deployed_plugin_version_belongs_to_one_stack() -> None:
    state = DataMigrationState()
    state.set_cdc_deployed_plugin_version("v53")
    state.set_cdc_stack_phase("absent")
    assert state.cdc_deployed_plugin_version is None
    state.set_cdc_deployed_plugin_version("v53")
    state.set_cdc_stack_name("dsql-cdc-other")
    assert state.cdc_deployed_plugin_version is None
    state.set_cdc_deployed_plugin_version("v53")
    state.adopt_cdc_stack("dsql-cdc-third")
    assert state.cdc_deployed_plugin_version is None


def _card(state, monkeypatch):
    from dsql_migrator.ui.data_migration import _cdc_ui

    notices: list = []
    real = _cdc_ui.render_notice
    monkeypatch.setattr(
        _cdc_ui, "render_notice", lambda ui, **kw: notices.append(kw) or real(ui, **kw)
    )
    _cdc_ui._render_cdc_start_action(
        _RecordingUi(), state, _StubJobManager({}), lambda: None, inventory=None, session=None
    )
    return [n for n in notices if n.get("header") == "This CDC infrastructure runs an older sink plugin"]


def test_a_stack_on_an_older_sink_plugin_warns_with_the_way_to_the_bundled_one(monkeypatch) -> None:
    state = DataMigrationState()
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    state.set_cdc_deployed_plugin_version("v53")
    (hit,) = _card(state, monkeypatch)
    assert hit["tone"] == "warning"
    assert "sink plugin v53" in hit["body"] and f"bundles {PLUGIN_VERSION}" in hit["body"]
    assert "Delete CDC infrastructure and deploy it again" in hit["body"]
    # The bundled plugin, an unknown one and no stack: nothing to say.
    for version in (PLUGIN_VERSION, None):
        state.set_cdc_deployed_plugin_version(version)
        assert _card(state, monkeypatch) == [], version
    absent = DataMigrationState()
    absent.set_cdc_stack_phase("absent")
    absent.cdc_deployed_plugin_version = "v53"  # a stale cache must not speak for no stack
    assert _card(absent, monkeypatch) == []


# ---------------------------------------------------------------------------
# 6. The deployment docs match the templates
# ---------------------------------------------------------------------------


def _deploy_commands(text):
    """Each ``aws cloudformation deploy`` command, its backslash-continued lines joined."""
    lines = text.splitlines()
    commands = []
    for i, line in enumerate(lines):
        if not line.lstrip().startswith("aws cloudformation deploy"):
            continue
        parts = [line]
        while parts[-1].rstrip().endswith("\\") and i + len(parts) < len(lines):
            parts.append(lines[i + len(parts)])
        commands.append(" ".join(parts))
    return commands


def test_every_documented_deploy_of_a_named_role_template_acknowledges_it() -> None:
    # FAILS WITHOUT THE FIX: "Update to a new image version" passed only CAPABILITY_IAM
    # for deploy/cloudformation.yaml, whose CdcDeployRole carries a RoleName.
    checked = 0
    for name in ("DEPLOYMENT.md", "DEPLOYMENT.ko.md", "DEPLOYMENT.ja.md"):
        for command in _deploy_commands((REPO / "deploy" / name).read_text(encoding="utf-8")):
            template = re.search(r"--template-file\s+(\S+)", command)
            if template is None:
                continue
            body = (REPO / template.group(1)).read_text(encoding="utf-8")
            if re.search(r"^\s+RoleName:", body, re.M):
                assert "CAPABILITY_NAMED_IAM" in command, (name, command)
                checked += 1
    assert checked >= 9  # create + update for the app stack, the EC2 stack, per language
