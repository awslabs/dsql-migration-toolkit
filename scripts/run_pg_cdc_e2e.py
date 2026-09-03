#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Headless PostgreSQL-source CDC end-to-end runner (operational utility, NOT shipped).

Live-verifies that the tool's OWN CDC pipeline works for a PostgreSQL source:
Debezium PostgreSQL connector (pgoutput) -> MSK -> custom DSQL sink. The Full Load +
Validation half is already covered by ``scripts/_pg18_e2e_check.py``; this script adds
the CDC half by REUSING the tool's engine (it re-implements nothing):

    Stage 0  check-wal-level  Confirm/await ``wal_level=logical`` on the source
                              (reuse the dialect probe + the prerequisite check).
    Stage 1  drop-target      DROP the migrated schema's tables on the DSQL target.
    Stage 2  full-load        Run Full Load with ``cdc_stack_name`` set, so the engine
                              creates a logical replication slot + publication on the
                              source at the consistency point and records the WAL LSN
                              (``Watermark.wal_lsn``/``slot_name``/``publication_name``)
                              -> written to the watermark JSON for a GAPLESS handoff.
    Stage 3  deploy-infra     ``create_stack`` the cdc-stack for a PG source
                              (``EngineType=postgres``, ``SeedMode=External``, no binlog
                              offset-seeder Lambda) via ``run_cdc_infra_deploy``.
    Stage 4  workload         Small INSERT/UPDATE/DELETE on the source (a live stream).
    Stage 5  start-cdc        ``run_cdc_start`` — create the PG source + DSQL sink
                              connectors. PG resumes from the pre-created slot
                              (``PgSnapshotMode=never``); NO connect-offsets seed.
    Stage 6  cdc-check        Let CDC catch up, then compare source vs target rows.
    Stage 7  validate         The tool's Validation (CHECKSUM) for the cut-over verdict.
    Stage 8  teardown         ``run_cdc_delete`` — delete the cdc-stack, drop the slot +
                              publication on the source, remove the tool-managed secret.

How the PG path differs from MySQL (why run_e2e_migration.py is not reusable here)
----------------------------------------------------------------------------------
* Watermark: MySQL carries binlog file:pos/GTID (``build_watermark_params`` ->
  ``Watermark*`` params + an in-VPC Lambda seeds ``connect-offsets``). PostgreSQL
  carries a WAL ``wal_lsn`` and the slot/publication NAMES; the slot itself pins the
  WAL from that LSN, so the connector resumes with ``snapshot.mode=never`` and there is
  NO offset seed. The ``Watermark*`` CFN params are empty for a PG watermark (correct).
* Engine select: ``dispatch_*`` branch on ``SourceType.POSTGRES`` -> the PG builders
  emit ``EngineType=postgres`` + ``PgSlotName``/``PgPublicationName``/``PgDatabaseName``/
  ``PgSnapshotMode`` + ``DebeziumPostgresPluginS3Key`` and force ``SeedMode=External``.

LAPTOP CAVEAT (blocker for stage 5 from outside the VPC)
--------------------------------------------------------
PG forces ``SeedMode=External``, so ``run_cdc_start`` does the CDC Kafka prep
IN-PROCESS over the PRIVATE MSK Serverless IAM bootstrap (port 9098) before creating the
connectors. That is reachable ONLY from inside the cdc-stack VPC, with kafka-cluster IAM
and the optional ``cdc-external`` extra (kafka-python) installed. Stages 0-4, 6-8 run
fine from a laptop; ``start-cdc`` must run from an in-VPC host (and needs
``DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR`` set at deploy-infra time so MSK admits the host on
9098). See the report accompanying this script.

Environment (read from env; the caller exports them)
----------------------------------------------------
Source PG : DB_HOST / DB_PORT=5432 / DB_USER / DB_PASSWORD / DB_NAME (connect db, e.g.
            ``postgres``); schema to migrate via CDC_WORKLOAD_SCHEMA.
Target DSQL: TARGET_ENDPOINT / TARGET_REGION=ap-northeast-2 / TARGET_DATABASE /
            TARGET_USERNAME (IAM auth).
CDC       : CDC_STACK_NAME (must start with a CdcDeployRole-scoped prefix — canonical
            "dsql-cdc-" or legacy "mysql-dsql-cdc-"; vary the suffix, e.g. dsql-cdc-pg17),
            CDC_TABLES (default t_orders,t_gen_stored,t_iv), CDC_VPC_ID (required at
            deploy-infra), CDC_CONNECTOR_SUBNET_IDS (required at deploy-infra,
            comma-separated), CDC_SOURCE_DB_SECURITY_GROUP_ID (optional),
            DSQL_MIGRATOR_CDC_DEPLOY_ROLE_ARN (optional assume-role),
            DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR (required for an in-VPC start-cdc).
            No infra identifiers are hardcoded — supply them via env/.env.

Usage (from repo root):
    set -a; source .env.seoul; set +a
    .venv/bin/python scripts/run_pg_cdc_e2e.py --plan               # plan only (no AWS/DB)
    .venv/bin/python scripts/run_pg_cdc_e2e.py --yes                # run all stages
    .venv/bin/python scripts/run_pg_cdc_e2e.py --yes --from-stage deploy-infra
    .venv/bin/python scripts/run_pg_cdc_e2e.py --yes --only teardown

This is an operational utility (NOT shipped in the app), kept alongside
``run_e2e_migration.py``. It writes to the source (slot/publication + workload),
drops+reloads the DSQL target, and creates/deletes a cdc-stack — run it only against a
disposable migration-test environment.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))

# The cdc-stack CloudFormation template (staged into the plugin bucket by the deployer;
# too large for CloudFormation's 51,200-byte inline --template-body limit).
TEMPLATE = os.path.join(_ROOT, "deploy", "cdc-stack", "cdc-stack.yaml")

STAGES = (
    "check-wal-level", "drop-target", "full-load", "deploy-infra",
    "workload", "start-cdc", "cdc-check", "validate", "teardown",
)


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _read_env(path: str) -> dict:
    out: dict = {}
    try:
        for raw in open(path, encoding="utf-8"):
            line = raw.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                out[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return out


ENV = _read_env(os.path.join(_ROOT, ".env"))


def cfg(key: str, default: str = "") -> str:
    return os.environ.get(key) or ENV.get(key) or default


# NB: PostgreSQL reserves the "pg_" schema-name prefix (CREATE SCHEMA pg_cdc_e2e ->
# "unacceptable schema name"), and the tool's own introspection filters "pg\_%" schemas,
# so the default must NOT start with "pg_".
SCHEMA = cfg("CDC_WORKLOAD_SCHEMA", "cdc_e2e")
# The cdc-stack name must start with a CdcDeployRole-scoped prefix — the canonical
# source-neutral "dsql-cdc-" (preferred) or the legacy "mysql-dsql-cdc-". The role
# (deploy/cloudformation-ec2.yaml) scopes CFN/kafka/cloudwatch/iam to those families
# BY DESIGN; a name outside them (e.g. "pg16-dsql-cdc") hits iterative AccessDenied
# (kafka:GetBootstrapBrokers, cloudwatch:PutMetricAlarm, iam:GetRole on the
# connector-execution-role). Keep a scoped prefix; vary only the suffix (dsql-cdc-pg17…).
STACK_NAME = cfg("CDC_STACK_NAME", "dsql-cdc-pg")
# The ordered table set for the run (comma-separated in CDC_TABLES). Single source of
# truth; the same order the converter/sink/validator/compare walk them.
TABLES = [t.strip() for t in cfg("CDC_TABLES", "t_orders,t_gen_stored,t_iv").split(",")
          if t.strip()]
WATERMARK_FILE = os.path.join(_ROOT, cfg("CDC_WATERMARK_FILE", "pg_cdc_e2e_watermark.json"))


def region() -> str:
    ep = cfg("TARGET_ENDPOINT")
    # DSQL endpoint: <id>.dsql.<region>.on.aws
    parts = ep.split(".")
    return cfg("TARGET_REGION") or (parts[2] if len(parts) > 2 else "ap-northeast-2")


def _profile():
    return os.environ.get("AWS_PROFILE")


def _source_config():
    from dsql_migrator.core.models import SourceConnectionConfig, SourceType

    return SourceConnectionConfig(
        source_type=SourceType.POSTGRES,
        host=cfg("DB_HOST", "<unset>"),
        port=int(cfg("DB_PORT", "5432")),
        database=cfg("DB_NAME", "postgres"),
        username=cfg("DB_USER", "postgres"),
    )


def _target_config():
    from dsql_migrator.core.models import TargetConnectionConfig

    return TargetConnectionConfig(
        cluster_endpoint=cfg("TARGET_ENDPOINT", "<unset>"),
        region=region(),
        database=cfg("TARGET_DATABASE", "postgres"),
        username=cfg("TARGET_USERNAME", "admin"),
    )


def _password():
    from dsql_migrator.config import SecretValue

    return SecretValue(cfg("DB_PASSWORD"))


def _require_creds():
    if (cfg("DB_HOST", "<unset>") == "<unset>" or not cfg("DB_PASSWORD")
            or cfg("TARGET_ENDPOINT", "<unset>") == "<unset>"):
        raise SystemExit(
            "ERROR: set DB_HOST / DB_PASSWORD / TARGET_ENDPOINT (source your env file)."
        )


def _introspect_tables():
    """Introspect the PG source and return (inventory, qualified_names, tabledefs).

    Mirrors _pg18_e2e_check.py: normalizes the wanted table names to
    ``<SCHEMA>.<table>`` (qualifying in place) so the converter/sink/validator all
    read/write the schema-qualified target, not ``public``.
    """
    from dsql_migrator.core.table_selection import TableSelection, TableSelector
    from dsql_migrator.ui.evaluation import _default_introspector_factory

    inventory = _default_introspector_factory(_password()).introspect(_source_config())
    present = {t.name: t for t in inventory.tables}
    wanted: list[str] = []
    missing: list[str] = []
    for t in TABLES:
        qualified = f"{SCHEMA}.{t}"
        if qualified in present:
            wanted.append(qualified)
        elif t in present:
            present[t].name = qualified
            wanted.append(qualified)
        else:
            missing.append(t)
    if missing:
        raise SystemExit(
            f"ERROR: tables not found in source inventory: {missing}; "
            f"had {sorted(present)}"
        )
    tables = list(TableSelector().resolve(inventory, TableSelection(selected_tables=wanted)))
    return inventory, wanted, tables


def _load_watermark():
    """Reconstruct the PostgreSQL Watermark from the stage-2 JSON file."""
    from dsql_migrator.core.models import Watermark

    if not os.path.exists(WATERMARK_FILE):
        raise SystemExit(
            f"watermark file {WATERMARK_FILE} missing — run the 'full-load' stage first."
        )
    data = json.load(open(WATERMARK_FILE, encoding="utf-8"))
    ts = data.get("snapshot_timestamp")
    snap = datetime.fromisoformat(ts) if isinstance(ts, str) else datetime.now(timezone.utc)
    return Watermark(
        wal_lsn=data.get("wal_lsn"),
        slot_name=data.get("slot_name"),
        publication_name=data.get("publication_name"),
        snapshot_timestamp=snap,
        table_row_counts=data.get("table_row_counts") or {},
        row_counts_approximate=True,
    )


def _run_cdc_job(label, work_fn, timeout):
    """Submit a run_cdc_* deploy fn to a JobManager and poll to completion.

    The deploy-engine functions (run_cdc_infra_deploy / run_cdc_start / run_cdc_delete)
    drive their stages through a JobManager job handle, exactly as the UI runs them; we
    reproduce that here (same pattern as run_full_load in _pg18_e2e_check.py) instead of
    re-implementing the CloudFormation calls.
    """
    from dsql_migrator.core.job_manager import JobManager

    jm = JobManager()
    captured: dict = {}

    def work(handle) -> None:
        try:
            work_fn(handle)
        except BaseException as exc:  # noqa: BLE001 - stash it so we can report it
            captured["exc"] = exc
            raise

    job_id = jm.submit(work)
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = jm.get_status(job_id)
        if job.status in ("DONE", "FAILED", "CANCELLED"):
            break
        time.sleep(10)
    job = jm.get_status(job_id)
    if job.status != "DONE":
        raise SystemExit(f"{label} ended {job.status}: {captured.get('exc')}")
    log(f"{label}: DONE.")


# --------------------------------------------------------------------------- #
# Stage 0: await wal_level=logical on the source
# --------------------------------------------------------------------------- #
def stage_check_wal_level(args) -> None:
    if not args.yes:
        log("[plan] would probe the source for wal_level=logical (read-only). Use --yes.")
        return
    _require_creds()
    from dsql_migrator.core.prerequisites_postgres import check_wal_level_logical
    from dsql_migrator.core.source_dialect import dialect_for
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.connect import make_source_engine_factory

    source = _source_config()
    dialect = dialect_for(SourceType.POSTGRES)
    engine = make_source_engine_factory(_password())(source)
    deadline = time.time() + args.wal_timeout
    try:
        while True:
            with engine.connect() as conn:
                facts = dialect.probe_cdc_prerequisites(conn, [])
            result = check_wal_level_logical(facts)
            log(f"wal_level check: {result.status.value} — {result.detail}")
            if result.status.value == "PASS":
                break
            if time.time() >= deadline:
                raise SystemExit(
                    f"wal_level not 'logical' after {args.wal_timeout:.0f}s. "
                    f"{result.remediation}"
                )
            log("  waiting for wal_level=logical (retry in 15s) …")
            time.sleep(15)
    finally:
        engine.dispose()


# --------------------------------------------------------------------------- #
# Stage 1: drop the migrated schema's tables on the DSQL target
# --------------------------------------------------------------------------- #
def stage_drop_target(args) -> None:
    if not args.yes:
        log(f"[plan] would DROP tables {TABLES} in DSQL schema '{SCHEMA}' "
            "(DESTRUCTIVE). Use --yes.")
        return
    _require_creds()
    from dsql_migrator.core.target_connection import DsqlConnector

    conn = DsqlConnector(_target_config(), aws_profile=_profile()).connect()
    conn.autocommit = True
    cur = conn.cursor()
    dropped = 0
    for t in TABLES:
        try:
            cur.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{t}"')
            dropped += 1
        except Exception as e:  # noqa: BLE001
            log(f"  drop {t}: {str(e).splitlines()[0]}")
    try:
        cur.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    except Exception as e:  # noqa: BLE001
        log(f"  drop schema: {str(e).splitlines()[0]}")
    conn.close()
    log(f"Dropped {dropped} table(s) + schema '{SCHEMA}' on DSQL. ✓")


# --------------------------------------------------------------------------- #
# Stage 2: Full Load with cdc_stack_name set (creates the slot + records the LSN)
# --------------------------------------------------------------------------- #
def stage_full_load(args) -> None:
    if not args.yes:
        log("[plan] would DROP+recreate the target tables, then Full Load with "
            f"cdc_stack_name='{STACK_NAME}' set — the engine creates the logical "
            "replication slot + publication on the SOURCE at the consistency point and "
            f"records the WAL LSN -> {WATERMARK_FILE}. (writes source + target) Use --yes.")
        return
    _require_creds()
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.job_manager import JobManager
    # DataMigrationInputs / default_migrator_factory / run_full_load are re-exported from
    # the data_migration package (they live in _full_load_engine now); import from the
    # package so this keeps working across the refactor.
    from dsql_migrator.ui.data_migration import (
        DataMigrationInputs, default_migrator_factory, run_full_load,
    )

    inventory, wanted, tables = _introspect_tables()
    inputs = DataMigrationInputs(
        source_config=_source_config(),
        source_password=_password(),
        target_config=_target_config(),
        inventory=inventory,
        aws_profile=_profile(),
        replace_tables=frozenset(wanted),  # clean slate: DROP+recreate the target
        cdc_stack_name=STACK_NAME,          # -> engine provisions slot/publication + LSN
    )
    migrator = default_migrator_factory(inputs)
    error_log = ErrorLogStore()
    jm = JobManager()

    def work(handle) -> None:
        run_full_load(handle, tables, migrator=migrator, error_log=error_log)

    log(f"Full Load {len(tables)} tables into {cfg('TARGET_ENDPOINT')} "
        f"(slot @ {STACK_NAME}) …")
    job_id = jm.submit(work)
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        job = jm.get_status(job_id)
        if job.status in ("DONE", "FAILED", "CANCELLED"):
            break
        time.sleep(5)
    job = jm.get_status(job_id)
    log(f"Full Load status: {job.status}")
    for c in job.chunks:
        log(f"  {c.chunk_id:<28} {c.status:<10} loaded={c.rows_loaded} "
            f"quarantined={getattr(c, 'rows_quarantined', 0)}")

    wm = job.watermark
    if wm is None:
        raise SystemExit("ERROR: no watermark captured (Full Load produced none).")
    if not wm.wal_lsn or not wm.slot_name:
        raise SystemExit(
            "ERROR: watermark has no wal_lsn/slot_name — the gapless handoff needs the "
            "source slot. Check the source user's replication privilege and wal_level."
        )
    out = {
        "wal_lsn": wm.wal_lsn,
        "slot_name": wm.slot_name,
        "publication_name": wm.publication_name,
        "snapshot_timestamp": wm.snapshot_timestamp.isoformat(),
        "table_row_counts": dict(wm.table_row_counts),
    }
    json.dump(out, open(WATERMARK_FILE, "w", encoding="utf-8"), indent=2)
    log(f"Watermark -> {WATERMARK_FILE}: lsn={wm.wal_lsn} slot={wm.slot_name} "
        f"pub={wm.publication_name}")
    if job.status != "DONE":
        raise SystemExit(f"Full Load ended {job.status}.")


# --------------------------------------------------------------------------- #
# Shared: build the PG source + sink configs via the tool's dispatch
# --------------------------------------------------------------------------- #
def _build_pg_configs(allow_empty_sink):
    """Return (pg_source_config, sink_config, tables) built via the tool's dispatch."""
    from dsql_migrator.core.cdc import CDC_DEFAULT_DLQ_TOPIC, CdcPipelineOrchestrator
    from dsql_migrator.core.cdc_postgres import dispatch_source_config
    from dsql_migrator.core.models import SourceType

    _inventory, _wanted, tables = _introspect_tables()
    watermark = _load_watermark()
    source_config = dispatch_source_config(
        SourceType.POSTGRES,
        tables,
        watermark,
        database=cfg("DB_NAME", "postgres"),
        stack_name=STACK_NAME,
    )
    sink_config = CdcPipelineOrchestrator().build_sink_config(
        "postgres-sink", tables, CDC_DEFAULT_DLQ_TOPIC, allow_empty=allow_empty_sink,
    )
    return source_config, sink_config, tables, watermark


def _deployer():
    from dsql_migrator.core.cdc_stack_deployer import build_cdc_stack_deployer

    return build_cdc_stack_deployer(
        region(),
        aws_profile=_profile(),
        assume_role_arn=os.environ.get("DSQL_MIGRATOR_CDC_DEPLOY_ROLE_ARN"),
    )


# --------------------------------------------------------------------------- #
# Stage 3: deploy the cdc-stack infra for a PG source (no connectors yet)
# --------------------------------------------------------------------------- #
def stage_deploy_infra(args) -> None:
    if not args.yes:
        log(f"[plan] would create_stack the cdc-stack '{STACK_NAME}' for a PG source "
            "(EngineType=postgres, SeedMode=External, no connectors yet). Reuses "
            "run_cdc_infra_deploy: ensure plugin bucket + upload artifacts, then "
            "create_stack MSK/VPC/IAM (~15-20 min). Use --yes.")
        return
    _require_creds()
    from dsql_migrator.config import load_config
    from dsql_migrator.core.cdc import CDC_DEFAULT_TOPIC_PREFIX
    from dsql_migrator.core.cdc_deployer import run_cdc_infra_deploy
    from dsql_migrator.core.cdc_postgres import dispatch_cdc_infra_params
    from dsql_migrator.core.dsql_metadata import build_dsql_client, fetch_dsql_cluster_arn
    from dsql_migrator.core.secrets import cdc_source_secret_name, ensure_source_secret

    rg = region()
    source_config, sink_config, tables, watermark = _build_pg_configs(allow_empty_sink=True)

    dsql_client = build_dsql_client(_profile(), rg)
    dsql_cluster_arn = fetch_dsql_cluster_arn(dsql_client, cfg("TARGET_ENDPOINT"))
    if not dsql_cluster_arn:
        raise SystemExit("Could not resolve the DSQL cluster ARN (need dsql:GetCluster).")

    source_secret_arn = ensure_source_secret(
        stack_name=STACK_NAME,
        username=cfg("DB_USER", "postgres"),
        password=cfg("DB_PASSWORD"),
        aws_profile=_profile(),
        region=rg,
    )
    source_secret_name = cdc_source_secret_name(STACK_NAME)

    cfg_app = load_config()

    # The watermark's per-table counts are schema-qualified (schema.table); the CFN
    # param wants bare table names, so strip the schema.
    row_counts = (
        {t.rpartition(".")[2]: c for t, c in watermark.table_row_counts.items()}
        if watermark.table_row_counts else None
    )

    # Networking identifiers come from env only (no committed defaults — this file is
    # tracked, so it must never embed real VPC/subnet IDs). Supply CDC_VPC_ID and
    # CDC_CONNECTOR_SUBNET_IDS for your own account/region.
    params = dispatch_cdc_infra_params(
        source_config, sink_config,
        vpc_id=cfg("CDC_VPC_ID"),
        connector_subnet_ids=cfg("CDC_CONNECTOR_SUBNET_IDS"),
        source_db_security_group_id=cfg("CDC_SOURCE_DB_SECURITY_GROUP_ID", ""),
        # Artifact params are stamped by run_cdc_infra_deploy after it uploads the
        # committed connector plugins; pass empty here.
        plugin_bucket_arn="",
        debezium_plugin_s3_key="",
        dsql_sink_plugin_s3_key="",
        source_db_hostname=cfg("DB_HOST"),
        source_db_port=int(cfg("DB_PORT", "5432")),
        source_secret_arn=source_secret_arn,
        source_secret_name=source_secret_name,
        dsql_cluster_arn=dsql_cluster_arn,
        target_endpoint=cfg("TARGET_ENDPOINT"),
        target_database=cfg("TARGET_DATABASE", "postgres"),
        target_username=cfg("TARGET_USERNAME", "admin"),
        stack_name=STACK_NAME,
        topic_prefix=CDC_DEFAULT_TOPIC_PREFIX,
        row_counts_by_table=row_counts,
        host_subnet_cidr=cfg_app.cdc_host_subnet_cidr,
    )

    template_body = open(TEMPLATE, encoding="utf-8").read()
    deployer = _deployer()

    log(f"Deploying cdc-stack '{STACK_NAME}' (PG source, region {rg}) …")
    if not cfg_app.cdc_host_subnet_cidr:
        log("  NOTE: DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR is empty — a laptop-driven "
            "start-cdc will NOT be able to run the External seed (MSK is VPC-private).")

    def work(handle) -> None:
        run_cdc_infra_deploy(
            handle,
            stack_name=STACK_NAME,
            template_body=template_body,
            params=params,
            deployer=deployer,
            on_log=lambda _ts, msg: log(f"  {msg}"),
            region=rg,
            aws_profile=_profile(),
            create_timeout_seconds=args.infra_timeout,
        )

    _run_cdc_job("deploy-infra", work, timeout=args.infra_timeout + 300)


# --------------------------------------------------------------------------- #
# Stage 4: a small INSERT/UPDATE/DELETE workload on the source (a live stream)
# --------------------------------------------------------------------------- #
def stage_workload(args) -> None:
    if not args.yes:
        log("[plan] would run a small INSERT/UPDATE/DELETE workload on the source "
            f"(writes to the source schema '{SCHEMA}'). Use --yes.")
        return
    _require_creds()
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import URL
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.core.source_dialect import dialect_for

    _inventory, _wanted, tables = _introspect_tables()
    want = cfg("CDC_WORKLOAD_TABLE", "")
    tbl = None
    for t in tables:
        bare = t.name.rpartition(".")[2]
        if want and bare != want:
            continue
        if len(t.primary_key) == 1:
            tbl = t
            break
    if tbl is None:
        log("  no single-column-PK table found for the workload — skipping.")
        return
    schema, _, name = tbl.name.rpartition(".")
    schema = schema or SCHEMA
    pk = tbl.primary_key[0]
    cols = [c.name for c in tbl.columns if not getattr(c, "generated", False)]

    def q(ident):
        return '"' + ident.replace('"', '""') + '"'

    fq = f"{q(schema)}.{q(name)}"
    src = _source_config()
    dialect = dialect_for(SourceType.POSTGRES)
    url = URL.create(
        dialect.driver_scheme,
        username=src.username,
        password=_password().reveal(),
        host=src.host,
        port=src.port,
        database=src.database,
    )
    engine = create_engine(url, **dialect.engine_kwargs()).execution_options(
        isolation_level="AUTOCOMMIT"
    )
    n = args.workload_rows
    log(f"Workload on {fq} (pk={pk}, {n} inserts + 1 update + 1 delete) …")
    try:
        with engine.connect() as conn:
            base = conn.execute(
                text(f"SELECT COALESCE(MAX({q(pk)}), 0) FROM {fq}")
            ).scalar() or 0
            base = int(base)
            col_list = ", ".join(q(c) for c in cols)
            select_cols = ", ".join(
                (f"({base} + :i)" if c == pk else q(c)) for c in cols
            )
            inserted = 0
            for i in range(1, n + 1):
                try:
                    conn.execute(
                        text(f"INSERT INTO {fq} ({col_list}) SELECT {select_cols} "
                             f"FROM {fq} WHERE {q(pk)} = (SELECT MAX({q(pk)}) FROM {fq})"),
                        {"i": i},
                    )
                    inserted += 1
                except Exception as e:  # noqa: BLE001
                    log(f"  INSERT {i} skipped: {str(e).splitlines()[0]}")
            conn.execute(
                text(f"UPDATE {fq} SET {q(pk)} = {q(pk)} "
                     f"WHERE {q(pk)} = (SELECT MIN({q(pk)}) FROM {fq})")
            )
            deleted = 0
            if inserted:
                deleted = conn.execute(
                    text(f"DELETE FROM {fq} WHERE {q(pk)} = :pk"),
                    {"pk": base + 1},
                ).rowcount or 0
            log(f"Workload done: inserted={inserted} updated~=1 deleted={deleted}")
    finally:
        engine.dispose()


# --------------------------------------------------------------------------- #
# Stage 5: start CDC (PG source + DSQL sink connectors; SeedMode=External)
# --------------------------------------------------------------------------- #
def stage_start_cdc(args) -> None:
    if not args.yes:
        log("[plan] would run_cdc_start (single-pass update): create the Debezium PG "
            "source + DSQL sink connectors. PG uses SeedMode=External, so the CDC Kafka "
            "prep runs IN-PROCESS over the PRIVATE MSK bootstrap (port 9098) — this "
            "stage MUST run from INSIDE the cdc-stack VPC with the 'cdc-external' extra. "
            "Use --yes.")
        return
    _require_creds()
    from dsql_migrator.core.cdc import CDC_DEFAULT_TOPIC_PREFIX
    from dsql_migrator.core.cdc_deployer import run_cdc_start
    from dsql_migrator.core.cdc_postgres import dispatch_cdc_stack_params

    # IN-VPC REQUIREMENT (runtime notice): SeedMode=External makes run_cdc_start reach
    # the PRIVATE MSK Serverless IAM bootstrap on port 9098 IN-PROCESS before creating
    # the connectors. This ONLY works from inside the cdc-stack VPC (with the
    # 'cdc-external' extra installed and DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR set at
    # deploy-infra time so MSK admits the host on 9098). If you launched this from a
    # laptop, expect the seed to hang/fail — run start-cdc from an in-VPC host instead.
    log("  NOTE: start-cdc seeds over the PRIVATE MSK bootstrap (9098) IN-PROCESS — this "
        "MUST run from INSIDE the cdc-stack VPC (needs the 'cdc-external' extra and "
        "DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR set at deploy-infra time).")

    source_config, sink_config, tables, watermark = _build_pg_configs(allow_empty_sink=False)
    params = dispatch_cdc_stack_params(
        source_config, sink_config,
        target_endpoint=cfg("TARGET_ENDPOINT"),
        target_database=cfg("TARGET_DATABASE", "postgres"),
        target_username=cfg("TARGET_USERNAME", "admin"),
        stack_name=STACK_NAME,
        topic_prefix=CDC_DEFAULT_TOPIC_PREFIX,
        deploy_sink=True,
    )
    deployer = _deployer()
    log(f"Starting CDC on '{STACK_NAME}' (PG source, SeedMode=External) …")

    def work(handle) -> None:
        run_cdc_start(
            handle,
            stack_name=STACK_NAME,
            params=params,
            deployer=deployer,
            on_log=lambda _ts, msg: log(f"  {msg}"),
            watermark=watermark,
            seed_mode="external",
            connector_timeout_seconds=args.connector_timeout,
        )

    _run_cdc_job("start-cdc", work, timeout=args.connector_timeout + 600)


# --------------------------------------------------------------------------- #
# Stage 6: CDC consistency check (settle, then compare source vs target rows)
# --------------------------------------------------------------------------- #
def stage_cdc_check(args) -> None:
    if not args.yes:
        log(f"[plan] would let CDC settle ({args.cdc_settle}s), then compare source vs "
            "target rows. Use --yes.")
        return
    _require_creds()
    log(f"Letting CDC catch up ({args.cdc_settle}s) …")
    time.sleep(args.cdc_settle)
    _compare_rows()


def _compare_rows():
    import subprocess

    cmp_script = os.path.join(_ROOT, "scripts", "compare_rows.py")
    if not os.path.exists(cmp_script):
        log("  compare_rows.py not found — skipping row compare.")
        return 0
    targets = []
    for t in TABLES:
        targets += ["-t", f"{SCHEMA}.{t}"]
    # This is a PG-source run: tell compare_rows.py to use its PostgreSQL source path
    # (psycopg + PG PK detection + double-quote quoting), else it defaults to MySQL and
    # mis-reports "SOURCE MISSING" against a PG source.
    env = {**os.environ, "SOURCE_TYPE": "postgres", "DB_PORT": cfg("DB_PORT", "5432")}
    proc = subprocess.run([sys.executable, cmp_script, *targets], env=env)
    return proc.returncode


# --------------------------------------------------------------------------- #
# Stage 7: Validation (CHECKSUM + reconcile) — the authoritative cut-over verdict
# --------------------------------------------------------------------------- #
def stage_validate(args) -> None:
    if not args.yes:
        log("[plan] would run the tool's Validation (CHECKSUM + reconcile) for the "
            "cut-over verdict. Use --yes.")
        return
    _require_creds()
    from dsql_migrator.core.models import ValidationMode
    from dsql_migrator.ui.validation import ValidationInputs, run_validation

    inventory, _wanted, tables = _introspect_tables()
    val_inventory = inventory.model_copy(update={"tables": tables})
    vinputs = ValidationInputs(
        source_config=_source_config(),
        source_password=_password(),
        target_config=_target_config(),
        inventory=val_inventory,
        mode=ValidationMode.CHECKSUM,
        reconcile=True,
        check_orphans=False,
    )
    report = run_validation(vinputs)
    ok = True
    for item in report.items:
        bare = item.table.rpartition(".")[2]
        rc = item.reconcile
        recon = (f" missing={rc.missing_on_target} extra={rc.extra_on_target}"
                 if rc else "")
        verdict = "MATCH" if item.matched else "MISMATCH"
        log(f"  {bare:<16} {verdict:<9} src={item.source_row_count} "
            f"tgt={item.target_row_count} csum={item.checksum_match}{recon}"
            + (f"  ERROR={item.error}" if item.error else ""))
        ok = ok and item.matched
    log(f"Validation verdict: {'MATCH (converged)' if ok else 'MISMATCH'} "
        f"(is_match={report.is_match}).")


# --------------------------------------------------------------------------- #
# Stage 8: teardown (delete cdc-stack, drop slot/publication, remove secret)
# --------------------------------------------------------------------------- #
def stage_teardown(args) -> None:
    if not args.yes:
        log(f"[plan] would run_cdc_delete: delete the cdc-stack '{STACK_NAME}', drop the "
            "PG replication slot + publication on the source, and remove the "
            "tool-managed source secret (DESTRUCTIVE, ~5-20 min). Use --yes.")
        return
    _require_creds()
    from dsql_migrator.core.cdc_deployer import run_cdc_delete

    deployer = _deployer()
    log(f"Deleting cdc-stack '{STACK_NAME}' + dropping source slot/publication …")

    def work(handle) -> None:
        run_cdc_delete(
            handle,
            stack_name=STACK_NAME,
            deployer=deployer,
            on_log=lambda _ts, msg: log(f"  {msg}"),
            region=region(),
            aws_profile=_profile(),
            cleanup_source_secret=True,
            delete_timeout_seconds=args.delete_timeout,
        )

    _run_cdc_job("teardown", work, timeout=args.delete_timeout + 300)


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
_STAGE_FUNCS = {
    "check-wal-level": stage_check_wal_level,
    "drop-target": stage_drop_target,
    "full-load": stage_full_load,
    "deploy-infra": stage_deploy_infra,
    "workload": stage_workload,
    "start-cdc": stage_start_cdc,
    "cdc-check": stage_cdc_check,
    "validate": stage_validate,
    "teardown": stage_teardown,
}


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--yes", action="store_true",
                    help="actually run (every side-effecting stage needs this)")
    ap.add_argument("--plan", action="store_true",
                    help="print the plan and exit (no AWS/DB)")
    ap.add_argument("--from-stage", choices=STAGES, default=STAGES[0],
                    help="start at this stage (resume)")
    ap.add_argument("--only", choices=STAGES, help="run only this one stage")
    ap.add_argument("--timeout", type=float, default=1800.0, help="Full Load timeout (s)")
    ap.add_argument("--infra-timeout", type=float, default=2400.0,
                    help="cdc-stack CREATE_COMPLETE timeout (s)")
    ap.add_argument("--connector-timeout", type=float, default=2700.0,
                    help="per-connector RUNNING wait (s)")
    ap.add_argument("--delete-timeout", type=float, default=1800.0,
                    help="cdc-stack DELETE_COMPLETE timeout (s)")
    ap.add_argument("--cdc-settle", type=float, default=120.0,
                    help="seconds to let CDC catch up before cdc-check")
    ap.add_argument("--wal-timeout", type=float, default=0.0,
                    help="seconds to await wal_level=logical (0 = check once)")
    ap.add_argument("--workload-rows", type=int, default=5,
                    help="INSERT rows for the workload stage")
    args = ap.parse_args()

    order = list(STAGES)
    if args.only:
        selected = [args.only]
    else:
        selected = order[order.index(args.from_stage):]

    log(f"PG CDC E2E — schema={SCHEMA} stack={STACK_NAME} region={region()} tables={TABLES}")
    log(f"Stages: {', '.join(selected)}  (yes={args.yes})")
    if args.plan or not args.yes:
        log("[plan] re-run with --yes to execute. Side-effecting stages: drop-target, "
            "full-load (writes source+target), deploy-infra/start-cdc (deploy), workload "
            "(writes source), teardown (delete). start-cdc's External seed needs an "
            "IN-VPC host — see the module docstring.")
    # --plan is a hard dry-run: dispatch each stage with yes forced False so no stage
    # can touch AWS/DB (every stage's plan branch returns before any import/connect).
    if args.plan:
        for stage in selected:
            log(f"===== STAGE (plan): {stage} =====")
            _STAGE_FUNCS[stage](argparse.Namespace(**{**vars(args), "yes": False}))
        return 0
    for stage in selected:
        log(f"===== STAGE: {stage} =====")
        _STAGE_FUNCS[stage](args)
    log("PG CDC E2E run complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
