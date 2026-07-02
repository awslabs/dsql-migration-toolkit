#!/usr/bin/env python3
"""End-to-end MySQL -> Aurora DSQL migration runbook (automated, resumable).

Runs the whole Full Load + CDC migration for the ``customers_sample_new`` schema
the way the UI would, but headless and repeatable, by ORCHESTRATING the building
blocks that already exist in this repo (it re-implements nothing):

    Stage 0  reset-infra   Delete the cdc-stack (CloudFormation) so CDC starts clean.
    Stage 1  drop-target   Drop the customers_sample_new schema + objects on DSQL.
    Stage 2  workload       Start the production-like 1s INSERT/UPDATE/DELETE loop
                            on the source (scripts/cdc_workload_customers_sample_new.py),
                            in the background, so CDC has a live stream to replicate.
    Stage 3  full-load      Run Full Load (scripts/run_full_load_harness.py) and write
                            the export watermark; then compare source vs target rows.
    Stage 4  start-cdc      Recreate the cdc-stack infra and start CDC, seeding the
                            Debezium start offset from the Full Load watermark for a
                            GAPLESS handoff. The offset seed is fully automatic: an
                            in-VPC Lambda custom resource (deployed by the stack)
                            seeds the connect-offsets topic before the source
                            connector is created — no bastion / SSM needed.
    Stage 5  cdc-check      Let CDC catch up, then compare source vs target rows to
                            confirm the stream converged.
    Stage 6  validate       Run the tool's Validation (scripts/run_validation_e2e or
                            the validator) for the authoritative cut-over verdict.

Design
------
* Each stage is idempotent and independently runnable (``--from-stage`` /
  ``--only``), so a long run can resume after a transient failure without redoing
  the expensive infra steps.
* VPC-internal work (the gapless offset seed) is performed by an in-VPC Lambda
  custom resource the cdc-stack deploys, so the laptop never has to reach the
  private MSK Serverless bootstrap directly (no bastion / SSM).
* DESTRUCTIVE stages (0 delete stack, 1 drop schema) require ``--yes``; without
  it the orchestrator prints the plan and stops.

Prerequisites
-------------
* ``.env`` with DB_HOST/DB_PORT/DB_USER/DB_PASSWORD (source) and
  TARGET_ENDPOINT/TARGET_REGION/TARGET_DATABASE/TARGET_USERNAME (DSQL).
* The committed connector + seeder artifacts under connectors/plugins/ (uploaded
  to the managed plugin bucket by stage 4 via core.s3_provision).
* AWS creds (the shared profile) able to drive CloudFormation + DSQL.

Usage (from repo root):
    set -a; source .env; set +a
    .venv/bin/python scripts/run_e2e_migration.py --plan            # show the plan
    .venv/bin/python scripts/run_e2e_migration.py --yes             # run all stages
    .venv/bin/python scripts/run_e2e_migration.py --yes --from-stage 3
    .venv/bin/python scripts/run_e2e_migration.py --only workload   # just one stage

This is an operational utility (NOT shipped in the app). It writes to the source
(workload), drops + reloads the DSQL target, and tears down/recreates the
cdc-stack -- run it only against a disposable migration-test environment.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # for _e2e_tables

from _e2e_tables import tables_for  # noqa: E402

SCHEMA = os.environ.get("CDC_WORKLOAD_SCHEMA", "customers_sample_new")
STACK_NAME = os.environ.get("CDC_STACK_NAME", "mysql-dsql-cdc-stack")
PY = sys.executable
WATERMARK_FILE = os.path.join(_ROOT, "e2e_watermark.json")
WORKLOAD_PIDFILE = os.path.join(_ROOT, "e2e_workload.pid")
WORKLOAD_LOG = "/tmp/e2e_workload.log"

# The ordered table set for the active schema (single source of truth:
# scripts/_e2e_tables.py), same order the Full Load harness / CDC monitor use.
TABLES = tables_for(SCHEMA)

STAGES = (
    "reset-infra", "drop-target", "workload", "full-load",
    "start-cdc", "cdc-check", "validate",
)


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _env(path: str) -> dict:
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


ENV = _env(os.path.join(_ROOT, ".env"))


def cfg(key: str, default: str = "") -> str:
    return os.environ.get(key) or ENV.get(key) or default


def run(cmd: list[str], *, check: bool = True, env: dict | None = None) -> int:
    """Run a subprocess, streaming output; return its exit code."""
    log("$ " + " ".join(cmd))
    proc = subprocess.run(cmd, env={**os.environ, **(env or {})})
    if check and proc.returncode != 0:
        raise SystemExit(f"command failed (exit {proc.returncode}): {' '.join(cmd)}")
    return proc.returncode


def aws_json(args: list[str]):
    """Run an aws CLI command returning parsed JSON (or None)."""
    import json
    out = subprocess.run(
        ["aws", *args, "--output", "json"], capture_output=True, text=True
    )
    if out.returncode != 0:
        return None
    try:
        return json.loads(out.stdout or "null")
    except Exception:  # noqa: BLE001
        return None


def region() -> str:
    ep = cfg("TARGET_ENDPOINT")
    # DSQL endpoint: <id>.dsql.<region>.on.aws
    parts = ep.split(".")
    return cfg("TARGET_REGION") or (parts[2] if len(parts) > 2 else "us-east-1")


# --------------------------------------------------------------------------- #
# Stage 0: reset CDC infra (delete the cdc-stack)
# --------------------------------------------------------------------------- #
def stage_reset_infra(args) -> None:
    rg = region()
    status = aws_json([
        "cloudformation", "describe-stacks", "--region", rg,
        "--stack-name", STACK_NAME,
        "--query", "Stacks[0].StackStatus",
    ])
    if status is None:
        log(f"cdc-stack '{STACK_NAME}' not present — nothing to delete. ✓")
        return
    log(f"cdc-stack '{STACK_NAME}' status={status} -> delete_stack (teardown).")
    if not args.yes:
        log("[plan] would delete the whole cdc-stack (DESTRUCTIVE). Use --yes.")
        return
    run(["aws", "cloudformation", "delete-stack", "--region", rg,
         "--stack-name", STACK_NAME], check=True)
    log("Waiting for stack DELETE_COMPLETE (MSK teardown ~5-15 min) ...")
    run(["aws", "cloudformation", "wait", "stack-delete-complete", "--region", rg,
         "--stack-name", STACK_NAME], check=True)
    log("cdc-stack deleted. ✓")


# --------------------------------------------------------------------------- #
# Stage 1: drop the schema + objects on the DSQL target
# --------------------------------------------------------------------------- #
def stage_drop_target(args) -> None:
    if not args.yes:
        log(f"[plan] would DROP every object in DSQL schema '{SCHEMA}' "
            "(DESTRUCTIVE). Use --yes.")
        return
    from dsql_migrator.core.models import TargetConnectionConfig
    from dsql_migrator.core.target_connection import DsqlConnector

    cfgt = TargetConnectionConfig(
        cluster_endpoint=cfg("TARGET_ENDPOINT"), region=region(),
        database=cfg("TARGET_DATABASE", "postgres"),
        username=cfg("TARGET_USERNAME", "admin"),
    )
    conn = DsqlConnector(cfgt, aws_profile=os.environ.get("AWS_PROFILE")).connect()
    conn.autocommit = True
    cur = conn.cursor()
    # DSQL has no DROP SCHEMA CASCADE for non-empty schemas in all cases; drop each
    # table explicitly (idempotent), then the schema. Tables are dropped in
    # reverse-dependency order is unnecessary here (no FKs on DSQL).
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
# Stage 2: start the production-like workload loop (background)
# --------------------------------------------------------------------------- #
def stage_workload(args) -> None:
    # Already running?
    if os.path.exists(WORKLOAD_PIDFILE):
        pid = open(WORKLOAD_PIDFILE).read().strip()
        if pid and os.path.exists(f"/proc/{pid}") or _pid_alive(pid):
            log(f"Workload loop already running (pid {pid}). ✓")
            return
    # Workload generator is per-schema (cdc_workload_<schema>.py) so a new test
    # schema brings its own FK-aware generator without touching the proven one.
    script = os.path.join(_ROOT, "scripts", f"cdc_workload_{SCHEMA}.py")
    if not os.path.exists(script):
        raise SystemExit(
            f"workload generator not found: {script} — add a "
            f"cdc_workload_{SCHEMA}.py for this schema."
        )
    log(f"Starting workload loop every {args.interval}s -> {WORKLOAD_LOG}")
    if not args.yes:
        log("[plan] would start the source workload loop (writes to source). Use --yes.")
        return
    logf = open(WORKLOAD_LOG, "ab")
    proc = subprocess.Popen(
        [PY, script, "--schema", SCHEMA, "--interval", str(args.interval)],
        stdout=logf, stderr=logf, env={**os.environ},
        start_new_session=True,
    )
    open(WORKLOAD_PIDFILE, "w").write(str(proc.pid))
    log(f"Workload loop started (pid {proc.pid}). Stop later with: "
        f"kill $(cat {WORKLOAD_PIDFILE})")


def _pid_alive(pid: str) -> bool:
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:  # noqa: BLE001
        return False


def stage_stop_workload() -> None:
    if not os.path.exists(WORKLOAD_PIDFILE):
        log("No workload pidfile — nothing to stop.")
        return
    pid = open(WORKLOAD_PIDFILE).read().strip()
    if pid and _pid_alive(pid):
        os.kill(int(pid), 15)
        log(f"Stopped workload loop (pid {pid}).")
    os.remove(WORKLOAD_PIDFILE)


# --------------------------------------------------------------------------- #
# Stage 3: Full Load + row compare
# --------------------------------------------------------------------------- #
def stage_full_load(args) -> None:
    harness = os.path.join(_ROOT, "scripts", "run_full_load_harness.py")
    cmd = [PY, harness, "--watermark-out", WATERMARK_FILE]
    if args.yes:
        cmd.append("--yes")
    run(cmd, check=True)
    if not args.yes:
        return
    log("Full Load done. Comparing source vs target rows ...")
    _compare_rows(strict=False)  # CDC not started yet -> differences expected


# --------------------------------------------------------------------------- #
# Stage 4: recreate infra + start CDC from the watermark (gapless)
# --------------------------------------------------------------------------- #
# CFN param base for the cdc-stack create/update. Per-schema via CDC_SAVED_PARAMS
# (a path, absolute or repo-root-relative) so a new schema selects its own param
# file (TableIncludeList / SinkTopics) without overwriting the proven default.
_DEFAULT_SAVED_PARAMS = os.path.join(_ROOT, "deploy", "cdc-stack", "e2e", "saved-params.json")
_SAVED_PARAMS = cfg("CDC_SAVED_PARAMS", "")
if _SAVED_PARAMS and not os.path.isabs(_SAVED_PARAMS):
    _SAVED_PARAMS = os.path.join(_ROOT, _SAVED_PARAMS)
_SAVED_PARAMS = _SAVED_PARAMS or _DEFAULT_SAVED_PARAMS
_TEMPLATE = os.path.join(_ROOT, "deploy", "cdc-stack", "cdc-stack.yaml")


def _params_with(overrides: dict) -> str:
    """Write a CloudFormation --parameters file from saved params + overrides.

    Returns the temp file path. Keys in ``overrides`` replace the saved value;
    everything else is carried forward verbatim so the recreate is faithful.
    """
    import json
    import tempfile

    base = {p["ParameterKey"]: p["ParameterValue"]
            for p in json.load(open(_SAVED_PARAMS))}
    base.update(overrides)
    rows = [{"ParameterKey": k, "ParameterValue": v} for k, v in base.items()]
    fd, path = tempfile.mkstemp(suffix=".json", prefix="cdc-params-")
    with os.fdopen(fd, "w") as f:
        json.dump(rows, f)
    return path


def _msk_bootstrap(rg: str) -> str:
    """Return the (new) MSK Serverless IAM bootstrap string, or '' if not found."""
    clusters = aws_json([
        "kafka", "list-clusters-v2", "--region", rg,
        "--query", "ClusterInfoList[?contains(ClusterName,'dsql')].ClusterArn",
    ]) or []
    if not clusters:
        return ""
    bb = aws_json([
        "kafka", "get-bootstrap-brokers", "--region", rg,
        "--cluster-arn", clusters[0],
        "--query", "BootstrapBrokerStringSaslIam",
    ])
    return bb or ""


def _load_watermark_params() -> list:
    """Load WATERMARK_FILE and return the cdc-stack Watermark* parameter pairs.

    Reuses core.cdc.build_watermark_params (the same builder the UI/deployer use)
    so the E2E seeds byte-identically to production.
    """
    import json as _json
    from datetime import datetime as _dt

    from dsql_migrator.core.cdc import build_watermark_params
    from dsql_migrator.core.models import Watermark

    data = _json.load(open(WATERMARK_FILE))
    ts = data.get("snapshot_timestamp")
    snap = _dt.fromisoformat(ts) if isinstance(ts, str) else datetime.now(timezone.utc)
    wm = Watermark(
        binlog_file=data.get("binlog_file"),
        binlog_position=data.get("binlog_position"),
        gtid_executed=data.get("gtid_executed"),
        server_uuid=data.get("server_uuid"),
        snapshot_timestamp=snap,
    )
    return build_watermark_params(wm)


def _upload_artifacts_for_e2e(rg: str):
    """Ensure the managed bucket + upload all three artifacts; return the result.

    Reuses core.s3_provision.ensure_and_upload_plugins so the E2E uploads the SAME
    committed artifacts the app deploys (including the v6 source worker config and
    the offset-seeder Lambda zip), and stamps PluginVersion/LambdaSeederS3Key from
    the upload rather than the stale saved-params.json.
    """
    from dsql_migrator.core.s3_provision import (
        build_s3_client,
        build_sts_client,
        ensure_and_upload_plugins,
    )

    s3 = build_s3_client(None, rg)
    sts = build_sts_client(None, rg)
    return ensure_and_upload_plugins(s3, sts, rg, on_progress=lambda m: log(f"  {m}"))


def _upload_template_to_bucket(rg: str, bucket: str) -> str:
    """Upload the cdc-stack template to ``bucket`` and return its https URL.

    The template exceeds CloudFormation's 51,200-byte inline limit, so it must be
    passed via --template-url (S3, up to ~460 KB).
    """
    from dsql_migrator.core.s3_provision import build_s3_client

    key = "cdc-plugins/cdc-stack.yaml"
    s3 = build_s3_client(None, rg)
    with open(_TEMPLATE, "rb") as fh:
        s3.put_object(Bucket=bucket, Key=key, Body=fh.read())
    # Region-specific virtual-hosted endpoint (NOT the global s3.amazonaws.com which
    # targets us-east-1): a bucket outside us-east-1 returns PermanentRedirect via the
    # global endpoint and CloudFormation rejects the TemplateURL.
    return f"https://{bucket}.s3.{rg}.amazonaws.com/{key}"


def stage_start_cdc(args) -> None:
    rg = region()
    if not args.yes:
        log("[plan] would: upload artifacts -> create_stack (infra + seeder key, no "
            "connectors) -> wait -> start the SOURCE connector WITH the watermark "
            "(the in-VPC seeder Lambda seeds the gapless offset automatically) -> "
            f"add the SINK. Use --yes. (needs {WATERMARK_FILE} from stage 3)")
        return
    if not os.path.exists(WATERMARK_FILE):
        raise SystemExit(
            f"watermark file {WATERMARK_FILE} missing — run stage 3 (full-load) first."
        )

    # Upload the committed artifacts (gets the current PluginVersion + the seeder
    # Lambda zip key) and load the gapless watermark params from stage 3's file.
    log("Uploading connector + seeder artifacts to the managed plugin bucket ...")
    upload = _upload_artifacts_for_e2e(rg)
    log(f"  PluginVersion={upload.plugin_version}, seeder={upload.lambda_seeder_key}")
    watermark_params = dict(_load_watermark_params())
    log(f"  watermark: {watermark_params.get('WatermarkBinlogFile')}"
        f":{watermark_params.get('WatermarkBinlogPos')}")

    # Artifact params stamped from the upload, so the stale saved-params.json never
    # pins an old PluginVersion or a missing seeder key.
    artifact_params = {
        "PluginBucketArn": upload.bucket_arn,
        "DebeziumPluginS3Key": upload.debezium_key,
        "DsqlSinkPluginS3Key": upload.dsql_sink_key,
        "LambdaSeederS3Key": upload.lambda_seeder_key,
        "PluginVersion": upload.plugin_version,
    }

    # The cdc-stack template exceeds CloudFormation's 51,200-byte inline
    # --template-body limit, so stage it in the managed plugin bucket and use
    # --template-url (S3 allows up to ~460 KB). Upload once; create-stack uses it.
    template_url = _upload_template_to_bucket(rg, upload.bucket_name)
    log(f"  staged template -> {template_url}")

    # Pass 0 — create the infra with NO connectors yet (MskBootstrapServers='' +
    # DeploySink='false'), exactly like run_cdc_infra_deploy. The LambdaSeederS3Key
    # is set now so it PERSISTS; the seeder itself is only created at Pass A (when
    # the bootstrap string + watermark make SeedOffset true). MSK takes ~15-20 min.
    log("create_stack: CDC infra (MSK/VPC/plugins/IAM, no connectors) ...")
    pfile = _params_with({
        **artifact_params,
        "MskBootstrapServers": "",
        "DeploySink": "false",
    })
    run(["aws", "cloudformation", "create-stack", "--region", rg,
         "--stack-name", STACK_NAME, "--template-url", template_url,
         "--parameters", f"file://{pfile}", "--capabilities", "CAPABILITY_NAMED_IAM"],
        check=True)
    log("Waiting for infra CREATE_COMPLETE (MSK Serverless ~15-20 min) ...")
    run(["aws", "cloudformation", "wait", "stack-create-complete", "--region", rg,
         "--stack-name", STACK_NAME], check=True)

    bootstrap = _msk_bootstrap(rg)
    if not bootstrap:
        raise SystemExit("could not read the new MSK bootstrap brokers.")
    log(f"MSK bootstrap: {bootstrap}")

    # Pass A — create the SOURCE connector (DeploySink=false). The Watermark* params
    # make the in-VPC OffsetSeeder Lambda create + seed the compacted offsets topic
    # BEFORE the source connector is created (the connector implicitly depends on
    # the seed), for a fully automatic gapless handoff — no bastion / SSM needed.
    log("update_stack Pass A: seed offset + create the Debezium SOURCE connector ...")
    pfile = _params_with({
        **artifact_params,
        **watermark_params,
        "MskBootstrapServers": bootstrap,
        "DeploySink": "false",
    })
    run(["aws", "cloudformation", "update-stack", "--region", rg,
         "--stack-name", STACK_NAME, "--use-previous-template",
         "--parameters", f"file://{pfile}", "--capabilities", "CAPABILITY_NAMED_IAM"],
        check=True)
    run(["aws", "cloudformation", "wait", "stack-update-complete", "--region", rg,
         "--stack-name", STACK_NAME], check=True)

    # Pass B — add the SINK connector.
    log("update_stack Pass B: add the DSQL SINK connector ...")
    pfile = _params_with({
        **artifact_params,
        **watermark_params,
        "MskBootstrapServers": bootstrap,
        "DeploySink": "true",
    })
    run(["aws", "cloudformation", "update-stack", "--region", rg,
         "--stack-name", STACK_NAME, "--use-previous-template",
         "--parameters", f"file://{pfile}", "--capabilities", "CAPABILITY_NAMED_IAM"],
        check=True)
    run(["aws", "cloudformation", "wait", "stack-update-complete", "--region", rg,
         "--stack-name", STACK_NAME], check=True)
    log("CDC started (source + sink), gapless offset seeded automatically. ✓")


# --------------------------------------------------------------------------- #
# Stage 5: CDC consistency check
# --------------------------------------------------------------------------- #
def stage_cdc_check(args) -> None:
    log(f"Letting CDC catch up ({args.cdc_settle}s) ...")
    if args.yes:
        time.sleep(args.cdc_settle)
    check = os.path.join(_ROOT, "scripts", "cdc_consistency_check.py")
    if os.path.exists(check):
        run([PY, check], check=False)
    log("Comparing source vs target rows (CDC should have converged) ...")
    _compare_rows(strict=False)


# --------------------------------------------------------------------------- #
# Stage 6: Validation (authoritative verdict)
# --------------------------------------------------------------------------- #
def stage_validate(args) -> None:
    log("Running validation (authoritative cut-over verdict) ...")
    rc = _run_validation()
    log(f"Validation exit={rc} (0 = MATCH / ready, non-zero = mismatch).")


# --------------------------------------------------------------------------- #
# Shared: row compare + validation (reuse existing tools)
# --------------------------------------------------------------------------- #
def _compare_rows(*, strict: bool) -> int:
    cmp_script = os.path.join(_ROOT, "scripts", "compare_rows.py")
    targets = []
    for t in TABLES:
        targets += ["-t", f"{SCHEMA}.{t}"]
    rc = run([PY, cmp_script, *targets], check=False)
    if strict and rc != 0:
        raise SystemExit("row compare reported a mismatch (strict).")
    return rc


def _run_validation() -> int:
    """Run the tool's Validator over the schema tables (headless) and print a verdict."""
    from dsql_migrator.config import SecretValue
    from dsql_migrator.core.models import (
        SourceConnectionConfig, TargetConnectionConfig, ValidationMode,
    )
    from dsql_migrator.core.table_selection import TableSelection, TableSelector
    from dsql_migrator.core.validator import Validator
    from dsql_migrator.ui.connect import make_source_engine_factory
    from dsql_migrator.ui.evaluation import _default_introspector_factory

    pwd = cfg("DB_PASSWORD")
    source = SourceConnectionConfig(
        host=cfg("DB_HOST"), port=int(cfg("DB_PORT", "3306")),
        database=SCHEMA, username=cfg("DB_USER", "admin"),
    )
    target = TargetConnectionConfig(
        cluster_endpoint=cfg("TARGET_ENDPOINT"), region=region(),
        database=cfg("TARGET_DATABASE", "postgres"),
        username=cfg("TARGET_USERNAME", "admin"),
    )
    password = SecretValue(pwd)
    inventory = _default_introspector_factory(password).introspect(source)
    # Match the Full Load harness: validate against the SCHEMA-qualified target
    # (the CDC sink + Full Load both write to "<SCHEMA>"."<table>"). Single-db
    # introspection returns unqualified names, so qualify them in place; the
    # validator then reads the source as `schema`.`table` and the target as
    # "schema"."table", consistent with where the rows actually live.
    present = {t.name: t for t in inventory.tables}
    wanted: list[str] = []
    for t in TABLES:
        qualified = f"{SCHEMA}.{t}"
        if qualified in present:
            wanted.append(qualified)
        elif t in present:
            present[t].name = qualified
            wanted.append(qualified)
    tables = TableSelector().resolve(inventory, TableSelection(selected_tables=wanted))
    # Inject the in-memory source password into the validator's MySQL engine
    # (the default factory builds a passwordless URL -> "using password: NO").
    # The target is DSQL IAM-token auth, so it needs no secret here.
    report = Validator(
        source_engine_factory=make_source_engine_factory(password),
    ).validate(
        source, target, list(tables), ValidationMode.ROW_COUNT,
        reconcile=True, max_workers=int(cfg("DSQL_MIGRATOR_VALIDATE_MAX_WORKERS", "4")),
    )
    matched = sum(1 for i in report.items if i.matched)
    log(f"Validation: {matched}/{len(report.items)} tables matched; "
        f"is_match={report.is_match}")
    for item in report.items:
        if not item.matched:
            log(f"  MISMATCH {item.table}: src={item.source_row_count} "
                f"tgt={item.target_row_count}"
                + (f" missing={item.reconcile.missing_on_target} "
                   f"extra={item.reconcile.extra_on_target}"
                   if item.reconcile else ""))
    return 0 if report.is_match else 1


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
_STAGE_FUNCS = {
    "reset-infra": stage_reset_infra,
    "drop-target": stage_drop_target,
    "workload": stage_workload,
    "full-load": stage_full_load,
    "start-cdc": stage_start_cdc,
    "cdc-check": stage_cdc_check,
    "validate": stage_validate,
}


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--yes", action="store_true",
                    help="actually run (destructive stages need this)")
    ap.add_argument("--plan", action="store_true", help="print the plan and exit")
    ap.add_argument("--from-stage", choices=STAGES, default=STAGES[0],
                    help="start at this stage (resume)")
    ap.add_argument("--only", choices=STAGES, help="run only this one stage")
    ap.add_argument("--interval", type=float, default=1.0,
                    help="workload op interval seconds (default 1)")
    ap.add_argument("--cdc-settle", type=float, default=120.0,
                    help="seconds to let CDC catch up before the cdc-check compare")
    ap.add_argument("--stop-workload", action="store_true",
                    help="stop the background workload loop and exit")
    args = ap.parse_args()

    if args.stop_workload:
        stage_stop_workload()
        return 0

    order = list(STAGES)
    if args.only:
        selected = [args.only]
    else:
        selected = order[order.index(args.from_stage):]

    log(f"E2E migration — schema={SCHEMA} stack={STACK_NAME} region={region()}")
    log(f"Stages: {', '.join(selected)}  (yes={args.yes})")
    if args.plan or not args.yes:
        log("[plan] re-run with --yes to execute. DESTRUCTIVE stages: "
            "reset-infra (delete stack), drop-target (drop DSQL schema).")
    for stage in selected:
        log(f"===== STAGE: {stage} =====")
        _STAGE_FUNCS[stage](args)
    log("E2E run complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
