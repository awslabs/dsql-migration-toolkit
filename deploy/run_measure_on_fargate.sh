#!/usr/bin/env bash
#
# Run Full Load PERFORMANCE measurement inside AWS Fargate, in the same VPC as the
# source RDS / target DSQL -- so the numbers reflect the real in-VPC network path
# (sub-ms RTT) rather than a laptop over VPN. This is the environment the prefetch
# queue was designed for: a LOCAL (long-RTT) A/B showed ~0% because reads and writes
# share the fixed VPN pipe, so the honest verdict was "re-measure in-VPC" (see the
# Obsidian note "Full Load 처리량 병목 분석과 개선 방향 — 2026-07-04" §7).
#
# By default it runs BOTH variants back-to-back on the SAME deployed image:
#   - prefetch=off  (DSQL_MIGRATOR_FULL_LOAD_PREFETCH=0)  -> the pre-improvement path
#   - prefetch=on   (default)                             -> the read-ahead queue
# ECS RunTask cannot override the container IMAGE (it is fixed in the task-def), but
# it CAN override the command + environment, so one image measures both variants via
# that env toggle -- no second build. Set VARIANTS to run a subset.
#
# Report recovery: a Fargate task's /tmp is gone once it stops, so the JSON report
# is echoed to stdout between markers (===REPORT-BEGIN=== / ===REPORT-END===) and
# this script pulls it back from CloudWatch Logs after the task stops -- needs NO new
# IAM (the task already logs via awslogs). Reports land in perf-runs/<variant>-in-vpc.json.
#
# It reuses the deployed app image (which bundles scripts/) and the app task
# definition (DSQL IAM task role + execution role already set). Network config
# (subnets, security group, public-IP) is read from the running ECS service.
#
# Prerequisites:
#   - The image you want to measure is pushed to ECR and the app-stack task
#     definition references it (deploy that image first via deploy/build_in_codebuild.sh).
#   - scripts/ is in the image (deploy/Dockerfile COPYs it).
#
# Usage:
#   AWS_REGION=us-east-1 \
#   APP_STACK=customer-migration-test \
#   DB_HOST=<rds-host> DB_PORT=3306 DB_USER=admin DB_PASSWORD=<pw> \
#   MEASURE_SCHEMA=customers_sample MEASURE_TABLES="payments orders" \
#   deploy/run_measure_on_fargate.sh
#
# Env vars:
#   AWS_REGION       (required, or a configured default)
#   APP_STACK        app-stack name (default: customer-migration-test)
#   DB_HOST/PORT/USER/PASSWORD   source MySQL connection (PASSWORD is passed as a
#                    task env override -- prefer a short-lived/test credential; for
#                    production use a Secrets Manager secret + task-def `secrets`).
#   MEASURE_SCHEMA   schema to load (default: customers_sample)
#   MEASURE_TABLES   space-separated table subset (default: all registered)
#   VARIANTS         which variants to run, space-separated (default: "off on";
#                    "off" = prefetch disabled/baseline, "on" = prefetch enabled,
#                    "shardN" = prefetch + N reader shards, "keep" = composite-A/B
#                    baseline (integer PK), "composite" = composite PK on
#                    COMPOSITE_LEADING). For the composite A/B: VARIANTS="keep composite".
#   COMPOSITE_LEADING  high-cardinality column prepended to each table's PK for the
#                    "composite" variant (e.g. customer_id; required by that variant).
#   TABLE_PARALLELISM / BATCH_PARALLELISM  loader knobs (default 4 / 8)
#   PROGRESS_INTERVAL  seconds between live progress logs (default 30)
#   OUT_DIR          local dir for recovered reports (default: perf-runs)
#
# Safety: measure_performance.py full-load is DESTRUCTIVE (DROP+recreate the target
# tables in MEASURE_SCHEMA). Run only against a NON-PRODUCTION target.

set -euo pipefail

: "${APP_STACK:=customer-migration-test}"
: "${MEASURE_SCHEMA:=customers_sample}"
: "${MEASURE_TABLES:=}"
: "${VARIANTS:=off on}"
: "${TABLE_PARALLELISM:=4}"
: "${BATCH_PARALLELISM:=8}"
: "${PROGRESS_INTERVAL:=30}"
: "${DB_PORT:=3306}"
: "${DB_USER:=admin}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
: "${OUT_DIR:=${REPO_ROOT}/perf-runs}"

REGION="${AWS_REGION:-$(aws configure get region || true)}"
[ -z "${REGION}" ] && { echo "error: set AWS_REGION." >&2; exit 1; }
[ -z "${DB_HOST:-}" ] && { echo "error: set DB_HOST (source MySQL)." >&2; exit 1; }
[ -z "${DB_PASSWORD:-}" ] && { echo "error: set DB_PASSWORD (source MySQL)." >&2; exit 1; }
[ -z "${TARGET_ENDPOINT:-}" ] && { echo "error: set TARGET_ENDPOINT (Aurora DSQL cluster endpoint; the app task-def does not bake one in). Source it from .env." >&2; exit 1; }

q() { aws cloudformation describe-stacks --stack-name "${APP_STACK}" --region "${REGION}" \
        --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue|[0]" --output text; }

CLUSTER="$(q ClusterName)"
SERVICE="$(q ServiceName)"
[ -z "${CLUSTER}" ] || [ "${CLUSTER}" = "None" ] && { echo "error: stack ${APP_STACK} not found." >&2; exit 1; }

echo "==> Cluster: ${CLUSTER}  Service: ${SERVICE}  Region: ${REGION}"

# Reuse the service's task definition + network config (subnets/SG/public-IP), so
# the measurement task lands on the same network path as the app. TASK_DEF can be
# overridden (e.g. a revision pinned to a freshly built image tag) so the measurement
# runs a different image than the live UI service without redeploying that service.
TASK_DEF="${TASK_DEF:-$(aws ecs describe-services --cluster "${CLUSTER}" --services "${SERVICE}" \
  --region "${REGION}" --query "services[0].taskDefinition" --output text)}"
NETCFG="$(aws ecs describe-services --cluster "${CLUSTER}" --services "${SERVICE}" \
  --region "${REGION}" --query "services[0].networkConfiguration" --output json)"
echo "==> Task definition: ${TASK_DEF}"

# Resolve the container name + awslogs group/prefix from the task-def so we can pull
# the report back from CloudWatch after the task stops (no hardcoding).
read -r CONTAINER LOG_GROUP LOG_PREFIX < <(aws ecs describe-task-definition \
  --task-definition "${TASK_DEF}" --region "${REGION}" \
  --query "taskDefinition.containerDefinitions[0].[name,logConfiguration.options.\"awslogs-group\",logConfiguration.options.\"awslogs-stream-prefix\"]" \
  --output text)
echo "==> Container: ${CONTAINER}  LogGroup: ${LOG_GROUP}  StreamPrefix: ${LOG_PREFIX}"

mkdir -p "${OUT_DIR}"

# Build the container command for one variant. Wraps the measure run so the JSON
# report is echoed to stdout between markers for CloudWatch recovery.
#
# The measurement task-def sets entryPoint=["/bin/sh","-c"] (overriding the image's
# `mysql-dsql-migrator` ENTRYPOINT, which an ECS command override alone can NOT do --
# containerOverrides has no entryPoint field). So the command override is a
# SINGLE-element array holding the shell string that `sh -c` runs.
build_cmd_json() {
  local report_path="$1" no_prefetch="$2" reader_shards="${3:-0}" composite_leading="${4:-}"
  python3 - "$MEASURE_SCHEMA" "$MEASURE_TABLES" "$TABLE_PARALLELISM" "$BATCH_PARALLELISM" \
            "$PROGRESS_INTERVAL" "$report_path" "$no_prefetch" "$reader_shards" "$composite_leading" <<'PY'
import json, sys
schema, tables, tp, bp, interval, report, no_prefetch, reader_shards, composite_leading = sys.argv[1:10]
measure = ["python", "scripts/measure_performance.py", "full-load", "--yes",
           "--schema", schema, "--table-parallelism", tp, "--batch-parallelism", bp,
           "--progress-interval", interval, "--report", report]
if tables.strip():
    measure += ["--tables", *tables.split()]
if no_prefetch == "1":
    measure += ["--no-prefetch"]
if reader_shards and reader_shards not in ("0", "1"):
    # reader range sharding on: also lower the size threshold so payments/orders
    # (each ~9M) qualify regardless of the default 1M gate.
    measure += ["--reader-shards", reader_shards]
if composite_leading.strip():
    # Composite-PK variant: prepend this high-cardinality column to each table's
    # PK (a table lacking it is skipped by measure_performance, keeping its key).
    measure += ["--composite-leading", composite_leading]
# Run the measure, then emit the report between markers so the wrapper can recover
# it from CloudWatch Logs (the task's /tmp is lost when it stops). The report JSON
# has no trailing newline, so a bare `cat` would print `}===REPORT-END===` on one
# line and the marker-strip would drop the closing brace -> invalid JSON. Emit the
# markers on their OWN lines (echo before/after, and a newline after cat).
inner = (" ".join(measure) +
         " ; echo '' ; echo '===REPORT-BEGIN==='"
         " ; cat " + report + " ; echo ''"
         " ; echo '===REPORT-END==='")
print(json.dumps([inner]))  # single arg for `sh -c`
PY
}

# Overrides: container command + source-DB env + target-DSQL env. The app task-def
# does NOT bake in a DSQL target (the UI collects it interactively), so the measure
# script's TARGET_ENDPOINT/REGION/DATABASE/USERNAME must be passed here (from the
# caller's env, typically sourced from .env). The DSQL IAM auth uses the task role.
# Adds DSQL_MIGRATOR_FULL_LOAD_PREFETCH for the variant.
build_overrides_json() {
  local cmd_json="$1" prefetch_env="$2"
  python3 - "$cmd_json" "$DB_HOST" "$DB_PORT" "$DB_USER" "$DB_PASSWORD" "$MEASURE_SCHEMA" \
            "$CONTAINER" "$prefetch_env" \
            "${TARGET_ENDPOINT:-}" "${TARGET_REGION:-}" "${TARGET_DATABASE:-}" "${TARGET_USERNAME:-}" <<'PY'
import json, sys
cmd = json.loads(sys.argv[1])
host, port, user, pw, schema, container, prefetch = sys.argv[2:9]
tgt_ep, tgt_region, tgt_db, tgt_user = sys.argv[9:13]
env = [
    {"name": "DB_HOST", "value": host},
    {"name": "DB_PORT", "value": port},
    {"name": "DB_USER", "value": user},
    {"name": "DB_PASSWORD", "value": pw},
    {"name": "CDC_WORKLOAD_SCHEMA", "value": schema},
    {"name": "DSQL_MIGRATOR_FULL_LOAD_PREFETCH", "value": prefetch},
]
# Target DSQL (only set the ones provided; measure_performance defaults region from
# the endpoint and database/username to postgres/admin).
for name, val in (("TARGET_ENDPOINT", tgt_ep), ("TARGET_REGION", tgt_region),
                  ("TARGET_DATABASE", tgt_db), ("TARGET_USERNAME", tgt_user)):
    if val:
        env.append({"name": name, "value": val})
print(json.dumps({"containerOverrides": [
    {"name": container, "command": cmd, "environment": env}
]}))
PY
}

run_variant() {
  local variant="$1"          # "off" | "on" | "shardN" | "keep" | "composite"
  # Map the variant name to (prefetch on/off, reader-shards, composite-leading).
  # "off" = pre-prefetch baseline; "on" = prefetch, single reader; "shardN" =
  # prefetch + N shard readers; "keep" = prefetch on, integer PK (composite A/B
  # baseline); "composite" = prefetch on + composite PK on COMPOSITE_LEADING.
  local no_prefetch prefetch_env reader_shards=0 composite_leading=""
  case "${variant}" in
    off)        no_prefetch=1; prefetch_env=0 ;;
    on)         no_prefetch=0; prefetch_env=1 ;;
    shard*)     no_prefetch=0; prefetch_env=1; reader_shards="${variant#shard}" ;;
    keep)       no_prefetch=0; prefetch_env=1 ;;
    composite)  no_prefetch=0; prefetch_env=1; composite_leading="${COMPOSITE_LEADING:?set COMPOSITE_LEADING for the composite variant}" ;;
    *)          no_prefetch=0; prefetch_env=1 ;;
  esac

  local report_in_task="/tmp/perf-${variant}.json"
  local cmd_json overrides
  cmd_json="$(build_cmd_json "${report_in_task}" "${no_prefetch}" "${reader_shards}" "${composite_leading}")"
  overrides="$(build_overrides_json "${cmd_json}" "${prefetch_env}")"

  echo ""
  echo "======================================================================"
  echo "==> Variant: ${variant}  (prefetch=${prefetch_env} reader_shards=${reader_shards} composite_leading='${composite_leading}')  schema=${MEASURE_SCHEMA} tables='${MEASURE_TABLES:-ALL}' tp=${TABLE_PARALLELISM} bp=${BATCH_PARALLELISM}"
  echo "======================================================================"
  local task_arn
  task_arn="$(aws ecs run-task --cluster "${CLUSTER}" --region "${REGION}" \
    --task-definition "${TASK_DEF}" \
    --launch-type FARGATE \
    --network-configuration "${NETCFG}" \
    --overrides "${overrides}" \
    --started-by "perf-${variant}" \
    --query "tasks[0].taskArn" --output text)"
  [ -z "${task_arn}" ] || [ "${task_arn}" = "None" ] && { echo "error: run-task failed for variant ${variant}." >&2; return 1; }
  local task_id="${task_arn##*/}"
  echo "==> Task: ${task_arn}"
  echo "    Waiting for the task to stop (this runs a full DROP+recreate load)..."
  # NOT `aws ecs wait tasks-stopped`: that caps at 100x6s = 10min, but a multi-million
  # row load runs far longer, so the waiter would give up mid-load. Poll lastStatus
  # until STOPPED with no fixed attempt cap (guarded by MAX_WAIT_MIN, default 6h).
  local max_wait_min="${MAX_WAIT_MIN:-360}"
  local waited=0
  while true; do
    local st
    st="$(aws ecs describe-tasks --cluster "${CLUSTER}" --tasks "${task_arn}" --region "${REGION}" \
      --query "tasks[0].lastStatus" --output text 2>/dev/null)"
    [ "${st}" = "STOPPED" ] && break
    if [ "${waited}" -ge "$(( max_wait_min * 60 ))" ]; then
      echo "warning: task ${task_id} still ${st} after ${max_wait_min}min; giving up the wait." >&2
      break
    fi
    sleep 30
    waited=$(( waited + 30 ))
  done

  local exit_code
  exit_code="$(aws ecs describe-tasks --cluster "${CLUSTER}" --tasks "${task_arn}" --region "${REGION}" \
    --query "tasks[0].containers[0].exitCode" --output text)"
  echo "==> Task stopped (container exitCode=${exit_code})"

  # Recover the report from CloudWatch Logs. ECS awslogs stream = <prefix>/<container>/<task-id>.
  local stream="${LOG_PREFIX}/${CONTAINER}/${task_id}"
  local out_report="${OUT_DIR}/${variant}-in-vpc.json"
  echo "==> Recovering report from CloudWatch: ${LOG_GROUP} :: ${stream}"
  # Give logs a moment to flush after the task stops.
  sleep 8
  # Extract between the markers. Robust to a marker glued to JSON on the same line
  # (e.g. `}===REPORT-END===`): on the BEGIN line keep only what follows the marker,
  # on the END line keep only what precedes it -- so the closing brace is never lost.
  aws logs get-log-events --log-group-name "${LOG_GROUP}" --log-stream-name "${stream}" \
    --region "${REGION}" --start-from-head --output text \
    --query "events[*].message" 2>/dev/null \
    | tr '\t' '\n' \
    | awk '
        /===REPORT-END===/   { sub(/===REPORT-END===.*/, ""); if (f && $0 != "") print; f=0; next }
        f                    { print }
        /===REPORT-BEGIN===/ { sub(/.*===REPORT-BEGIN===/, ""); f=1; if ($0 != "") print }
      ' > "${out_report}" || true

  if [ -s "${out_report}" ] && python3 -c "import json,sys; json.load(open('${out_report}'))" 2>/dev/null; then
    echo "==> Saved report -> ${out_report}"
  else
    echo "warning: could not recover a valid JSON report for variant ${variant} from CloudWatch." >&2
    echo "         Inspect the log stream manually: ${LOG_GROUP} :: ${stream}" >&2
    rm -f "${out_report}"
  fi
}

for v in ${VARIANTS}; do
  run_variant "${v}"
done

echo ""
echo "==> Done. Recovered reports in ${OUT_DIR}/:"
ls -1 "${OUT_DIR}"/*-in-vpc.json 2>/dev/null || echo "   (none recovered)"
echo ""
echo "Compare with (first = baseline):"
echo "  python scripts/perf_compare.py compare ${OUT_DIR}/*-in-vpc.json --per-table"
