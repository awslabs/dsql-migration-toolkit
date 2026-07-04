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
#                    "off" = prefetch disabled/baseline, "on" = prefetch enabled)
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

q() { aws cloudformation describe-stacks --stack-name "${APP_STACK}" --region "${REGION}" \
        --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue|[0]" --output text; }

CLUSTER="$(q ClusterName)"
SERVICE="$(q ServiceName)"
[ -z "${CLUSTER}" ] || [ "${CLUSTER}" = "None" ] && { echo "error: stack ${APP_STACK} not found." >&2; exit 1; }

echo "==> Cluster: ${CLUSTER}  Service: ${SERVICE}  Region: ${REGION}"

# Reuse the service's task definition + network config (subnets/SG/public-IP), so
# the measurement task lands on the same network path as the app.
TASK_DEF="$(aws ecs describe-services --cluster "${CLUSTER}" --services "${SERVICE}" \
  --region "${REGION}" --query "services[0].taskDefinition" --output text)"
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
build_cmd_json() {
  local report_path="$1"
  python3 - "$MEASURE_SCHEMA" "$MEASURE_TABLES" "$TABLE_PARALLELISM" "$BATCH_PARALLELISM" \
            "$PROGRESS_INTERVAL" "$report_path" "$2" <<'PY'
import json, sys
schema, tables, tp, bp, interval, report, no_prefetch = sys.argv[1:8]
measure = ["python", "scripts/measure_performance.py", "full-load", "--yes",
           "--schema", schema, "--table-parallelism", tp, "--batch-parallelism", bp,
           "--progress-interval", interval, "--report", report]
if tables.strip():
    measure += ["--tables", *tables.split()]
if no_prefetch == "1":
    measure += ["--no-prefetch"]
# sh -c: run the measure, then emit the report between markers so the wrapper can
# recover it from CloudWatch Logs (the task's /tmp is lost when it stops).
inner = (" ".join(measure) +
         " ; echo '===REPORT-BEGIN==='"
         " ; cat " + report +
         " ; echo '===REPORT-END==='")
print(json.dumps(["sh", "-c", inner]))
PY
}

# Overrides: container command + source-DB env (target DSQL comes from the task-def
# env + IAM task role). Adds DSQL_MIGRATOR_FULL_LOAD_PREFETCH for the variant.
build_overrides_json() {
  local cmd_json="$1" prefetch_env="$2"
  python3 - "$cmd_json" "$DB_HOST" "$DB_PORT" "$DB_USER" "$DB_PASSWORD" "$MEASURE_SCHEMA" \
            "$CONTAINER" "$prefetch_env" <<'PY'
import json, sys
cmd = json.loads(sys.argv[1])
host, port, user, pw, schema, container, prefetch = sys.argv[2:9]
env = [
    {"name": "DB_HOST", "value": host},
    {"name": "DB_PORT", "value": port},
    {"name": "DB_USER", "value": user},
    {"name": "DB_PASSWORD", "value": pw},
    {"name": "CDC_WORKLOAD_SCHEMA", "value": schema},
    {"name": "DSQL_MIGRATOR_FULL_LOAD_PREFETCH", "value": prefetch},
]
print(json.dumps({"containerOverrides": [
    {"name": container, "command": cmd, "environment": env}
]}))
PY
}

run_variant() {
  local variant="$1"          # "off" or "on"
  local no_prefetch prefetch_env
  if [ "${variant}" = "off" ]; then no_prefetch=1; prefetch_env=0; else no_prefetch=0; prefetch_env=1; fi

  local report_in_task="/tmp/perf-${variant}.json"
  local cmd_json overrides
  cmd_json="$(build_cmd_json "${report_in_task}" "${no_prefetch}")"
  overrides="$(build_overrides_json "${cmd_json}" "${prefetch_env}")"

  echo ""
  echo "======================================================================"
  echo "==> Variant: prefetch=${variant}  schema=${MEASURE_SCHEMA} tables='${MEASURE_TABLES:-ALL}' tp=${TABLE_PARALLELISM} bp=${BATCH_PARALLELISM}"
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
  aws ecs wait tasks-stopped --cluster "${CLUSTER}" --tasks "${task_arn}" --region "${REGION}"

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
  aws logs get-log-events --log-group-name "${LOG_GROUP}" --log-stream-name "${stream}" \
    --region "${REGION}" --start-from-head --output text \
    --query "events[*].message" 2>/dev/null \
    | tr '\t' '\n' \
    | awk '/===REPORT-BEGIN===/{f=1;next} /===REPORT-END===/{f=0} f' > "${out_report}" || true

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
echo "Compare with:"
echo "  python scripts/perf_compare.py compare ${OUT_DIR}/off-in-vpc.json ${OUT_DIR}/on-in-vpc.json --per-table"
