#!/usr/bin/env bash
#
# Windowed composite-PK A/B: run each variant for a FIXED WALL-CLOCK WINDOW (not to
# completion), then stop the task and read the throughput achieved in that window
# from CloudWatch. rows/s is a rate, so a 15-min sample is representative for an A/B
# and avoids the ~1h full-load per variant.
#
# For each variant: RunTask (4 vCPU task-def, composite/keep via --composite-leading)
#  -> poll the awslogs stream, capturing the last "progress: ... N rows ... X rows/s"
#     line inside the window -> after WINDOW_MIN, stop-task -> record the final
#     in-window rows/s (overall + per-table) + pull DSQL CommitLatency for the window.
#
# Env (same as run_measure_on_fargate.sh) + WINDOW_MIN (default 15).
set -euo pipefail

: "${APP_STACK:=customer-migration-test}"
: "${MEASURE_SCHEMA:=customers_sample}"
: "${MEASURE_TABLES:=orders payments}"
: "${VARIANTS:=keep composite}"
: "${TABLE_PARALLELISM:=4}"
: "${BATCH_PARALLELISM:=16}"
: "${WINDOW_MIN:=15}"
: "${DB_PORT:=3306}"
: "${DB_USER:=admin}"
: "${COMPOSITE_LEADING:=customer_id}"

REGION="${AWS_REGION:-us-east-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUT_DIR="${REPO_ROOT}/perf-runs/composite-ab-15min"
mkdir -p "${OUT_DIR}"

q() { aws cloudformation describe-stacks --stack-name "${APP_STACK}" --region "${REGION}" \
        --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue|[0]" --output text; }
CLUSTER="$(q ClusterName)"
SERVICE="$(q ServiceName)"
TASK_DEF="${TASK_DEF:?set TASK_DEF (the 4 vCPU composite-pk revision)}"
NETCFG="$(aws ecs describe-services --cluster "${CLUSTER}" --services "${SERVICE}" \
  --region "${REGION}" --query "services[0].networkConfiguration" --output json)"
read -r CONTAINER LOG_GROUP LOG_PREFIX < <(aws ecs describe-task-definition \
  --task-definition "${TASK_DEF}" --region "${REGION}" \
  --query "taskDefinition.containerDefinitions[0].[name,logConfiguration.options.\"awslogs-group\",logConfiguration.options.\"awslogs-stream-prefix\"]" \
  --output text)
CID="$(python3 -c "import sys;print('${TARGET_ENDPOINT}'.split('.')[0])")"

echo "==> cluster=${CLUSTER} taskdef=${TASK_DEF##*/} window=${WINDOW_MIN}min tp=${TABLE_PARALLELISM} bp=${BATCH_PARALLELISM}"

run_variant() {
  local variant="$1" composite_leading=""
  [ "${variant}" = "composite" ] && composite_leading="${COMPOSITE_LEADING}"

  # Build the measure command (no --report; we read progress from logs). recreate=drop.
  local inner
  inner="$(python3 - "$MEASURE_SCHEMA" "$MEASURE_TABLES" "$TABLE_PARALLELISM" "$BATCH_PARALLELISM" "$composite_leading" <<'PY'
import json,sys
schema,tables,tp,bp,lead=sys.argv[1:6]
m=["python","scripts/measure_performance.py","full-load","--yes","--schema",schema,
   "--table-parallelism",tp,"--batch-parallelism",bp,"--progress-interval","20"]
if tables.strip(): m+=["--tables",*tables.split()]
if lead.strip(): m+=["--composite-leading",lead]
print(json.dumps([" ".join(m)]))
PY
)"
  local overrides
  overrides="$(python3 - "$inner" "$DB_HOST" "$DB_PORT" "$DB_USER" "$DB_PASSWORD" "$MEASURE_SCHEMA" "$CONTAINER" "${TARGET_ENDPOINT}" <<'PY'
import json,sys
cmd=json.loads(sys.argv[1]); host,port,user,pw,schema,container,ep=sys.argv[2:9]
env=[{"name":"DB_HOST","value":host},{"name":"DB_PORT","value":port},{"name":"DB_USER","value":user},
     {"name":"DB_PASSWORD","value":pw},{"name":"CDC_WORKLOAD_SCHEMA","value":schema},
     {"name":"TARGET_ENDPOINT","value":ep},{"name":"DSQL_MIGRATOR_FULL_LOAD_PREFETCH","value":"1"}]
print(json.dumps({"containerOverrides":[{"name":container,"command":cmd,"environment":env}]}))
PY
)"

  echo ""
  echo "======================================================================"
  echo "==> Variant: ${variant} (composite_leading='${composite_leading}')  ${WINDOW_MIN}min window"
  echo "======================================================================"
  local start_epoch task_arn task_id
  start_epoch=$(date +%s)
  task_arn="$(aws ecs run-task --cluster "${CLUSTER}" --region "${REGION}" \
    --task-definition "${TASK_DEF}" --launch-type FARGATE \
    --network-configuration "${NETCFG}" --overrides "${overrides}" \
    --started-by "win-${variant}" --query "tasks[0].taskArn" --output text)"
  task_id="${task_arn##*/}"
  echo "==> task=${task_id}  (스트리밍 진행 ${WINDOW_MIN}min...)"
  local stream="${LOG_PREFIX}/${CONTAINER}/${task_id}"

  # Poll every 60s until WINDOW_MIN elapsed, printing the latest progress line.
  local deadline=$(( start_epoch + WINDOW_MIN*60 ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    sleep 60
    local line
    line="$(aws logs get-log-events --log-group-name "${LOG_GROUP}" --log-stream-name "${stream}" \
      --region "${REGION}" --output text --query "events[*].message" 2>/dev/null \
      | tr '\t' '\n' | grep -E "progress:" | tail -1)"
    echo "    [$(( ($(date +%s)-start_epoch)/60 ))min] ${line:-(로그 대기)}"
  done

  # Window elapsed: capture the final in-window progress, then stop the task.
  local final
  final="$(aws logs get-log-events --log-group-name "${LOG_GROUP}" --log-stream-name "${stream}" \
    --region "${REGION}" --output text --query "events[*].message" 2>/dev/null \
    | tr '\t' '\n' | grep -E "progress:|rows/s|orders|payments" | tail -12)"
  aws ecs stop-task --cluster "${CLUSTER}" --task "${task_arn}" --region "${REGION}" \
    --reason "15-min window elapsed" >/dev/null 2>&1 || true
  local end_epoch=$(date +%s)

  {
    echo "variant=${variant} composite_leading=${composite_leading}"
    echo "window_min=${WINDOW_MIN} start_epoch=${start_epoch} end_epoch=${end_epoch}"
    echo "task_id=${task_id}"
    echo "--- final in-window progress ---"
    echo "${final}"
  } > "${OUT_DIR}/${variant}-window.txt"
  echo "==> ${variant} 종료. 요약 -> ${OUT_DIR}/${variant}-window.txt"
  echo "${start_epoch} ${end_epoch}" > "${OUT_DIR}/${variant}-window.epoch"

  # brief settle so the next variant starts on a quiet target
  sleep 20
}

for v in ${VARIANTS}; do run_variant "$v"; done

echo ""
echo "==> DSQL CommitLatency (each 15-min window)"
for v in ${VARIANTS}; do
  read -r s e < "${OUT_DIR}/${v}-window.epoch"
  st="$(python3 -c "import datetime;print(datetime.datetime.utcfromtimestamp(${s}).strftime('%Y-%m-%dT%H:%M:%SZ'))")"
  et="$(python3 -c "import datetime;print(datetime.datetime.utcfromtimestamp(${e}).strftime('%Y-%m-%dT%H:%M:%SZ'))")"
  echo "--- ${v}: ${st} .. ${et} ---"
  aws cloudwatch get-metric-statistics --namespace AWS/AuroraDSQL --metric-name CommitLatency \
    --dimensions Name=ClusterId,Value=${CID} --start-time "${st}" --end-time "${et}" \
    --period 900 --statistics Average Maximum --extended-statistics p50 p90 p99 \
    --unit Milliseconds --region "${REGION}" \
    --query "Datapoints[].{avg:Average,p50:ExtendedStatistics.p50,p90:ExtendedStatistics.p90,p99:ExtendedStatistics.p99,max:Maximum}" \
    --output table 2>&1 | tail -8
done
echo ""
echo "==> done. windows in ${OUT_DIR}/"
