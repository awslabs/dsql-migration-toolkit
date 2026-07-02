#!/usr/bin/env bash
# Raise max.message.bytes to 4 MiB on the CDC data topics (path A — non-destructive).
#
# WHY: MSK Serverless auto-creates each per-table data topic with max.message.bytes
# at the broker default (~1 MiB). Even with producer.max.request.size raised, the
# BROKER rejects a 1-4 MiB change event (RecordTooLargeException "... larger than the
# max message size the server will accept"), so the source task silently drops it and
# it never reaches the sink/DLQ. The cdc-stack.yaml fix
# (topic.creation.default.max.message.bytes) only applies to NEWLY created topics, so
# the ALREADY-created topics must be altered in place. This script does that without
# deleting topics or data.
#
# WHERE TO RUN: a host INSIDE the MSK Serverless VPC (EC2/bastion in the connector
# subnets, with the MSK security group allowing 9098) that has the AWS IAM auth jar
# and Apache Kafka CLI tools installed. MSK Serverless = IAM + VPC only; this cannot
# run from a laptop outside the VPC.
#
# PREREQUISITES on that host:
#   - Kafka CLI tools (kafka-configs.sh) on PATH or set KAFKA_BIN.
#   - aws-msk-iam-auth jar on the CLASSPATH (set in client.properties below).
#   - An IAM role/credentials allowed to alter topic configs on this cluster.
set -euo pipefail

# Required: the MSK Serverless IAM bootstrap broker string. No default -- do not
# bake a real cluster endpoint into the repo (read it from `aws kafka
# get-bootstrap-brokers` for your stack and export BOOTSTRAP=...).
BOOTSTRAP="${BOOTSTRAP:?Set BOOTSTRAP to the MSK Serverless IAM bootstrap brokers (host:9098)}"
MAX_BYTES="${MAX_BYTES:-4194304}"   # 4 MiB; MSK Serverless topic max is 8388608 (8 MiB)
PREFIX="${PREFIX:-dsqlcdc}"
KAFKA_BIN="${KAFKA_BIN:-}"          # e.g. /opt/kafka/bin ; empty = on PATH

# The 11 CDC data topics = <prefix>.<db>.<table> for the deployed table list.
TABLES=(
  customers_sample_new.categories
  customers_sample_new.countries
  customers_sample_new.customer_addresses
  customers_sample_new.customers
  customers_sample_new.order_items
  customers_sample_new.orders
  customers_sample_new.payments
  customers_sample_new.product_reviews
  customers_sample_new.products
  customers_sample_new.regions
  customers_sample_new.suppliers
)

# IAM SASL client config for MSK Serverless.
CLIENT_PROPS="$(mktemp)"
trap 'rm -f "$CLIENT_PROPS"' EXIT
cat > "$CLIENT_PROPS" <<'EOF'
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF

CFG="${KAFKA_BIN:+$KAFKA_BIN/}kafka-configs.sh"

echo "Bootstrap : $BOOTSTRAP"
echo "Set       : max.message.bytes=$MAX_BYTES on ${#TABLES[@]} topics"
echo

for t in "${TABLES[@]}"; do
  topic="${PREFIX}.${t}"
  echo "--- $topic ---"
  "$CFG" --bootstrap-server "$BOOTSTRAP" --command-config "$CLIENT_PROPS" \
    --entity-type topics --entity-name "$topic" \
    --alter --add-config "max.message.bytes=${MAX_BYTES}"
done

echo
echo "=== verify (should show max.message.bytes=$MAX_BYTES) ==="
for t in "${TABLES[@]}"; do
  topic="${PREFIX}.${t}"
  echo "--- $topic ---"
  "$CFG" --bootstrap-server "$BOOTSTRAP" --command-config "$CLIENT_PROPS" \
    --entity-type topics --entity-name "$topic" --describe \
    | grep -o 'max.message.bytes=[0-9]*' || echo "  (no explicit override shown)"
done

echo
echo "Done. Now re-run the DLQ simulation (insert one ~1.5 MiB row) — the source"
echo "should produce OK, the sink should reject on DSQL's 1 MiB limit and quarantine"
echo "to the DLQ, and the UI per-table 'Quarantined' count should increment."
