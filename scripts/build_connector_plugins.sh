#!/usr/bin/env bash
#
# Build the two MSK Connect plugin artifacts the cdc-stack uploads to S3:
#
#   connectors/plugins/debezium-mysql-plugin.zip   (Debezium MySQL source)
#   connectors/plugins/dsql-sink-plugin.zip        (custom DSQL sink, single jar)
#
# Converters: the cdc-stack worker configs use the built-in JSON converter
# (org.apache.kafka.connect.json.JsonConverter, schemas.enable=true) -- the spike's
# proven configuration. JSON converter is part of the MSK Connect runtime, so
# NEITHER plugin bundles the Glue Schema Registry Avro converter (that ~59 MiB
# shaded jar bloated the plugins and pulled a conflicting AWS SDK).
#
# Likewise, do NOT bundle aws-msk-iam-auth: the MSK Connect 3.7.x runtime provides
# IAMLoginModule/IAMClientCallbackHandler. Bundling it alongside msk-config-providers
# causes NoSuchFieldError: AUTH_SCHEME_PROVIDER at worker start (SPIKE gotcha #4).
#
# Re-run this whenever the sink source or a component version changes. The output
# artifacts are what src/dsql_migrator/core/s3_provision.py uploads.
set -euo pipefail

# Repo root = parent of this script's dir.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PLUGINS="${ROOT}/connectors/plugins"

command -v mvn >/dev/null 2>&1 || { echo "ERROR: mvn (Maven) not found on PATH." >&2; exit 1; }
command -v java >/dev/null 2>&1 || { echo "ERROR: java (JDK) not found on PATH." >&2; exit 1; }
command -v zip >/dev/null 2>&1 || { echo "ERROR: zip not found on PATH." >&2; exit 1; }

echo "==> 1/3  Building the DSQL sink jar (maven-shade)"
( cd "${ROOT}/connectors/dsql-sink" && mvn -B -q clean package -DskipTests )
SINK_JAR="${ROOT}/connectors/dsql-sink/target/dsql-sink-connector-0.1.0-SNAPSHOT.jar"
test -f "${SINK_JAR}" || { echo "ERROR: sink jar not built." >&2; exit 1; }

echo "==> 2/3  Assembling the Debezium source plugin zip"
# The debezium-connector-mysql/ folder is the committed build input (Debezium 2.7.4
# jars + msk-config-providers-0.4.0-all.jar). No Glue converter, no aws-msk-iam-auth.
( cd "${PLUGINS}" && rm -f debezium-mysql-plugin.zip \
    && zip -r -q debezium-mysql-plugin.zip debezium-connector-mysql/ )

echo "==> 3/3  Assembling the DSQL sink plugin zip (single jar)"
( cd "${PLUGINS}" \
    && rm -rf dsql-sink-plugin dsql-sink-plugin.zip \
    && mkdir -p dsql-sink-plugin \
    && cp "${SINK_JAR}" dsql-sink-plugin/ \
    && zip -r -q dsql-sink-plugin.zip dsql-sink-plugin/ )

echo
echo "==> Verification"
echo "-- debezium-mysql-plugin.zip must NOT contain the Glue converter or iam-auth:"
if unzip -l "${PLUGINS}/debezium-mysql-plugin.zip" | grep -qiE "schema-registry-kafkaconnect-converter|aws-msk-iam-auth"; then
  echo "ERROR: Debezium zip still bundles a forbidden jar (Glue converter or aws-msk-iam-auth)." >&2
  exit 1
fi
echo "   OK (clean)."
echo "-- debezium-mysql-plugin.zip must contain msk-config-providers (Secrets Manager provider):"
unzip -l "${PLUGINS}/debezium-mysql-plugin.zip" | grep -q "msk-config-providers" \
  || { echo "ERROR: msk-config-providers missing from Debezium zip." >&2; exit 1; }
echo "   OK."
echo "-- dsql-sink-plugin.zip layout (should be just the sink jar):"
unzip -l "${PLUGINS}/dsql-sink-plugin.zip"

echo
echo "Done. Both plugin zips are ready under connectors/plugins/."
echo "Remember: PLUGIN_VERSION in s3_provision.py must be bumped when these change."
