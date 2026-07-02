#!/usr/bin/env bash
#
# Build the offset-seeder Lambda deployment zip the cdc-stack uploads to S3:
#
#   connectors/plugins/offset-seeder-lambda.zip
#
# This is a CloudFormation custom-resource Lambda (deploy/cdc-stack/lambda/seeder.py)
# that runs IN-VPC during cdc-stack deployment and seeds the Debezium source
# connector's connect-offsets record for a gapless Full Load -> CDC handoff.
#
# Its only runtime dependencies are kafka-python and the AWS MSK IAM SASL signer,
# both PURE-PYTHON, so we can vendor them into a committed zip with nothing but
# `pip install --target` -- no Docker, no native build, mirroring how the connector
# plugin zips are committed (deployment convenience: a fresh clone deploys CDC with
# no toolchain). Re-run this whenever seeder.py, cfnresponse.py, or a dependency
# version changes; the output zip is what core/s3_provision.py uploads.
#
# Lambda zip layout: the handler module and every dependency sit at the ARCHIVE
# ROOT (no top-level folder), so the handler is importable as `seeder.handler`.
set -euo pipefail

# Repo root = parent of this script's dir.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PLUGINS="${ROOT}/connectors/plugins"
LAMBDA_SRC="${ROOT}/deploy/cdc-stack/lambda"
OUT_ZIP="${PLUGINS}/offset-seeder-lambda.zip"

command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 not found on PATH." >&2; exit 1; }
command -v zip >/dev/null 2>&1 || { echo "ERROR: zip not found on PATH." >&2; exit 1; }

test -f "${LAMBDA_SRC}/seeder.py" || { echo "ERROR: ${LAMBDA_SRC}/seeder.py missing." >&2; exit 1; }
test -f "${LAMBDA_SRC}/cfnresponse.py" || { echo "ERROR: ${LAMBDA_SRC}/cfnresponse.py missing." >&2; exit 1; }

BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "${BUILD_DIR}"' EXIT

echo "==> 1/3  Installing pure-Python deps into the build dir"
# --only-binary=:all: would force wheels; these are pure-python sdists/wheels, so a
# plain install is fine and stays platform-independent (no native extensions).
python3 -m pip install --quiet --target "${BUILD_DIR}" \
  kafka-python aws-msk-iam-sasl-signer-python

echo "==> 2/3  Adding the handler + cfnresponse to the build dir"
cp "${LAMBDA_SRC}/seeder.py" "${LAMBDA_SRC}/cfnresponse.py" "${BUILD_DIR}/"

# The AWS SDK (boto3/botocore/s3transfer/jmespath + their deps dateutil/urllib3/six)
# is ALREADY provided by the Lambda Python runtime, and the MSK IAM signer uses only
# botocore's stable SigV4 APIs -- so we prune it from the committed zip rather than
# ship ~25 MiB of vendored SDK. click/bin are the signer's CLI, unused at runtime.
for pkg in boto3 botocore s3transfer jmespath dateutil urllib3 six.py click bin; do
  rm -rf "${BUILD_DIR:?}/${pkg}"
done

# Drop pyc / test bloat so the committed zip stays small. Do NOT strip *.dist-info:
# the MSK IAM signer calls importlib.metadata.version("aws-msk-iam-sasl-signer-python")
# at runtime to build its User-Agent, which fails ("No package metadata was found")
# without the dist-info. The dist-info dirs are tiny, so keep them all.
find "${BUILD_DIR}" -type d -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true
find "${BUILD_DIR}" -type d -name 'tests' -prune -exec rm -rf {} + 2>/dev/null || true

echo "==> 3/3  Zipping to ${OUT_ZIP}"
rm -f "${OUT_ZIP}"
( cd "${BUILD_DIR}" && zip -r -q -X "${OUT_ZIP}" . )

echo
echo "==> Verification"
# Capture the listing once -- piping `unzip -l` straight into `grep -q` makes grep
# close the pipe on first match, killing unzip with SIGPIPE, which `pipefail` then
# reports as a (spurious) failure. Grep the captured text instead.
LISTING="$(unzip -l "${OUT_ZIP}")"
echo "-- handler must sit at the archive root (seeder.py + cfnresponse.py):"
grep -qE " seeder\.py$" <<<"${LISTING}" \
  || { echo "ERROR: seeder.py not at zip root." >&2; exit 1; }
grep -qE " cfnresponse\.py$" <<<"${LISTING}" \
  || { echo "ERROR: cfnresponse.py not at zip root." >&2; exit 1; }
echo "   OK."
echo "-- both pure-Python deps must be bundled:"
grep -q "kafka/" <<<"${LISTING}" \
  || { echo "ERROR: kafka-python missing from zip." >&2; exit 1; }
grep -q "aws_msk_iam_sasl_signer/" <<<"${LISTING}" \
  || { echo "ERROR: aws-msk-iam-sasl-signer-python missing from zip." >&2; exit 1; }
echo "   OK."

echo
echo "Done. offset-seeder-lambda.zip is ready under connectors/plugins/ (commit it)."
echo "Remember: PLUGIN_VERSION in s3_provision.py must be bumped when this changes."
