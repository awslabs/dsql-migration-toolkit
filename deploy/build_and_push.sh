#!/usr/bin/env bash
#
# Build the app container image and push it to Amazon ECR in one command.
#
# Usage:
#   AWS_REGION=us-east-1 deploy/build_and_push.sh [IMAGE_TAG]
#
# Environment variables (all optional except where noted):
#   AWS_REGION       AWS region for ECR (required; or configure a default region).
#   ECR_REPO         ECR repository name (default: mysql-dsql-migrator).
#   IMAGE_TAG        Image tag (default: arg $1, else the project version, else 'latest').
#   IMAGE_PLATFORM   Target image platform (default: linux/amd64 to match the
#                    Fargate task's default X86_64 architecture). Set to
#                    linux/arm64 only if the Fargate task uses ARM64/Graviton.
#
# On success it prints the full image URI to pass to CloudFormation as
# ContainerImageUri. No credentials are baked into the image.

set -euo pipefail

# Resolve repository root (this script lives in <root>/deploy).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

: "${ECR_REPO:=mysql-dsql-migrator}"

# Build for the architecture the Fargate task runs on. The CloudFormation task
# defaults to X86_64, so default to linux/amd64 even on Apple Silicon hosts
# (override with IMAGE_PLATFORM=linux/arm64 if the task uses ARM64/Graviton).
: "${IMAGE_PLATFORM:=linux/amd64}"

# Tag precedence: $1 > IMAGE_TAG env > project version in pyproject.toml > latest.
PROJECT_VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' "${REPO_ROOT}/pyproject.toml" | head -n1)"
IMAGE_TAG="${1:-${IMAGE_TAG:-${PROJECT_VERSION:-latest}}}"

# Region must be resolvable.
REGION="${AWS_REGION:-$(aws configure get region || true)}"
if [ -z "${REGION}" ]; then
  echo "error: set AWS_REGION (or configure a default region)." >&2
  exit 1
fi

ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE_URI="${REGISTRY}/${ECR_REPO}:${IMAGE_TAG}"

echo "==> Region:     ${REGION}"
echo "==> Repository: ${ECR_REPO}"
echo "==> Image URI:  ${IMAGE_URI}"

# Create the repository if it does not exist (idempotent).
if ! aws ecr describe-repositories --repository-names "${ECR_REPO}" \
      --region "${REGION}" >/dev/null 2>&1; then
  echo "==> Creating ECR repository ${ECR_REPO}"
  aws ecr create-repository --repository-name "${ECR_REPO}" \
    --image-scanning-configuration scanOnPush=true \
    --region "${REGION}" >/dev/null
fi

echo "==> Logging in to ECR"
aws ecr get-login-password --region "${REGION}" \
  | docker login --username AWS --password-stdin "${REGISTRY}"

# Preflight: the Dockerfile COPYs three pre-built CDC deploy artifacts (it compiles
# nothing). All are committed in the repo, so a normal deploy needs no build. These
# are the exact files the running app uploads to S3 (s3_provision._artifact_paths),
# so the set MUST match the Dockerfile's COPY list. Fail early with an actionable
# message rather than a cryptic Docker COPY error.
DEBEZIUM_ZIP="${REPO_ROOT}/connectors/plugins/debezium-mysql-plugin.zip"
DEBEZIUM_PG_ZIP="${REPO_ROOT}/connectors/plugins/debezium-postgres-plugin.zip"
SINK_ZIP="${REPO_ROOT}/connectors/plugins/dsql-sink-plugin.zip"
SEEDER_ZIP="${REPO_ROOT}/connectors/plugins/offset-seeder-lambda.zip"
for artifact in "${DEBEZIUM_ZIP}" "${DEBEZIUM_PG_ZIP}" "${SINK_ZIP}" "${SEEDER_ZIP}"; do
  if [ ! -f "${artifact}" ]; then
    echo "error: missing committed CDC deploy artifact:" >&2
    echo "  ${artifact}" >&2
    echo "All connectors/plugins/*.zip are committed pre-built; restore from" >&2
    echo "version control. Rebuild the sink only if you changed the Java source:" >&2
    echo "  (cd connectors/dsql-sink && mvn -q package) then repackage the plugin zip." >&2
    exit 1
  fi
done

echo "==> Building image (context: ${REPO_ROOT}, platform: ${IMAGE_PLATFORM})"
docker build --platform "${IMAGE_PLATFORM}" -f "${REPO_ROOT}/deploy/Dockerfile" -t "${ECR_REPO}:${IMAGE_TAG}" "${REPO_ROOT}"

echo "==> Tagging and pushing"
docker tag "${ECR_REPO}:${IMAGE_TAG}" "${IMAGE_URI}"
docker push "${IMAGE_URI}"

echo ""
echo "Pushed: ${IMAGE_URI}"
echo "Pass this as ContainerImageUri to deploy/cloudformation.yaml."
