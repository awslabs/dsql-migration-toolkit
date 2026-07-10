#!/usr/bin/env bash
#
# Build the app image in AWS CodeBuild (no local Docker) and push it to ECR.
#
# Prerequisite: deploy the build infrastructure once:
#   aws cloudformation deploy --template-file deploy/codebuild.yaml \
#     --stack-name mysql-dsql-migrator-build --capabilities CAPABILITY_IAM \
#     --region "$AWS_REGION"
#
# Usage:
#   AWS_REGION=us-east-1 deploy/build_in_codebuild.sh [IMAGE_TAG]
#
# Environment variables:
#   AWS_REGION    AWS region (required; or configure a default region).
#   BUILD_STACK   CodeBuild infra stack name (default: mysql-dsql-migrator-build).
#   IMAGE_TAG     Image tag (default: arg $1, else project version, else 'latest').
#
# Steps: zip the working tree -> upload to the stack's S3 source bucket ->
# start the CodeBuild project -> wait -> print the pushed image URI to use as
# the app-stack's ContainerImageUri.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

: "${BUILD_STACK:=mysql-dsql-migrator-build}"

PROJECT_VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' "${REPO_ROOT}/pyproject.toml" | head -n1)"
IMAGE_TAG="${1:-${IMAGE_TAG:-${PROJECT_VERSION:-latest}}}"

REGION="${AWS_REGION:-$(aws configure get region || true)}"
if [ -z "${REGION}" ]; then
  echo "error: set AWS_REGION (or configure a default region)." >&2
  exit 1
fi

stack_output() {
  aws cloudformation describe-stacks --stack-name "${BUILD_STACK}" --region "${REGION}" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue | [0]" --output text
}

echo "==> Reading build infrastructure from stack ${BUILD_STACK}"
BUCKET="$(stack_output SourceBucketName)"
PROJECT="$(stack_output ProjectName)"
REPO_URI="$(stack_output EcrRepositoryUri)"
if [ -z "${BUCKET}" ] || [ "${BUCKET}" = "None" ] || [ -z "${PROJECT}" ] || [ "${PROJECT}" = "None" ]; then
  echo "error: stack '${BUILD_STACK}' not found or missing outputs. Deploy deploy/codebuild.yaml first." >&2
  exit 1
fi

IMAGE_URI="${REPO_URI}:${IMAGE_TAG}"

echo "==> Packaging source"
TMPZIP="$(mktemp -t mysql-dsql-migrator-source-XXXXXX).zip"
trap 'rm -f "${TMPZIP}"' EXIT
( cd "${REPO_ROOT}" && zip -r -q -X "${TMPZIP}" . \
    -x '.git/*' '.venv/*' '*/__pycache__/*' '*.pyc' '.pytest_cache/*' '*.sqlite' '*.log' \
       '.env' '.env.*' '*.pem' )

echo "==> Uploading source to s3://${BUCKET}/source.zip"
aws s3 cp "${TMPZIP}" "s3://${BUCKET}/source.zip" --region "${REGION}" >/dev/null

echo "==> Starting CodeBuild project ${PROJECT} (tag ${IMAGE_TAG})"
# Optional release: set PUBLIC_IMAGE_URI (e.g. public.ecr.aws/<alias>/mysql-dsql-migrator)
# to ALSO publish the image to ECR Public (the default customers pull from).
ENV_OVERRIDES="name=IMAGE_TAG,value=${IMAGE_TAG},type=PLAINTEXT"
if [ -n "${PUBLIC_IMAGE_URI:-}" ]; then
  echo "==> Will also publish to ECR Public: ${PUBLIC_IMAGE_URI}:${IMAGE_TAG}"
  ENV_OVERRIDES="${ENV_OVERRIDES} name=PUBLIC_IMAGE_URI,value=${PUBLIC_IMAGE_URI},type=PLAINTEXT"
fi
BUILD_ID="$(aws codebuild start-build --project-name "${PROJECT}" --region "${REGION}" \
  --environment-variables-override ${ENV_OVERRIDES} \
  --query 'build.id' --output text)"
echo "==> Build id: ${BUILD_ID}"

echo "==> Waiting for the build to complete..."
while true; do
  STATUS="$(aws codebuild batch-get-builds --ids "${BUILD_ID}" --region "${REGION}" \
    --query 'builds[0].buildStatus' --output text)"
  case "${STATUS}" in
    SUCCEEDED)
      echo "==> Build SUCCEEDED"
      break
      ;;
    IN_PROGRESS)
      sleep 10
      ;;
    *)
      echo "error: build ${STATUS}. See CodeBuild logs for project ${PROJECT}." >&2
      exit 1
      ;;
  esac
done

echo ""
echo "Pushed: ${IMAGE_URI}"
echo "Pass this as ContainerImageUri to deploy/cloudformation.yaml."
