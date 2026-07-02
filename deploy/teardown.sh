#!/usr/bin/env bash
#
# Tear down the app-stack and (optionally) the ECR repository.
#
# Usage:
#   AWS_REGION=us-east-1 deploy/teardown.sh [STACK_NAME]
#
# Environment variables:
#   AWS_REGION   AWS region (required; or configure a default region).
#   STACK_NAME   CloudFormation stack name (default: arg $1, else mysql-dsql-migrator).
#   ECR_REPO     ECR repository name (default: mysql-dsql-migrator).
#   DELETE_ECR   When 'true', also delete the ECR repository and its images.
#                Default 'false' (images are kept; the repo is not in the stack).
#
# Route 53 records created manually are NOT removed by this script.

set -euo pipefail

STACK_NAME="${1:-${STACK_NAME:-mysql-dsql-migrator}}"
: "${ECR_REPO:=mysql-dsql-migrator}"
: "${DELETE_ECR:=false}"

REGION="${AWS_REGION:-$(aws configure get region || true)}"
if [ -z "${REGION}" ]; then
  echo "error: set AWS_REGION (or configure a default region)." >&2
  exit 1
fi

echo "==> Deleting stack ${STACK_NAME} in ${REGION}"
aws cloudformation delete-stack --stack-name "${STACK_NAME}" --region "${REGION}"
echo "==> Waiting for stack deletion to complete"
aws cloudformation wait stack-delete-complete --stack-name "${STACK_NAME}" --region "${REGION}"
echo "==> Stack deleted."

if [ "${DELETE_ECR}" = "true" ]; then
  echo "==> Deleting ECR repository ${ECR_REPO} (and all images)"
  aws ecr delete-repository --repository-name "${ECR_REPO}" --force --region "${REGION}"
  echo "==> ECR repository deleted."
else
  echo "==> Keeping ECR repository ${ECR_REPO} (set DELETE_ECR=true to remove)."
fi
