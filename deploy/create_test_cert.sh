#!/usr/bin/env bash
#
# TEST ONLY: create a self-signed certificate and import it into AWS Certificate
# Manager, so the app-stack's HTTPS ALB listener has a CertificateArn without
# owning a domain. Browsers show a warning for a self-signed cert — this is for
# internal smoke testing only.
#
# For production, request/import a real ACM certificate for a domain you control
# and pass its ARN as CertificateArn instead; do NOT use this script.
#
# Usage:
#   AWS_REGION=us-east-1 deploy/create_test_cert.sh [COMMON_NAME]
#
# Prints CertificateArn to pass to deploy/cloudformation.yaml as CertificateArn.
# The private key is generated in a temp dir and deleted after import (never
# printed). ACM stores the imported key.

set -euo pipefail

REGION="${AWS_REGION:-$(aws configure get region || true)}"
if [ -z "${REGION}" ]; then
  echo "error: set AWS_REGION (or configure a default region)." >&2
  exit 1
fi

CN="${1:-mysql-dsql-migrator.test}"

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

cat > "${TMP}/openssl.cnf" <<EOF
[req]
distinguished_name = dn
x509_extensions = v3
prompt = no
[dn]
CN = ${CN}
[v3]
subjectAltName = DNS:${CN}
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
EOF

openssl req -x509 -nodes -newkey rsa:2048 -days 365 \
  -keyout "${TMP}/key.pem" -out "${TMP}/cert.pem" \
  -config "${TMP}/openssl.cnf" 2>/dev/null

ARN="$(aws acm import-certificate --region "${REGION}" \
  --certificate "fileb://${TMP}/cert.pem" \
  --private-key "fileb://${TMP}/key.pem" \
  --tags Key=Name,Value=mysql-dsql-migrator-test-selfsigned \
  --query CertificateArn --output text)"

echo "CertificateArn=${ARN}"
echo "(self-signed, TEST only — browsers will warn; use a real cert for prod)"
