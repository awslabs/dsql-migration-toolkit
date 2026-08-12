# Test procedure — Lambda-free "EC2 + MSK only" mode (SeedMode=External)

_Language: **English** | [한국어](TEST_EC2_MSK_ONLY.ko.md)_

Hands-on validation of Option ③: run the whole control plane on a single in-VPC
EC2 host (`deploy/cloudformation-ec2.yaml`), reach it over SSM port-forward, and
drive a CDC start that seeds Kafka **in-process** (no offset-seeder Lambda).

This procedure is for **you to run**. It goes safe → risky: first prove the
existing deployments are untouched (read-only), then stand up the EC2 host and run
a real External CDC.

> Prereqs: AWS CLI v2 with the **Session Manager plugin** installed, credentials
> for the target account, `jq`, and a shell in the repo root. Set once:
> ```bash
> export AWS_REGION=us-east-1          # your region
> export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
> ```

---

## 0. Automated tests (no AWS) — baseline

```bash
# From the repo root (worktree uses an editable venv, so set PYTHONPATH):
PYTHONPATH="$PWD/src" .venv/bin/python -m pytest -q
```
Expect: all pass (2900+). This covers the pure logic, the in-process seed, the
SeedMode gating, the EC2 template structure, and the host-is-mode config wiring —
all with injected seams, no real Kafka/AWS.

Targeted subsets if you want to eyeball them:
```bash
PYTHONPATH="$PWD/src" .venv/bin/python -m pytest -q \
  tests/test_ec2_appstack.py tests/test_cdc_stack_seedmode.py \
  tests/test_cdc_kafka_seed.py tests/test_config.py
```

---

## 1. Prove the DEFAULT (Fargate + Lambda) path is unchanged — read-only

### 1a. Template lints clean
```bash
aws cloudformation validate-template \
  --template-body file://deploy/cloudformation-ec2.yaml --region "$AWS_REGION" >/dev/null && echo "EC2 template OK"
aws cloudformation validate-template \
  --template-body file://deploy/cdc-stack/cdc-stack.yaml --region "$AWS_REGION" >/dev/null && echo "cdc-stack OK"
```
> The cdc-stack exceeds the 51,200-byte inline limit for some CLIs; if
> `validate-template` complains about size, skip to the change-set (1b), which
> uploads via S3.

### 1b. Change-set dry-run on a LIVE Lambda-mode cdc-stack → expect ZERO changes
This is the key proof: your existing cdc-stack, re-templated with the new file at
its **current** parameters (SeedMode defaults to `Lambda`, HostSubnetCidr to `""`),
must show **no resource changes**.

```bash
CDC_STACK=mysql-dsql-cdc-<yoursuffix>     # your existing cdc-stack name

# Upload the (oversize) template to the managed plugin bucket, then point the
# change-set at it via TemplateURL.
BUCKET=mysql-dsql-migrator-plugins-$ACCOUNT-$AWS_REGION
aws s3 cp deploy/cdc-stack/cdc-stack.yaml "s3://$BUCKET/cdc-plugins/cdc-stack-test.yaml" --region "$AWS_REGION"

# Note: SeedMode is a NEW parameter (not in the deployed stack), so it must be given
# an explicit value (Lambda = the default/unchanged mode) — NOT UsePreviousValue.
# HostSubnetCidr is also new but has a default (""), so it can be omitted. Do NOT
# pass `--use-previous-template` (it is a value-less flag; adding `false` errors, and
# it must be absent when supplying --template-url).
aws cloudformation create-change-set \
  --stack-name "$CDC_STACK" \
  --change-set-name seedmode-nochange-$(date +%s) \
  --template-url "https://$BUCKET.s3.$AWS_REGION.amazonaws.com/cdc-plugins/cdc-stack-test.yaml" \
  --parameters ParameterKey=SeedMode,ParameterValue=Lambda \
               $(aws cloudformation describe-stacks --stack-name "$CDC_STACK" --region "$AWS_REGION" \
                 --query "Stacks[0].Parameters[?ParameterKey!='SeedMode'].ParameterKey" --output text \
                 | tr '\t' '\n' | sed 's/^/ParameterKey=/;s/$/,UsePreviousValue=true/') \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --region "$AWS_REGION"

# Inspect: the Changes array should be EMPTY (or only benign Metadata).
aws cloudformation describe-change-set --stack-name "$CDC_STACK" \
  --change-set-name <the-name-above> --region "$AWS_REGION" \
  --query 'Changes[].ResourceChange.{Action:Action,Type:ResourceType,Id:LogicalResourceId}' --output table

# DELETE the change-set WITHOUT executing (this changed nothing):
aws cloudformation delete-change-set --stack-name "$CDC_STACK" \
  --change-set-name <the-name-above> --region "$AWS_REGION"
```
✅ Pass = empty Changes. That proves adding SeedMode/HostSubnetCidr is inert for an
existing Lambda-mode stack. ❌ If any resource shows Modify/Remove, stop and report.

> The Fargate app-stack (`deploy/cloudformation.yaml`) is **not edited** by Option
> ③, so there is nothing to change-set there.

---

## 2. Publish an image that contains the `cdc-external` extra

`SeedMode=External` needs `kafka-python` + the MSK IAM SASL signer baked in. The
default published image (`:0.1.303`) predates the `--extra cdc-external` Dockerfile
change, so **build and push a fresh image** (or use CodeBuild if you have no local
Docker):

```bash
# Local Docker → ECR Public (see deploy/build_and_push.sh for the full script):
deploy/build_and_push.sh                 # builds from deploy/Dockerfile, pushes a tag
# note the pushed image URI, e.g. public.ecr.aws/z0q0i9j0/mysql-dsql-migrator:0.1.308
export IMAGE_URI=public.ecr.aws/z0q0i9j0/mysql-dsql-migrator:0.1.308
```
Verify the extra is in the image:
```bash
docker run --rm --entrypoint python "$IMAGE_URI" -c "import kafka, aws_msk_iam_sasl_signer; print('cdc-external OK')"
```
✅ Pass = prints `cdc-external OK`. ❌ `ModuleNotFoundError` = the image lacks the
extra; rebuild after confirming `deploy/Dockerfile` has `uv sync ... --extra cdc-external`.

---

## 3. Deploy the EC2 app-stack (single in-VPC host)

Pick a subnet that (a) is in the **same VPC as your MSK/cdc-stack** and (b) has a
**NAT/PrivateLink egress route** (the host has no public IP). Get its CIDR — you'll
need it in step 4.

```bash
export VPC_ID=vpc-xxxxxxxx
export HOST_SUBNET_ID=subnet-xxxxxxxx     # in VPC_ID, NAT-egress, co-located with MSK
export DSQL_CLUSTER_ARN=arn:aws:dsql:$AWS_REGION:$ACCOUNT:cluster/xxxx
export SOURCE_DB_SG=sg-source             # or SourceDbCidr

# The host subnet's CIDR (used in step 4 to admit the host to MSK 9098):
export HOST_SUBNET_CIDR=$(aws ec2 describe-subnets --subnet-ids "$HOST_SUBNET_ID" \
  --region "$AWS_REGION" --query 'Subnets[0].CidrBlock' --output text)
echo "HOST_SUBNET_CIDR=$HOST_SUBNET_CIDR"

aws cloudformation deploy \
  --template-file deploy/cloudformation-ec2.yaml \
  --stack-name mysql-dsql-migrator-ec2 \
  --region "$AWS_REGION" \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    VpcId="$VPC_ID" \
    HostSubnetId="$HOST_SUBNET_ID" \
    DsqlClusterArn="$DSQL_CLUSTER_ARN" \
    SourceDbSecurityGroupId="$SOURCE_DB_SG" \
    ContainerImageUri="$IMAGE_URI" \
    MskEgressCidr="$HOST_SUBNET_CIDR"      # or the connector-subnet CIDR; narrows 9098 egress
```
> Stack name must **not** start with `mysql-dsql-cdc-` (that prefix falls into the
> CdcDeployRole scope). `mysql-dsql-migrator-ec2` is fine.

Read the outputs:
```bash
aws cloudformation describe-stacks --stack-name mysql-dsql-migrator-ec2 \
  --region "$AWS_REGION" --query 'Stacks[0].Outputs' --output table
```
Note `HostInstanceId` and `SsmPortForwardCommand`.

### 3a. Confirm the host booted the container
```bash
INSTANCE_ID=$(aws cloudformation describe-stacks --stack-name mysql-dsql-migrator-ec2 \
  --region "$AWS_REGION" --query "Stacks[0].Outputs[?OutputKey=='HostInstanceId'].OutputValue" --output text)

# Wait ~2-3 min for user-data, then check via SSM Run Command:
aws ssm send-command --instance-ids "$INSTANCE_ID" --document-name AWS-RunShellScript \
  --parameters 'commands=["docker ps --format {{.Names}}\\ {{.Status}}","tail -n 20 /var/log/dsql-migrator-userdata.log","mount | grep dsql-migrator"]' \
  --region "$AWS_REGION" --query Command.CommandId --output text
# then: aws ssm get-command-invocation --command-id <id> --instance-id $INSTANCE_ID --region $AWS_REGION --query StandardOutputContent --output text
```
✅ Pass = `dsql-migrator  Up ...`, the EBS mount on `/var/lib/dsql-migrator`, no
errors in the user-data log.

### 3b. Reach the UI via SSM port-forward
Run the `SsmPortForwardCommand` output (or):
```bash
aws ssm start-session --target "$INSTANCE_ID" \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}' \
  --region "$AWS_REGION"
```
Open `http://localhost:8080` → the migration tool UI loads. Leave the session open.

---

## 4. Admit the host to MSK on 9098 (the reachability piece)

The app does **not** auto-inject `HostSubnetCidr` into the cdc-stack yet
(deferred), so add the ingress once, either by re-deploying the cdc-stack with the
param **or** (simplest for a test) via the UI's CDC infra deploy passing the param.
The template-param path:

```bash
# If you deploy/adopt the cdc-stack yourself, add HostSubnetCidr to its params:
#   ... --parameter-overrides ... HostSubnetCidr="$HOST_SUBNET_CIDR"
# This creates ConnectorHostDiagnosticsIngress (9098 from the host subnet CIDR).
```
Verify the rule exists on the connector SG:
```bash
CDC_STACK=mysql-dsql-cdc-<yoursuffix>
SG=$(aws cloudformation describe-stack-resources --stack-name "$CDC_STACK" --region "$AWS_REGION" \
  --query "StackResources[?LogicalResourceId=='ConnectorSecurityGroup'].PhysicalResourceId" --output text)
aws ec2 describe-security-groups --group-ids "$SG" --region "$AWS_REGION" \
  --query "SecurityGroups[0].IpPermissions[?FromPort==\`9098\`].IpRanges[].CidrIp" --output text
```
✅ Pass = your `$HOST_SUBNET_CIDR` (and the bastion `172.31.0.0/20`) appear.

---

## 5. End-to-end: External CDC from the EC2 host

In the UI (over the port-forward), walk the normal journey: **Connect → Evaluation
→ Schema Conversion → Data Migration (Full Load) → Start CDC**. Because the host's
container has `DSQL_MIGRATOR_CDC_SEED_MODE=external`, Start CDC runs the seed
in-process.

Confirm the mode actually took effect (host env + deploy log):
```bash
# The container env should show external:
aws ssm send-command --instance-ids "$INSTANCE_ID" --document-name AWS-RunShellScript \
  --parameters 'commands=["docker exec dsql-migrator printenv DSQL_MIGRATOR_CDC_SEED_MODE"]' \
  --region "$AWS_REGION" --query Command.CommandId --output text
# expect: external
```
In the CDC deploy log (UI, or `docker logs dsql-migrator`) you should see the
in-process prep line: `SeedMode=External: preparing CDC topics + offset in-process
…` followed by `In-process CDC prep complete (offset seed: true|skipped)`, then the
connectors reaching RUNNING.

Confirm **no offset-seeder Lambda** was created in External mode:
```bash
aws cloudformation describe-stack-resources --stack-name "$CDC_STACK" --region "$AWS_REGION" \
  --query "StackResources[?ResourceType=='AWS::Lambda::Function']" --output table
# expect: EMPTY (no OffsetSeederFunction) when the cdc-stack was deployed SeedMode=External
```

Verify data replicates (source → DSQL):
```bash
# Use the repo's compare-rows helper (or the /compare-rows skill):
.venv/bin/python scripts/compare_rows.py <table> ...
```
✅ Pass = row counts converge; CDC changes land in DSQL.

---

## 6. Negative + resilience checks

- **Loud failure when unreachable (no silent gap):** deploy the EC2 host but do
  NOT add the 9098 ingress (skip step 4), then Start CDC. Expect the deploy to fail
  with `CdcDeployError: In-process CDC seed (SeedMode=External) failed before
  creating the connectors …` and **no connectors created** — never a silent success.
- **EBS resume:** reboot the instance
  (`aws ec2 reboot-instances --instance-ids $INSTANCE_ID`), reconnect the
  port-forward, reopen `http://localhost:8080` → the in-flight job/session state is
  restored from the retained EBS volume (SQLite survives the restart).
- **Fargate/laptop still Lambda:** on a Fargate deploy (or `uv run mysql-dsql-migrator ui`
  locally) with `DSQL_MIGRATOR_CDC_SEED_MODE` unset, Start CDC must still create the
  in-VPC OffsetSeederFunction (Lambda mode) — proving host-is-mode didn't leak.

---

## 7. Teardown

```bash
# CDC infra (if you deployed a test cdc-stack) — via the UI's Delete CDC infra, or:
aws cloudformation delete-stack --stack-name "$CDC_STACK" --region "$AWS_REGION"

# The EC2 app-stack. NOTE: the state EBS volume has DeletionPolicy: Retain, so it
# survives stack deletion by design — delete it manually if you don't want to keep it.
aws cloudformation delete-stack --stack-name mysql-dsql-migrator-ec2 --region "$AWS_REGION"
aws cloudformation wait stack-delete-complete --stack-name mysql-dsql-migrator-ec2 --region "$AWS_REGION"

# Find + delete the retained state volume if desired:
aws ec2 describe-volumes --region "$AWS_REGION" \
  --filters "Name=tag:aws:cloudformation:stack-name,Values=mysql-dsql-migrator-ec2" \
  --query 'Volumes[].VolumeId' --output text
# aws ec2 delete-volume --volume-id vol-xxxx --region "$AWS_REGION"
```

---

## Known gaps to expect while testing (deferred, by design)
- **`HostSubnetCidr` is a manual param** — the app doesn't auto-derive/inject it
  into the cdc-stack yet, so step 4 is a manual one-time add.
- **Image republish needed** — the published `:0.1.303` default lacks the
  `cdc-external` extra; step 2 builds `:0.1.308`. Point `ContainerImageUri` at it.
- **`ContainerImageUri` default drift** — the template default still points at an
  older tag; always pass your freshly-built image.
- **Large-table Full Load staging** — `staging_bucket` is S3-only; for very large
  tables set `DSQL_MIGRATOR_STAGING_BUCKET` or size the EBS volume accordingly.
