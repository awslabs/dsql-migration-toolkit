# cdc-stack — optional CDC data plane

This CloudFormation template provisions the optional **change-data-capture (CDC)**
data plane: Amazon MSK Serverless + MSK Connect (a Debezium MySQL source connector
and this project's custom DSQL sink connector), a Glue Schema Registry, the
connector networking, and the connector IAM role.

> **You normally do not run this template by hand.** The migration tool's UI owns
> the full cdc-stack lifecycle — Deploy / Start / Stop / Delete — from the Data
> Migration step. The app validates inputs, auto-discovers the network, uploads the
> plugin artifacts, creates/cleans up the source-credentials secret, and streams
> deploy progress. Drive CDC from the app; the CLI below is for inspection or
> break-glass recovery only.

## How the app deploys it (the normal path)

1. On the **Data Migration** step, choose a migration type that includes CDC.
2. Its **Prerequisites** sub-step then offers **Deploy CDC infrastructure** (this
   template, ~15–20 min). Deploying here is deliberate: it precedes the Full Load,
   so the MSK create runs **while** the snapshot loads instead of after it.
3. The CDC sub-step takes over from there: **Start CDC** (creates the connectors) →
   live monitoring → **Stop** / **Delete**. It can also deploy the infrastructure,
   for a session that reaches CDC with nothing provisioned yet.
4. The deploy form asks for only your **VPC ID**; subnets/NAT egress, the plugin
   S3 bucket + artifacts, the DSQL cluster ARN, the source host, and the
   source-credentials secret are all resolved automatically. A confirmation dialog
   shows the network plan and a rough monthly cost before any billable resource is
   created.

The privileged CloudFormation/MSK/IAM operations are not held by the app's task
role; the app assumes a dedicated, least-privilege **CdcDeployRole** (created by
the app-stack, `DSQL_MIGRATOR_CDC_DEPLOY_ROLE_ARN`) only for the duration of each
cdc-stack operation. See [`../DEPLOYMENT.md`](../DEPLOYMENT.md) for the app-stack
setup that wires this up.

## What it provisions

- **`AWS::MSK::ServerlessCluster`** — the Kafka backbone (IAM auth).
- **`AWS::Glue::Registry`** — schema registry for Debezium events, named per stack
  (`${AWS::StackName}-registry`) so several cdc-stacks can run concurrently.
- **`AWS::KafkaConnect::CustomPlugin`** ×2 — the Debezium MySQL source plugin and
  the custom DSQL sink plugin (uploaded by the app to a managed S3 bucket).
- **`AWS::KafkaConnect::Connector`** ×2 — the Debezium source connector
  (`table.include.list`, `snapshot.mode=schema_only`, watermark-seeded start
  offset) and the custom DSQL sink connector (PK upsert/delete, DLQ, ≤3,000-row
  batch). The configuration maps mirror what the Python control plane builds
  (`CdcPipelineOrchestrator.build_source_config` / `build_sink_config`).
- **Connector networking** — when your VPC has no NAT egress, the stack creates its
  own private subnets + NAT gateway + route table (gated on `CreateOwnedNetwork`),
  and tears them down on stack delete. Your existing route tables are untouched.
- **`AWS::IAM::Role`** — connector execution role, least-privilege:
  `dsql:DbConnect`/`DbConnectAdmin` (scoped to the cluster), Secrets Manager read
  for the source credential (scoped to that secret ARN), MSK/Glue/CloudWatch Logs.

## Not provisioned (by design)

- **No compute cluster / no EKS / no Kubernetes** — the connectors run on the
  managed MSK Connect runtime. Scale is via MSK partitions + MSK Connect MCU +
  connector `tasks.max`.
- **DLQ topic** — Kafka topics are not CloudFormation resources; the DLQ
  (`errors.deadletterqueue.topic.name`) is created by the sink connector at
  runtime (auto-create).
- **The source-credentials secret** — created/updated and cleaned up by the app
  (out of band) when the source uses username/password auth; reused as-is when the
  source was connected via Secrets Manager.

## Naming (multi-DB)

Every cdc-stack name must start with the canonical, source-neutral prefix `dsql-cdc-`
(default `dsql-cdc-stack`); the legacy prefix `mysql-dsql-cdc-` is **still accepted** so
existing deployments keep working (their stacks stay discoverable, adoptable, and
manageable). The CdcDeployRole's IAM is scoped to **both** the `dsql-cdc-*` and
`mysql-dsql-cdc-*` families, so one app can manage several concurrent cdc-stacks — one
per source database (e.g. `dsql-cdc-orders`, `dsql-cdc-billing`). New stacks always take
the canonical prefix. Set a per-DB name (suffix only) in the deploy form's Advanced field.

## Manual deploy (inspection / break-glass only)

The template exceeds CloudFormation's 51,200-byte inline limit, so the CLI stages it in an S3
bucket: the tool's plugin bucket, which `PluginBucketArn` names and the stack's connector
plugins and offset seeder are read from. That bucket is
`mysql-dsql-migrator-plugins-<account>-<region>-an` or, for a deployment that has a bucket by
the name earlier versions created (and always in me-south-1 and me-central-1),
`mysql-dsql-migrator-plugins-<account>-<region>`. The block below uses the `-an` bucket when
this account owns one, and the earlier one otherwise, as the app does (the app's activity log
entry *S3 bucket in use* names the bucket it uses). The earlier name follows from the account
ID and Region, so another AWS account could own a bucket by that name: the deploy runs only if
the `head-bucket` check confirms that this account owns the bucket. Set `AWS_REGION` to the
app's Region (the plugin bucket's name holds it), replace `...` with the other parameters (the
template's `Parameters` section lists them), and add `--no-execute-changeset` to inspect the
change set without applying it.

```bash
export AWS_REGION=us-east-1
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export PLUGIN_BUCKET=mysql-dsql-migrator-plugins-$ACCOUNT-$AWS_REGION-an
aws s3api head-bucket --bucket "$PLUGIN_BUCKET" --expected-bucket-owner "$ACCOUNT" \
  || export PLUGIN_BUCKET=mysql-dsql-migrator-plugins-$ACCOUNT-$AWS_REGION
aws s3api head-bucket --bucket "$PLUGIN_BUCKET" --expected-bucket-owner "$ACCOUNT" \
  && aws cloudformation deploy --template-file deploy/cdc-stack/cdc-stack.yaml \
  --stack-name dsql-cdc-stack --region "$AWS_REGION" --capabilities CAPABILITY_NAMED_IAM \
  --s3-bucket "$PLUGIN_BUCKET" --s3-prefix cdc-plugins \
  --parameter-overrides PluginBucketArn="arn:aws:s3:::$PLUGIN_BUCKET" \
    ...
```

Prefer the in-app flow: it fills almost every parameter for you and avoids the
MSK-Serverless partition-quota pitfalls (each connector create/delete consumes
quota that is not reclaimed). The control-plane app-stack does not depend on this
stack; deploy a cdc-stack only when CDC is needed.
