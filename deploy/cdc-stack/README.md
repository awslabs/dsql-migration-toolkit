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

1. On the **Migration plan** step, choose a mode that includes CDC.
2. On the **Data Migration** step, the **CDC pipeline** card walks you through:
   **Deploy CDC infrastructure** (this template, ~15–20 min) → **Start CDC**
   (creates the connectors) → live monitoring → **Stop** / **Delete**.
3. The deploy form asks for only your **VPC ID**; subnets/NAT egress, the plugin
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

Every cdc-stack name must start with `mysql-dsql-cdc-` (default `mysql-dsql-cdc-stack`); the
CdcDeployRole's IAM is scoped to the `mysql-dsql-cdc-*` family, so one app can manage
several concurrent cdc-stacks — one per source database (e.g. `mysql-dsql-cdc-orders`,
`mysql-dsql-cdc-billing`). Set a per-DB name in the deploy form's Advanced field.

## Manual deploy (inspection / break-glass only)

```bash
aws cloudformation validate-template --template-body file://deploy/cdc-stack/cdc-stack.yaml

aws cloudformation deploy --template-file deploy/cdc-stack/cdc-stack.yaml \
  --stack-name mysql-dsql-cdc-stack --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides ...   # see the template's Parameters section
```

Prefer the in-app flow: it fills almost every parameter for you and avoids the
MSK-Serverless partition-quota pitfalls (each connector create/delete consumes
quota that is not reclaimed). The control-plane app-stack does not depend on this
stack; deploy a cdc-stack only when CDC is needed.
