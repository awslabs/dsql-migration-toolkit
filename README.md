# mysql-dsql-migration-tool-with-AI

_Language: **English** | [한국어](README.ko.md) | [日本語](README.ja.md)_

A web-based all-in-one tool for migrating Amazon RDS MySQL / Aurora MySQL to
**Amazon Aurora DSQL**, with **optional AI assistance (Amazon Bedrock)** for the
parts that genuinely need judgment.

Aurora DSQL is a PostgreSQL 16–compatible *distributed* database, not a MySQL one,
so this is a **heterogeneous migration** with two overlapping conversions: MySQL →
PostgreSQL dialect, then PostgreSQL → DSQL constraints (no foreign keys, optimistic
concurrency, per-transaction row/time limits, async indexes, `C` collation, …).

The goal is not fully automated zero-downtime migration. It is to **assess
migratability, automate what converts deterministically (`sqlglot`), and clearly
surface what needs human work.** The source database is always accessed read-only.

> **Start here:** read the [**Customer FAQ**](docs/manual/en/11-customer-faq.md)
> (what to plan for — Full Load vs CDC, DSQL limits, validation, cut-over, cost),
> then follow the [**User Manual**](docs/manual/README.md) for the step-by-step
> walkthrough.

---

## At a glance

Two data paths converge on Aurora DSQL: a one-shot **Full Load** driven by the
tool, and an optional continuous **CDC** stream on managed MSK Connect. A
binlog/GTID watermark bridges the two for a gapless handoff.

<p align="center">
  <b>Simple architecture</b><br>
  <img src="deploy/architecture-aws-simple.png" alt="Architecture diagram" width="720">
</p>

---

## What it does / doesn't do

**✅ Does**

- **Assessment** — introspects your MySQL schema and classifies every object
  (`AUTO` / `MANUAL` / `UNSUPPORTED`) with effort estimates and name-conflict checks.
- **Schema conversion** — converts and applies DDL MySQL → DSQL (type mapping, FK
  removal, async indexes, PK strategies), review-and-apply from an object tree.
- **Full Load** — bulk-loads a consistent snapshot by streaming; resumable, large-scale.
- **CDC** — continuous replication for near-zero-downtime cut-over.
- **Validation** — proves source ↔ target match by row count, checksum, and PK
  reconciliation, and reports drift.
- **AI assist** (optional, off by default) — conversion suggestions for hard items,
  applied only after your review.

**❌ Doesn't / out of scope**

- **Not fully automated / zero-downtime** — hard conversions and the final **Cut
  over** are yours to decide and perform.
- **Never writes to the source** — read-only, kept as a rollback anchor.
- **CDC doesn't replicate DDL** — schema changes go through Schema Conversion.
- **No cross-region** — source and target must be in the same region.
- **DSQL's omitted features stay constraints** — no FK / triggers / stored
  procedures, per-transaction row limit, 1 MiB per-value limit, etc.

> Full enforced-limit list and workarounds: User Manual
> [Chapter 6 — Limitations](docs/manual/en/06-limitations.md).

---

## Workflow

The web UI guides you through five steps, with **Connect** as the preliminary step:

`Connect → Evaluation → Schema Conversion → Data Migration → Validation → Cut over`

| Step | What it does |
| --- | --- |
| Connect | Enter source (RDS/Aurora MySQL) and target (Aurora DSQL) connection details. Credentials stay in per-session memory and are discarded on session end. |
| 1. Evaluation | Introspect source **and** target, produce a compatibility report (`AUTO`/`MANUAL`/`UNSUPPORTED`) with effort estimates and name-conflict detection, plus optional AI strategy. |
| 2. Schema Conversion | Browse objects, view source-vs-converted DDL side by side, apply to target (SKIP / REPLACE) with idempotent retry. |
| 3. Data Migration | Choose the migration type (**Full Load** only, or add **CDC**), run prerequisite checks and pick tables, then run the snapshot (watermark → export → load, per-table progress + error log). For a CDC type the streaming infrastructure is deployed here too, so its ~15–20 min create runs **while** the Full Load does. |
| 4. Validation | Compare target against source as of the watermark; report row-count/checksum results and drift; export the report. |
| 5. Cut over | Runbook for switching your app MySQL → DSQL once validation passes — the one step the tool doesn't execute. MySQL source kept as rollback anchor. |

Each step shows its status (not started / in progress / done / failed) and can be
run or re-run independently. Feature-level detail lives in the
[User Manual](docs/manual/README.md).

---

## Quick start

Same tool, same UI — only **where it runs** changes. Run **locally** for
evaluation / small migrations, on **ECS Fargate** for real ones, or on a **single
EC2 host** (from source) when your account can't use containers/ECR or AWS Lambda.

| | **Local** | **ECS Fargate** | **EC2 (from source)** |
|---|---|---|---|
| Best for | Evaluation, small migrations | Real / large-scale migrations | No containers/ECR or Lambda allowed |
| Setup | `uv sync` + run (seconds) | Deploy CloudFormation app-stack | Deploy CloudFormation EC2 stack (`git` + `uv`, no image) |
| Migration engine runs on | Your machine | A single-task Fargate service in your VPC | A single EC2 host in your VPC |
| Reaches source & DSQL | From your machine (VPN / SSM for a private source) | Privately inside AWS (source → Fargate → DSQL) | Privately inside AWS (source → EC2 → DSQL) |
| Reach the UI | Browser → `127.0.0.1:8080` | ALB URL (`internal` by default) | SSM port-forward (no ALB / public IP) |
| Data path | Through your machine | Stays in AWS; your browser only loads the UI | Stays in AWS; your browser only loads the UI |
| Private source | Needs tunneling | Native (in-VPC) | Native (in-VPC) |
| Compute / cost | Your laptop, free | Fargate task (bill until teardown) | One EC2 instance + EBS (bill until teardown) |

### Local (fastest)

Your machine is the migration engine, so it must reach **both** the source MySQL
and DSQL (a private source needs VPN / SSM forward). AWS credentials just need to
be usable in your shell (`aws sso login`, `AWS_PROFILE=…`).

```bash
git clone <repo-url> mysql-dsql-migrator
cd mysql-dsql-migrator
uv sync                       # create + fill a .venv (needs uv)
cp .env.example .env          # optional: pre-fill connection details (git-ignored)
uv run mysql-dsql-migrator ui
```

Binds to `http://127.0.0.1:8080` by default. Open the printed URL and start from
the **Connect** step.

### ECS Fargate (real migrations)

Deploy the app-stack with CloudFormation (no image build — uses the published ECR
Public image); the tool comes up as a single-task Fargate service **inside your
VPC**, reachable at the ALB URL it outputs. Here **all migration traffic stays in
AWS** (source → Fargate → DSQL); your browser only opens the UI — suited to
large-scale migrations and private sources.

**Full procedure: [`deploy/DEPLOYMENT.md`](deploy/DEPLOYMENT.md)** (quick deploy,
parameters, Dev/Test vs Prod, DNS & Cognito, teardown, troubleshooting).

<p align="center">
  <b>Console (UI)</b><br>
  <img src="docs/demo-ui.png" alt="The tool's UI — the guided five-step migration workflow" width="720">
</p>

### EC2 host (from source — no container, no Lambda)

For accounts that **can't use containers/ECR or AWS Lambda.** The same engine runs on a
**single in-VPC EC2 host straight from source** (`git clone` + `uv sync` + a **systemd**
service) — no image build, no ALB; you reach the UI over an **SSM port-forward** and
state lives on a **retained EBS volume** (no S3 needed). For CDC it seeds Kafka
**in-process**, so it needs **no offset-seeder Lambda**. The private in-VPC data path
(source → EC2 → DSQL) is the same one Fargate gives.

**Full procedure: [`deploy/DEPLOYMENT.md` → Run on a single EC2 host](deploy/DEPLOYMENT.md#run-on-a-single-ec2-host-from-source-lambda-free).**

---

## Architecture

The tool is a **Python app** (NiceGUI UI + an importable engine) the operator runs
inside the customer environment: assess → convert → bulk-load a consistent snapshot
→ validate. Deployed, it runs as a **single-task Amazon ECS Fargate service** behind
an **HTTPS ALB** (`internal` by default, optional Cognito), pulling the image from
**Amazon ECR**. For accounts that can't use containers or Lambda, it can instead run
**from source on a single EC2 host** (systemd + SSM port-forward, no ALB/ECR) — see
[Quick start](#quick-start).

[![Full AWS architecture topology](deploy/architecture-aws.png)](deploy/architecture-aws.png)

> Click the diagram for the full-resolution image.

- **AI assist is control-plane only** — when enabled, Amazon Bedrock adds conversion
  suggestions, CDC-readiness assessment, and DLQ triage. It never sees Full Load /
  CDC row data — only schema/DDL/plan metadata. Off by default; no third-party API
  keys (scoped `bedrock:InvokeModel`).
- **CDC is a separate path** (`cdc-stack`) — Amazon MSK + Debezium → a
  **custom Aurora DSQL sink connector**
  ([`connectors/dsql-sink/`](connectors/dsql-sink)) on managed MSK Connect. A stock
  JDBC sink can't handle DSQL's short-lived IAM tokens, statement-level OCC retry,
  and ≤3,000-row batches, so we built our own. The tool stays the control plane and
  runs no sink compute of its own.

> More: [CDC & DSQL constraints](docs/manual/en/04-cdc-and-dsql-constraints.md) ·
> [Performance and tuning](docs/manual/en/07-performance-and-tuning.md).

<details>
<summary><b>AWS services used</b> (app-stack always; cdc-stack optional)</summary>

The migration **source** (RDS / Aurora MySQL) is customer-owned and external to
both stacks. Debezium is open-source software running *on* MSK Connect.

**Control plane & shared (app-stack)**

| Service | Role |
| --- | --- |
| Amazon ECS (Fargate) | Runs the single-task control-plane app (NiceGUI + engine). |
| Amazon ECR | Stores the app container image (published ECR Public image by default). |
| Elastic Load Balancing (ALB) | HTTPS entry point forwarding to the app (`internal` by default). |
| Amazon Route 53 | DNS for the app domain (only with a public domain; operator-provided). |
| AWS WAF | Web protection in front of the ALB (recommended when publicly exposed). |
| Amazon Cognito | OIDC auth gate at the ALB (required when exposed to the public internet). |
| AWS Certificate Manager | TLS certificate for the ALB HTTPS listener. |
| Amazon VPC | Private subnets, security groups, NAT / VPC endpoints. |
| AWS IAM | Least-privilege roles and DSQL IAM-token auth. |
| AWS Secrets Manager | UI session-cookie signing secret (auto-created); optional reuse of an existing source-creds secret. |
| Amazon Aurora DSQL | The migration target (PostgreSQL-compatible, IAM auth, OCC). |
| Amazon S3 | Full Load staging, connector plugin artifacts, CodeBuild source. |
| Amazon CloudWatch (Logs) | App and connector logs; CDC lag / metrics. |
| Amazon Bedrock | Optional AI assist (control plane only). |
| AWS CloudFormation | Infrastructure-as-code for both stacks. |

A normal deploy uses the ECR Public image as-is (no build). **AWS CodeBuild** is
not a runtime component — an optional build tool (`deploy/codebuild.yaml`) used
once only when you must build your own image on a restricted network.

> **EC2 (from-source) deploy** uses **Amazon EC2 + a retained EBS volume + AWS Systems
> Manager** (Session Manager) in place of ECS / ECR / ALB / Cognito, and keeps **app
> state on that EBS volume instead of S3**; in that mode CDC seeds Kafka in-process, so
> the **AWS Lambda** offset-seeder below is **not** created. (CDC still auto-provisions
> the S3 plugin bucket above for the connector artifacts.) See [Quick start](#quick-start).

**Optional CDC data plane (cdc-stack)**

| Service | Role |
| --- | --- |
| Amazon MSK (Serverless) | Kafka backbone: per-table topics partitioned by PK, plus a DLQ topic. |
| Amazon MSK Connect | Managed Kafka Connect hosting the Debezium source and our custom DSQL sink connector (JSON converter, `schemas.enable=true` — no schema registry). |
| AWS Lambda | In-VPC offset seeder (CFN custom resource) auto-seeding the Debezium GTID watermark for a gapless handoff. |
| Amazon VPC (dedicated) | CDC runs in its own VPC to reach the source MySQL privately. |

</details>

---

## Prerequisites

- A source **RDS / Aurora MySQL** with a read-only schema/data user.
- A target **Aurora DSQL** cluster in the **same region** (IAM-token auth, no password).
- **AWS credentials** via the standard chain (env / `~/.aws` / profile) with
  `dsql:DbConnect`. Optionally `secretsmanager:GetSecretValue` and `bedrock:InvokeModel`.
- **Local run only:** Python 3.10+ (pinned 3.12) and [`uv`](https://docs.astral.sh/uv/).

> Full checklist incl. source-DB / CDC setup (binlog, etc.):
> [User Manual §1.1](docs/manual/en/01-setup.md).

---

## Project layout

| Path | What's there |
|---|---|
| `src/dsql_migrator/core/` | Importable migration engine (no UI dependencies). |
| `src/dsql_migrator/ui/` | NiceGUI web application — the **primary interface**. |
| `src/dsql_migrator/cli/` | Command-line entrypoint for automation. |
| `connectors/dsql-sink/` | Custom Aurora DSQL Kafka Connect **sink connector** (Java; optional CDC plugin). |
| `deploy/` | `Dockerfile`, CloudFormation templates, build/teardown scripts, diagrams. See [`deploy/DEPLOYMENT.md`](deploy/DEPLOYMENT.md). |
| `docs/manual/` | The step-by-step user manual (EN / KO / JA). |

---

## Documentation

| Doc | What's inside |
|---|---|
| [**Deployment guide**](deploy/DEPLOYMENT.md) | Run locally in one command, deploy on ECS Fargate (AWS Console or CLI), or run from source on a single EC2 host (no container/Lambda) — prerequisites, parameters, custom domain / Cognito / AI assist, teardown, troubleshooting. |
| [**User manual**](docs/manual/) | Step-by-step walkthrough of the six migration steps — plus **performance tuning & measured test results**, testing / verification, and a **customer FAQ**. |
| [**Architecture**](#architecture) | How the pieces fit, plus the AWS and CDC-pipeline diagrams (`deploy/architecture-*.png`). |
| [**Changelog**](CHANGELOG.md) | Per-release changes (semantic-versioned). |

Localized: [한국어 README](README.ko.md) · [日本語 README](README.ja.md) — the deployment
guide, changelog, and user manual are translated too.

---

## Deployment

The tool connects to a customer's private RDS/Aurora and DSQL in the customer's IAM
context, so it runs **inside the customer environment (single-tenant)** — in
production as a single-task **ECS Fargate** service from `deploy/cloudformation.yaml`
(no image build). For accounts that can't use containers/ECR or AWS Lambda, it can
instead run **from source on a single EC2 host** (`deploy/cloudformation-ec2.yaml`).
Optional streaming CDC is a separate **cdc-stack**.

**▶ Full step-by-step: [`deploy/DEPLOYMENT.md`](deploy/DEPLOYMENT.md).**

> [!IMPORTANT]
> **Single-region only.** The tool works in any region offering Aurora DSQL, but
> the source (RDS / Aurora MySQL) and target (Aurora DSQL) **must be in the same
> region** (derived from the DSQL endpoint), and all provisioned infrastructure —
> especially the CDC VPC, which must reach the source privately — deploys there.
> Cross-region source/target is not supported.

---

## Configuration (advanced — usually no need to touch)

Everything is done in the UI with sensible defaults — **most operators never touch
this.** The full environment-variable reference (for automation / tuning) is below.

<details>
<summary><b>Environment-variable reference</b> — click to expand</summary>

Read from environment variables (no config file, no persisted credentials). On
Fargate, set these in the ECS task definition. The four Full Load / Validation
parallelism knobs can also be retuned **at runtime** from the sidebar's
**Settings → Performance** tab (no redeploy; resets on restart).

| Variable | Default | Description |
| --- | --- | --- |
| `DSQL_MIGRATOR_APP_HOST` | `127.0.0.1` | Host/interface the UI binds to. |
| `DSQL_MIGRATOR_APP_PORT` | `8080` | Port the UI listens on. |
| `DSQL_MIGRATOR_AWS_REGION` | _(unset)_ | AWS region for boto3 clients. |
| `DSQL_MIGRATOR_AWS_PROFILE` | _(unset)_ | Optional global AWS named profile; falls back to the standard chain. Only the (non-secret) name is stored. |
| `DSQL_MIGRATOR_JOB_STATE_PATH` | `job_state.sqlite` | Full Load job snapshots (status, per-table progress, watermark) for resume after restart. |
| `DSQL_MIGRATOR_ACTIVITY_LOG_PATH` | `migration_activity.log` | Structured activity log (one UTC-timestamped JSON line per event); downloadable from the UI, size-capped/rotated (~20 MB × 4 backups). |
| `DSQL_MIGRATOR_SESSION_STATE_PATH` | `session_state.sqlite` | Per-session non-secret workbench state so a reconnecting browser resumes. Pair with `DSQL_MIGRATOR_STORAGE_SECRET`. Local disk — the Fargate deploy uses the durable S3 store below instead. |
| `DSQL_MIGRATOR_SESSION_STATE_BUCKET` | _(unset)_ | Durable S3 store for the per-session snapshot, so resume survives a Fargate task replacement (a redeploy), not just an in-task restart. The Fargate deploy auto-sets it to the managed plugin bucket (no setup); leave unset locally to use the SQLite path above. |
| `DSQL_MIGRATOR_STAGING_BUCKET` | _(unset)_ | S3 bucket for Full Load staging (streaming multipart upload — the scalable path for large tables). Unset = bounded local temp CSV (dev / small tables). |
| `DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM` | `4` (≤16) | Tables loaded concurrently. Keep total DSQL connections within the cluster quota. |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM` | `8` (≤32) | In-flight `INSERT … ON CONFLICT` batches per table. Higher = more throughput but more OCC (40001) collisions. |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_ROWS` | `2000` (≤3000) | Rows per batched write, capped at DSQL's 3000-row per-transaction limit. |
| `DSQL_MIGRATOR_FULL_LOAD_PREFETCH` | `1` (on) | Read-ahead prefetch queue (reader thread fills a bounded queue while writes drain). Keep on; set `0` only to reproduce the pre-prefetch path in an A/B benchmark. |
| `DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS` | `1` (off, ≤8) | Split one large single-integer-PK table's read across K concurrent readers. Rarely worth it (the reader is GIL-bound) — see manual §7.2. |
| `DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS` | `1000000` | Minimum estimated rows for a table to be reader-sharded; smaller tables always use one reader. |
| `DSQL_MIGRATOR_VALIDATE_MAX_WORKERS` | `4` (≤32) | Tables compared concurrently in Validation. `1` = sequential. |
| `DSQL_MIGRATOR_LOG_LEVEL` | `INFO` | Startup log level; `DEBUG` adds a stacktrace (call stack only) to failure events. Also changeable at runtime via **Settings → Diagnostics**. |
| `DSQL_MIGRATOR_ACTIVITY_LOG_STDOUT` | `false` | Mirror activity-log events to stdout (→ CloudWatch on ECS). Also toggleable at runtime via **Settings → Diagnostics**. |
| `BEDROCK_MODEL_ID` | `global.anthropic.claude-sonnet-5` | Bedrock model / inference-profile id for AI assist. Always a `global.*` profile: those are reachable from any commercial region, while a `us.*` profile only resolves from us-east-1/us-east-2/us-west-2. |
| `BEDROCK_REGION` | _(unset)_ | Region for Amazon Bedrock calls. |

AI assist is off by default and enabled in the UI, which also offers a **Verify AI
access** preflight (checks Bedrock reachability, reports actionable failures).
Full background on the tuning knobs: manual
[Performance and tuning](docs/manual/en/07-performance-and-tuning.md).

> **CDC scaling is inferred, not set here.** The connector knobs (per-table topic
> partitions, sink `tasks.max`, MSK Connect MCUs) are derived from the captured-table
> count at cdc-stack deploy time; advanced env overrides
> (`DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS` / `_SINK_TASKS_MAX` / `_MCU_COUNT`) are
> documented in manual [§7.2 — CDC](docs/manual/en/07-performance-and-tuning.md).

</details>

---

## Version / changelog

Current version: [`pyproject.toml`](pyproject.toml); changes per version:
[**CHANGELOG.md**](CHANGELOG.md).

---

## License

Licensed under the **Apache License 2.0** — see [`LICENSE`](LICENSE) and
[`NOTICE`](NOTICE). Bundles pre-built third-party connector artifacts under
`connectors/plugins/` (Debezium + runtime deps); licenses in
[`THIRD-PARTY-NOTICES.md`](THIRD-PARTY-NOTICES.md). One dependency, MySQL
Connector/J, is under GPL-2.0 with the Universal FOSS Exception — review before
redistributing.
