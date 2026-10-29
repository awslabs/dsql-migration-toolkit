# 1. Set up

_Language: **English** | [한국어](../ko/01-setup.md) | [日本語](../ja/01-setup.md)_

> **Prev:** [0. Before you begin](00-before-you-begin.md)

This chapter takes you from "I have an Amazon RDS or Aurora database (MySQL or
PostgreSQL)" to "the tool is open in my browser and connected to my source and my
Aurora DSQL target."

**Shortest path:** to evaluate, run the tool locally ([§1.2](#12-run-the-tool-locally))
and connect ([§1.5](#15-connect-to-your-source-and-target)). If you followed
[`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md) and the UI is already open at your
`AppUrl`, go straight to §1.5. You need [§1.6](#16-prepare-the-source-for-cdc) only if you
will run Full load + CDC.

The tool runs in one of three ways. You connect to the **same** source and target in
each; only where the tool *process* runs differs.

| Option | Best for | You reach the UI via | State is kept in | Details |
|---|---|---|---|---|
| **Local** | Evaluation and smaller migrations; fastest to start | `http://127.0.0.1:8080` | Local files | [§1.2](#12-run-the-tool-locally) |
| **ECS Fargate** | Real migrations — the form most teams use | An Application Load Balancer endpoint (`AppUrl`), optionally behind Cognito | S3 — survives a task replacement | [§1.3](#13-run-the-tool-on-aws-ecs-fargate) |
| **Single EC2 host** (from source) | Accounts that can't use containers/ECR or AWS Lambda | An SSM port-forward — no ALB, public IP or inbound rule | A retained EBS volume — survives an instance replacement | [§1.4](#14-run-on-a-single-ec2-host-from-source) |

---

## 1.1 Prerequisites

**Your databases**

- A **source database** — Amazon RDS or Aurora **MySQL**, or Amazon RDS or Aurora
  **PostgreSQL** — you can reach over the network, with a user that can read the schema
  and data. Read-only is enough for Evaluation, Schema Conversion, Full Load and
  Validation, and a MySQL source (or a PostgreSQL Full-Load-only migration) is never
  written. The only exceptions come with CDC: a MySQL **Full load + CDC** run on a source
  not running `gtid_mode=ON` issues one `FLUSH BINARY LOGS` (its user needs `RELOAD`; no
  data is written and no table is locked), and **PostgreSQL CDC** creates — and at
  teardown drops — a logical replication slot and a publication scoped to exactly the
  migrated tables, sets `REPLICA IDENTITY FULL` on a table whose CDC key changed, and adds
  a table selected after the Full Load to that publication (AUTOCOMMIT, restricted to a
  small allowlist, audited).
- **Supported versions** — both source paths (Schema Conversion, Full Load, CDC and cut
  over) are validated end to end on live infrastructure:
  - **RDS for MySQL** 5.7 / 8.0 / 8.4 and **Aurora MySQL** 5.7 (v2) / 8.0 (v3) / 8.4. MySQL
    5.7 is past standard support (RDS/Aurora Extended Support may apply) but is fully
    supported as a source.
  - **RDS for PostgreSQL / Aurora PostgreSQL** PG **13–18** (tested end to end). There is
    no hard version gate: the tool reads the server version for display only and never
    rejects a major version.
- A target **Amazon Aurora DSQL** cluster in the **same AWS region** as the tool. DSQL has
  no password: you give the tool the cluster **endpoint** and your AWS identity (which must
  be allowed to connect to that cluster), and it mints the short-lived IAM tokens itself,
  renewing each before it expires.

**For running locally**

- Python 3.10+ (3.12 is pinned in `.python-version`) and [`uv`](https://docs.astral.sh/uv/).
- AWS credentials on the standard chain (environment, `~/.aws` or a named profile) that
  can **generate Aurora DSQL IAM tokens** (`dsql:DbConnect` / `dsql:DbConnectAdmin`).
  Optional: `secretsmanager:GetSecretValue` if your source credentials live in Secrets
  Manager, and both `bedrock:InvokeModel` and `bedrock:InvokeModelWithResponseStream`
  for AI assist (the chat streams its replies, so the first alone is not enough).

**For deploying on AWS** — see [`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md);
the short version is in [§1.3](#13-run-the-tool-on-aws-ecs-fargate).

**For CDC (optional)** — only for Full load + CDC; a **Full-Load-only** migration needs
none of this. The source needs a few settings (binary logging or logical decoding, a
replication user, binlog retention), which the tool's prerequisite gate checks before CDC
starts: [§1.6](#16-prepare-the-source-for-cdc).

---

## 1.2 Run the tool locally

From a fresh clone:

```bash
uv sync                          # install dependencies into a local virtualenv
uv run mysql-dsql-migrator ui    # launch the web UI (binds to 127.0.0.1:8080 by default)
```

Then open **http://127.0.0.1:8080** in your browser.

Optionally, copy `.env.example` to `.env` and fill in the connection fields (source DB
host/port/user, target DSQL endpoint, region, etc.). The **Connect** screen prefills its
form from them. `.env` is git-ignored and for local development only.

```bash
cp .env.example .env
```

The app doesn't hot-reload (`reload=False`): restart it to pick up code edits. This
matters only if you're modifying the tool itself.

---

## 1.3 Run the tool on AWS (ECS Fargate)

For real migrations most teams deploy the tool as a **single-task ECS Fargate service**
behind an Application Load Balancer (optionally gated by Amazon Cognito OIDC), with the
image in Amazon ECR. The full, parameterized CloudFormation flow is in
[`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md); in essence:

```bash
# 1. Build + push the image. No local Docker? Use AWS CodeBuild instead:
AWS_REGION=us-east-1 deploy/build_in_codebuild.sh      # prints the image URI

# 2. Deploy the app stack (ECS Fargate + ALB + IAM) with that image URI and your
#    VPC/subnet/cert/DSQL/source details as parameters; the exact
#    `aws cloudformation deploy` command is in deploy/DEPLOYMENT.md.
```

A fresh `git clone` deploys with minimal setup: the connector plugin artifacts are
committed (no Java/Maven toolchain), the tool provisions its own S3 bucket and uploads
artifacts itself, and the optional CDC infrastructure is auto-discovered — you supply
only what can't be inferred, such as the VpcId.

> **The VPC and its subnets must belong to the account you deploy into.** RAM-shared
> (cross-account) subnets are not supported: the CDC deploy role's EC2 permissions are
> scoped to this account's resources, so creating the connector's network interface in a
> shared subnet fails with `AccessDenied`.

> **Security note:** the deployed app has **no authentication of its own** — it relies
> on the ALB's optional Cognito gate. The template's `Rules` block an internet-facing ALB
> open to `0.0.0.0/0` **without** Cognito (`CognitoRequiredWhenIngressOpen`): enable
> Cognito (`EnableCognitoAuth=true`) or scope `AllowedIngressCidr` to your network.

The fastest way to *see it run* is the **Dev/Test profile** (`internal` ALB,
`EnableCognitoAuth=false`, self-signed cert) — still real Fargate, just fewer moving
parts. For anything beyond evaluation use the **Prod profile**; every parameter is
described in [`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md). Its key settings:

- Cognito on (`EnableCognitoAuth=true`, with `CognitoDomainPrefix` and `CognitoAdminEmail`
  — the template requires all three together) and a real domain/certificate.
- An `internal` ALB; `internet-facing` only with `AllowedIngressCidr` scoped to your
  network.
- `AssignPublicIp=DISABLED`, with a NAT gateway or VPC endpoints.
- An immutable image tag or digest.
- The CloudWatch mirror of the activity log turned on (**Settings → Diagnostics**) for a
  durable audit trail.

State is kept in S3, so a task replacement keeps the workbench and the Full Load record. A
load running at that moment is interrupted, and **Retry unfinished tables** reloads each
table it did not finish from its start
([§6.3](06-limitations.md#63-deployment-limits-the-aws-hosted-form)).

---

## 1.4 Run on a single EC2 host (from source)

For accounts that **can't use containers/ECR or AWS Lambda**, the tool runs on a
**single in-VPC EC2 host straight from source** — no image to build or pull. The
parameterized CloudFormation flow (`deploy/cloudformation-ec2.yaml`), its parameters and
the SSM port-forward command are in the
[Deployment guide](../../../deploy/DEPLOYMENT.md#run-on-a-single-ec2-host-from-source-lambda-free).
In essence:

- The host bootstraps from source (`git clone` + `uv sync` + a **systemd** service) —
  **no Docker, no ECR**.
- You reach the UI over an SSM port-forward (Session Manager), so there is no ACM
  certificate or Cognito.
- For CDC it seeds Kafka **in-process**, so **no offset-seeder Lambda** is created (CDC
  still auto-provisions the S3 plugin bucket for the connector artifacts).

The data path (source → EC2 → DSQL) stays private in the VPC, as on Fargate, with far
fewer moving parts.

---

## 1.5 Connect to your source and target

Open the tool and start at the **Connect** step. You provide:

| Field | What to enter |
|---|---|
| **Source** | Pick your **source engine** (MySQL or PostgreSQL), then enter host, port, user, and password — **or** a Secrets Manager secret ARN/name holding them. The default port follows the engine (**3306** for MySQL, **5432** for PostgreSQL); for PostgreSQL you also give the single **database** to connect to. Auth for both engines is password or Secrets Manager (IAM-token auth is target-DSQL only). |
| **Target** | Your Aurora DSQL **cluster endpoint**, region, database (fixed to `postgres`, shown read-only), and username (`admin` by default). **No password** — the tool generates an IAM token. |

Then **test** each connection. The tool reads the source **read-only** to confirm
reachability and permissions, and generates a DSQL IAM token to confirm it can connect to
the target.

**Credentials live in per-session process memory only.** They are never written to disk,
logs, reports, or job state, and are discarded when the session ends — a strict,
non-negotiable rule of the tool. After a restart you re-enter them.

> **Single region.** This tool works in any region where Aurora DSQL is available, but
> **source and target must be in the same region** — cross-region migration is **not**
> supported. Run the tool in that region too.

Once both connections test green, continue to **Evaluation** — the tool introspects both
databases and produces a compatibility report. The guided flow then runs through Schema
Conversion, Data Migration (Full Load only, or add CDC), Validation, and finally
**Cut over** (the runbook for switching your application to DSQL), each covered in the
following chapters. **Planning Full load + CDC?** Prepare the source first
([§1.6](#16-prepare-the-source-for-cdc)): its binary-log (MySQL) or logical-decoding
(PostgreSQL) settings must be in place before you start the Full Load.

---

## 1.6 Prepare the source for CDC

*Only for Full load + CDC (the near-zero-downtime cut-over); a Full-Load-only migration can
skip this section.* CDC reads the source's change stream — the **MySQL binary log** or, for
PostgreSQL, the **logical-decoding WAL** via a replication slot + publication — so set the
following up once, before you start the Full Load. The tool's prerequisite gate checks each
setting before CDC starts and tells you exactly what's missing.

**On RDS/Aurora you can't edit `my.cnf` / `postgresql.conf` or run `SET GLOBAL`.** Server
variables are set through a **parameter group** (PostgreSQL's `rds.logical_replication` is
static and **requires a reboot**), and MySQL operational settings like binlog retention
through **RDS stored procedures** (`mysql.rds_*`). The tables below use this managed method.

### MySQL source

| Setting | Value | How to set it |
|---|---|---|
| Binary logging (`log_bin`) | `ON` | **RDS for MySQL:** can't be set directly — enable **automated backups** (backup retention period > 0). **Aurora MySQL:** off until you set `binlog_format`. |
| `binlog_format` | `ROW` | **RDS:** a custom DB parameter group attached to the instance. **Aurora:** a **cluster-level** parameter — set it in a custom DB *cluster* parameter group (the default group can't be changed) and **reboot** the cluster if it was `OFF` (the default). |
| `binlog_row_image` | `FULL` | The same parameter group. `FULL` is the default; set it explicitly anyway. |
| `binlog_row_value_options` (MySQL 8.0+) | empty | The same parameter group. `PARTIAL_JSON` logs a JSON update as a diff CDC cannot apply; after clearing it, reboot the source or recycle the applications' connection pools — a connection opened before the change keeps logging diffs. |
| Binlog retention | **168 hours (7 days)** is a safe default | `CALL mysql.rds_set_configuration('binlog retention hours', 168);` on RDS for MySQL and Aurora MySQL. The Aurora MySQL maximum is **2160** hours (90 days); lower it again after cut-over. |
| Replication user | `SELECT`, `REPLICATION CLIENT`, `REPLICATION SLAVE`, `RELOAD`, `LOCK TABLES`; on MySQL 8.0.19+ also `XA_RECOVER_ADMIN` if the application uses XA transactions | Dedicated and least-privilege — not an admin account. `RELOAD` and `LOCK TABLES` are for initial-snapshot bookkeeping. |
| `gtid_mode` | `ON` — recommended, not required | A parameter group. Decide before the Full Load and keep it unchanged until cut over. |

The first four rows are **hard requirements**: the gate fails CDC without them.

- **Binlog retention.** RDS/Aurora purge binary logs aggressively — **Aurora MySQL keeps
  them only 24 hours**. CDC resumes from the **watermark** captured during Full Load, so
  the binlog at that position **must still exist when CDC starts**, and deploying the CDC
  stack (MSK + MSK Connect) takes **~15–20 minutes** before streaming begins. Choose a
  window that covers the gap between Full Load and CDC start plus catch-up time. The gate
  **warns** (never blocks) if retention is unset or under 24 hours, since only you know
  how long your Full Load will run.
- **GTID.** With `gtid_mode=ON`, CDC positions by GTID, which also survives a failover to
  a replica whose binary log files differ; with `OFF`, `OFF_PERMISSIVE` or `ON_PERMISSIVE`
  it uses the binlog `file:position`. A start point recorded — or a CDC stack last
  started — while `gtid_mode` was not `ON` cannot start CDC once it is `ON`
  ([§6.2](06-limitations.md#62-migration-process-limits)).
- **Without GTID**, a Full load + CDC run makes its `file:position` start point exact with
  `FLUSH BINARY LOGS`, which needs `RELOAD` (no data is written and no table is locked;
  the binlog moves to a new file). On Aurora MySQL with enhanced binlog the tool does not
  rely on that, so such a run needs `gtid_mode=ON` (or enhanced binlog turned off — either
  is a writer reboot) ([§3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)).

### PostgreSQL source

| Setting | Value | How to set it |
|---|---|---|
| Logical decoding (`wal_level`) | `logical` — a **hard requirement** (the gate fails CDC without it) | On **RDS for PostgreSQL / Aurora PostgreSQL**, set the static parameter **`rds.logical_replication=1`** in a custom DB (cluster) parameter group, then **reboot** (Aurora: the writer). |
| Endpoint | The cluster **writer**, not a standby (`pg_is_in_recovery()` must be false) | A standby can't host a replication slot. |
| Replication user | Able to create and read a logical replication slot — a **superuser**, a role with the **REPLICATION attribute** (`pg_roles.rolreplication`), or a member of **`rds_replication`** — plus **SELECT** on the migrated tables for the snapshot | Dedicated and least-privilege — not an admin account. On RDS/Aurora the REPLICATION attribute can't be granted directly. (This is NOT the community `REPLICATION` object privilege.) |
| `REPLICA IDENTITY` on every replicated table (every partition of a partitioned table) | `DEFAULT` with a valid primary key that is not `DEFERRABLE`, `USING INDEX` with a valid unique index, or `FULL` (`ALTER TABLE … REPLICA IDENTITY FULL`), which works for any table except, on PostgreSQL 18, one with a generated column | Without one, UPDATE/DELETE on the table would fail on the source once it is published, so the prerequisite check fails it and the Full Load stops before it changes anything on the source. |
| Replication slot / `max_wal_senders` headroom | A free slot and walsender | A non-blocking **WARN**: a full walsender pool blocks a new slot even when slot entries are free. |

- **No retention setting to change.** The **logical replication slot** the tool creates at
  the Full Load consistency point **pins the required WAL** from that LSN, so the ~15–20
  min CDC-stack provisioning is safe. The risk runs the other way: an **inactive or
  unconsumed slot keeps pinning WAL and can fill the source's disk**, so monitor WAL/slot
  health. The tool surfaces `wal_status`, and `wal_status='lost'` means the slot was
  invalidated → gapless resume is broken → Start CDC blocks and names the way out
  ([§4.3](04-cdc-and-dsql-constraints.md)): with Full load + CDC, re-run Full Load with
  **Drop & reload**, after Delete CDC infrastructure and a new deploy if the stack's
  connectors were started before.

---

**Next:** [2. Evaluation and Schema Conversion →](02-evaluation-and-schema-conversion.md)
