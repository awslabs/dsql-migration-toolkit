# 1. Set up

_Language: **English** | [한국어](../ko/01-setup.md) | [日本語](../ja/01-setup.md)_

> **Prev:** [0. Before you begin](00-before-you-begin.md)

This chapter gets you from "I have an Amazon RDS or Aurora database (MySQL or
PostgreSQL)" to "the tool is open in my browser and connected to both my source
and my Aurora DSQL target."

> **Already deployed on AWS?** If you followed [`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md)
> and the UI is already open at your `AppUrl`, skip ahead to [§1.5 Connect](#15-connect-to-your-source-and-target).
> This chapter also covers running locally, which the deployment guide does not.

There are three ways to run the tool:

- **Local** — run it on your laptop/workstation for evaluation and smaller
  migrations. Fastest to start.
- **On AWS (ECS Fargate)** — the deployed form most teams use for real
  migrations, reached through a web endpoint behind an Application Load Balancer.
- **On AWS (single EC2 host, from source)** — for accounts that can't use
  containers/ECR or AWS Lambda: the tool runs straight from source (`git clone` +
  `uv sync` + a **systemd** service) on one in-VPC EC2 host, reached over an SSM
  port-forward (no ECS/ALB/image). → [§1.4](#14-run-on-a-single-ec2-host-from-source).

You connect to the **same** source and target in every case; only where the tool
*process* runs differs.

---

## 1.1 Prerequisites

**Your databases**

- A source database — **Amazon RDS or Aurora MySQL**, or **Amazon RDS or Aurora
  PostgreSQL** — you can reach over the network, with a user that can read the
  schema and data. Read-only is enough for Evaluation, Schema Conversion, Full
  Load and Validation; a MySQL source (and a PostgreSQL Full-Load-only migration)
  is never written. The single exception is **PostgreSQL CDC**: at the Full Load
  consistency point the tool creates — and at teardown drops — a logical
  replication slot and a publication scoped to exactly the migrated tables
  (AUTOCOMMIT, restricted to a small allowlist, audited). These are the only
  writes the tool ever makes to any source.
  - **Supported source engines & versions** — both source paths (Schema
    Conversion + Full Load + CDC + cut over) are validated end-to-end on live
    infrastructure:
    - **RDS for MySQL** 5.7 / 8.0 / 8.4, and **Aurora MySQL** 5.7 (v2) / 8.0 (v3)
      / 8.4. MySQL 5.7 is past end of standard support (RDS/Aurora Extended
      Support may apply), but the tool fully supports it as a migration source.
    - **RDS for PostgreSQL / Aurora PostgreSQL**, PG **13–16** (tested
      end-to-end). There is no hard version gate in the tool — it reads the
      server version for display only and never rejects a major version; PG
      13–16 is the validated range.
- A target **Amazon Aurora DSQL** cluster in the **same AWS region** as you'll
  run the tool. (DSQL uses IAM-token auth — there is no password to manage.)

**For running locally**

- Python 3.10+ (the project pins 3.12 via `.python-version`).
- [`uv`](https://docs.astral.sh/uv/) for dependency management.
- AWS credentials reachable through the standard credential chain (environment,
  `~/.aws`, or a named profile) that can **generate Aurora DSQL IAM tokens**
  (`dsql:DbConnect` / `dsql:DbConnectAdmin`). Optionally
  `secretsmanager:GetSecretValue` (if your source credentials live in Secrets
  Manager) and `bedrock:InvokeModel` (only if you turn on AI assist).

**For deploying on AWS** — see the full [`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md);
the short version is in §1.3.

> **Note on the DSQL target:** there is no DSQL "endpoint + password" to copy
> into a config file. You give the tool the DSQL **cluster endpoint** and your
> AWS identity; the tool mints a short-lived IAM token per connection. Make sure
> the identity you run under is allowed to connect to that DSQL cluster.

**For CDC (the optional streaming pipeline)** — only if you'll use CDC for a
near-zero-downtime cut-over. A **Full-Load-only** migration needs none of this;
these are the source-side requirements that let CDC read the source's change
stream — the **MySQL binary log**, or, for a PostgreSQL source, the
**logical-decoding WAL** via a replication slot + publication. The tool's
prerequisite gate checks each one before starting CDC and tells you exactly
what's missing, but setting them up is a source-side task you do once.

> **Managed RDS/Aurora is configured differently from a self-managed (community)
> server.** On a self-managed MySQL or PostgreSQL server you'd edit `my.cnf` /
> `postgresql.conf` and run `SET GLOBAL`. On **Amazon RDS / Aurora you can't do
> either** — server variables are set through a **parameter group** (MySQL:
> `binlog_format`, etc.; **PostgreSQL: `rds.logical_replication`**), and MySQL
> operational settings like binlog retention are changed with **RDS stored
> procedures** (`mysql.rds_*`). PostgreSQL's enabling parameter is **static, so it
> requires a reboot**. The steps below use the managed (RDS/Aurora) method, which
> is what this tool targets.

- **MySQL source — binary logging must be on, in ROW format with a full row image**
  — `log_bin=ON`, `binlog_format=ROW`, `binlog_row_image=FULL`. This is a **hard
  requirement** for CDC (the gate fails CDC if it isn't met). How to set it on
  managed MySQL:
  - **RDS for MySQL:** you **can't turn `log_bin` on directly** and you **can't edit
    `my.cnf`**. Instead, enable **automated backups** (set the backup retention
    period > 0) — that turns binary logging on — then set `binlog_format=ROW` and
    `binlog_row_image=FULL` in a **custom DB parameter group** attached to the
    instance. (`binlog_row_image` defaults to `FULL`; set it explicitly to be sure.)
  - **Aurora MySQL:** `binlog_format` is a **cluster-level** parameter — set it to
    `ROW` in a **custom DB *cluster* parameter group** (you can't change the default
    group), then **reboot** the cluster if you changed it from `OFF`. The default is
    `OFF`, so binary logging is off until you do this.
  - **Community / self-managed MySQL (for contrast):** there you'd set
    `log_bin`/`binlog_format`/`binlog_row_image` in `my.cnf` (or `SET GLOBAL` at
    runtime) and restart — **none of which applies to RDS/Aurora.**
- **PostgreSQL source — logical decoding must be enabled** — **`wal_level=logical`**
  (a **hard requirement**; the gate fails CDC if it isn't met). On **RDS for
  PostgreSQL / Aurora PostgreSQL** set the static parameter
  **`rds.logical_replication=1`** in a custom DB (cluster) parameter group, then
  **reboot** (Aurora: reboot the writer). On self-managed PostgreSQL set
  `wal_level=logical` in `postgresql.conf` and restart. (No
  `binlog_format`/`binlog_row_image` analog exists for PostgreSQL.)
- **A source user with replication privileges.** **MySQL:** `SELECT`,
  `REPLICATION CLIENT`, and `REPLICATION SLAVE` (plus `RELOAD` and `LOCK TABLES`,
  used for the initial snapshot bookkeeping). **PostgreSQL:** the CDC user must be
  able to create and read a logical replication slot — it passes if it is a
  **superuser**, has the **REPLICATION role attribute** (`pg_roles.rolreplication`),
  or is a member of **`rds_replication`** (on RDS/Aurora, where the REPLICATION
  attribute can't be granted directly); it also needs **SELECT** on the migrated
  tables for the snapshot. (This is NOT the community `REPLICATION` object
  privilege.) Use a dedicated least-privilege CDC user, not an admin account.
- **MySQL source — increase binlog retention so the logs aren't purged before CDC
  catches up.** RDS/Aurora purge binary logs aggressively by default — **Aurora
  MySQL keeps them only 24 hours**; on RDS for MySQL they're governed by backup
  retention. CDC resumes from the **watermark** captured during Full Load, so the
  binlog at that position **must still exist when CDC starts** — and deploying the
  CDC stack (MSK + MSK Connect) takes **~15–20 minutes** before streaming even
  begins. Set a generous window with the RDS stored procedure (hours; works on both
  RDS for MySQL and Aurora MySQL):

  ```sql
  CALL mysql.rds_set_configuration('binlog retention hours', 168);  -- e.g. 7 days
  ```

  Choose a window that comfortably covers the gap between Full Load and CDC start
  plus your expected catch-up time (7 days is a safe default). The Aurora MySQL
  maximum is **2160 (90 days)**; you can lower it again after cut-over. The gate
  now **warns** (non-blocking) if retention looks too short (under 24h) or is unset,
  so you catch it before the binlog is purged — but it never blocks, since only you
  know how long your Full Load will run.
- **PostgreSQL source — no binlog-retention setting to change.** Instead the
  **logical replication slot** the tool creates at the Full Load consistency point
  **pins the required WAL automatically** from that LSN, so there is nothing to set;
  the ~15–20 min CDC-stack provisioning is fine because the slot holds the start
  position. The trade-off is the opposite risk: an **inactive/unconsumed slot keeps
  pinning WAL and can fill the source's disk**, so monitor WAL/slot health — the
  tool surfaces `wal_status`, and `wal_status='lost'` means the slot was invalidated
  → gapless resume is broken → re-run Full Load.
- **MySQL source — GTID is recommended, but not required.** With `gtid_mode=ON`, CDC
  resume survives a source failover or replica promotion; without it, the tool falls
  back to the binlog `file:position` watermark — which works, but is less robust
  across failover. The gate reports a missing GTID as informational, never as a
  blocker. (PostgreSQL has no GTID / `file:position` concept.)
- **PostgreSQL source — CDC must run against the cluster writer.** The source must
  be the **writer, not a standby** (`pg_is_in_recovery()` must be false) — a standby
  can't host a replication slot, so point CDC at the writer endpoint.
- **PostgreSQL source — every replicated table needs a usable REPLICA IDENTITY.** A
  primary key gives the default; otherwise set `ALTER TABLE … REPLICA IDENTITY FULL`
  (FULL or an index identity also work). A table left at `REPLICA IDENTITY NOTHING`
  makes UPDATE/DELETE error on the publisher, so it is refused. Replication-slot /
  `max_wal_senders` (walsender) **headroom** is checked as a non-blocking **WARN** (a
  full walsender pool blocks a new slot even when slot entries are free).

---

## 1.2 Run the tool locally

From a fresh clone:

```bash
# Install dependencies into a local virtualenv
uv sync

# Launch the web UI (binds to 127.0.0.1:8080 by default)
uv run mysql-dsql-migrator ui
```

Then open **http://127.0.0.1:8080** in your browser.

Optional convenience: copy `.env.example` to `.env` and fill in the connection
fields. The **Connect** screen prefills its form from these so you don't retype
them each session. `.env` is git-ignored and is for local development only.

```bash
cp .env.example .env
# edit .env: source DB host/port/user, target DSQL endpoint, region, etc.
```

> The app runs with `reload=False`, so it does **not** hot-reload code changes —
> restart it to pick up edits. This matters only if you're modifying the tool
> itself.

---

## 1.3 Run the tool on AWS (ECS Fargate)

For a real migration most teams deploy the tool as a **single-task ECS Fargate
service** behind an Application Load Balancer (optionally gated by Amazon Cognito
OIDC auth), with the container image in Amazon ECR. The full, parameterized
CloudFormation flow is in [`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md);
the essence is:

```bash
# 1. Build + push the image. No local Docker? Use AWS CodeBuild instead:
AWS_REGION=us-east-1 deploy/build_in_codebuild.sh      # prints the image URI

# 2. Deploy the app-stack (ECS Fargate + ALB + IAM), passing that image URI
#    and your VPC/subnet/cert/DSQL/source details as parameters.
#    See deploy/DEPLOYMENT.md for the exact `aws cloudformation deploy` command.
```

**Deployment-convenience by design:** a fresh `git clone` can deploy with minimal
setup — the connector plugin artifacts are committed (no Java/Maven toolchain
needed), the tool provisions its own S3 bucket and uploads artifacts itself, and
the optional CDC infrastructure is auto-discovered (you supply only what truly
can't be inferred, such as the VpcId).

> **The VPC and its subnets must belong to the account you deploy into.** RAM-shared
> (cross-account) subnets are not supported: the CDC deploy role's EC2 permissions are
> scoped to this account's resources, so creating the connector's network interface in a
> shared subnet fails with `AccessDenied`.

> **Security note:** the deployed app enforces **no authentication of its own** —
> it relies on the ALB's optional Cognito gate. An internet-facing ALB left open to
> `0.0.0.0/0` **without** Cognito is blocked by the template's `Rules`
> (`CognitoRequiredWhenIngressOpen`); either enable Cognito (`EnableCognitoAuth=true`)
> or scope `AllowedIngressCidr` to your network.

### Recommended settings for a customer deployment

The stack is parameterized so you *can* take a shortcut for a quick test, but for
a real deployment these are the safer, more durable choices. Each maps to a
CloudFormation parameter in [`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md):

| Setting | Recommended | Why |
|---|---|---|
| `AlbScheme` | **`internal`** (Recommended) | Keeps the tool off the public internet — reach it over VPN / Direct Connect / VPC peering. Use `internet-facing` only when `AllowedIngressCidr` is scoped to your network (an ALB open to `0.0.0.0/0` without Cognito is blocked). |
| `EnableCognitoAuth` | **`true`** (Recommended; **required** only if `AllowedIngressCidr=0.0.0.0/0`) | The app has no authentication of its own; Cognito is the gate. Set `CognitoDomainPrefix` **and `CognitoAdminEmail`** with it — the template requires all three together, because the user pool has no self sign-up and without a first user the deploy would hand you an app nobody can log in to. |
| `AllowedIngressCidr` | **scoped to your network** (Recommended) | Restrict who can reach the ALB; don't leave it wide open (`0.0.0.0/0`). |
| `AssignPublicIp` | **`DISABLED` + NAT gateway or VPC endpoints** (Recommended for production) | `ENABLED` in public subnets is a **test-only** shortcut to skip a NAT. |
| Task egress | **VPC endpoints** (Recommended where practical) | Reach DSQL / Secrets Manager / ECR / Logs (/ Bedrock) privately, with no public path; otherwise a NAT gateway. |
| Image reference | **immutable tag or digest** (Recommended) | Reproducible deploys; avoid a moving `:latest`. |
| Activity-log CloudWatch mirror | **on** (Recommended) | A durable audit trail — the on-task `/tmp` copy is lost on task replacement. |
| Job/session state | **S3-backed** — the managed bucket (Recommended for zero-loss resume) | Survives a task replacement so an in-flight Full Load resumes; the default `/tmp` is per-task and ephemeral. |

> The fastest path to *see it run* is the **Dev/Test profile** (`internal` ALB,
> `EnableCognitoAuth=false`, self-signed cert) — still real Fargate, just fewer
> moving parts. Promote to the **Prod profile** (Cognito + a real domain/cert) for
> anything beyond evaluation. See `deploy/DEPLOYMENT.md` for both profiles.

---

## 1.4 Run on a single EC2 host (from source)

For accounts that **can't use containers/ECR or AWS Lambda**, the same tool runs on
a **single in-VPC EC2 host straight from source** — no image to build or pull. The
full, parameterized CloudFormation flow (`deploy/cloudformation-ec2.yaml`) is in
[`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md#run-on-a-single-ec2-host-from-source-lambda-free);
the essence:

- The host bootstraps from source (`git clone` + `uv sync` + a **systemd** service)
  — **no Docker, no ECR**.
- You reach the UI over an **SSM port-forward** (Session Manager), so it needs **no
  ALB, no public IP, and no inbound rule**; there is no ACM certificate or Cognito.
- App state lives on a **retained EBS volume** (not S3), so it survives an instance
  replacement.
- For CDC it seeds Kafka **in-process**, so **no offset-seeder Lambda** is created
  (CDC still auto-provisions the S3 plugin bucket for the connector artifacts).

The private in-VPC data path (source → EC2 → DSQL) is the same one Fargate gives, with
far fewer moving parts. See the
[Deployment guide](../../../deploy/DEPLOYMENT.md#run-on-a-single-ec2-host-from-source-lambda-free)
for the parameters and the SSM port-forward command.

---

## 1.5 Connect to your source and target

Open the tool and start at the **Connect** step. You provide:

| Field | What to enter |
|---|---|
| **Source** | Pick your **source engine** (MySQL or PostgreSQL), then enter host, port, user, and password — **or** a Secrets Manager secret ARN/name holding them. The default port follows the engine (**3306** for MySQL, **5432** for PostgreSQL); for PostgreSQL you also give the single **database** to connect to. Auth for both engines is password or Secrets Manager (IAM-token auth is target-DSQL only). |
| **Target** | Your Aurora DSQL **cluster endpoint**, region, database (fixed to `postgres`, shown read-only), and username (`admin` by default). **No password** — the tool generates an IAM token. |

Then click to **test** each connection. The tool:

- reads the source **read-only** to confirm reachability and permissions, and
- generates a DSQL IAM token to confirm it can connect to the target.

**Credentials live in per-session process memory only.** They are never written
to disk, logs, reports, or job state, and they are discarded when the session
ends — a strict, non-negotiable rule of the tool. After a restart you re-enter
them.

> **Single region.** This tool works in any region where Aurora DSQL is
> available, but **source and target must be in the same region** — cross-region
> migration is **not** supported. Run the tool in that region too.

Once both connections test green you move on to **Evaluation** — the tool
introspects both databases and produces a compatibility report. From there the
guided flow continues through Schema Conversion, Data Migration (where you choose
Full Load only or add CDC, and run it), Validation, and finally **Cut over**
(the runbook for switching your application to DSQL), each covered in the
following chapters.

---

**Next:** [2. Evaluation and Schema Conversion →](02-evaluation-and-schema-conversion.md)
