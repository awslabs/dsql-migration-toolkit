# 0. Before you begin

_Language: **English** | [한국어](../ko/00-before-you-begin.md) | [日本語](../ja/00-before-you-begin.md)_

Read this short page **before** you start a migration. Aurora DSQL is **not** a drop-in
replacement for MySQL or PostgreSQL — it's a distributed engine with its own rules — so
treat this as a **heterogeneous migration**. (A PostgreSQL source is closer, since both
speak the PostgreSQL-16 wire protocol, but DSQL's constraints still apply.) The tool makes
the differences explicit up front (**Evaluation**), converts what it can deterministically
(**Schema Conversion**, **Full Load**, **CDC**) and proves the result (**Validation**).
Each item below is a short "must know" plus a pointer to the chapter that covers it; the
full, enforced limits are in [Chapter 6 — Limitations](06-limitations.md).

---

## Pre-flight checklist

**Scope and access**

- [ ] **Source and target are in the same AWS region** — run the tool there too.
      Cross-region migration is **not supported**, and the optional CDC pipeline runs in a
      single region/VPC. → [§1.5](01-setup.md#15-connect-to-your-source-and-target),
      [§6.2](06-limitations.md#62-migration-process-limits)

- [ ] **Two source engines are supported:** **RDS for MySQL** / **Aurora MySQL** (**5.7 /
      8.0 / 8.4**) and **RDS for PostgreSQL** / **Aurora PostgreSQL** (PG **13–18**, all
      tested end to end; version-specific edges in
      [§6.2](06-limitations.md#62-migration-process-limits)). Both authenticate with a
      password or an **AWS Secrets Manager** username/password — IAM auth is for the DSQL
      target only. → [§1.1](01-setup.md#11-prerequisites)

- [ ] **Your source is read-only in almost every path** — a read-capable user is enough.
      Only CDC writes anything: a MySQL **Full load + CDC** run on a source without
      `gtid_mode=ON` issues one `FLUSH BINARY LOGS` (its user needs `RELOAD`), and
      **PostgreSQL CDC** creates — and at teardown drops — a replication slot and a
      publication, sets `REPLICA IDENTITY FULL` on a table whose CDC key changed, and adds a
      table selected after the Full Load to the publication (auto-committed, small
      allowlist, audited). Nothing else is ever written to a source.
      → [§1.1](01-setup.md#11-prerequisites)

- [ ] **DSQL is PostgreSQL-compatible, IAM-authenticated and distributed.** It speaks the
      PostgreSQL wire protocol, uses **short-lived IAM tokens** (no password) and
      **optimistic concurrency** (no locks). You give the tool the DSQL **cluster
      endpoint** and an AWS identity allowed to connect to it.
      → [§1.1](01-setup.md#11-prerequisites),
      [§1.5](01-setup.md#15-connect-to-your-source-and-target)

- [ ] **Credentials live in session memory only.** They're never written to disk, logs or
      reports and are discarded when the session ends, so after a restart you re-enter
      your connection details. → [§1.5](01-setup.md#15-connect-to-your-source-and-target)

**Your schema**

- [ ] **Every table you migrate needs a primary key.** DSQL distributes data by primary
      key and Full Load reads in PK order, so a table with no PK can't be migrated:
      Evaluation flags it `UNSUPPORTED` and Full Load refuses it. Add one before loading
      (single-column and composite PKs both work).
      → [§6.1](06-limitations.md#61-aurora-dsql-feature-limits-your-schema-must-fit-these)

- [ ] **DSQL omits some features your source may have:** no **triggers / stored
      procedures / events**, a **per-transaction row limit (≤ 3000)**, a **1 MiB per-value
      limit** and **no spatial types**. (**Foreign keys are supported** and enforced — the
      tool preserves them and re-creates them after the data load, for a CDC migration at
      cut over.) You don't have to hunt for these: **Evaluation** flags every one as
      `AUTO` / `MANUAL` / `UNSUPPORTED` with a recommended action. For a **PostgreSQL
      source** it also covers PG-only column types DSQL can't store: most are re-typed to
      one that holds the same data (an array → `jsonb`, `money` → `numeric`, `inet` /
      `enum` → `text`) for you to review, and only a type with no stand-in (a composite, a
      domain, an extension type such as pgvector) is `UNSUPPORTED`. Resolve the
      `UNSUPPORTED` items and decide the `MANUAL` ones before loading data.
      → [Chapter 2](02-evaluation-and-schema-conversion.md), [Chapter 6](06-limitations.md)

- [ ] **PostgreSQL row-level security needs a migration user that reads every row.** DSQL
      has no row-level security, so Evaluation grades a table with policies `UNSUPPORTED`
      (the rule moves into the application). The tool reads the source with
      `row_security = off`, as `pg_dump` does, so the user must be a superuser, have
      `BYPASSRLS`, or own the table (when it is not `FORCE ROW LEVEL SECURITY`) — otherwise
      Full Load and Validation stop on it instead of copying a subset.
      → [Chapter 2](02-evaluation-and-schema-conversion.md), [Chapter 6](06-limitations.md)

**Choosing CDC**

- [ ] **CDC is optional, and billable while it runs.** You only need streaming CDC for a
      large-scale or near-zero-downtime cut-over; it provisions MSK + MSK Connect (and
      possibly a NAT gateway) that cost money until you tear them down. For a one-shot
      migration with a short freeze, **Full Load alone** is enough and provisions no
      streaming infrastructure. → [Chapter 4](04-cdc-and-dsql-constraints.md),
      [§10.1](10-conclusion.md#101-which-path-do-i-need)

- [ ] **CDC needs source-side setup first, and it differs by engine.** The prerequisite
      gate **blocks** CDC until it's done; Full Load alone needs none of it. **MySQL:**
      ROW-format binary logging with a full row image, a replication user, and enough
      binlog retention (the gate only warns, but too-short retention is a real silent-gap
      risk). **PostgreSQL:** `wal_level=logical`, a replication user, the cluster
      **writer** endpoint, and a usable `REPLICA IDENTITY` on every replicated table (`FULL`
      works for any table except, on PostgreSQL 18, one with a generated column). On
      RDS/Aurora use parameter groups and `mysql.rds_*` stored procedures, not `my.cnf` /
      `SET GLOBAL`. → [§1.6](01-setup.md#16-prepare-the-source-for-cdc)

- [ ] **CDC replicates data, not schema.** During CDC, source **DDL** changes are
      **not** propagated to DSQL — you make the matching change on Aurora DSQL yourself
      (**Fix target schema…** for an added or renamed column, `ALTER … DROP NOT NULL` for a
      column that now allows NULL, a recreate with CDC stopped for a type or key change;
      Schema Conversion's Apply is blocked while CDC streams). Some changes are written
      with no dead letter, so validate.
      → [§4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important)

**Running the tool**

- [ ] **AI assist is optional and Amazon Bedrock-only.** It is **off by default**; when
      enabled it runs **exclusively through Amazon Bedrock** with your AWS credentials
      (`bedrock:InvokeModel` and `bedrock:InvokeModelWithResponseStream`), and the chosen
      model must be enabled once for your AWS account (its AWS Marketplace agreement
      accepted — Chapter 2). There is **no direct API-key entry**. Suggestions are
      **review-only** and AI is never in the data path; the same assistant is the read-only
      **AI DBA** panel on every step. → [Chapter 2](02-evaluation-and-schema-conversion.md),
      [Chapter 9](09-query-validation.md)

- [ ] **(If deploying on AWS) it's a single-task control plane with optional auth.** The
      usual form is one **ECS Fargate** task behind an ALB with its **optional Cognito**
      gate (enable it for any internet-facing deployment); job/session state is kept in a
      managed S3 bucket, so a task swap resumes. A **single-EC2-host-from-source** mode
      (state on a retained EBS volume, SSM port-forward, no ALB/Cognito) also exists.
      → [§1.4](01-setup.md#14-run-on-a-single-ec2-host-from-source),
      [§6.3](06-limitations.md#63-deployment-limits-the-aws-hosted-form)

---

**Next:** [1. Set up →](01-setup.md)
