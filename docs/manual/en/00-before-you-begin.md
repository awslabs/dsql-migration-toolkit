# 0. Before you begin

_Language: **English** | [한국어](../ko/00-before-you-begin.md) | [日本語](../ja/00-before-you-begin.md)_

Read this short page **before** you start a migration. Aurora DSQL is **not** a
version of MySQL — it's a different engine with different rules — so a handful of
facts will shape your plan from the very first step. Each item below is a
one-line "must know" plus a pointer to the chapter that covers it in depth.

> This page is the **pre-flight checklist**. It does not replace the detailed
> chapters — it makes sure you don't discover a constraint *after* you've already
> committed to an approach. The full, enforced limits are in
> [Chapter 6 — Limitations](06-limitations.md).

---

## Pre-flight checklist

- [ ] **Source and target are in the same AWS region.** The tool works in any
      region where Aurora DSQL is available, but **source and target must be in
      the same region** — **cross-region migration is not supported**, and the
      optional CDC pipeline runs in a single region/VPC. Run the tool in that
      region too. → [§1.5](01-setup.md#15-connect-to-your-source-and-target),
      [§6.2](06-limitations.md#62-migration-process-limits)

- [ ] **Your source is only ever read.** The tool never writes to your RDS /
      Aurora MySQL source — a read-capable user is enough. Supported sources:
      **RDS for MySQL** and **Aurora MySQL**, versions **5.7 / 8.0 / 8.4**.
      → [§1.1](01-setup.md#11-prerequisites)

- [ ] **DSQL is PostgreSQL-compatible, IAM-authenticated, and distributed.** It
      speaks the PostgreSQL wire protocol, uses **short-lived IAM tokens** (no
      password to manage), and uses **optimistic concurrency** (no locks). You
      give the tool the DSQL **cluster endpoint** and an AWS identity allowed to
      connect to it. → [§1.1](01-setup.md#11-prerequisites),
      [§1.5](01-setup.md#15-connect-to-your-source-and-target)

- [ ] **Every table you migrate to DSQL must have a primary key.** DSQL
      distributes and stores data **by primary key**, so a PK is required, and the
      tool's Full Load reads data in PK order. A table with no PK cannot be
      migrated — Evaluation flags it `UNSUPPORTED` and Full Load refuses it. Add a
      PK to such tables before loading. (Both single-column and composite PKs are
      supported.) → [§6.1](06-limitations.md#61-aurora-dsql-feature-limits-your-schema-must-fit-these)

- [ ] **DSQL intentionally omits features MySQL has.** No **foreign keys**, no
      **triggers / stored procedures / events**, a **per-transaction row limit
      (≤ 3000)**, a **1 MiB per-value limit**, `DECIMAL` **precision ≤ 38**, and
      **no spatial types**. You don't have to find these yourself — the
      **Evaluation** step inspects your schema and flags every one as
      `AUTO` / `MANUAL` / `UNSUPPORTED` with a recommended action. Plan to resolve
      the `UNSUPPORTED` items and decide the `MANUAL` ones before loading data.
      → [Chapter 2](02-evaluation-and-schema-conversion.md),
      [Chapter 6](06-limitations.md)

- [ ] **CDC is optional, and billable while it runs.** You only need streaming
      CDC for a large-scale or near-zero-downtime cut-over; it provisions MSK +
      MSK Connect (and possibly a NAT gateway) that cost money until you tear them
      down. For a one-shot migration with a short freeze, **Full Load alone** is
      enough and provisions no streaming infrastructure. → [Chapter 4](04-cdc-and-dsql-constraints.md),
      [§10.1](10-conclusion.md#101-which-path-do-i-need)

- [ ] **CDC requires source binlog set up first (managed-MySQL way).** If you'll
      use CDC, the source must have **binary logging on in ROW format with a full
      row image** (`binlog_format=ROW`, `binlog_row_image=FULL`) and a user with
      **replication privileges** — the prerequisite gate **blocks** CDC until both
      are met. Also **raise binlog retention** so the binlog at the Full Load
      watermark still exists when CDC starts; the gate only **warns** here, but
      too-short retention is a real silent-gap risk. On RDS/Aurora this is done via
      **parameter groups** and the `mysql.rds_set_configuration` **stored
      procedure** — not `my.cnf`/`SET GLOBAL` as on community MySQL. Full Load alone
      needs none of this. → [§1.1](01-setup.md#11-prerequisites)

- [ ] **CDC replicates data, not schema.** During CDC, source **DDL** changes are
      **not** propagated to DSQL — you re-apply them yourself via Schema
      Conversion. → [§4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important)

- [ ] **Credentials live in session memory only.** They're never written to disk,
      logs, or reports, and are discarded when the session ends — so after a
      restart you re-enter your connection details. → [§1.5](01-setup.md#15-connect-to-your-source-and-target)

- [ ] **AI assist is optional and Amazon Bedrock-only.** AI is **off by default**;
      when enabled it works **exclusively through Amazon Bedrock** using your AWS
      credentials (the `bedrock:InvokeModel` permission). There is **no direct
      API-key entry** (no Anthropic/OpenAI key field) — the only way to use AI is
      Bedrock. Every suggestion is **review-only** and AI is never in the data
      path. The same assistant is available on **every step** as the **AI DBA**
      panel — a read-only helper that can diagnose Full Load / CDC failures, triage
      the DLQ, and check prerequisites. → [Chapter 2](02-evaluation-and-schema-conversion.md),
      [Chapter 9](09-query-validation.md)

- [ ] **(If deploying on AWS) it's a single-task control plane with optional auth.**
      The usual form is one **ECS Fargate** task behind an ALB with the ALB's
      **optional Cognito** gate (enable it for any internet-facing deployment); on
      the AWS-hosted deploy, job/session state is kept durable in a managed S3
      bucket so a task swap resumes. A **single-EC2-host-from-source** mode also
      exists — state on a retained EBS volume, reached over an SSM port-forward, no
      ALB/Cognito. → [§1.4](01-setup.md#14-run-on-a-single-ec2-host-from-source),
      [§6.3](06-limitations.md#63-deployment-limits-the-aws-hosted-form)

---

## The mindset

Treat this as a **heterogeneous migration**, not an upgrade. The tool's whole job
is to make DSQL's differences explicit up front (**Evaluation**), convert what it
can deterministically (**Schema Conversion**, **Full Load**, **CDC**), and prove
the result (**Validation**). If you internalize the checklist above, the rest of
the manual is just the detail.

---

**Next:** [1. Set up →](01-setup.md)
