# 11. Customer FAQ

_Language: **English** | [한국어](../ko/11-customer-faq.md) | [日本語](../ja/11-customer-faq.md)_

> **Prev:** [10. Conclusion](10-conclusion.md)

Short answers to the questions customers ask most, about **Full Load**, **CDC**,
**limitations**, and **what to plan for**; each links to the chapter with the full detail.
If you read only one thing first, read
[Chapter 0 — Before you begin](00-before-you-begin.md).

---

## A. Getting started & general

**Q1. What does this tool migrate, and in which direction?**

Amazon **RDS / Aurora MySQL or PostgreSQL → Amazon Aurora DSQL**, one direction only.
Supported sources are **RDS for MySQL** and **Aurora MySQL** (**5.7 / 8.0 / 8.4**) and
**RDS for PostgreSQL** and **Aurora PostgreSQL** (**PG 13–18**; the tool enforces no hard
version floor or ceiling). Both paths — Schema Conversion, Full Load, CDC and cut over —
are **validated end to end** in testing, with a checksum. This is a **heterogeneous**
migration, not a version upgrade: from MySQL the path is **MySQL → PostgreSQL dialect →
DSQL constraints**, and from PostgreSQL it is a **near-identity PG → DSQL** pass-through.

The **source is read-only** except for CDC's few bookkeeping writes: a PostgreSQL
replication slot and publication (dropped at teardown), `REPLICA IDENTITY FULL` on a
re-keyed table, and, for a MySQL Full load + CDC run on a source without `gtid_mode=ON`,
one `FLUSH BINARY LOGS` (it needs `RELOAD` and writes no data). The exact list is in
[Chapter 0](00-before-you-begin.md).

**Q2. Is Aurora DSQL a drop-in replacement for my source engine (Aurora MySQL /
PostgreSQL)?**

No. DSQL speaks the **PostgreSQL** wire protocol, authenticates with short-lived **IAM
tokens** (no password), uses **optimistic concurrency** instead of locks, and
intentionally omits features that don't scale horizontally — **no triggers/stored
procedures, a per-transaction row limit, a 1 MiB per-value limit**, and more (foreign
keys, by contrast, **are supported and enforced**). Your schema and, in places, your
application must adapt. Evaluation (step 1) tells you exactly where.

**Q3. Should I run the tool locally, deploy it on ECS Fargate, or run it on a single EC2
host?**

It's the same web UI in all three; pick by use case:

| Option | Pick it for | What it is |
|---|---|---|
| **Run locally** | A quick evaluation or a small job | The web UI on your laptop or workstation (default `http://127.0.0.1:8080`); the fastest to start |
| **Deploy on AWS (ECS Fargate)** | A real production migration | A single task behind an Application Load Balancer (optionally gated by Amazon Cognito), so a team can share access and a long migration runs reliably |
| **Run on a single EC2 host** (from source) | Accounts that can't use containers/ECR or AWS Lambda | The same engine in-VPC straight from source (`git clone` + `uv` + a systemd service), reached over an SSM port-forward, with state on a retained EBS volume and no ALB / ECR / Cognito front door |

Either way the source is accessed read-only. See how to run local and Fargate in
[Chapter 1 §1.2–§1.3](01-setup.md), and the EC2-host path in the
[Deployment guide](../../../deploy/DEPLOYMENT.md#run-on-a-single-ec2-host-from-source-lambda-free).

**Q4. Do source and target have to be in the same AWS region?**

**Yes.** Source and target must be in the **same region**, and the optional CDC pipeline
runs in a single region/VPC. **Cross-region migration is not supported.**

**Q5. What are the two data paths, and which do I need?**

| Your situation | Use |
|---|---|
| One-shot migration; a short maintenance write-freeze is acceptable | **Full Load only** — no streaming infrastructure, no ongoing cost. |
| Large-scale / continuous; you need **near-zero-downtime** cut-over | **Full Load + CDC** — a gapless handoff keeps DSQL converging while the source stays live. |

CDC adds real moving parts (MSK, MSK Connect, a sink connector) and **costs money while
deployed**. Use it only when you genuinely need continuous replication; otherwise Full
Load alone is simpler and cheaper. See
[Chapter 10 §10.1](10-conclusion.md#101-which-path-do-i-need).

**Q6. Does the tool cut over my application for me?**

No. The tool **assesses, converts, loads, streams, and validates** — but *when* and *how*
you repoint your application is an operational decision only you can make. It gives you an
evidence-backed go/no-go (Validation) and, for CDC, a gapless stream; you perform the
cut-over using the runbook it shows
([Chapter 10 §10.3](10-conclusion.md#103-the-cut-over-switching-your-application-to-dsql)).

---

## B. Full Load (the bulk copy)

**Q7. How does Full Load work — and is it safe for very large tables?**

Full Load is the tool's **own bulk loader** (not a Debezium snapshot). It **streams**
source rows by **primary-key keyset pagination** (never `OFFSET`) and writes them as they
flow, so **memory stays bounded by one page regardless of table size** — a whole table is
never loaded into RAM. It's architected for **large-scale** sources with very large
tables. See [Chapter 3 — Full Load](03-full-load.md).

**Q8. Is Full Load idempotent? What if it's interrupted?**

Yes. Rows load in bounded-parallel, idempotent batches, so writing a row again never
duplicates it. If a load is **interrupted** (crash, stop, task replacement), the tables it
finished are kept, and **Retry unfinished tables** reloads each unfinished table **from
its start**, skipping the rows it had already written (with **Drop & reload**, the table
is recreated first). It converges to exactly the uninterrupted state — **no duplicates, no
loss** — but a very large table interrupted near its end is read again in full. Per-table
failures are isolated: one bad table doesn't abort the others.

**Q8a. What if the source Aurora cluster fails over mid-load?**

It's handled automatically. A writer promotion (patching, an instance replacement, an AZ
event) closes every open source connection, so a multi-hour load will meet one. The
affected table is **re-read automatically** from a fresh consistent snapshot (3 attempts
by default, with a 15s → 30s → 60s backoff to let DNS re-point at the promoted writer).
Rows already written are skipped, so the retry costs re-read I/O, not duplicates.

With Full load + CDC, the stream replayed from the run's start point corrects every row
that changed between the two reads; with Full load only, if the source took writes, re-run
with **Drop & reload**. A MySQL table read in parts (*Reader shards per large table*,
[§3.8](03-full-load.md#38-parallel-loading)) is not re-read: the table fails, and **Retry
unfinished tables** loads it again.

Only **connection-level** failures retry; a data or schema error fails at once. If the
retries run out, the per-table error says so and re-running is safe. Tune the attempts
with `DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_ATTEMPTS` (1 = no retry).

**Q9. Does Full Load need binary logging on the source?**

**No.** Full Load alone needs none of the CDC prerequisites (no binlog and no replication
user on MySQL; no `wal_level=logical`, replication slot, or publication on PostgreSQL). It
reads the source with an ordinary read-only connection on either engine. Only **CDC**
needs those prerequisites (see Q12).

**Q10. Can I tune Full Load throughput?**

Yes, with bounded defaults you can raise for big hardware or lower to protect a busy
source: `DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM` (tables at once, default 4),
`…_BATCH_PARALLELISM` (in-flight batches per table, default 8), `…_BATCH_ROWS` (rows per
write, default 2000, hard-capped at DSQL's 3000). Total concurrent DSQL connections ≈
table × batch parallelism — keep it within the cluster's connection quota. See
[Chapter 7 §7.2](07-performance-and-tuning.md#72-tuning-parallelism).

**Q11. Why does raising parallelism sometimes not speed things up?**

DSQL distributes storage **by primary key**, and a monotonic auto-incrementing PK (MySQL
`AUTO_INCREMENT`, or a PostgreSQL `serial` / `IDENTITY` / sequence-backed key) makes every
insert target the same "rightmost" key range — a **write hot-spot**. At high parallelism
this raises the optimistic-concurrency conflict rate (`SQLSTATE 40001`), so throughput
gains flatten. This is why Evaluation flags such monotonic PKs regardless of source engine
and Schema Conversion offers PK strategies (a cached identity — the default for a
generated key — / keep integer / composite key). See
[Chapter 7 §7.1](07-performance-and-tuning.md#primary-key-strategy--avoid-hot-partitions).

---

## C. CDC (streaming replication)

**Q12. What does CDC require on the source?**

It depends on the source engine. The prerequisite check **blocks CDC** until the required
items are met and only **warns** about the rest.

**MySQL source**

- **Required:** binary logging in **ROW** format with a full row image
  (`binlog_format=ROW`, `binlog_row_image=FULL`) and a user with **replication
  privileges**. On RDS/Aurora, set them through parameter groups and
  `mysql.rds_set_configuration`, not `my.cnf`.
- **Raise binlog retention** so the log at the Full Load watermark survives until CDC
  starts (Aurora MySQL keeps binlogs only about 24 h by default). A retention that looks
  too short only warns.

**PostgreSQL source**

- **Required:** `wal_level=logical` (RDS/Aurora: set `rds.logical_replication=1` in a
  custom parameter group, then reboot the writer); a **replication-privileged user**; the
  cluster **writer**, not a standby or reader; and a usable **`REPLICA IDENTITY`** on
  every replicated table (`FULL` works for any table except, on PostgreSQL 18, one with a
  generated column; on a partitioned table every partition needs one).
- Replication-slot and `max_wal_senders` headroom only **warn**.
- There is **no binlog retention** to set: the tool creates the publication and a logical
  slot, and the slot pins the WAL, so watch `wal_status`, because an inactive, unconsumed
  slot can fill the source disk.

See [§1.6](01-setup.md#16-prepare-the-source-for-cdc) and
[Chapter 6 §6.2](06-limitations.md#62-migration-process-limits).

**Q13. How is the Full Load → CDC handoff "gapless"?**

Full Load captures a **watermark** at the consistent snapshot point — for a MySQL source a
**binlog/GTID position**, for a PostgreSQL source a **WAL LSN** pinned by a pre-created
logical replication slot. CDC starts streaming from **exactly that watermark**, and
because the apply is idempotent and PK-keyed, any overlap between the snapshot and the
stream **can't create duplicates**. The result is no gap and no double-apply. See
[Chapter 4 — CDC](04-cdc-and-dsql-constraints.md).

**Q14. What does CDC replicate — and what does it NOT?**

CDC replicates **row data changes** (INSERT/UPDATE/DELETE), not SQL and **not DDL**. A
schema change on the source **during** CDC is **not** propagated to DSQL — you make the
matching change on Aurora DSQL yourself (**Fix target schema…** for an added or renamed
column, otherwise an `ALTER` or the recreate in §4.2; Schema Conversion's Apply is blocked
while CDC streams). Most rows that no longer match the target go to the **DLQ**
(dead-letter) and hold Cut over until the table matches by CHECKSUM, but some changes are
written with no dead letter, so validate with CHECKSUM after any source DDL. See
[Chapter 4 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important).

**Q15. Why a custom DSQL sink connector instead of a stock JDBC sink?**

A stock sink can't handle three DSQL specifics: (1) on a write conflict (`40001`) it must
re-run the **whole batch transaction**, an idempotent PK-keyed upsert/delete, with
**exponential backoff and full jitter**; (2) it must mint and **refresh short-lived IAM
tokens**, so an hours-long stream never stalls on auth; (3) it must respect DSQL's
3,000-row / 1 MiB envelopes and route poison rows to a DLQ. See
[Chapter 4 §4.1](04-cdc-and-dsql-constraints.md#41-the-pipeline).

**Q16. Can CDC be paused, and is it billable?**

The streaming pipeline (MSK Serverless + MSK Connect, plus a NAT gateway if the tool
created one) **costs money for as long as it runs** — there is no free "pause". Tear the
cdc-stack down after cut-over (via **Start over (top right)** → *Delete all CDC
infrastructure*; the Cut over runbook points you there). Full Load alone provisions **no**
streaming infrastructure. See
[Chapter 6 §6.2](06-limitations.md#62-migration-process-limits).

**Q17. How long do the CDC steps take — and why so long?**

CDC has **three** long-running operations, and they are separate. Budget for all three (a
common surprise is treating *Start CDC* as instant because the infrastructure is already
up):

| Operation | Typical | What dominates it |
|---|---|---|
| **Deploy CDC infrastructure** | **~15–20 min** | Waiting for the MSK Serverless cluster to become ready |
| **Start CDC** (create the connectors) | **~20–30 min** | Each MSK Connect connector going `CREATING → RUNNING` |
| **Delete CDC infrastructure** | **~15–25 min** | ENI cleanup for the VPC-attached Lambda (see below) |

None of this is instant, by design. **Deploying** waits for the MSK Serverless cluster to
become ready (AWS-side; nothing to tune). **Start CDC** creates the two MSK Connect
connectors (source = Debezium, sink = DSQL) in one pass and waits for both to reach
`RUNNING`, which takes many minutes, so plan for it separately from the deploy.
**Teardown** on a MySQL source is slow because deleting the last resource, the in-VPC
offset-seeder Lambda, forces AWS to clean up the network interfaces (ENIs) it used, which
can take up to ~20 minutes; a **PostgreSQL** teardown has no such Lambda and no
ENI-cleanup delay.

> **Tip:** when iterating or restarting, keep the existing infrastructure instead of
> deleting and recreating it, which avoids the ~20-minute ENI cleanup. **Stop CDC**
> preserves the MSK cluster, plugins, VPC and IAM (it only removes the two connectors), so
> a later **Start CDC** skips the infrastructure wait and resumes where it left off — a
> MySQL source from the offsets kept in the cluster's `connect-offsets` topic, a
> PostgreSQL source from its logical slot's LSN position. Stop CDC does not drain the
> sink, though: the changes it had not applied by then are applied from the CDC topics, so
> that resume is gapless only while the topics still retain them (their retention period
> on MSK; for a PostgreSQL source, [§4.3](04-cdc-and-dsql-constraints.md) names the route
> once it has passed). Tear the stack down for good once cut-over is done and you no
> longer need CDC.

Each operation records its **outcome** (succeeded or failed, with the elapsed time) in the
activity log, so you can confirm afterwards that, for example, the Stop you ran before
cut-over really completed.

---

## D. Schema & type conversion

**Q18. How does the tool decide what can move?**

Evaluation classifies **every object** as **AUTO** (converts automatically), **MANUAL**
(converts but needs a decision/app-side change), or **UNSUPPORTED** (no automatic
conversion — redesign needed), with effort estimates and name-conflict detection. Resolve
every UNSUPPORTED item and decide each MANUAL item **before** you load. See
[Chapter 2](02-evaluation-and-schema-conversion.md).

**Q19. How are source data types mapped to DSQL?**

For a **MySQL source**, here are the highlights:

| MySQL type | Aurora DSQL type | Note |
|---|---|---|
| `TINYINT(1)` | `boolean` | MySQL's boolean convention. |
| `…INT UNSIGNED` (unsigned integers) | `smallint` / `integer` / `bigint` / `numeric(20,0)` | Widened **losslessly** to a wider type. |
| `BIT(n)` | sized integer (`smallint`…`numeric(20,0)`) | DSQL has no `BIT` type. |
| `DECIMAL(p,s)` | `numeric(p,s)` | Precision and scale kept (Aurora DSQL allows precision up to 1000). |
| `DATETIME` | `timestamp` | Wall clock copied as it is, no zone shift; a default that reads the clock names the source's time zone (`SOURCE_TIME_ZONE`). |
| `ENUM` | `text` + `CHECK` | DSQL has no `ENUM`. |
| `BLOB` / `BINARY` | `bytea` | Raw bytes preserved. |
| `JSON` | `json` | |

The **complete mapping for every type** (its Aurora DSQL type and what to know) is in
[Chapter 2 §2.3](02-evaluation-and-schema-conversion.md#23-type-mapping-reference). The
Full Load loader and the CDC sink apply the identical mapping, so a row lands the same
whichever path migrates it.

For a **PostgreSQL source**, conversion and the data path are a **near-identity PG-16 →
DSQL pass-through**: the types Aurora DSQL supports carry over **verbatim**. A type with
no equivalent is re-typed where a stand-in keeps the value (arrays → `jsonb` or a child
table; `money` → `numeric(23,4)`; `enum`, network, geometric, `xml`, `bit`, `tsvector` and
`range` types → `text`), with a warning, and graded **MANUAL** (the application adapts). A
composite type, a domain, or an extension type such as pgvector has no stand-in and is
**UNSUPPORTED** until you remodel it. `STORED` generated columns are **preserved**;
`VIRTUAL` ones become ordinary columns and must be recomputed in the application.

**Q20. What happens to foreign keys, triggers, and stored procedures?**

**Foreign keys are supported** — Aurora DSQL enforces them. The tool **preserves** each
source FK and re-creates it after the load as
`ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY … NOT VALID`: added at once, then validated by
Aurora DSQL against the rows already loaded, in the background. After a Full Load that is
the **Apply foreign keys** action; for a **CDC** migration it is deferred to **cut over**,
because the constraints must not exist while the sink streams rows out of order. Two
PostgreSQL 18 kinds are not created: a key declared `NOT ENFORCED` (`PG_FK_NOT_ENFORCED`)
and a `FOREIGN KEY … PERIOD` (`PG_FK_PERIOD`: enforce it in the application). A PostgreSQL
key keeps its `MATCH FULL` and `DEFERRABLE` / `INITIALLY DEFERRED`. You can instead **opt
to strip** foreign keys and enforce referential integrity in the application. Weigh the
runtime costs: DML on related tables incurs **extra reads**, a concurrent conflict is a
retryable `40001`, and a `CASCADE`/`SET NULL`/`SET DEFAULT` action counts toward the
3,000-row transaction limit.

**Triggers, stored procedures/functions and scheduled jobs** are omitted and flagged
UNSUPPORTED, so reimplement them in your application (scheduled jobs, such as MySQL's
Event Scheduler or a PostgreSQL scheduler like `pg_cron`, move to EventBridge / Lambda).
See
[Chapter 6 §6.1](06-limitations.md#61-aurora-dsql-feature-limits-your-schema-must-fit-these).

**Q21. My table has no primary key. Can I migrate it?**

Not as-is. Aurora DSQL can create a table without a primary key, but this tool needs one —
Full Load reads by primary-key keyset and CDC applies changes by key — so a PK-less table
is flagged **UNSUPPORTED** and can't be loaded. Add a primary key on the source (or in
your redesign) first; Evaluation names a UNIQUE index whose columns are all NOT NULL,
which already identifies each row, when there is one.

---

## E. Validation & correctness

**Q22. How do I know the migration is correct?**

Validation (step 4) gives you **evidence**, comparing source against target in **three
increasingly strict passes**:

| Pass | What it checks | What it catches |
|---|---|---|
| **Row count** | Exact per-table `COUNT(*)` on each side | Whole rows missing or extra |
| **Checksum** (order-independent) | Hashes every row the **same way on both engines** and compares | Rows that **match in number but differ in value** |
| **Full primary-key reconciliation** | Merges **every PK** on both sides and names the PKs **missing on the target** and **extra on the target** | Exactly **which rows** are missing/extra, row by row |

- The verdict is **MATCH only when every difference is explained**: **drift** from a
  source that kept changing (Q23), an intentional **oversized-value quarantine** (Q24), or
  **CDC still catching up**. A single unexplained missing or extra row means it is *not* a
  MATCH.
- **A count-only "match" is never trusted.** A table must also pass the checksum and PK
  reconciliation. With the optional **Fast sweep** (deep-check only the tables whose
  counts differ), the tables it skipped are labelled *verified by row count only*, and you
  can deep-check just those from the report.
- **A few column types are excluded from the checksum by design**, because their text form
  is not stable across engines: floating-point (MySQL `FLOAT`/`DOUBLE`; PostgreSQL
  `real`/`double precision`) and `json`. On a PostgreSQL source `jsonb` **is**
  checksummed. Row counts, PK reconciliation and every other column still validate.

**Fixed a mismatch? Re-check that one table.** Each failing table has a **Re-check**
action (plus *Re-check all N*) that re-compares only that table and merges the result into
the report, so you don't re-run a multi-hour validation to confirm one fix, and the
go/no-go verdict updates on its own.

Running this before cut-over means you switch on **proof that source and target match**,
not on hope. See [Chapter 5 — Validation](05-validation.md).

**Q23. The source keeps changing during migration — won't validation be wrong?**

No — it's attributed correctly. Validation compares the source's current position with the
Full Load watermark and reports **drift** (by **GTID**, or binlog **file:position** when
GTID isn't enabled, on MySQL; the current **WAL LSN** against the Full Load LSN on
PostgreSQL), so a count delta from **new source activity** is distinguished from a bug. On
a PostgreSQL source the tool does not recompute an LSN-based drift figure; watch CDC
convergence instead. For a clean final go/no-go, freeze source writes and let CDC drain
first (Q27).

**Q24. What happens to rows DSQL can't store (e.g. a value > 1 MiB)?**

They are **never silently dropped**. In Full Load, a value over DSQL's ~1 MiB per-value
limit is **quarantined** per row (its primary key and the reason go in the error log)
while the rest of the table loads; in CDC, such a row goes to the **DLQ**. A value too
large to travel through the pipeline at all (> ~8 MiB) is **excluded at capture**: add a
rule under **Exclude tables & columns** (Data Migration step), or use **+ add rule** in
its *Oversized-LOB candidates* section, which lists the columns Evaluation flags
`OVERSIZED_LOB` (**Check sizes** measures them on request). An excluded column is NULL in
the rows the tool writes; rows already on Aurora DSQL before CDC started keep their
values, and Validation counts them. See
[Chapter 6 §6.1](06-limitations.md#61-aurora-dsql-feature-limits-your-schema-must-fit-these).

**Q25. What does "loud over silent" mean in practice?**

The tool refuses to corrupt data quietly. **MySQL-source** examples: a `TINYINT(1)` value
outside `{0,1}` **stops that table's load** rather than flattening it to `true`; an
out-of-range `TIME` fails rather than being truncated. (A **PostgreSQL** source Full Load
is a pure pass-through — psycopg native on both ends — so it has few such value
conversions.) Either way, an incomplete load is reported **FAILED**, never a false
success. You fix the source (or exclude the column) and re-run.

---

## F. Cut-over, rollback & operations

**Q26. Is there downtime at cut-over?**

With **Full Load only**, downtime is the length of the final freeze + load + validation.
With **Full Load + CDC**, it shrinks to a brief **final drain + smoke test** — CDC keeps
DSQL converging while the source stays live, so you freeze only at the very end.

**Q27. What's the safe cut-over sequence?**

(CDC path) Let CDC catch up → **freeze source writes** → wait for the final drain (lag → 0)
→ **re-run Validation** → cut over only on a clean MATCH → repoint the app and smoke-test
→ **tear the CDC pipeline down last**. Full details and the Full-Load-only variant are in
[Chapter 10 §10.3](10-conclusion.md#103-the-cut-over-switching-your-application-to-dsql).

**Q28. Can I roll back?**

Yes, if you plan it. Keep the source **frozen (read-only), not dropped** until you've
signed off on DSQL — before you repoint, rollback is trivial (the source is untouched and
authoritative). **After** the application writes to DSQL, those new rows live **only** on
DSQL (this tool replicates source → DSQL, **not** the reverse), so rolling back then means
reconciling them yourself. Decide your rollback rule *before* you cut over.

**Q29. What credentials and permissions are needed, and how is my data protected?**

The source uses an ordinary read-only connection to **MySQL or PostgreSQL** (a password,
or an AWS Secrets Manager username/password — **never IAM**; IAM-token auth is target-DSQL
only); CDC adds replication privileges, and a MySQL Full load + CDC run needs `RELOAD`
unless the source runs `gtid_mode=ON` ([§1.6](01-setup.md#16-prepare-the-source-for-cdc)).
The target uses DSQL's **IAM-token auth (no DB password)**. All connections use **TLS**.
Credentials live in **per-session memory only**, never written to disk, logs, reports, or
job state, and logs and reports record primary keys and counts, **never row values**.
Optional AI assist (Amazon Bedrock, off by default) is **advisory only** and **never** in
the CDC data path.

The AWS permissions are the same whether you run locally or on Fargate; what differs is
**where they attach**. **Locally**, the tool uses **your own IAM identity** through the
standard credential chain (`~/.aws`, env vars, `AWS_PROFILE`, SSO), so your identity must
hold them directly (for CDC, the broader infrastructure-creation set in the last row
below). On **Fargate**, CloudFormation creates a **least-privilege Task Role**; privileged
work such as deploying the CDC infrastructure is not granted to the long-running task,
which instead **assumes a dedicated deploy role (CdcDeployRole) only for the duration of
that operation**. (Whoever first deploys the app-stack needs permission to create IAM
roles, once.)

The concrete IAM actions, by what you're doing (on **Fargate these attach to the
auto-created Task Role — you don't write them**; when **running locally your own identity
must hold them**):

| For | IAM actions (minimum) |
|---|---|
| **Target DSQL (always)** | `dsql:DbConnect`, `dsql:DbConnectAdmin` (mint the IAM token), `dsql:GetCluster` |
| **Source (always)** | none in IAM — an ordinary read-only MySQL or PostgreSQL user/password (or the Secrets Manager secret below); CDC adds replication privileges, and a MySQL Full load + CDC run `RELOAD` unless the source runs `gtid_mode=ON` |
| **Source secret (optional)** | `secretsmanager:GetSecretValue` — only if the source credentials come from a Secrets Manager secret |
| **AI assist (optional)** | `bedrock:InvokeModel`, `bedrock:InvokeModelWithResponseStream` — on the Fargate stack any Anthropic model unless `BedrockModelArns` narrows it (the EC2 stack scopes it to the chosen model); only when AI is enabled |
| **CDC — deploy/tear down the pipeline** | the broad infra-creation set the cdc-stack needs: `cloudformation:CreateStack`/`UpdateStack`/`DeleteStack`/`Describe*`/`GetTemplate`; a wide `ec2:*` set (VPC subnets, NAT, EIP, route tables, security groups, VPC endpoints, network interfaces); `iam:CreateRole`/`AttachRolePolicy`/`PassRole`/… (the connector roles); `kafka:*` / `kafkaconnect:*` (MSK Serverless + MSK Connect); `logs:*`; `s3:*` on the plugin bucket. Full-Load-only migrations need **none** of this. |

> **Full Load only** needs just the first three rows. **CDC** adds the last row — a large
> infrastructure-creation surface. On Fargate that surface lives on the isolated
> `CdcDeployRole` (assumed only during a deploy/teardown), so the always-running task
> never holds it; **running locally, your own identity must hold the entire CDC row**,
> which is why Fargate is recommended for CDC.

**Q30. Can I run more than one migration / scale the tool horizontally?**

The AWS-hosted form is a **single ECS Fargate task**; don't run more than one. Its job and
session state are already kept in the tool's managed S3 bucket, so a task replacement
keeps the workbench and the Full Load record (a job running at that moment is interrupted,
and Retry unfinished tables reloads each table it did not finish from its start). The
one-task limit is about **single-writer orchestration**: CDC deploys are coordinated, and
each session's live state is held, inside that one task. See
[Chapter 6 §6.3](06-limitations.md#63-deployment-limits-the-aws-hosted-form).

**Q31. How do I remove ALL the AWS infrastructure and start completely fresh?**

First, be clear on the difference: the UI's **Start over** button only resets the tool's
*session* (connections, plan, progress) — it deliberately does **not** delete any AWS
resource, so nothing costly is torn down by accident. To actually remove the
infrastructure and stop all cost, tear the stacks down in this order (the full procedure
with exact commands is
[Deployment §9 — Teardown](../../../deploy/DEPLOYMENT.md#teardown)):

1. **CDC infrastructure first** (only if you ever deployed CDC — this is the costly MSK /
   MSK Connect / NAT part). Do it **while the app is still running**, from the UI: **Start
   over (top right) → "Delete all CDC infrastructure"** (the app drives the `cdc-stack`
   deletion, ~15–25 min, and also removes the managed source- credentials secret it
   created; on PostgreSQL it keeps that secret if it cannot drop the replication slot,
   since it holds the credentials needed to drop the slot). If the app is already gone,
   delete the stack manually:
   `aws cloudformation delete-stack --stack-name dsql-cdc-stack` (use your cdc-stack's
   name: the default is `dsql-cdc-stack`; a legacy one may be `mysql-dsql-cdc-stack`).
2. **app-stack** — run `deploy/teardown.sh <stack-name>` (deletes the ECS/ALB/IAM stack;
   pass `DELETE_ECR=true` to also remove the container image repo).
3. **build-stack** — only if you used the optional CodeBuild build path.
4. **Check for leftovers the stacks don't own:** the tool auto-creates a per-account/
   region **plugin bucket** `mysql-dsql-migrator-plugins-<account>-<region>` that is NOT
   part of any stack, so `delete-stack` won't remove it — delete it manually if you want a
   truly clean slate (`aws s3 rb` with `--force`; it cannot be undone, and every
   deployment of the tool in that account and region shares the bucket, so only once none
   of them is in use). It keeps a copy of every version of a CDC plugin file that a deploy
   uploaded (`cdc-plugins/<hash>/<file>`, up to about 31 MiB each), and nothing prunes
   them; on ECS Fargate it also holds the saved session and job state. Also remove any
   **Route 53** records you added by hand. Then confirm no `mysql-dsql-*` or `dsql-cdc-*`
   CloudFormation stacks remain.

After teardown you can redeploy from scratch exactly like a first-time install (Deployment
guide). If you only want to *restart the migration itself* — same infra, fresh workflow —
use **Start over** instead; it's instant and free. If CDC has streamed on the CDC stack
you keep, a fresh Full load + CDC run on it needs **Delete CDC infrastructure** and a new
deploy first: the Full Load step refuses a new start point until then
([Chapter 3 §3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)). Where the target
tables still hold the earlier run's rows, choose **Drop & reload** in the new run's Start
Full Load dialog: the default *Append* keeps them as they were.

---

## G. Where to go next

- **Plan your migration:** [Chapter 0 — Before you begin](00-before-you-begin.md).
- **The enforced limits to design around:** [Chapter 6 — Limitations](06-limitations.md).
- **Prove correctness:** [Chapter 5 — Validation](05-validation.md).
- **Choose a path and cut over:** [Chapter 10 — Conclusion](10-conclusion.md).
