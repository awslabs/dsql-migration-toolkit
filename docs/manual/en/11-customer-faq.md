# 11. Customer FAQ

_Language: **English** | [한국어](../ko/11-customer-faq.md) | [日本語](../ja/11-customer-faq.md)_

> **Prev:** [10. Conclusion](10-conclusion.md)

Heterogeneous database migration raises a lot of questions — this chapter answers
the ones customers ask most, about **Full Load**, **CDC**, **limitations**, and
**what to plan for**. Every answer reflects what the tool actually does; each links
to the chapter with the full detail. If you read only one thing first, read
[Chapter 0 — Before you begin](00-before-you-begin.md).

---

## A. Getting started & general

**Q1. What does this tool migrate, and in which direction?**

Amazon **RDS / Aurora MySQL → Amazon Aurora DSQL**, one direction only. This is a
**heterogeneous** migration (MySQL → PostgreSQL dialect → DSQL constraints), not a
version upgrade — DSQL is PostgreSQL-wire-compatible, distributed, serverless, and
IAM-authenticated. The **source is read-only the entire time**; the tool never
writes to your MySQL.


**Q2. Is Aurora DSQL a drop-in replacement for Aurora MySQL?**

No. DSQL speaks the **PostgreSQL** wire protocol, authenticates with short-lived
**IAM tokens** (no password), uses **optimistic concurrency** instead of locks,
and intentionally omits features that don't scale horizontally — **no foreign
keys, no triggers/stored procedures, a per-transaction row limit, a 1 MiB
per-value limit**, and more. Your schema and, in places, your application must
adapt. Evaluation (step 2) tells you exactly where.


**Q3. Should I run the tool locally or deploy it on AWS (ECS Fargate)?**

It's the same web UI either way — pick based on your use case.

- **Run locally** — launch it on your laptop/workstation (defaults to
  `http://127.0.0.1:8080`). **Fastest to start**, and a good fit for the
  compatibility assessment or a smaller migration.
- **Deploy on AWS (ECS Fargate)** — the form most teams use for a real migration.
  It runs as a single task behind an Application Load Balancer (optionally gated by
  Amazon Cognito), so a team can share access and run a long migration reliably.

In short: **local for a quick evaluation or a small job, Fargate for a real
production migration.** Either way the source is accessed read-only; see how to run
each in [Chapter 1 §1.2–§1.3](01-setup.md).


**Q4. Do source and target have to be in the same AWS region?**

**Yes.** Source and target must be in the **same region**, and the optional CDC
pipeline runs in a single region/VPC. **Cross-region migration is not supported.**


**Q5. What are the two data paths, and which do I need?**

| Your situation | Use |
|---|---|
| One-shot migration; a short maintenance write-freeze is acceptable | **Full Load only** — no streaming infrastructure, no ongoing cost. |
| Large-scale / continuous; you need **near-zero-downtime** cut-over | **Full Load + CDC** — a gapless handoff keeps DSQL converging while MySQL stays live. |

CDC adds real moving parts (MSK, MSK Connect, a sink connector) and **costs money
while deployed**. Use it only when you genuinely need continuous replication;
otherwise Full Load alone is simpler and cheaper. See
[Chapter 10 §10.1](10-conclusion.md#101-which-path-do-i-need).


**Q6. Does the tool cut over my application for me?**

No. The tool **assesses, converts, loads, streams, and validates** — but *when*
and *how* you repoint your application is an operational decision only you can
make. It gives you an evidence-backed go/no-go (Validation) and, for CDC, a gapless
stream; you perform the cut-over using the runbook it shows
([Chapter 10 §10.3](10-conclusion.md#103-the-cut-over-switching-your-application-to-dsql)).

---

## B. Full Load (the bulk copy)

**Q7. How does Full Load work — and is it safe for very large tables?**

Full Load is the tool's **own bulk loader** (not a Debezium snapshot). It **streams**
source rows by **primary-key keyset pagination** (`WHERE pk > :last ORDER BY pk
LIMIT n`, never `OFFSET`) over a server-side cursor and writes them as they flow,
so **memory stays bounded by one page regardless of table size** — a whole table is
never loaded into RAM. It's architected for **large-scale** sources with very large
tables. See [Chapter 3 — Full Load](03-full-load.md).


**Q8. Is Full Load idempotent? What if it's interrupted?**

Yes. Rows load in bounded-parallel, idempotent `INSERT … ON CONFLICT` batches, each
mapped to a stable PK range. If a load is **interrupted** (crash, stop, task
replacement), re-running it **re-runs only the unfinished PK ranges** and converges
to exactly the uninterrupted state — **no duplicates, no loss**. Per-table failures
are isolated: one bad table doesn't abort the others.


**Q9. Does Full Load need binary logging on the source?**

**No.** Full Load alone needs none of the CDC prerequisites (no binlog, no
replication user). It reads the source with an ordinary read-only connection. Only
**CDC** requires binlog (see Q12).


**Q10. Can I tune Full Load throughput?**

Yes, with bounded defaults you can raise for big hardware or lower to protect a busy
source: `DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM` (tables at once, default 4),
`…_BATCH_PARALLELISM` (in-flight batches per table, default 8), `…_BATCH_ROWS`
(rows per write, default 2000, hard-capped at DSQL's 3000). Total concurrent DSQL
connections ≈ table × batch parallelism — keep it within the cluster's connection
quota. See [Chapter 7 §7.2](07-performance-and-tuning.md#72-tuning-parallelism).


**Q11. Why does raising parallelism sometimes not speed things up?**

DSQL distributes storage **by primary key**, and a monotonic `AUTO_INCREMENT` PK
makes every insert target the same "rightmost" key range — a **write hot-spot**. At
high parallelism this raises the optimistic-concurrency conflict rate (`SQLSTATE
40001`), so throughput gains flatten. This is why Evaluation flags `AUTO_INCREMENT`
and Schema Conversion offers PK strategies (keep integer / convert to UUID /
identity-with-caching). See [Chapter 7 §7.1](07-performance-and-tuning.md#71-why-this-design-the-technical-case).

---

## C. CDC (streaming replication)

**Q12. What does CDC require on the source?**

CDC requires binary logging in **ROW format with a full row image**
(`binlog_format=ROW`, `binlog_row_image=FULL`) and a user with **replication
privileges**. You must also **raise binlog retention** so the log at the Full Load
watermark survives until CDC starts (Aurora MySQL keeps binlogs only ~24 h by
default). On RDS/Aurora these are set via **parameter groups** and
`mysql.rds_set_configuration`, not `my.cnf`. The prerequisite gate **blocks CDC**
until these are met. See [§1.1](01-setup.md#11-prerequisites) and
[Chapter 6 §6.2](06-limitations.md#62-migration-process-limits).


**Q13. How is the Full Load → CDC handoff "gapless"?**

Full Load captures a **binlog/GTID watermark** at the consistent snapshot point.
CDC starts streaming from **exactly that watermark**, and because the apply is
idempotent and PK-keyed, any overlap between the snapshot and the stream **can't
create duplicates**. The result is no gap and no double-apply. See
[Chapter 4 — CDC](04-cdc-and-dsql-constraints.md).


**Q14. What does CDC replicate — and what does it NOT?**

CDC replicates **row data changes** (INSERT/UPDATE/DELETE), not SQL and **not
DDL**. A schema change on the source **during** CDC is **not** propagated to DSQL —
you must apply the equivalent DDL to DSQL yourself. A row that no longer matches the
target's shape goes to the **DLQ** (dead-letter), not lost. See
[Chapter 4 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important).


**Q15. Why a custom DSQL sink connector instead of a stock JDBC sink?**

Three DSQL-specific reasons a stock sink can't handle: (1) it retries write
conflicts at the **statement level** (only the conflicting row re-runs) instead of
re-submitting the whole batch, which avoids livelock on hot keys; (2) it mints and
**refreshes short-lived IAM tokens** so an hours-long stream never stalls on auth;
(3) it enforces DSQL's 3000-row / 1 MiB envelopes and routes poison rows to a DLQ.
See [Chapter 4 §4.1](04-cdc-and-dsql-constraints.md#41-the-pipeline).


**Q16. Can CDC be paused, and is it billable?**

The streaming pipeline (MSK Serverless + MSK Connect, plus a NAT gateway if the tool
created one) **costs money for as long as it runs** — there is no free "pause". Tear
the cdc-stack down after cut-over (the **Cut over** step's *Delete all CDC
infrastructure* does this). Full Load alone provisions **no** streaming
infrastructure. See [Chapter 6 §6.2](06-limitations.md#62-migration-process-limits).


**Q17. How long does CDC infrastructure take to deploy / tear down — and why so long?**

Both deploy and teardown take roughly **15–25 minutes**. It isn't instant, by
design — here's why.

**Why startup (deploy) takes a while** — the cdc-stack creates several AWS
resources in order:

1. **Provisioning the MSK Serverless cluster** — waiting for the managed Kafka
   cluster to become ready (the biggest single chunk).
2. **Creating the two MSK Connect connectors** — the source (Debezium) connector
   first (so its topics exist), then the sink (DSQL) connector. Each takes a few
   minutes to go `CREATING → RUNNING`, and they run in sequence, so the times add up.
3. Meanwhile the in-VPC **offset-seeder Lambda** seeds the offset for the gapless
   handoff.

**Why teardown takes a while** — deleting MSK itself is relatively quick, but the
last resource to go — the in-VPC **offset-seeder Lambda** — is the bottleneck.
Deleting a VPC-attached Lambda forces AWS to clean up the **network interfaces
(ENIs)** it used, and that cleanup is a documented AWS behavior that can take **up
to ~20 minutes** (in our own testing it accounted for most of the delete time). The
Lambda **must** live inside the VPC to reach the private MSK bootstrap, so this
delay is hard to avoid on teardown.

> **Tip:** for iterating or restarting, **restart in place (keep the existing
> infrastructure)** rather than full delete-and-recreate where possible — it avoids
> this ~20-minute ENI cleanup. Tear the stack down for good once cut-over is done and
> you no longer need CDC.

---

## D. Schema & type conversion

**Q18. How does the tool decide what can move?**

Evaluation classifies **every object** as **AUTO** (converts automatically),
**MANUAL** (converts but needs a decision/app-side change), or **UNSUPPORTED** (no
automatic conversion — redesign needed), with effort estimates and name-conflict
detection. Resolve every UNSUPPORTED item and decide each MANUAL item **before** you
load. See [Chapter 2](02-evaluation-and-schema-conversion.md).


**Q19. How are MySQL data types mapped to DSQL?**

Here are the highlights:

| MySQL type | Aurora DSQL type | Note |
|---|---|---|
| `TINYINT(1)` | `boolean` | MySQL's boolean convention. |
| `…INT UNSIGNED` (unsigned integers) | `smallint` / `integer` / `bigint` / `numeric(20,0)` | Widened **losslessly** to a wider type. |
| `BIT(n)` | sized integer (`smallint`…`numeric(20,0)`) | DSQL has no `BIT` type. |
| `DECIMAL(p,s)` | `numeric(p,s)` | Precision > 38 is unsupported. |
| `DATETIME` | `timestamp` (UTC) | Normalized to UTC. |
| `ENUM` | `text` + `CHECK` | DSQL has no `ENUM`. |
| `BLOB` / `BINARY` | `bytea` | Raw bytes preserved. |
| `JSON` | `json` | |

The **complete mapping for every type** (target DSQL type + stored value form) is in
[Chapter 4 §4.6](04-cdc-and-dsql-constraints.md#46-mysql--dsql-type-and-constraint-handling-reference).
The **Full Load loader and the CDC sink honor the identical mapping** (a shared
write-contract test enforces it), so a row lands the same whichever path migrates it.


**Q20. What happens to foreign keys, triggers, and stored procedures?**

DSQL has none of these. **Foreign keys** are removed from the DDL but **preserved in
the report** (enforce referential integrity in your application) — flagged MANUAL.
**Triggers, stored procedures/functions, and scheduled events** are flagged
UNSUPPORTED — reimplement them in your application (scheduled events → EventBridge /
Lambda). See [Chapter 6 §6.1](06-limitations.md#61-aurora-dsql-feature-limits-your-schema-must-fit-these).


**Q21. My table has no primary key. Can I migrate it?**

Not as-is. DSQL **requires a primary key**, and the tool's keyset export also needs
one, so a PK-less table is flagged **UNSUPPORTED** and can't be loaded. Add a
primary key on the source (or in your redesign) first.

---

## E. Validation & correctness

**Q22. How do I know the migration is correct?**

Validation (step 5) gives you **evidence**, comparing source vs target in **three
increasingly strict passes** — each more precise (and more expensive) than the last.

| Pass | What it checks | What it catches |
|---|---|---|
| **Row count** | Exact per-table `COUNT(*)` on each side | Whole rows missing or extra |
| **Checksum** (order-independent) | Hashes every row the **same way on both engines** and compares | Rows that **match in number but differ in value** |
| **Full primary-key reconciliation** | Merges **every PK** on both sides and names the PKs **missing on the target** and **extra on the target** | Exactly **which rows** are missing/extra, row by row |

- The verdict is **MATCH only when every difference is explained** — e.g. **drift**
  from the source still changing during migration (Q23), an intentional
  **oversized-value quarantine** (Q24), or **CDC still catching up**. A single
  unexplained missing/extra row means it is *not* a MATCH.
- **A count-only "match" is never trusted.** Equal counts can still hide differing
  values, so a table must also pass the checksum and PK reconciliation to count as a
  true match.

So running this before cut-over means you decide to switch based on **proof that
source and target actually match**, not on hope. See
[Chapter 5 — Validation](05-validation.md).


**Q23. The source keeps changing during migration — won't validation be wrong?**

No — it's attributed correctly. Validation reads the source GTID against the Full
Load watermark and reports **drift**, so a count delta from **new source activity**
is distinguished from a bug. For a clean final go/no-go, freeze source writes and
let CDC drain first (Q27).


**Q24. What happens to rows DSQL can't store (e.g. a value > 1 MiB)?**

They are **never silently dropped**. In Full Load a value over DSQL's ~1 MiB
per-value limit is **quarantined** per-row (its primary key + reason recorded in the
error log) while the rest of the table loads; in CDC such a row goes to the **DLQ**.
Values that can't even traverse the pipeline (> ~8 MiB) are **excluded at capture**,
driven by the `OVERSIZED_LOB` flag from Evaluation. You see exactly what was set
aside. See [Chapter 6 §6.1](06-limitations.md#61-aurora-dsql-feature-limits-your-schema-must-fit-these).


**Q25. What does "loud over silent" mean in practice?**

The tool refuses to corrupt data quietly. Examples: a `TINYINT(1)` value outside
`{0,1}` **stops that table's load** rather than flattening it to `true`; an
out-of-range `TIME` fails rather than being truncated; an incomplete load is
reported **FAILED**, never a false success. You fix the source (or exclude the
column) and re-run.

---

## F. Cut-over, rollback & operations

**Q26. Is there downtime at cut-over?**

With **Full Load only**, downtime is the length of the final freeze + load +
validation. With **Full Load + CDC**, it shrinks to a brief **final drain + smoke
test** — CDC keeps DSQL converging while MySQL stays live, so you freeze only at the
very end.


**Q27. What's the safe cut-over sequence?**

(CDC path) Let CDC catch up → **freeze source writes** → wait for the final drain
(lag → 0) → **re-run Validation** → cut over only on a clean MATCH → repoint the app
and smoke-test → **tear the CDC pipeline down last**. Full details and the Full-Load-only
variant are in [Chapter 10 §10.3](10-conclusion.md#103-the-cut-over-switching-your-application-to-dsql).


**Q28. Can I roll back?**

Yes, if you plan it. Keep the MySQL source **frozen (read-only), not dropped** until
you've signed off on DSQL — before you repoint, rollback is trivial (the source is
untouched and authoritative). **After** the application writes to DSQL, those new
rows live **only** on DSQL (this tool replicates MySQL → DSQL, **not** the reverse),
so rolling back then means reconciling them yourself. Decide your rollback rule
*before* you cut over.


**Q29. What credentials and permissions are needed, and how is my data protected?**

The source uses an ordinary read-only MySQL connection; the target uses DSQL's
**IAM-token auth (no DB password)**. All connections use **TLS**. Credentials live
in **per-session memory only** — never written to disk, logs, reports, or job state.
Logs and reports record primary keys and counts, **never row values**. Optional
AI-assist (Amazon Bedrock, off by default) is **advisory only** and **never** in the
CDC data path.

The AWS permissions the tool needs are the same whether you run it locally or on
Fargate — what differs is **where those permissions attach**:

- **Required (both modes):** `dsql:DbConnect` / `dsql:DbConnectAdmin` to mint the
  DSQL IAM token (scoped to your cluster), plus read-only access to the source
  MySQL. Optionally `secretsmanager:GetSecretValue` (to reuse a source-credentials
  secret) and `bedrock:InvokeModel` (only if you enable AI-assist).
- **Run locally** — the tool uses **your own IAM identity** via the standard
  credential chain (`~/.aws`, env vars, `AWS_PROFILE`, SSO), so *your* identity must
  hold those permissions directly. In particular, to use CDC your identity needs the
  broader **infrastructure-creation permissions** the cdc-stack requires, because it
  provisions MSK, MSK Connect, and IAM roles via CloudFormation.
- **Deploy on AWS (ECS Fargate)** — you don't hand credentials to the app.
  CloudFormation creates a **least-privilege Task Role** and attaches those
  permissions to it. Privileged work such as deploying the CDC infrastructure is not
  granted to the long-running task role; instead the task **assumes a dedicated
  deploy role (CdcDeployRole) only for the duration of that operation**, so the
  privilege escalation is isolated. (Whoever *first deploys* the app-stack does need
  permission to create IAM roles, once.) This mode fits least-privilege best.

The concrete IAM actions, by what you're doing (on **Fargate these attach to the
auto-created Task Role — you don't write them**; when **running locally your own
identity must hold them**):

| For | IAM actions (minimum) |
|---|---|
| **Target DSQL (always)** | `dsql:DbConnect`, `dsql:DbConnectAdmin` (mint the IAM token), `dsql:GetCluster` |
| **Source MySQL (always)** | none in IAM — an ordinary read-only MySQL user/password (or the secret below) |
| **Source secret (optional)** | `secretsmanager:GetSecretValue` — only if the source credentials come from a Secrets Manager secret |
| **AI assist (optional)** | `bedrock:InvokeModel`, `bedrock:InvokeModelWithResponseStream` — scoped to the chosen model; only when AI is enabled |
| **CDC — deploy/tear down the pipeline** | the broad infra-creation set the cdc-stack needs: `cloudformation:CreateStack`/`UpdateStack`/`DeleteStack`/`Describe*`/`GetTemplate`; a wide `ec2:*` set (VPC subnets, NAT, EIP, route tables, security groups, VPC endpoints, network interfaces); `iam:CreateRole`/`AttachRolePolicy`/`PassRole`/… (the connector roles); `kafka:*` / `kafkaconnect:*` (MSK Serverless + MSK Connect); `logs:*`; `s3:*` on the plugin bucket. Full-Load-only migrations need **none** of this. |

> **Full Load only** needs just the first three rows. **CDC** adds the last row —
> a large infrastructure-creation surface. On Fargate that surface lives on the
> isolated `CdcDeployRole` (assumed only during a deploy/teardown), so the
> always-running task never holds it; **running locally, your own identity must
> hold the entire CDC row**, which is why Fargate is recommended for CDC.


**Q30. Can I run more than one migration / scale the tool horizontally?**

The AWS-hosted form is a **single ECS Fargate task** whose job/session state is local
SQLite on ephemeral storage — a task replacement loses in-flight job state (you
reconnect and re-run the read-only steps). Don't run more than one task without
moving that state to a shared store (e.g. an EFS mount for zero-loss resume). See
[Chapter 6 §6.3](06-limitations.md#63-deployment-limits-the-aws-hosted-form).


**Q31. How do I remove ALL the AWS infrastructure and start completely fresh?**

First, be clear on the difference: the UI's **Start over** button only resets the
tool's *session* (connections, plan, progress) — it deliberately does **not** delete
any AWS resource, so nothing costly is torn down by accident. To actually remove the
infrastructure and stop all cost, tear the stacks down in this order (the full
procedure with exact commands is [Deployment §9 — Teardown](../../../deploy/DEPLOYMENT.md#9-teardown)):

1. **CDC infrastructure first** (only if you ever deployed CDC — this is the costly
   MSK / MSK Connect / NAT part). Do it **while the app is still running**, from the
   UI: **Start over (top right) → "Delete all CDC infrastructure"** (the app drives
   the `cdc-stack` deletion, ~15–25 min, and also removes the managed source-
   credentials secret it created). If the app is already gone, delete the stack
   manually: `aws cloudformation delete-stack --stack-name mysql-dsql-cdc-stack`.
2. **app-stack** — run `deploy/teardown.sh <stack-name>` (deletes the ECS/ALB/IAM
   stack; pass `DELETE_ECR=true` to also remove the container image repo).
3. **build-stack** — only if you used the optional CodeBuild build path.
4. **Check for leftovers the stacks don't own:** the tool auto-creates a per-account/
   region **plugin bucket** `mysql-dsql-migrator-plugins-<account>-<region>` that is
   NOT part of any stack, so `delete-stack` won't remove it — delete it manually if
   you want a truly clean slate. Also remove any **Route 53** records you added by
   hand. Then confirm no `mysql-dsql-*` CloudFormation stacks remain.

After teardown you can redeploy from scratch exactly like a first-time install
(Deployment guide). If you only want to *restart the migration itself* — same infra,
fresh workflow — use **Start over** instead; it's instant and free.

---

## G. Where to go next

- **Plan your migration:** [Chapter 0 — Before you begin](00-before-you-begin.md).
- **The enforced limits to design around:** [Chapter 6 — Limitations](06-limitations.md).
- **Prove correctness:** [Chapter 5 — Validation](05-validation.md).
- **Choose a path and cut over:** [Chapter 10 — Conclusion](10-conclusion.md).
