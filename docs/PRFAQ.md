# PRFAQ — MySQL to Aurora DSQL Migration Tool

> **INTERNAL WORKING DOCUMENT — Working Backwards PR/FAQ**
> Product: MySQL → Amazon Aurora DSQL Migration Tool
> Status: Draft
> Format follows the Amazon PR/FAQ mechanism (press release written as if GA,
> then a brutally honest FAQ). All technical claims are grounded in the tool's
> implementation and Aurora DSQL's documented behavior; placeholder names/dates
> are marked `[…]`.

---

## Part 1 — Press Release

### MySQL to Aurora DSQL Migration Tool
*Assess, convert, bulk-load, stream, and validate a MySQL → Aurora DSQL
migration from a single guided web workflow — with the source read-only the
whole time.*

**For teams moving Amazon RDS / Aurora MySQL workloads to Amazon Aurora DSQL,
this tool removes the weeks of manual, error-prone work that a heterogeneous
migration normally demands and replaces it with a guided, evidence-backed
cut-over.**

`SEATTLE, WA — [TARGET GA DATE] —` AWS today announced general availability of
the MySQL to Aurora DSQL Migration Tool, a web application (run as a single-task
Amazon ECS Fargate service) that automates the full MySQL → Aurora DSQL
migration lifecycle: a read-only compatibility **assessment**, deterministic
**schema conversion**, a purpose-built **bulk Full Load**, an optional
near-zero-downtime **streaming CDC** path (Debezium → Amazon MSK → a custom
Aurora DSQL sink connector), and an authoritative **validation** report — all
from one browser workflow, with the source database accessed read-only
throughout.

**The problem.** Aurora DSQL is not MySQL: it is PostgreSQL-wire-compatible,
distributed, serverless, and IAM-authenticated, and it intentionally omits
features MySQL applications rely on — no foreign keys, no triggers or stored
procedures, a 3,000-row-per-transaction limit, a 1 MiB per-value limit, and
optimistic concurrency instead of locks. Moving to it today means an engineer
manually reconciling every incompatible type and object, hand-writing a loader
that respects DSQL's transaction envelope and retries `SQLSTATE 40001` conflicts,
discovering DSQL's constraints one failed load at a time, and validating by hand.
A mid-size migration (tens-to-hundreds of tables, hundreds of GB to multiple TB)
routinely costs **engineer-weeks** and carries real risk of **silent data drift**
at cut-over. No AWS-managed tool handles the DSQL-specific DDL constraints, the
Full-Load-to-streaming handoff, and the validation layer end-to-end — AWS
Database Migration Service does not support Aurora DSQL as a target.

**The solution.** With the tool, an engineer opens a browser, connects the source
MySQL and the target DSQL cluster (no password — the tool mints short-lived IAM
tokens), and within minutes gets a **compatibility report** that classifies every
object `AUTO` / `MANUAL` / `UNSUPPORTED` with a reason, a recommended action, and
an effort estimate. After reviewing the converted DDL side-by-side and applying
it, the tool runs a **Full Load** that streams the source by primary-key pages
(bounded memory at any table size) into batched, idempotent `INSERT … ON CONFLICT`
writes sized to DSQL's limits, with statement-level OCC retry and async index
builds — and captures a binlog/GTID watermark. For a live cut-over, **CDC**
resumes streaming from exactly that watermark (gapless, no duplicates).
**Validation** then proves the target matches the source by row count, checksum,
and full primary-key reconciliation, accounting for drift on a live source.
Engineer time to a confident cut-over drops from weeks to hours.

> *"Database migrations are one of the highest-anxiety events in a customer's
> cloud journey — every hour of downtime and every drifted row erodes trust. This
> tool turns a move to Aurora DSQL into something a team can do confidently in a
> sprint, with evidence, instead of a quarter-long project they dread."*
> — [Leader, AWS Databases]

> *"We'd put off the DSQL migration for months because of our schema — ~180
> tables, JSON columns, foreign keys everywhere, and a hard no-data-loss bar. The
> tool flagged every incompatibility up front, gave us conversion DDL we could
> actually review, and ran the CDC stream so we didn't have to stand up a Debezium
> pipeline ourselves. We cut over on a Saturday in about four hours, and the
> validation report showed every table matched."*
> — [Lead Database Engineer, mid-size SaaS]

**Getting started.** Deploy the tool's app-stack (one CloudFormation template;
the connector artifacts are committed so no Java/Maven toolchain is needed) or run
it locally with `uv run mysql-dsql-migrator ui`. Connect a **non-production** MySQL
database and run an assessment in minutes. See the
[User Manual](manual/README.md).

**Call to action.** Read the [manual](manual/README.md) and the
[architecture overview](../README.md); start with a read-only assessment against a
test database.

---

## Part 2 — Tenets

1. **Customer data safety over feature velocity.** If a step risks silent data
   loss the user hasn't acknowledged, we fail **loudly** and stop. A visible,
   fixable failure (e.g. a `TINYINT(1)` value outside `{0,1}` that can't become a
   DSQL `boolean`) always beats a quiet, wrong value. An incomplete load is never
   reported as success.

2. **Deterministic first; AI assists, never decides.** Conversion and apply are
   deterministic (`sqlglot` + a fixed type/constraint contract). Optional Amazon
   Bedrock suggestions are review-only, require explicit human approval, and are
   **never** placed in the data path.

3. **The source is sacred and read-only.** We never write to the customer's MySQL
   source. Credentials live in per-session memory only — never on disk, in logs,
   in reports, or in job state.

4. **Built for TB scale, not demos.** Every data-path choice is bounded in memory
   and resumable, and respects Aurora DSQL's real limits (3,000 rows / 10 MiB /
   5 min per transaction, 1 MiB per value, OCC). If a design doesn't hold at a
   billion-row table, we redesign it before shipping it.

5. **Prove it, don't claim it.** A migration isn't "done" until Validation
   produces evidence (counts, checksums, PK reconciliation) the operator can
   attach to a cut-over decision.

---

## Part 3 — FAQ

### Customer / External FAQs

**C1. Which sources and targets are supported?**
Source: Amazon RDS MySQL / Aurora MySQL (read-only). Target: Amazon Aurora DSQL.
**Source and target must be in the same AWS region** — cross-region migration is
not supported, and the streaming CDC pipeline runs in a single region/VPC.

**C2. What exactly does the assessment flag, and what do I do about it?**
Every object is classified `AUTO` (converts automatically), `MANUAL` (converts but
needs a decision or app-side change — e.g. foreign keys, `AUTO_INCREMENT`, CI
collation, partitioning, oversized LOB, `ENUM`/`SET`, generated columns,
multi-database), or `UNSUPPORTED` (no automatic conversion — triggers, stored
procedures/functions, events, no primary key, spatial types, `DECIMAL` precision
> 38, > 255 columns/table, > 1,000 tables/database, FULLTEXT/SPATIAL indexes). Each
item carries a reason, a recommendation, and an effort estimate. You resolve the
`UNSUPPORTED` items and decide the `MANUAL` ones before loading.

**C3. How does the bulk Full Load work, and is it safe for huge tables?**
It streams the source by **primary-key keyset pagination** over a server-side
cursor inside a consistent snapshot, so memory stays bounded by one page
regardless of table size. Rows are written in **batched `INSERT … ON CONFLICT`**
(≤ 3,000 rows, ≤ 8 MiB, within DSQL's bind-parameter limit) across a bounded
connection pool, each batch with statement-level `40001` (OCC) retry. Secondary
indexes are built **after** the load with `CREATE INDEX ASYNC`. Loads are
idempotent and resumable: a stop/retry re-runs only the unfinished primary-key
ranges, with no duplicates.

**C4. How does CDC work and how is the handoff gapless?**
Full Load captures a binlog/GTID **watermark**. CDC seeds the connector's start
offset to that watermark and runs Debezium with `snapshot.mode=recovery`, so it
streams the **first change after the snapshot** — no gap, no re-reading data — and
the sink's idempotent PK-keyed upserts/deletes make even an overlap safe. **CDC
replicates row data, not DDL**: source schema changes during CDC are not
propagated; you re-apply equivalent DDL to DSQL yourself.

**C5. Why a custom DSQL sink connector instead of a stock JDBC sink?**
A stock JDBC sink retries `40001` conflicts **per batch**, which collapses
throughput under high-contention TB-scale CDC. The custom sink retries at the
**statement level**, handles DSQL's short-lived IAM tokens (refresh before
expiry), keeps batches within the 3,000-row limit, and reconnects safely.

**C6. What does Validation actually check — and what does it not?**
Per table: exact `COUNT(*)`; optional order-independent **checksum**; optional
**full primary-key reconciliation** (every PK on both sides, reporting which rows
are missing/extra). It accounts for **live-source drift** via the watermark GTID.
A table is reported "matched" only when the evidence supports it. Reconciliation
applies to single-column integer PKs (well-defined cross-engine ordering).

**C7. Is there downtime at cut-over?**
With CDC, near-zero: DSQL stays continuously updated until you switch the
application over. With Full Load only, downtime is the freeze window you choose for
the final load. The tool does not flip your application's connection string — you
control the cut-over moment.

**C8. What credentials/permissions are needed, and how is data protected?**
Source: a read-capable MySQL user (or a Secrets Manager secret). Target: an AWS
identity allowed to mint DSQL IAM tokens (`dsql:DbConnect`/`DbConnectAdmin`);
**no DB password**. Connections use TLS; credentials live in per-session memory
only and are never persisted. Quarantine/DLQ logs record primary keys and reasons,
**never row values**.

**C9. What happens to rows DSQL can't store?**
A value over DSQL's **1 MiB** limit is quarantined per-row at Full Load and at the
CDC sink (recorded by PK + reason) while the rest of the table proceeds; values
> 8 MiB are excluded at capture. A value that can't be converted without silent
loss (e.g. `TINYINT(1)` = 2 → DSQL `boolean`) fails that table loudly rather than
corrupting data.

**C10. Can I tune throughput?**
Yes. Full Load table/batch parallelism and Validation parallelism are environment
variables (`DSQL_MIGRATOR_FULL_LOAD_*`, `DSQL_MIGRATOR_VALIDATE_MAX_WORKERS`); CDC
scales via cdc-stack parameters (`SinkTasksMax`, MSK partitions, worker MCU/count).
See the manual's [Performance and tuning](manual/en/07-performance-and-tuning.md)
chapter. Keep total DSQL connections within the cluster quota and mind the OCC
collision rate on hot keys.

### Internal / Stakeholder FAQs

**I1. Why build this vs. AWS DMS?**
DMS does not support Aurora DSQL as a target. Even a generic "full load" is JDBC
`INSERT` under the hood with no DSQL-specific OCC handling, no DSQL transaction-
envelope batching, and no custom sink for streaming. The DSQL-specific gaps (DDL
constraints, gapless Full-Load→CDC handoff, statement-level OCC, validation) are
exactly what this tool fills.

**I2. What does it cost to operate?**
The control plane is one small ECS Fargate task (and an ALB) — negligible. The
**optional** CDC data plane (MSK Serverless + MSK Connect, and a NAT gateway if
created) is billable **only while deployed**; tear it down after cut-over. Aurora
DSQL itself meters DPUs — the batched-load + after-load async-index design is
chosen partly to minimize per-transaction DPU overhead vs. a naive row-by-row or
huge-transaction loader.

**I3. What's the failure mode if the custom DSQL sink has a bug?**
Records that can't be applied are isolated to the DLQ (surfaced via CloudWatch),
not silently dropped; a record that can be neither applied nor dead-lettered fails
the task **loudly**. Apply is idempotent, so replaying offsets after a fix can't
create duplicates. The source MySQL stays live throughout CDC, so the fallback is
always "keep running on the source." (A known class of subtle bug — a silent
SUCCESS-on-failure in the offset seeder, and a Debezium schema-history gap — was
found and fixed during end-to-end testing.)

**I4. What survives a control-plane (Fargate task) failure?**
The deployed cdc-stack, the DSQL cluster, and migrated data are external and
survive. In-flight job/session state lives on the task's ephemeral storage and is
lost on replacement — the operator reconnects and re-runs the read-only steps; an
interrupted Full Load resumes from completed PK ranges. For zero-loss resume, the
job/session state can be pointed at a shared EFS mount. It is a **single-task**
control plane by design; do not scale beyond one task without shared state.

**I5. What's the security posture?**
No app-level auth of its own — it relies on the ALB's **optional Cognito** gate,
and the deploy template **blocks** an internet-facing ALB without Cognito.
Least-privilege IAM (task role scoped to DSQL connect + scoped Secrets + optional
scoped Bedrock; privileged CDC CloudFormation operations isolated on a separate
assumed deploy role). Credentials never persisted; DLQ/error logs are value-free.

**I6. How does this relate to the Aurora DSQL roadmap (will it become
redundant)?**
If/when first-party DSQL migration tooling closes these gaps, this tool's value
narrows to its UX and its custom streaming sink. The design keeps the deterministic
engine importable and the connector independently useful, so components can be
contributed or retired rather than stranded.

**I7. How was it tested?**
Unit tests cover the conversion contract, the loader's batching/quarantine/resume
logic, the sink's error classification, the offset seeder, config, and UI seams
(no test touches a real MySQL/DSQL/AWS — seams are injected). End-to-end runs
against real RDS MySQL + Aurora DSQL + MSK validated Full Load + CDC + Validation,
including deliberate failure rows (oversized LOB, out-of-range `TINYINT(1)`) and a
final 3/3-table MATCH after fixing the gapless-handoff bugs.

**I8. Who owns it long-term, and what's the rollback story for a customer?**
[Owning team / TPM]. Customer rollback: because the source stays live and
read-only through cut-over, the rollback is to keep serving from MySQL until the
DSQL target is proven by Validation; nothing on the source is mutated to undo.

---

*See also: [User Manual](manual/README.md) · [Architecture & AWS services](../README.md) · [Deployment](../deploy/DEPLOYMENT.md)*
