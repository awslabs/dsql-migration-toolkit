# mysql-dsql-migrator — User Manual (English)

_Language: **English** | [한국어](../ko/README.md)_

A guided manual for migrating an **Amazon RDS / Aurora MySQL** database to
**Amazon Aurora DSQL** with this tool. It is written for a **database operations
(DB Operation) practitioner who knows MySQL well and is about to start using
Aurora DSQL** — because of its distributed-database design, DSQL differs
considerably even from PostgreSQL, and this manual explains *how* the tool helps
you convert across those differences.

> New to the project? Read the [top-level README](../../../README.md) for the
> architecture and AWS-services overview first; this manual is the task-oriented
> companion that walks you through actually running a migration.
>
> This manual assumes the tool is **already running** (locally or on AWS). If it
> isn't up yet, deploy it with [`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md)
> first, then come back — [Set up](01-setup.md) also covers running locally.

## What this tool is

It is a **web tool** (and importable engine) that performs a **heterogeneous,
deterministic-first migration**: MySQL → PostgreSQL-dialect → DSQL constraints.
The **source is always read-only**. The migration is a six-step guided flow,
with **Connect** as the preliminary step:

```
Connect → 1. Migration plan → 2. Evaluation → 3. Schema Conversion → 4. Data Migration → 5. Validation → 6. Cut over
```

Data Migration is **Full Load** (the tool's own bulk loader) and, optionally,
streaming **CDC** (a separate, optional pipeline for near-zero-downtime cut-over).
The final step, **Cut over**, is the operational runbook for switching your
application to DSQL once Validation passes.

## Manual contents

| # | Chapter | What you'll learn |
|---|---|---|
| 0 | [Before you begin](00-before-you-begin.md) | The pre-flight checklist — the must-know facts (same-region only, read-only source, DSQL's omitted features, CDC is optional/billable) that shape your plan from step one. **Start here.** |
| 1 | [Set up](01-setup.md) | Prerequisites, how to run the tool (local or on AWS), and how to connect to your source and target. |
| 2 | [Evaluation and Schema Conversion](02-evaluation-and-schema-conversion.md) | How the tool assesses what will/won't move to DSQL (AUTO / MANUAL / UNSUPPORTED, effort estimates, name conflicts) and converts + applies the schema. |
| 3 | [Full Load](03-full-load.md) | How the bulk snapshot load works: streaming export, batched idempotent load, the watermark, and how failures are isolated. |
| 4 | [CDC and DSQL constraints](04-cdc-and-dsql-constraints.md) | How streaming CDC works, the gapless Full Load → CDC handoff, and how DSQL's constraints (no FK, 1 MiB values, OCC, IAM auth) are handled in the data path. |
| 5 | [Validation](05-validation.md) | How the tool proves the target matches the source: row counts, checksums, full PK reconciliation, and live-source drift. |
| 6 | [Limitations](06-limitations.md) | The real, enforced limits you must plan around (DSQL constraints, single-region CDC, single-task control plane). |
| 7 | [Performance and tuning](07-performance-and-tuning.md) | Why the data path is built this way (AWS-grounded: OCC retry, hot-partition PKs, transaction envelope, async indexes, IAM tokens), how to tune Full Load / Validation / CDC parallelism — locally and on Fargate — and a reproducible measured example backing the rationale. |
| 8 | [Testing — the DSQL-driven scenarios](08-testing-and-verification.md) | The migration scenarios each Aurora DSQL characteristic *forces* you to test (transaction caps, OCC, 1 MiB values, IAM tokens, async indexes, no-FK, gapless handoff, drift) and how the tool exercises each — offline and on real AWS. |
| 9 | [Query validation and the AI DBA](09-query-validation.md) | The optional Query Playground: convert a single MySQL query to Aurora DSQL, test it read-only on the target (`EXPLAIN` / `EXPLAIN ANALYZE` + DPU cost), and have the **AI DBA** rewrite it for DSQL efficiency and prove the improvement by re-testing. |
| 10 | [Conclusion](10-conclusion.md) | When to use which path, a recommended end-to-end flow, and where to go next. |
| 11 | [Customer FAQ](11-customer-faq.md) | The questions customers ask most — Full Load, CDC, limitations, type mapping, validation, cut-over/rollback, and operations — each answered from the tool's actual behavior with links to the detail. |

## A note for MySQL users about Aurora DSQL

Aurora DSQL is **not MySQL** and **not a drop-in Aurora MySQL replacement**. It
speaks the **PostgreSQL** wire protocol, authenticates with **short-lived IAM
tokens** (no password), is **distributed** (optimistic concurrency, not locks),
and intentionally omits features that don't scale horizontally — **no foreign
keys, no triggers, no stored procedures, a per-transaction row limit, and a
1 MiB per-value limit**. This manual calls out each of these where it matters and
shows what the tool does about it, so you don't have to learn DSQL's rules the
hard way.
