# 4. How CDC works, and how DSQL constraints are handled

_Language: **English** | [한국어](../ko/04-cdc-and-dsql-constraints.md) | [日本語](../ja/04-cdc-and-dsql-constraints.md)_

> **Prev:** [3. Full Load](03-full-load.md)

**CDC (Change Data Capture)** is the **optional** streaming pipeline for a
near-zero-downtime cut-over. After Full Load copies the existing rows, CDC keeps
DSQL continuously up to date with every new insert/update/delete on the source —
so you can cut over with minimal downtime instead of taking a long outage.

You only need CDC for **large-scale or continuous** migrations. For a one-shot
cut-over where a short freeze is acceptable, Full Load alone is enough.

---

## 4.1 The pipeline

<p align="center">
  <img src="../../images/architecture-cdc-pipeline.png" alt="CDC pipeline: source MySQL binlog → Debezium source connector → Amazon MSK (per-table topics keyed by PK + DLQ) → custom DSQL sink connector → Aurora DSQL" width="900">
</p>

- **Debezium MySQL source connector** reads the source's binary log (read-only)
  and emits change events.
- **Amazon MSK (Kafka)** is the durable backbone: **one topic per table**, keyed
  by primary key (so all changes for a row stay ordered on one partition), plus a
  dead-letter (DLQ) topic.
- **The custom DSQL sink connector** — a Java Kafka Connect plugin this project
  owns — applies the changes to DSQL. Both connectors run on **managed MSK
  Connect**; the tool runs **no sink compute of its own**, it is the control
  plane (it builds the configs, seeds the start offset, and monitors).

**Why a *custom* sink and not a stock JDBC sink?**

A standard JDBC sink retries optimistic-concurrency conflicts (`SQLSTATE 40001`)
**per batch**, which collapses throughput under high-contention large-scale CDC.
The custom sink retries at the **statement level** and handles DSQL's short-lived
IAM tokens, ≤3000-row batches, and reconnects (details in §4.4).

---

## 4.2 CDC replicates *data*, not *schema* — important

CDC replicates **row-level data changes** (insert / update / delete). It does
**not** replicate SQL statements or **DDL**. Concretely:

- Debezium runs with `include.schema.changes=false`, and the sink applies only
  DML.
- Source **DDL** (`ALTER TABLE`, `CREATE`, `DROP`, …) is **not** propagated to
  DSQL.

The DSQL target schema is fixed when the **Schema Conversion** step runs. **If you
change the source schema while CDC is running**, apply the equivalent DDL to DSQL
yourself (via Schema Conversion) **first**. Until you do, rows that no longer
match the target shape (e.g. referencing a new column) are isolated to the **DLQ**
rather than lost — visible, not silent.

---

## 4.3 The gapless Full Load → CDC handoff

This is the bit designed to ensure **no missed changes and no duplicates** between
the bulk load and the stream.

1. Full Load captured a **watermark** (binlog position + GTID) at the snapshot
   point ([Chapter 3 §3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)).
2. When you start CDC, the tool **seeds the connector's start offset** to exactly
   that watermark (an in-VPC Lambda writes the offset record before the source
   connector starts). So Debezium begins streaming the **first change after the
   snapshot** — not from "now," and not by re-reading the data.
3. The source connector runs with **`snapshot.mode=recovery`**: because an offset
   is already seeded, Debezium rebuilds its internal **schema history** from the
   **current source tables** (so it can decode binlog events) **without re-reading
   any row data**, then resumes from the seeded offset.

The result: every change between the snapshot and "now" is applied exactly once.
Because the sink's applies are **idempotent** (keyed by PK), even an overlap or a
retry can't create duplicates.

> **The binlog at the watermark must still exist when CDC starts.** This handoff
> only works if the source hasn't purged the binary log at the watermark position
> yet. RDS/Aurora purge binlogs aggressively by default (Aurora MySQL keeps them
> 24 h), and deploying the CDC stack takes ~15–20 min, so **raise binlog retention
> before you start** — see [§1.1](01-setup.md#11-prerequisites). If the segment is
> already gone, CDC can't resume gaplessly and you'd re-run Full Load to capture a
> fresh watermark.

> **Why `recovery` and not a plain schema-only start?** With a seeded offset
> present, Debezium takes its "resume" path and expects an existing schema-history
> topic. `recovery` is the mode that rebuilds that history from the live database
> without re-snapshotting rows — exactly the "I already loaded the data myself,
> just resume from this offset" situation.

---

## 4.4 How the sink handles DSQL's constraints in the data path

DSQL is distributed and PostgreSQL-compatible, so the sink can't behave like a
classic MySQL/JDBC writer. Here is what it does for each constraint:

| DSQL constraint | How the custom sink handles it |
|---|---|
| **IAM-token auth (no password)** | Generates a short-lived IAM token (admin or standard) and uses it as the JDBC password over TLS; **refreshes before expiry** (15-min token, 2-min refresh margin) so long-running CDC never stalls on an expired token. |
| **Optimistic concurrency (no locks)** | On `SQLSTATE 40001` it retries at the **statement level** with exponential backoff + jitter (up to 10 attempts), not per whole batch. This is the key throughput difference under contention. |
| **≤ 3000 rows per transaction** | Applies in chunks of ≤ 3000 rows (default batch 1000), one `commit()` per chunk. |
| **No UPDATE-by-statement / replays** | Every change is an **idempotent upsert or delete keyed by PK**: `INSERT ... ON CONFLICT (pk) DO UPDATE` for inserts/updates, `DELETE ... WHERE pk = ?` for deletes (and Kafka tombstones). Re-applying the same event is safe. |
| **Connections drop (idle close / token expiry / worker recycle)** | Detects a dead/half-open connection, reconnects with a fresh token, and **replays the same offsets** (safe because applies are idempotent) rather than dropping records. Connectivity errors are treated as transient and retried, not mistaken for poison rows. |

---

## 4.5 The 1 MiB per-value limit, and the DLQ

DSQL rejects a **single value larger than ~1 MiB** (a `TEXT`/`bytea` value). The
pipeline handles oversized values in **three bands**:

| Value size | What happens |
|---|---|
| **≤ 1 MiB** | Applied normally. |
| **1 MiB – 8 MiB** | The sink measures each value **before writing** and **quarantines** the oversized one to the **DLQ** (it can never be applied), while the rest of the record's table keeps flowing. To let such a record even traverse Kafka to be dead-lettered, the per-table topic and client limits are raised (default 4 MiB, max 8 MiB). |
| **> 8 MiB** | Cannot enter Kafka at all. These must be **excluded at capture**: Debezium `column.exclude.list` drops the oversized LOB column (driven by the Evaluation `OVERSIZED_LOB` flag) so it never reaches the pipeline. |

### What gets dead-lettered

Beyond oversized values, the DLQ isolates any record DSQL **permanently** rejects
— a type mismatch, a constraint violation, a missing target column (e.g. after an
un-propagated source `ALTER`). Transient failures (OCC `40001`, connection drops)
are **retried**, not dead-lettered.

### Where the DLQ surfaces — CloudWatch, not a Kafka topic you read

The sink logs each quarantined record to its **CloudWatch** connector log group,
and the tool's monitoring parses those lines into the UI (per-table
"Quarantined" counts and a single downloadable error log). The logged reason
includes the **SQL template** (column names + `?` placeholders) — **never row
values or credentials** — so you can see the exact statement shape DSQL rejected
without exposing data. A record that can be neither applied nor dead-lettered
makes the task **fail loudly** rather than silently skip — visible over silent
loss.

### What CDC does with later changes to a row that Full Load quarantined

If Full Load quarantined a row (its oversized value could not be written), the row
is **absent from the target** while still present on the source. What happens when
that same row changes later depends on the operation:

| Source operation on the missing row | What CDC does | Result |
|---|---|---|
| `DELETE` | `DELETE … WHERE pk = ?` matches **0 rows**. The sink does not treat a 0-row delete as an error, so it is applied and committed silently. | **Correct.** The intended end state — "the row is not on the target" — already holds. Treating this as a failure would break idempotency: a replayed or retried delete must stay safe. |
| `UPDATE` that shrinks the value below 1 MiB | Debezium sends the **full after-image** and the sink writes `INSERT … ON CONFLICT (pk) DO UPDATE`. With no existing row, `ON CONFLICT` never fires and the row is **inserted**. | **The gap heals itself.** No action needed. |
| `UPDATE` where the value is still oversized | The sink measures values **before** writing, so the record is quarantined to the **DLQ** for the same reason. | Gap persists, and it is **visible** (DLQ depth / "Quarantined" in the monitor). |
| `INSERT` of a *different* row | Unaffected. | Normal. |

Two consequences worth planning around:

- **A quarantine gap is not self-announcing after Full Load.** If the source never
  touches that row again, the gap simply persists — CDC has nothing to replicate.
  It is **Validation (Step 4)** that reports it, which is why the Full Load
  completeness verdict points you there.
- **A 0-row delete is indistinguishable from a normal replay.** The sink cannot tell
  "this delete found nothing because the row was quarantined" from "this delete was
  simply reprocessed". That is a deliberate trade-off in favour of idempotency;
  gap accounting belongs to Validation, not to the delete path.

The practical guidance is unchanged: fix the oversized source value and **Reload**
that table before cutting over, or exclude the oversized LOB column at capture
(§4.5) so it never enters the pipeline.

---

## 4.6 Monitoring CDC progress (the migration monitor)

While CDC runs, the Data Migration screen shows a live per-table monitor. It
refreshes on its own (no manual reload) and is **scan-free** on the source — it
never runs a `COUNT(*)` against your production database. The columns:

| Column | What it means |
|---|---|
| **Full Load rows** | Rows the one-shot snapshot loaded into the table. |
| **Net rows since Full Load** | The **net** rows CDC has applied since Full Load — *not* a count of CDC events: inserts add, deletes subtract, updates don't change it. Reported live by the sink (scan-free). It is **negative** when the stream net-deleted rows (more deletes than inserts) — expected, not an error. |
| **Source rows** | Scan-free `information_schema` **estimate**. **Target rows** — exact DSQL count. |
| **Stream lag** | How far the target is behind the source **in time** (see below). |
| **Consistency** | A colored badge: green *consistent* = counts match · *replicating…* = catching up · red *rows missing* = the newest change landed but rows went missing mid-stream · red *data quarantined* = the DLQ has un-applied events. |

### Stream lag — a real time measure, not a row count

**Stream lag** is the end-to-end replication delay: for each change, the sink
records **apply time − the source commit time** (Debezium `source.ts_ms`) and
emits it as a per-table `ReplicationLagMs` CloudWatch metric; the monitor shows
the most recent value.

- Reads as a **duration**: `caught up` (sub-second / the stream has drained),
  `8.5s behind`, `2m 10s behind`, `1h 4m behind`.
- It is a true **time** lag, so it is accurate for **any** primary-key type and
  reflects update/delete lag too — not just the newest insert.
- **Fallback:** when the time metric isn't available yet (an older sink plugin, or
  the metric hasn't emitted a datapoint), the monitor falls back to a `MAX(pk)`
  **leading-edge** check and shows `N behind (PK)` (how many PK units the target's
  newest key trails the source's) or `caught up`. This fallback only works for a
  single-column integer PK; the time-based value above is the preferred, general
  measure.

Lag is emitted strictly **best-effort** and never affects replication. Use "Stream
lag → caught up" as the signal that it's safe to proceed to
[cut-over](10-conclusion.md).

---

**Next:** [5. Validation →](05-validation.md)
