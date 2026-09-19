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
  <img src="../../images/architecture-cdc-pipeline.png" alt="CDC pipeline: source change log (MySQL binlog / PostgreSQL WAL via a logical replication slot) → Debezium source connector → Amazon MSK (per-table topics keyed by PK + DLQ) → custom DSQL sink connector → Aurora DSQL" width="900">
</p>

- **The Debezium source connector (MySQL or PostgreSQL)** reads the source's
  change log — the binary log for MySQL, or the write-ahead log (WAL) through a
  logical replication slot for PostgreSQL — and emits change events. The source
  stays read-only, with the one sanctioned exception that for a PostgreSQL source
  the tool creates (and at teardown drops) its logical slot and a publication
  scoped to exactly the migrated tables.
- **Amazon MSK (Kafka)** is the durable backbone: **one topic per table**, keyed
  by primary key (so all changes for a row stay ordered on one partition), plus a
  dead-letter (DLQ) topic, a Debezium **schema-history** topic (what `recovery`
  rebuilds — **MySQL source only**; the PostgreSQL/pgoutput connector has no
  schema-history topic), and a **heartbeat** topic that keeps the committed source
  position advancing during idle windows — the binlog offset for MySQL, and for
  PostgreSQL the slot's confirmed LSN (which also stops WAL accumulating) — so a
  restart can still resume gaplessly.
- **The custom DSQL sink connector** — a Java Kafka Connect plugin this project
  owns — applies the changes to DSQL. Both connectors run on **managed MSK
  Connect**; the tool runs **no sink compute of its own**, it is the control
  plane (it builds the configs, establishes the resume point — for MySQL it seeds
  the Kafka start offset, for PostgreSQL it creates the logical replication slot
  and publication — and monitors).

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

### Detecting and handling source schema drift

You don't have to notice a source `ALTER` yourself. When dead-lettered records
indicate the source shape changed, the tool **classifies the drift** by SQLSTATE
(added column / dropped column / type change) and shows a **"Source schema change
detected"** banner on the CDC step. For the common case — a source **`ADD COLUMN`** —
it offers a **"Fix target schema…"** action that shows the exact `ALTER TABLE … ADD
COLUMN` statements and applies them (one DDL per transaction); CDC then resumes and
you use the per-table **Reload** to backfill the rows that were set aside while the
column was missing. Drop-column / type-change drifts are surfaced for you to resolve
by hand (the tool never drops target data automatically).

---

## 4.3 The gapless Full Load → CDC handoff

This is the bit designed to ensure **no missed changes and no duplicates** between
the bulk load and the stream.

1. Full Load captured a **watermark** at the snapshot point — for MySQL the binlog
   file:position (plus GTID when available); for PostgreSQL the WAL LSN returned
   when the logical replication slot is created, recorded together with the slot
   and publication names
   ([Chapter 3 §3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)).
2. When you start CDC, the tool establishes the resume point at exactly that
   watermark. **For a MySQL source** it **seeds the connector's start offset** (an
   in-VPC Lambda writes the offset record before the source connector starts).
   **For a PostgreSQL source** there is no offset-seeder Lambda; instead, at the
   Full Load consistency point the tool creates the logical replication slot (and a
   publication scoped to exactly the migrated tables), and the slot's returned
   consistent WAL LSN *is* the watermark — CDC resumes directly from that
   pre-created slot, so nothing is written to Kafka's connect-offsets. Either way
   Debezium begins streaming the **first change after the snapshot** — not from
   "now," and not by re-reading the data.
3. **For a MySQL source**, the source connector runs with
   **`snapshot.mode=recovery`**: because an offset is already seeded, Debezium
   rebuilds its internal **schema history** from the **current source tables** (so
   it can decode binlog events) **without re-reading any row data**, then resumes
   from the seeded offset. **For a PostgreSQL source**, the Debezium pgoutput
   connector has no schema-history topic: on the gapless path it runs with
   **`snapshot.mode=never`** (the logical slot already holds the start LSN and the
   rows are loaded), while a stand-alone / manual start uses **`initial`** (snapshot
   then stream). Debezium PostgreSQL can only resume from the slot's own
   position — it cannot seek an arbitrary WAL LSN.

The result: every change between the snapshot and "now" is applied exactly once.
Because the sink's applies are **idempotent** (keyed by PK), even an overlap or a
retry can't create duplicates.

> **MySQL: the binlog at the watermark must still exist when CDC starts.** This
> handoff only works if the source hasn't purged the binary log at the watermark
> position yet. RDS/Aurora purge binlogs aggressively by default (Aurora MySQL
> keeps them 24 h), and deploying the CDC stack takes ~15–20 min, so **raise binlog
> retention before you start** — see [§1.1](01-setup.md#11-prerequisites). If the
> segment is already gone, CDC can't resume gaplessly and you'd re-run Full Load to
> capture a fresh watermark.
>
> **PostgreSQL: the logical replication slot pins the WAL for you.** From the
> moment the slot is created it holds the required WAL, so nothing is purged out
> from under you — the prerequisite is `wal_level=logical` (RDS/Aurora
> `rds.logical_replication`), not binlog retention. But the slot must stay healthy:
> if it falls too far behind and Postgres reports `wal_status='lost'` (slot
> invalidated), gapless resume is broken and you must re-run Full Load to capture a
> fresh watermark.

> **Why `recovery` and not a plain schema-only start? (MySQL source)** With a
> seeded offset present, Debezium takes its "resume" path and expects an existing
> schema-history topic. `recovery` is the mode that rebuilds that history from the
> live database without re-snapshotting rows — exactly the "I already loaded the
> data myself, just resume from this offset" situation. **This does not apply to a
> PostgreSQL source:** PG has no schema-history topic, so Debezium simply resumes
> from the pre-created logical slot with `snapshot.mode=never` (or does an
> `initial` snapshot for a stand-alone start).

---

## 4.4 How the sink handles DSQL's constraints in the data path

DSQL is distributed and PostgreSQL-compatible, so the sink can't behave like a
classic MySQL/JDBC writer. Here is what it does for each constraint:

| DSQL constraint | How the custom sink handles it |
|---|---|
| **IAM-token auth (no password)** | Generates a short-lived IAM token (admin or standard) and uses it as the JDBC password over TLS; **refreshes before expiry** (15-min token, 2-min refresh margin) so long-running CDC never stalls on an expired token. |
| **Optimistic concurrency (no locks)** | On `SQLSTATE 40001` it retries at the **statement level** with exponential backoff + jitter (up to 10 attempts), not per whole batch. This is the key throughput difference under contention. |
| **≤ 3000 rows per transaction** | Applies in chunks of ≤ 3000 rows (default batch 3000), one `commit()` per chunk. |
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
| **Inserts** | Cumulative CDC inserts applied to this table since Full Load — a **non-negative** per-table running count reported live by the sink (scan-free). |
| **Updates** | Cumulative CDC updates applied to this table since Full Load — a **non-negative** per-table running count reported live by the sink (scan-free). |
| **Deletes** | Cumulative CDC deletes applied to this table since Full Load — a **non-negative** per-table running count reported live by the sink (scan-free). |
| **Quarantined** | Per-table count of change events set aside to the **DLQ** — permanently-rejected rows (bad type, oversized > 1 MiB value, constraint / schema-drift), reported live by the sink. |
| **Source rows (est.)** | Scan-free catalog **estimate** (`information_schema.tables` for MySQL; `pg_class.reltuples` for PostgreSQL). **Target rows** — exact DSQL count. |
| **Stream lag** | How far the target is behind the source **in time** (see below). |
| **Consistency** | A colored badge: green *consistent* = counts match · *replicating…* = catching up · red *rows missing* = the newest change landed but rows went missing mid-stream · red *data quarantined* = the DLQ has un-applied events. |

### Stream lag — a real time measure, not a row count

**Stream lag** is the end-to-end replication delay: for each change, the sink
records **apply time − the source commit time** (Debezium `source.ts_ms`) and
emits it as a per-table `ReplicationLagMs` CloudWatch metric; the monitor shows
the **worst (maximum) lag** observed over the recent window, not the latest single
value.

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

### Pipeline health and change flow

Above the per-table table, a **pipeline-health card** shows each connector's state
and a **"change flow"** status line — *Streaming*, *No changes flowing — idle*, or
**"Sink stalled — changes are NOT reaching DSQL"** — with two rec/s gauges (source
poll vs sink send), so you can tell an idle stream from a stuck one at a glance. A
**live stream-lag chart** plots the worst end-to-end lag over time, distinguishing
"caught up" from a confirmed sink stall.

### Inspecting the DLQ

An interactive **DLQ inspector** lists dead-lettered records — **Time / Table /
SQLSTATE / Reason** — paged and filterable, with a per-table breakdown and a depth
badge, so you can triage poison rows without leaving the UI. **"Download CDC error
log"** exports them as NDJSON. (The reason carries the SQL template, never row
values — see §4.5.)

---

**Next:** [5. Validation →](05-validation.md)
