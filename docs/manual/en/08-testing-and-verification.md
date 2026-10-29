# 8. Testing and verification

_Language: **English** | [한국어](../ko/08-testing-and-verification.md) | [日本語](../ja/08-testing-and-verification.md)_

> **Prev:** [7. Performance and tuning](07-performance-and-tuning.md)

The most unsettling question in any migration is: **"Will my data be silently lost or
stored wrongly on the way, and how will I know the target really matches the source when
it's done?"**

This chapter answers both. Aurora DSQL is a different engine from MySQL, so there are
places where a naive copy would corrupt data silently. **§8.1** lists what the tool does
for *your* data in those situations and where *you* check it in the UI. **§8.2** shows an
end-to-end run on real AWS with deliberately hostile data — and the **Validation** step at
the end of your own migration gives you the same evidence for *your* data.

> These behaviors are not guesses: each situation is reproduced deterministically by
> automated tests that need no AWS, and confirmed by live runs on real infrastructure with
> data planted to fail (summarized in §8.2). Both source engines were validated end to end
> this way — a MySQL source (RDS MySQL + Aurora DSQL + MSK) and a PostgreSQL source
> (RDS/Aurora PostgreSQL → MSK → Aurora DSQL), each across Schema Conversion, Full Load,
> CDC and cut over.

One principle is worth remembering: **a loud failure beats silent corruption.** The tool
stops with a visible, fixable error rather than quietly distorting or dropping data, and
it never reports an incomplete load as a success.

---

## 8.1 What can go wrong with your data — and what the tool does for you

Below are the typical situations where something harmless on the source becomes risky on
Aurora DSQL. Each row shows what the tool does when *your* data has it, and where you
check the result. A PostgreSQL source's type surface is nearly the same as Aurora DSQL's
(both speak PostgreSQL 16), so the type-mismatch rows do not apply to it, but the
distributed, serverless, IAM-auth and OCC rows do.

| If your data has… | Why a naive copy is risky (the DSQL difference) | What the tool does for you | Where you check |
|---|---|---|---|
| **Very large tables** (hundreds of millions of rows) | DSQL limits a transaction to 3,000 rows, 10 MiB, 5 minutes and one DDL; pushing a table in at once fails or is cut off | **Streams** in primary-key order and loads in batches that fit the limits, so memory and transactions stay safe at any table size; a large table is written in several primary-key ranges, into tables whose secondary indexes were created while empty. Validation counts and checksums in bounded pages too, so a large table does not hit the 5-minute limit. | Full Load progress, Validation **row counts** |
| **Values over 1 MiB** (MySQL LOB/TEXT, PostgreSQL `text`/`bytea`/`json`) | DSQL cannot store a value over 1 MiB | Only that **row is quarantined** (primary key and reason recorded); the rest of the table keeps loading. During CDC the same value goes to the DLQ, so one value never stops a whole table. | Full Load **error log**, CDC **DLQ**, Validation shortfall |
| **Values that convert ambiguously** (MySQL only — for example `2` in a `TINYINT(1)`, a `TIME` outside 24 hours) | Squashing `2` to `true`, or cutting an out-of-range `TIME` to a day, would corrupt data silently | The tool does not flatten them: it **stops that table's load loudly**, names the column and value and says how to fix it. A PostgreSQL source has a native `boolean`, so this risk does not arise. | Full Load **error log** (table-level failure) |
| **Objects and types DSQL does not support** (MySQL: triggers, procedures, more than 255 columns, FULLTEXT/SPATIAL indexes — not created, flagged `MANUAL`; PostgreSQL: composite types, domains, pgvector, …) | A plain copy would fail one object at a time during the load | **Evaluation** classifies everything up front (AUTO / MANUAL / UNSUPPORTED) with the reason, the recommended action and the effort, so you decide before loading. Types with a value-preserving stand-in (arrays → `jsonb`, range and network types → `text`, `money` → `numeric(23,4)`, enum → `text` with a `CHECK`) are converted **with a warning** naming the type change your application must follow; types with none are `UNSUPPORTED`. | **Evaluation** report |
| **A schema with many source-specific types** (all MySQL integers, `DECIMAL`, `BIT`, date/time, `ENUM`/`SET`/`JSON`, LOBs) | If Full Load (bulk) and CDC (streaming) stored the same value **differently**, they would disagree later | One conversion rule makes both paths store every value in the **same form**. | Validation **checksum** (compares values) |
| **A write-heavy workload** | DSQL has no locks: a conflict (`40001`) reaches the loser at commit (or while a statement runs) and that transaction must re-run | The tool rolls back the failed attempt and **retries automatically** with backoff. If the retry budget runs out it **records a failure** instead of dropping anything silently. | Job status / error summary |
| **A temporary Aurora DSQL condition** (`XX000` "server unavailable") | DSQL cannot answer for a moment — if a commit was in flight, whether it landed is unclear | The whole transaction is **retried automatically** on the same budget as `40001`. Re-running is idempotent by primary key, so a commit that had landed leaves the same rows (on a table with a secondary `UNIQUE` index the sink can meet an exception: [§4.4](04-cdc-and-dsql-constraints.md#44-how-the-sink-handles-dsqls-constraints-in-the-data-path)). Past the budget a Full Load batch is **recorded as failed** (retryable) and CDC has the batch redelivered — no row is quarantined or sent to the DLQ for it. | Job status / error summary |
| **Unique conflicts across partitions** (`23505`) | A primary-key `UPDATE` can reach the sink as an insert before the delete of the old row, which sits on another partition, and a unique index besides the primary key rejects that insert | The sink **holds** that partition until the delete has applied, then applies the change with no dead letter. A change that a later change of the same row replaces is skipped instead. One still stuck after 5 minutes is dead-lettered with its real reason — never lost silently ([§4.4](04-cdc-and-dsql-constraints.md#44-how-the-sink-handles-dsqls-constraints-in-the-data-path), [§6.2](06-limitations.md#62-migration-process-limits)). | CDC step **DLQ** card / sink connector log |
| **A long load or CDC stream** | DSQL connections use short-lived IAM tokens and a 60-minute connection limit | Dead or half-open connections are dropped and the tool **reconnects with a fresh token** and carries on — nothing for you to do. | Automatic (progress keeps going) |
| **The hand-off from bulk load to CDC** (near-zero-downtime switch) | A gap between the bulk load and CDC **loses rows**; an overlap **duplicates** them | CDC starts from the **watermark** the bulk load captured (MySQL: binlog position or GTID; PostgreSQL: a logical replication slot created at the snapshot point), and every change applies **idempotently by primary key**, so an overlap creates no duplicates. | Validation **reconciliation** (0 missing / 0 extra) |
| **Many foreign keys** | With keys enforced during the load and CDC, rows that arrive out of order (a child before its parent) fail, and cascades the source applied never reach CDC and leave orphans | Foreign keys are **preserved** (a PostgreSQL 18 key declared `NOT ENFORCED` or with `PERIOD` is not created) and re-created after the data is in — for CDC, at cut over — as `ADD CONSTRAINT … NOT VALID`; Aurora DSQL then validates the existing rows. The **orphan check** is the early warning for child rows with no parent, and your integrity safety net if you chose to strip the keys. | Validation **orphan check** |
| **A live source that keeps changing** | If the source advances during the migration, row counts differ — a bug or normal? | A count difference after the snapshot is attributed to **new source activity**, not a bug. **MySQL:** drift is **detected and reported** from the watermark's binlog/GTID position. **PostgreSQL:** Validation does not recompute drift (`drifted` = could not be determined), so watch CDC converge instead. | **drifted** flag in the Validation report |
| **Stopping and restarting midway** | A naive loader starts over or creates duplicates | Writes are safe to re-apply (`INSERT … ON CONFLICT`). On resume, finished tables are left alone and only unfinished ones are loaded again (**Retry unfinished tables**). | Full Load progress after resume |

> The "Where you check" column is always a screen you look at in the UI. The
> **Validation** step in particular decides with exact row counts, checksums and a full
> primary-key reconciliation, so you decide on cut over from evidence, not hope (see
> [Chapter 5](05-validation.md)).

---

## 8.2 Real evidence — an end-to-end run on deliberately tricky data

To stress the situations above **all at once**, we ran the full flow on real AWS — a
**MySQL source** (RDS MySQL + Aurora DSQL + MSK) — with a schema built for it:

- a parent → child → lob **foreign-key chain** (forces FK preservation and apply, required
  primary keys and the orphan check);
- **the widest range of types** — all integers/unsigned, `DECIMAL` up to MySQL's maximum
  (65,30), `FLOAT`/`DOUBLE`, `BIT`, collations, the full DATE/TIME family,
  `ENUM`/`SET`/`JSON` and the full LOB family (forces type mismatches and unsupported
  types);
- **data planted to fail** — a ~1.5 MiB LOB value (forces the 1 MiB quarantine) and a
  `TINYINT(1)` = `2` in a quarantined table (forces the loud table failure).

> **PostgreSQL source.** The same end-to-end path was validated separately on live
> infrastructure (RDS/Aurora PostgreSQL → MSK → Aurora DSQL): Schema Conversion, Full
> Load, CDC and cut over, foreign keys preserved. A PostgreSQL source passes PG-16 → DSQL
> almost unchanged, so the type-mismatch and `TINYINT(1)` situations above do not apply to
> it, but the distributed, serverless, IAM-auth, OCC and gapless Full Load → CDC hand-off
> situations all do.

The run exercised the hardest scenario for real — the **gapless hand-off**: Full Load →
live workload → CDC from the watermark → convergence → per-primary-key reconciliation.
That is why a live run is worth doing: the first run exposed a real CDC data gap (a
continuous stretch between the watermark and the CDC start was missing and absent from the
DLQ), and we found the cause and **fixed it**. The final run below proves the fix with
real data.

### Verification results

After the fix, the full run was repeated and **every clean table reconciled exactly — with
zero unexplained differences**:

| Table | Source rows | Target rows | Missing on target | Extra on target | Verdict |
|---|---|---|---|---|---|
| `typetest_parent` | 3,906 | 3,906 | 0 | 0 | **MATCH** |
| `typetest_child` | 3,785 | 3,785 | 0 | 0 | **MATCH** |
| `typetest_lob` | 2,309 | 2,309 | 0 | 0 | **MATCH** |

What this means for you:

- **Zero data loss, zero duplication.** Across Full Load **and** live CDC, the
  per-primary-key reconciliation found **0 missing / 0 extra** rows in every table — every
  source row arrived exactly once and every delete was applied. The continuous gaps of the
  first run (50–70 rows per CDC round) are now **zero**.
- **Data planted to fail was caught, not lost.** The ~1.5 MiB LOB row was **quarantined**
  by design (absent from the target, recorded by primary key) and the out-of-range
  `TINYINT(1)` row caused a **loud table failure** — exactly the safe behavior. Nothing
  was silently dropped or distorted.
- **Values matched, not just counts.** Validation's checksum and reconciliation compared
  the actual values (across the whole range of types), so "match" here means the values
  are equal, not merely the counts.

### What §8.1 promised, confirmed live separately

Besides the hand-off run, the **resume** and **CDC edge behaviors** promised in §8.1 were
stressed separately on real infrastructure:

- **Large-scale resume** — a load of about 1.8 million rows was stopped on purpose partway
  (around 10,000 rows in) and resumed: only the **unfinished tables** ran again, the whole
  load completed, and the per-primary-key reconciliation converged to **0 missing / 0
  extra** in every table (including a 1.07-million-row table). An interruption loses and
  duplicates nothing.
- **CDC edge operations** — primary-key changes, repeated insert-delete-reinsert, insert +
  delete within one transaction, a burst of consecutive UPDATEs and a large multi-row
  INSERT in a single commit all landed **exactly as on the source**. A row from a **column
  added** on the source during CDC (a schema change, which CDC does not propagate because
  it replicates data, not DDL) was **isolated to the DLQ** instead of silently landing
  wrong, as §8.1 promises.

**Bottom line:** on a schema deliberately built to stress every DSQL difference —
including data planted to fail — the migration produced **100% match on every clean table
and zero unexplained differences**, and the data that *should* fail was caught and
reported, not lost. And because you run **Validation** yourself at the end of your
migration, you get the same evidence — exact row counts, checksums and per-primary-key
reconciliation — for *your* data before cut over. It is not a request to trust the result;
it is something you can reproduce.

> The same per-primary-key reconciliation is available from a shell:
> `scripts/cdc_consistency_check.py` (lists the exact PKs **missing** or **extra** on the
> target) and `scripts/compare_rows.py -t <schema.table> --watch N` (row counts / live
> convergence) — see [`scripts/README.md`](../../../scripts/README.md). The authoritative
> go/no-go is the built-in **Validation** step.

---

**Next:** [9. The Query Converter and the AI DBA →](09-query-validation.md)
