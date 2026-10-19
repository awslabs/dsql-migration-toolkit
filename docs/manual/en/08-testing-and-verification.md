# 8. Testing — the DSQL-driven scenarios

_Language: **English** | [한국어](../ko/08-testing-and-verification.md) | [日本語](../ja/08-testing-and-verification.md)_

> **Prev:** [7. Performance and tuning](07-performance-and-tuning.md)

This chapter is organized around a single idea: **each Aurora DSQL characteristic
forces a specific migration scenario that must be tested.** A like-for-like
migration (MySQL→MySQL or PostgreSQL→PostgreSQL) never has to prove these — they
exist precisely because DSQL is a different engine (PostgreSQL-wire, distributed,
serverless, IAM-auth, optimistic concurrency). A PostgreSQL→DSQL migration is
near-homogeneous on the *type* surface (both are PostgreSQL-16 wire), so the
type-heterogeneity scenario in particular does not apply to a PostgreSQL source —
but the distributed, serverless, IAM-auth, and OCC scenarios still do. For each
DSQL trait below, we list the scenario it forces and how the tool exercises it.

> Two test layers back every scenario: an **offline suite** (~3,100 Python + 72
> Java tests; no AWS — seams injected) that proves the behavior deterministically,
> and a **live end-to-end run** on real infrastructure with deliberately failing
> rows (summarized in §8.2). Both source engines were validated end to end this
> way — a MySQL source (RDS MySQL + Aurora DSQL + MSK) and a PostgreSQL source
> (RDS/Aurora PostgreSQL → MSK → DSQL), each across Schema Conversion + Full Load +
> CDC + cut over. A small `integration`-marked tier (`tests/test_integration_e2e.py`)
> also exercises a local MySQL + a PostgreSQL-16 container, but only under
> `RUN_INTEGRATION_TESTS=1`; it self-skips otherwise, so the default `pytest` run
> stays fully offline.

---

## 8.1 Scenarios forced by DSQL characteristics

### Transaction shape — DSQL caps every write transaction

DSQL allows **≤ 3,000 rows**, **≤ 10 MiB of modified data**, **≤ 5 minutes**, and
**one DDL** per transaction. A loader that ignores this either fails outright or
silently truncates.

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| ≤ 3,000 rows / txn | A table larger than one transaction can hold; a batch exactly at the cap; a request above the cap | Batch row-count is capped; cap is accepted, over-cap is rejected (`test_batch_size_at_the_hard_cap_is_accepted`, `…_above_the_hard_cap_is_rejected`) |
| ≤ 10 MiB / txn (8 MiB safety budget) | Wide rows whose **bytes** exceed the limit before the row count does; a single row larger than the budget | Byte-aware split before row-count split; an oversized single row is yielded alone (`test_iter_batches_splits_on_byte_budget_before_row_count`, `…_single_oversized_row_yields_alone`) |
| One DDL / txn | Creating a table's secondary indexes | Each index DDL runs in its own transaction (`test_indexes_created_after_all_data_each_its_own_statement`) |

### Optimistic concurrency — DSQL has no locks; conflicts surface as 40001 (at commit, or while a statement runs)

DSQL detects write conflicts at commit, and a schema conflict (`OC001`) can also come
while a statement runs; both return `SQLSTATE 40001`. The losing transaction must
**re-run**, and under contention this happens often.

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| 40001 conflict on a write | A batch hits a serialization conflict, then succeeds on retry | The whole batch — one transaction — is re-run with backoff/jitter and succeeds (`test_occ_conflict_on_batch_is_retried_then_succeeds`); the CDC sink re-runs its chunk's transaction the same way (`OccRetryTest.java`) |
| 40001 while a statement runs (`OC001`) | The sink's chunk, or one row, gets a 40001 before COMMIT, which leaves its transaction aborted | The failed attempt is rolled back and the whole transaction re-runs: a one-row chunk is applied, not dead-lettered, and a multi-row chunk commits on the batched path (`DsqlSinkOccRollbackTest.java`, against a double with PostgreSQL's aborted-transaction semantics) |
| `XX000` "server unavailable" (a temporary Aurora DSQL condition) | A Full Load batch, a sink chunk or row, its parameter describe or its COMMIT gets `XX000` once, or until the budget runs out | Retried like a `40001`, from the same budget: a Full Load batch is never split or quarantined, and one that outlasts the budget fails, retryable (`test_g22_server_unavailable_is_retried_and_never_quarantines`, `test_g22_exhausted_server_unavailable_fails_the_batch_unsplit`); the sink rolls the attempt back and re-runs it, and one that never clears is redelivered, never dead-lettered (`DsqlSinkOccRollbackTest.java`) |
| `XX000` at commit (uncertain outcome) | The commit landed but answered `XX000`, so the retry re-runs work that already committed | Every Full Load mode ends with the same rows (`test_g22_a_commit_that_landed_but_answered_xx000_is_re_run_exactly`), and so does every sink write shape: upsert, delete, move, the spared delete, tombstone (`DsqlSinkRekeyTest.java`), though on a table with a secondary `UNIQUE` index the sink's re-run can meet an intermediate image the landed commit already replaced: on a table that is not re-keyed it is replaced by the row's later change in the batch and skipped, or else held like any unique conflict ([§4.4](04-cdc-and-dsql-constraints.md#44-how-the-sink-handles-dsqls-constraints-in-the-data-path)); a post-load index that a retry finds already there is reported, never counted as created (`test_g22_index_already_there_after_server_unavailable_is_reported_never_counted`) |
| Unique conflict across partitions (`23505`) | A primary-key change's insert reaches the sink before the old row's delete, on another partition | Held on its partition (nothing after it written, its commit capped; after one more batch is read for a later change of the row that replaces it, rewound and paused) until the delete applies, then applied with no dead letter; on a table that is not re-keyed, a primary-key change and its reversal, or a replayed older image whose newer image is in the same or the next batch, is skipped instead, never dead-lettered, and a change of another row between such a skipped change and its replacement that needs a value the skipped change would have freed is dead-lettered after its 5 minutes, never lost silently (`DsqlSinkSupersededSkipTest.java`); a genuine conflict is dead-lettered with its real reason after 5 minutes, counted again after a transient failure of the sink task that holds it, its tries backing off (500 ms doubling to 10 s) and logged while Aurora DSQL does not answer, and counts as its table's lag while it waits (`DsqlSinkHoldBudgetAndLagTest.java`); per-key order, redelivery and rebalance are pinned (`DsqlSinkUniqueHoldTest.java`), and so is a put that ends a hold and then fails, through Kafka Connect's redelivery and a crash, on a model of Connect's own sink loop, and a skipped change that such a redelivery runs again, skipped again or written once its conflict has cleared, never dead-lettered (`DsqlSinkHoldRedeliveryTest.java`) |
| Retry budget exhausted | Conflicts never clear | Recorded as a **failure**, never silently dropped (`test_exhausted_occ_conflict_is_recorded_as_failure`) |
| High concurrency vs. connection quota (10,000/cluster, 100/s) | Many tables × many batches at once | Total in-flight connections stay bounded (`test_parallel_connection_use_is_bounded`); default 4 tables × 8 batches = 32 ≪ quota |

### 1 MiB per-value limit — a single big value can't be stored

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| Value > 1 MiB | A large value (MySQL LOB/TEXT, or PostgreSQL `text`/`bytea`/`json`) over the limit during Full Load **and** during CDC | Per-row **quarantine** (PK + reason recorded, table keeps loading) at Full Load; **DLQ** at the sink, measured before the write (`DsqlSinkTask` oversized guard) |
| Value > 8 MiB (can't traverse Kafka) | An even larger column | Excluded **at capture** via Debezium `column.exclude.list` — you exclude the column with a rule in **Exclude tables & columns** (under the table list on the Data Migration step; its *Oversized-LOB candidates* section lists the same columns Evaluation flags `OVERSIZED_LOB`) |

### IAM-token auth — no password, 15-min tokens, 60-min connections

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| Short-lived token / dropped connection | A long-running load or CDC stream outliving a token or hitting the 60-min connection cap | The pool discards a dead/half-open connection and the retry reconnects with a **fresh** token (`test_transient_connection_error_is_retried_and_recovers`, `test_pool_discards_connection_after_in_use_error`; sink: `DsqlSinkTaskTest`, `DsqlIamTokenProviderTest`) |

### Asynchronous indexes — `CREATE INDEX ASYNC`, built after data

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| Indexes built async, after load | A table with secondary indexes; a load where a data batch fails | Indexes are created only after **all** data lands; if any data batch fails, indexes are **not** created (`test_indexes_created_after_all_data…`, `test_indexes_are_skipped_when_a_data_batch_fails`) |

### Schema differences — FK enforced, PK required, unsupported types/objects

Aurora DSQL enforces foreign keys and omits triggers, stored procedures, and several
types; it creates a table without a primary key, but this tool requires one (keyset
Full Load, key-based CDC).

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| Foreign keys enforced | An FK-laden source | Each source FK is preserved and re-created as a post-load `ALTER TABLE … ADD CONSTRAINT … NOT VALID` (deferred to cut over for a CDC migration), added at once and then validated by Aurora DSQL; Validation's **orphan check** is the early warning for that validation (and the safety net when FKs are stripped) (`test_orphan_records_are_detected_and_fail_the_match`) |
| Primary key required | A table with **no** PK; a **composite** PK | No-PK is blocked up front (`UNSUPPORTED`) and keyset export refuses it; a composite PK loads via the keyset predicate the source's PK index serves as a range — on MySQL an explicit lexicographic disjunction (`(k0 > :last_0) OR (k0 = :last_0 AND k1 > :last_1) OR …`), deliberately **not** the row-value tuple form, which isn't PK-index-friendly on MySQL 5.7; on PostgreSQL that row-value comparison, its index condition, where the disjunction is only a filter. A MySQL `ENUM` / `SET` key column pages in the order MySQL sorts it (member index / bitmask), a key led by an `ENUM` one member at a time, with the members read from the source's catalog when the read starts; a single-precision `FLOAT` key column followed by another key column stops the read of a table larger than one page, and a `TIME` / `BIT` key column (negative `TIME` values included) is bound as the duration text / the integer MySQL compares exactly (`test_exporter.py`, `test_g125_keyset_review.py`, `test_g130_keyset_walk_review.py`, `test_g138_composite_validation.py`, `test_g140_validation_followups.py`, scenario doc) |
| Large composite-key table in Validation | A composite / `ENUM` / `SET` / text-led key past Aurora DSQL's 300 s transaction age | Each side is paged by a key it enforces as unique (Aurora DSQL by its live primary key, so a re-keyed table too) and only the totals are compared — no whole-table statement on Aurora DSQL; a composite source key's checksum pages are held to its `COUNT(*)` in the same snapshot, read before them, and stop as soon as they read more; such a read, or a stalled cursor, falls back to one statement on a MySQL source and errors the table on a PostgreSQL source; a MySQL key with a single-precision `FLOAT` part is checksummed in one statement on the source, one with a `TIME` / `BIT` part is paged with the exact cursor, a one-column key's checksum pages are held to the source's row count too, and a one-column key's page boundary comes back as text and is read in the column's own type, so a `real` key's boundary row is not counted twice (`test_g138_composite_validation.py`, `test_g140_validation_followups.py`, `test_g141_validation_followups.py`) |
| Unsupported types/objects | **MySQL:** > 255 columns, triggers/routines (FULLTEXT/SPATIAL indexes are not emitted and graded `MANUAL`). **PostgreSQL:** unsupported column types — arrays, geometric (point/line/box/…), network (inet/cidr/macaddr), xml, money, bit/bit varying, tsvector/tsquery, range/multirange, enum, composite, pgvector | Where a stand-in keeps the value (an array as `jsonb`, a range or network type as `text`, `money` as `numeric(23,4)`, an enum as `text` with a `CHECK`), Schema Conversion emits it **with a warning** naming the type change the application must follow, and Evaluation grades it `MANUAL` with the remodel target — never silently; a type with no stand-in (composite, domain, pgvector) is `UNSUPPORTED` (`test_converter.py`, assessor tests). A bare PG `numeric`/`decimal` is stored as `numeric(18,6)` (flagged `MANUAL`). Numeric precision is no longer a gate: Aurora DSQL allows 1000, beyond MySQL's 65 and equal to PostgreSQL's own limit |
| TINYINT(1) → boolean, out of range (MySQL source only) | A `TINYINT(1)` holding `2` | A **loud, table-fatal** error — refuses to flatten `2` to `true` (no silent corruption). A PostgreSQL source has a native `boolean` that passes through unchanged, so it has no analogous flattening risk |
| Type heterogeneity (MySQL → PG dialect; PostgreSQL source is near-identity pass-through) | Max type-diversity schema | Full Load (Python) and CDC sink (Java) must encode each value to the **identical** stored form — enforced by a shared **write-contract** parity test (`test_dsql_write_contract.py`). For a MySQL source this includes the MySQL→PostgreSQL dialect translation; for a PostgreSQL source there is no translation (both source and DSQL target are PostgreSQL-16 wire), so both loaders pass the value through verbatim — but the parity requirement still holds for both engines |

### Gapless Full Load → CDC handoff — the hardest correctness property

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| Bulk load then resume streaming with no gap / no duplicate | Load a snapshot, run a live INSERT/UPDATE/DELETE workload, start CDC from the watermark, converge | **MySQL:** the binlog `file:position` (or GTID) watermark is seeded into Kafka `connect-offsets` by the in-VPC offset-seeder Lambda, and Debezium resumes with `snapshot.mode=recovery`. **PostgreSQL:** the WAL LSN watermark is pinned by a pre-created `pgoutput` logical replication slot (plus a publication for exactly the migrated tables) and Debezium resumes from the slot with `snapshot.mode=never` — no offset-seeder Lambda / `connect-offsets` seed (`seed_mode='external'`). Either way, idempotent PK-keyed apply means overlap can't duplicate (`test_cdc_pipeline.py`, `test_cdc_offset_seed.py`, `test_offset_seeder_lambda.py`; PG slot/publication: `test_cdc_pg_slot.py`, `test_cdc_postgres.py`) |
| CDC replicates data, not DDL | A source schema change mid-CDC | A row that no longer matches the target shape goes to the **DLQ** and holds Cut over until the table matches by CHECKSUM; most changes Aurora DSQL accepts silently are reported by the source schema re-check, so validate with CHECKSUM (`test_cdc_dlq.py`) |

### Live source keeps changing — drift must be attributed correctly

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| Source advances during/after migration | Validate while the source position (MySQL binlog/GTID, or PostgreSQL WAL LSN) has moved past the watermark | A count delta after the snapshot is attributed to **new source activity**, not a bug. **MySQL:** drift is detected and reported via the binlog/GTID position on the watermark (`test_drift_since_snapshot_is_reported`, `test_no_drift_when_gtid_unchanged`). **PostgreSQL:** the watermark records a WAL LSN (`Watermark.wal_lsn`), but Validation does not recompute LSN-based drift (reports `drifted = could not be determined`) — watch CDC convergence instead |
| Equal counts but different data | Rows match in number but differ in value | **Checksum** catches it; a count-only "match" is never trusted (`test_deliberate_data_mismatch_with_equal_counts_is_not_a_match`) |

### Resumability — interruptions must not lose or duplicate work

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| Idempotent re-apply | Re-run a batch / resume an interrupted load | `INSERT … ON CONFLICT` never duplicates (`test_reapplying_batches_does_not_duplicate_rows`). An interrupted load keeps its finished tables and leaves the interrupted ones to **Retry unfinished tables**, which reloads each from its start (`test_job_manager_restore_reconciles_interrupted_run`); only a re-read of the same snapshot inside a running load skips the batches it already committed, and converges to the uninterrupted state (`test_resume_skips_done_batches_and_converges_to_uninterrupted_state`) |

**Run the suites:** `\.venv/bin/python -m pytest -q` and
`cd connectors/dsql-sink && mvn -q test`.

**Browser UI E2E (optional):** a Playwright suite under `tests/e2e` drives the
real web UI in a headless browser (the app is launched as a subprocess). It is
opt-in — the default `pytest` run deselects it — and needs the `playwright` dev
dependency plus its Chromium build (once):

```bash
uv sync                                         # installs the playwright dev dep
.venv/bin/python -m playwright install chromium # one-time browser download
.venv/bin/python -m pytest -m e2e               # run the browser E2E suite
```

The default "smoke" E2E needs no external infrastructure (it verifies the UI
renders, navigates, and the Query Converter converts a query). A **connected**
tier drives the real flow against live infrastructure — verify source + target on
the Connect screen, then Query Converter → Test on target (`EXPLAIN ANALYZE` +
DPU) and, when Amazon Bedrock is reachable, Tune with AI DBA → re-test → the
per-code-block copy button:

```bash
RUN_E2E_CONNECTED=1 .venv/bin/python -m pytest -m e2e   # connected tier too
```

It is triple-gated so it never flakes on a missing dependency: `RUN_E2E_CONNECTED=1`
opts in, then it skips cleanly unless the source MySQL + target Aurora DSQL are
actually reachable (from `.env`), and the AI-tuning cases skip additionally unless
Bedrock is reachable. Connection values come from the same `.env` the app prefills
the Connect form from.

---

## 8.2 Putting the scenarios together — the live end-to-end run

The scenarios above were also exercised **together, on real AWS** — for a **MySQL
source** (RDS MySQL + Aurora DSQL + MSK), using a purpose-built schema designed to
hit as many DSQL characteristics as possible in one run:

- A parent → child / lob foreign-key chain (forces the **FK-preservation** + **PK** +
  **orphan-check** scenarios).
- Maximum type diversity — every integer/unsigned variant, `DECIMAL` up to
  MySQL's maximum (65,30), `FLOAT`/`DOUBLE`, `BIT`, collation, the full DATE/TIME family,
  `ENUM`/`SET`/`JSON`, and the full LOB family (forces the **type-heterogeneity**
  and **unsupported-type** scenarios).
- **Deliberately failing rows**: ~1.5 MiB LOB values (forces the **1 MiB
  quarantine/DLQ** scenario) and a `TINYINT(1)` = `2` in an isolated table (forces
  the **loud table-fatal** scenario).

> **PostgreSQL source.** The same end-to-end path was validated separately on live
> infrastructure with a PostgreSQL source (RDS/Aurora PostgreSQL → MSK → Aurora
> DSQL) — Schema Conversion + Full Load + CDC + cut over, including foreign-key
> preservation. Because a PostgreSQL source is a near-identity PG-16→DSQL
> pass-through, the type-heterogeneity and `TINYINT(1)`-flattening scenarios above
> don't apply to it, but the distributed, serverless, IAM-auth, OCC, and gapless
> Full Load → CDC handoff scenarios all do.

The run executed the **gapless handoff** scenario for real — Full Load → live
workload → CDC from the watermark → converge → authoritative per-PK reconciliation.
This is what makes testing on real infrastructure worth it: an early run surfaced a
genuine CDC data-loss bug (a contiguous block lost between the watermark and CDC
start, no DLQ) that the offline suite couldn't — traced to a silently-failing
offset seed and a Debezium schema-history gap, **both fixed**. The point of the
final run was to prove the fixes hold.

### Verification results

After the fixes, the full run was re-executed and **every clean table reconciled
exactly — zero unexplained mismatches**:

| Table | Source rows | Target rows | Missing on target | Extra on target | Verdict |
|---|---|---|---|---|---|
| `typetest_parent` | 3,906 | 3,906 | 0 | 0 | **MATCH ✓** |
| `typetest_child` | 3,785 | 3,785 | 0 | 0 | **MATCH ✓** |
| `typetest_lob` | 2,309 | 2,309 | 0 | 0 | **MATCH ✓** |

What this demonstrates, point by point:

- **Zero data loss, zero duplication.** Across Full Load **and** live CDC, the
  per-primary-key reconciliation found **0 missing** and **0 extra** rows on every
  table — every source row arrived exactly once, and every source delete was
  applied. The earlier contiguous gap (50–70 rows per round) was driven to **0**.
- **The deliberate failures were caught, not lost.** The ~1.5 MiB oversized LOB
  rows were correctly **quarantined** (absent on the target by design, recorded by
  primary key), and the out-of-range `TINYINT(1)` row triggered the **loud,
  table-fatal** stop — exactly the safe behavior. Nothing was silently dropped or
  silently corrupted.
- **Values matched, not just counts.** Validation's checksum/reconciliation
  compared the actual data (across the full heterogeneous type surface), so a
  "MATCH" here means the rows are *equal*, not merely *equal in number*.
- **The deployment itself passed.** The Fargate redeploy reached
  `UPDATE_COMPLETE` and served HTTP 200, so the result was produced by the
  shipping configuration, not a one-off local setup.

**Bottom line for adopters:** on a schema deliberately built to stress every
DSQL difference — including rows engineered to fail — the migration produced a
**100% match on every clean table with no unexplained discrepancy**, and the rows
that *should* fail were caught and reported rather than lost. And because you run
**Validation** yourself at the end of your own migration, you get this same
evidence — exact counts, checksums, and per-PK reconciliation — for *your* data
before you cut over. You don't have to take the result on trust; you reproduce it.

> The same per-PK reconciliation is also available from a shell:
> `scripts/cdc_consistency_check.py` (names the exact PKs **missing** / **extra** on
> the target) and `scripts/compare_rows.py -t <schema.table> --watch N` (row-count /
> live-convergence check) — see [`scripts/README.md`](../../../scripts/README.md).
> The built-in **Validation** step remains the authoritative go/no-go.

---

**Next:** [9. The Query Converter and the AI DBA →](09-query-validation.md)
