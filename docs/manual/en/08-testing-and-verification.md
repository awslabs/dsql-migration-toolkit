# 8. Testing — the DSQL-driven scenarios

_Language: **English** | [한국어](../ko/08-testing-and-verification.md)_

This chapter is organized around a single idea: **each Aurora DSQL characteristic
forces a specific migration scenario that must be tested.** A MySQL→MySQL
migration never has to prove these — they exist precisely because DSQL is a
different engine (PostgreSQL-wire, distributed, serverless, IAM-auth, optimistic
concurrency). For each DSQL trait below, we list the scenario it forces and how
the tool exercises it.

> Two test layers back every scenario: an **offline suite** (~1,700 Python + 42
> Java tests; no AWS — seams injected) that proves the behavior deterministically,
> and a **live end-to-end run** (real RDS MySQL + Aurora DSQL + MSK) that proves it
> on real infrastructure with deliberately failing rows (summarized in §8.2).

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

### Optimistic concurrency — DSQL has no locks; conflicts surface at commit (40001)

DSQL detects write conflicts at commit and returns `SQLSTATE 40001`. The losing
transaction must **re-run**, and under contention this happens often.

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| 40001 conflict on a write | A batch hits a serialization conflict, then succeeds on retry | Statement-level retry with backoff/jitter recovers (`test_occ_conflict_on_batch_is_retried_then_succeeds`); the same logic in the CDC sink (`OccRetryTest.java`) |
| Retry budget exhausted | Conflicts never clear | Recorded as a **failure**, never silently dropped (`test_exhausted_occ_conflict_is_recorded_as_failure`) |
| High concurrency vs. connection quota (10,000/cluster, 100/s) | Many tables × many batches at once | Total in-flight connections stay bounded (`test_parallel_connection_use_is_bounded`); default 4 tables × 8 batches = 32 ≪ quota |

### 1 MiB per-value limit — a single big value can't be stored

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| Value > 1 MiB | A LOB/TEXT value over the limit during Full Load **and** during CDC | Per-row **quarantine** (PK + reason recorded, table keeps loading) at Full Load; **DLQ** at the sink, measured before the write (`DsqlSinkTask` oversized guard) |
| Value > 8 MiB (can't traverse Kafka) | An even larger column | Excluded **at capture** via Debezium `column.exclude.list`, driven by the Evaluation `OVERSIZED_LOB` flag |

### IAM-token auth — no password, 15-min tokens, 60-min connections

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| Short-lived token / dropped connection | A long-running load or CDC stream outliving a token or hitting the 60-min connection cap | The pool discards a dead/half-open connection and the retry reconnects with a **fresh** token (`test_transient_connection_error_is_retried_and_recovers`, `test_pool_discards_connection_after_in_use_error`; sink: `DsqlSinkTaskTest`, `DsqlIamTokenProviderTest`) |

### Asynchronous indexes — `CREATE INDEX ASYNC`, built after data

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| Indexes built async, after load | A table with secondary indexes; a load where a data batch fails | Indexes are created only after **all** data lands; if any data batch fails, indexes are **not** created (`test_indexes_created_after_all_data…`, `test_indexes_are_skipped_when_a_data_batch_fails`) |

### Schema differences — no FK, PK required, unsupported types/objects

DSQL omits foreign keys, triggers, stored procedures, several types, and requires a
primary key.

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| No foreign keys | An FK-laden source | FK removed from DDL, preserved as a `MANUAL` note; Validation's **orphan check** confirms app-side integrity held (`test_orphan_records_are_detected_and_fail_the_match`) |
| Primary key required | A table with **no** PK; a **composite** PK | No-PK is blocked up front (`UNSUPPORTED`) and keyset export refuses it; composite PK loads via row-value tuple comparison (`test_exporter.py`, scenario doc) |
| Unsupported types/objects | Spatial types, `DECIMAL` precision > 38, > 255 columns, triggers/routines | Flagged `UNSUPPORTED` in Evaluation with a reason (`test_converter.py`, assessor tests) |
| TINYINT(1) → boolean, out of range | A `TINYINT(1)` holding `2` | A **loud, table-fatal** error — refuses to flatten `2` to `true` (no silent corruption) |
| Type heterogeneity (MySQL → PG dialect) | Max type-diversity schema | Full Load (Python) and CDC sink (Java) must encode each type to the **identical** stored form — enforced by a shared **write-contract** parity test (`test_dsql_write_contract.py`) |

### Gapless Full Load → CDC handoff — the hardest correctness property

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| Bulk load then resume streaming with no gap / no duplicate | Load a snapshot, run a live INSERT/UPDATE/DELETE workload, start CDC from the watermark, converge | Offset seeded to the watermark + `snapshot.mode=recovery`; idempotent PK-keyed apply means overlap can't duplicate (`test_cdc_pipeline.py`, `test_cdc_offset_seed.py`, `test_offset_seeder_lambda.py`) |
| CDC replicates data, not DDL | A source schema change mid-CDC | A row that no longer matches the target shape goes to the **DLQ**, not lost (`test_cdc_dlq.py`) |

### Live source keeps changing — drift must be attributed correctly

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| Source advances during/after migration | Validate while the source GTID has moved past the watermark | Drift is detected and reported via watermark GTID, so a count delta is attributed to **new source activity**, not a bug (`test_drift_since_snapshot_is_reported`, `test_no_drift_when_gtid_unchanged`) |
| Equal counts but different data | Rows match in number but differ in value | **Checksum** catches it; a count-only "match" is never trusted (`test_deliberate_data_mismatch_with_equal_counts_is_not_a_match`) |

### Resumability — interruptions must not lose or duplicate work

| DSQL characteristic | Scenario tested | How it's exercised |
|---|---|---|
| Idempotent re-apply | Re-run a batch / resume an interrupted load | `INSERT … ON CONFLICT` never duplicates; resume re-runs only unfinished PK ranges and converges to the uninterrupted state (`test_reapplying_batches_does_not_duplicate_rows`, `test_resume_skips_done_batches_and_converges_to_uninterrupted_state`) |

**Run the suites:** `\.venv/bin/python -m pytest -q` and
`cd connectors/dsql-sink && mvn -q test`.

---

## 8.2 Putting the scenarios together — the live end-to-end run

The scenarios above were also exercised **together, on real AWS** (RDS MySQL +
Aurora DSQL + MSK), using a purpose-built schema designed to hit as many DSQL
characteristics as possible in one run:

- A parent → child / lob foreign-key chain (forces the **no-FK** + **PK** +
  **orphan-check** scenarios).
- Maximum type diversity — every integer/unsigned variant, `DECIMAL` incl.
  precision > 38, `FLOAT`/`DOUBLE`, `BIT`, collation, the full DATE/TIME family,
  `ENUM`/`SET`/`JSON`, and the full LOB family (forces the **type-heterogeneity**
  and **unsupported-type** scenarios).
- **Deliberately failing rows**: ~1.5 MiB LOB values (forces the **1 MiB
  quarantine/DLQ** scenario) and a `TINYINT(1)` = `2` in an isolated table (forces
  the **loud table-fatal** scenario).

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

---

**Next:** [9. Conclusion →](09-conclusion.md)
