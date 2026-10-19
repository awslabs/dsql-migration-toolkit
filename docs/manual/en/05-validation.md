# 5. How Validation works

_Language: **English** | [한국어](../ko/05-validation.md) | [日本語](../ja/05-validation.md)_

> **Prev:** [4. CDC and DSQL constraints](04-cdc-and-dsql-constraints.md)
>
> **Excluded columns** are proven NULL rather than value-compared: see [13.9 Validation and Cut over](13-excluding-tables-and-columns.md#139-validation-and-cut-over).

**Validation** is the step that proves the target matches the source — your
evidence for a safe cut-over. It's the only step that runs **exact** `COUNT(*)`
and checksums (Full Load deliberately used scan-free estimates), so it gives you
an authoritative verdict rather than an approximation.

Run it from the **Validation** step after Full Load (and, if you're using CDC,
after the stream has caught up). It compares the migrated DSQL target against the
source **as of the watermark**, and — on a **MySQL** source — reports drift if the
live source has moved on (a **PostgreSQL** source does not get a drift verdict; see
5.2).

---

## 5.1 What it compares

Validation works **per table**, with increasing levels of rigor you can choose:

| Mode | What it checks | Cost |
|---|---|---|
| **Row count** (default) | Exact `COUNT(*)` on source vs target per table. | Cheap. |
| **Checksum** | Adds an **order-independent** per-table checksum (computed the same way on both engines) — catches value differences that row counts alone would miss. | Heavier (reads every row). |
| **Reconcile** (full PK set) | Streams **every primary key** from both sides in PK order and sorted-merges them, reporting exactly which PKs are **missing on the target** and which are **extra on the target** (a delete CDC hasn't applied yet). | Heaviest; the pre-cutover "no mismatched records" proof. |

You don't have to run the heavy modes on everything: an option computes the
expensive checksum/reconcile **only on tables whose counts already mismatch**, so
count-matched tables are reported as "not deeply checked" (not as a false match).

> **CDC migrations:** any table the stream dead-lettered a change for is also
> compared by **CHECKSUM**, whatever mode you pick — a row count cannot see a missed
> UPDATE (see 5.5).

> **Reconcile applies to single-column integer PKs.** Those have a well-defined
> ordering across both engines. Composite or non-integer PKs are skipped for the
> reconcile pass (count/checksum still apply).

> **Every table with a primary key is counted and checksummed in bounded pages, a
> composite key too.** Each side reads its own rows in its own primary-key order — a
> MySQL source with the cursor the Full Load reader uses (`ENUM` / `SET` keys included),
> a PostgreSQL source and Aurora DSQL with a row-value cursor — at most 5,000 rows per
> statement on Aurora DSQL, and the two totals are compared. Pages are never compared
> with each other (the engines sort text differently), so a mismatch is reported for the
> table as a whole. Aurora DSQL is paged by the primary key it holds, read for each
> table, so a table re-keyed in Schema Conversion is counted by its new key, never by a
> key Aurora DSQL does not keep unique. On the source a composite key's row count stays
> one `COUNT(*)` (a source has no transaction limit), read before the pages, and the
> checksum's pages must add up to it: they stop as soon as they read more. Pages that
> cannot be trusted that way, or that do not move past their cursor, make a PostgreSQL
> source's table an error, never a match; on a MySQL source the checksum is read in one
> statement instead (the log says why). A MySQL key with a descending or prefix part is
> checksummed on the source in one statement, and so is one with a single-precision
> `FLOAT`, `TIME` or `BIT` part, a one-column key too. Only a table with no primary key
> on Aurora DSQL (which the tool does not migrate) is still counted and checksummed
> there in one statement; on a very large one Aurora DSQL's 300-second transaction limit
> stops it, and it is reported as an error that says what to do, never as a match.

> **Checksum excludes floating-point and JSON columns from a MySQL source.** MySQL
> `FLOAT`/`DOUBLE` and `JSON` have no exact text form that renders identically on both
> engines, so they are left out of the checksum and listed in the report as **"not
> value-compared."** On a **PostgreSQL** source one expression renders both ends, so
> `real`/`double precision` **are** checksummed, and so is `jsonb` (which has a
> canonical form) — only plain `json` is excluded — so a jsonb value drift in a
> non-key column is still caught. A value
> difference confined to such a **non-key** column is therefore not caught by any mode
> (row count is unchanged by an in-place edit, and reconcile compares PK *presence*, not
> values). Key columns and every other type are still fully compared. Column count is
> **not** a checksum limit — a very wide table's columns are hashed in groups (nested
> MD5) so the checksum stays within DSQL's 100-argument function cap.

---

## 5.2 Live-source drift

A real source keeps changing while you migrate, so "do the counts match?" needs a
reference point. Validation uses the **watermark**:

- **On a MySQL source**, it compares the **current** source position against the
  watermark's: by **GTID** when both the watermark and the live source expose one,
  otherwise by the binlog **file:position** (the path whenever GTID can't be enabled,
  e.g. RDS MySQL 8.0; the coordinate is read via `SHOW BINARY LOG STATUS` on MySQL
  8.2+/8.4, else `SHOW MASTER STATUS`). If the source has advanced, the report marks
  **`drifted = true`**.
- **On a PostgreSQL source**, the watermark records a **WAL LSN** (plus the logical
  replication slot and publication), not a binlog/GTID coordinate, and Validation does
  **not** currently recompute LSN-based drift: the binlog/GTID probes are skipped and
  `drifted` is reported as **"could not be determined."** Watch the target **converge**
  during CDC (next bullet) rather than rely on a drift flag.
- Either way, the report carries the watermark's **snapshot timestamp** as the "as of"
  point — so a count difference is correctly attributed to *new source activity since
  the snapshot*, not to a migration bug.
- During CDC, you typically watch the target **converge** toward the source
  (the lightweight `scripts/compare_rows.py --watch N` re-checks every *N*
  seconds and is handy for this), then run full Validation once it's caught up.

---

## 5.3 The verdict model

The verdict is **sound by construction** — it only ever reports a match when the
evidence supports it:

- A **table** is `matched` only if its counts are equal **and** (in checksum mode)
  the checksums are equal **and** (if reconciled) the PK sets are consistent.
- The **whole report** is `is_match` only if **every** table matched and there are
  no orphaned rows.
- A failure comparing one table is **isolated** to that table's error entry — it
  never aborts the run or silently passes the rest.

Aurora DSQL **enforces** foreign keys, and the tool re-creates them after the load
(at cut over for a CDC migration). An optional **orphan check** counts target child
rows whose (non-null) foreign key has no matching parent: it is the **pre-apply
gate** for that step — an enforced `ADD CONSTRAINT` fails if any child row is
orphaned, so the tool skips that constraint with an actionable message rather than
failing the run — and it is the referential-integrity safety net when you chose to
**strip** the foreign keys instead. A foreign key whose parent table is not on Aurora
DSQL cannot be counted: the report lists it as **not checked**, which changes no
verdict. An orphan check that fails (for example on a foreign-key column whose type no
longer matches its parent key) marks only its own table not matched: its row counts and
its row-count / CHECKSUM verdicts stay shown, so a real mismatch is still seen, and
**Orphan records** lists the foreign key and the error. Fix the cause and re-check the
table, or turn off **Check for orphan records** and re-run.

For a table that mismatches, an optional bounded **row-diff sample** lists a few
differing PKs (and checksum tokens) — **never the row values** — so you can
investigate without exposing data.

The report is **exportable** so you can attach it to a cut-over decision.

---

## 5.4 How to read a typical result

| Table | source | target | verdict |
|---|---|---|---|
| `orders` | 3906 | 3906 | **MATCH** |
| `order_items` | 3785 | 3785 | **MATCH** |
| `documents` | 2309 | 2306 | **investigate** |

- **MATCH** on every table (and no orphans) → safe to cut over (for a CDC
  migration, once the dead-letter check in 5.5 clears).
- A small **target deficit** is usually either (a) **drift** — the source advanced
  since the watermark (check the drift flag), or (b) **intentional** — rows that were
  quarantined (e.g. values over DSQL's 1 MiB limit). You no longer have to work out
  which by hand: when the deficit is **exactly** the number of rows the migration
  permanently dropped, the table says so — *"Fully explained: N rows were permanently
  dropped during the migration … this deficit is expected, not new data loss"*. When
  the deficit is **larger** than the drop, it says *"Partly explained: … but N more
  are missing and are NOT accounted for"* — those are the rows to investigate.
  The table still reports **MISMATCH**, deliberately: the rows really are absent, so
  the attribution explains the gap rather than excusing it. Close it by fixing the
  source value and reloading that table, or accept it as a known gap before cut over.
  (After an app restart the per-table drop counts are gone — they are not persisted —
  so the deficit is reported unexplained rather than guessed at. Cross-check the
  **Full Load error log** in that case. CDC dead letters are different: the tool keeps
  a per-table count of them across restarts, and Cut over checks it (5.5).)
- A **target surplus** (extra PKs on the target) during CDC usually means a source
  **delete** that the stream hasn't applied yet — re-check after it converges.

> **Rule of thumb:** if the only differences are explained by drift, intentional
> quarantine (oversized values), or not-yet-converged CDC, the migration is sound.
> Any **unexplained** missing/extra PK on a count-matched, caught-up table is what
> Validation exists to surface.

> **The report's own recovery card.** When a mismatch is **unexplained**, the report shows
> **How to recover** with the ordered click-path. On a **Full load + CDC** or **CDC only**
> migration whose CDC stack has **streamed** — stopped after a Start, its connectors in
> place, or a Start CloudFormation rolled back — or whose CDC state has not been read yet
> (after an app restart, until the Data Migration step reads it; the card says so, and
> shows the steps for the stack as it is once it has been read), that backfill takes a
> **new start point**, so the steps put new CDC topics first: Stop CDC if it is running →
> **Delete CDC infrastructure** and deploy it again → **Re-run Full Load** (**Re-run all
> tables** when some failed) choosing **Drop & reload** → **Start CDC**, which starts from
> the new Full Load's start point → **Re-run validation**. With CDC only, a step after the
> redeploy sets the migration type to Full load + CDC, which has the Full Load step. The
> Full Load step refuses a new start point on such a stack until the infrastructure is
> deployed again
> ([Chapter 3 §3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)): a resume after a
> Full Load that took a new start point is **not** gapless, which is why the new topics come
> first. On a stack that never streamed the order is shorter (with CDC only, switch the
> migration type to Full load + CDC first → **Re-run Full Load** (**Re-run all tables** when
> some failed) choosing **Drop & reload** → **Start CDC**, which starts from the new Full
> Load's start point → **Re-run validation**). These are the steps above without Stop CDC
> and the redeploy, and the card shows them once that redeploy is done: the re-run is a
> **Drop & reload** there too, because CDC starts from its new start point and an append
> would keep rows older than it. In **Full load only** the card is *Re-run Full
> Load to backfill the gap*: the re-run, then **Re-run validation**, with no CDC step.
> Reloading **one table** is different: **Drop & reload** of specific tables keeps the
> **original** start point, so the per-table way forward in 5.5–5.7 is never gated. When
> the target holds rows the source does not, the card says *The target holds rows the
> source does not — a delete never landed*: a re-run only adds rows, so it names that
> per-table reload (**Reload specific tables**, with CDC stopped, choosing **Drop &
> reload**; with CDC only, after switching the migration type to Full load + CDC). With CDC
> only and no Full Load record in this session there is no table to reload, so it names a new
> Full Load start point instead: Stop CDC if it is running and **Delete CDC
> infrastructure** and deploy it again (on a stack that has streamed, or whose CDC state is
> not read yet), then switch the migration type to Full load + CDC, run its Full Load
> choosing **Drop & reload**, and **Start CDC**.

Once Validation reports a clean **MATCH** (or every difference is explained) — and,
for a CDC migration, the dead-letter check in 5.5 clears — the
final workflow step — **Cut over** — appears in the UI with the runbook for
switching your application from the source database (RDS/Aurora MySQL or PostgreSQL)
to DSQL. In Full load only, Cut over also needs the report to cover every table
selected after the Full Load: such a table that the report has no row for holds Cut over
until you load it and re-run Validation (or turn its chip off under **Objects to
validate**). The same holds in Full load + CDC (and CDC only) for a table selected after the Full Load and loaded under its start point: a report with no row for it holds Cut over until you re-run Validation. In Full load + CDC, a table unticked after the Full Load (before CDC starts) is not validated: Validation compares the tables still selected. Re-run Validation if your last report predates the change. See the recommended end-to-end flow
in the [Conclusion](10-conclusion.md) for the cut-over sequence (CDC-drain vs
Full-Load freeze) and the rollback anchor.

> **Verify from the command line (optional).** Besides this built-in Validation,
> two read-only CLI scripts let you spot-check from a shell (and gate a script on
> their exit code): `scripts/compare_rows.py` (per-table row-count / PK-range check,
> with `--watch N` to watch the target converge during CDC) and
> `scripts/cdc_consistency_check.py` (full primary-key reconciliation that names the
> exact PKs **missing** or **extra** on the target — the zero-data-loss proof before
> cut-over). See [`scripts/README.md`](../../../scripts/README.md). This built-in
> Validation step remains the authoritative go/no-go (it also runs checksums and
> drift attribution).

---

## 5.5 CDC dead letters and cut over

A **dead letter** is a change the CDC stream set aside because Aurora DSQL rejected it
(for example after a source schema change). It is **never retried**. A later full-row
update of the same row heals it, but a row that is never touched again stays missing
or stale, and a lost DELETE leaves an extra row on the target. A missed UPDATE changes
no row count, so the default row-count mode cannot see it.

So for a CDC migration (**Full Load + CDC** or **CDC only**) the tool keeps a
**per-table count** of dead letters (with the latest time and the drift kinds), and
Validation and Cut over use it:

- **Validation compares those tables by CHECKSUM automatically.** Whatever mode you
  pick, a run also re-compares every dead-lettered table that matched without a value
  comparison (the Options show which tables before you run). If a dead letter arrives
  later, the report says *"Tables match — cut over waits for a CHECKSUM re-check of N
  table(s)"* and offers **Re-check N table(s) with CHECKSUM**. If the dead-letter history
  cannot be confirmed (below), the report says *"Tables match — cut over waits for one
  CHECKSUM run of every table"* instead, and the Options say so before you run: one run
  in CHECKSUM mode with Fast sweep off clears it. Tables whose REPLICA IDENTITY dropped
  below FULL ([§5.6](#56-re-keyed-postgresql-tables-and-cut-over)) or whose source schema
  changed while CDC ran ([§5.7](#57-source-schema-changes-and-cut-over)) are compared by
  CHECKSUM the same way.
- **Cut over opens only when all of the following hold:**
  1. each dead-lettered table matched in a CHECKSUM comparison that **started more
     than a minute after** its latest dead-lettered record;
  2. the tool has made a **successful, complete read** of the CDC dead-letter log in
     CloudWatch Logs that **started after the latest validation finished**;
  3. the history of that read reaches back to the **Full Load watermark** (or, for CDC
     only, the CDC start).
- **A held change is not a dead letter yet.** A change the sink holds on a unique index
  ([§4.4](04-cdc-and-dsql-constraints.md#44-how-the-sink-handles-dsqls-constraints-in-the-data-path))
  is dead-lettered only when its wait ends, so a read made meanwhile cannot count it:
  wait at least 5 minutes after the source's last write before the final validation
  ([§10.3](10-conclusion.md#103-the-cut-over-switching-your-application-to-dsql), step
  3). Its table's Stream lag shows the wait.
- **Restarts.** The per-table count, its latest time and the drift kinds survive an
  app restart; the record list on the Data Migration step does not. After a restart,
  Cut over re-reads the log automatically — one bounded read each time the Validation or
  Cut over step is drawn, at most once a minute.

It **fails closed**. Cut over shows a red notice instead of the runbook when:

- the log cannot be read — usually a missing `logs:FilterLogEvents` permission (the
  deployed task role covers CDC stacks named `dsql-cdc-*` or `mysql-dsql-cdc-*`; a
  local AWS profile needs the same permission);
- the CDC infrastructure (and with it the log group) was deleted;
- a read stopped at its page limit.

If the history cannot be confirmed — a pipeline you attached while it was already
running, or a session saved by an earlier version of the tool — it shows an amber
notice instead; one full CHECKSUM run of every table clears it by itself.

**Check again** continues the read where it stopped: a log group too large for one
read is read further each time. **Continue without this check** records your decision
in the activity log; it is offered only once no table is left to re-check. It is scoped
to the records seen so far and re-opens when a newer one arrives — except while the log
cannot be read: the tool cannot see a newer record then, so check the stream another
way before you repoint the application. A new validation run clears it, and it never
overrides a Validation mismatch. A dead-lettered table whose CHECKSUM keeps failing on
re-check (the table's error says why) is the one case a re-check cannot clear: turn it
off under **Objects to validate**, re-run, and then acknowledge it on Cut over. A table
whose orphan check failed is not that case: its CHECKSUM finished and its verdict stands,
and **Orphan records** says what to fix (5.3).

CHECKSUM names the columns it cannot value-compare (5.1); for a dead-lettered table
Cut over repeats them as a non-blocking warning.

**The way forward** for a table that does not match:

- **Full Load + CDC:** re-run Step 1 (Evaluation) if the source schema changed →
  Stop CDC → reload the table with **Drop & reload** → Start CDC → re-check it.
- **CDC only:** correct the rows on Aurora DSQL by hand (the dead-letter queue card
  lists each record's primary key and Kafka offset, with the key's value when that is an
  integer or a UUID) → re-check; for a table whose key values the card withholds, use
  **Start over**. A source `TRUNCATE`'s dead letter names no row: Stop CDC → delete the
  rows the source no longer has (at most 3,000 rows per transaction) → Start CDC →
  re-check
  ([§4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important)).

**Full Load only** is unaffected, unless CDC ran earlier in the same session: dead
letters recorded then still need their CHECKSUM match.

---

## 5.6 Re-keyed PostgreSQL tables and cut over

This applies only to a **PostgreSQL** source and only to tables CDC **re-keys** — a
composite target key (Schema Conversion's Composite key, or an edited key) that adds a
column outside the source primary key. A `DELETE` on such a table carries that column
only while the source's `REPLICA IDENTITY` is **FULL** on the table and every partition.
If it drops below FULL while CDC runs (`ALTER TABLE … REPLICA IDENTITY DEFAULT`, a new
partition, `USING INDEX`), a delete can leave its row on Aurora DSQL with no error and no
dead letter ([§4.6](04-cdc-and-dsql-constraints.md#re-keyed-postgresql-tables-the-replica-identity-re-check)
says when). The CDC monitor re-reads it and records each drop
([§4.6](04-cdc-and-dsql-constraints.md#re-keyed-postgresql-tables-the-replica-identity-re-check));
Validation and Cut over use that record:

- **Validation compares those tables by CHECKSUM automatically**, named in the Options
  before you run. While a table is still below FULL the report says *"Tables match —
  cut over is held: REPLICA IDENTITY on N re-keyed table(s) is below FULL"*: no CHECKSUM
  releases it, because changes can keep being lost. Once it is FULL again, a match started
  before the fix is not enough: the report says *"cut over waits for a CHECKSUM re-check"*
  and offers **Re-check N table(s) with CHECKSUM**.
- **Cut over opens only when** every such table is FULL again **and** matched in a
  CHECKSUM comparison that **started more than a minute after** the tool read it FULL.
  Cut over also makes **one fresh read** of REPLICA IDENTITY after the latest
  validation (a few seconds) before it opens, so a change since then is caught.
- A table left out of the report, or inside a gap you accepted, can be acknowledged
  with **Continue without this check** once no other re-keyed table is below FULL or
  needs a re-check (recorded in the activity log; it re-opens if the identity drops
  again). A table inside an accepted gap cannot be cleared by a CHECKSUM until its source
  rows are corrected and it is reloaded with **Drop & reload**. A table still below FULL
  is never waivable.
- **If the source can't be read** — no password after an app restart, a failed read, a
  table not found — and no table is still recorded below FULL, Cut over does not hold:
  it shows a warning above the runbook, and the acknowledgement line in the activity log
  records that the identity was not re-checked. Confirm on the source that these tables
  and their partitions are FULL before you repoint, or reconnect the source on the
  Connect step (the tool then re-reads it by itself; after a failed read, choose
  **Check again**). A table still recorded below FULL keeps Cut over held until the tool
  reads it FULL; the held notice says when the re-read failed.
- **Restarts.** The record of each drop survives an app restart and is cleared only by
  Start over. **MySQL** sources and **Full load only** are unaffected.

**The way forward** for a table that differs after the fix: Full Load + CDC — let CDC
catch up, Stop CDC, reload it with **Drop & reload**, Start CDC, re-check it; CDC only —
delete the extra rows on Aurora DSQL by hand, then re-check.

---

## 5.7 Source schema changes and cut over

This applies to a CDC migration (**Full Load + CDC** or **CDC only**), MySQL or
PostgreSQL. Some source DDL changes what CDC writes without a single rejected row — a
dropped nullable column, a widened type, a renamed table and more
([§4.2](04-cdc-and-dsql-constraints.md#changes-that-dead-letter-nothing-the-source-schema-re-check)).
The CDC monitor re-reads the captured tables' definitions on the source and records each
table whose definition no longer matches the Step 1 (Evaluation) inventory
([§4.6](04-cdc-and-dsql-constraints.md#the-source-schema-re-check)); Validation and Cut
over use that record:

- **Validation compares those tables by CHECKSUM automatically**, named in the Options
  before you run. While a table's source definition still differs from the Step 1
  inventory, Cut over stays held whatever the report shows. An added or dropped column,
  or a missing table, usually means the table cannot be compared at all (*"N table(s)
  could not be compared"*; re-run Step 1). Otherwise the report says *"Tables match —
  cut over is held: the source schema changed on N table(s)"*. No CHECKSUM releases it:
  the comparison renders each column from the Step 1 inventory, so it can match falsely
  (a `TINYINT(1)` widened to `INT` still compares as true or false).
- **It clears in two steps.** First the source must match the Step 1 inventory again:
  re-run Step 1 (Evaluation) so it holds the new definition (for an added or renamed
  column, add or rename it on Aurora DSQL with **Fix target schema…** first: Validation
  cannot compare a column the Step 1 inventory does not have), or undo the change on the
  source — the tool re-reads by itself, or choose **Check again**. Then the table must
  match in a CHECKSUM comparison that **started more than a minute after** that; the
  report offers **Re-check N table(s) with CHECKSUM**. A Step 1 re-run that changes a captured table's
  definition is treated the same way: values CDC applied before it may differ, so that
  table also needs a CHECKSUM started after the re-run.
- **Cut over** makes **one fresh read** of the source definitions after the latest
  validation or Step 1 run (a few seconds) before it opens, so a change since then is
  caught.
- A table left out of the report, no longer in the Step 1 inventory (dropped or renamed
  at the source, then Step 1 re-run), or inside a gap you accepted, can be acknowledged
  with **Continue without this check** once nothing else is open or needs a re-check
  (recorded in the activity log; it re-opens if the table changes again). A table whose
  source still differs is never waivable.
- **If the source can't be checked** — no password after an app restart, no Step 1
  inventory (Step 1 is running or its last run failed), a failed read — and nothing is
  recorded as changed, Cut over does not hold: it shows a warning above the runbook, and
  the acknowledgement line in the activity log records that the source schema was not
  re-checked. Confirm that no DDL ran on the migrated tables while CDC ran before you
  repoint, or reconnect the source on the Connect step (after a failed read, choose
  **Check again**). A table still recorded as changed keeps Cut over held.
- **Restarts.** The record of each change survives an app restart and is cleared only by
  Start over. **Full load only** is unaffected, unless CDC ran earlier in the same
  session: changes recorded then still apply.

**The way forward** for a table that differs in that CHECKSUM: Full Load + CDC — let CDC
catch up, Stop CDC, reload it with **Drop & reload**, Start CDC, re-check it; CDC only —
correct its rows on Aurora DSQL by hand, then re-check.

---

**Next:** [6. Limitations →](06-limitations.md)
