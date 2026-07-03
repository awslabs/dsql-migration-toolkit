# 5. How Validation works

_Language: **English** | [한국어](../ko/05-validation.md) | [日本語](../ja/05-validation.md)_

> **Prev:** [4. CDC and DSQL constraints](04-cdc-and-dsql-constraints.md)

**Validation** is the step that proves the target matches the source — your
evidence for a safe cut-over. It's the only step that runs **exact** `COUNT(*)`
and checksums (Full Load deliberately used scan-free estimates), so it gives you
an authoritative verdict rather than an approximation.

Run it from the **Validation** step after Full Load (and, if you're using CDC,
after the stream has caught up). It compares the migrated DSQL target against the
source **as of the watermark**, and reports drift if the live source has moved on.

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

> **Reconcile applies to single-column integer PKs.** Those have a well-defined
> ordering across both engines. Composite or non-integer PKs are skipped for the
> reconcile pass (count/checksum still apply).

---

## 5.2 Live-source drift

A real source keeps changing while you migrate, so "do the counts match?" needs a
reference point. Validation uses the **watermark**:

- It compares the **current** source GTID to the watermark's GTID. If the source
  has advanced, the report marks **`drifted = true`** with the watermark's
  snapshot timestamp as the "as of" point — so a count difference is correctly
  attributed to *new source activity since the snapshot*, not to a migration bug.
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

Because DSQL has no foreign keys, an optional **orphan check** can count target
child rows whose preserved (app-enforced) foreign key has no matching parent —
useful for confirming referential integrity held up after FK removal.

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

- **MATCH** on every table (and no orphans) → safe to cut over.
- A small **target deficit** is usually either (a) **drift** — the source advanced
  since the watermark (check the drift flag), or (b) **intentional** — rows you
  expected to be quarantined (e.g. values over DSQL's 1 MiB limit). Cross-check
  the deficit against the **Full Load error log** / **CDC DLQ** before treating it
  as a real mismatch.
- A **target surplus** (extra PKs on the target) during CDC usually means a source
  **delete** that the stream hasn't applied yet — re-check after it converges.

> **Rule of thumb:** if the only differences are explained by drift, intentional
> quarantine (oversized values), or not-yet-converged CDC, the migration is sound.
> Any **unexplained** missing/extra PK on a count-matched, caught-up table is what
> Validation exists to surface.

Once Validation reports a clean **MATCH** (or every difference is explained), the
final workflow step — **Cut over** — appears in the UI with the runbook for
switching your application from MySQL to DSQL. See the recommended end-to-end flow
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

**Next:** [6. Limitations →](06-limitations.md)
