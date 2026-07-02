# Verification scripts — check your Full Load / CDC migration

These two **read-only** helper scripts let you independently verify that your
Full Load and/or CDC migration actually landed on Aurora DSQL — comparing your
**source MySQL** against the **target DSQL** yourself, outside the tool's own UI.

| Script | What it answers | Cost |
|---|---|---|
| [`compare_rows.py`](compare_rows.py) | Do the **row counts** (and PK min/max range) match, per table? | Cheap — a quick sanity check you can run repeatedly. |
| [`cdc_consistency_check.py`](cdc_consistency_check.py) | **Zero data loss?** For every table it loads the full primary-key set from both sides and names the exact PKs **missing on target** (lost rows) and **extra on target** (a source delete not yet applied). | Heavier — reads every PK; run it when you want proof, e.g. before cut-over. |

Both are **read-only on both sides** (source is never modified), print a
per-table report, and **exit `0` only when everything matches / is consistent**
(non-zero otherwise) — so you can gate a shell script on them.

> These are optional, standalone utilities — the migration tool's own
> **Validation** step (Chapter 5 of the manual) is the authoritative go/no-go and
> also runs checksums + full reconciliation. These scripts are a convenient way to
> spot-check from the command line.

---

## 1. Prerequisites

- **Python 3.10+** and this repo's dependencies. From the repo root:
  ```bash
  python -m venv .venv && .venv/bin/pip install -e .
  ```
- **Connection settings via environment / `.env`** (copy `.env.example` → `.env`).
  Both scripts read:

  | Variable | For | Notes |
  |---|---|---|
  | `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD` | source **MySQL** | read-only connection |
  | `TARGET_ENDPOINT` | target **Aurora DSQL** | e.g. `your-cluster-id.dsql.<region>.on.aws` |
  | `TARGET_REGION` | DSQL | optional — auto-derived from the endpoint |
  | `TARGET_DATABASE`, `TARGET_USERNAME` | DSQL | default `postgres` / `admin` |

  The DSQL side authenticates with a short-lived **IAM token** (no password); make
  sure your AWS credentials can connect to the cluster.

- **Tables need a single-column integer primary key** for the per-PK check
  (`cdc_consistency_check.py`). Row-count compare works for any table.

Load your `.env` into the shell first:
```bash
set -a; source .env; set +a
```

---

## 2. `compare_rows.py` — quick row-count check

Point it at **your own tables** with `-t schema.table` (repeatable). A table name
without a schema defaults to the `cdc_demo` schema, so always qualify it.

```bash
# One or more tables
.venv/bin/python scripts/compare_rows.py -t sales.orders -t sales.customers

# Re-check every 10s until they match (useful while CDC catches up); Ctrl-C to stop
.venv/bin/python scripts/compare_rows.py -t sales.orders --watch 10
```

For each table it prints `SOURCE` vs `TARGET` counts and `MATCH` / `DIFFER (Δ=…)`,
plus the PK min..max range on a mismatch. Exit `0` = all match.

**When to use:** right after Full Load (expect counts equal), or during CDC to
watch the target converge.

---

## 3. `cdc_consistency_check.py` — zero-data-loss reconciliation

Stronger than counts: it compares the **full set of primary keys** on both sides.

```bash
# Check specific tables (unqualified names + --schema)
.venv/bin/python scripts/cdc_consistency_check.py --schema sales --tables orders customers payments

# Machine-readable output
.venv/bin/python scripts/cdc_consistency_check.py --schema sales --tables orders --json
```

> **Defaults are the tool's internal sample schema** (`customers_sample_new`, 11
> tables). Always pass `--schema` + `--tables` for **your** database, or set
> `CDC_WORKLOAD_SCHEMA` for the schema.

Per table it reports `source_count` / `target_count`, **`missing_on_target`**
(rows that did not arrive) and **`extra_on_target`** (a source delete not yet
replicated), with sample PKs. The verdict is **`ZERO DATA LOSS`** only when every
table has `missing == 0` and `extra == 0`.

**When to use:** after the source has stopped changing and CDC has drained — the
pre-cut-over proof that nothing was lost.

### Optional: op-log cross-check
If you drive changes with a script that records an op-log (JSONL of
`{ts, op, table, pk}`), pass `--op-log <file>` to pin any lost INSERT / stale
DELETE to the exact operations, independent of the count comparison.

---

## 4. A typical flow

```bash
set -a; source .env; set +a

# After Full Load: counts should match
.venv/bin/python scripts/compare_rows.py -t sales.orders -t sales.customers

# During CDC: watch the target catch up
.venv/bin/python scripts/compare_rows.py -t sales.orders --watch 10

# Before cut-over (source frozen, CDC drained): prove zero data loss
.venv/bin/python scripts/cdc_consistency_check.py --schema sales --tables orders customers payments
```

Only what these two files touch is verified here; for checksums, full
reconciliation, and drift attribution use the tool's built-in **Validation** step.
