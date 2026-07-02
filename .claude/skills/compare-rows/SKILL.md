---
name: compare-rows
description: Compare row counts (and single-column PK min/max range) between the
  source MySQL and the target Aurora DSQL for one or more tables. Use when the user
  asks whether source and target match/are consistent, to check migration progress,
  to verify a Full Load result, or to watch CDC converge during a Full Load + CDC
  test (e.g. "source와 target 행 비교", "데이터 일치 확인", "CDC 반영 확인").
---

# Compare source vs target row counts

Runs the read-only comparison script `scripts/compare_rows.py`, which uses the
migration tool's own connectors: PyMySQL for the source (handles MySQL native-auth
that the Homebrew `mysql` 9.x CLI dropped) and the DSQL IAM-token connector for the
target. Connection settings come from `.env` (`DB_HOST`/`DB_PORT`/`DB_USER`/
`DB_PASSWORD` for source; `TARGET_ENDPOINT` for DSQL). It is read-only and safe to
run repeatedly.

## Steps

1. Run from the repo root, sourcing `.env` so the DB password is available:

   ```bash
   set -a; source .env 2>/dev/null; set +a
   .venv/bin/python scripts/compare_rows.py
   ```

   - Default compares `cdc_demo.orders`. To compare other/multiple tables, add
     `-t schema.table` (repeatable), e.g.
     `.venv/bin/python scripts/compare_rows.py -t cdc_demo.orders -t cdc_demo.customers`.
   - The script exits 0 when every checked table matches, 1 otherwise — so it can
     gate a test.

2. (Optional) If the user wants to WATCH CDC converge in real time, you can pass
   `--watch <seconds>` (e.g. `--watch 10`), but that loops until match/Ctrl-C — do
   NOT run a `--watch` loop in the foreground here; instead run single checks and
   re-run on request, or launch it in the background if the user explicitly wants a
   running watcher.

## Interpreting the output

The script prints a table (SOURCE | TARGET | RESULT) and a VERDICT line:

- **MATCH** — count and PK range agree. Source and target are consistent for that
  table (this is the success state after Full Load, or once CDC has caught up).
- **DIFFER (Δ=+N)** — counts differ; N is how many rows the source is ahead. During
  an active insert loop / CDC catch-up this is expected and shrinks over time.
- **TARGET MISSING** — the target table doesn't exist yet (pre-Full-Load state).
- **SOURCE MISSING** — the source table/schema isn't there.
- A `[target schema: public]` note means the target table was found in `public`
  rather than the qualified schema — worth flagging (the Full Load and CDC sink
  should write to the same schema).

Report the verdict plainly: state the source vs target counts, whether they match,
and — if they differ — whether it's expected (e.g. CDC still catching up, insert
loop running) or a real inconsistency to investigate.

## Notes

- This is the right tool for Full Load + CDC testing: run it after Full Load
  (expect MATCH on the loaded tables), and again during/after CDC to confirm the
  streamed changes landed.
- It does not modify either database.
