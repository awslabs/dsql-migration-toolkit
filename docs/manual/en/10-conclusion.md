# 10. Conclusion

_Language: **English** | [한국어](../ko/10-conclusion.md) | [日本語](../ja/10-conclusion.md)_

> **Prev:** [9. Query validation and the AI DBA](09-query-validation.md)

You've now seen the whole journey: connect, plan, **evaluate**, **convert the
schema**, **Full Load**, optionally **CDC**, **validate**, and **cut over**. This
chapter ties it together — which path to choose, a recommended end-to-end flow, the
cut-over runbook, and where to go next.

---

## 10.1 Which path do I need?

| Your situation | Use |
|---|---|
| One-shot migration; a short maintenance freeze is acceptable | **Full Load only** (no streaming infrastructure, no ongoing cost). |
| Large-scale / continuous; need **near-zero-downtime** cut-over | **Full Load + CDC** (gapless handoff keeps DSQL live until you switch over). |

CDC adds real moving parts (MSK, MSK Connect, the sink connector) and **ongoing
cost while deployed**. Reach for it only when you genuinely need continuous
replication; otherwise Full Load alone is simpler and cheaper.

---

## 10.2 A recommended end-to-end flow

1. **Connect** to source (read-only) and target (DSQL, IAM-token).
2. **Evaluation** — read the compatibility report. Resolve every **UNSUPPORTED**
   item (PK, triggers, routines, spatial types, precision > 38, oversized LOBs)
   and decide each **MANUAL** item (FK → app-side integrity, partitioning, etc.).
   *Don't skip this* — it's what turns "the load failed mysteriously" into "I knew
   that object needed changing." It is also where you learn whether CDC is viable
   at all: cascading foreign keys never reach the binary log, so CDC cannot
   replicate them.
3. **Schema Conversion** — review the source-vs-converted DDL and apply it to DSQL.
4. **Data Migration** — choose the migration type (**Full Load** only, or add
   **CDC**) now that the report tells you what you are dealing with. Run the
   prerequisite checks, then **Full Load** bulk-copies the rows and captures the
   watermark (check the error log: quarantined rows are expected, e.g. oversized
   values, or actionable). For a CDC type, deploy the streaming infrastructure from
   the Prerequisites sub-step **before** starting the load, so its ~15–20 min create
   overlaps the snapshot; then start **CDC** from that watermark and watch the target
   converge.
5. **Validation** — run row-count + checksum (+ reconcile before cut-over). The
   verdict is **MATCH** only when every difference is explained (drift, intentional
   quarantine, not-yet-converged CDC).
6. **Cut over** — once Validation is a clean MATCH, switch your application to
   DSQL. This is the one step the tool does not perform for you; follow the
   tailored runbook (§10.3 below).

---

## 10.3 The cut-over: switching your application to DSQL

"Cut over" is the final workflow step and the moment your application stops using
MySQL and starts using Aurora DSQL. It is the one step the tool does **not** do for
you — it proves the target is correct and gives you a gapless replication stream,
but *when* and *how* you repoint your application is an operational decision only
you can make. The UI shows the matching runbook on the **Cut over** step once
Validation is a clean MATCH; below is the same guidance for each path.

### Full Load only (a short maintenance freeze)

Use this when a brief write-freeze is acceptable. There is no streaming, so the
freeze lasts the length of the load + validation.

1. **Freeze writes on the source** — put the application in maintenance / read-only
   mode so no new rows are written to MySQL. (The tool keeps the source read-only;
   *your application* is what must stop writing.)
2. **Re-run Full Load** if the source took writes since the snapshot — it's
   idempotent, so it only fills the unfinished work, never duplicates.
3. **Validation** — confirm a clean **MATCH** (the go/no-go gate).
4. **Repoint the application** to the DSQL endpoint (PostgreSQL wire, IAM-token
   auth — no password). Smoke-test the critical read/write paths.
5. **Lift the freeze** — the application is now live on DSQL.

### Full Load + CDC (near-zero-downtime)

CDC keeps DSQL converging while the application keeps writing to MySQL, so the
freeze shrinks to just the final drain + smoke test.

1. **Let CDC catch up** until replication lag is at/near zero.
2. **Freeze writes on the source** (brief read-only / maintenance mode).
3. **Wait for the final drain** — let CDC apply the last in-flight changes until
   lag is zero again. MySQL and DSQL now hold the same rows.
4. **Re-run Validation** for the final go/no-go; cut over only on a clean MATCH.
5. **Repoint the application** to the DSQL endpoint and smoke-test.
6. **Tear the CDC pipeline down LAST** — on the **Cut over** step, click *Start
   over* → *Delete all CDC infrastructure*. It ends replication, stops MSK / MSK
   Connect / NAT cost, and clears the old stack (a future fresh Full Load or CDC
   needs it removed before it can deploy).

### Rollback anchor

Decide your rollback rule *before* you cut over. Until you've signed off on DSQL,
keep the MySQL source **frozen (read-only), not dropped**. Before you repoint,
rollback is trivial — the source is untouched and still authoritative. After the
application writes to DSQL, those new rows live **only** on DSQL (this tool
replicates MySQL → DSQL, **not** the reverse), so rolling back then means
reconciling them yourself first.

---

## 10.4 Principles the tool follows (so you can trust the verdicts)

- **Source is always read-only.** The tool never writes to your MySQL source.
- **Loud over silent.** It refuses to corrupt data quietly — a value it can't
  represent stops the table visibly; an incomplete load is never reported as
  success.
- **Idempotent and resumable.** Re-running Full Load or replaying CDC never
  creates duplicates; an interrupted load resumes only the unfinished work.
- **Sound validation.** A "match" is reported only when the evidence (counts,
  checksums, PK sets) supports it.
- **Credentials stay in memory.** Never written to disk, logs, reports, or job
  state.
- **AI is advisory only.** Optional Bedrock suggestions are review-only and never
  touch the data path.

---

## 10.5 Where to go next

- **Common questions:** [Chapter 11 — Customer FAQ](11-customer-faq.md).
- **Run a migration:** start at [Chapter 1 — Set up](01-setup.md).
- **Architecture & AWS services:** the [top-level README](../../../README.md).
- **Deploying on AWS:** [`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md).
- **The custom DSQL sink connector:** [`connectors/dsql-sink/`](../../../connectors/dsql-sink).

Migrating from Aurora MySQL to Aurora DSQL is a **heterogeneous** move — a
different engine with different rules, not a version upgrade. This tool's job is
to make those differences explicit up front (Evaluation), handle them
deterministically where it can (Schema Conversion, Full Load, CDC), and prove the
result (Validation) — so your cut-over is a decision backed by evidence, not a
leap of faith.
