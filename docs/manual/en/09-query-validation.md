# 9. The Query Converter and the AI DBA

_Language: **English** | [한국어](../ko/09-query-validation.md) | [日本語](../ja/09-query-validation.md)_

> **Prev:** [8. Testing and verification](08-testing-and-verification.md)

Migrating the schema and the data is only part of the story — your application's
**queries** have to run on Aurora DSQL too, and run *well*. The **Query Converter**
(an optional tool in the sidebar) is where you convert a single MySQL query to
Aurora DSQL, test it read-only on the target, and — with AI assist on — have an
**AI DBA** rewrite it for DSQL efficiency and prove the improvement.

This chapter covers that workflow. It never writes to the source, and it never
executes DML against the target.

---

## 9.1 Convert a query

Paste a MySQL statement and the tool converts its MySQL idioms to Aurora DSQL
(PostgreSQL) with the same deterministic-first engine used for schema conversion
(`sqlglot`) — the Convert step parses the input as **MySQL**, so it is
MySQL-source-specific — then classifies it:

- **AUTO** — converted deterministically; ready to test.
- **MANUAL** — converted, but review it (an idiom that has a caveat on DSQL).

It also **rewrites** MySQL idioms and **flags** ones that matter on DSQL — e.g.
`ON DUPLICATE KEY UPDATE` → `INSERT ... ON CONFLICT DO UPDATE` (MANUAL — you confirm
the conflict target), `JSON_UNQUOTE(JSON_EXTRACT(...))` → `JSON_EXTRACT_PATH_TEXT`,
MySQL `HAVING`-alias references inlined, and `SELECT ... FOR UPDATE` (which behaves
differently under DSQL's optimistic concurrency). Every such finding is classified
**MANUAL**. For the broader application-code anti-pattern scan (monotonic keys —
`AUTO_INCREMENT`, serial / `IDENTITY` — / trigger reliance, unsupported functions)
see [Chapter 2](02-evaluation-and-schema-conversion.md).

> **PostgreSQL source.** Convert transpiles **MySQL** idioms, so the rewrites above
> (`ON DUPLICATE KEY UPDATE`, `JSON_UNQUOTE`/`JSON_EXTRACT`, MySQL `HAVING`-alias
> inlining) don't apply to a PostgreSQL source — there, application SQL is already
> PostgreSQL and runs on Aurora DSQL near-identically (PG → DSQL is a near-identity
> dialect), so most queries are already valid DSQL. Skip straight to **Test on
> target** and the **AI DBA**, which are engine-neutral. DSQL execution-model
> caveats such as `SELECT ... FOR UPDATE` under optimistic concurrency still apply
> to PostgreSQL-source queries too.

The original and the converted SQL are shown side by side so you can see exactly
what changed.

---

## 9.2 Test on target (read-only)

For a converted **SELECT**, **Test on target** plans it on your verified DSQL target
with `EXPLAIN` — the query is planned but **not executed**, so no rows are read. Turn
on the **EXPLAIN ANALYZE** toggle to actually run the (read-only) query and capture
real timings, row counts, and Aurora DSQL's per-statement **DPU cost estimate**.

What can and can't be tested:

- **SELECT** → `EXPLAIN` (plan only) or `EXPLAIN ANALYZE` (executes read-only).
- **DDL** → a dry run inside a transaction that is **rolled back** (never committed).
- **DML** (INSERT/UPDATE/DELETE) → **never executed** against the target.

The verdict shows whether DSQL accepted the statement, the exact error (with
SQLSTATE) if it didn't, the captured query plan, and — with ANALYZE — the DPU cost.

**Which schema is tested.** An unqualified table name (`FROM orders`) resolves
against a schema via the session `search_path`. For a **MySQL** source — where a
database *is* a schema — the tool defaults to the connected database's same-named
DSQL schema, the natural default. For a **PostgreSQL** source a database holds many
schemas and tables are schema-qualified (e.g. `public.orders`), so the
same-named-database default usually won't resolve — use the **Test against schema**
picker to select the actual target schema (e.g. `public`). A `relation "…" does not
exist` (SQLSTATE **42P01**) simply means the table isn't in the tested schema —
pick the right one and re-test.

> **Reading a DSQL plan:** Aurora DSQL is a *distributed* PostgreSQL-compatible
> engine, so its plans read a little differently — see §9.4.

---

## 9.3 Tune a query with the AI DBA

When **AI assist is enabled** (Connect screen; off by default), a **Tune with AI DBA**
button appears — but only after a converted SELECT has **passed Test on target**. The
AI needs the query's real execution plan to give grounded advice instead of guesses,
so testing first is required. Run the test with **EXPLAIN ANALYZE** on so a **DPU
baseline** is captured; the button then shows the current cost (e.g. *now ≈ 0.03
DPU*) and the AI can later prove how much a rewrite saves.

Clicking it opens the persistent app-wide AI panel and sets its scope to this query,
grounded on **this query's real EXPLAIN plan and DPU** plus Aurora DSQL's execution
model. The AI:

- proposes a rewritten query in a code block,
- explains **what it changed and why it is cheaper on DSQL** (which scan type or
  filter layer improved, and why fewer bytes cross from storage to compute), and
- keeps the query's **results identical** — it is told never to change semantics to
  make a query faster.

It is deliberately steered *away* from vanilla-PostgreSQL tuning advice that does not
apply to DSQL (no `VACUUM`/`REINDEX`, fillfactor, planner GUCs, or "lower the
`cost=` number" reasoning).

### Prove it: re-test the rewrite

Under each proposed rewrite there is a **Test rewrite on target** action. The tool
extracts the exact SELECT from the reply, re-runs it read-only on the target with
`EXPLAIN ANALYZE`, and feeds the measured **before/after DPU** back into the same
chat — so the **AI reports the actual improvement** (and says so honestly if the
rewrite didn't help). The measured DPU, not the model's prose, is the proof.

> **Advisory only.** Nothing is auto-applied. You copy the rewrite back into the
> editor and re-run Convert / Test yourself — the human-review gate. AI assist stays
> opt-in, on the control plane, and never touches the data path.

### Review or fix a query with the AI DBA

Separately from **Tune** (which is about *cost*), a **Review with AI DBA** button —
relabelled **Fix with AI DBA** when the target *rejected* the converted statement —
is a *correctness* action: it asks the AI whether the conversion is right and, on a
rejection, why DSQL refused it (grounded on the exact error + SQLSTATE) and how to
rewrite it. Unlike Tune, it needs **no passed test** — reach for it the moment a
conversion looks wrong or a Test on target fails. It is advisory only, same as Tune.

---

## 9.4 Why DSQL query tuning is different from PostgreSQL

Aurora DSQL is PostgreSQL-compatible on the wire, but it executes queries as a
*distributed* engine, so a few facts change how you make a query efficient. The AI
DBA is grounded on these; they are also useful to know when reading a plan yourself.

- **The primary key *is* the table.** Every table is a B-tree organized by its
  primary key — there is no separate heap. A table with no usable index for the
  predicate is read with a **Full Scan** (not a "Seq Scan"). Range/equality filters
  on the primary key are physically sequential and inherently cheap, so **primary
  key choice matters far more than in PostgreSQL**.
- **Compute and storage are separated.** Every row that crosses from storage to
  compute costs latency and **DPU** (Distributed Processing Unit — DSQL's cost unit,
  shown in `EXPLAIN ANALYZE VERBOSE`; the PostgreSQL `cost=` number is not the
  goal). **Pushing filters down** so fewer bytes move is the main lever.
- **Three filter layers, best to worst:** (1) *Index Condition* — an equality/range
  predicate on an indexed key column; (2) *Storage Filter* — a non-key column added
  to an index `INCLUDE` clause so storage filters before transfer; (3) *Query
  Processor Filter* — shown as a top-level `Filter:` line, where all unfiltered data
  has already crossed the network. Move predicates 3 → 2 → 1.
- **Scan types, cheapest last:** **Full Scan** (add a PK or an index) → **Index
  Scan** (a `Storage Lookup` node means an incomplete covering index — add the
  missing columns to `INCLUDE`) → **Index Only Scan** (ideal).

Common DSQL-appropriate rewrites the AI DBA will suggest: project only the columns
you need instead of `SELECT *`; avoid a leading-wildcard `LIKE '%x%'` (it can't use
an index); add a **redundant join predicate** the optimizer can't infer across a
join; use **CTE late materialization** for `ORDER BY … LIMIT`; add `INCLUDE` columns
to make an index covering; and prefer randomly-distributed keys (UUID) over
monotonic ones (auto-increment, serial / `IDENTITY` / sequence `nextval`,
timestamps) that create hot partitions.

---

## 9.5 The AI DBA — your app-wide assistant

The **Tune** / **Review** actions above open the same assistant that lives on
**every** step: the **AI DBA** panel (header **AI DBA** button). It's the same
Bedrock-only AI assist you enable on Connect (**off by default**), surfaced as a
persistent, session-backed right-side panel — its conversation and scope survive
moving between steps, a browser refresh, and even an app restart.

What makes it more than a chatbot:

- **Ask anything about *this* migration.** A general chat mode answers grounded on
  your actual run, not generic docs.
- **Read-only diagnostic tools.** To answer, the model calls tools that pull the
  migration's real state — never row values, never credentials. It can diagnose a
  **Full Load** failure (which tables failed and why), explain **why CDC isn't
  streaming** or **why a CDC deploy failed** and triage the **DLQ** (sample
  dead-lettered records by table + SQLSTATE), report **prerequisite verdicts** (what's
  blocking CDC and how to fix it), explain a **Validation** mismatch, and read the
  **target DSQL** catalog (tables, schema, row counts).
- **Cross-step context.** Major actions mirror into the panel as an activity feed, a
  **live Full Load progress card stays pinned and keeps updating** while you work
  elsewhere, and recent actions ground each answer.
- **"What's next?" briefing.** A header action gives a tool-grounded read of what to
  do next and the top risks right now.
- **Scoped deep-links.** Each step offers a one-click "ask about this" into the panel
  — per-failed-table / per-quarantine help on Full Load, drift + DLQ triage on CDC,
  "Explain this mismatch" on Validation, and a **GO / HOLD "is it safe to cut over?"**
  read on Cut over.

**Safety — the same model as all AI assist:** off by default; Bedrock-only (no API-key
entry); the tools are strictly read-only and see **schema / status / DDL / plan
metadata — never Full Load or CDC row data, never credentials**; nothing it suggests is
applied without your explicit action. See
[§2.1](02-evaluation-and-schema-conversion.md) for the permission model and how to
enable it.

---

## 9.6 Where to go next

- **Tuning the data path (parallelism):** [Chapter 7 — Performance and tuning](07-performance-and-tuning.md).
- **Conclusion and cut-over:** [Chapter 10 — Conclusion](10-conclusion.md).
- **Common questions:** [Chapter 11 — Customer FAQ](11-customer-faq.md).

---

**Next:** [10. Conclusion →](10-conclusion.md)
