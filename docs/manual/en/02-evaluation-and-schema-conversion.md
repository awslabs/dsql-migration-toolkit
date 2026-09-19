# 2. Evaluation and Schema Conversion

_Language: **English** | [한국어](../ko/02-evaluation-and-schema-conversion.md) | [日本語](../ja/02-evaluation-and-schema-conversion.md)_

> **Prev:** [1. Set up](01-setup.md)

Between connecting and moving data are the two steps that make the migration
**deterministic and predictable** instead of trial-and-error:

```
Connect → [ 1. Evaluation → 2. Schema Conversion ] → 3. Data Migration → 4. Validation → 5. Cut over
```

- **Evaluation** answers *"what in my source database (MySQL or PostgreSQL) will and
  won't move to DSQL, and how much work is each piece?"*
- **Schema Conversion** turns the source DDL into DSQL-compatible DDL and applies
  it to the target.

Whatever your source engine (Aurora MySQL or Aurora PostgreSQL), **this is where you
learn what DSQL won't accept — before you've moved a single row.** Don't skip it.

---

## 2.1 Evaluation — the compatibility assessment

When you run **Evaluation**, the tool introspects the **source** (for a MySQL source:
tables, columns, types, primary keys, indexes, foreign keys, views, triggers, routines,
`AUTO_INCREMENT`, charset/collation; for a PostgreSQL source, the analogous objects across
**all non-system schemas** — schema-qualified table names, generated/identity columns and
sequences, `REPLICA IDENTITY`, etc., with the system schemas
`pg_catalog`/`information_schema`/`pg_toast` excluded) **and** the **target**, then produces
a **compatibility assessment report**.

### Every object gets one classification

| Class | Meaning | Examples |
|---|---|---|
| **AUTO** | Converts automatically, no human action. | Ordinary tables/columns with mappable types and a PK; **foreign keys** (converted automatically, flagged `RECOMMENDED` — see below). |
| **MANUAL** | Converts, but needs a decision or an app-side change. | Case-insensitive collation, partitioned tables, oversized LOB columns, `ENUM`/`SET`, generated columns, `ON UPDATE` timestamps, spatial/geometry types, multi-database sources, foreign keys with `CASCADE`/`SET NULL` actions (which CDC can't replicate). |
| **UNSUPPORTED** | No automatic conversion — redesign needed. | Triggers, stored procedures/functions, scheduled events, tables with no PK, `DECIMAL` precision > 38, > 255 columns/table, > 1000 tables/database, FULLTEXT/SPATIAL indexes. |

The examples above are drawn from a MySQL source. For a **PostgreSQL source** the same
scale applies: unsupported PG column types — arrays (`[]`), geometric
(`point`/`line`/`box`/…), network (`inet`/`cidr`/`macaddr`/`macaddr8`), `xml`, `money`,
`bit`/`bit varying`/`varbit`, `tsvector`/`tsquery`, range + multirange, `pgvector`,
`enum`, composite — are flagged **MANUAL**/**UNSUPPORTED** and **never auto-substituted**,
and the structural gates (no PK, `numeric` precision > 38, > 8-column keys, etc.) apply to
a PostgreSQL source equally.

Nothing is left unclassified — an object matched by no rule defaults to **AUTO**.
When several rules match one object, the **most demanding** classification wins
(`UNSUPPORTED` > `MANUAL` > `AUTO`) and the reasons/recommendations of all matched
rules are combined, so no finding is hidden.

### Gaps versus recommendations

Not every finding is a problem. A finding is one of two things, and the report marks
which:

- a **gap** — something could not be carried over or changed meaning (a dropped
  collation, an unreproducible `ON UPDATE` timestamp). You have to decide what to do
  about it.
- a **recommendation** (`RECOMMENDED`, info-blue) — the conversion is complete and
  correct; this is advice about *running well* on DSQL. Ignoring it costs performance,
  not correctness.

`AUTO_INCREMENT` (and, for a PostgreSQL source, the analogous `serial`/`IDENTITY`
`nextval` key) is the main recommendation. Such a key converts cleanly and works:
moving to a UUID/random or cached-identity key buys **insert throughput**, because DSQL
stores rows in primary-key order so a monotonic key concentrates writes on one partition
(see [Chapter 7 §7.1](07-performance-and-tuning.md#primary-key-strategy--avoid-hot-partitions)).

**Foreign keys** are the other advisory finding. Aurora DSQL **enforces** foreign keys,
so each one converts cleanly and is preserved — flagged `RECOMMENDED` with **no required
effort**, not a gap. The note carries the runtime caveats worth weighing: DML on a
referenced/referencing table incurs **extra reads** (AWS suggests benchmarking before you
add them), a concurrent conflict is a retryable serialization error (`SQLSTATE 40001`),
and a `CASCADE`/`SET NULL`/`SET DEFAULT` action counts toward DSQL's
3000-row-per-transaction limit (a cascade touching > 3000 rows fails) — so prefer
`NO ACTION`/`RESTRICT` where child cardinality is unbounded. A cascade action also can't
survive CDC (see [Chapter 6 §6.2](06-limitations.md#62-migration-process-limits)), which
is the one case Evaluation still flags **MANUAL**.

Because a recommendation is optional, **it does not count toward an object's estimated
effort** — otherwise nearly every table with a generated/monotonic key would be inflated
to `MEDIUM` by that key alone. The finding still shows what taking the advice would cost
("effort if you take it"), so the choice stays informed.

> **Identity keys and the loaded rows.** Whenever you choose an identity PK strategy —
> converting a MySQL `AUTO_INCREMENT` (or a PostgreSQL `serial`/`IDENTITY`) key to a
> cached identity (`GENERATED BY DEFAULT AS IDENTITY`) — the tool advances that sequence
> past the loaded rows when Full Load completes, and records it in the activity log
> (`identity sequences synced … RESTART WITH <max(pk)+1>`).
>
> This is not cosmetic. `BY DEFAULT` is what lets the load write your source's own key
> values — but an explicitly-supplied value does **not** advance the sequence, so without
> this step the sequence would still sit at its start while those values are already
> taken, and your application's **first insert after cut-over** would fail with a
> duplicate key. Row counts and checksums match in that state, so Validation passes
> clean and the problem only appears after you have frozen the source.
>
> It runs only after a **complete** load: `MAX(pk)` is the value the sequence has to
> clear, so syncing a partial load could still leave a collision for the rows yet to
> arrive. A retry that completes the load performs the sync. Tables with a plain integer
> key (the default) have no sequence and are left untouched.

### What each report item tells you

For every assessed object you get:

- its **classification** (AUTO / MANUAL / UNSUPPORTED),
- a **risk description** — *why* it's flagged (e.g. "Aurora DSQL has no
  `ON UPDATE CURRENT_TIMESTAMP` clause"),
- a **recommended action** — *what to do* (e.g. "set the timestamp explicitly on
  update in your application"), and
- for non-automatic items, an **effort estimate** (SCT-style buckets, e.g.
  simple / medium / significant) so you can size the work.

The report also rolls these up into **summaries**: counts per classification and
per effort level — your at-a-glance picture of how migratable the schema is.

### Target name-conflict detection (before you apply anything)

Evaluation also checks the **target**: if an object you're about to create already
exists on DSQL, that conflict is surfaced **now**, so Schema Conversion's apply
step won't fail mid-way on an unexpected clash. You decide up front whether to
**skip** or **replace** it.

### Optional AI assist (Amazon Bedrock) — what it is, what it can do, how to turn it on

AI assist is an **opt-in, off-by-default** augmentation that uses **Amazon
Bedrock** to help with the *human-decision* parts of conversion. It is covered
fully here because it's the one place the tool can call out to an LLM — so it's
worth understanding exactly what it does, what it's allowed to do, and what it
never touches.

**What it does when enabled:**

- In **Evaluation**, it adds a **strategy narrative** (a plain-language read of how
  migratable your schema is and where the work is).
- In **Schema Conversion**, it proposes **conversion suggestions** for the hard
  `MANUAL` / `UNSUPPORTED` items the deterministic converter flags (e.g. how to
  reshape an unsupported construct).

**What it never does (the safety model):**

- **It never replaces the deterministic path.** The deterministic source→DSQL
  converter always runs first (`sqlglot` for a MySQL source, the PG→DSQL converter for
  a PostgreSQL source); AI only *augments* it. With AI off (the default) the workflow
  behaves identically.
- **Every suggestion is review-only.** A suggestion can touch the target schema
  **only after you explicitly approve it** — pending, edited-but-unapproved, and
  rejected suggestions are all excluded from the apply path (a hard human-review
  gate). You can edit a suggestion before approving.
- **AI is never in the data path.** It only assists schema/strategy at design
  time; it never sees or touches Full Load or CDC **row data**.
- **Your source/target data is not sent.** Suggestions are built from schema/DDL
  metadata, not row values.

**The permission / deployment model:**

- **Amazon Bedrock is the only AI backend — there is no direct API-key entry.** The
  tool has **no field for an Anthropic/OpenAI (or any other) API key**; AI works
  *only* through Bedrock, authenticated with your AWS credentials. This keeps AI on
  the same IAM/credential model as everything else (no extra secret to store).
- AI assist calls Bedrock with the **`bedrock:InvokeModel`** permission only —
  nothing else. It uses the **same credentials as the rest of the tool** (the AWS
  profile you selected on Connect, or the environment/role credential chain).
- **Running locally:** your credential chain must allow `bedrock:InvokeModel` for
  the chosen model, and you must have **enabled that model** in the Bedrock console
  for your region.
- **Deployed on Fargate:** AI assist is **off unless you deploy with
  `EnableAiAssist=true`**, which grants the task role a **scoped**
  `bedrock:InvokeModel` (least privilege — it can invoke only the model(s) in scope).
  By default `BedrockModelArns` is empty and that scope is **auto-derived from
  `BedrockModelId`**, so you don't have to list anything; `BedrockModelArns` is
  **optional** and only overrides the auto-derived scope. You also set `BedrockRegion`
  and the task's egress must be able to reach the Bedrock runtime endpoint (NAT or
  a Bedrock VPC endpoint). See [`deploy/DEPLOYMENT.md`: "Enable AI-assisted conversion (optional)"](../../../deploy/DEPLOYMENT.md).
- **Model:** the default is `global.anthropic.claude-sonnet-5` (a general,
  cost-effective choice); override it with `BedrockModelId` (deploy) or in the
  Connect screen's Bedrock settings.

**How to turn it on:**

1. On the **Connect** screen, expand **AI Assist** and toggle **Enable AI Assist**
   (optionally set the model id / region under *Bedrock settings*).
2. Use the **Verify AI access** preflight — a non-blocking check that confirms the
   credentials can actually reach Bedrock (a valid AWS profile alone does **not**
   guarantee `bedrock:InvokeModel`), so you find a permission gap now, not mid-run.

> **Cost & latency:** enabling AI calls Bedrock, which **adds cost and some
> latency**. It's a convenience for the `MANUAL`/`UNSUPPORTED` long tail, not a
> requirement — the deterministic path migrates everything it can on its own.

### How to use the report

Work the list top-down by severity:

1. **Resolve every UNSUPPORTED item.** These block a clean migration. Add a PK
   where missing; move triggers/routines/events into the application (or
   EventBridge/Lambda); replace spatial types; reduce `DECIMAL` precision to ≤ 38;
   exclude or split oversized LOB columns.
2. **Decide every MANUAL item.** Drop partitioning, accept the default collation,
   handle `ENUM`/`SET`, decide `ON UPDATE` timestamps, replace a cascade action CDC
   can't replicate, etc.
3. **AUTO items** need nothing from you.

> **PostgreSQL source.** The same top-down triage applies. Remodel unsupported PG types
> before you convert — arrays → `jsonb` or a child table; `network`/`xml`/`bit`/range →
> `text`; `money` → `numeric`; `enum` → `text`; composite → separate columns or `jsonb`.
> The PG **CDC** prerequisite of a usable `REPLICA IDENTITY` on each replicated table
> belongs to the Data Migration step, not here (see
> [Chapter 4](04-cdc-and-dsql-constraints.md)).

> **Why this matters for DSQL specifically:** DSQL deliberately omits triggers,
> stored procedures, and several types, and enforces foreign keys with runtime
> caveats. Evaluation is the moment you find that out — cheaply, read-only, before
> any data moves — instead of discovering it as a failed load later.

---

## 2.2 Schema Conversion — generate and apply the DSQL DDL

With the assessment in hand, **Schema Conversion** turns the source schema into
DSQL-compatible DDL and applies it to the target. It's the SCT-like step:

- **Browse** the source/target object tree.
- **Compare** source DDL vs the converted DSQL DDL **side by side** for each
  object.
- **Apply** the converted DDL to the target — all objects at once with **Apply
  all**, or one object at a time with its own **Apply to target** button. For an
  object that already exists on the target, you choose **SKIP** (leave it) or
  **REPLACE** (drop and recreate); when you apply a single existing object the tool
  asks **Replace / Skip / Cancel** right then, so the choice is explicit — this is
  how you *re-apply* a table after changing its DDL, e.g. reverting a composite key
  back to the integer key (SKIP would leave the old table in place; REPLACE
  recreates it with the new key).

### What the conversion does for you

Conversion handles the dialect and constraint bridging automatically. For a **MySQL
source** it runs through `sqlglot` (MySQL → PostgreSQL dialect, then DSQL constraints);
for a **PostgreSQL source** it is a near-identity PG-16 → DSQL rebuild constructed from
the source's exact captured column types (`format_type` strings), not a `sqlglot`
MySQL→PostgreSQL transpile:

- **Type mapping** — for a **MySQL source**, the full MySQL → DSQL type table
  (`TINYINT(1)` → `boolean`, `BIT(n)` → integer, `ENUM` → `text` + `CHECK`,
  `BLOB`/`BINARY` → `bytea`, etc.; see
  [§2.3](#23-mysql--dsql-type-and-constraint-handling-reference) below). For a
  **PostgreSQL source** there is no such translation table — DSQL-supported column types
  pass through verbatim (see the §2.3 scope note).
- **Column defaults (MySQL source)** — a source `DEFAULT` is carried across (Aurora DSQL
  supports them), including `CURRENT_TIMESTAMP` **and its fractional `CURRENT_TIMESTAMP(n)`**,
  `NOW()`, `CURRENT_DATE`/`CURRENT_TIME`, and `LOCALTIME`/`LOCALTIMESTAMP`. Two
  translations happen for you:
  a `TINYINT(1)` default becomes `TRUE`/`FALSE` for the `boolean` target, and a
  `DATETIME` default is pinned to UTC so it matches the naive-UTC values the loader
  writes. This matters most for a **`NOT NULL` column with a default**: MySQL accepts
  an `INSERT` that omits it, and without the default the target would reject that same
  statement — an application break that would only show up after cut-over. Where a
  default genuinely has no DSQL equivalent (MySQL's `UUID()` translates, but an
  expression referencing another column does not), it is dropped and **reported** rather
  than silently lost. `ON UPDATE CURRENT_TIMESTAMP` cannot be reproduced at all — DSQL
  has no `ON UPDATE` clause and no triggers — so it is flagged **MANUAL** for the
  application to handle. **For a PostgreSQL source** the converter emits **no** column
  DEFAULTs at all — including `serial`/`IDENTITY` `nextval` and generated-column
  expressions — so the chosen primary-key strategy governs identity on the target
  instead, and `STORED` generated columns are created as ordinary columns.
- **Foreign-key preservation** — Aurora DSQL **enforces** foreign keys, so each
  source FK is kept **out of the `CREATE TABLE`** and rendered as a separate
  post-load `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY` statement. Full Load
  re-creates them as a run-level pass **after** the data has loaded; for a **CDC**
  migration the apply is **deferred to cut over** (the constraints must not exist
  while the sink streams rows out of parent-before-child order — an FK violation
  would be dead-lettered, `SQLSTATE 23503`), and the rendered DDL is shown here so
  you can apply it at cut over. You can instead **opt to strip** the foreign keys
  (`SchemaConvertOptions.preserve_foreign_keys=False`) and enforce referential
  integrity in the application layer.
- **Primary-key strategies** — keep the integer PK, convert to UUID, or use an
  identity column with caching (to avoid hot-partition contention on a
  monotonic key). Each table's card also has a **primary-key picker** to switch it
  to a **composite key** (a high-cardinality column prepended to the original key,
  e.g. `(customer_id, id)`) — the one strategy that spreads writes across DSQL
  partitions and moves a write hot-partition wall. It validates the choice against
  DSQL's key limits and keeps the original key unique via a `CREATE UNIQUE INDEX
  ASYNC`; see [Chapter 7 §7.1](07-performance-and-tuning.md#primary-key-strategy--avoid-hot-partitions)
  for when and why to use it.
- **Indexes as `CREATE INDEX ASYNC`** — DSQL builds secondary indexes
  asynchronously, after data.
- **One DDL per transaction** — conversion emits each DDL statement as its own
  execution unit (DSQL allows one DDL per transaction), and the apply path retries
  optimistic-concurrency conflicts (`40001`) idempotently.
- **Views** are transpiled to PostgreSQL `CREATE VIEW`; a definition that can't be
  parsed becomes a clearly-marked placeholder flagged for manual work rather than
  silently dropped.

### Foreign keys — enforced on DSQL, but applied *after* the data loads

Aurora DSQL **enforces** foreign keys, so the tool **preserves** them by default (the
**Preserve foreign keys** toggle above the DDL). But a foreign key is never part of
the `CREATE TABLE` and is **never applied during Schema Apply** — it is rendered as a
separate **post-load** statement:

```sql
ALTER TABLE "orders" ADD CONSTRAINT "fk_customer"
  FOREIGN KEY ("customer_id") REFERENCES "customers" ("id") NOT VALID;
```

**Why it is deferred.** The bulk load writes parent and child tables **concurrently,
in per-table primary-key order, with no parent-before-child sequencing**, so a child
row can be written before its parent exists. If the foreign key were already enforced
that write would fail (`SQLSTATE 23503`); and for a **CDC** migration the sink applies
row changes **out of order across tables**, so an enforced FK during the stream would
**dead-letter** those rows. The tool therefore adds the constraints only once the data
is in a consistent state.

**When they are applied:**

- **Full Load only** — automatically, as a run-level pass **at the end of the load**.
  The activity log records an `apply foreign keys` step (`N applied, S skipped,
  F failed`).
- **CDC** — **deferred to cut over**. After the final drain and **before** you repoint
  the application, click **Apply foreign keys** in the cut-over runbook (Step 4). It is
  idempotent — safe to re-run.

`NOT VALID` is used because it is the only `ADD CONSTRAINT` form DSQL accepts: it adds
the constraint **without scanning existing rows** and **enforces every new write
immediately**; the already-loaded rows are then checked by a background
`ALTER TABLE ASYNC … VALIDATE CONSTRAINT`.

**Orphan pre-gate.** Before adding each FK the tool counts child rows whose parent is
missing. If any exist, that FK is **skipped with an actionable note** (table / foreign
key / row count) instead of failing with an opaque error — resolve the orphans (often
an un-replicated source cascade) and click **Apply foreign keys** again.

**In the preview.** Because the FKs run later and separately, the generated target
script shows them in their own read-only **"Foreign keys — applied after Full Load"**
section (not in the editable `CREATE` box), and the Apply results say the foreign keys
were deferred. **A target with no foreign keys immediately after Schema Apply is
expected, not a failure** — they appear at load end (Full Load) or at cut over (CDC).

**Cascading actions are a CDC gap.** MySQL performs `ON DELETE`/`ON UPDATE CASCADE`
(and `SET NULL`) *inside the engine*, and those cascaded child changes never reach the
binary log — so CDC replicates the parent change but **not** the cascade, which can
leave orphaned or stale child rows. Such FKs are flagged **MANUAL** at Evaluation;
replace the automatic action with explicit child-row statements, or quiesce source
writes before the final comparison (see
[Chapter 4](04-cdc-and-dsql-constraints.md)).

**Prefer to enforce integrity in the application?** Turn **Preserve foreign keys**
off. The tool then strips the FK DDL from every path (the relationship is still kept as
metadata for Validation's orphan check). This choice is remembered across a reconnect /
instance restart, and the deferred-FK DDL itself is re-derived from the read-only
source, so a crash never loses it. Note that enforced FKs add a per-write read cost and
use commit-time optimistic concurrency (retryable `40001`), and that
`CASCADE`/`SET NULL`/`SET DEFAULT` rows count toward DSQL's 3,000-rows-per-transaction
limit.

### Query (DML) conversion and anti-pattern linting

Beyond schema, the tool can convert queries and **lint your application's SQL** for
patterns DSQL won't accept or that won't scale — e.g. `SELECT ... FOR UPDATE`
(pessimistic locking against DSQL's optimistic concurrency), `AUTO_INCREMENT`
assumptions, trigger/stored-procedure calls, and unsupported functions. Use this to
find code that needs changing **before** cut-over.

### The end state

After Schema Conversion, the **DSQL target schema is fixed**. This is the schema
Full Load writes into and the schema CDC streams into — and, importantly, CDC does
**not** propagate later source DDL, so if you change the source schema during CDC
you must re-apply the equivalent DDL here yourself (see
[Chapter 4 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important)).

## 2.3 MySQL → DSQL type and constraint handling (reference)

This is what Schema Conversion and the data path do to bridge the dialects. It's
the same mapping the Full Load value converter and the CDC sink both honor (a
shared "write contract" keeps them identical).

> **Scope: this reference describes a MySQL source.** For a **PostgreSQL source**,
> conversion and the data path are a near-identity PG-16 → DSQL **pass-through** —
> DSQL-supported types carry verbatim, with **no** MySQL-style type translation.
> DSQL-supported PG types pass unchanged (int family; `numeric`/`decimal`;
> `real`/`double precision`; `char`/`varchar`/`text`/`bpchar`;
> `date`/`time`/`timetz`/`timestamp`/`timestamptz`; `interval`; `boolean`; `bytea`;
> `uuid`; `json`; `jsonb`). A different **UNSUPPORTED** set is flagged
> **MANUAL**/**UNSUPPORTED** at both Evaluation and Schema Conversion and is **never
> auto-substituted**: arrays (`[]`), geometric
> (`point`/`line`/`lseg`/`box`/`path`/`polygon`/`circle`), network
> (`inet`/`cidr`/`macaddr`/`macaddr8`), `xml`, `money`, `bit`/`bit varying`/`varbit`,
> `tsvector`/`tsquery`, range + PG14 multirange, `pgvector`, `enum`, composite. The PG
> numeric rules match the table below: `numeric(p,s)` with `p > 38`/`s > 37` is clamped
> with a warning, and a bare `numeric`/`decimal` becomes `numeric(18,6)`. Column
> DEFAULTs, `serial`/`IDENTITY` `nextval`, and generated-column expressions are **not**
> emitted (the primary-key strategy governs identity), and `STORED` generated columns
> become ordinary columns.

### Type mapping (complete reference)

Every MySQL data type below is what Schema Conversion emits as the target DDL
type **and** how the value is stored on Aurora DSQL. Both migration paths honor
the same mapping — the Full Load bulk loader (Python) and the CDC sink (Java) —
enforced by a shared **write-contract** parity test, so the same source row lands
identically whichever path migrates it. Class: **AUTO** = automatic, lossless;
**MANUAL** = converts but review/decision needed; **UNSUPPORTED** = no automatic
conversion (redesign).

#### Integer types

| MySQL type | Aurora DSQL type | Stored value form | Class | Note |
|---|---|---|---|---|
| `TINYINT` | `smallint` | `smallint` | AUTO | Signed 8-bit. |
| `TINYINT(1)` | `boolean` | `boolean` (`true`/`false`) | MANUAL | MySQL boolean convention; `0/1`→`false/true`. A value **outside `{0,1}` fails loudly** (no silent flatten). |
| `SMALLINT` | `smallint` | `smallint` | AUTO | Signed 16-bit. |
| `MEDIUMINT` | `integer` | `integer` | AUTO | PostgreSQL has no 3-byte int; `integer` covers the signed 24-bit range. |
| `INT` / `INTEGER` | `integer` | `integer` | AUTO | Signed 32-bit. |
| `BIGINT` | `bigint` | `bigint` | AUTO | Signed 64-bit. |
| `TINYINT UNSIGNED` | `smallint` | `smallint` | AUTO | Widened to preserve `0..255`. |
| `SMALLINT UNSIGNED` | `integer` | `integer` | AUTO | Widened to preserve `0..65535`. |
| `MEDIUMINT UNSIGNED` | `integer` | `integer` | AUTO | Widened to preserve `0..16M`. |
| `INT UNSIGNED` | `bigint` | `bigint` | AUTO | Widened to preserve `0..4.29B`. |
| `BIGINT UNSIGNED` | `numeric(20,0)` | `numeric(20,0)` | AUTO | No wider integer exists; full `2^64-1` range preserved. (CDC needs `bigint.unsigned.handling.mode=precise`.) |
| `INT(11)`, `BIGINT(20)`, … (display width) | bare `smallint`/`integer`/`bigint` | `smallint`/`integer`/`bigint` | AUTO | The `(N)` display width is **dropped** (cosmetic in MySQL; PostgreSQL integers take no width). |
| `BIT(n)` | `smallint` (n≤15) / `integer` (≤31) / `bigint` (≤63) / `numeric(20,0)` (64) | `smallint`/`integer`/`bigint`/`numeric(20,0)` | MANUAL | DSQL has **no `BIT` type**; the bit pattern is stored as the unsigned integer it represents. |

#### Fixed-point & floating-point

| MySQL type | Aurora DSQL type | Stored value form | Class | Note |
|---|---|---|---|---|
| `DECIMAL(p,s)` / `NUMERIC(p,s)` | `numeric(p,s)` | `numeric(p,s)` | AUTO | Precision/scale preserved. **Precision > 38** → Evaluation flags it **UNSUPPORTED** (DSQL caps NUMERIC at 38); Schema Conversion still emits DDL by **clamping to `numeric(38,37)`** with a data-loss warning (scale is also capped at 37). |
| `DECIMAL(p,s) UNSIGNED` | `numeric(p,s)` | `numeric(p,s)` | AUTO | Unsigned-ness is not representable and carries no storage meaning. |
| `FLOAT` | `real` | `real` | AUTO | Single-precision float. |
| `FLOAT(M,D)` | `real` | `real` | AUTO | The `(M,D)` display spec is dropped (PostgreSQL `float` takes one precision, not a scale). |
| `DOUBLE` / `DOUBLE UNSIGNED` | `double precision` | `double precision` | AUTO | Double-precision float. |

#### Date & time

| MySQL type | Aurora DSQL type | Stored value form | Class | Note |
|---|---|---|---|---|
| `DATE` | `date` | `date` | AUTO | |
| `DATETIME` | `timestamp` (without time zone) | `timestamp` (UTC wall-clock) | AUTO | Treated/normalized as **UTC**. |
| `DATETIME(6)` | `timestamp` | `timestamp` (UTC, microsecond precision) | AUTO | Fractional seconds preserved to microseconds. |
| `TIMESTAMP` | `timestamptz` | `timestamptz` (UTC instant) | AUTO | Stored as an absolute UTC instant. |
| `TIME` | `time` (without time zone) | `time` | MANUAL | In-range `00:00:00..23:59:59`. An **out-of-range** MySQL `TIME` (negative or `> 24h`, MySQL range `-838:59:59..838:59:59`) has no `time` representation → **fails loudly** (needs an `interval` column instead). |
| `YEAR` | `smallint` | `smallint` (integer year) | MANUAL | DSQL has no `YEAR` type; `1901–2155` fits `smallint`, stored as the integer year (`YEAR` display semantics not preserved). |

#### Strings, binary, and structured

| MySQL type | Aurora DSQL type | Stored value form | Class | Note |
|---|---|---|---|---|
| `CHAR(n)` | `char(n)` | `char(n)` | AUTO | |
| `VARCHAR(n)` | `varchar(n)` | `varchar(n)` | AUTO | |
| `TINYTEXT`/`TEXT`/`MEDIUMTEXT`/`LONGTEXT` | `text` | `text` | AUTO | A single value **> ~1 MiB** is rejected by DSQL → per-row quarantine (Full Load) / DLQ (CDC); flag oversized LOB columns at Evaluation. |
| `CHAR`/`VARCHAR`/`TEXT` with `COLLATE` (e.g. `utf8mb4_*_ci`) | same, **collation dropped** | `text` (collation dropped) | MANUAL | DSQL uses its default collation; a case-insensitive collation is not preserved → flagged MANUAL. |
| `BINARY(n)` / `VARBINARY(n)` | `bytea` | `bytea` (raw bytes) | AUTO | The length modifier is dropped (PostgreSQL `bytea` takes none). |
| `TINYBLOB`/`BLOB`/`MEDIUMBLOB`/`LONGBLOB` | `bytea` | `bytea` (raw bytes) | AUTO | Binary payload preserved byte-for-byte. |
| `ENUM('a','b',…)` | `text` + `CHECK (col IN ('a','b',…))` | `text` | MANUAL | DSQL has no `ENUM`; ordering semantics not preserved. |
| `SET('x','y',…)` | `text` | `text` (comma-joined) | MANUAL | No lossless mapping; multi-value set semantics handled in the app. |
| `JSON` | `json` | `json` | AUTO | (CDC wraps the JSON text in a `PGobject(type=json)` so it targets the `json` column.) |
| spatial (`GEOMETRY`/`POINT`/`LINESTRING`/…) | `bytea` | `bytea` (raw WKB bytes) | MANUAL | DSQL has no spatial type; the data is **preserved** as raw WKB bytes (Full Load reads `ST_AsBinary(col)`, CDC extracts Debezium geometry's `.wkb`; **SRID is dropped**). The `geometry` *column type* itself is flagged MANUAL (auto-substituted to `bytea`, WKB preserved), so the values are not lost. |

> **A `bytea` column cannot be a key on DSQL.** Any column that maps to `bytea`
> (`BINARY`/`VARBINARY`, `*BLOB`, or spatial) **cannot** be part of a primary key or
> index: a PK over such a column makes the table **UNSUPPORTED**, and a secondary
> index on it is **dropped (MANUAL)**. Re-key as `text`/`uuid` or a hash column. See
> the Structural constraints below.

### Structural constraints

| DSQL rule | What the tool does |
|---|---|
| **Foreign keys are enforced** | Aurora DSQL supports **enforced** foreign keys. Each source FK is kept out of the `CREATE TABLE` and re-created as a post-load `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY` (Full Load applies it after the data lands; a CDC migration defers it to cut over). Runtime caveats: DML on referenced/referencing tables incurs **extra reads**, a concurrent conflict is a retryable `40001`, and `CASCADE`/`SET NULL`/`SET DEFAULT` actions count toward the 3000-row-per-transaction limit — prefer `NO ACTION`/`RESTRICT` for unbounded child cardinality. You may instead strip them (`preserve_foreign_keys=False`) and enforce integrity in the app. |
| **Primary key required** | A table with no PK is flagged **UNSUPPORTED** (and can't be loaded). |
| **No `TRUNCATE`** | "Replace" loads use **DROP + recreate**, never `TRUNCATE`. |
| **One DDL per transaction** | Schema conversion emits exactly one DDL statement per execution unit. |
| **`CREATE INDEX ASYNC`** | Secondary indexes are created asynchronously, after data. |
| **Optimistic concurrency** | Every batch and DDL is wrapped in `40001` retry. |
| **Column defaults ARE supported** | **MySQL source:** a source `DEFAULT` is carried across, including `CURRENT_TIMESTAMP[(n)]`, `NOW()`, `CURRENT_DATE`/`CURRENT_TIME`, and `LOCALTIME`/`LOCALTIMESTAMP`. `TINYINT(1)` defaults become `TRUE`/`FALSE` (a `boolean` column rejects `DEFAULT 1`), a `DATETIME` default is pinned to UTC to match the loader's naive-UTC values, and function defaults are translated (`UUID()`→`gen_random_uuid()`, `CURDATE()`→`CURRENT_DATE`, `CURTIME()`→`CURRENT_TIME`, `UTC_TIMESTAMP()`/`UTC_DATE()`→UTC expressions). A default with no DSQL equivalent (e.g. one referencing another column) is dropped and **reported**, never silently lost. Keeping the default matters most on a `NOT NULL` column: MySQL accepts an `INSERT` that omits it, the target would not. **PostgreSQL source:** the converter emits **no** column defaults — `serial`/`IDENTITY` `nextval` and generated-column expressions are stripped — and identity is governed by the chosen PK strategy on the target instead. |
| **No `ON UPDATE CURRENT_TIMESTAMP`** | Unreproducible: DSQL has neither an `ON UPDATE` clause nor triggers (the usual PostgreSQL workaround). The column keeps its insert-time default and is flagged **MANUAL** — set the timestamp explicitly on update in your application. |
| **No triggers / stored procedures / events** | Flagged **UNSUPPORTED** — reimplement in the application (or EventBridge/Lambda for scheduled events). |
| **No native partitioning** | DSQL auto-distributes; partitioned tables are flagged MANUAL. |
| **One database per cluster** | A multi-database source is flagged MANUAL (consolidate into schemas or split clusters). |
| **Source `CHECK` constraints are dropped** | A source `CHECK` constraint (a MySQL `CHECK` requires 8.0.16+) is **not** carried to the target — its functions/operators may differ on DSQL — and is flagged **MANUAL**: re-add a DSQL-compatible `CHECK` by hand or enforce it in the app. (The `CHECK … IN (...)` the tool *generates* for an `ENUM` is unaffected.) |
| **`bytea` cannot be a key** | A `bytea` column (from `BINARY`/`VARBINARY`, `*BLOB`, or spatial) cannot be a **primary-key** column (table → **UNSUPPORTED**) or an **index** column (index dropped → **MANUAL**). Re-key as `text`/`uuid`/hash. |
| **≤ 8 columns per key** | A primary key or index over more than 8 columns exceeds DSQL's limit: the PK case is **UNSUPPORTED**, an over-wide secondary index is **dropped (MANUAL)**. |
| **≤ 24 indexes per table** | DSQL allows at most 24 indexes per table (the PK counts, so ≤ 23 secondary). Excess indexes are flagged and not all emitted. |
| **≤ 1 KiB key value** | The combined key value must stay under ~1 KiB; a wide key is flagged as a **recommendation** (it can fail at insert time on real data). |
| **63-byte identifiers** | DSQL/PostgreSQL truncate identifiers to 63 bytes; where truncation would collide two names, the object is flagged **UNSUPPORTED** (rename to keep the first 63 bytes unique). |

The **Class** column above uses the same AUTO / MANUAL / UNSUPPORTED scale Evaluation
applies to every object — see [§2.1](#every-object-gets-one-classification) for what each
one means and how the most demanding class wins when several rules match.

---

**Next:** [3. Full Load →](03-full-load.md)
