# 2. Evaluation and Schema Conversion

_Language: **English** | [한국어](../ko/02-evaluation-and-schema-conversion.md)_

> **Prev:** [1. Set up](01-setup.md)

Between connecting and moving data are the two steps that make the migration
**deterministic and predictable** instead of trial-and-error:

```
Connect → 1. Migration plan → [ 2. Evaluation → 3. Schema Conversion ] → 4. Data Migration → 5. Validation → 6. Cut over
```

- **Evaluation** answers *"what in my MySQL database will and won't move to DSQL,
  and how much work is each piece?"*
- **Schema Conversion** turns the source DDL into DSQL-compatible DDL and applies
  it to the target.

For an Aurora MySQL user, **this is where you learn what DSQL won't accept —
before you've moved a single row.** Don't skip it.

---

## 2.1 Evaluation — the compatibility assessment

When you run **Evaluation**, the tool introspects the **source** (tables, columns,
types, primary keys, indexes, foreign keys, views, triggers, routines,
`AUTO_INCREMENT`, charset/collation) **and** the **target**, then produces a
**compatibility assessment report**.

### Every object gets one classification

| Class | Meaning | Examples |
|---|---|---|
| **AUTO** | Converts automatically, no human action. | Ordinary tables/columns with mappable types and a PK. |
| **MANUAL** | Converts, but needs a decision or an app-side change. | Foreign keys, `AUTO_INCREMENT`, case-insensitive collation, partitioned tables, oversized LOB columns, `ENUM`/`SET`, generated columns, `ON UPDATE` timestamps, multi-database sources. |
| **UNSUPPORTED** | No automatic conversion — redesign needed. | Triggers, stored procedures/functions, scheduled events, tables with no PK, spatial/geometry types, `DECIMAL` precision > 38, > 255 columns/table, > 1000 tables/database, FULLTEXT/SPATIAL indexes. |

Nothing is left unclassified — an object matched by no rule defaults to **AUTO**.
When several rules match one object, the **most demanding** classification wins
(`UNSUPPORTED` > `MANUAL` > `AUTO`) and the reasons/recommendations of all matched
rules are combined, so no finding is hidden.

### What each report item tells you

For every assessed object you get:

- its **classification** (AUTO / MANUAL / UNSUPPORTED),
- a **risk description** — *why* it's flagged (e.g. "Aurora DSQL does not support
  foreign keys"),
- a **recommended action** — *what to do* (e.g. "enforce this relationship in the
  application layer"), and
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

- **It never replaces the deterministic path.** The `sqlglot` MySQL→DSQL
  conversion always runs first; AI only *augments* it. With AI off (the default)
  the workflow behaves identically.
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
  `bedrock:InvokeModel` limited to the model ARN(s) you list in `BedrockModelArns`
  (least privilege — it can invoke only those models). You also set `BedrockRegion`
  and the task's egress must be able to reach the Bedrock runtime endpoint (NAT or
  a Bedrock VPC endpoint). See [`deploy/DEPLOYMENT.md` §9](../../../deploy/DEPLOYMENT.md).
- **Model:** the default is `us.anthropic.claude-sonnet-4-6` (a general,
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
2. **Decide every MANUAL item.** Choose how to enforce removed foreign keys in the
   app, drop partitioning, accept the default collation, handle `ENUM`/`SET`, etc.
3. **AUTO items** need nothing from you.

> **Why this matters for DSQL specifically:** DSQL deliberately omits foreign
> keys, triggers, stored procedures, and several types. Evaluation is the moment
> you find that out — cheaply, read-only, before any data moves — instead of
> discovering it as a failed load later.

---

## 2.2 Schema Conversion — generate and apply the DSQL DDL

With the assessment in hand, **Schema Conversion** turns the source schema into
DSQL-compatible DDL and applies it to the target. It's the SCT-like step:

- **Browse** the source/target object tree.
- **Compare** source DDL vs the converted DSQL DDL **side by side** for each
  object.
- **Apply** the converted DDL to the target, choosing **SKIP** or **REPLACE** for
  objects that already exist (driven by the conflict detection from Evaluation).

### What the conversion does for you

Conversion (via `sqlglot`, MySQL → PostgreSQL dialect, then DSQL constraints)
handles the dialect and constraint bridging automatically:

- **Type mapping** — the full MySQL → DSQL type table (`TINYINT(1)` → `boolean`,
  `BIT(n)` → integer, `ENUM` → `text` + `CHECK`, `BLOB`/`BINARY` → `bytea`, etc.;
  see [Chapter 4 §4.6](04-cdc-and-dsql-constraints.md#46-mysql--dsql-type-and-constraint-handling-reference)).
- **Foreign-key removal** — FKs are stripped from the DDL but **preserved in the
  report**, with a note to enforce referential integrity in the application.
- **Primary-key strategies** — keep the integer PK, convert to UUID, or use an
  identity column with caching (to avoid hot-partition contention on a
  monotonic key).
- **Indexes as `CREATE INDEX ASYNC`** — DSQL builds secondary indexes
  asynchronously, after data.
- **One DDL per transaction** — conversion emits each DDL statement as its own
  execution unit (DSQL allows one DDL per transaction), and the apply path retries
  optimistic-concurrency conflicts (`40001`) idempotently.
- **Views** are transpiled to PostgreSQL `CREATE VIEW`; a definition that can't be
  parsed becomes a clearly-marked placeholder flagged for manual work rather than
  silently dropped.

### Query (DML) conversion and anti-pattern linting

Beyond schema, the tool can convert queries and **lint your application's SQL** for
patterns DSQL won't accept or that won't scale — e.g. `SELECT ... FOR UPDATE`
(pessimistic locking against DSQL's optimistic concurrency), dependence on foreign
keys, `AUTO_INCREMENT` assumptions, trigger/stored-procedure calls, and
unsupported functions. Use this to find code that needs changing **before**
cut-over.

### The end state

After Schema Conversion, the **DSQL target schema is fixed**. This is the schema
Full Load writes into and the schema CDC streams into — and, importantly, CDC does
**not** propagate later source DDL, so if you change the source schema during CDC
you must re-apply the equivalent DDL here yourself (see
[Chapter 4 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important)).

---

**Next:** [3. Full Load →](03-full-load.md)
