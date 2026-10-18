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

The read is read-only and in autocommit, waits at most 5 seconds for a lock another session
holds (a running `ALTER TABLE`), reports progress per table, and can be cancelled
(**Cancel evaluation**). A re-run keeps the previous report until it succeeds. A MySQL
migration user needs `SHOW VIEW` to read view definitions, and `TRIGGER`, `EVENT` and
`SHOW_ROUTINE` (or `SELECT` on `*.*`) to list triggers, events and routines; without them
the report says what it could not list (`SOURCE_VISIBILITY`) instead of omitting it.

For a PostgreSQL source, Evaluation also reads two things the inventory does not hold:

- **Row-level security.** Aurora DSQL has none, so a table with row-level security enabled
  is graded **UNSUPPORTED** (`PG_ROW_LEVEL_SECURITY`), with each policy's `USING` /
  `WITH CHECK` text to rebuild the rule in the application from. The finding also says
  whether the source user reads every row: a superuser, a role with `BYPASSRLS`, or the
  table's owner when the table is not `FORCE ROW LEVEL SECURITY`. Every PostgreSQL source
  session runs with `row_security = off`, as `pg_dump` does, so for any other user the Full
  Load and Validation stop on that table with an error instead of copying and comparing
  only the rows its policies show; the Data Migration prerequisite checks flag it
  (`ROW_SECURITY`) before the load.
- **Standalone sequences** (a sequence no column owns). Schema Conversion does not create
  them, so each one is a **MANUAL** item (`PG_STANDALONE_SEQUENCE`) carrying the Aurora DSQL
  `CREATE SEQUENCE` that continues after the source's current value: `CACHE 1`, or
  `CACHE 65536` when the source already caches (Aurora DSQL accepts only those), and the
  source's own bounds when they are not bigint's. Create it before cut over, and once source
  writes stop move it past the source's final value with `setval`: the tool neither creates
  nor syncs it.

If either read fails, the report says so (`PG_CATALOG_UNREAD`) rather than reading as if
there were none.

### Every object gets one classification

| Class | Meaning | Examples |
|---|---|---|
| **AUTO** | Converts automatically, no human action. | Ordinary tables/columns with mappable types and a PK; **foreign keys** (converted automatically, flagged `RECOMMENDED` — see below); a multi-database MySQL source (each database becomes its own schema, flagged `RECOMMENDED`); an index name another table, view or index of the schema also uses (the index is renamed automatically, flagged `RECOMMENDED`). |
| **MANUAL** | Converts, but needs a decision or an app-side change. | Case-insensitive collation, FULLTEXT/SPATIAL indexes (not emitted: the search they served moves outside the database), oversized LOB and JSON columns, `ENUM`/`SET`, generated columns, `ON UPDATE` timestamps, spatial/geometry types, foreign keys with `CASCADE`/`SET NULL` actions (which CDC can't replicate), secondary indexes on `bytea` columns, or on PostgreSQL `json`/`jsonb`/`interval`/`time with time zone` columns, which Aurora DSQL cannot index (they are not emitted; a UNIQUE one stops enforcing uniqueness), PostgreSQL non-btree indexes (built as btree) and partial indexes whose predicate reads a re-typed column (other partial indexes keep their `WHERE`), a PostgreSQL `numeric` with no declared precision (stored as `numeric(18,6)`), a view whose definition the source did not return, objects the source user cannot list (`SOURCE_VISIBILITY`), a PostgreSQL sequence no column owns (`PG_STANDALONE_SEQUENCE`). |
| **UNSUPPORTED** | No automatic conversion — redesign needed. | Triggers, stored procedures/functions, scheduled events, tables with no PK, primary keys on `bytea` columns (`BINARY`/`VARBINARY`/`BLOB`, PostgreSQL `bytea`) or on PostgreSQL `json`/`jsonb`/`interval`/`time with time zone` columns, names that collide when truncated to 63 bytes (two columns of a table, or two tables/views of a schema), `DECIMAL` precision > 1000, > 255 columns/table, more than 1,000 tables, 10 schemas or 5,000 views in the target database (counting what the cluster already holds), row-level security on a PostgreSQL table (`PG_ROW_LEVEL_SECURITY`). |

The examples above are mostly drawn from a MySQL source. For a **PostgreSQL source** the same
scale applies: unsupported PG column types — arrays (`[]`), geometric
(`point`/`line`/`box`/…), network (`inet`/`cidr`/`macaddr`/`macaddr8`), `xml`, `money`,
`bit`/`bit varying`/`varbit`, `tsvector`/`tsquery`, range + multirange, `pgvector`,
`enum`, composite — are never migrated silently: where a stand-in keeps the value (an array
as `jsonb`; a range, network, `xml`, geometric, bit or text-search type as `text`; `money` as
`numeric(23,4)`; an enum as `text` with a `CHECK`) Schema Conversion emits it with a warning and
the table is **MANUAL**, and a type with no stand-in (composite, domain, `pgvector`) is
**UNSUPPORTED**; the structural gates (no PK, `numeric` precision > 1000, > 8-column keys, etc.) apply to
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

A generated key — MySQL `AUTO_INCREMENT`, or a PostgreSQL `serial`/`IDENTITY` key — is
the main recommendation, and Schema Conversion keeps it generating by default: a
single-column integer key becomes a cached identity
(`bigint GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)`; Aurora DSQL allows an identity only
on `bigint`). Full Load writes the source's own ids, and the sequence is moved past them
after the load and again at cut over (never moved backwards). Expect gaps and loosely ordered
values, and three MySQL insert idioms an identity does not reproduce: an explicit `NULL` or `0`
for the key, `LAST_INSERT_ID()` (use `RETURNING`), and an id the application supplies itself,
which does not advance the sequence. A key nothing generates on the target is a **gap**, not a
recommendation, and counts toward effort: a MySQL `bigint unsigned` key (wider than `bigint`,
kept by default), a key column inside a composite primary key, or a non-integer key stays a
plain value the application must supply on every insert. Beyond that, a UUID/random key buys **insert throughput**, because DSQL stores rows in
primary-key order so a monotonic key concentrates writes on one partition
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
effort** — otherwise nearly every table with a generated key that keeps generating would be
inflated to `MEDIUM` by that key alone. The finding still shows what taking the advice would cost
("effort if you take it"), so the choice stays informed.

> **Identity keys and the loaded rows.** Whenever a key is a cached identity
> (`GENERATED BY DEFAULT AS IDENTITY`) — the default for a MySQL `AUTO_INCREMENT` or
> PostgreSQL `serial`/`IDENTITY` key that `bigint` holds — the tool advances that sequence
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
> key (Keep source PK, a composite key, or a `bigint unsigned` key) have no sequence and
> are left untouched. The sequence is also moved past the source's own counter, so an id
> the source issued and then deleted is not issued again.

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

On screen, the list shows the objects by kind, most critical first. A kind with work to
do opens on its own; each kind shows 50 objects at a time with a page control, and an
object's risks and recommendations appear when you expand its row. The
**Classification** and **Estimated manual effort** filters and the search box narrow the
list; the box matches an object name or a finding's rule (type `BYTEA_KEY` to list every
table one fix applies to). The HTML, JSON and Text exports always list every object, and each
opens with what it describes: the source (engine and address, never a credential), the target
endpoint, when it was exported (and when the assessment ran, for a run made since the app last
started), the tool version, the readiness band with
its counts, and what the effort levels mean. Each also lists the names that already exist or
are held on the target. The file name carries the database (or host) and the export time, so
two exports do not overwrite each other.

### The migration readiness score

The card at the top of the report rates the per-object work from 0 to 100. An AUTO object
costs nothing; any other object costs its effort weight (simple 1, medium 3, significant 7;
an UNSUPPORTED object with no estimate counts as significant), and recommendations cost
nothing. The score is 100 minus that cost as a share of the worst case (every object
significant), and it stays below 100 while any object needs work. A database-level limit
item does not count toward the score: it sets the band instead.

The band is decided by blockers first, so a large source cannot average them away:

| Band | When |
|---|---|
| **Blocked** (red) | The migration exceeds an Aurora DSQL database limit: more than 1,000 tables, 10 schemas or 5,000 views in one database, counting what the target cluster already holds. Nothing else matters until it is resolved, so this band wins whatever the score. |
| **Unsupported objects** (amber) | At least one object is UNSUPPORTED and the score alone would read Ready or Low effort. (With a lower score, the effort band below keeps its label.) |
| **Ready** / **Low effort** (green) | Score ≥ 90 / ≥ 70, with nothing UNSUPPORTED. |
| **Moderate effort** / **Significant effort** (amber) | Score ≥ 45 / ≥ 1. |
| **High effort** (red) | Score 0. |

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
- AI assist calls Bedrock with **`bedrock:InvokeModel`** and
  **`bedrock:InvokeModelWithResponseStream`** (the chat streams its answer) — nothing else. It uses the **same credentials as the rest of the tool** (the AWS
  profile you selected on Connect, or the environment/role credential chain).
- **Running locally:** your credential chain must allow both actions for
  the chosen model, and the model must be **enabled for your AWS account** (see
  *Model access* below).
- **Deployed on Fargate:** AI assist is **off unless you deploy with
  `EnableAiAssist=true`**, which grants the task role both actions, **scoped to the
  `anthropic.` provider**: any Anthropic foundation model or cross-region inference
  profile in the account, so you can switch the Model ID among Anthropic models without
  redeploying (never a blanket `*`, never another provider). `BedrockModelArns` is
  **optional** and narrows that scope to specific ARN(s) (or allows a non-Anthropic model). You also set `BedrockRegion`
  and the task's egress must be able to reach the Bedrock runtime endpoint (NAT or
  a Bedrock VPC endpoint). See [`deploy/DEPLOYMENT.md`: "Enable AI-assisted conversion (optional)"](../../../deploy/DEPLOYMENT.md).
- **Model:** the default is `global.anthropic.claude-sonnet-5` (a general,
  cost-effective choice); override it with `BedrockModelId` (deploy) or in the
  Connect screen's Bedrock settings.
- **Model access (once per AWS account):** Anthropic models on Bedrock are enabled
  per account through an AWS Marketplace subscription — the model's *agreement*. The
  first call to a model in an account starts that subscription in the background, and
  calls can succeed for up to about 15 minutes even if it is going to fail. It fails
  when the caller lacks `aws-marketplace:Subscribe` / `aws-marketplace:Unsubscribe` /
  `aws-marketplace:ViewSubscriptions`, when Anthropic's one-time first-time use form
  has not been submitted for the account (or its AWS Organization), or when the
  account has no valid payment method. When Bedrock's denial is about the model's AWS
  Marketplace subscription, the tool says *the configured model is not enabled for
  this AWS account yet* and names the fix below. A missing first-time use form can
  instead show up as Verify AI access reporting the model
  *not enabled or not available*: submit the form once in the Amazon Bedrock console,
  then verify again. If the identity the tool uses can't subscribe (the Fargate task
  role deliberately can't), someone who can accepts the model's agreement **once** —
  in the Amazon Bedrock console, or with:

  ```bash
  aws bedrock list-foundation-model-agreement-offers --model-id <foundation-model-id>
  aws bedrock create-foundation-model-agreement --model-id <foundation-model-id> --offer-token <offer-token>
  ```

  `<foundation-model-id>` is the model id without its `global.` prefix
  (`aws bedrock list-foundation-models --by-provider anthropic` lists them), and
  `<offer-token>` comes from the first command's output. Allow about 2 minutes, then
  run **Verify AI access** again. After that no caller needs AWS Marketplace
  permissions, and no IAM change to the tool is needed.

**How to turn it on:**

1. On the **Connect** screen, expand **AI Assist** and toggle **Enable AI Assist**
   (optionally set the model id / region under *Bedrock settings*).
2. Use the **Verify AI access** preflight — a non-blocking check that makes a
   minimal call to the configured model with these credentials (a valid AWS profile
   alone does **not** guarantee `bedrock:InvokeModel`), so you find a permission gap
   now, not mid-run. If Bedrock rejects that model as unavailable, or denies it for a
   reason other than its model agreement, the check also tries a fallback model; if
   the fallback answers, the Model ID switches to it and the result is a warning that
   names both models rather than a clean pass (to go back, select your model again
   once it is enabled, then verify). A model-agreement (AWS Marketplace) denial,
   expired or invalid credentials, throttling and network errors stop at the first
   call. A pass means the configured model **answered just now**. In a new AWS
   account a pass can be temporary (see *Model access* above): if AI replies are later
   denied, the AI DBA panel and the AI Assist status on the Connect screen say *the
   configured model is not enabled for this AWS account yet* and name the fix —
   accept the model's agreement once, wait about 2 minutes, then verify again.

> **Cost & latency:** enabling AI calls Bedrock, which **adds cost and some
> latency**. It's a convenience for the `MANUAL`/`UNSUPPORTED` long tail, not a
> requirement — the deterministic path migrates everything it can on its own.

### How to use the report

Work the list top-down by severity:

1. **Resolve every UNSUPPORTED item.** These block a clean migration. Add a PK
   where missing; move triggers/routines/events into the application (or
   EventBridge/Lambda); reduce `DECIMAL` precision to ≤ 1000.
2. **Decide every MANUAL item.** Accept the default collation,
   handle `ENUM`/`SET`, adapt to a substituted type (a spatial column as `bytea`, a
   PostgreSQL array as `jsonb`), decide `ON UPDATE` timestamps, replace a cascade action CDC
   can't replicate, etc. For an oversized LOB or JSON column (`OVERSIZED_LOB`), run the
   Data Migration prerequisite checks (or **Check sizes**,
   [§13.5](13-excluding-tables-and-columns.md#135-oversized-lob-candidates-and-check-sizes))
   first, and act only if a value already exceeds the limit: move that content to
   external storage (e.g. Amazon S3), or exclude the column (an excluded column is
   migrated as NULL).
3. **AUTO items** need nothing from you.

> **PostgreSQL source.** The same top-down triage applies. Remodel unsupported PG types
> before you convert — arrays → `jsonb` or a child table; `network`/`xml`/`bit`/range →
> `text`; `money` → `numeric(23,4)`; `enum` → `text`; composite → separate columns or `jsonb`.
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
  recreates it with the new key). A `CREATE` that Aurora DSQL answers with its
  temporary `XX000` ("server unavailable") is retried: a table, view or schema the
  retry then finds already there was created by the earlier attempt, but an index
  found that way is not reported as created, because its name could belong to
  another table's index.

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
  expression referencing another column does not, nor a zero date such as `'0000-00-00'`), it
  is dropped and **reported** rather than silently lost, at Evaluation too (`DEFAULT_DROPPED`). `ON UPDATE CURRENT_TIMESTAMP` cannot be reproduced at all — DSQL
  has no `ON UPDATE` clause and no triggers — so it is flagged **MANUAL** for the
  application to handle. **For a PostgreSQL source** a column DEFAULT **is** carried
  across **verbatim** — the exact text PostgreSQL stores (`pg_get_expr`), never re-rendered,
  so `CURRENT_TIME`, `CURRENT_TIMESTAMP(0)`, a `date_part(...)` stored before PostgreSQL 14
  and a 3-argument `date_trunc(..., 'Asia/Seoul')` reach Aurora DSQL unchanged; a primary-key
  `serial`/`IDENTITY` `nextval` is governed by the chosen primary-key strategy instead, and a
  non-key `nextval`, a default that does not parse, one that casts to a type DSQL lacks, or one
  that calls a function DSQL lacks (another uuid-ossp generator, or a schema-qualified function
  outside `pg_catalog`) is dropped with a note — Evaluation reports it too (`DEFAULT_DROPPED`) —
  while uuid-ossp's `uuid_generate_v4()` becomes the built-in `gen_random_uuid()` (an enum default
  is kept as its plain text literal, since the column becomes `text`). For a non-key `nextval` the note says how to keep the database generating
  the value: an identity, which Aurora DSQL allows only on a `bigint` column — and it cannot
  change a column's type after apply — so a `serial` (`integer`) column has to be
  declared `bigint GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)` in the target DDL before
  applying, and the identity restarted past the loaded values before cut over (Evaluation's
  `NON_KEY_SEQUENCE` finding gives the same instruction). `CACHE 1` instead hands values out
  one at a time and in order, as the source does, at lower insert throughput (an aborted or retried
  transaction still uses one up, so gaps stay possible). A `DEFAULT` that calls a
  sequence several tables share gets no identity: create that sequence and re-attach the `DEFAULT`, as
  the sequence's own Step 1 item says. A `STORED` generated column IS preserved, too: its expression is re-emitted
  verbatim as `GENERATED ALWAYS AS (expr) STORED` and Aurora DSQL maintains the value on every
  write, so nothing has to be re-implemented (a write must simply OMIT the column, which
  Full Load and the CDC sink do). A `VIRTUAL` generated column — PG18's default kind — has
  no DSQL equivalent and is created ordinary.
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
- **Primary-key strategies** — a cached identity column (the default for a
  single-column generated key that `bigint` holds), or keep the integer PK. Each table's card also has a **primary-key picker** to switch it
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
- **Views** — a MySQL view is transpiled to PostgreSQL `CREATE VIEW`; a definition that
  can't be parsed becomes a clearly-marked placeholder flagged for manual work rather than
  silently dropped, and a view calling a function the transpiler does not translate
  (`FIND_IN_SET`, `UNIX_TIMESTAMP`, a user function) is flagged MANUAL at both steps
  (`VIEW_NOT_CONVERTED`): its `CREATE VIEW` fails unless the function exists on Aurora DSQL. MySQL calls with a
  faithful PostgreSQL form are rewritten to it (`YEAR()`/`HOUR()`/`DAYOFWEEK()`/`WEEKOFYEAR()` as `EXTRACT`,
  `DATEDIFF`/`TO_DAYS`, `NOW()`/`CURRENT_TIMESTAMP`/`LOCALTIMESTAMP` and `CURTIME()` as the session's wall clock and
  `UTC_DATE()`/`UTC_TIMESTAMP()`/`UTC_TIME()` as the UTC one, truncated to their precision as MySQL does,
  `STDDEV`/`VARIANCE` as the population forms MySQL computes and `VAR_SAMP`/`STDDEV_SAMP` as the sample ones,
  `CAST(x AS CHAR [CHARSET ...])` as text — `varchar(n)` with a length, which truncates and never pads — and
  a binary column's bytes decoded (`CONVERT_FROM`), `BINARY x` as its bytes (in a comparison x's text, byte-wise as
  Aurora DSQL compares already — a number's or a date's as MySQL's text of it, `'010'` is not `10`, a value whose type
  cannot be told the value itself, a number then compared as a number; a `GROUP BY`,
  `PARTITION BY` or `COUNT(DISTINCT ...)` key the value itself; an `ORDER BY` key a text, a date or bytes itself, a number
  or a value whose type cannot be told by the bytes of its text — an output name by the expression it names, a name a
  `FROM` column also has by that column, as MySQL reads it — and a
  `UNION`'s or a `SELECT DISTINCT`'s, which can be ordered only by what it selects, x itself, flagged), `CAST(x AS UNSIGNED)`
  rounding first (a string read by its leading integer) and wrapping a negative number as MySQL does, `/` of
  integers and decimals as MySQL's exact division (computed with the fraction digits MySQL keeps, truncated — 9 for
  integers, so `2 / 3 * 1000000` is `666666.6660` — shown at the dividend's scale plus 4 — `10 / 3` is `3.3333` —
  and NULL for a zero divisor, as `%`, `MOD` and `DIV` are) and `AVG` as the same division of its `SUM` by its
  `COUNT`, a `FLOAT` in double precision as MySQL computes it (its `SUM`, `f * f`),
  `ROUND`/`TRUNCATE(x, d)` of a `FLOAT`/`DOUBLE` in double precision as MySQL computes them (to places read at
  run time too; of an integer, the integer), a `TINYINT(1)` (a boolean on Aurora DSQL) and a comparison read as a
  number — in arithmetic, a bitwise operator, `ROUND`/`SIGN`/`POWER` or a numeric cast, an aggregate (`SUM(qty > 2)`
  counts rows) or beside a number (`= 1`, `<=> 1`, `IN`, a simple `CASE`) — as its integer, a number stored in a
  `TINYINT(1)` (`SET paid = 1`, `INSERT ... VALUES (..., 1)` or `INSERT ... SELECT`, by the column it fills) as a
  boolean, `SET col = a = b` as MySQL reads it (`col = (a = b)`), `GROUP_CONCAT` over MySQL's text (a `TINYINT(1)` or a
  comparison as its digit), `CONCAT_WS` skipping NULLs and
  `GREATEST`/`LEAST` returning NULL as MySQL does, `LENGTH` as the bytes of its text and `CHAR_LENGTH` as its characters (a `CHAR(n)` column without its
  padding, a JSON value as its text), a number, a date, a `TINYINT(1)` or a comparison as MySQL's text of it where MySQL
  wants a string (`CONCAT`/`CONCAT_WS` — bytes beside text decoded, flagged — `LPAD`/`RPAD`, `LEFT`/`RIGHT`, `SUBSTRING`,
  `REPLACE`, `UPPER`/`LOWER`, `TRIM`, `REVERSE`, `REPEAT`, `MD5`, `ASCII`, `LOCATE`/`INSTR`, `LIKE`, `REGEXP`,
  `GREATEST`/`LEAST` beside a string — but a number string below 2, whose string order is the numbers' (the clamp
  `GREATEST(x, '0')`) — a `UNION` column of numbers or dates beside a text column or a
  string of another sort, `'N/A'`; a `DATETIME(6)` with all six
  fractional digits), a `COALESCE`/`CASE`/`IF` MySQL makes a string — a date or a time beside a string or a text column, a
  number beside a string or a date, a JSON value beside either — as MySQL reads that string where it stands (beside
  strings PostgreSQL reads as its values with the same text — `IFNULL(end_date, '9999-12-31')` beside a `DATE`,
  `IFNULL(qty, '0')` — a date stays a date, and a number shown a number, so a query reading a derived table's, a CTE's or
  a view's column, or an `INSERT ... SELECT`, computes with it as MySQL does; read as a
  number — arithmetic, `SUM`/`AVG`, a numeric function, compared with a number — the double MySQL reads from it, so
  `IFNULL(qty, '0') + 1` adds and `''` is 0; read as a date — a date function, date arithmetic, compared with a date — the
  date, when its strings are dates of its sort, a `DATE` beside one with a time a timestamp and a string with no digit
  NULL in a date function and MySQL's zero date compared with a date (`'-infinity'`, below every date); anywhere else —
  shown, made text, in JSON, ordered — MySQL's string: each value's text, the literal as written),
  a `TIME` beside a date as MySQL's `DATETIME`, a `COALESCE` of exact numbers at its widest scale (`IFNULL(price, 0)` is
  `0.00`), a
  `DATE` plus days/months staying a `DATE` (an amount read from a column or an expression, or a fraction,
  counted in whole units as MySQL counts it — a string by its leading integer — and a composite unit such as
  `'1 2' DAY_HOUR` by its parts; a date string with a two-digit year, or as `yyyymmdd`, read as MySQL reads it),
  `INTERVAL x unit + d` as `d + INTERVAL x unit`, a `TIMESTAMP '...'` literal as the `DATETIME` it is,
  `CONVERT_TZ` with a `+hh:mm` offset (a literal one, or one read from a column), `JSON_ARRAY`/`JSON_ARRAYAGG`
  keeping NULLs and writing a `DATETIME`, a `TINYINT(1)` (`1`/`0`) and a comparison (`true`/`false`) as MySQL does,
  `JSON_EXTRACT`/`->` and MySQL's `->>`
  (which MySQL stores in a view as `json_unquote(json_extract(...))`) over a path of member names and array indexes
  following it as MySQL does (`[0]` of a value that is no array is the value itself, up to three `[0]` steps, counted
  through the calls nested in its document; a
  document whose type cannot be told cast to `json`) — `->>` of a JSON null is the text `'null'` (NULL where a date
  function reads it, as MySQL reads it; a grouped `->>` written as its `GROUP BY` key wherever the query reads it), and
  its text, or a JSON value, compared with a number, in arithmetic, `SUM`/`AVG`, `ROUND` or a numeric function is read
  as the double MySQL reads (flagged: a text that is not a number fails there) — `STR_TO_DATE` typed as MySQL types it
  (a `TIME`, `DATE` or `DATETIME` by its format, an empty string NULL, a two-digit year read as MySQL reads it,
  input left over after the format ignored), a join without `ON` (`CROSS JOIN`, `LATERAL`, `INNER JOIN`, a `JOIN`
  at the top of a Query Converter statement — a comma stays one) as a `CROSS JOIN`, `CROSS JOIN ... ON` and
  `STRAIGHT_JOIN` as a `JOIN`, a recursive CTE's recursive values cast to its first SELECT's type where that has a
  modifier (`CAST(x AS CHAR(n))`, MySQL's way to widen its column, a `VARCHAR(n)` or `DECIMAL(p,s)` column; an integer
  beside a recursive fraction is flagged), and every `DATE_FORMAT` / `STR_TO_DATE` /
  `FROM_UNIXTIME` format written from MySQL's own specifiers, `%%` and unknown ones included); others (`WEEK` in a
  mode other than ISO's, `SUBSTRING_INDEX`, `SHA2`, `JSON_SET`, ...) are flagged the same way, as is a stored
  function even when PostgreSQL has one of its name (`date_trunc`), a JSON path Aurora DSQL cannot follow (`'$'`,
  a wildcard, `last`, a range, a path computed at run time, more than three `[0]` steps, several paths) and a JSON
  value compared, ordered, grouped or made distinct (Aurora DSQL has no comparison of json values), and a format token
  with no
  Aurora DSQL pattern (MySQL's `%U`/`%u`/`%V`/`%X` week numbers, `%w`) is flagged too: the view would be
  created but return different text. So are `STR_TO_DATE` over a column (MySQL returns NULL for a value it cannot
  read with its format; Aurora DSQL raises an error for one and reads one whose separators differ as a date — and
  in a session time zone with daylight saving time, a time in the hour the clocks skip comes back an hour later),
  decoding bytes (invalid bytes are NULL in MySQL, an error on Aurora DSQL), a format read from
  a column, `MOD` of a `FLOAT`/`DOUBLE` (MySQL's is C's fmod, which Aurora DSQL has no operator for, nor an exact
  form of), and `GROUP_CONCAT(DISTINCT x ORDER BY y)`. Each value is written by what it holds — a column by its
  type, read through MySQL's stored joins and bound as MySQL binds it (`db`.`table`.`col` to that database's
  table — past an alias whose table lacks the column, never to a derived table or a CTE), a derived table's or CTE's
  column by what its body selects
  (a CTE named like the table it reads too), an expression by what it computes — and the Query Converter writes the same MySQL expressions the same
  way (in an `UPDATE`, a `DELETE` and an `ON DUPLICATE KEY UPDATE` too), typed by the Evaluation inventory once
  Step 1 has run (a view read in single-database mode, whose stored text names its own database's tables bare, is
  read in its own schema). `LENGTH` and `BINARY` of a value whose
  type cannot be told — another view's column, a derived table's or CTE's that it computes, or one in the Query
  Converter before Step 1 — are flagged: `LENGTH` counts the text of `x || ''`, MySQL's count for text, bytes, integers,
  decimals and dates (not a `TINYINT(1)`, a `FLOAT`, a `TIMESTAMP` or a fractional `DATETIME`), and `BINARY` is
  `CAST(x AS BYTEA)`, right for a binary column only; in a string function, `LIKE` or a `BINARY` comparison it is left
  as it is — right for text, whose index still serves `LIKE 'prefix%'`; a number there fails on Test on target — but in
  `LPAD`/`RPAD` and a `CONCAT` with no text in it, as common over numbers, it is `x || ''`. A quotient is shown at its scale
  where MySQL shows it: a statement's output and a text, and the rows of a view, derived table or CTE that MySQL
  materializes — a view declared `ALGORITHM=TEMPTABLE`, or one with an aggregate, a window, `DISTINCT`, `GROUP BY`,
  `HAVING`, `LIMIT`, a `UNION`, no table, or an output holding a subquery that reads its row — each stored at its
  column's scale (a window's values too, and a `CASE`/`IF` its query groups by: `0.0000` — under `WITH ROLLUP`, which
  MySQL groups by sorting, each row's own value). A `UNION` column with a `FLOAT`/`DOUBLE` branch keeps every digit
  of its quotients, and one with a `FLOAT` branch is typed as MySQL types it: a `DOUBLE` beside a `DOUBLE`, a
  `DECIMAL` (a quotient, an exact `SUM`) or an `INT` column, or after another integer (a literal, a `BIGINT`, `COUNT(*)`) placed
  before the `FLOAT`; a `FLOAT` beside a `TINYINT`/`SMALLINT`/`MEDIUMINT`/`YEAR` column or such an integer after it
  (`SELECT f ... UNION ALL SELECT 0` shows the `FLOAT`'s `0.1`). A view MySQL merges
  into the query that reads it is computed there, with every digit the quotient keeps, so the view is left
  unrounded and its readers return MySQL's results. The trade-off: selected directly, its quotient column shows the
  digits MySQL computes with — `0.333333333` where MySQL shows `0.3333`, `(x / 3) * 3` as `9.999999999` where MySQL
  shows `10.0000` — graded AUTO, with no note per view; and a view reading it, which cannot know the column is a
  quotient (a PostgreSQL view does not merge into its reader), shows what it computes from it with those digits too
  and rounds or truncates it to as many places as asked (MySQL keeps no more than the quotient's scale:
  `TRUNCATE(v.q, 6)` of `1 / 3` is `0.3333`). Where the shown value matters, read it as `ROUND(q, 4)`. Not
  reproduced: MySQL keys a `GROUP BY`, a window's `PARTITION BY` / `ORDER BY` and a `COUNT(DISTINCT ...)` on a
  quotient at its shown scale (`1 / 3` and `3333 / 10000` are one key) where the conversion keys on the digits
  kept — flagged MANUAL (a `GROUP BY 1` or a `GROUP BY` alias keys on the shown output on both); grouping through a
  temporary table (no index on the grouping columns) MySQL also stores a quotient's `SUM`, `MAX`, `MIN` and `AVG`
  at that scale, and a `CASE`/`IF` output that is no key (`0.0000`, shown here as `0`), which depends on its plan —
  group by and aggregate `ROUND(q, 4)` where that matters. MySQL 8.0.22+
  (Aurora MySQL 3) pushes a filter on a materialized view's or derived table's quotient into it (`WHERE v.q =
  0.3333`) and compares every digit there, while the rows it returns carry the stored scale; Aurora DSQL compares
  the stored value — compare `ROUND(q, s)` or a range where that matters. A `JSON_ARRAY`/`JSON_OBJECT` with a
  window function among its arguments is computed after the window, its quotient arguments read at their scale
  (`[7.1429, 2.6667]`), where the conversion writes every digit kept (`7.142857142`). A `GREATEST`/`LEAST` nested
  more than three levels deep behind `IFNULL`, `IF`, `CASE` or another function, a composite-unit interval read
  from a column, and an expression nested more than 200 levels deep (past what the conversion types) are flagged
  MANUAL. Flagged as still
  different: a `FLOAT`'s text (MySQL writes 6 significant digits, `1234.57`) and a `FLOAT` in JSON, and date arithmetic
  over a date beside a date string of the other sort (`IFNULL(d, '2024-06-01 10:00:00') + INTERVAL 1 DAY`: MySQL's
  result is a string whose form follows each row, `2024-03-06` for a date). Still different, not flagged: a number
  beside number strings — a shown `COALESCE`/`CASE`/`IF`, a `UNION` column — is ordered (or compared with text) as a
  number where it is read (MySQL orders the strings, `'10'` before `'9'`); a `DECIMAL` beside a number string, read by another view in arithmetic,
  keeps its scale (`20.00` where MySQL's double shows `20`); JSON of a `COALESCE` its query groups by (a `sql_mode`
  without `ONLY_FULL_GROUP_BY`) is written at its widest scale (`[0.00]`, MySQL `[0]`); and a `UNION` of a `DECIMAL` and
  an integer shows the integer branch's rows without the column's scale (`5` where MySQL shows `5.00`). A **PostgreSQL** view's definition (`pg_get_viewdef`) is PostgreSQL
  already, so it is carried **verbatim** under the quoted view name, and the source pane
  shows that same text. Re-rendering it would change what the view returns — for example
  a `::text` cast on a jsonb key would apply to the whole `->` and turn a jsonb column
  into text. The one rewrite is a cast to an enum type the conversion turns into text
  (`'paid'::order_status` becomes `'paid'::text`), since that type does not exist on Aurora
  DSQL. A view that reads columns the conversion re-types (an enum, an array that becomes
  jsonb, a range that becomes text) is flagged **MANUAL** in both steps
  (`VIEW_REMODELLED_COLUMN`), naming each column with its old and new type: an array
  subscript or a range operator in it fails at apply or returns something else.

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
you must make the matching change on Aurora DSQL yourself — not by re-applying it
here while CDC streams (Apply, **Replace** included, is blocked then, and Replace
drops the table). With CDC stopped, a type or key change is a recreate: **Replace**
here, then **Drop & reload** (see
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
> `uuid`; `json`; `jsonb`). A different set has no DSQL equivalent and is flagged at both
> Evaluation and Schema Conversion: a stand-in that keeps the value is emitted with a warning
> and graded **MANUAL** (an array as `jsonb`, `money` as `numeric(23,4)`, an enum as `text`
> with a `CHECK`, the others as `text`), and a type with no stand-in (composite, domain,
> `pgvector`) is **UNSUPPORTED**: arrays (`[]`), geometric
> (`point`/`line`/`lseg`/`box`/`path`/`polygon`/`circle`), network
> (`inet`/`cidr`/`macaddr`/`macaddr8`), `xml`, `money`, `bit`/`bit varying`/`varbit`,
> `tsvector`/`tsquery`, range + PG14 multirange, `pgvector`, `enum`, composite. The PG
> numeric rules match the table below: `numeric(p,s)` with `p > 1000`/`s > 1000` is clamped
> with a warning, and a bare `numeric`/`decimal` is stored as `numeric(18,6)`: a value with
> more than 6 fractional digits is rounded on load and Validation compares that column at
> scale 6, so Evaluation flags it (`PG_UNCONSTRAINED_NUMERIC`). Column DEFAULTs **are**
> emitted; only a `serial`/`IDENTITY` `nextval` is stripped (the primary-key strategy
> governs identity). A `STORED` generated column's expression **is** re-emitted
> (`GENERATED ALWAYS AS (expr) STORED`), because Aurora DSQL supports and maintains those;
> a `VIRTUAL` one becomes an ordinary column.
>
> A **MySQL** generated column is created as an ordinary column (the tool reads its MySQL
> expression and shows it in the finding and the source DDL, but does not translate it): Full
> Load copies the computed value, and CDC carries a `STORED` one's value. You can re-add the
> translated expression (`GENERATED ALWAYS AS (...) STORED`) in Schema Conversion before
> applying, for any migration type: Full Load and CDC then leave every column the applied DDL
> makes `GENERATED` out of their writes (Aurora DSQL refuses a value for one, SQLSTATE `428C9`),
> and Aurora DSQL computes it. A generated column that is part of the primary key stays
> ordinary: Full Load and CDC write every key column, so a `GENERATED` key column would have
> every write rejected.

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
| `TINYINT(1)` | `boolean` | `boolean` (`true`/`false`) | MANUAL | MySQL boolean convention; `0/1`→`false/true`. A value **outside `{0,1}` is refused** (no silent flatten): Full Load stops the table, CDC dead-letters the row. Edit the type to `smallint` here if the column holds other values. |
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
| `DECIMAL(p,s)` / `NUMERIC(p,s)` | `numeric(p,s)` | `numeric(p,s)` | AUTO | Precision/scale preserved. **Precision > 1000** → Evaluation flags it **UNSUPPORTED** (DSQL documents a maximum precision of 1000, scale -1000..1000, default `numeric(18,6)`); Schema Conversion still emits DDL by **clamping to the maximum** with a data-loss warning. A MySQL `DECIMAL` (max 65,30) and every PostgreSQL `numeric` (whose own limit is the same 1000) are inside this, so the flag and the clamp are only a safety net. A value with more decimal places than the target keeps (a source widened after Step 1, or a narrower type set here) is refused on Full Load and CDC, never rounded. |
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
| `TIME` | `time` (without time zone) | `time` | MANUAL | In-range `00:00:00..23:59:59`. An **out-of-range** MySQL `TIME` (negative or `> 24h`, MySQL range `-838:59:59..838:59:59`) has no `time` representation → **fails loudly** (needs an `interval` column instead). Evaluation flags it too (`TIME_TYPE`). Under CDC (sink plugin v54+) such a value is written as an interval: it lands in a column remapped to `interval` and is dead-lettered with that reason for a `time` column. Remapped to `interval` or text, Full Load and CDC both write `[-]HH:MM:SS[.f]` (hours not capped at 24) and Validation compares the durations. |
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
| `JSON` | `json` | `json` | AUTO | A single value **> ~1 MiB** is rejected by DSQL (for `json` the limit applies to the compressed size) → per-row quarantine (Full Load) / DLQ (CDC); Evaluation flags the column `OVERSIZED_LOB`. (CDC wraps the JSON text in a `PGobject(type=json)` so it targets the `json` column.) |
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
| **Column defaults ARE supported** | **MySQL source:** a source `DEFAULT` is carried across, including `CURRENT_TIMESTAMP[(n)]`, `NOW()`, `CURRENT_DATE`/`CURRENT_TIME`, and `LOCALTIME`/`LOCALTIMESTAMP`. `TINYINT(1)` defaults become `TRUE`/`FALSE` (a `boolean` column rejects `DEFAULT 1`), a `DATETIME` default is pinned to UTC to match the loader's naive-UTC values, and function defaults are translated (`UUID()`→`gen_random_uuid()`, `CURDATE()`→`CURRENT_DATE`, `CURTIME()`→`CURRENT_TIME`, `UTC_TIMESTAMP()`/`UTC_DATE()`→UTC expressions). A default with no DSQL equivalent (e.g. one referencing another column) is dropped and **reported**, never silently lost. Keeping the default matters most on a `NOT NULL` column: MySQL accepts an `INSERT` that omits it, the target would not. **PostgreSQL source:** a source `DEFAULT` **is** carried across verbatim (the exact text PostgreSQL stores, never re-rendered) — only a `serial`/`IDENTITY` `nextval` and a generated-column expression are stripped, and identity is governed by the chosen PK strategy on the target instead. A converted enum column keeps its default as a plain text literal, and uuid-ossp's `uuid_generate_v4()` becomes `gen_random_uuid()`; a default that casts to another type DSQL lacks, or calls a function it lacks, is dropped with a note. |
| **No `ON UPDATE CURRENT_TIMESTAMP`** | Unreproducible: DSQL has neither an `ON UPDATE` clause nor triggers (the usual PostgreSQL workaround). The column keeps its insert-time default and is flagged **MANUAL** — set the timestamp explicitly on update in your application. |
| **No triggers / stored procedures / events** | Flagged **UNSUPPORTED** — reimplement in the application (or EventBridge/Lambda for scheduled events). |
| **No native partitioning** | DSQL auto-distributes; Schema Conversion drops the partitioning, and a partitioned table gets a recommendation (what depended on its partitions, and pausing partition maintenance under CDC), not a gap. |
| **One database per cluster** | Each source database of a multi-database MySQL source becomes its own schema automatically (flagged `RECOMMENDED`); see the next row for how many fit. |
| **≤ 10 schemas, ≤ 1,000 tables, ≤ 5,000 views per database** | Evaluation counts what the target database will hold after the migration (the source's objects plus those already on the cluster; the built-in `public` schema is not counted, an empty schema is, and names are compared as stored, cut to 63 bytes) and flags any excess **UNSUPPORTED** (`SCHEMA_COUNT_LIMIT` / `TABLE_COUNT_LIMIT` / `VIEW_COUNT_LIMIT`), which makes the readiness score read **Blocked**. Otherwise the apply would create the first objects and then fail with `54000`. At the limit Aurora DSQL rejects every `CREATE SCHEMA`, even `IF NOT EXISTS` for a schema that already exists (live-verified), so the apply checks the catalog and treats ensuring an existing schema as done. |
| **Source `CHECK` constraints are dropped** | A source `CHECK` constraint (a MySQL `CHECK` requires 8.0.16+) is **not** carried to the target — its functions/operators may differ on DSQL — and is flagged **MANUAL**: re-add a DSQL-compatible `CHECK` by hand or enforce it in the app. (The `CHECK … IN (...)` the tool *generates* for an `ENUM` is unaffected.) |
| **`bytea` cannot be a key** | A `bytea` column (from `BINARY`/`VARBINARY`, `*BLOB`, or spatial) cannot be a **primary-key** column (table → **UNSUPPORTED**) or an **index** column (index dropped → **MANUAL**; a dropped UNIQUE index no longer enforces uniqueness, and the note says so). Evaluation flags the same tables (`BYTEA_KEY`). Re-key as `text`/`uuid`/hash. |
| **≤ 8 columns per key** | A primary key or index over more than 8 columns exceeds DSQL's limit: the PK case is **UNSUPPORTED**, an over-wide secondary index is **dropped (MANUAL)**. |
| **≤ 24 indexes per table** | DSQL allows at most 24 indexes per table (the PK counts, so ≤ 23 secondary). Excess indexes are flagged and not all emitted. |
| **≤ 1 KiB key value** | The combined key value must stay under ~1 KiB; a wide key is flagged as a **recommendation** (it can fail at insert time on real data). |
| **63-byte identifiers** | DSQL/PostgreSQL truncate identifiers to 63 bytes (on a character boundary). Two column names of one table that truncate to the same name make the table **UNSUPPORTED** (`IDENTIFIER_LENGTH`). Two tables or views of one schema that truncate to the same name are **UNSUPPORTED** too (`NAME_COLLISION`), and Schema Conversion creates neither of them (Data Migration does not offer them either), because whichever came first would receive the other's rows; rename on the source so the first 63 bytes differ. Schema names are cut the same way, so two source databases whose names agree in their first 63 bytes share one target schema. A longer name that collides with nothing is a **recommendation**: DSQL stores the shortened name. |
| **Index names are unique per schema** | Aurora DSQL keeps a schema's tables, views and indexes in one namespace, while MySQL scopes an index name to its table. Schema Conversion therefore **renames** every index whose name another relation of the schema also uses to `<table>_<index>` (cut to 63 bytes with a hash suffix if needed); tables and views keep their names, and view names are emitted quoted like table names, so their case is kept. Every index, a UNIQUE one included, is then created. Evaluation and Schema Conversion flag the renamed indexes `RECOMMENDED` (`NAME_COLLISION`), and a primary-key strategy change keeps the same names. If an earlier apply left a relation that holds a name now needed by a different object (an index named like a table), the apply reports that object **FAILED** with the holder named, instead of skipping it as "already exists", and Evaluation's target analysis lists it under *Names held on the target by another object*. On a target an earlier version built, the apply still creates the renamed index and the quoted view, and its result notes the old copy left under the previous name: an old index copy is a duplicate Aurora DSQL still maintains on every write (a UNIQUE one keeps enforcing uniqueness), an old view copy is no longer kept in step with the source. Drop it (a **Replace** drops it). |
| **Mixed-case names** | Schema Conversion keeps every name exactly as spelled by quoting it, and Aurora DSQL folds an **unquoted** name to lower case. MySQL matches column names case-insensitively, so application SQL that writes `CustomerId` or `Orders` unquoted finds nothing after cut over. Evaluation lists such names, and any tables whose names differ only by case, as **MANUAL** (`MIXED_CASE_IDENTIFIERS`, MySQL sources): quote them in the application exactly as spelled, or rename them to lower case on the **source** before Schema Conversion. Never rename them on the target before cut over: Full Load, CDC, Validation and the cut-over DDL all address the source spelling. Only ASCII capitals count — Aurora DSQL folds nothing else. |

The **Class** column above uses the same AUTO / MANUAL / UNSUPPORTED scale Evaluation
applies to every object — see [§2.1](#every-object-gets-one-classification) for what each
one means and how the most demanding class wins when several rules match.

---

**Next:** [3. Full Load →](03-full-load.md)
