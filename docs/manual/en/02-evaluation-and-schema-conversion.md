# 2. Evaluation and Schema Conversion

_Language: **English** | [한국어](../ko/02-evaluation-and-schema-conversion.md) | [日本語](../ja/02-evaluation-and-schema-conversion.md)_

> **Prev:** [1. Set up](01-setup.md)

Between connecting and moving data are the two steps that make the migration
**deterministic and predictable** instead of trial-and-error:

```
Connect → [ 1. Evaluation → 2. Schema Conversion ] → 3. Data Migration → 4. Validation → 5. Cut over
```

- **Evaluation** answers *"what in my source database will and won't move to Aurora DSQL,
  and how much work is each piece?"*
- **Schema Conversion** turns the source schema into Aurora DSQL DDL, lets you review and
  edit it, and applies it to the target.

**Shortest path:** **Run evaluation** → look at the readiness band and the **Unsupported**
items ([§2.1](#21-evaluation--the-compatibility-assessment)) → fix or decide each finding
([§2.4](#24-findings-reference) says what to do) → **Generate DDL for selected** → review
and **Apply** ([§2.2](#22-schema-conversion--generate-and-apply-the-dsql-ddl)).

Both steps only read the source, and Schema Conversion writes to the target only when you
apply. Whatever your source engine (MySQL or PostgreSQL), **this is where you learn what
Aurora DSQL won't accept, before you've moved a single row.** Don't skip it.

The steps are the same for both source engines. Where they differ, a short note or table
here says how; [§2.3](#23-type-mapping-reference) maps every column type of each engine,
and [§2.4](#24-findings-reference) lists the findings and what to do about each.

---

## 2.1 Evaluation — the compatibility assessment

Click **Run evaluation**. The tool reads the source schema (on MySQL the database you
named on Connect, or every database when you left it blank; on PostgreSQL every non-system
schema of the connected database) and what the target already holds, then shows the
compatibility report. It changes nothing on either side. The run shows its progress table
by table, **Cancel evaluation** stops it after the table being read, and a re-run keeps
the previous report on screen until it succeeds. The privileges the migration user needs
are in [§1.1](01-setup.md#11-prerequisites).

Evaluation also reads the source's **server settings** once — settings only, no table is
scanned — and lists them on the Evaluation screen under the source object counts
(**Source server settings**, collapsed; the downloaded reports leave the list out, though
each item that grades a setting quotes its value). They say what the application runs
under, which no table definition shows: the time zone that clock functions read and, on
MySQL, the `sql_mode`, which decides for one whether the source can hold zero dates that
Aurora DSQL has no value for (`ZERO_DATES`,
[§6.2](06-limitations.md#62-migration-process-limits)). The read cannot see a value the
application sets for itself, a session `SET` (`SET sql_mode`, `SET TIME ZONE`) or an
option its driver sends when it connects, so check those yourself.
[Source time zone and server settings](#source-time-zone-and-server-settings) below says
what the graded settings mean for you.

### Every object gets one classification

Each object Evaluation assesses gets one of three classes. The screen shows the label; the
exported reports show the value in brackets.

| Class | What it means | What you do |
|---|---|---|
| **Automatic** (`AUTO`) | Converts with no action from you, like an ordinary table with a primary key. | Nothing. |
| **Review needed** (`MANUAL`) | Converts, but something is lost or behaves differently: a dropped case-insensitive collation, an `ENUM`, a large LOB or JSON column, an `ON UPDATE` timestamp. | Decide: accept it, change the target DDL in Schema Conversion, or change the application before cut over. |
| **Unsupported** (`UNSUPPORTED`) | Cannot move as it is: a table with no primary key, a trigger, routine or event, a primary key on a binary column, more objects than one Aurora DSQL database holds. | Change the source, or rebuild the object outside the database, before you migrate it. |

A finding can also be a **Recommended** note. The conversion is then complete and correct,
and the note is advice about running well on Aurora DSQL: ignoring it costs performance,
not correctness, and it adds nothing to the object's effort or to the readiness score. The
two you will see most are a generated key that keeps generating
([Choose the primary-key strategy](#choose-the-primary-key-strategy)) and a preserved
foreign key ([Foreign keys](#foreign-keys)).

An object gets the most demanding class among its findings (a **Recommended** note does
not count), and every finding is listed under it. An object that no rule matches is
**Automatic**.

### Read the report

The report reads from the top down:

- **Migration readiness** rates the per-object work from 0 to 100 and gives it a band.
- **Compatibility by object kind** counts the **Automatic**, **Review needed** and
  **Unsupported** objects of each kind.
- **Objects by importance (most critical first)** lists every object, grouped by kind.
  Expand one to see, for each finding, the **Risk** (why it is flagged; **Note** for
  advice) and the **Recommendation** (what to do), and the object's effort:
  **Simple (< 2h)**, **Medium (2–6h)** or **Significant (> 6h)**. The **Classification**
  and **Estimated manual effort** filters narrow the list, and the search box matches an
  object name or a rule id.
- **Target analysis (Aurora DSQL)** lists the objects that already exist on the target
  (**Objects already exist on target**) and the names another object holds there
  (**Names held on the target by another object**), so that no apply fails part-way on a
  surprise. You choose to skip or replace each one when you apply
  ([Review, edit and apply](#review-edit-and-apply)).
- **Export:** the **HTML**, **JSON** and **Text** reports list every object and the target
  analysis. They are deterministic and hold no AI output.

**The readiness score.** An **Automatic** object costs nothing, and any other object costs
its effort weight (Simple 1, Medium 3, Significant 7; an **Unsupported** object with no
estimate counts as Significant); a **Recommended** note costs nothing. The score is 100
minus that cost as a share of the worst case, and it stays below 100 while any object
needs work. Use it to rank and plan the work, not as a time estimate. The band is decided
by blockers first, so a large source cannot average them away:

| Band | When |
|---|---|
| **Blocked** (red) | The migration exceeds an Aurora DSQL database limit: more than 1,000 tables, 10 schemas or 5,000 views in one database, counting what the target cluster already holds. Nothing else matters until it is resolved, so this band wins whatever the score. |
| **Unsupported objects** (amber) | At least one object is **Unsupported** and the score alone would read Ready or Low effort. (With a lower score, the effort band below keeps its label.) |
| **Ready** / **Low effort** (green) | Score ≥ 90 / ≥ 70, with nothing **Unsupported**. |
| **Moderate effort** / **Significant effort** (amber) | Score ≥ 45 / ≥ 1. |
| **High effort** (red) | Score 0. |

### How to use the report

Work the list top-down by severity:

1. **Resolve every Unsupported item** (`UNSUPPORTED`). These block the migration. Add a
   primary key where one is missing, move triggers, routines and events into the
   application (a schedule to Amazon EventBridge Scheduler), and re-key a table whose
   primary key Aurora DSQL cannot index. [§2.4](#24-findings-reference) says what to do for each.
2. **Decide every Review needed item** (`MANUAL`). Each one says what is lost or changes,
   and what to do: accept it, change the target DDL in Schema Conversion, or change the
   application before cut over. For an oversized LOB or JSON column (`OVERSIZED_LOB`), run
   the Data Migration prerequisite checks (or **Check sizes**,
   [§13.5](13-excluding-tables-and-columns.md#135-oversized-lob-candidates-and-check-sizes))
   first, and act only if a value already exceeds the limit: move that content to external
   storage (for example Amazon S3), or exclude the column (the tool then writes NULL
   there).
3. **Automatic items** (`AUTO`) need nothing from you, and a **Recommended** note is
   optional advice.

> **Tip:** to see every object one fix applies to, type its rule id (such as `BYTEA_KEY`)
> in the list's search box.

### The findings you will meet most

- A table with **no primary key** (`NO_PRIMARY_KEY`), or a primary key on a binary column
  (`BYTEA_KEY`): add or re-key it on the source.
- **Triggers, routines and events**: they are not migrated; rebuild the logic in the
  application.
- **Text compares differently** (`CI_COLLATION`): Aurora DSQL compares text by its bytes, so after a
  case-insensitive MySQL collation `=`, `LIKE` and `UNIQUE` become case-sensitive.
- A **dropped default**, or MySQL `ON UPDATE CURRENT_TIMESTAMP` (`DEFAULT_DROPPED`,
  `ON_UPDATE_TIMESTAMP`): have the application set the value.
- On MySQL, `TINYINT(1)` becoming `boolean` (`TINYINT_BOOLEAN`) and zero or invalid dates
  (`ZERO_DATES`).

Every finding, by source engine and with what to do about it, is in
[§2.4](#24-findings-reference). AI assist can explain a finding or draft a fix
([§2.5](#25-ai-assist-optional)).

---

## 2.2 Schema Conversion — generate and apply the DSQL DDL

With the assessment in hand, Schema Conversion turns the source schema into Aurora DSQL
DDL and applies it to the target:

1. **Select** objects in the **Object browser**: tick schemas or objects (or
   **Select all**), then click **Generate DDL for selected**.
2. **Review** each object's DDL, choose its primary-key strategy, and edit the target DDL
   where a finding asks you to.
3. **Apply** the DDL to the target, one object at a time or all at once.

**Reset all** discards the generated DDL and your edits, so you can generate a new
selection. The selection decides which objects are created on the target. To leave tables
or columns out of the data migration instead, use **Exclude tables & columns...** on Data
Migration ([Chapter 13](13-excluding-tables-and-columns.md)): an excluded column stays on
Aurora DSQL, and every row the tool writes holds NULL there. If the target tables already
exist (applied earlier, or by hand), **Skip conversion & continue to Data Migration**
unlocks the next step; Data Migration loads only the tables that have a target table.

### What the conversion does for you

Most of the conversion needs nothing from you. A MySQL schema is translated to Aurora
DSQL's PostgreSQL dialect, and a PostgreSQL schema is kept almost as it is. Column types,
in short (the full mappings are in [§2.3](#23-type-mapping-reference)):

| | MySQL source | PostgreSQL source |
|---|---|---|
| What changes | Each type is mapped to an Aurora DSQL type: `TINYINT(1)` → `boolean`, `DATETIME` → `timestamp`, `BLOB` → `bytea`, `ENUM` → `text` with a CHECK. | Most types stay as they are. A type Aurora DSQL lacks is changed to one that holds the same data: an array → `jsonb`, `money` → `numeric(23,4)`, `inet` → `text`. |
| Review needed | `TINYINT(1)`, `BIT`, `YEAR`, `TIME`, `ENUM`/`SET`, spatial types, `MEDIUMTEXT`/`LONGTEXT`/`MEDIUMBLOB`/`LONGBLOB`/`JSON`, case-insensitive collations | `numeric` with no precision, every changed type, `bytea`/`json`/`jsonb`, an over-long `varchar(n)`/`char(n)`, collations other than `C` |
| Unsupported | — | Composite types, domains, user-defined ranges, extension types (`citext`, `hstore`, PostGIS, pgvector), system types (`oid`, `pg_lsn`, …) |
| Full mapping | [MySQL → Aurora DSQL](#mysql--aurora-dsql) | [PostgreSQL → Aurora DSQL](#postgresql--aurora-dsql) |

The rest of what the conversion does:

- **Defaults** are carried over, which matters most for a `NOT NULL` column (an `INSERT`
  that leaves it out keeps working). One with no Aurora DSQL equivalent is dropped and
  reported (`DEFAULT_DROPPED`), and MySQL's `ON UPDATE CURRENT_TIMESTAMP` is not
  reproduced.
- **Clock defaults** (and MySQL views that read the clock) are written in the source's
  time zone, automatically
  ([Source time zone and server settings](#source-time-zone-and-server-settings)).
- **Generated columns.** A PostgreSQL `STORED` generated column is kept, and Aurora DSQL
  computes it (a write must leave the column out, as Full Load and CDC do). A MySQL
  generated column, and a PostgreSQL `VIRTUAL` one, becomes an ordinary column: its values
  are copied, but nothing computes them after cut over. You can re-add a MySQL expression,
  translated, as `GENERATED ALWAYS AS (…) STORED` in the target DDL. Decide before you
  apply: Aurora DSQL can turn a generated column into an ordinary one later, but cannot
  make an existing column generated.
- **Views.** A MySQL view is translated, MySQL's date, string, number and JSON behaviour
  included; a PostgreSQL view is copied as PostgreSQL stores it. Review a flagged view
  side by side before applying (`VIEW_NOT_CONVERTED`, or a note that its results differ;
  on PostgreSQL, `VIEW_REMODELLED_COLUMN` for a view over a re-typed column).
- **Indexes** are created with the table, before any row loads (`CREATE INDEX ASYNC`;
  Aurora DSQL builds them in the background). MySQL FULLTEXT and SPATIAL indexes are not
  created, and a PostgreSQL GIN or GiST index becomes a btree that no longer serves its
  search. A PostgreSQL `UNIQUE … NULLS NOT DISTINCT` index keeps `NULLS NOT DISTINCT`.
- **Partitioning** is dropped: Aurora DSQL distributes rows by primary key itself, and the
  partitions become one table.
- **Several MySQL databases** become one schema each in the single Aurora DSQL database.
- **Names** are kept exactly as spelled, by quoting them (mind mixed-case names:
  `MIXED_CASE_IDENTIFIERS`).
- **Foreign keys** are kept, and applied after the load ([Foreign keys](#foreign-keys)).

### Choose the primary-key strategy

Each table's card has a **Primary key** choice. Decide it before you apply: once the table
exists on Aurora DSQL its key cannot change, and changing it later takes **Replace** and a
reload with CDC stopped ([The end state](#the-end-state)).

| Strategy | Offered | What it means for the application |
|---|---|---|
| **Server-generated (IDENTITY)** | For a one-column generated key (MySQL `AUTO_INCREMENT`, PostgreSQL `serial` or identity); the default when the key fits `bigint` | Aurora DSQL keeps generating the key, as a cached `bigint` identity: expect gaps and loosely ordered values. Full Load keeps the source's ids. On MySQL three insert idioms stop working: an explicit `NULL` or `0` for the key, `LAST_INSERT_ID()` (use `RETURNING`), and an id the application supplies itself, which does not advance the identity. |
| **Keep source PK** | Always; the default for a key the source does not generate, and for a MySQL `BIGINT UNSIGNED` key, which `bigint` cannot hold | The key keeps its type, and nothing generates it on Aurora DSQL: for a key the source generated, the application must supply the value on every insert. |
| **Composite key** | When the table has a high-cardinality column to lead the key | The column you pick under **Leading column (high-cardinality)** goes in front of the key (for example `(customer_id, id)`), which spreads writes across Aurora DSQL partitions ([§7.1](07-performance-and-tuning.md#primary-key-strategy--avoid-hot-partitions)). The application's queries, joins and upserts must use the full key, and a UNIQUE index keeps the original key unique. With CDC, every key column must stay captured. Pick a column whose value never changes: the tool cannot check it. |

After a complete Full Load, and again at cut over, the tool moves each identity past the
loaded ids, so the application's first insert after cut over cannot collide
([§3.4](03-full-load.md#34-batched-idempotent-bounded-parallel-load),
[§10.3](10-conclusion.md#103-the-cut-over-switching-your-application-to-dsql)).

### Foreign keys

Aurora DSQL enforces foreign keys, and the tool keeps them by default
(**Preserve foreign keys**, beside **Generate DDL for selected**). Schema Apply never
creates them: the load writes parent and child tables with no parent-before-child order,
and CDC applies changes across tables in any order, so an enforced key would refuse rows
that only arrive early. Each table's keys appear in their own read-only pane, **Foreign
keys — applied by a separate action, not at Schema Apply**, and **a target with no foreign
keys right after Schema Apply is expected, not a failure.**

**When they are applied:**

- **Full load only** — with the explicit **Apply foreign keys** action on the Data
  Migration step, once the load has finished (not automatically at the end of the load).
- **CDC** — on the Cut over step, after the final drain and before you repoint the
  application
  ([§10.3](10-conclusion.md#103-the-cut-over-switching-your-application-to-dsql)). It is
  safe to re-run.

**Added first, validated after.** Each key is added `NOT VALID`, the only form Aurora DSQL
accepts: it is added without reading a row, in seconds whatever the size of the child
table, and it enforces every new write from then on. Aurora DSQL then validates the rows
already loaded, in the background.

**When a validation fails.** The key stays in place, `NOT VALID`, and still refuses every
new write that would break it, but some loaded rows reference a parent row that is not on
Aurora DSQL. The screen shows how many rows break the key and a query that lists them. Fix
those rows on Aurora DSQL in batches of at most 3,000 rows per transaction (add the
missing parents, point the rows at an existing parent or NULL, or delete them), then click
**Validate again**.

**Keys counted before they are added.** A key whose parent side is not fully on Aurora
DSQL by design (its referenced column is excluded, or its parent or child table is not
migrated) is counted first. If child rows reference parent rows that Aurora DSQL does not
hold, the key is skipped with a note that says why and how many rows: added, it would
refuse the application's writes that name such parents.

**A key you add or rename.** A foreign key you add or rename in the target DDL is applied
and validated like the others, under the name Aurora DSQL stores: an unquoted name in
lower case (`FK_Orders_Cust` is `fk_orders_cust`).

**What they cost.** An enforced key adds reads to every write on the tables involved
(benchmark write-heavy tables first), and a conflict comes back as a retryable `40001`. A
`CASCADE`, `SET NULL` or `SET DEFAULT` action counts toward Aurora DSQL's
3,000-rows-per-transaction limit, so a larger cascade fails: prefer `NO ACTION` or
`RESTRICT` where the number of child rows has no bound.

**MySQL source with CDC: cascades are a gap.** MySQL performs `ON DELETE` / `ON UPDATE`
`CASCADE`, `SET NULL` and `SET DEFAULT` inside the engine, and those child changes never
reach the binary log, so CDC can leave orphaned or stale child rows (`FK_CASCADE_CDC_GAP`,
**Review needed**). Replace the action with explicit child-row statements, or stop source
writes before the final comparison. At cut over, **I've cut over** waits until such a key,
and one on a text column MySQL matches more loosely than Aurora DSQL does (a
case-insensitive collation, or on a `VARCHAR` a PAD SPACE one such as `utf8mb4_bin`), has
validated, or until you choose to cut over without that check
([§10.3](10-conclusion.md#103-the-cut-over-switching-your-application-to-dsql)).

**PostgreSQL source.** A key keeps the clauses it declares, such as `MATCH FULL`,
`DEFERRABLE` and the column list of an `ON DELETE SET NULL (…)`. A PostgreSQL 18 key
declared `NOT ENFORCED` or with `PERIOD` is not created (`PG_FK_NOT_ENFORCED`,
`PG_FK_PERIOD`).

**Prefer to enforce integrity in the application?** Turn **Preserve foreign keys** off.
The tool then leaves the foreign keys out of every path, but still keeps the relationships
for Validation's orphan check. The choice is remembered across a reconnect.

### Review, edit and apply

After **Generate DDL for selected**, each object shows its source DDL and its Aurora DSQL
DDL side by side, with what the conversion changed highlighted, and lists its
**Conversion warnings** and **Recommendations**.

- **Edit** the target DDL to act on a finding (for example, change a `TINYINT(1)` column
  that holds other numbers to `smallint`), then click **Done**. **Reset to generated**
  discards your edit.
- With AI assist on, the AI DBA can draft the change
  ([Optional AI assist](#optional-ai-assist-amazon-bedrock)).
- **Apply to target** applies one object. If the object already exists on the target, the
  tool asks **Replace**, **Skip** or **Cancel** right then: this is how you re-apply a
  table after changing its DDL (Skip keeps the old one).
- The **Apply generated DDL to target** card applies every generated object. Under
  **When an object already exists on the target**, choose
  **Skip if the object already exists** (the default) or
  **Replace existing objects (destructive)**, then click
  **Apply all N generated objects to target**. Replace drops and re-creates each existing
  object, so a table loses its rows: the tool lists what it will drop and asks you to
  **Confirm REPLACE**.
- **Apply results** shows each object as created, skipped or failed. Fix a failed object's
  DDL and click **Retry failed (N)**.

Foreign keys are not part of the apply ([Foreign keys](#foreign-keys)), and while CDC
streams into the target the apply is blocked ([The end state](#the-end-state)).

### The end state

After Schema Conversion, the **Aurora DSQL target schema is fixed**: it is the schema Full
Load writes into and CDC streams into. CDC does **not** propagate later source DDL, so if
you change the source schema during CDC you must make the matching change on Aurora DSQL
yourself — not by re-applying it here while CDC streams (Apply, **Replace** included, is
blocked then, and Replace drops the table). With CDC stopped, a type or key change is a
recreate: **Replace** here, then **Drop & reload** (see
[Chapter 4 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important)).

### Check your application's queries

Schema Conversion converts the schema, not the SQL your application sends. To convert a
MySQL query, test a query read-only on the target and, with AI assist on, tune it, use the
**Query Converter**, an optional tool in the sidebar
([Chapter 9](09-query-validation.md)). On either engine, check `SELECT … FOR UPDATE`,
which behaves differently under Aurora DSQL's optimistic concurrency.

---

## 2.3 Type mapping (reference)

What Schema Conversion writes for each source column type, and what to know about it.
Aurora DSQL's own limits (keys, indexes, columns, value sizes, objects per database) are
in [Chapter 6 §6.1](06-limitations.md#61-aurora-dsql-feature-limits-your-schema-must-fit-these).

### MySQL → Aurora DSQL

Schema Conversion writes each column with the Aurora DSQL type below, and Full Load and
CDC store each value the same way: CDC carries every MySQL type. Where a value cannot be
stored as it is (a `TINYINT(1)` holding `2`, a negative `TIME`), Full Load fails the table
and CDC dead-letters the row, rather than write something else. MySQL's display widths
(`INT(11)`) and `ZEROFILL` only change how MySQL prints a number, so they are dropped.

#### Integers

<!-- type-map: mysql -->
| MySQL type | Aurora DSQL type | Classification | What to know |
|---|---|---|---|
| `TINYINT`, `TINYINT UNSIGNED` | `smallint` | AUTO | PostgreSQL has no 1-byte integer. |
| `TINYINT(1)` (`BOOL`, `BOOLEAN`) | `boolean` | MANUAL `TINYINT_BOOLEAN` | MySQL's boolean convention: `0` and `1` become `false` and `true`. Any other value is refused. If the column holds other numbers, change its type to `smallint` in Schema Conversion before applying. `TINYINT(1) UNSIGNED` is an ordinary `smallint`. |
| `SMALLINT` | `smallint` | AUTO | |
| `SMALLINT UNSIGNED`, `MEDIUMINT`, `MEDIUMINT UNSIGNED`, `INT` (`INTEGER`) | `integer` | AUTO | Widened where needed, so every value fits. |
| `INT UNSIGNED`, `BIGINT` | `bigint` | AUTO | |
| `BIGINT UNSIGNED` | `numeric(20,0)` | AUTO | No wider integer exists: `numeric(20,0)` holds every value up to 18,446,744,073,709,551,615. |
| `BIT(n)` | `smallint`, `integer`, `bigint` or `numeric(20,0)` | MANUAL `BIT_TYPE` | Aurora DSQL has no bit-string type. The column gets the smallest type that holds every `BIT(n)` value (`smallint` up to `BIT(15)`, `integer` up to `BIT(31)`, `bigint` up to `BIT(63)`), and stores the number the bits make (`b'101'` is `5`). Rewrite queries that use bit operators or `b'…'` literals. |

**Auto-increment keys.** An `AUTO_INCREMENT` column that is the whole primary key becomes
`bigint GENERATED BY DEFAULT AS IDENTITY`, so Aurora DSQL keeps generating its values (no
longer gap-free or strictly increasing). One in a composite key, or a `BIGINT UNSIGNED`
key, keeps its type without value generation, so the application must supply the value.
For a `BIGINT UNSIGNED` key you can pick **Server-generated (IDENTITY)** instead: it then
becomes `bigint`, and a row whose key is above 9,223,372,036,854,775,807 does not load.
See [Choose the primary-key strategy](#choose-the-primary-key-strategy).

#### Fixed-point and floating-point

<!-- type-map: mysql -->
| MySQL type | Aurora DSQL type | Classification | What to know |
|---|---|---|---|
| `DECIMAL(p,s)`, `DECIMAL(p,s) UNSIGNED` (`NUMERIC`, `DEC`, `FIXED`) | `numeric(p,s)` | AUTO | Precision and scale are kept: MySQL's largest, `DECIMAL(65,30)`, is well within Aurora DSQL's limit of 1000. `UNSIGNED` is dropped, so the target accepts negative values too. |
| `FLOAT`, `FLOAT(M,D)`, `FLOAT UNSIGNED` | `real` | AUTO | `(M,D)` and `UNSIGNED` are dropped: values are copied exactly, but new values are no longer rounded to `D` decimals or refused when negative. |
| `DOUBLE`, `DOUBLE(M,D)`, `DOUBLE UNSIGNED` (`REAL`, `DOUBLE PRECISION`) | `double precision` | AUTO | As for `FLOAT`. |

#### Date and time

<!-- type-map: mysql -->
| MySQL type | Aurora DSQL type | Classification | What to know |
|---|---|---|---|
| `DATE` | `date` | AUTO | A zero or invalid date (`0000-00-00`, `2024-02-30`) has no Aurora DSQL value: if the source can hold one, Evaluation's `ZERO_DATES` item says what Full Load and CDC do with it. |
| `DATETIME`, `DATETIME(fsp)` | `timestamp` | AUTO | The same wall-clock value: no time zone is applied or shifted. Fractional seconds are kept. Zero dates: as for `DATE`. |
| `TIMESTAMP`, `TIMESTAMP(fsp)` | `timestamptz` | AUTO | The same instant, in UTC. Fractional seconds are kept. Zero dates: as for `DATE`. |
| `TIME`, `TIME(fsp)` | `time` | MANUAL `TIME_TYPE` | MySQL `TIME` is a duration (`-838:59:59` to `838:59:59`); Aurora DSQL `time` is a time of day. A value below `00:00:00` or above `24:00:00` is refused. If the column holds durations, change its type to `interval` in Schema Conversion before loading (to `text` if a key or index uses it). |
| `YEAR` | `smallint` | MANUAL `YEAR_TYPE` | Stored as the year number (`2024`). |

#### Strings, binary and structured

<!-- type-map: mysql -->
| MySQL type | Aurora DSQL type | Classification | What to know |
|---|---|---|---|
| `CHAR(n)` | `char(n)` | AUTO | |
| `VARCHAR(n)` | `varchar(n)` | AUTO | |
| `TINYTEXT`, `TEXT` | `text` | AUTO | |
| `MEDIUMTEXT`, `LONGTEXT` | `text` | MANUAL `OVERSIZED_LOB` | A value over Aurora DSQL's 1 MiB limit (its compressed size counts) cannot be stored: Full Load quarantines the row and CDC dead-letters it. The Data Migration prerequisite checks find such values before you load. |
| `BINARY(n)`, `VARBINARY(n)`, `TINYBLOB`, `BLOB` | `bytea` | AUTO | The bytes are copied exactly. |
| `MEDIUMBLOB`, `LONGBLOB` | `bytea` | MANUAL `OVERSIZED_LOB` | A value over 1 MiB cannot be stored (Aurora DSQL does not compress `bytea`): Full Load quarantines the row and CDC dead-letters it. |
| `ENUM('a','b',…)` | `text`, with a CHECK on the values | MANUAL `ENUM_SET_TYPE` | The CHECK keeps the allowed values. Sorting and comparison become alphabetical instead of the declaration order, and MySQL's empty-string error value (`''`) is refused. |
| `SET('a','b',…)` | `text` | MANUAL `ENUM_SET_TYPE` | Stored as the comma-separated members (`'a,b'`), with no CHECK: the allowed values are no longer enforced. |
| `JSON` | `json` | MANUAL `OVERSIZED_LOB` | The same JSON text. A document over 1 MiB (compressed) cannot be stored: Full Load quarantines the row and CDC dead-letters it. |

**Case-insensitive collations.** Most MySQL text columns use one (`utf8mb4_0900_ai_ci` is
MySQL 8's default). The collation is not carried: Aurora DSQL compares text
case-sensitively, so `=`, `LIKE`, `ORDER BY` and `UNIQUE` behave differently although
every value migrates exactly (a `UNIQUE` column that refused `Bob` beside `bob` accepts
both). Evaluation flags such tables MANUAL (`CI_COLLATION`). A `_bin` or `_cs` collation
is case-sensitive already and is not flagged.

#### Others

<!-- type-map: mysql -->
| MySQL type | Aurora DSQL type | Classification | What to know |
|---|---|---|---|
| `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, `GEOMETRYCOLLECTION` | `bytea` | MANUAL `SPATIAL_TYPE` | Aurora DSQL has no spatial types. The geometry is kept as its WKB bytes, without the SRID; spatial functions and `SPATIAL` indexes are not available. |

**Columns Aurora DSQL cannot index.** A `bytea` column (from `BINARY`, `VARBINARY`, the
`BLOB` types or a spatial type) cannot be part of a primary key or index: a primary key
over one makes the table UNSUPPORTED, and an index over one is not created (`BYTEA_KEY`).
Neither can an `interval` column, so a keyed `TIME` column that holds durations goes to
`text`.

### PostgreSQL → Aurora DSQL

Aurora DSQL is PostgreSQL-compatible, so most types are kept as they are. A type Aurora
DSQL does not have is changed to one that holds the same data (MANUAL: the application
must follow the new type), or, when no such type exists, left as it is and the table
graded UNSUPPORTED until you change the column in Schema Conversion. Full Load carries
every value of a converted column.

The **CDC** column says where CDC differs from Full Load. Evaluation reports these columns
as `CDC_UNCARRYABLE_COLUMN`; they matter only when you replicate with CDC:

- *Arrives NULL*: the column is missing from CDC's change events, so a row CDC inserts
  gets NULL (or the column's default) and an update leaves the old value. For a `NOT NULL`
  column with no default those changes fail instead; the `CDC_UNCARRYABLE_COLUMN` item
  says how.
- *Dead-lettered*: every change that sets the column is dead-lettered and does not reach
  the target.
- *Arrives altered*: CDC writes a different value than Full Load does.

Only a CHECKSUM validation sees *Arrives NULL* and *Arrives altered*.
[Chapter 4](04-cdc-and-dsql-constraints.md) and
[§6.2](06-limitations.md#62-migration-process-limits) say how to work around each.

#### Integers

<!-- type-map: postgres -->
| PostgreSQL type | Aurora DSQL type | Classification | CDC | What to know |
|---|---|---|---|---|
| `smallint`, `integer`, `bigint` (`int2`, `int4`, `int8`) | `smallint`, `integer`, `bigint` | AUTO | — | The same type. |

**Serial and identity columns.** `smallserial`, `serial`, `bigserial` and identity columns
are these integer types fed by a sequence. One that is the whole primary key becomes
`bigint GENERATED BY DEFAULT AS IDENTITY`, so Aurora DSQL keeps generating its values (see
[Choose the primary-key strategy](#choose-the-primary-key-strategy)). In a composite key or on another column
the sequence is not carried, so the application must supply the value (Evaluation names
such a non-key column: `NON_KEY_SEQUENCE`).

#### Fixed-point and floating-point

<!-- type-map: postgres -->
| PostgreSQL type | Aurora DSQL type | Classification | CDC | What to know |
|---|---|---|---|---|
| `numeric(p,s)` (`decimal(p,s)`) | `numeric(p,s)` | AUTO | — | Precision and scale are kept (Aurora DSQL accepts up to 1000, as PostgreSQL does). |
| `numeric` (`decimal`), with no precision | `numeric` | MANUAL `PG_UNCONSTRAINED_NUMERIC` | — | Aurora DSQL stores it as `numeric(18,6)`: a value with more than 6 decimal places is rounded, and one with more than 12 integer digits does not fit. If the column holds such values, give it an explicit `numeric(p,s)` in Schema Conversion before applying. |
| `real`, `double precision` (`float4`, `float8`) | `real`, `double precision` | AUTO | — | The same type. |
| `money` | `numeric(23,4)` | MANUAL `PG_UNSUPPORTED_TYPE` | — | The exact amount is kept, not its currency-formatted text. Currency formatting and `money` arithmetic move to the application. |

#### Date and time

<!-- type-map: postgres -->
| PostgreSQL type | Aurora DSQL type | Classification | CDC | What to know |
|---|---|---|---|---|
| `date`, `time without time zone`, `timestamp without time zone`, `timestamp with time zone` (`time`, `timestamp`, `timestamptz`) | `date`, `time`, `timestamp`, `timestamptz` | AUTO | — | The same type and precision. |
| `time with time zone` (`timetz`) | `timetz` | AUTO | — | Cannot be part of a primary key or index (`PG_UNINDEXABLE_COLUMN`). |
| `interval`, with or without fields or a precision (`interval day to second`, `interval(3)`) | `interval` | AUTO | — | The fields are kept and a precision is dropped from the declaration (not from the values). Cannot be part of a primary key or index (`PG_UNINDEXABLE_COLUMN`). |

#### Strings, binary and structured

<!-- type-map: postgres -->
| PostgreSQL type | Aurora DSQL type | Classification | CDC | What to know |
|---|---|---|---|---|
| `character varying(n)`, `character varying`, `character(n)`, `text` (`varchar(n)`, `varchar`, `char(n)`) | `varchar(n)`, `varchar`, `char(n)`, `text` | AUTO | — | The same type. On Aurora DSQL a value holds at most 65,535 bytes in a `varchar`, 4,096 in a `char` and 1 MiB in a `text`; for the columns with no length limit, Evaluation recommends a size check (`OVERSIZED_LOB`). |
| `character varying(n)` with n over 65535, `character(n)` with n over 4096 | `text` | MANUAL `PG_CHARACTER_LENGTH` | — | Aurora DSQL refuses such a declared length. `text` holds every value up to 1 MiB, but the length limit is no longer enforced: add `CHECK (length(col) <= n)` if the application relies on it. |
| `bytea` | `bytea` | MANUAL `OVERSIZED_LOB` | — | The bytes are copied exactly. A value over 1 MiB cannot be stored (Aurora DSQL does not compress `bytea`), and the column cannot be part of a primary key or index (`BYTEA_KEY`). |
| `boolean`, `uuid` (`bool`) | `boolean`, `uuid` | AUTO | — | The same type. |
| `json`, `jsonb` | `json`, `jsonb` | MANUAL `OVERSIZED_LOB` | — | A document over 1 MiB (compressed) cannot be stored: Full Load quarantines the row and CDC dead-letters it. Cannot be part of a primary key or index (`PG_UNINDEXABLE_COLUMN`). |
| `xml` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | The document text is kept; XML functions such as `xpath()` no longer apply. |

#### Others

<!-- type-map: postgres -->
| PostgreSQL type | Aurora DSQL type | Classification | CDC | What to know |
|---|---|---|---|---|
| A user-defined enum (`CREATE TYPE … AS ENUM`) | `text`, with a CHECK on the values | MANUAL `PG_UNSUPPORTED_TYPE` | — | Aurora DSQL has no `CREATE TYPE`. The CHECK keeps the allowed values and a default is kept as text; sorting becomes alphabetical instead of the enum's order. |
| A composite type (`CREATE TYPE … AS (…)`) | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | Arrives NULL | Aurora DSQL has no `CREATE TYPE`. Change the column to `text` in Schema Conversion (Full Load then copies the value's text form, such as `(1,abc)`). |
| A domain (`CREATE DOMAIN`) | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | — | Aurora DSQL supports domains, but the tool does not create them: create the domain on Aurora DSQL first, or change the column to the domain's base type (and its CHECK) in Schema Conversion. CDC treats the column as its base type. |
| `int4range`, `int8range`, `numrange`, `tsrange`, `tstzrange`, `daterange` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | Kept in its text form (`[1,5)`); range operators (`@>`, `&&`) and functions (`lower()`, `upper()`) no longer apply. |
| `int4multirange`, `int8multirange`, `nummultirange`, `tsmultirange`, `tstzmultirange`, `datemultirange` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | Arrives NULL | As for ranges (text form `{[1,3),[5,7)}`). |
| A user-defined range (`CREATE TYPE … AS RANGE`) | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | Arrives NULL | Change the column to `text` in Schema Conversion. |
| `point` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | Kept in its text form (`(1,2)`); geometric operators no longer apply. |
| `line`, `lseg`, `box`, `path`, `polygon`, `circle` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | Arrives NULL | As for `point`. |
| `inet`, `cidr`, `macaddr`, `macaddr8` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | The canonical address text is kept; network operators and functions no longer apply. |
| `bit(n)`, with n of 2 or more | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | Kept as the bit string (`'10101010'`); bit operators no longer apply. |
| `bit(1)` (`bit`), `bit varying(n)` (`varbit(n)`) | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | Arrives altered | As above. CDC writes a `bit(1)` value as `true`/`false`, and a `bit varying(n)` value shorter than n padded with leading zeros. |
| `bit varying` (`varbit`), with no length | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | Dead-lettered | As above. |
| `tsvector`, `tsquery` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | Arrives NULL | Kept as text; full-text search operators and indexes no longer apply. |
| `integer[]`, `text[]` and other arrays | `jsonb` | MANUAL `PG_UNSUPPORTED_TYPE` | — | Stored as a JSON array (`{1,2}` becomes `[1, 2]`): replace array operators and subscripts with JSON ones. A table keyed on an array column fails to apply (`PG_UNINDEXABLE_COLUMN`). |
| `numeric[]` (no precision), `time with time zone[]`, `bytea[]` | `jsonb` | MANUAL `PG_UNSUPPORTED_TYPE` | Dead-lettered | As for arrays. Give a `numeric[]` a precision on the source (`numeric(p,s)[]` is carried by CDC). |
| `interval[]`, `money[]`, `xml[]`, `point[]`, `bit(n)[]`, `bit varying(n)[]` | `jsonb` | MANUAL `PG_UNSUPPORTED_TYPE` | Arrives NULL | As for arrays. Arrays of the other types this table marks *Arrives NULL* arrive NULL too. |
| `"char"`, `name`, `oid` | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | — | PostgreSQL system types. Change the column to `char(1)`, `varchar(63)` or `bigint` in Schema Conversion. |
| `jsonpath`, `pg_lsn`, `xid`, `regclass` (and `regtype`, `regproc`, …) | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | Arrives NULL | PostgreSQL system types. Change the column to `text` in Schema Conversion. |
| Extension types: `citext`, `hstore`, `ltree`, PostGIS `geometry` and `geography` | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | — | Aurora DSQL has no extensions. Change the column to `text` in Schema Conversion (Full Load then copies the value's text form); a `citext` column then compares case-sensitively. |
| Extension types: pgvector `vector`, `halfvec`, `sparsevec`, and `cube` | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | Arrives NULL | Change the column to `jsonb` or `text` (`text` for `sparsevec`) in Schema Conversion. CDC cannot carry it whatever its type on Aurora DSQL. |

**Collations.** Aurora DSQL compares text with the `C` collation only. A column with
another collation (`PG_COLLATION_DROPPED`), or a database whose default collation is not
`C` (`PG_DATABASE_COLLATION`), sorts and compares text differently after the move,
although every value migrates exactly.

**Columns Aurora DSQL cannot index.** A `bytea`, `json`, `jsonb`, `interval` or
`time with time zone` column, and an array (now `jsonb`), cannot be part of a primary key
or index: a primary key over one makes the table fail to apply, and an index over one is
not created (`BYTEA_KEY`, `PG_UNINDEXABLE_COLUMN`).

---

## 2.4 Findings reference

Reference for the findings Evaluation reports, by source engine, plus the source time zone
and server settings. Use it as a lookup while you work through the report
([How to use the report](#how-to-use-the-report)).

### Common findings and what to do

These are the findings you are most likely to meet, grouped by what they mean for you. On
the screen each one also names the objects and values involved and gives the full advice.
They are split by source engine: read **Both engines** and the block for your own.

The time zone and the graded MySQL settings (`SOURCE_TIME_ZONE`, `SQL_MODE`,
`EXPLICIT_DEFAULTS_FOR_TIMESTAMP`, `AUTO_INCREMENT_STEP`) are explained in the next
section.

#### Both engines

**Blocks the migration** (Unsupported)

| Finding | What to do |
|---|---|
| No primary key (`NO_PRIMARY_KEY`) | Add one on the source (the finding names a unique key you can promote): the tool loads, replicates and validates every table by its key. |
| Triggers, stored procedures and functions, events; PostgreSQL materialized views and foreign tables (`TRIGGER_UNSUPPORTED`, `PROC_PLPGSQL`, `EVENT_UNSUPPORTED`, `PG_UNSUPPORTED_RELATION`) | They are not migrated. Rebuild the logic in the application, a schedule outside the database (for example Amazon EventBridge Scheduler with AWS Lambda), and a materialized view as a table that a job refreshes. |
| A primary key on a column Aurora DSQL cannot index (`BYTEA_KEY`, `PG_UNINDEXABLE_COLUMN`) | Re-key the table on the source, for example on a text or hash form of the value. Such columns are MySQL `BINARY`, `VARBINARY`, `BLOB` and spatial types (all `bytea` on Aurora DSQL), and PostgreSQL `bytea`, `json`, `jsonb`, `interval`, `time with time zone` and arrays. An index over one is not created (**Review needed**), so a `UNIQUE` one no longer enforces uniqueness. |
| Over an Aurora DSQL database limit (`TABLE_COUNT_LIMIT`, `SCHEMA_COUNT_LIMIT`, `VIEW_COUNT_LIMIT`) | The target database would hold more than 1,000 tables, 10 schemas or 5,000 views, counting what it already holds, and the score reads **Blocked**. Split the migration across clusters. |
| Over a table or name limit (`TOO_MANY_COLUMNS`, `TOO_MANY_KEY_COLUMNS`, `IDENTIFIER_LENGTH`, `NAME_COLLISION`) | More than 255 columns, a primary key over more than 8 columns, or (on MySQL) names that agree in their first 63 bytes: two such columns make the table's `CREATE TABLE` fail, and of two such tables or views neither is created. Change the table or the names on the source. |

**Changes or refuses data** (Review needed)

| Finding | What to do |
|---|---|
| A value over Aurora DSQL's 1 MiB limit (`OVERSIZED_LOB`) | Check sizes before the load (step 2 of [How to use the report](#how-to-use-the-report)): Full Load quarantines a row with such a value, and CDC dead-letters it. |
| A type Aurora DSQL lacks is changed (`PG_UNSUPPORTED_TYPE`, `ENUM_SET_TYPE`, `BIT_TYPE`, `YEAR_TYPE`, `SPATIAL_TYPE`, `PG_CHARACTER_LENGTH`) | The data migrates in a stand-in type (a PostgreSQL array as `jsonb`, an enum as `text` with a CHECK, `money` as `numeric(23,4)`, a MySQL spatial column as `bytea`): adapt the application to it. |
| Text compares differently (`CI_COLLATION`, `PG_COLLATION_DROPPED`, `PG_DATABASE_COLLATION`) | Aurora DSQL compares text by its bytes (the `C` collation), and a source collation is not carried. After a MySQL case-insensitive one, `=`, `LIKE`, `ORDER BY` and `UNIQUE` become case-sensitive (`Bob` and `bob` are two values); after another PostgreSQL one, text sorts differently. Review those queries and keys. |

**Needs an application change before cut over** (Review needed)

| Finding | What to do |
|---|---|
| A dropped default, or MySQL `ON UPDATE CURRENT_TIMESTAMP` (`DEFAULT_DROPPED`, `ON_UPDATE_TIMESTAMP`) | Have the application set the value: an `INSERT` that leaves the column out stores NULL (or is refused on a `NOT NULL` column), and an update no longer refreshes the timestamp. For a dropped default you can also add one after apply. |
| A generated column that becomes ordinary (`GENERATED_COLUMN`) | Nothing computes the value after cut over: compute it in the application, or re-add the expression before applying ([What the conversion does for you](#what-the-conversion-does-for-you)). A PostgreSQL `STORED` column is kept. |
| Source `CHECK` constraints (`CHECK_CONSTRAINT_DROPPED`) | They are not carried. Re-add each one Aurora DSQL accepts after the load (the conversion note gives the statement), or enforce the rule in the application. |
| A view the tool cannot reproduce (`VIEW_NOT_CONVERTED`, `VIEW_UNSUPPORTED_SQL`, `VIEW_REMODELLED_COLUMN`) | As converted it would fail to apply or return different results: edit its target DDL before applying, or re-create it by hand after the load. |
| An index that is not carried as it was (`UNSUPPORTED_INDEX_TYPE`, `PG_INDEX_NOT_PRESERVED`, `EXPRESSION_INDEX`, `TOO_MANY_INDEXES`) | Move full-text, spatial, GIN or GiST searches outside the database (for example Amazon OpenSearch Service), re-create an expression index after the load (`CREATE INDEX ASYNC`), and keep at most 23 secondary indexes per table. |

#### MySQL only

**Changes or refuses data** (Review needed)

| Finding | What to do |
|---|---|
| `TINYINT(1)` becomes `boolean` (`TINYINT_BOOLEAN`) | A value other than 0 or 1 is refused: Full Load stops the table, and CDC dead-letters the row. If the column holds other numbers, change its target type to `smallint` before applying. |
| `TIME` used for durations (`TIME_TYPE`) | A value below 00:00:00 or above 24:00:00 is refused. For durations, change the column's target type to `interval` before loading. |
| Zero or invalid dates (`ZERO_DATES`) | Aurora DSQL has no such value, so Full Load and CDC write NULL, the column's default or `1970-01-01` instead ([§6.2](06-limitations.md#62-migration-process-limits)). Count them with the finding's queries, and correct them on the source before the load. |

**Needs an application change before cut over** (Review needed)

| Finding | What to do |
|---|---|
| Mixed-case names (`MIXED_CASE_IDENTIFIERS`) | Aurora DSQL folds an unquoted name to lower case, so SQL that writes `CustomerId` unquoted finds nothing. Quote such names exactly as spelled, or rename them to lower case on the source before Schema Conversion (never on the target before cut over). |
| A cascading foreign-key action under CDC (`FK_CASCADE_CDC_GAP`) | CDC does not replicate MySQL's cascades: see [Foreign keys](#foreign-keys). |

#### PostgreSQL only

**Blocks the migration** (Unsupported)

| Finding | What to do |
|---|---|
| Row-level security (`PG_ROW_LEVEL_SECURITY`) | After cut over every role that reaches the table reaches every row: enforce the policies in the application (the finding lists them). Migrate as a user that reads every row (a superuser, a role with `BYPASSRLS`, or the owner of a table that is not `FORCE ROW LEVEL SECURITY`), or the load stops on that table. |
| A column type with no stand-in (`PG_UNSUPPORTED_TYPE`) | A composite type, a domain, an extension type (`citext`, `hstore`, PostGIS, pgvector) or a system type: change the column's target type in Schema Conversion (for example to `text`) before applying. |

**Changes or refuses data** (Review needed)

| Finding | What to do |
|---|---|
| `numeric` with no precision (`PG_UNCONSTRAINED_NUMERIC`) | Stored as `numeric(18,6)`, which rounds past 6 decimal places and holds at most 12 integer digits. If the values need more, give the column an explicit `numeric(p,s)` before applying. |
| A column CDC cannot carry (`CDC_UNCARRYABLE_COLUMN`) | With CDC only: the value arrives NULL or altered, or the row is dead-lettered. Before you deploy CDC, change the column's type on the source, exclude it, or move the table with Full load only. |

**Needs an application change before cut over** (Review needed)

| Finding | What to do |
|---|---|
| A `serial` or identity column that is not the key (`NON_KEY_SEQUENCE`) | It has no generator on the target, so an `INSERT` that leaves it out is refused (`23502`), or stores NULL in a nullable column. Have the application supply the value, or declare the column `bigint GENERATED BY DEFAULT AS IDENTITY` in the target DDL before applying (its type cannot change later) and restart it past the loaded values before cut over. |
| A sequence no column owns (`PG_STANDALONE_SEQUENCE`) | It is not created. Create it before cut over (the finding gives the statement), and move it past the source's last value at cut over: the tool does not sync it ([§10.3](10-conclusion.md#103-the-cut-over-switching-your-application-to-dsql)). |
| Rules Aurora DSQL cannot enforce as declared (`PG_EXCLUSION_CONSTRAINT`, `PG_TEMPORAL_KEY`, `PG_FK_PERIOD`) | An `EXCLUDE` constraint, a `WITHOUT OVERLAPS` key and a `FOREIGN KEY … PERIOD` are not enforced after cut over: enforce the rule in the application, or change the design. For a large table with a `WITHOUT OVERLAPS` primary key, add a btree index on its key columns on the source before loading. |
| A `DEFERRABLE` primary or unique key (`PG_DEFERRABLE_KEY`) | It is checked at once, so a write that briefly gives two rows one key value (swapping or renumbering) fails. With CDC, such a primary key cannot be the table's `REPLICA IDENTITY`: a table left at `REPLICA IDENTITY DEFAULT` fails the CDC prerequisite check until you re-create the key `NOT DEFERRABLE` on the source or set the table `REPLICA IDENTITY FULL`. |
| An installed extension (`PG_EXTENSION_UNSUPPORTED`) | Aurora DSQL has no extensions: replace the application's calls to it (`gen_random_uuid()` is built in). |

#### Advice, and an incomplete report

**Advice only** (Recommended). Nothing to do for the migration itself:

- `AUTO_INCREMENT`: the key keeps generating
  ([Choose the primary-key strategy](#choose-the-primary-key-strategy)). When nothing will
  generate it on Aurora DSQL, the finding is **Review needed** instead.
- `FK_PRESERVED`: the foreign keys are kept and applied after the load
  ([Foreign keys](#foreign-keys)).
- `PARTITIONED_TABLE`: the partitions become one table, and no data is lost. With CDC,
  pause partition maintenance (drop, truncate, detach, attach) until cut over: CDC cannot
  carry those changes.
- `MULTIPLE_DATABASES` (MySQL): each database becomes its own schema; point the
  application at schema-qualified names, or set its `search_path`.
- `NAME_COLLISION` (an index): an index whose name another table, view or index of the
  schema uses is created as `<table>_<index>`.
- `PG_FK_NOT_ENFORCED` (PostgreSQL 18): the key is not created, so the target behaves like
  the source; to have Aurora DSQL enforce such a key (not one declared with `PERIOD`,
  which Aurora DSQL has no form of), add its `ALTER TABLE … ADD CONSTRAINT … NOT VALID` to
  the table's target DDL.

**When the report is incomplete,** it says so. On MySQL: objects the migration user cannot
list (`SOURCE_VISIBILITY`: it lacks `TRIGGER`, `EVENT` or `SHOW_ROUTINE`, and the first
two also allow changing those objects), and views whose definition it cannot read
(`VIEW_DEFINITION_UNREADABLE`: it lacks `SHOW VIEW`). On PostgreSQL: a catalog read that
failed (`PG_CATALOG_UNREAD`). On both: tables that disappeared while Evaluation read the
catalog (`CATALOG_CHANGED_DURING_READ`). Objects it could not see are neither assessed nor
migrated: grant what is missing (or review those objects as an administrator), or re-run
Evaluation.

### Source time zone and server settings

An Aurora DSQL session runs in UTC unless it sets another zone, while a column without a
time zone holds the source's wall clock as it was written. So when the zone Evaluation
read is not UTC, Schema Conversion writes that zone into whatever reads the clock,
automatically: a `DATETIME` (`timestamp`) default becomes
`DEFAULT (now() AT TIME ZONE 'Asia/Seoul')`, and a MySQL view's `NOW()` reads that zone's
clock. A PostgreSQL view is copied as it is, so the item lists the views that read the
clock for you to review.

The `SOURCE_TIME_ZONE` item (**Review needed**) names those columns and views. Your part
is in the application: if it relies on the session's zone (`NOW()` or `CURRENT_DATE`
written into such a column, or compared with one), it runs `SET TIME ZONE 'Asia/Seoul'` on
every new Aurora DSQL connection, or names the zone in its SQL. When the zone cannot be
told, an advisory item says so and the conversion assumes UTC: if the source is not in
UTC, edit those defaults in Schema Conversion and set the zone in the application. The
zone read is the one an application session gets by default: on MySQL the GLOBAL
`time_zone` (or the one the parameter group's `init_connect` sets), on PostgreSQL the
default `TimeZone`, including an `ALTER DATABASE` or `ALTER ROLE` default but not the
migration user's own.

On a MySQL source, three more settings are graded, each a database-level **Review needed**
item (`ZERO_DATES` is in the tables above):

| Setting on the source | Finding | What it means on Aurora DSQL |
|---|---|---|
| `sql_mode` without `ONLY_FULL_GROUP_BY` | `SQL_MODE` | A view or query that selects a column its `GROUP BY` does not determine is refused (`42803`), a view when Schema Conversion applies it. The same item flags `ANSI_QUOTES` and `PIPES_AS_CONCAT`, under which SQL you paste into the Query Converter means something else. |
| `explicit_defaults_for_timestamp=OFF` | `EXPLICIT_DEFAULTS_FOR_TIMESTAMP` | Writing NULL to a `TIMESTAMP` column declared `NOT NULL` stored the current time on MySQL; Aurora DSQL refuses that write. Have the application write the time, or leave the column out of the `INSERT`. |
| `auto_increment_increment` above 1 | `AUTO_INCREMENT_STEP` | The identity on Aurora DSQL steps by 1: move any logic that relies on the step (key ranges per writer) into the application. |

---

## 2.5 AI assist (optional)

AI assist works in both steps. This section says what it does and how to turn it on.

### Optional AI assist (Amazon Bedrock)

AI assist is optional, off by default, and runs only on Amazon Bedrock. It helps with the
parts of the conversion that need a human decision:

- In **Evaluation**, the **Ask AI about this finding** icon beside a finding opens the
  **AI DBA** panel on that finding.
- In **Schema Conversion**, the AI icon beside a conversion warning, or
  **Ask AI DBA about converting this object**, opens the AI DBA on that object. When a
  reply holds DDL, **Use as target DDL** copies it into the object's target DDL, where you
  review and apply it like any edit. For triggers, routines and events,
  **Ask AI DBA how to reimplement these** says how to rebuild each one.

It never replaces the deterministic conversion (with AI off, the workflow is the same),
nothing it writes reaches the target until you apply it, and it sees schema and DDL
metadata, never Full Load or CDC row data.

**How to turn it on:**

1. On the **Connect** screen, expand **AI Assist** and switch on **Enable AI Assist** (the
   model and region are under **Bedrock settings**). There is no API-key field: AI assist
   calls Bedrock with the tool's AWS credentials, which need `bedrock:InvokeModel` and
   `bedrock:InvokeModelWithResponseStream` on the chosen model (default
   `global.anthropic.claude-sonnet-5`). On the AWS-hosted deployment the stack must enable
   it too: see
   [Enable AI-assisted conversion (optional)](../../../deploy/DEPLOYMENT.md#enable-ai-assisted-conversion-optional).
   Each call adds Bedrock cost and some latency.
2. Use the **Verify AI access** preflight — a non-blocking check that calls the configured
   model with these credentials (a valid AWS profile alone does **not** guarantee
   `bedrock:InvokeModel`). If Bedrock rejects that model as unavailable, or denies it for
   a reason other than its model agreement, the check also tries a fallback model; if the
   fallback answers, the Model ID switches to it and the result is a warning that names
   both models.

**If the model is not enabled for the account.** Anthropic models on Bedrock are enabled
once per AWS account through an AWS Marketplace agreement. The AI DBA panel and the AI
Assist status on Connect then say *the configured model is not enabled for this AWS
account yet* (a missing first-time use form can instead show as *not enabled or not
available*). Someone with AWS Marketplace permissions accepts the model's agreement in the
Amazon Bedrock console (and submits the form if it asks for one); allow about 2 minutes,
then run **Verify AI access** again (the command-line steps are in
`deploy/DEPLOYMENT.md`). In a new account a pass can be temporary: calls can succeed for
up to about 15 minutes before a missing agreement stops them.

---

**Next:** [3. Full Load →](03-full-load.md)
