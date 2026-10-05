# 13. Excluding tables and columns

_Language: **English** | [한국어](../ko/13-excluding-tables-and-columns.md) | [日本語](../ja/13-excluding-tables-and-columns.md)_

> **Prev:** [Appendix: Performance test results](12-performance-test-results.md)

Not everything in the source has to reach Aurora DSQL. You may need to keep personal
data (national IDs, password hashes, dates of birth) out of the target. A large-object
column may hold values over Aurora DSQL's per-value limit: such rows would otherwise be
quarantined at load, and CDC cannot apply them either (Chapter 4 §4.5). And some tables, such as audit or staging
tables, may not be needed at all. You choose all of this in one place, the
**Exclude tables & columns** popup on the **Data Migration** step. It works the same
way for every migration type (Full load only, Full load + CDC, CDC only), and you
write the exclusions as short text rules instead of ticking boxes.

> **Mental model:** you write rules, and Save turns them into one exclusion set. Every
> path reads that same set: Full Load, Start CDC, the CDC capture list, the prerequisite
> checks, Validation and Cut over. An excluded **table** is not migrated. An excluded
> **column** still exists on Aurora DSQL, but every row the tool writes holds **NULL**
> there.

---

## 13.1 What excluding does

| | Excluding a table | Excluding a column |
|---|---|---|
| Rule | `exclude table shop.audit_log` | `exclude shop.users.ssn` |
| In the tool | The table is unselected in the table tree, the same as unticking it. It is not loaded and CDC does not capture it. | The table still migrates. The tool never writes this column's source values to Aurora DSQL. |
| On Aurora DSQL | Left as is. It is not loaded, and it is **never dropped**, so a table that already exists keeps its rows. | The column stays in the table (as Schema Conversion defined it). Before the first row is written, it is made nullable with no DEFAULT, so every row the tool writes stores NULL there (§13.6). |
| Validation | Not compared. | The rows are counted and compared as usual. The excluded column is not value-compared, but Validation proves it is NULL (§13.9). |

Some points to understand early:

- **NULL, not the DEFAULT.** An excluded column is loaded as NULL, not filled with the
  target column's DEFAULT. This is also why a NOT NULL column with no DEFAULT can be
  excluded: the tool drops the NOT NULL first. Dropping NOT NULL is **one-way**
  (§13.4).
- **Saving changes nothing on Aurora DSQL.** The target changes happen only when a load,
  Start CDC or Change & reload runs, and each of these shows them to you first.
- **Some columns can never be excluded.** Key columns, columns that a preserved foreign
  key references, and columns Aurora DSQL computes are refused, and the reason is always
  shown (§13.4).
- **The set becomes fixed once data has moved.** After a Full Load, or once a CDC stack is deployed, the exclusions are locked. They can still be changed, but only through the routes in §13.7. In Full load only, which tables migrate can still change after a Full Load; in Full load + CDC, tables can still be dropped or added until CDC starts, and a table you add is loaded on the Full Load step first, normally by Retry unfinished tables under the Full Load's start point (§13.7).

---

## 13.2 The Exclude tables & columns popup

### Where it is

On the **Data Migration** step, directly under **Tables to migrate** in the table
list, there is an **Exclude tables & columns...** button. A status line beside it
says what is excluded now:

| Status line | Meaning |
|---|---|
| `Nothing excluded` | No table or column is excluded now (no rules are saved, or the saved rules exclude nothing). |
| `Rules exclude 2 tables, 5 columns` | The saved set. "Tables" counts only the tables a rule unselected, not the ones you unticked yourself. |
| `... excluded - locked` | The set can no longer be edited directly (§13.7). |
| `Your rules now ... - review` (warning) | The saved rules would resolve differently against today's schema. For example, a new column matches a wildcard, or a column's type changed so that a `type:` rule no longer matches it. (A saved column that has disappeared from the source shows the red line below instead.) Such drift is **never applied by itself**: open the popup and Save to accept it. The Start Full Load dialog repeats this warning. |
| `N saved exclusions no longer apply. Starting is blocked until you review them.` (red, with **Review**) | A saved entry is refused or names something the source no longer has (§13.4). |

A second line appears when the selected tables have columns that can exceed Aurora
DSQL's value limit (§13.5). It turns into a warning once a size check (**Check sizes**
or the prerequisite checks) has measured such a value.

### Writing and checking rules

The popup has a rules editor (one rule per line; the **?** icon shows examples) and a
live preview under it. About 300 ms after you stop typing, the preview updates:

| Part | What it shows |
|---|---|
| Error box | `N lines need fixing. Save is disabled.` Each problem is listed with its line, and with a one-click fix where one exists. |
| Line status | One line per rule, for example `L3  matches 4 columns - 1 kept by include on L5 - 2 blocked (primary key, computed)`. A `type:` rule shows its count per type (`longtext: 3 columns in 2 tables`). Flags such as `only unselected tables`, `all blocked` or `cancels nothing` mark a line that does less than it seems. |
| Result | The tables your rules unselect, the columns excluded ("migrated as NULL"), every refused column with its reason, and how many excluded columns have a warning (§13.4; each warning is listed in Review all, Status: Warning). |
| Review all N items | One paginated table of everything involved, filterable by **All / Excluded / Blocked / Warning**. It shows each item's status, the lines that matched it, and why it is there or which target change it gets. |
| Oversized-LOB candidates (N) | Collapsed. See §13.5. |
| Changes to Aurora DSQL (N statements, run when the load starts) | Collapsed. The exact `ALTER` statements in run order (§13.6). For CDC only, it reads "run when CDC starts". |
| CDC capture list (B of 4,096 bytes) | CDC migration types only. The exact `ColumnExcludeList` that Start CDC would send (§13.8). |
| Save diff | What Save changes, for example `+1 column, -0; unselects 2 tables, re-selects 1`. |

**Save stays disabled** while any of these problems remains:

- a syntax error;
- a name that matches nothing. The suggestion comes from the first level that fails
  (schema, then table, then column): first a name that differs only in case, otherwise
  up to three close matches;
- a table rule, or a deleted table rule, that would change the table set while it is locked, which applies:
  - in CDC only after a Full Load;
  - in Full load + CDC once CDC has started;
  - while CDC streams or a stack holds the set;
  - in Full load only and Full load + CDC while a Full Load, a foreign-key pass or a cut-over DEFAULT restore runs, until it finishes;
  - in Full load + CDC also while the CDC infrastructure is created or deleted, while a failed create is in the account, or while the CDC state is read;
- a column rule that matches a column of a table whose name contains `:` (§13.11);
- a source column that is missing from the Aurora DSQL table and is not excluded, so
  the load would certainly fail with 42703 (§13.4). The fix appends the `exclude` line
  for it, but you must click it: a column is never dropped from a migration
  automatically;
- a CDC capture list over 4,096 bytes, even in its compact form (§13.8);
- rules that would leave no table selected.

A fix for one line rewrites only that line. A fix for the whole set, such as the 42703
case, appends a rule line. If that line has changed since its fix was shown, the fix
does nothing and the preview is checked again. Save also waits until the preview has
caught up with what you typed, so it never stores something you have not seen.

### Saving, cancelling, and more than one tab

- **Save** stores the rule text together with the resolved result: the excluded
  columns, the table selection, and which tables the rules unselected. It changes
  nothing on Aurora DSQL.
- **Esc and a click on the backdrop do not close the popup**, so you cannot lose an edit
  by accident. **Cancel** (or the close icon) discards the edit.
- **Each browser tab gets at most one popup.** If the exclusions changed after you
  opened it (in another tab, through an attached stack, or because a run or a deploy
  started), or if a lock took hold, Save is refused and your text is kept.
- The rules, the tables they unselected, your re-ticks and any pending lines (§13.7)
  are saved with the session and restored with it.

### When the popup is read-only

When the exclusions are locked, the popup opens read-only. It shows:

- the cause of the lock and the remedies that work in this session;
- the saved rules, the pending lines, and a note on what each saved entry does now;
- for a deployed stack, its capture list as deployed. If the stack has already streamed
  and the session's list differs from it (so Start CDC would refuse it), both lists are
  shown side by side.

The buttons it can offer are **Change & reload** and **Change migration type**. The
second one only scrolls to the migration-type selector; the tool never switches the
type for you. See §13.7.

---

## 13.3 Rule language reference

| Line | Meaning |
|---|---|
| `exclude table schema.table` | Leave the table out of the migration (unselect it). |
| `exclude schema.table.column` | Migrate the column as NULL. |
| `exclude type:name[,name...]` | Exclude every column of these **source** data types. |
| `include ...` | Any of the three forms above, as an exception: it cancels a matching `exclude`. |
| `# ...` | A comment, on its own line or after a rule. Blank lines are ignored. |

On a MySQL source, `schema` is the database name. On PostgreSQL, it is the schema.

**Names and keywords.**

- `exclude`, `include`, `table` and `type:` are case-insensitive. **Names are
  case-sensitive.** `shop.Users` does not match `shop.users`, and the error offers the
  right spelling.
- A column rule always has three parts. A two-part name without `table` is an error,
  and its fix inserts `table`.
- Write one rule per line. A comma list is allowed only after `type:`, and it may not
  contain spaces: `type:longtext,longblob`, not `type:longtext, longblob`. The error
  for the second form offers **Remove the spaces**.
- Text after a rule that is not a comment is an error, and its fix turns that text into
  a comment.

**Wildcards.** `*` is the only wildcard. It matches any run of characters, including
none, **within one name part**, and never crosses a `.`:

- `exclude table shop.audit_*` matches every table in `shop` whose name starts with
  `audit_`;
- `exclude shop.*.password_hash` matches that column in every `shop` table;
- `exclude *.*.ssn` matches every `ssn` column.

`_`, `%` and `?` are ordinary characters. A quoted `"*"` is a literal asterisk, and
type names take no wildcard.

**Quoting.** Put a name part in double quotes when it contains a space, `.`, `,`, `*`,
`#` or `"`. Inside the quotes, write `"` as `""`:

```
exclude "my schema"."order.items".notes
exclude shop.reviews."say ""hi"""
```

A name that contains a line break cannot be written as a rule. Exclude its table
instead, or change the target DDL in Schema Conversion.

**Include wins, whatever the line order.** If an `include` and an `exclude` match the
same item, the item is kept. The order of the lines never matters (unlike AWS DMS
selection rules). Tables and columns are separate: `include table`
cancels only table rules, and a column or `type:` include cancels only column rules.

**Scope.** Column and `type:` rules only take effect on selected tables. If every match
of a line is on an unselected table, the line is flagged `only unselected tables`.
Column rules on a table that a table rule unselects in the same Save have no effect.

**Table rules remember what they unselected.** `exclude table s.t` unselects those
tables in the tree, and the popup records which tables a rule unselected. If you later
delete the rule (or add an `include table`), exactly those tables are selected again.
A table you unticked yourself stays unticked. If you tick a rule-unselected table again
in the tree, that is treated as a deliberate override: the row and the popup name it,
but your next Save unselects it again unless you also change the rule.

**`type:` keys.** A `type:` rule matches the **source** column type, reduced to one key:

| Source | Key | Examples |
|---|---|---|
| MySQL | The base type in lower case, cut at `(` or a space | `int unsigned` → `int`; `decimal(10,2) unsigned` → `decimal`; `varchar(255)` → `varchar`; `longtext` |
| PostgreSQL | The `format_type` name without typmods. Multi-word names are kept (quote them), `[]` is kept, and the case of user-defined types is kept | `numeric(10,2)` → `numeric`; `character varying(40)` → `"character varying"`; `integer[]`; `interval day to second(3)` → `interval` |

You can type the key exactly, with typmods (`type:varchar(255)` means `type:varchar`),
or in a different case (`type:TEXT` finds `text`). There are no aliases: on
PostgreSQL, `type:varchar` does not match `character varying`, but the error suggests
`type:"character varying"`. The same applies to MySQL `type:integer`, which suggests
`int`.

**Examples.** A typical set for personal data and large objects on a MySQL source:

```
# Personal data: never copy these values
exclude *.*.ssn
exclude shop.*.password_hash
exclude shop.customers.date_of_birth

# Large objects that can exceed Aurora DSQL's per-value limit
exclude type:longtext,longblob
include shop.pages.body        # page bodies are small: keep them

# Tables this migration does not need
exclude table shop.audit_*
exclude table shop."tmp import"
```

The same idea on a PostgreSQL source:

```
exclude public.users.national_id
exclude type:bytea,jsonb
include public.settings.preferences
```

A line that matches nothing is an error, so remove the lines that do not apply to this
schema. For example, `exclude *.*.ssn` is an error when no table has an `ssn` column.

---

## 13.4 What cannot be excluded, and the warnings

### Columns that are refused

One eligibility rule decides for every path, and its reason is always shown. A refused
column is listed in the preview as `(not excluded)` and is never stored. You can still
Save; the column simply migrates.

| Column | Shown as | Why |
|---|---|---|
| A source primary-key column | primary key | The tool needs it to read, de-duplicate and replicate rows. |
| A target primary-key column chosen in Schema Conversion: the leading column of a composite key, a column of a named `CONSTRAINT ... PRIMARY KEY` in an edited DDL, or a CDC re-key column (`message.key.columns`) | target key | The loader reads every target-key column of every row, and CDC needs it to write and delete each row by that key. |
| A column that a preserved foreign key of a selected table references (only while foreign keys are preserved) | referenced by a foreign key | Every child row would become an orphan, and the foreign key could not be added. |
| A column Aurora DSQL computes: a preserved `GENERATED ... STORED` column, or an identity column outside the primary key | computed | Aurora DSQL computes it and the tool never writes it. Dropping a DEFAULT cannot make an identity column NULL. |

### Saved exclusions that no longer apply

Saved exclusions can come from an older session or a restore, so they are judged again
every time:

- **Before any data has moved under the set,** an entry the rule refuses, or one that
  names a table or column the source no longer has, holds **Continue / Start Full
  Load** (including the page-header Run), **Deploy CDC infrastructure** and **Attach**
  to an existing stack: "N saved column exclusions no longer apply: ... Remove them in
  Exclude tables & columns (under the table list) first."
- **After data has moved,** nothing is held, because the set is already part of what
  was loaded or captured. Each entry is reported instead. A key entry "is still
  migrated" (key entries are dropped from the set on every path, so Validation
  compares that column). A computed one "is already omitted". A column referenced by a
  foreign key "stays excluded, but: ...".

### Warnings (allowed, but read them)

These warnings do not block Save. The Start dialogs repeat only the first two, as the
DROP NOT NULL and DROP DEFAULT counts and statements. Change & reload shows all of them
for the tables it reloads.

| If the excluded column... | What happens |
|---|---|
| is NOT NULL on Aurora DSQL | NOT NULL is dropped. **This is one-way:** Aurora DSQL cannot add NOT NULL back in place, and only a later Drop & reload of the table restores it. The column stays nullable after cut over. |
| has a DEFAULT | The DEFAULT is dropped for the migration and restored at Cut over. The restored DEFAULT applies to new rows only (§13.9). |
| is the child column of a preserved foreign key | Its relationship to the parent is not migrated (NULL passes the foreign key). |
| is in a UNIQUE constraint or index | Uniqueness is not enforced for the migrated rows, because they are all NULL. |
| is used by a generated column that is kept | If the generated column is NOT NULL and its expression yields NULL when this column is NULL, every row is rejected (23502). If it is nullable, it is computed from NULL and differs from the source. |
| is mentioned by a view (text match) | The view reads NULL. |

A hand-written `CHECK` constraint that rejects NULL is **not** detected in advance.
Such a table fails loudly at load (23514).

Excluding a **table** also has consequences, which the preview lists:

- it is not loaded and is left as is on Aurora DSQL (not dropped);
- a selected child table's foreign key to it is checked against the rows this table has
  on Aurora DSQL when foreign keys are applied (on the Data Migration step for Full load
  only, at Cut over for CDC): applied if every referenced row is there, skipped by the
  orphan check if one is missing, and not applied if the table is not on Aurora DSQL;
- a view that reads it returns no rows;
- column rules on it have no effect.

### A source column missing from the Aurora DSQL table (42703)

Suppose you delete a column from the target DDL in Schema Conversion but do not exclude
it. Every Full Load batch for that table would then fail with 42703, and CDC would
dead-letter every change to it. So **Start Full Load**, **Deploy CDC infrastructure**
and **Start CDC** are held with "shop.t.c is not in your Aurora DSQL table; ... Add it
back to the target DDL in Schema Conversion. Columns are never dropped from a
migration automatically." Either add the column back, or exclude it (the popup offers
the line). Only a certain failure is reported: both the applied DDL and the live target
table must lack the column.

### Prerequisite checks

The prerequisite checks know about exclusions:

- `TARGET_COLUMNS_LOADABLE` treats an excluded NOT NULL column with no DEFAULT as
  loadable, because the load (or Start CDC) makes it nullable first.
- On PostgreSQL, `COLUMN_REPLICABLE` skips excluded columns, because CDC never captures
  them.
- If you change the exclusions after the checks ran, a neutral note says so
  ("Exclusions changed since these checks ran at HH:MM UTC; size results describe the
  earlier set.") instead of blocking. A table added after the checks still blocks.

---

## 13.5 Oversized LOB candidates and Check sizes

Aurora DSQL limits the size of a single value (1 MiB for most types; see
[Chapter 4 §4.5](04-cdc-and-dsql-constraints.md#45-the-1-mib-per-value-limit-and-the-dlq)).
At load, a row with a larger value is quarantined: it is kept out of Aurora DSQL, and
past 1,000 quarantined rows the table fails. CDC cannot apply such a row either (§4.5
describes what happens to it). If you exclude the column instead, the rows are kept and
the column is NULL.

The popup's collapsed **Oversized-LOB candidates (N)** section lists the in-scope
columns whose **type** can hold a value over the limit:

- **MySQL:** `mediumtext`, `longtext`, `mediumblob`, `longblob`, `json`;
- **PostgreSQL:** `text`, `bytea`, `json`, `jsonb`, and `character varying` (or a bare
  `bpchar`) with no length. A column that a CHECK limits to a fixed set of literals is not
  listed.

These are the same columns Evaluation flags `OVERSIZED_LOB`. Each row shows the source
and target type, what the type allows (for example "Type allows up to 4 GiB."), the
limit, and a **+ add rule** button that adds `exclude schema.table.column` to the
editor. A column the eligibility rule refuses shows its reason instead. A column that
an `include` keeps shows a button for that include's line (for example **Go to L5**).

**The popup measures nothing by default** (the prerequisite checks run their own size
check; a column they measured shows that result here too). Note that a type is only a
ceiling: a `longtext` or `json` column may never hold more than a few kilobytes. To
know for sure, click **Check sizes**:

- it measures the candidates on the current page (up to 25) with one bounded query per
  column on the source. Each query stops at the first value over the limit and runs for
  at most 15 s. The cost, including the worst case, is stated before you click;
- only one pass runs at a time, and **Cancel** stops after the current column;
- each column gets one of three results: **Over the limit found**, **No value over the
  limit**, or **Not determined** (the read failed, took longer than 15 s, or was
  cancelled). "Not determined" is never counted as a pass;
- **+ add rules for the N over the limit** adds a rule for every column found over the
  limit.

Each column is judged against the limit of its **applied target type**. For example,
after you change a column to `varchar` in Schema Conversion, that type's limit is used.
On MySQL, a text column that is not in `utf8mb4` is measured in its own charset, and
the result says so, because Aurora DSQL stores UTF-8, which can be larger.

The section also says what it does not highlight: MySQL spatial columns;
PostgreSQL arrays, `xml`, `tsvector` and geometric types; and large `varchar(n)` or
`char(n)` columns holding multi-byte text. A row over 2 MiB is not checked. You can
still exclude any of these with a rule.

---

## 13.6 What happens on Aurora DSQL

### The overlay: nullable, with no DEFAULT, before the first row

Before a table's first row is written, the load makes each excluded column nullable
with no DEFAULT. Per column, it first drops NOT NULL and then the DEFAULT:

```sql
-- shop.users.ssn: NOT NULL, no DEFAULT
ALTER TABLE "shop"."users" ALTER COLUMN "ssn" DROP NOT NULL
-- shop.users.status: NOT NULL DEFAULT 'active'
ALTER TABLE "shop"."users" ALTER COLUMN "status" DROP NOT NULL
ALTER TABLE "shop"."users" ALTER COLUMN "status" DROP DEFAULT
```

- **Only what is needed runs.** An already nullable column gets no `DROP NOT NULL`, and
  a column with no DEFAULT gets no `DROP DEFAULT`.
- **The order matters.** In the reverse order, a concurrent insert that omits the
  column can reach a NOT NULL column that has no DEFAULT and be rejected (23502).
- **One statement at a time.** The ALTERs run serially, because concurrent DDL on
  Aurora DSQL fails with 40001, and they are retried on 40001. Each table ends with a
  catalog read-back.
- **The DEFAULT is recorded first.** Before it is dropped, it is written to the overlay
  ledger (see below), so Cut over can restore it. If the live DEFAULT differs from the
  record, the tool refuses before any ALTER, so it never loses a DEFAULT it did not
  record.
- **Failures stay local.** Any overlay failure fails only its own table, with the
  SQLSTATE, before that table writes a row.
- **Computed columns are left alone.** A column Aurora DSQL computes is only omitted,
  and is never reported as "loaded as NULL".

The overlay runs on **every Full Load path**: thread and multi-process loads, sharded
tables, an empty table promoted to replace, every Drop & reload recreate, the retry of
a table the tool recreated but did not finish, and the quarantine route. The activity
log shows `overlay planned` (naming the exact DEFAULTs to drop), `overlay applied`,
`column excluded`, and `loaded as NULL: ...` on the table's load line.

### The Start Full Load dialog shows the changes first

When exclusions are in play, the Start Full Load dialog shows:

- "N columns in M tables will load as NULL" (with **show columns**);
- "Aurora DSQL changes before loading: x x DROP NOT NULL (one-way), y x DROP DEFAULT
  (restored at Cut over)", with the exact statements in run order under
  **show statements**. These are redrawn when you switch between Append and Drop;
- the tables that must be dropped and reloaded (next section);
- how many rule-unselected tables are not loaded.

The page-header **Run** also goes through this dialog whenever exclusions are in play.
If a run would change something different from what the dialog showed, it refuses and
asks you to open the dialog again.

### Tables that already hold rows: Drop & reload

An append never changes a column it omits, so appending cannot turn stored values into
NULL. The tool therefore never mixes NULL with stored values. The Start Full Load
dialog **forces Drop & reload** for:

- a populated table whose rows were not all loaded with the current exclusion set;
- a populated table in which a previously excluded column is included again, over rows
  that hold NULL;
- a table the tool recreated but did not finish.

Forced tables are listed in a red card with their reasons, with no Append choice, and
the confirm button reads "Drop N table(s), recreate and load" in the destructive
colour. Cancel, Esc, a backdrop click or a lost tab discards the forced set, so a later
run never inherits a drop. The engine applies the same verdict: if you try to append to
such a table by another route, the table fails with "needs Drop & reload: ..." instead.

Exceptions:

- an unreadable target leaves the table undecided, with a warning; it is never forced;
- while CDC streams, nothing is forced, and the table is appended with a WARNING;
- a resume or a retry under the same set is never forced.

### Start CDC applies the same overlay

CDC only, Retry CDC and a re-Start after Stop apply the same overlay **before any
connector is created or changed**. They use the same statements, order and checks as
the Full Load, so every row CDC inserts holds NULL in the excluded columns.

- After a Full Load that already did the overlay, this is a verified no-op.
- After a cut-over restore, it drops the restored DEFAULT again, so **restore DEFAULTs
  after your last Start CDC**.
- Any failure fails the Start as "Start CDC blocked: ...", and no connector is touched.
  The job also refuses if the session's target changed after the click.
- The CDC-only Start dialog lists the changes: "Aurora DSQL changes when CDC starts: N x
  DROP NOT NULL (one-way), M x DROP DEFAULT (restored at Cut over)", with the
  statements under **show statements**.

**Rows already on Aurora DSQL before CDC started keep their values** in an excluded
column, because CDC never writes an excluded column. The CDC-only Start dialog names
those tables, an `overlay append` WARNING is logged for each, and Validation reports
them (§13.9).

### The overlay ledger

A dropped DEFAULT cannot be read back from the target, so the tool records it, before
the ALTER, in the **overlay ledger**. For each column, the ledger records the original
DEFAULT text (`pg_get_expr`), whether NOT NULL was dropped, a digest of the Schema
Conversion DDL, and the restore state. The ledger is keyed by the **target** (cluster
endpoint + database), not by the session. A restored session, or a new session after
Start over, therefore still sees what an earlier session changed.

| Deployment | Where the ledger lives |
|---|---|
| Fargate | `overlay-ledger/<sha256>.json` in the tool's session-state bucket (`mysql-dsql-migrator-plugins-<account>-<region>`), outside `sessions/` and `jobs/`, so no prune touches it. The task role's existing grant on the bucket covers it. |
| EC2 | The `overlay_ledger` table in the session-state SQLite file, on the retained EBS volume. |
| Local run | The `overlay_ledger` table in the session-state SQLite file. |

Every write is verified by reading it back. **An unreadable ledger is never treated as
"no record".** For a session that uses it, the Start dialog disables Start and offers
**Retry**, a run fails every table before anything is dropped or written, and Start
CDC is blocked. A run with no exclusions still reads the ledger once, so it does not
append values into rows an earlier session left NULL. If that one read fails for a
session that never used the ledger, the run proceeds and logs a WARNING.

---

## 13.7 Changing exclusions later

### Why the set locks

Once data has moved under an exclusion set, changing the set in place would leave some
rows NULL and others with values. A CDC stack also carries its own copy of the set: it
was deployed with its capture list, and **Stop CDC does not free it**, because a
stopped stack keeps its exclusions and its committed offsets. The tool therefore locks
the set and offers only the remedies that are true for your situation:

| Your situation | How to change the exclusions |
|---|---|
| Nothing has run yet (no Full Load, no CDC stack) | Not locked: edit the rules and Save. |
| The CDC infrastructure is being deleted | Wait for the deletion to finish. The popup shows an engine-aware estimate (about 15-25 min on MySQL, 2-5 min on PostgreSQL). |
| The CDC infrastructure is being created | Wait for the create to finish, then follow the matching "CDC stack exists" row. |
| A CDC stack exists (deployed, streaming or stopped) and a Full Load ran | 1. Stop CDC if it streams, then **Delete CDC infrastructure** on the CDC step. 2. Use **Change & reload** for the affected tables (CDC only: switch to Full load + CDC first). 3. Deploy again and Start CDC (on MySQL, see "Starting CDC again after a delete" below). In Full load + CDC, while CDC has never been started for this migration (no Start CDC was submitted), you can still untick or tick tables in the table list (see "Changing the table set after a Full Load (Full load + CDC)"). |
| A CDC stack exists, no Full Load ran, CDC only | Stop CDC if it streams, Delete CDC infrastructure, change the rules (once CDC has streamed, only the column rules: changing the table set needs Start over), deploy again and Start CDC. Deleting the stack also deletes the stream position (see below). |
| A CDC stack exists, and no Full Load has run yet (Full load + CDC) | Stop CDC if it streams, Delete CDC infrastructure, change the rules, deploy again, then Start Full Load. Nothing needs reloading. If CDC already streamed, choose drop and recreate in the Start Full Load dialog. |
| No CDC stack, and a Full Load ran (Full load only or Full load + CDC) | **Change & reload** (next section). Full load only: the table set can also change; Full load + CDC: tables can be dropped or added until CDC starts (see the two subsections below). |
| No CDC stack, and a Full Load ran before the type was switched to CDC only | Switch the type to Full load + CDC, use Change & reload there, then continue with CDC. |
| A Full Load ran, but its job record is no longer available | Delete the CDC stack first if one exists, then **Start over**. |

More details:

- **Full load only with a stack.** If a stack exists while the type is Full load only,
  switch to Full load + CDC to delete it, because Full load only has no CDC step.
- **The table set.** In CDC only, it is fixed after a Full Load; switch to Full load + CDC to change it before CDC starts. In Full load + CDC, tables can be dropped and added until CDC starts (see "Changing the table set after a Full Load (Full load + CDC)" below); after that, use Start over. In Full load only, see "Changing the table set after a Full Load (Full load only)" below.
- **Starting CDC again after a delete.** On MySQL, Start CDC on Automatic re-seeds from
  the Full Load watermark (the binlog since that load must still exist; Start CDC warns
  if it was purged). Change & reload keeps that watermark, so Start CDC also refuses if
  a captured table's definition changed since that load (for example a column the source
  added): then Re-run Full Load for all tables with "Drop & reload" for a newer
  watermark, or choose Manual and Fetch current position (see
  [Chapter 6](06-limitations.md)). On PostgreSQL, deleting the stack drops the
  replication slot and the publication, so choose **"Re-snapshot every table instead"**
  in the Start CDC dialog.
- **The cost of deleting a CDC-only stack.** On MySQL, the next Start CDC on Automatic
  begins at the source's current position. Changes to the captured tables made after
  the last one Aurora DSQL applied are **not** replicated. To re-copy them, switch to
  Full load + CDC and run its Full Load, choosing to drop and recreate the tables that
  hold rows. On PostgreSQL, the next Start CDC snapshots the captured tables again, but
  a row the source has deleted in the meantime stays on Aurora DSQL. In both cases, rows
  already on Aurora DSQL keep their values in newly excluded columns.

### Change & reload

For a Full Load migration type whose run is recorded and that has no CDC stack, the
read-only popup offers **Change & reload**. It opens **Change exclusions & reload**:

- **Column rules are editable.** In Full load only, a table line changes the selection (see the next
  section): a table it unselects is left as is on Aurora DSQL (not dropped); a table it
  selects that is not loaded yet is loaded by Retry unfinished tables; a table it selects
  that the last Full Load already loaded is kept as is, unless a column rule now excludes
  columns in it, in which case it is listed for **Drop & reload** like any other table
  whose excluded columns change. In Full load + CDC, until CDC starts, a table line can unselect tables (left as is on Aurora DSQL, not captured by CDC) and select any migratable table: one the last Full Load did not load is listed under Retry unfinished tables on the Full Load step, and CDC captures it once it is loaded. A column rule on a table CDC does not capture yet drops nothing here: its load on the Full Load step applies it. Once CDC has started, any line that would change the selection is an error. With a CDC stack the popup is read-only: use the table list instead.
- **Before anything is dropped, it shows exactly what will happen:** each table it will
  drop and reload, with the column diff (`+ c (NULL)`, `- c`) and a row estimate ("N
  rows re-read"), plus the warnings and the ALTERs those tables will get. "Their rows on
  Aurora DSQL are permanently deleted; each table is recreated from your Schema
  Conversion DDL, made nullable where needed, and reloaded. Other tables are not
  touched."
- **The confirm names the action:** **Drop & reload** *table*, or **Drop & reload N
  tables**, in the destructive colour. It acts only on the tables the preview showed.
  No Start dialog follows: the reload starts right away.
- **A text-only change drops nothing.** If no table's excluded columns change (for
  example, you only added a comment), the button reads **Save** and stores the rules
  only (in Full load only and Full load + CDC, also the table selection a table line changes).
- **Omit-only conversion.** For each table holding omit-only entries (excluded before
  v0.1.562, or adopted from a stack as omit-only; see §13.10), there is an unticked
  checkbox "Reload *table* so its ... are NULL (now they hold the target DEFAULT)". Tick
  it to convert those columns to NULL in the same reload.

Change & reload is not offered for CDC only (the popup offers **Change migration type**
instead), or when the Full Load's job record is gone (use Start over). With no Full Load
recorded, nothing is locked and you edit the rules directly. Where it is offered, it is
disabled, with the reason, while a Full Load runs, while foreign keys are being applied,
while Cut over restores DEFAULTs, and, after an app restart, until you re-verify the
connections on the Connect step.

### Changing the table set after a Full Load (Full load only)

In Full load only, the table set is not fixed by a Full Load. While no Full Load,
foreign-key pass or cut-over DEFAULT restore runs and no CDC stack exists, tick or untick
tables in the table list, or use a table line (`exclude table ...`) in Change & reload.
No Start over is needed.

- **An unticked table is left as is on Aurora DSQL (not dropped).** This migration no
  longer loads or validates it. When a table rule unselects it, its excluded-column
  entries are not kept.
- **A ticked table that is not loaded yet is loaded by Retry unfinished tables.** The
  Full Load step lists it as "Not loaded yet — selected after the last Full Load", and
  the step reads Failed until it is loaded. The retry keeps every loaded table and reuses
  the original watermark. The new table was never checked, so re-run the prerequisite
  checks first when the tool asks: until then, Retry unfinished tables is disabled when
  only new tables are unfinished, and refuses them when other tables are retried too.
  Its column rules apply on that first load: Change & reload never drops and reloads a
  table the last Full Load did not have.
- **A ticked table the last Full Load already loaded is kept as is.** It is not loaded
  again (Reload specific tables on the Full Load step reloads it). If your rules had
  unselected it, ticking it again does not bring back its excluded columns (their entries
  were not kept): the Exclude tables & columns row warns, and Change & reload applies your
  rules to it again.
- **Foreign keys.** Unticking tables after a complete **Apply foreign keys** pass keeps
  its result: the keys stay in place. After you tick a table (or untick one after a
  stopped pass), the card asks you to apply again: keys already in place are kept, but
  the pass re-checks every key.
- **Validation.** Once a newly selected table is loaded, re-run Validation: Cut over
  holds until the result covers every table selected since the Full Load (or you turn
  its chip off under Objects to validate). While such a table is not loaded yet, Cut over
  points you back to the Data Migration step.
- **Switching to a CDC type.** Unticking tables does not lock it unless CDC has already started for this migration (then any change to the set does, until you restore the set or Re-run Full Load), and a CDC type captures the loaded tables still selected. A table ticked since the Full Load keeps the type selector locked until **Re-run Full Load** takes a fresh watermark over the current set, or you untick it again: a table loaded only in Full load only is not covered by the watermark, while one added by a guarded load in Full load + CDC is (see the next section).
- **Refresh objects stays hidden** after a load, as before.

### Changing the table set after a Full Load (Full load + CDC)

In Full load + CDC, the table set can change after a Full Load while CDC has never started for this migration: no Start CDC was submitted, no connectors exist, and any CDC infrastructure has never streamed. Untick or tick tables in the table list, or use a table line in Change & reload. It works the same in a restored session. No Start over is needed.

- **An unticked table is left as is on Aurora DSQL (not dropped).** CDC does not capture it, and this migration no longer loads or validates it; the cut-over DEFAULT restore lists it under **Needs your decision** instead of restoring it. When a table rule unselects it, its excluded-column entries are not kept.
- **A table you tick that the Full Load did not load** is loaded under the Full Load's start point first, and only then captured by CDC:
  - Re-run the prerequisite checks when the tool asks (the table was never checked). Until it is loaded, the Full Load step lists it as "Not loaded under the Full Load's start point yet" and reads Failed, and Start CDC, Validation and Cut over wait.
  - **Retry unfinished tables** loads it under the original watermark and keeps every loaded table. Its load first checks that CDC can capture it with no gap since the Full Load. If a check fails, the table is not loaded, and its row says why and what to do.
  - It loads only into an **empty** Aurora DSQL table. If the table already holds rows, the dialog drops and reloads it: the tool cannot tell whether they predate the changes CDC captures for it (on MySQL, those since the Full Load's start point; on PostgreSQL, those since the table joined CDC's publication), and an append keeps every row already there.
  - **MySQL:** a Full Load records the start-point definition of the migratable tables it does not load, and the load compares it with the table's definition now. A Full Load from an earlier version, or a table converted after the load, has no such record: use **Re-run all tables**, or untick it. A definition changed since the start point also needs Re-run all tables.
  - **PostgreSQL:** the tool adds the table to CDC's publication on the source and waits (up to 10 minutes) for the transactions already open to finish before it copies the table. The table, and each of its partitions, must have a usable `REPLICA IDENTITY` on the source at that moment: a primary key, a valid identity index, or `FULL` (a re-keyed table must already be `REPLICA IDENTITY FULL`). Otherwise every UPDATE and DELETE on it would fail on the source once it is in the publication, so its load refuses it before adding it (and also when the identity cannot be read). The source user must own the table and the publication. A table added for a load that then fails stays in the publication until **Delete CDC infrastructure** (CDC's table list filters it out), so until then its UPDATE and DELETE keep needing that replica identity. After **Delete CDC infrastructure** (which drops the slot and the publication), adding a table needs Re-run all tables.
  - CDC captures it only after a load with **no quarantined row**.
  - **Change & reload never drops and reloads a table CDC does not capture yet** (selected after the Full Load, or loaded with quarantined rows): Save stores its column rules, and its load on the Full Load step applies them.
  - **The cost:** on MySQL, CDC replays every change made to the table since the Full Load's start point over the newer copy; on PostgreSQL, it streams every change made to the table since it joined CDC's publication (just before its copy) over that copy. Either way it converges. A change that briefly conflicts on a unique index with a row already copied can be dead-lettered; Cut over then waits for a CHECKSUM validation. On MySQL, the binary log must still hold every change since the Full Load. If the CDC infrastructure was deployed before the table was loaded, its Kafka topic gets the infrastructure's default partition count.
- **A table you tick again after the CDC infrastructure is deployed without it** is captured when CDC starts: its Kafka topic gets the infrastructure's default partition count, which was planned for the tables it was deployed with.
- **A table an exclude table rule unselected** comes back only by deleting that rule in Change & reload, which applies its column rules (ticking it in the table list is refused: its column rules would not apply). A lock icon marks it, with the reason.
- **With CDC infrastructure deployed, the popup is read-only:** use the table list.
- **A failure only in unticked tables no longer holds the step.** It reads Done, Continue to CDC unlocks, and on PostgreSQL CDC still resumes from the load's slot. Retry unfinished tables never reloads an unticked table.
- **It waits, never Start over,** while a Full Load, a foreign-key pass or a cut-over DEFAULT restore runs; while the CDC infrastructure is created or deleted; while a failed create is still in the account (delete it on the CDC step); and for a few seconds after the page opens while the CDC state is read. If the CDC state cannot be read, the CDC step says why: fix the AWS access and use **Re-check CDC state**.
- **Unselect all is allowed**, but Deploy CDC infrastructure and Start CDC need at least one selected table.
- **Re-run Full Load over the current set** takes a fresh watermark over every selected table; on PostgreSQL it reuses the existing publication and adds any selected table it lacks (only with a usable `REPLICA IDENTITY`, as above: otherwise the run stops before any table loads and says which).
- **On PostgreSQL, when CDC resumes from the Full Load's slot, the publication still lists the unticked tables** (the tool does not remove them on the source); the connector discards their changes. A Start that re-snapshots instead (a captured table did not finish loading, **Re-snapshot every table instead**, or a Start after the CDC infrastructure was deleted) lets the connector set the publication to the captured tables.
- **Foreign keys.** A selected table keeps its foreign keys to an unticked table: at Cut over, the orphan check reports a key whose parent rows were not replicated.
- **Once CDC has started, even if stopped since, the set is fixed again.**
- **CDC only:** switch to Full load + CDC to change the set before CDC starts. Start CDC refuses while a selected table is not loaded under the Full Load's start point.

### From a quarantine or a dead letter

You often discover a column to exclude only after data has moved. Two recovery buttons
lead into the same popup:

- **Full Load quarantine: "Exclude column & reload".** This opens Change & reload with
  one line per flagged column already inserted, but **not saved**
  (`exclude s.t.c  # from the Full Load quarantine`). The reload therefore still takes
  two deliberate clicks: the button, then the confirm. (In Full load + CDC, for a table
  CDC does not capture yet, the confirm is a plain Save: the table's load on the Full Load
  step, listed under Retry unfinished tables, applies the rule; see §13.7.) If an `include` keeps the
  column, the popup points at that line (for example **Go to L5**). If the reason does
  not point to one of the table's flagged columns (it names no type, as with
  `maximum column size exceeded`, or a type none of them has), the LOB section opens
  filtered to the table, so you can pick the column with **+ add rule**. If a CDC
  stack still holds the exclusions (or Change & reload is not offered in this session),
  the read-only popup opens and the lines are recorded as pending. If Change & reload
  is offered but cannot run yet (for example, before you reconnect after a restart),
  the button is disabled with the reason.
- **CDC dead letters: "Exclude column...".** This button is enabled in every state. It
  records `exclude s.t.c  # from the CDC dead-letter queue` as a pending line, then
  opens the popup in the mode the lock allows. Follow the lock's steps (see the table
  above): Stop CDC, then delete the CDC infrastructure; use Change & reload if a Full
  Load ran (otherwise change the rules directly); then deploy and Start CDC.

**Pending lines** are rules waiting for the editor. The read-only popup shows each one
as `Pending: "exclude ..." (from ...) will be added when the rules can be edited
again.` They persist with the session. The next time the popup opens editable, they
appear in the editor, and they take effect only when you Save. A pending line never
changes what is excluded by itself.

---

## 13.8 The CDC capture list

CDC excludes columns at capture: Debezium's `column.exclude.list` keeps an excluded
column out of the change stream entirely. The tool sends it as the `ColumnExcludeList`
parameter of the cdc-stack. The list covers only the tables CDC captures, and it also
contains the generated columns Aurora DSQL computes, which CDC must not stream.

**Exact matching.** Debezium reads each entry as an anchored, case-insensitive regular
expression, trims it, and splits the list on commas. The tool therefore writes each
entry so that it matches exactly one column:

- regex characters are escaped, a comma in a name is sent as `\,`, and leading or
  trailing whitespace as `\u00XX`;
- `(?-i)` is added only when another captured column has the same qualified name
  ignoring case (for example `"Notes"` next to `notes`);
- a plain name such as `shop.users.ssn` is sent unchanged.

Two names that join to the same `db.table.column` string (possible when a name part
contains `.`) cannot be told apart by any escape. Deploy, Start CDC and the CDC step
warn about this unless both are excluded.

**The 4,096-byte limit.** `ColumnExcludeList` is one CloudFormation parameter, and a
parameter value is limited to 4,096 bytes.

- **Compact form.** When the plain list would be longer, tables of one database that
  share the same excluded columns are collapsed into one equivalent, still exact
  pattern:

  ```
  shop\.(?:orders_2023|orders_2024)\.(?:card_number|cvv)
  ```

- **Too long even in compact form.** Save is disabled in the popup ("Narrow the rules:
  exclude fewer columns in the tables CDC captures"). Deploy CDC infrastructure and
  Start CDC are held, and for Full load + CDC so is Start Full Load, before any load.
  The message gives the byte count and the remedy that applies in the current state.
- **Generated columns alone over the limit.** Fewer exclusions cannot help then.
  Deploy CDC infrastructure and Start CDC refuse with "capture fewer tables in this CDC
  pipeline, or use Full load only". In the popup this is a warning that does not block
  Save, and Start Full Load is not held.
- **A value the stack already carries is never refused.**

**A column the source adds or renames after the rules were saved.** The list holds
concrete column names, fixed when the rules were saved. A new column that a wildcard or
`type:` rule matches, or an excluded column the source renames, is therefore captured,
and every insert and update to that table that carries it is dead-lettered (SQLSTATE
42703). **Fix target schema…** does not add it, because that would copy the data the
rule keeps out. To keep it out: Stop CDC, **Delete CDC infrastructure**, then re-run
Step 1 (Evaluation) so the rules see the column. For a renamed column, change the rule
that excluded it so it matches the new name: a rule that names the old column now
reports `No column "…"` and offers the fix when the names are close, while a wildcard
that still matches other tables reports nothing and would let the column through. In
Full load + CDC, also drop the old column on Aurora DSQL by hand first
(`ALTER TABLE … DROP COLUMN …`; the dialog shows the statement): it holds only NULLs, or
the values the rule keeps out, and Change & reload refuses to destroy a column the
refreshed definition no longer has. Then save the rules as
[Why the set locks](#why-the-set-locks) describes (Full load + CDC: **Change & reload**,
which creates the column on Aurora DSQL as NULL), deploy again and Start CDC (on MySQL,
see "Starting CDC again after a delete"). To replicate a new column instead, change the
rule in the same steps so it no longer matches it.

**What you see.**

- In the popup: **CDC capture list (B of 4,096 bytes)**, computed by the same code as
  Start CDC.
- The **Deploy CDC infrastructure** confirm says "Excluded: N tables, M columns. They
  are fixed once deployed: changing them later needs Delete CDC infrastructure.", with
  the exact list under **show**.
- The CDC step shows a read-only summary, "Excluded: N tables, M columns", and an
  expandable **What CDC receives (B of 4,096 bytes; as deployed | Start CDC sends)**
  holding the exact value. When the deployed value and the session's value differ, the
  step shows both.

**Attaching a stack that excludes columns.** When a fresh, un-run session attaches a
startable cdc-stack whose `ColumnExcludeList` excludes columns, the tool reads the list
back the way Debezium matches it. It adopts the list only if it round-trips
byte-for-byte. The adopted columns appear as rule lines marked
`# adopted from <stack>`. Each adopted column becomes NULL where that changes nothing
that holds data, and omit-only otherwise. Start CDC, Retry CDC and, for Full load + CDC,
Start Full Load refuse a session set that differs from a stack that has ever streamed,
and name both sets. Start CDC also re-checks the live value before the overlay changes
anything.

---

## 13.9 Validation and Cut over

### Validation proves the excluded columns are NULL

The target pass that counts a table's rows also counts the rows that hold a value in
each excluded column. This needs no extra scan. It reads only the target, so it works
for MySQL and PostgreSQL sources alike, and when nothing is excluded its SQL is
unchanged. An excluded column is not value-compared (the tool did not write it), but
its NULL proof is shown:

- in the per-table **Excluded** chip (`NULL`, or `N non-NULL`);
- in the readiness notice ("Excluded columns are NULL on Aurora DSQL");
- in the failure reasons, the AI facts and the text report.

What a value in an excluded column means depends on the migration type:

| Migration | A value in an excluded column |
|---|---|
| Full load only, Full load + CDC, or CDC only where a Full Load ran | The table is **not matched**, and cut over is blocked: "Excluded columns hold values: reload those tables with Drop & reload". The values usually mean rows were appended over data already on the table. Re-running the Full Load does not clear them, because an append keeps a stored value. The remedy follows the state: Stop CDC first while it streams, switch the type for CDC only, or Start over when the job record is gone. |
| CDC only onto a target that already held rows | A warning only: "Excluded columns hold values from before CDC started". |
| Omit-only entries (§13.10) | Information: "excluded before v0.1.562 (omit-only); its rows hold the target DEFAULT (N non-NULL)", or "... omit-only (adopted from the attached CDC stack) ...", followed by the steps that convert it in your session. A column with no values reads "NULL in every row (0 non-NULL), nothing to convert." |

If the NULL check itself fails, it is reported as not determined. The table's row count
and verdict are unaffected.

### Cut over restores the dropped DEFAULTs

The cut-over runbook gains a block **Excluded-column DEFAULTs restored: n of m**. It
shows the exact statements, a **Restore DEFAULTs** button, and each column's live
status (pending, done, diverged, gone or unknown):

```sql
ALTER TABLE "shop"."users" ALTER COLUMN "status" SET DEFAULT ('active'::text)
```

- **Automatic restore.** A DEFAULT is restored automatically only when the column is
  still excluded, its recorded text is a single expression, and the Schema Conversion
  DDL is unchanged since it was recorded (or still declares the same DEFAULT). Every
  other pending column is listed under **Needs your decision** with its reason and its
  statement. It gets its own **Restore** button when its recorded text is a single
  expression; otherwise, run the statement yourself.
- **Never overwritten.** A DEFAULT that someone has since set to something else is left
  as it is.
- **New rows only.** A restored DEFAULT applies to new rows; the migrated rows stay
  NULL.
- **NOT NULL stays dropped.** Aurora DSQL cannot add NOT NULL back in place, so the
  block lists which excluded columns stay nullable.
- **Under CDC, tick the drained confirmation first.** This is the checkbox "I have frozen
  source writes, both CDC connectors are RUNNING, and CDC has drained to zero lag". The
  foreign-key block uses the same checkbox, and it is shown whenever either block is.
  Tick it only when that is true, because the tool cannot see the lag. A DEFAULT
  restored while a connector is still snapshotting or replaying gives those rows the
  DEFAULT. A restore is refused while the stack or a connector is still changing.
  A running Full Load or Start CDC blocks a restore, and the reverse is also true.
  Every Start CDC clears the checkbox.
- **Restore after your last Start CDC.** Start CDC drops a restored DEFAULT again
  (§13.6).
- **Cutting over with DEFAULTs owed is not blocked.** A warning above
  "I've cut over to DSQL" carries its own **Restore DEFAULTs** button, and the
  acknowledgement logs `cut over with N DEFAULTs not restored`.
- **A table no longer selected.** A column whose table is no longer selected for the
  migration (Full load only, and Full load + CDC before CDC starts, can unselect a loaded table) is never restored
  automatically: it is listed under **Needs your decision**.

If the ledger cannot be read, the block says so ("The record of dropped DEFAULTs could
not be read ...") and offers **Check again**.

---

## 13.10 Upgrading from 0.1.561

In 0.1.561, an excluded column was only left out of the INSERT, so every row took the
target column's DEFAULT. A NOT NULL column with no DEFAULT could not be excluded at all
(the prerequisite check held the run, and where a load ran anyway every row was
rejected with 23502). The exclusions were ticked in "Oversized LOB columns (optional
exclusion)" cards, which the popup replaces.

### Restored exclusions open as rules

A 0.1.561 session holds only excluded columns. The popup therefore shows one
synthesized `exclude schema.table.column` line per entry, and legacy entries are
commented `# excluded before v0.1.562`. Nothing is stored until you Save. Entries the
lines cannot reproduce are listed ("N saved exclusions are not reproduced by these
rules"), so a first Save cannot shrink the set silently. Restored entries are judged by
the eligibility rule like any other (§13.4). From now on, a key entry is migrated on
every path, and Validation compares it.

### Omit-only (legacy) entries and how to convert them

If rows may already have been written under the old meaning, the restored entries keep
it (**omit-only**). Any one of the following makes **every** restored entry legacy:

- the Full Load job record is missing (pruned);
- a table of that Full Load job was attempted, or loaded, skipped or quarantined rows
  (or an excluded table did so in another recorded job);
- connectors or a CDC start are recorded, even after a Stop or a Delete.

A legacy entry is only omitted. It is never ALTERed and never recorded in the ledger.
Its rows take the target DEFAULT, and a NOT NULL column with no DEFAULT still rejects
the row (23502). Validation lists such entries as information, not as a mismatch.

When none of this evidence is found, every restored entry becomes an ordinary exclusion
that loads as NULL on the next run, with the Start dialog's disclosure and any forced
Drop & reload. When a CDC stack is possible, the tool's read-only check of the CDC
stack settles the question, and until it does, the session behaves as legacy.

To make a legacy column NULL:

1. If a CDC stack exists, delete the CDC infrastructure first (Stop CDC first while it
   streams). Change & reload is refused while a stack exists, and a fresh session would
   adopt the same column again as omit-only.
2. Open **Change & reload** (for CDC only, switch to Full load + CDC first), tick
   "Reload *table* so its columns excluded before v0.1.562 (...) are NULL", and confirm.
   A reload with the box unticked omits them again.
3. If no run is recorded, use Start over instead (for CDC only, choose Full load + CDC),
   exclude the columns again, and choose Drop & reload in the Start Full Load dialog.

Validation's line for each such column names the steps that apply in your session.

### New outcomes to expect

- A direct Reload or Retry, under an exclusion, of a table whose rows predate the record
  (for example, rows loaded outside the tool) now fails with "needs Drop & reload"
  instead of appending.
- A table whose excluded column holds values now fails Validation. 0.1.561 left
  excluded columns out of the comparison and reported a match. The new result is a true
  finding; the fix is Drop & reload of that table.
- The converter now carries more MySQL DEFAULTs (literal expression defaults,
  `DEFAULT ''`, BINARY/VARBINARY defaults). As a result, excluding one of those columns
  can now also show a `DROP DEFAULT` and a restore at Cut over.

### Deployed CDC stacks

- **No Delete + Deploy is needed.** The sink plugin, the worker configuration and the
  stack parameters are unchanged.
- **A stack deployed by 0.1.561 keeps its capture list.** When the stack carries
  exactly the 0.1.561 `ColumnExcludeList` for the session, Start CDC keeps it
  byte-for-byte, and the other connector parameters (`MessageKeyColumns` included) are
  built as in 0.1.561. A running pipeline therefore gets no connector update. It also
  keeps 0.1.561's matching:
  - a column whose name contains `$` was never excluded at capture, so CDC kept
    streaming its values;
  - a case twin of an excluded column (`"Notes"` next to `notes`) was dropped too, so
    CDC stopped replicating the twin.

  The CDC step warns "The deployed CDC stack may not exclude these columns exactly ...
  To match exactly, delete the CDC infrastructure and deploy it again." A kept list
  that excludes a composite primary-key column is refused at Start CDC, as in 0.1.561,
  with that same remedy. A new stack, a stack deployed by 0.1.562, or a changed
  selection gets the exact form (§13.8).
- **The stack template changed.** The cdc-stack no longer opens MSK port 9098 to a
  hard-coded `172.31.0.0/20`. The first Start CDC that updates a stack deployed by
  0.1.561 removes that rule; every Start after a Stop does. Connectors are unaffected.
  A host in that range that reached MSK on 9098 only through this rule (for example a
  diagnostics bastion) loses that access once that Start completes. To keep it, add your
  own TCP 9098 inbound rule for the host to the stack's `ConnectorSecurityGroup`.
  `HostSubnetCidr` can admit it only when the CDC infrastructure is deployed again,
  because it is fixed at create. If that host is the machine running this tool, which
  seeds the CDC start position itself (the in-process seed; see
  [`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md)),
  and the stack's `HostSubnetCidr` does not contain it, also set
  `DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR` to the stack's `HostSubnetCidr` value, then
  restart the app.

### Downgrading to 0.1.561

A 0.1.562 session snapshot always carries the new exclusion fields, which 0.1.561
rejects. 0.1.561 therefore starts a **fresh session**: it logs "Could not parse
persisted session ...; ignoring it" (SQLite store: a local run or the EC2 deployment) or
"Ignoring unreadable session snapshot ..." (the Fargate deployment's S3 store). The
rules and every other restored step are lost.

0.1.561 has no ledger and no cut-over restore, so the overlaid columns stay nullable
**without their DEFAULT**. Before you downgrade, finish Cut over's restore on 0.1.562.
Otherwise, before the application writes, run this for each recorded column:

```sql
ALTER TABLE <table> ALTER COLUMN <column> SET DEFAULT (<original_default>)
```

Read `original_default` from the ledger. On Fargate, it is
`overlay-ledger/<sha256 of "v1|<cluster endpoint, lower-case>|<database>">.json` in the
session-state bucket. On EC2 or a local run, it is the `overlay_ledger` table in the
session-state SQLite file. The ledger is left in place, keyed by the target.

---

## 13.11 Limits, and what was not exercised live

**Limits to plan around.**

- **NOT NULL is dropped one-way.** Only a later Drop & reload of the table, which
  recreates it from the Schema Conversion DDL, restores it.
- A restored DEFAULT applies to **new rows only**; the migrated rows stay NULL.
- A hand-written `CHECK` constraint that rejects NULL is not detected in advance (the
  table fails with 23514).
- In CDC only, the **table set** changes after a Full Load only through Start over (or switch to Full load + CDC to change it before CDC starts). In Full load + CDC, tables can be dropped or added (§13.7) until CDC starts; after that, Start over. For Full load only, see "Changing the table set after a Full Load" in §13.7. Once a CDC stack is deployed, its exclusions change only after Delete CDC infrastructure.
- Rows already on Aurora DSQL **before CDC started** keep their values in an excluded
  column.
- The capture list must fit 4,096 bytes (§13.8). Two names that join to the same
  `db.table.column` string cannot be separated at capture.
- A table whose name contains `:` cannot hold a saved column exclusion: exclude the
  whole table, or keep its columns. A name with a line break cannot be written as a
  rule.
- An overlay ALTER or a cut-over `SET DEFAULT` on a table that live CDC is streaming
  makes the CDC sink write its next batch for that table one row at a time (slower),
  once per sink connection. No row is lost, and nothing is dead-lettered.
- The tool cannot see CDC lag. Before a restore, you confirm yourself (with the cut-over
  checkbox) that source writes are frozen and CDC has drained.
- Check sizes is type-based and opt-in. It does not highlight every type that can
  exceed the limit, and it does not check a row over 2 MiB (§13.5).

**Verified live.** On a MySQL source (Aurora MySQL 8.0.42) into Aurora DSQL, including
a 120k-row run with two workers. The `$` capture-list case was verified live on MySQL:
`n$b` was NULL on the target while its sibling `nb` replicated.

**Not exercised live** (covered by unit tests only; the capture-list escapes were also
checked against the bundled debezium-core 2.7.4):

- the compact capture-list form over 4,096 bytes (whether Debezium honours it);
- every capture-list escape except `$`: a `.`, comma or edge whitespace in a name, and
  the `(?-i)` case-twin form, on MySQL as well as PostgreSQL;
- keeping a stack deployed by 0.1.561 on its 0.1.561 capture list;
- the PostgreSQL Full Load overlay, and the PostgreSQL CDC half (the PostgreSQL Start
  CDC overlay and capture);
- the S3-backed ledger used by the Fargate deployment. The live runs used the local
  SQLite store.

> **From the command line (optional).** `scripts/run_pg_cdc_e2e.py` accepts
> `--exclude-columns table.col[,..]` and `--exclude-rules FILE`. With exclusions, it
> passes the same inputs as the UI, sends the UI's exact `column.exclude.list`, runs
> the product's Start CDC overlay, and validates with the NULL check. A rules file is
> syntax-checked before any connection and resolved by the popup's own resolver and
> eligibility rule; table rules are refused there. Pass the same exclusion flags to
> every stage of the harness (infrastructure, start, validate); the script's header
> shows an example.

---

**Prev:** [Appendix: Performance test results](12-performance-test-results.md)
