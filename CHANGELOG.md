# Changelog

_Language: **English** | [한국어](CHANGELOG.ko.md) | [日本語](CHANGELOG.ja.md)_

All notable changes to this project are recorded here. This project follows
[semantic versioning](https://semver.org/) (patch releases for bug fixes).

## v0.1.233

### Added

- **A Full Load prerequisite now catches a target NOT NULL column the source cannot
  fill, before the load instead of partway through it.** Full Load builds its INSERT
  column list from the source table, so a column present only on the target — e.g. one
  added while editing the target DDL in Schema Conversion — is never named in an INSERT.
  That is harmless when the column is nullable, has a DEFAULT, or is an identity column
  (verified on a live cluster: the load fills it with NULL / the default). It is fatal
  for a `NOT NULL` column with no default: the row has nothing to put there and the load
  fails with a not-null violation after the target already holds partial data. The new
  `TARGET_COLUMNS_LOADABLE` check (required) reads the target's value-required columns,
  subtracts the source columns, and fails only on the remainder — a column that also
  exists on the source is filled by the INSERT and is not flagged. It defers to
  `TARGET_SCHEMA_READY` (passes) when the target table is missing or unreadable, so it
  never double-reports the same cause.

## v0.1.232

### Added

- **AI assist now falls back to Claude Sonnet 4.6 when the configured model is not
  enabled for the account.** A `global.` Bedrock inference profile being `ACTIVE` in a
  region does not mean the account may invoke it — model access is granted per account,
  so a fresh account can see `global.anthropic.claude-sonnet-5` as active and still get
  a model-not-enabled error, leaving "Verify AI access" at a dead end with nothing to do
  but edit the model id by hand. The preflight now retries the fallback and reports
  which model actually answered, naming both models and how to restore the chosen one.
  Deliberately narrow: the retry fires only for a model-not-enabled failure — a missing
  IAM permission, a throttle, or a connectivity error is a property of the caller or the
  network, so retrying another model would only add latency and bury the real cause. A
  fallback pass is shown as a warning rather than a green success, since reporting a
  clean pass would imply the operator's chosen model works when it does not.

## v0.1.231

### Fixed

- **Switching migration type left a red "Migration failed" banner beside a "Success"
  header.** After a Full Load that quarantined rows, selecting CDC only showed three
  verdicts at once: the header said `Success`, the status said `DONE`, and the banner
  still said `Migration failed` -- so the screen gave no answer to "did this work?" The
  banner was rendered from the last error message alone, and nothing cleared it on a
  type switch. The message is not noise, though: it reports rows genuinely missing from
  the target, which is exactly what someone about to start CDC needs to know, since CDC
  streams ongoing changes and does not backfill a Full Load gap. So an error recorded
  under a different migration type is now kept but demoted from error to warning and
  re-framed as carried-over context, naming the remedy (re-run Full Load). An error
  whose provenance is unknown -- restored from an older session -- stays an error rather
  than being silently softened.

### Changed

- **The Data Migration status badge now names the phase it describes**, e.g.
  `Full Load: DONE` instead of a bare `DONE`. One underlying step backs every migration
  type, so after a finished Full Load a switch to CDC only made the badge read as though
  CDC had completed when none had run. For Full load + CDC the label follows the actual
  progress, becoming `CDC` only once the pipeline is genuinely streaming.

## v0.1.230

### Fixed

- **v0.1.229 could not be applied to an existing stack.** Naming the ALB (the Cognito
  login fix) replaces it, and CloudFormation replacement is create-new → repoint →
  delete-old. The target group had no name, so nothing about it changed and it was
  *reused*: the new listener tried to attach a group the old ALB still held, and ELBv2
  allows a target group on only one load balancer, so the update failed with
  `The following target groups cannot be associated with more than one load balancer`
  (`ServiceLimitExceeded`) and rolled back — leaving fresh deploys working but every
  existing stack stuck on the old release. The target group is now named
  `${AWS::StackName}-tg`, which is also create-only, so it is replaced in the same
  update and the new listener attaches a group no load balancer holds. Verified by
  upgrading a live stack: `TargetGroup` now appears in the change set (it was absent
  before), the listener that previously failed within 3 seconds completed, and the stack
  reached `UPDATE_COMPLETE`. This is the structural fix, not a one-off — any future ALB
  replacement (e.g. flipping `AlbScheme`, also create-only) would have hit the same wall.
  Note that a named target group means later changing a create-only property of it
  (`AppPort`, `VpcId`, `TargetType`) fails with `DuplicateTargetGroupName` rather than
  replacing — the same trade already accepted for the named ALB.

## v0.1.229

### Fixed

- **An empty target that already carried the chosen primary key was still dropped and
  recreated "to apply" it.** After picking a composite key in Schema Conversion and
  running Apply all to target, the first Full Load's confirm dialog announced
  `1 empty table will be recreated to apply the chosen primary key` for a table that had
  just been created with exactly that key — a contradiction, plus a wasted DROP+CREATE
  round trip (DSQL permits one DDL per transaction, so it is its own trip per table).
  Both the disclosure and the engine's promotion decided on the applied DDL vs the
  *source* key alone and never read the target's real key, even though the append path
  right below already did. Both now consult the target's actual primary key and skip the
  recreate when it already matches; the key is read once by the probe that already runs
  before the dialog opens, so the render path stays free of target I/O. Deliberately
  asymmetric: only a definitely-equal key skips the recreate — a key that cannot be read
  is unknown, not safe, and still recreates.

- **Cognito login never succeeded: the ALB and the app client disagreed on the callback
  URL's letter case.** With `EnableCognitoAuth=true`, signing in always failed — the
  hosted UI bounced back with `Client is not enabled for OAuth2.0 flows.`, even though
  `AllowedOAuthFlowsUserPoolClient` was `true` the whole time. Left unnamed, the ALB got
  a CloudFormation-generated mixed-case name (`mysql--LoadB-u9DQdeKlckt9`) and its
  `DNSName` inherited that casing, so the app client's `CallbackURLs` — built from
  `GetAtt DNSName` — were mixed case. But the ALB sends the OAuth `redirect_uri` with
  the host lower-cased, and Cognito compares the two exactly. `/oauth2/authorize`
  tolerates the mismatch, which is why the login page rendered and only the submit
  failed, and why a first sign-in appeared to "change the password, then error out":
  the password change had already been applied when the redirect was rejected. The ALB
  is now named `${AWS::StackName}-alb`, so its DNS name follows the (lower-case) stack
  name and the two strings match.

### Added

- **The stack now creates the first Cognito login user.** The user pool sets
  `AllowAdminCreateUserOnly`, so with no user a `EnableCognitoAuth=true` deploy
  succeeded and handed back an app nobody could sign in to. A new required parameter
  `CognitoAdminEmail` creates that user and Cognito emails it a temporary password; a
  template `Rules` assertion rejects the deploy up-front when it is missing. The pool ID
  is also exported (`CognitoUserPoolId`) so further users can be added, and
  `CognitoHostedUiDomain` is now the full sign-in URL instead of the bare prefix.

### Changed

- **The deployment guides now state the stack-name constraints.** Because the ALB is
  named after the stack, the stack name must be lower case and 28 characters or fewer —
  a longer name fails the deploy with
  `The load balancer name '<stack>-alb' cannot be longer than '32' characters` only
  after a ~2 minute rollback, and a mixed-case name breaks Cognito login as above.

## v0.1.228

### Fixed

- **The reconstructed source DDL rendered string defaults unquoted, producing invalid SQL.**
  `information_schema.COLUMN_DEFAULT` stores the value without quotes and it was emitted
  raw, so a column MySQL prints as `DEFAULT 'pending'` appeared as `DEFAULT pending`. The
  pane has a **Copy Source DDL** button, so what it handed over could not be run — MySQL
  reads a bare `pending` as a column reference. Present since the initial release.

  Quoting is now decided by `default_is_expression`, the same signal the converter uses:
  it comes from MySQL's `DEFAULT_GENERATED` flag, the only thing that can tell the literal
  string `'CURRENT_TIMESTAMP'` from the function call of the same name (a shape heuristic
  cannot). Embedded apostrophes are escaped. Verified against the live source: the
  reconstructed `ecommerce.orders` DDL now executes on MySQL and the column round-trips
  identically.

## v0.1.227

### Fixed

- **The Schema Conversion screen was silent about things Evaluation had already flagged.**
  Reported from a workshop for `AUTO_INCREMENT`; auditing every captured field and every
  assessor rule against the conversion output found **nine** cases of the same shape — the
  tool told you about a problem in Evaluation, then showed a conversion that looked clean.

  The reconstructed **source DDL** now shows what the target cannot reproduce:
  `AUTO_INCREMENT`, `ON UPDATE CURRENT_TIMESTAMP`, `COLLATE`, `FULLTEXT`/`SPATIAL` index
  kinds, and markers for generated columns and native partitioning (both of which only
  have a boolean captured — they are noted, never invented as syntax). A plain table renders
  exactly as before.

  The **conversion notes** now cover the six rules that produced no note at all:

  - **FULLTEXT / SPATIAL index** → emitted as an ordinary `CREATE INDEX ASYNC` on the same
    column, i.e. identical DDL to a normal index. The index is created but `MATCH …
    AGAINST` cannot use it. Evaluation rates this UNSUPPORTED / SIGNIFICANT.
  - **Native partitioning** → correctly dropped (DSQL distributes by primary key), but
    partition-scoped SQL and `DROP`/`TRUNCATE PARTITION` archiving do not carry over.
  - **255-column limit** and **24-index limit** (the primary key counts) → these are hard
    limits, so the DDL is *rejected at apply*; the index case fails after the table
    succeeds, leaving a partially-indexed target.
  - **Oversized LOB/TEXT** → the DDL is fine; the ~1 MiB cap bites per row during
    migration, where oversized values are permanently dropped.
  - **Generated column** → becomes an ordinary column. Full Load copies the values the
    source computed, so the target starts correct and drifts on the first write that does
    not supply one.
  - **Case-insensitive collation** → DSQL compares case-sensitively, so equality, `LIKE`,
    `ORDER BY` and `UNIQUE` behaviour change while every row count and checksum still
    matches. Only `_ci` collations are reported; `_cs`/`_bin` already match the target.
  - **`ON UPDATE CURRENT_TIMESTAMP`** → the `DEFAULT` survives, but nothing refreshes the
    value on an `UPDATE`, so `updated_at` freezes at insert time.

  No generated DDL changed — these are notes. Limits and type sets are imported from the
  assessor instead of restated, and a new test drives both engines over the same tables and
  fails if any rule fires with no conversion note, so a future rule cannot regress into the
  same gap.

## v0.1.226

### Fixed

- **The Schema Conversion tree renamed objects the Evaluation report had already named.**
  Evaluation lists **Stored procedures** and **Functions** (its `KIND_LABELS`), while the
  object browser lumped both under a single **Routines (n)** node — so the same objects
  appeared under a different name on the next screen, and there was no way to line one
  screen's list up against the other's. Found in a workshop.

  The tree now splits them and takes its headings from `assessor.KIND_LABELS`, the same
  mapping the Evaluation list, the UI chart axis and the HTML export already share —
  hard-coding the label here is how the two drifted apart. The introspector had always
  distinguished `PROCEDURE` from `FUNCTION`, so the tree was discarding information it
  already had. A routine MySQL reports as neither still groups under **Routines**, rather
  than being asserted into one of the named kinds.

  Node IDs are unchanged (`routine:<name>` regardless of kind): they are parsed back when
  resolving a selection and are what the ticked / generated sets persist across renders, so
  re-keying them by kind would have silently invalidated a restored selection and the DDL
  generation scope with it.

## v0.1.225

### Fixed

- **The object browser collapsed to its initial state when you pressed "Generate DDL for
  selected".** Expanding a schema to find and tick its tables, then generating, threw the
  tree shut — hiding the very rows you had just been working with. The tree is rebuilt on
  every render (Generate, an apply, the progress poll) and NiceGUI keeps its open/closed
  state client-side, so anything not restored in Python snaps back to collapsed. The ticked
  set was already carried across renders; expansion was the missing half. Both browser
  panes (source and target) now record and restore it. "Clear" still discards the generated
  DDL, edits and AI suggestions — but not where you had navigated to, which is not analysis.

## v0.1.224

### Fixed

- **A cached-identity primary key left its sequence unadvanced after Full Load, so the
  application's first insert after cut-over failed with a duplicate key.** The converter's
  identity strategy emits `GENERATED BY DEFAULT AS IDENTITY` — and `BY DEFAULT` is exactly
  what lets Full Load write your source's own key values — but an explicitly-supplied value
  does **not** advance the underlying sequence. So after a load the sequence still sat at
  its start while those values were already taken.

  Reproduced on a live `ap-northeast-2` cluster: load ids 1–3, then an id-less insert
  raised `duplicate key value violates unique constraint`.

  This was the worst shape a migration failure can take. Row counts and checksums **match**,
  so Validation passes clean, and it only surfaces after cut-over — once the source has been
  frozen and rollback is no longer trivial.

  Full Load now advances each target identity sequence past the rows it loaded
  (`ALTER TABLE … ALTER COLUMN … RESTART WITH max(pk)+1`, also verified live) and records it
  in the activity log. It runs only after a **complete** load, because `MAX(pk)` is the
  value the sequence must clear and syncing a partial load could still leave a collision for
  the rows yet to arrive; a retry that completes the load performs the sync. Tables with a
  plain integer key (the `KEEP_INTEGER` default) have no sequence and are left untouched, as
  are empty tables — restarting from a NULL `MAX(pk)` would only move the sequence
  backwards. A failure here never fails the load, but it is logged as a FAILURE with the
  exact manual command, because an unrepaired sequence is a post-cut-over outage.

## v0.1.223

### Changed

- **The CloudFormation template's default app image is now `0.1.222`** (was `0.1.209`, 13
  releases behind). It is the default a fresh `git clone` deploys with, so leaving it stale
  meant a new deployment silently shipped an old app — including without the Start-CDC
  restart fix and the validation/cut-over corrections from 0.1.218–0.1.222.

## v0.1.222

### Fixed

- **A multi-stack Start-over teardown reported only the first stack, then went silent while
  the rest were still deleting.** v0.1.214 made Start over tear down *every* discovered
  cdc-stack, but the durable teardown marker is a single slot and only the first job claims
  it — so the banner tracked stack 1 and disappeared the moment it finished, while the
  others were still deleting and still billing for MSK / NAT with nothing on screen. The
  banner now follows a queue of every launched teardown: when the tracked stack settles it
  advances to the next unfinished one, and while several are pending it says which
  (*"Deleting 'cdc-b' (2 of 3; the rest follow)"* — and *"(3 of 3, the last one)"* on the
  final stack, where the operator is deciding whether to wait).

- **A finished teardown left no trace after a page refresh.** Completion was signalled only
  by a `ui.notify` toast, which hangs off a `ui.timer` and dies with the page — so an
  operation that takes 15–45 minutes and is explicitly designed to be walked away from left
  nothing to distinguish *"it finished"* from *"it never ran"*. The banner now reports the
  result durably (**"CDC infrastructure deleted — MSK / NAT billing has stopped"**, naming
  every stack) and keeps it until the operator closes it with the **✕** button. It never
  auto-hides, and an in-flight teardown takes precedence over a stale completion notice.

## v0.1.221

### Fixed

- **Cut-over was unreachable when the only remaining difference was a row DSQL cannot
  store — while the gate's own copy promised otherwise.** The message read *"Cut over only
  when Validation reports a clean MATCH (**or every difference is explained**)"*, but the
  gate tested `ready_for_cutover`, which is a bare match. No "explained" path existed
  anywhere. So a migration whose sole finding was a permanently quarantined row (a value
  over DSQL's ~1 MiB per-value limit) could never finish the workflow — and reloading could
  never fix it, because DSQL is unable to store that value at all. The step was unreachable
  by design rather than by any decision the operator made.

  The promised path now exists, as an explicit sign-off: when every difference is exactly
  the rows the migration already reported dropping, the step offers **"Accept the N-row gap
  and continue to cut-over"**, naming the tables, the row count, and the fact that
  reloading will not change it. Accepting unlocks the runbook; the alternative (fix the
  source value(s) and re-run Validation for a full match) is offered alongside.

  Deliberately **not** auto-released — the rows really are absent from the target, and that
  is the operator's call — and once accepted the runbook still leads with **"Cutting over
  with an accepted gap"** rather than reading as a clean match, so the sign-off is not
  quietly forgotten at the moment it matters most.

  The sign-off is only ever offered when the shortfall is **entirely** accounted for. An
  unexplained mismatch, a table that could not be compared at all, or one explained table
  beside a genuinely wrong one all keep cut-over shut — and an acceptance cannot leak
  forward onto a later, worse run.

## v0.1.220

### Fixed

- **Validation reported rows the migration had already dropped as unexplained failures,
  contradicting itself on the same screen.** With one table short by exactly its quarantined
  rows, the panel showed **Not ready for cut-over — "1 of 8 table(s) did not pass. Review
  the failing checks"** plus two red **Failed** checks counting those rows — directly above
  a per-table entry that read *"Fully explained: 3 rows were permanently dropped … this
  deficit is expected, not new data loss."* The reviewer was sent to investigate a defect
  that had already been found, reported, and explicitly accepted in the Full Load step.

  The per-table model already knew this (`deficit_explained_by_quarantine`), but nothing
  aggregated it, so the summary and the readiness checks never saw it. They do now:

  - **The verdict** states what is actually outstanding — *"Cut-over blocked only by rows
    dropped during the migration … Nothing unexplained"* — and offers the two real choices:
    fix the source value(s) and reload, or accept the gap deliberately.
  - **The readiness checks** name the cause inline and drop from **Failed** to
    **Heads-up**.

  It is deliberately *not* reported as passing: those rows really are absent from the
  target, so cut-over stays a decision the operator must make rather than something the
  tool waves through.

  The softening requires the shortfall to be **entirely** accounted for. A table that
  dropped 1 row but is 3 short stays a hard failure, and a run with one explained table
  beside a genuinely mismatched one stays red — hiding a real loss behind a known one is
  the one outcome this attribution must never produce.

## v0.1.219

### Fixed

- **The Validation in-progress panel rendered the Cancel button's tooltip as its label.**
  The whole sentence — *"Stop the comparison. Tables not yet started are skipped; the ones
  already running finish first. Read-only, so nothing is left half-changed."* — became the
  button's caption, so the button stretched across the panel and the actual verb ("Cancel
  validation") disappeared.

  Cause: NiceGUI's `Element.tooltip()` creates the tooltip and returns **`self`** (the
  owning element) for chaining — it does not hand back the tooltip. So binding its result
  and calling `set_text()` on it to swap the tooltip between the running and stopping
  wording was setting the **button's** text instead. The handle now comes from
  `ui.tooltip()` created inside the button's context, which does return the tooltip
  element; the text still swaps in place, so a hovered tooltip is never destroyed by the
  poll.

  This was invisible to the test suite because the NiceGUI double modelled
  `Element.tooltip()` as returning a tooltip object — the opposite of the real API — so the
  mistaken call looked correct in tests while being broken on screen. The double now
  mirrors the real return value, and the regression test asserts the tooltip copy never
  appears as button text. Verified by re-introducing the bug: three tests fail.

## v0.1.218

### Fixed

- **Start CDC was dead after a Stop whenever the Full Load job record was gone — even
  though the pipeline could resume perfectly.** The button's readiness gate required a
  start point (a Full Load watermark or a manually entered coordinate), but the watermark
  is read off the Full Load **job record**. So after an app restart (job record pruned) or
  in a CDC-only session there was none, and Start CDC went disabled with *"Set the CDC
  start point above first"*.

  Nothing had actually been lost. Stopping CDC deletes only the two connectors: the source
  connector's offsets topic is pinned to a fixed name (`<stack>-debezium-source-offsets`,
  not a per-instance UUID topic), so it survives a Stop, and on the next Start the seeder
  reads that offset and *skips* re-seeding when it is at/past the watermark. Streaming
  resumes exactly where it stopped. The gate was therefore blocking a restart that the
  backend already supported — and pushing the operator toward re-entering binlog
  coordinates by hand, or re-running the entire Full Load, to recover a position the
  connector still had.

  Start CDC now also unlocks when the stack holds a committed resume offset. The signal is
  `DeploySink=true` while `MskBootstrapServers` is blank, which is unambiguous: the infra
  create pins `DeploySink=false`, only Start CDC sets it `true`, and Stop overrides *only*
  the bootstrap (everything else carries through as `UsePreviousValue`) — so that
  combination is reachable only by "started, then stopped". A stack that has never streamed
  still requires a start point, which is what prevents a first start from beginning at the
  source's current binlog and silently losing the whole Full Load window.

- **A restart is now described as a restart.** The panel showed the first-start copy
  ("…begins streaming"), and the start-point card badged **Action needed** and offered
  *"Automatic — needs a Full Load watermark (unavailable)"* — while the button beneath it
  was enabled and would have worked. On a resume there is no start point left to choose
  (the position lives in the offsets topic, which that card cannot set), so it now states
  the resume instead: *"Resuming from the last streamed position"*.

- **The Stop CDC dialog now says the stream position survives.** It said MSK and the
  plugins are kept "so you can restart with Start CDC" — which describes the
  infrastructure but never the *position*, leaving the operator to guess. The reasonable
  guess (that deleting the connectors loses it) is wrong, and acting on it costs a
  re-load. It now states plainly that Start CDC continues from exactly where streaming
  stopped, with no gap, nothing re-applied, and no Full Load or start point needed again —
  and that stop/restart can be repeated freely.

## v0.1.217

### Changed

- **The Activity log tab's Download button is a full-size primary action again.** The
  previous release routed it through the shared form-field row, which put it in the
  right-hand *control* slot — a slot sized for a number input and right-aligned so that a
  COLUMN of inputs lines up, which is meaningless for a single button. The result was a
  small button stranded at the far right with the description wrapping beneath it. This tab
  is an action, not a set of fields, so it now reads as a described section with the action
  below it, named ("Download activity log") rather than a bare verb.

- **The Settings header caveat is now an info notice instead of gray micro-text.** It is
  worth keeping — it prevents a real mistake: an operator who tunes a value and walks away
  assumes it persists, but any restart (including a Fargate task replacement they did not
  initiate) silently reverts it to the deploy-time default, so a carefully tuned run
  behaves differently next time with no sign why. As a caption under the title it read as
  boilerplate and was skipped. It now leads with the consequence ("These settings are not
  permanent") and names the durable alternative (set the `DSQL_MIGRATOR_*` environment
  variable in the deployment).

- **Start CDC no longer stacks two equal-weight notices on the happy path.** Under "Ready
  to start CDC" sat a second full-width blue box whose header was "This table set is now
  fixed" — so a normal first start showed two notices, and the one line an operator
  actually scans for (WHICH tables will stream) was buried inside a paragraph about MSK
  partition accounting. The table list is now plain text beside a check glyph — still fully
  visible, since it is the verifiable fact — with the immutability rationale and the
  re-scoping remedy moved to an info tooltip, as background needed at most once. The
  re-start caution (amber) is unchanged: that one is a real warning, because repeated
  start/stop really does consume MSK capacity that is never reclaimed.

## v0.1.216

### Changed

- **The Settings dialog no longer resizes when you switch tabs.** The panel container used
  a min/max height range, so it took each tab's natural height (Full Load has three knobs,
  Validation one) — the card grew and shrank on every switch and, because a centred dialog
  is positioned from its middle, the tab strip itself moved under the pointer. Clicking
  through the tabs made the whole panel jump. It is now a fixed height sized to the tallest
  panel, so the strip stays anchored and only the content changes (the viewport cap and
  internal scrolling remain, so a small screen still can't push the dialog off-screen).

- **Tab order now follows the migration journey: Full Load → CDC → Validation.** CDC pairs
  with Full Load (both are data-movement throughput) while Validation is the after-the-fact
  check; CDC previously sat last only because that knob was added later. The order is a
  property of the config registry, which is what the tab strip derives from.

- **"Sink compute (MCU)" has an info tooltip with the guidance that doesn't fit one line:**
  when to raise it (sink lag while the source keeps up — raising the *source* MCUs buys
  nothing), what it costs (each step up bills for as long as the connector runs, 8 is the
  MSK Connect API ceiling), and when it lands (next Start CDC; re-running it purely to
  resize is safe because connector capacity updates in place — no replication gap, no MSK
  partition-quota cost). The visible label, description and accepted values are unchanged,
  so this is added depth rather than a return to hover-only guidance.

- **The Activity log tab matches the other tabs.** It was a loose paragraph with a button
  underneath, which made it look like a different kind of screen; it is now the same form
  row (label + description + control), with the button reduced to "Download" since the row
  already says what the file is. Its tooltip notes that on ECS the file lives on ephemeral
  task storage, pointing at Diagnostics → Mirror to stdout for a durable CloudWatch copy.

## v0.1.215

### Changed

- **Settings now has a tab per tuning category — Full Load, Validation, CDC — instead of
  one "Performance" tab holding all of them.** "Performance" is not a category an operator
  thinks in: they arrive wanting to change the Full Load or the CDC sink. The combined
  panel made them read past the other groups, and each group's apply-timing caption ("the
  next run" vs "the next Start CDC") sat mid-list where it read as a note on whichever
  field came next. Each tab now leads with its own timing, and the Full Load
  connection-product caution is scoped to the tab it applies to (it is meaningless beside
  a single Validation or CDC field). The tab strip is derived from the config registry, so
  a knob added in a new group grows it automatically.

- **Every settings control is now an AWS-style (Cloudscape) form field: visible label,
  description, and constraint text listing the accepted values.** The descriptions were
  previously hidden behind a hover-only info glyph so each knob could stay on one line —
  which made the form unreadable at a glance (you had to hover five fields in turn to
  learn what any of them did) and inaccessible on touch, where there is no hover. Controls
  are right-aligned so a column of inputs lines up down the panel, and constraints render
  in monospace so accepted values read as data. Added as `form_field` in
  `ui/design.py` (the single source of truth) rather than styled inline.

- **The Diagnostics tab uses the same form rows.** A floating-label select beside a bare
  switch read as two unrelated widgets rather than one form; both now carry a label and a
  description explaining what they do (including that the stdout mirror is what reaches
  CloudWatch, because the log file itself lives on ephemeral task storage).

- **The modal's header no longer claims "changes apply to the next run"** — that is true
  only of the Full Load / Validation knobs. It now states only what holds for everything
  in the dialog: nothing here is a deploy-time parameter, and values reset on restart.

## v0.1.214

### Fixed

- **Start over offered "Delete all CDC infrastructure" and then deleted nothing when the
  account held two or more cdc-stacks.** The offer counted every discovered stack, but the
  teardown resolved a *single* name and adopted a discovered stack only when there was
  exactly one — with several it fell back to this session's own stack name, which in that
  branch is precisely the name the probe did **not** find. So the delete found no stack,
  reported success, and the operator kept paying for MSK / NAT with nothing in the tool
  pointing at it. The offer, the dialog's listing and the teardown now share one resolver
  and act on **every** stack. The shared source-credentials secret is still cleaned up
  exactly once (it is created out-of-band, so re-scheduling its delete per stack would
  fail for each extra one).

- **The Start over teardown tiles now name the cdc-stacks they would delete.** "Delete all
  CDC infrastructure" did not say *what* it deletes. Because a stack carries no owner tag,
  the account may hold a pipeline another window is using — and the tool cannot tell — so
  the name is the only thing that lets an operator answer safely. The name appeared in the
  notice above the tiles, but there it reads as context for the question rather than as
  the delete target. Both destructive tiles now carry the names (and the count, when there
  is more than one), and the wording switches to plural throughout.

- **The Start CDC tip told the operator to pick their tables at a point where the picker is
  already locked.** "Pick all your tables before you start … Choosing everything you need
  up front keeps this smooth" rendered only for card phase `infra`, which requires a probed
  `cdc_stack_phase` of `infra` — exactly the condition `selection_lock_reason` freezes the
  table picker on, for every migration type that can reach the button. The checkboxes were
  disabled while the tip pointed at them.

  It now states the fact and the remedy that actually works, which differs by situation:
  after a Full Load, only **Start over** re-scopes (the Full-Load lock clause takes
  precedence and is *not* released by deleting the cdc-stack, so the previous draft of this
  fix would have sent the operator through a ~45 min teardown that left the picker just as
  locked); for a CDC-only session, deleting and redeploying the infrastructure genuinely
  does. The after-a-Full-Load wording also explains *why* the set is fixed — it matches the
  snapshot, which is what makes the handoff gapless.

## v0.1.213

### Added

- **The CDC sink's compute is now tunable from the UI — Settings → Performance → CDC →
  "Sink compute (MCU)".** The manual has long advised raising `SinkMcuCount` (not the
  source's MCUs) when the sink can't keep up, because the sink is the CPU-bound half of
  the pipeline while the single-task Debezium source has spare CPU. But the app never
  sent that parameter: `grep SinkMcuCount src/` found nothing, so every deploy silently
  used the template default and `submit_update` carried it forward as
  `UsePreviousValue`. The only way to act on the manual's advice was to edit stack
  parameters in the CloudFormation console — which conflicts with "everything core is
  reachable from the browser". The tool now passes `SinkMcuCount` on all three paths
  (infra create, Start CDC, and the read-only parameter preview, so the preview cannot
  advertise a value the deploy contradicts).

  Only 1 / 2 / 4 / 8 are offered, rendered as a dropdown rather than a number field:
  those are the MSK Connect API's valid values for `mcuCount` (max 8 per worker), so a
  spinner would happily accept 3 and CloudFormation would reject it minutes into a
  billable Start CDC. The value is validated against that exact set before it is stored.

  The tool's default deliberately equals the template's (4). A different default would
  read as a real config change against any stack deployed before the tool sent this
  parameter, needlessly recreating both RUNNING connectors on the next Start CDC and
  burning MSK partition quota that is never reclaimed.

### Changed

- **The Settings → Performance form is now split into sections that each state their own
  apply timing.** It previously reported "applies to the next run" for everything, which
  is true only for the Full Load / Validation knobs (the loader and validator call
  `load_config()` per run). A CDC knob is a CloudFormation parameter: nothing re-reads
  it, and a sink already streaming keeps its capacity until Start CDC updates the
  connector. So the CDC section reads "applies to the next Start CDC", and the
  confirmation toast repeats each knob's own timing. Grouping moved into the config
  registry, which also removes a latent rendering bug: the old loop emitted a header
  whenever the group changed while walking the tuple, so a group whose knobs were not
  contiguous would have been split across two headers.

- **Manual §7 (Performance and tuning) now documents when the sink MCU change takes
  effect**, including that re-running Start CDC purely to resize the sink is safe:
  connector `Capacity` is an in-place update, so the sink is resized rather than
  recreated — no partition-quota cost and no replication gap, unlike a table-set change.

## v0.1.212

### Fixed

- **"Automatic — gapless from Full Load" was offered for a watermark that cannot give a
  gapless start.** The option was gated on "has any resume coordinate", but the handoff
  works by seeding MSK's `connect-offsets` with a record keyed on the binlog
  **file:position** — the in-VPC seeder rejects a watermark without it, and
  `build_watermark_params` returns all-empty values so the template skips the seeder and
  the connector starts from the source's **current** binlog. A GTID set alone therefore
  showed "gapless (recommended)" and *Ready* while every change made during the Full Load
  was silently lost, undetected until Validation or after cut over.

  This is reachable, not theoretical: the two coordinates come from separate queries that
  degrade independently — `SHOW MASTER STATUS` needs the `REPLICATION CLIENT` grant
  (commonly restricted on RDS/Aurora) while `@@GLOBAL.gtid_executed` is a plain global
  read. Automatic is now gated on `can_seed_offset()`, and the GTID-only case gets its own
  wording — it does **not** claim "needs a Full Load watermark" (there is one) but names
  the missing binlog position, what would happen, and the fix (grant `REPLICATION CLIENT`,
  re-run the Full Load).
- **The CDC step still offered Attach for a pipeline streaming other tables.** v0.1.211
  guarded the plan-level banner; this panel is a separate render path and had no check at
  all. It now withholds Attach with the same scope test.

### Changed

- **When attaching is not safe, deploying is presented as the way forward.** The deploy
  form sat collapsed behind a warning triangle labelled "Deploy a separate CDC pipeline
  instead" — so with a mismatched candidate the operator saw a prominent blue Attach button
  they must not press, and the correct action looked like the risky one *and* was hidden.
  With no attachable candidate it now renders expanded, titled "Deploy a CDC pipeline for
  this table set", with no warning glyph. When attaching **is** valid it stays collapsed
  and flagged, since a second MSK cluster is expensive and rarely intended.

### Tests

- Includes an invariant test that the UI's gapless claim equals whether the seeder would
  actually be deployed, across all four watermark shapes. Four mutations killed; one
  initially survived — swapping `can_seed_offset()` back to `has_coordinates()` — because
  every other test passes the flag in pre-computed, leaving the wiring untested.

## v0.1.211

### Fixed

- **"Attach" was offered for a CDC pipeline that streams a different set of tables.**
  Attaching points the session at a live pipeline and — because the pipeline is streaming —
  promotes Data Migration to `DONE` and unlocks Validation. Verified against a live
  account: a stack was replicating 11 `ecommerce_demo.*` tables while the session had just
  loaded 8 `ecommerce.*` tables. Attaching would have reported the migration complete and
  let the operator proceed toward cut over, while **every table this session loaded had no
  CDC at all** — silently losing each source change after the watermark.

  Attach is now withheld when a candidate pipeline does not replicate the tables this
  session loaded, replaced by a notice naming exactly which tables it would leave
  uncovered, both ways forward (deploy CDC for this table set, or change the selection to
  match), and a reminder that the idle infrastructure is still billing. Deliberately
  asymmetric: a pipeline that is **broader** than the selection is not a mismatch — it may
  serve another table set in parallel and leaves nothing this session owns uncovered. And a
  candidate whose table set cannot be read stays attachable, because blocking on an
  unprobed stack would push the operator toward deploying a second, costly MSK cluster —
  the very thing this banner exists to prevent.

### Changed

- **Start over no longer implies the running CDC pipeline is this session's.** It now names
  the stack and says plainly that it may have been left by an earlier session or be in use
  by another window onto the same account — the stack carries no owner tag, so the tool
  cannot tell. "Leave CDC untouched" is described as the right choice when something else
  is using the pipeline, instead of reading as a deferral.

## v0.1.210

### Fixed

- **Start over did not offer to tear down a cdc-stack deployed under another name.** It
  reported no CDC at all, and then moving to the CDC step offered to *attach* to
  `mysql-dsql-cdc-stack-0729-new` — the two prompts contradicting each other about a stack
  that really did exist in the account. Both of Start over's signals
  (`cdc_stack_phase`, `cdc_connector_names`) are scoped to the name **this** session
  targets, so a stack from an earlier session, or with a custom suffix, was invisible to
  it — and the silent prompt was the one that would have stopped the MSK / NAT billing.
  Start over now also consults the discovered stacks, and the teardown resolves the **same**
  stack the offer was made about (keying only the offer off the discovery would have
  offered to delete a stack and then targeted a name that does not exist — a silent no-op
  that leaves the infrastructure billing). With several discovered stacks it offers but
  does not choose: each may be a separate pipeline, so which to delete stays the
  operator's call on the CDC step.

### Deployment

- Published `0.1.209` to ECR Public and pointed the template's `ContainerImageUri` default
  at it. The default had drifted to `0.1.188` — 21 releases behind — which is what a fresh
  `git clone` deploys, so the guard test that enforces this now passes again. The Seoul
  Fargate stack was updated to the same build (change set: TaskDefinition + Service only,
  all 24 parameters retained).

## v0.1.209

### Fixed

- **Each dropped row offered its own "Reload" button, but Reload acts on the whole
  table.** Three dropped rows produced three cards, each with a Reload that did exactly
  the same thing while looking like it acted on that row alone. The dropped rows are now
  **grouped into one card per table**, with a single Reload.

### Changed

- **A table's dropped rows are listed compactly instead of one card each.** Every card
  repeated the table name and the same reason, so three dropped rows filled the screen
  with three near-identical boxes. One card per table now states the table and the reason
  once, shows the count ("3 rows dropped"), and lists the primary keys as monospace chips
  — which stays readable as the count grows. Beyond 12 chips the list truncates with a
  "+N more" marker; the count badge always reports the real total, and the full list is in
  the downloadable error log. Genuinely different reasons within one table are all kept —
  deduplication must not hide a second cause — and a row whose message has no parseable
  primary key still contributes its reason rather than vanishing.

### Tests

- Six mutations killed, including removing the grouping, collapsing two different reasons
  into one, dropping the chip limit, and letting the count badge report the truncated
  number instead of the true total.

## v0.1.208

### Fixed

- **Only one dropped row was listed even when several were dropped.** The count said "3
  rows permanently dropped" while the list below showed exactly one — the panel was built
  from `latest_messages()`, which keeps one message per **table** (last write wins), so a
  table that dropped N rows listed one and the two numbers on the same screen disagreed.
  Every dropped row is now listed with its own primary key, which is the actionable part
  of each entry (it is what you search the source with). A caller with no per-row records
  — an older call site, or a restored session whose in-memory log is gone — still gets the
  per-table view rather than nothing.

### Changed

- **Accepting the gap no longer shows two near-identical green boxes.** The confirmation
  notice repeated what the completeness banner directly above it already says (the count,
  the table, that the next step is unblocked, that Validation reports the gap). It is now
  a single line carrying only the fact the banner lacks — that reloading a table after
  fixing its source value still closes the gap — with a checkmark to acknowledge the
  click.
- **The error-log download moved out from under the accept button.** Sitting immediately
  below "Accept quarantined rows & continue" it read as that decision's secondary option,
  when it just takes the same per-row information away with you. It now sits with the
  detail it serializes.
- **The watermark's per-table counts match the panel around them.** Quasar's default
  expansion header — a grey full-bleed bar with a large leading glyph — was a heavy band
  across an otherwise flat panel, and the count rows used a different alignment from the
  coordinates above them. Both now use the same label-then-monospace-value shape, with the
  header sized like the field labels.
- **Removed the standing caption under "Workloads to migrate".** Each of its three claims
  is already made where it serves the reader better: the picker's caption says where the
  selection came from (with the badges listing it right above), the Export watermark panel
  shows the actual coordinate rather than the promise of one, and the confirm dialog states
  the source is read-only at the moment the user commits.

## v0.1.207

### Changed

- **The accept-the-gap action now sits below the verdict it carries out.** It rendered
  inside the quarantine panel, which comes *before* the completeness banner — so the
  operator was asked to decide before reading the conclusion they were deciding on. The
  order is now: per-row detail → verdict → the action the verdict describes → download.
- **The "Data errors" heading and count are gone.** With errors, every one was already
  listed above with its table, primary key and reason, so a heading restating the count
  was the same fact a fourth time; with none, it printed a section header over "No data
  errors recorded." — a block asserting an absence. What the section uniquely offers is
  the download, so it is now just that button.
- **The download button is named for the reader, not the file format.** "Download error
  log (NDJSON)" led with a format nobody asked about and never said which step's errors it
  held, though both Full Load and CDC offer one. They now read "Download Full Load error
  log (3 errors)" / "Download CDC error log (N errors)", with the format and the
  per-line contents moved to the tooltip.

### Fixed

- **"Accept quarantined rows & continue" looked like it did nothing.** The click DID work
  — it marked the step complete, unblocked the next step, and wrote an activity entry —
  but no render path read the accepted flag (it was only consumed when a *new* load runs),
  so the panel and its button re-rendered identically. The button is now replaced by a
  green "Gap accepted — Full Load marked complete" notice that also says what is now
  possible, rather than leaving a control that invites a second, equally invisible click.
- **An accepted gap was still reported as a problem.** The amber "Full Load finished with
  issues" banner sat directly below the green confirmation, re-flagging the very thing the
  operator had just resolved by explicit decision. It now reads "Full Load complete — with
  an accepted gap", naming the dropped rows and pointing at Validation — while never
  claiming every row loaded, because they did not. A run with a **real** failure keeps the
  warning even when the flag is set, so accepting a gap can never paper over retryable
  work.

### Changed

- **The snapshot row counts now match the watermark panel.** They hung *below* the panel
  as a full-width expansion wrapping a bordered `ui.table` with its own sortable headers —
  a second visual container in a style nothing else on the screen uses. They are one value
  per table, so they are now labelled rows inside the panel, in the same shape as the
  coordinates above it, with right-aligned monospace thousands-separated counts that line
  up on the digits. Still collapsed by default (the list can be long).

### Tests

- Six mutations killed. One initially survived — deleting
  `quarantine_accepted=migration_state.accept_quarantined_rows` from the render call —
  because every other test passes the flag directly, leaving the **wiring** untested. That
  is the third time this session a state→render wiring gap slipped past otherwise-green
  tests, so it now has a structural assertion covering both render calls.

## v0.1.206

### Changed

- **Stopped announcing the same drop in eight places.** A 3-row quarantine was reported by
  the summary chip, the row's Status badge, the Attempts cell, a section header, the
  per-row card, the completeness banner, the data-error count *and* a red "Load failed"
  box. Each box now owns one job:
  - the **Attempts** cell no longer repeats it — the same row's Status badge already
    carries a "3 dropped" chip with the explanation on hover, so it said the same fact
    twice in one table row (other errors still show `1 · 3 errors`);
  - the quarantine section's **count header** is gone; the section shows the per-row
    detail (which row, why, Reload) that nothing else provides, and the banner states the
    verdict;
  - the red **"Load failed"** box is suppressed when quarantine is the *only*
    incompleteness. It restated the banner in red with an exception class name and
    contradicted the amber "the rest loaded" framing — and "failed" overstates a run whose
    only gap is rows that can never load. A real failure still shows it, with its exact
    text.
- **The export watermark moved below the progress table and is now compact.** It sat
  between the separator and the per-table progress, pushing the progress (and, on a
  finished run, the completeness verdict and quarantine detail) below static reference
  data. It is provenance read once, so it now follows the live detail — while still
  rendering *outside* the refreshable region, so the ~1.5s poll cannot collapse its
  row-counts expansion. The four fixed coordinates were a sortable two-column `ui.table`
  with "Field"/"Value" headers for four rows; they are now labelled monospace lines in one
  bordered panel, with the identifying summary on the header row and unavailable
  coordinates muted rather than styled like a missing value. The per-table snapshot
  counts stay a collapsed table — they are genuinely tabular.

### Tests

- Four mutations killed. One initially survived: the render-order assertion used
  `src.index("_live_detail()")`, which matches the `def` line first and therefore passed
  with the two calls swapped. It now compares the **call** line numbers via AST, verified
  by swapping them.

## v0.1.205

### Fixed

- **The quarantine header counted tables, not rows.** It read "Quarantined rows (1)"
  directly above a banner saying "3 rows permanently dropped" — two boxes on one screen
  disagreeing about the same number. The list it measured holds one entry per *table*
  (each carrying that table's latest message). The header now reports rows and tables
  separately: *"3 rows permanently dropped across 1 table — the rest of each table
  loaded"*.

### Changed

- **A dropped row now reads as three labelled facts instead of one run-on line.** It
  rendered as raw log text — `quarantined row pk[id=3]: datatype limit greater than
  1048576 bytes not supported for bytea` — with the table name in a badge above and the
  primary key buried mid-sentence. The entry is now an amber card: the table name in
  prominent text, the **primary key as its own monospace chip** (it is the actionable
  handle — what you search the source with), a "dropped" badge, and the technical reason
  below without the redundant `quarantined row pk[...]` stem. An unparseable message is
  still shown verbatim rather than mangled.
- **The Attempts column says what the number means.** `1 · 3 err` read like a retry count
  and gave no hint that it meant *rows the target will never hold*. It now shows
  `1 · 3 rows dropped` for permanently quarantined rows and `1 · 3 errors` otherwise.
- **Removed the duplicate caption beside "Accept quarantined rows & continue".** It
  repeated the completeness banner's own remedy ("fix the source value(s) and Reload that
  table … or accept the gap to continue"), so the same advice appeared twice on one
  screen. The banner keeps it — it states the verdict and the remedy together.

### Tests

- Five mutations killed, including restoring the table-count header, reverting the
  cryptic `err` marker, and letting a malformed message yield a bogus primary key.

## v0.1.204

### Fixed

- **"Accept quarantined rows & continue" disappeared in a restored session — a complete
  dead end.** Full Load ends with `FullLoadIncompleteError`, whose message tells the
  operator to use that button; after an app restart the button was not rendered. The
  quarantine-only gate counted rows in `ErrorLogStore`, which is **in-memory**, so a
  restart made the count 0 and the gate `False`. Nothing else could recover the run
  either — a permanently-rejected value never loads on retry — leaving only "Start over".
  The count now comes from the **job's chunks** (the job store is durable), falling back
  to scanning the error log so a job written by an older version still works. The
  guard that withholds the override while any table is genuinely unfinished is unchanged.

### Added

- **Failures now carry diagnostic detail on the durable activity log.** The activity log
  is the record that outlives the session, and three of its failure entries could not be
  troubleshot from:
  - **Each quarantined row** was recorded only to the in-memory error log, so after a
    restart nothing said *which* rows were lost — only a count. Every dropped row now logs
    its primary key, the rejection reason, and that the rest of the table loaded. Wired
    into all three load paths (in-process, sharded worker, single-table worker) via one
    shared helper — a sharded table is a large one, exactly the case least likely to be
    checked by hand.
  - **"1 of 8 table(s) did not fully load"** was a count, not a diagnosis. The run summary
    now names the affected tables with their reasons, deduplicated and capped at 8 (with a
    "+N more" note) so a large run cannot flood the rotated log.
  - **"connector X failed"** named no cause. The entry now carries the peer connectors'
    states (which side of the pipeline broke), the DLQ depth, the per-table error counts,
    and a pointer to the connector's CloudWatch log group for the stack trace. Degrades
    gracefully when a poll has not gathered diagnostics yet, and never reports a DLQ depth
    it did not actually read.

### Tests

- Six mutations killed. One initially survived and is now covered: the connector
  transition passing `detail=None` — every other test called the detail builder directly,
  so the *wiring* was untested, which is the same class of gap that shipped the
  restored-session table-selection bug earlier.

## v0.1.203

### Added

- **The Full Load table now marks which tables dropped rows.** A quarantining table
  finishes `DONE`, so its Status badge was identical to a clean table's — the only signal
  was one amber panel below the whole table, which does not say *which* row it belongs to
  and scrolls out of view (the affected row can even be on another page). Two markers,
  both amber and both explaining themselves on hover:
  - an outlined **"N dropped"** badge beside the row's `Done` badge, whose tooltip says
    what happened, that the rest of the table loaded normally (it is `DONE`, not failed),
    and how to close the gap (fix the source value, Reload that table);
  - a **"Dropped: N rows"** chip in the state summary above the table, so the loss is
    visible in the same glance as `Done: 8` instead of only after scrolling — the exact
    summary that made the reported run look flawless.

  Both render only when something was actually dropped; a clean run is unchanged.

### Tests

- Covered the summary chip (present/absent/pluralized), the per-row tooltip, and a
  contract check on the Quasar slot template — every `props.row.*` key it reads must be
  supplied by the row mapping. That last one matters because a wrong key in a slot
  renders **blank at runtime with the suite still green**; the mutation that renamed a key
  is now caught. Four mutations killed.

## v0.1.202

### Added

- **Validation now attributes a target deficit to rows the migration dropped.** A table
  whose rows were quarantined (a value DSQL cannot store) is short on the target, so
  Validation reported a bare `MISMATCH` / "investigate" — and the manual told the
  operator to *"cross-check the deficit against the Full Load error log / CDC DLQ"*,
  which is information the tool already had. Validation had **no** knowledge of
  quarantine at all. Now:
  - when the deficit is **exactly** the number of dropped rows, the table reads *"Fully
    explained: N rows were permanently dropped during the migration … this deficit is
    expected, not new data loss"*;
  - when the deficit is **larger**, it reads *"Partly explained: … but N more are missing
    and are NOT accounted for"* — naming precisely what still needs investigating. The
    exact-match requirement is the safeguard: a table 4 rows short that dropped 1 has 3
    unaccounted for, and calling that "expected" is how real loss would slip past the one
    check meant to catch it.
  - The verdict deliberately still **fails**. The rows really are absent, so the
    attribution explains the gap rather than excusing it — a quarantine can never flip a
    table to `matched` and unlock cut-over on missing data.
  - After an app restart the per-table counts are gone (they are not persisted), so the
    deficit is reported unexplained rather than guessed at.

  Counts flow from the Full Load job's chunks (`quarantined_rows_by_table`) and are
  attached once to the finished report, keeping the source-vs-target comparison a pure
  function of the two databases.

### Docs

- Manual §4.5 documents what CDC does with later changes to a row Full Load quarantined:
  a `DELETE` matches 0 rows and is applied silently (correct — the intended end state
  already holds, and treating it as an error would break idempotency); an `UPDATE` that
  shrinks the value below 1 MiB **heals the gap** via the sink's upsert; an `UPDATE` still
  over the limit is re-quarantined to the DLQ. Also states the two consequences: the gap
  is not self-announcing (Validation is what reports it) and a 0-row delete is
  indistinguishable from a normal replay, a deliberate trade-off for idempotency.
- Manual §5 replaced the manual cross-checking instruction with the new attribution
  (en/ko/ja).

## v0.1.201

### Fixed

- **A Full Load that permanently dropped rows still reported "loaded every source row".**
  Reported from a real run: an amber "Quarantined rows (1) — these rows were permanently
  dropped" box sat directly above a green "Full Load complete — All 8 tables loaded every
  source row", with the table itself showing `12 / 15`. Two causes, both now fixed:
  - The per-table `complete` check compared loaded-vs-source-estimate only and never saw
    the drop, and the estimate's 20% sampling tolerance (there because
    `information_schema` counts are sampled and drift either way) silently absorbed the
    3-row shortfall on a 15-row table. A quarantined row is a **confirmed** loss, not
    estimate noise, so it now fails the check outright — before any baseline comparison,
    so it is caught even when there is no estimate at all.
  - The row count never reached the verdict: `ChunkState`/`FullLoadTableRow` had no
    quarantine field, so the run-level summary was structurally blind to it. The engine
    already recorded the drop to the error log and treated it as an incomplete load; it
    now also records the count on the chunk, which the completeness summary reads.
- **The dropped rows were reportable as expected estimate drift.** With an approximate
  baseline, count differences are (correctly) shown as a calm "counts differ from the
  pre-load estimate … This is expected" note. Quarantined rows can never belong there —
  nothing about a sampled estimate explains a row the loader could not write — so they
  now always surface as "Full Load finished with issues", named with their table and
  count, and are not double-reported as a separate row-count mismatch.
- **The remedy no longer points at a control that does not apply.** A quarantining table
  finishes `DONE`, so it is not in the retry set; the banner said "Retry the failed
  tables" even when nothing failed. It now tells the user to fix the source value and
  Reload that table (or accept the gap), and only mentions retrying when a table really
  did fail.

## v0.1.200

### Fixed

- **The Prerequisites guard message was right-aligned.** Adding a table after the checks
  ran showed "Re-run the prerequisite checks — … was added to the selection after the
  checks ran…" ragged against the right edge. The nav row is `justify-end` because it
  normally holds only the primary "Continue" button (per the design system, primary
  actions sit on the right), and the guard sentence that *replaces* that button inherited
  the alignment. The row now right-aligns only when it holds the button and left-aligns
  the message, which reads as prose beside the content it explains. Checked the other
  `justify-end` rows in Data Migration, Schema Conversion and Validation — all hold
  buttons only, so none had the same defect.

## v0.1.199

### Fixed

- **A restored session still pre-ticked every target table.** v0.1.198 keyed the default
  off `generated_node_ids`, which is only set by pressing "Generate DDL for selected" — so
  a session that applied without it (or pressed Clear afterwards) restored with that field
  empty and fell straight through to "every table on the target", re-ticking them all.
  "Start over" appeared to fix it only because a fresh session repopulates the generated
  ids. The default now resolves the Step 2 scope the same way Schema Conversion's own
  apply does (`_selected_apply_names`): the committed generated ids when present, **else
  the ticked ids** — both persisted, so it survives a restart. The target-existing
  fallback is now reached only when neither is known.

### Tests

- Added the wiring assertion that was missing: a mutation removing `ticked_node_ids` from
  a call site passed every test, which is exactly how this shipped broken — the pure
  helper was correct and tested while the UI still over-ticked. The new test parses the
  screen's `default_migration_selection(...)` calls and fails if any omits the ticked
  scope. Three mutations killed (dropping the ticked fallback, preferring ticked over
  generated, and un-wiring a call site).

## v0.1.198

### Fixed

- **"Tables to migrate" pre-ticked every table instead of the ones chosen in Schema
  Conversion.** The default was "every table that already exists on the target", so a
  target still carrying tables from earlier runs silently re-selected all of them and
  discarded the deliberate Step 2 selection — reported from a real session as picking 3
  tables and finding 11 ticked. It also defaulted to migrating *more* than asked, the
  wrong direction for a long-running load.

  The pre-tick set is now this session's Schema Conversion selection when there is one,
  falling back to the target-existing set only when nothing was generated in this session
  (a reconnect, or the schema applied out of band) — where the Step 2 choice is genuinely
  unknown and an empty default would leave the picker with nothing ticked and no
  explanation. All four call sites share one `default_migration_selection()` helper so
  they cannot drift.
- **The picker's caption described the default rather than what was ticked.** It always
  read "Pre-selected: N table(s) already on the target", which stopped being true once
  the default followed Schema Conversion. It now reports the actual pre-ticked count out
  of the total and names where the set came from ("selected in Schema Conversion" vs
  "already on the target"), derived from the sets differing — so a reconnected user is
  never told their tables were a Step 2 choice they did not make in this session.

## v0.1.197

### Fixed

- **Restarting the app during a schema apply left the step spinning forever.** Reported
  from a real session: the UI was restarted while "Applying converted DDL to the
  target..." was running, and after reconnecting the spinner never stopped and the Apply
  controls stayed locked behind it. The apply runs in-process and its job id is
  deliberately never persisted, so a restart killed the work *and* lost the handle — the
  step still restored as `IN_PROGRESS` (which draws the spinner) while the poll timer
  that finalizes the status returned immediately on a missing job id. Nothing could ever
  clear it.

  A reconnect with no live apply handle now reconciles the step to `FAILED` (not `DONE`:
  there is no report proving completion, and the run demonstrably did not finish) and
  explains what happened — objects created before the restart are already on the target,
  and re-running with "Skip if exists" finishes the rest without touching them. A
  genuinely live apply, which still holds its job id, is left alone. Step 4 (Validation)
  already had this reconciliation; Step 2 never got it.

### Changed

- **The bulk apply now reads as the action on the Generated DDL list above it.** Its card
  sits below that list, and its title was the literal string "Apply to target" — the same
  three words as each row's per-object button — so the bulk action looked like a separate
  feature; the copy even had to point back with "…in the Generated DDL list above" twice.
  The card is now titled "Apply generated DDL to target", the body states the scope with
  its live count ("Applies the 7 objects from the Generated DDL list above"), and the
  button names what it applies ("Apply all 7 generated objects to target") instead of the
  scope-ambiguous "Apply all to target (7)". The single-object pointer is dropped when
  the scope is one object, where it only told the user to do what the button already does.

### Docs

- `CLAUDE.md`: recorded that the version the UI **displays** comes from installed package
  metadata (`importlib.metadata`), not `pyproject.toml` — the editable install picks up
  code edits but not the version, so a bump needs **`uv sync`** (not just `uv lock`)
  before restarting. The local UI had drifted six releases behind this way.

## v0.1.196

### Fixed

- **The target primary-key probe returned every column of every table on real Aurora
  DSQL.** `target_primary_key_columns()` (added in v0.1.192) unnested the whole of
  `pg_index.indkey`, but only its first `indnkeyatts` entries are the key — the rest are
  the index's non-key stored/included columns. On DSQL that is not an edge case: every
  primary index carries the table's remaining columns as payload, so an 11-table schema
  reported `indnatts` of up to 14 against `indnkeyatts = 1` throughout, and the function
  disagreed with `information_schema.key_column_usage` on **11 of 11** tables.

  The consequence was the opposite of the v0.1.192 intent: since a full column list never
  equals the applied composite key, every append into a populated target with a changed
  key would have been refused, quoting an absurd "actual" primary key. Bounding the
  unnest to `indnkeyatts` fixes it — re-verified against the same cluster, 11/11 tables
  now agree, and a missing table still returns `None`.

  Verified read-only against a live cluster (`ap-northeast-2`): `unnest … WITH
  ORDINALITY`, `JOIN LATERAL`, `pg_index.indisprimary/indkey/indnkeyatts`,
  `pg_table_is_visible` and `pg_attribute` all work on DSQL, and a real two-key index
  whose `indkey` is `'2 1'` returns its columns in **index** order — the guarantee the
  composite-key strategy depends on.

### Tests

- The `_PkCursor` double now honors the query's key-column bound instead of echoing a
  canned primary key, so it returns the stored columns whenever the statement omits
  `indnkeyatts` — reproducing the live-cluster shape. Two new tests (payload excluded;
  composite key kept in order with payload dropped) fail if the bound is removed. With
  the previous fake, all 2394 tests passed against a function that was wrong on every
  real table.

## v0.1.195

### Tests

- **The Full Load confirm dialog is now verified by actually opening it.** v0.1.194's
  disclosure was covered only by its pure helper plus a structural check on the closure,
  because the dialog builds lazily inside the Start handler. It is now driven for real:
  NiceGUI's `context.client` (a read-only property on a Context instance) and the
  pre-dialog `run.io_bound` target probe are patched, the captured Start handler is
  awaited, and the rendered text and button label are asserted — the disclosure naming
  the table and explaining that no data is lost, and the unchanged "Confirm and start"
  path when nothing will be recreated. Both mutations (removing the notice, not renaming
  the button) are caught by rendered output rather than source text.

## v0.1.194

### Fixed

- **The Full Load confirm dialog now discloses the tables whose schema it will
  recreate.** v0.1.193 recreates an empty target when its applied primary key differs
  from the source, but that decision was made inside the engine — *after* the
  confirmation dialog — so the dialog said only "Confirm and start" and never mentioned
  that a table would be dropped and recreated. Nothing is lost (the tables are empty,
  and the DDL is the one already approved in Schema Conversion), but a manual change made
  to a target table outside Schema Conversion is replaced, so it must be stated before
  the run. The dialog now lists those tables in an informational notice and labels the
  button "Recreate and load". A **populated** table is unaffected and still goes through
  the existing Append / Drop & reload choice, with its destructive label and red button.

### Added

- `schema_recreate_tables()` — a pure helper naming the empty targets whose primary key
  the load will recreate, so the dialog and the engine agree on the same set.

### Tests

- Covered the disclosure helper (changed key on an empty target, exclusion of populated
  tables, silence with no conversion or inventory) and a structural check that the list
  is threaded into the dialog as a parameter and closed over. That last test exists
  because the disclosure was first written to read `conv_state`/`inventory` from inside
  the dialog closure — names not in that scope — which would have raised `NameError` on
  every Start click with the whole suite still green, since no test opens the dialog.
  Four mutations killed, including restoring that fault.

## v0.1.193

### Fixed

- **A changed primary key is now delivered by recreating the schema, not by appending
  into whatever shape the target has.** v0.1.192 let an *empty* target load on the
  reasoning that "nothing exists to conflict with", which was true about row conflicts
  but missed the point: a changed primary key is a **schema** change, and appending
  cannot retrofit a key onto an existing table. An empty target still carrying the
  original single-column key therefore accepted every row and reported success — so a
  user who chose the Composite key strategy to avoid hot partitions got a table keyed
  the old way, now populated, correctable only by a destructive reload. Loading data in
  the wrong shape silently is worse than refusing.

  A table whose applied DDL asks for a different key and whose target is **empty** is
  now promoted to the replace path: the target is recreated from the applied DDL (which
  destroys nothing) and loaded with a plain `INSERT`, so the chosen key is real by
  construction. A **populated** target is unchanged from v0.1.192 — decided against its
  actual key, and refused when it disagrees, since a `DROP` there would destroy data the
  user never agreed to lose. The refusal now names the remedy that is actually
  available: `Drop & reload` normally, but "stop CDC first" while a sink is streaming,
  where recreating the table is impossible.
- **A sharded load keyed its skip-filter on the wrong columns.** Sharding is chosen from
  the *source* primary key, so a table with a single integer `id` shards even when its
  *target* key is a composite `(leading, id)` — and the shard worker passed no
  `key_columns` at all. The importer fell back to the source key, so an idempotent
  re-load filtered on `WHERE (id) IN (…)` against a target keyed `(leading, id)`, where
  `id` alone is not unique: the filter could match a different row and skip a source row
  that was never written. Only tables above the shard threshold (1M rows by default)
  were affected — the loads least likely to be verified by hand. The shard worker now
  passes the target key, and still defers to the source-key fallback when the key is
  unchanged.

### Tests

- Covered schema recreation on an empty target (including one still on the old key),
  CDC-coexisting appends and their distinct refusal wording, and both shard-worker key
  paths. Six mutations killed, including removing the recreate promotion, applying it to
  a populated target (destructive), applying it under a live sink, and dropping the shard
  key.

## v0.1.192

### Fixed

- **A table using the recommended Composite key strategy could not be loaded into an
  empty target.** Choosing "Composite key" in Schema Conversion (the hot-partition
  remedy the tool itself recommends), applying it, and then running the first Full Load
  failed the table with *"configured with a changed primary key … Load it fresh (Drop &
  reload)"*. The guard assumed that an append means "the target still has its original
  key" — but Schema Conversion had just applied the composite key, so the target really
  did have it. A safe load was refused as unsafe.

  The suggested remedy was also unreachable: the "Drop & reload" choice only renders for
  tables that already contain data, and the replace set is *derived* from that same set —
  so on an empty target there was no way to select it. **Full load + CDC** was worse
  still: it forces the append path regardless (a DROP would race the live sink), so no
  path existed at all.

  Full Load now resolves the key against the live target instead of assuming: an **empty
  target** loads with the applied key (nothing exists to conflict with, and rows unique
  on the source key stay unique under a composite key containing it); a **populated
  target** is checked against its *actual* primary key, read from the catalog, and used
  when it matches. It still refuses — with a message naming the real key — when the
  target genuinely disagrees, or when its key cannot be read at all ("unknown" is never
  treated as safe). Tables whose target key equals the source key are unaffected and
  never incur a target probe.

### Added

- `target_primary_key_columns()` — a read-only catalog probe returning a target table's
  actual primary-key columns in key order (schema and table travel as bound parameters).
  Returns `None` for "cannot determine", which callers must treat as unsafe.

### Tests

- Covered every branch of the append key decision (empty target, Full-load-+-CDC, a
  populated target that matches, one that still has the old key, an unreadable key, and
  the unchanged-key path asserting the target is never probed) plus the new probe's key
  ordering, bare-name resolution, injection-safety, and unknown paths. Eight mutations —
  including restoring the old blanket refusal and treating an unknown key as agreement —
  each killed.

## v0.1.191

### Fixed

- **The Data Migration table picker locked too early, with a dead-end remedy.** It froze
  the moment the prerequisite checks ran — but the checks are a *preview*, not a commitment,
  so the scope was locked before any migration began. Worse, the lock's tooltip told you to
  "re-run the checks to change which tables are migrated", yet re-running re-pins the same
  set, so there was no way out but Start over. The picker now stays editable until the
  selection is actually committed to something irreversible, and each lock explains its own
  cause and remedy:
  - a Full Load has run for this set (remedy: Start over);
  - CDC is streaming, so the source connector's table list is fixed (remedy: stop CDC);
  - CDC infrastructure is deployed or deploying — each table's Kafka topic partitions are
    fixed when the topic is created, so a table added afterwards would stream on a single
    partition forever (remedy: delete the CDC infrastructure). This lock covers the
    ~15-20 min window the MSK create overlaps the Full Load, which was previously unguarded.
- **A table added after the prerequisite checks could silently fail the whole Full Load.**
  A prerequisite report outlives the selection it covered (nothing clears it, and the picker
  is now editable). A table added since was never checked for a target schema, and one
  per-table failure fails the entire job. The Run button now blocks with the unchecked
  table named, and the Prerequisites panel shows a matching notice, until the checks are
  re-run. Removing a table is not treated as a gap — the report is then a superset, so
  everything still selected was already checked.

### Tests

- Added coverage for the table-picker lock, which previously had none: the pure
  `selection_lock_reason` across every commit state (editable with only a report; locked by
  a running/finished Full Load, live CDC, or deployed/deploying CDC infrastructure; scoped
  so a Full-load-only run is not frozen by an unrelated CDC stack), the rendered lock tooltip
  carrying the per-cause reason, and the asymmetric `prereq_scope_gap` (a removal is fine, an
  addition blocks). Every test was confirmed by mutation testing — nine mutations, including
  reintroducing the old too-early lock, each killed.

## v0.1.190

### Fixed

- **Edit mode's Copy button copied the pre-edit DDL.** The editor header captured the DDL
  string as it was when the editor was built, so after typing a fix, "Copy Target DDL"
  handed back the original — with a positive "copied" toast — while "Apply to target" sent
  the edited version. The same button row disagreed with itself. `_render_copy_ddl_button`
  now accepts a callable read at click time, and the editor header passes one that reads the
  live edit buffer. Verified in a browser: typing then copying now yields the edited DDL.

### Tests

- **Closed five gaps a code review found by mutation testing** — behaviours that could
  regress with the suite still green, because the tests asserted on `inspect.getsource(...)`
  substrings rather than rendered output, and the `_NotesUi` / `_DdlPaneUi` doubles discarded
  `props()`/`classes()`/`on_click`. Now caught, each confirmed by re-running the mutation:
  - inverting the conversion-note tints (a real `LOSS` shown calm sky-blue, an optional
    recommendation neutral-gray — the severity inversion this series existed to fix);
  - flipping the advisory badge to `negative` (red advice);
  - deleting `dialog.open()` / inverting the render guard / raising in the expand handler
    (the whole expand feature made a no-op);
  - clobbering `current` back to the generated DDL (the saved edit vanishing from view while
    Apply still sends it);
  - deleting the inline `.ddl-pane` height rules (the comparison panes falling back to
    CodeMirror's 256px default with no scroller cap).
  The `_NotesUi` and `_DdlPaneUi` doubles now record card-to-badge pairing, editor classes,
  button clicks and dialog opens, so these assert on what renders rather than on source text.

## v0.1.189

### Changed

- **The published ECR Public default now points at `0.1.188`.** Both regional ECRs
  (`ap-northeast-2`, `us-east-1`) and ECR Public carry `0.1.188`, verified including the
  anonymous pull path a fresh deploy uses.

## v0.1.188

### Fixed

- **Schema Conversion's note cards now match the Evaluation findings.** Each conversion
  warning and recommendation carried a bare `border`, which renders Tailwind's default
  near-black — it read as an outlined table cell rather than one of this app's cards, and put a
  harder line around an optional recommendation than Evaluation puts around an `UNSUPPORTED`
  finding. Both screens now use the same tinted surface with a matching `*-200` border: neutral
  gray for a real gap, the calm sky tone for advice (the same pair Evaluation uses), with
  `rounded-md` corners and the same padding. A test pins the two together, so restyling one
  screen surfaces the other being left behind.

## v0.1.187

### Changed

- **The published ECR Public default now points at `0.1.186`**, so a fresh clone deploys the
  code-editor DDL comparison and the aligned Bedrock defaults without building an image. Both
  regional ECRs (`ap-northeast-2`, `us-east-1`) and ECR Public carry `0.1.186`.

## v0.1.186

### Changed

- **The expanded DDL now opens as a dialog over the page, sized to its content, instead of
  taking the whole screen.** Maximized covered a 1440x900 display to show a panel that needs
  about **1060x800 at its widest** — measured across a real source, the longest line is 144
  characters and the longest DDL 29 lines — and losing the page behind it also lost the
  context the comparison sat in. The dialog is now `min(1100px, 92vw)` wide and grows with the
  DDL up to `min(44rem, 74vh)`: a 29-line object gets a 679px-tall editor, a 4-line one gets
  160px, neither clipped. `height: auto` on the wrapper is what makes that work — CodeMirror
  falls back to a fixed 256px otherwise, which pinned every DDL to the same height regardless
  of length.

## v0.1.185

### Added

- **Each DDL pane can be expanded full-screen.** The comparison is a split view, so every
  pane gets half the window — and measured against a real source, **14 of 18 tables had a line
  too long for that width** and 4 exceeded the pane's height. Both scroll, but reading a
  144-character `CHECK` constraint through a half-width porthole is what makes an operator
  copy the DDL out to an editor instead of reviewing it here. An expand icon beside each
  pane's copy button opens that DDL in a maximized dialog: full width, ~82vh tall, same
  dialect highlighting, read-only. Full-screen rather than a taller pane because **width** is
  the binding constraint. Opt-in, so the default two-pane view is unchanged for the objects
  that already fit. Edit mode deliberately has none — the dialog is read-only, and offering
  it beside a live editor would invite edits into a copy that is discarded on close.

## v0.1.184

### Fixed

- **The DDL editor now says which side you are editing.** Pressing **Edit** dropped both
  header bands, leaving a bare code box with nothing naming it — and since the source pane is
  read-only by design, "which one am I changing?" was a fair question on a screen whose whole
  point is source-vs-target. The editor now carries the same **Target — Aurora DSQL** header
  band as the read-only comparison, with its copy button, and the `Editing` badge moved onto
  that band beside the title it qualifies. Only the target header appears (full width): the
  source is not on screen to be confused with, and repeating it would imply it is editable
  too. The header is now one shared helper, so the two modes cannot drift apart.
- **The editor matches the comparison pane's treatment.** It was highlighted as generic
  `SQL` with wrapping on, while the pane beside it used `PostgreSQL` without wrapping — so
  switching into Edit changed how the same DDL read. Both now use the target dialect and
  keep one logical line on one line.

## v0.1.183

### Changed

- **The Schema Conversion DDL comparison is now a real code editor on each side.** It was a
  hand-built diff table that aligned the two DDLs line-for-line, which reads well until a
  line is long: it wrapped with `break-all` and split mid-token — an `ENUM` list came out as
  `'cancel` / `led')` across two visual rows — and one logical line occupying several rows
  pushed the two sides out of the vertical alignment the table existed to provide. Each pane
  is now NiceGUI's bundled CodeMirror, which brings what the table never had: **real SQL
  highlighting in each dialect** (MySQL on the left, PostgreSQL on the right, so backtick and
  double-quoted identifiers are each lexed correctly), line numbers, code folding, and
  selection that copies clean lines. Long lines stay on one line and scroll horizontally,
  like a Markdown fence.
  - What is given up is the line-for-line pairing: each pane starts at line 1, so a changed
    line is no longer physically beside its counterpart. The panes hold the DDL for one
    object and the conversion notes below already name what changed (removed foreign keys,
    async indexes, remapped types), so that pairing is stated in words rather than inferred
    from row positions.
  - The panes are `disable`d, not `readonly`: NiceGUI's CodeMirror has no readonly prop and
    silently ignores one, which left the comparison editable — a user could type into it,
    watch the change vanish on the next re-render, and have **Apply to target** still send
    the unedited DDL. Editing still has its own mode behind the **Edit** button, and Apply
    still sends that buffer; both were verified end-to-end in a browser.
  - The diff engine behind the old view (`diff_ddl_lines`, `DiffRow`, `DiffKind`, the cell
    renderer and its `DIFF_*` design tokens) had no other caller and is gone — a net 139
    fewer lines.

## v0.1.182

### Changed

- **The published ECR Public default now points at `0.1.181`**, so a fresh clone deploys the
  aligned Bedrock defaults and Sonnet 5 without building an image. Both regional ECRs
  (`ap-northeast-2`, `us-east-1`) and ECR Public carry `0.1.181`.

## v0.1.181

### Fixed

- **AI Assist failed with `AccessDenied` on a default deploy.** The CloudFormation template
  defaulted `BedrockModelId` to `us.anthropic.claude-sonnet-4-6` while the app's own default
  was `global.anthropic.claude-sonnet-4-6`. The task role's `bedrock:InvokeModel` scope is
  **derived** from the template value, but the app falls back to *its* default whenever the
  Connect form's Model ID is left blank — so a stock deploy invoked a profile whose ARN the
  policy never allowed, and "Verify AI access" reported a permissions error that looked like
  a broken IAM policy rather than two defaults out of step. A test now asserts the two
  cannot drift, and that the default is one of the `AllowedValues`.
- **`BEDROCK_MODEL_ID` stopped reaching the form once AI Assist was enabled.** The prefill
  compared the *whole* config to a pristine `AiAssistConfig()`, so merely flipping the Enable
  switch made it unequal and silently skipped the seed on every later render — leaving the
  app on its built-in default while IAM was scoped to the deployment's. The check is now
  per-field: the model id seeds while it still holds the built-in default, the region while
  it is unset, and a value the user typed is never overwritten.

### Changed

- **The default model is now Claude Sonnet 5** (`global.anthropic.claude-sonnet-5`), with
  Opus 5 offered alongside it. Verified live: the profile is `ACTIVE` and invokes
  successfully from `ap-northeast-2`, and the IAM scope the template derives from it
  (`inference-profile/global.anthropic.claude-sonnet-5` plus
  `foundation-model/anthropic.claude-sonnet-5`) resolves to a real model.
- **Only `global.` inference profiles are offered now.** The `us.` variants resolved from
  just `us-east-1` / `us-east-2` / `us-west-2` and failed everywhere else, while `global.`
  works in all of them — verified against `us-east-1`, `us-west-2` and `ap-northeast-2`. They
  were a trap rather than a choice, and having two geo prefixes is what let the template
  default drift from the app's in the first place. No `us.` model id remains anywhere in the
  repo; the deployment guides and manual (EN/KO/JA) and the README were updated to match.

## v0.1.180

### Fixed

- **One object kind is no longer named two ways on the same screen.** The source tally read
  `3 Routines` while the list and chart below split the very same objects into
  **Stored procedures** and **Functions** — so a reader counted three of something whose
  heading does not exist. MySQL does group both under `information_schema.ROUTINES`, so the
  inventory field is named correctly; the assessment splits them because DSQL treats them
  differently (a `LANGUAGE SQL` function can survive where plpgsql cannot). The tally now
  speaks the list's vocabulary: `2 Stored procedures · 1 Functions`, falling back to
  `Routines` only when a subtype is genuinely unknown, and empty kinds are dropped instead of
  showing a zero tile.
- **The chart axes showed raw enum values.** Both the UI chart and the HTML export's chart
  labeled their bars `PROCEDURE` / `FUNCTION` beside a list heading reading
  `Stored procedures` — the same mismatch, one row lower. The label map moved from the UI
  into `core/assessor.py` (`KIND_LABELS`), so the list headings, both charts and the tally
  now read from a single source; a test asserts the UI holds the same object, not a copy.

## v0.1.179

### Changed

- **The published ECR Public default now points at `0.1.178`.** That is the image a fresh
  `git clone` deploys without building anything, and it had been pinned at `0.1.167` for
  eleven releases — so a new deployment shipped none of the Evaluation work from `0.1.168`
  onwards. Both regional ECRs (`ap-northeast-2`, `us-east-1`) and ECR Public now carry
  `0.1.178`.

## v0.1.178

### Changed

- **A collapsed object row now carries one labeled badge per findings category, replacing the
  single governing badge.** That badge named only the most severe classification and was
  silent about the rest, so a row reading `Unsupported` could hide six findings of which four
  were merely review-needed and one was optional advice — the object looked wholly blocked
  when most of it was not. Each row now reads
  `1 Unsupported · 4 Review needed · 1 Recommended` as colored badges: red, amber, and the
  calm info-blue that advisory findings already use inside. Every badge keeps its label
  rather than showing a bare count, so severity never rests on color alone — a monochrome
  screenshot or a colorblind reader would otherwise need the chart legend to decode it. The
  leading badge is the classification the old single badge showed, so the row still reads
  worst-first, and the separate gray breakdown line it replaces is gone.

### Fixed

- **A cluster-level finding rendered in the old, pre-`concerns` style.** The
  `Database / cluster-level` row (multiple source databases, table-count limit) showed bare
  **Risk** / **Recommendation** paragraphs while every table beside it used the labeled card
  treatment — one row in the list looked like a different application. The cause was data,
  not styling: inventory-level checks build their `AssessmentItem` directly instead of going
  through the aggregation that populates `concerns`, and left it empty. They now carry their
  finding as a concern, so the row gets the same category badge, spine and Risk/Recommendation
  panels as any table — and the text and HTML exports pick it up for free, since all three
  render the same list. A report-wide test now asserts that only an `AUTO` object may have no
  concerns.

## v0.1.177

### Changed

- **A collapsed object row now breaks down the findings its badge hides.** The header badge
  states only the *governing* classification, which is silent about the rest: measured on a
  real source, 16 of 18 tables carried a mix — typically a real gap plus the
  `AUTO_INCREMENT` recommendation — behind a single badge, and a row reading `Unsupported`
  could hide six findings of which four were merely review-needed and one was optional
  advice. The object looked wholly blocked when most of it was not. Each row now adds
  `1 Unsupported · 3 Review needed · 1 Recommended`, in the same `N Label · M Label` shape
  the kind-group heading above it already uses, with advisory findings counted as
  `Recommended` — the word their own badge uses inside. It is omitted when it would merely
  repeat the badge (a lone finding of the governing class, or a clean object).
- **The per-object effort badge is gone from the collapsed row.** It described the object as
  a whole while the row now summarises its findings, and one `SIMPLE` fix beside one
  `SIGNIFICANT` one does not average into a useful number. Each finding still carries its
  own estimate when expanded, and the schema-wide distribution stays in the summary above
  the list.

## v0.1.176

### Changed

- **Effort badges now render the same neutral outline everywhere.** The summary row colored
  each level on the green/amber/red ramp while the object rows and finding cards drew the
  same value in gray — one value, two treatments. The ramp is also the wrong signal: on this
  screen it means *compatibility* (the chart, the classification badges, the
  Risk/Recommendation panels all use it), whereas effort is an ordered scale of hours, not a
  severity. Coloring it both diluted that meaning and collided on the object rows, where an
  amber `Review needed` badge sat beside an amber `effort: MEDIUM` and a red `Unsupported`
  beside a red `effort: SIGNIFICANT`. All three surfaces now share one constant, so colour
  stays reserved for compatibility and cannot drift apart again.

## v0.1.175

### Changed

- **The effort summary moved out of the report header and down beside the object list.**
  It sat directly under the classification row, above a chart that splits by
  classification — so a summary the chart says nothing about sat beside the one the chart
  is built from. Worse, the two rows looked identical but did not add up to the same total
  (`SIMPLE 1 · MEDIUM 3 · SIGNIFICANT 2` = 6 against 8 objects), because an object with no
  required work — all-`AUTO`, or carrying only a recommendation since v0.1.174 — has no
  effort estimate and lands in no bucket. Read beside a classification row that does total
  every object, that looked like missing objects rather than a different question. Effort
  is a tool for working the list, so it now sits with that list and its effort filter, and
  spells out "(*n* of *m* objects need work)". It is omitted entirely when nothing needs
  work. The header keeps only the classification counts, matching the chart word for word.

## v0.1.174

### Changed

- **An object's findings are now ordered by priority: real gaps first, advice last.**
  Sorting by severity alone interleaved the two — the advisory `AUTO_INCREMENT` finding is
  classified `MANUAL`, so it landed above a genuine `MANUAL` gap purely by rule
  declaration order, and a reader expanding a table met an optional throughput note before
  the foreign key they actually have to deal with. Gaps now sort ahead of every
  recommendation and stay ranked by severity among themselves (`UNSUPPORTED` before
  `MANUAL`), so the list reads top-to-bottom as "act on this now" down to "you could also
  tune this". A consequence worth having: the row header's governing rule is now a real
  gap whenever the object has one, instead of sometimes advertising a recommendation as
  the object's headline. An object whose only finding is advice still reports it. The
  screen, the text export and the HTML export all render the same list, so all three
  reorder together.

## v0.1.173

### Changed

- **Evaluation now separates recommendations from real conversion gaps.** `Classification`
  answers "how much work" but not "is anything actually wrong", and conflating the two made
  advice look like a defect. A finding now also carries a **note kind**: a `LOSS` (something
  could not be carried over or changed meaning) or a `RECOMMENDATION` (the conversion is
  complete and correct; ignoring it costs performance, not correctness). `AUTO_INCREMENT` is
  the recommendation — such a key converts cleanly, and switching to a UUID/random or
  cached-identity key buys insert throughput.
  - The enum is `ConversionNoteKind`, moved from `core/converter.py` into `core/models.py`
    so **both** assessments share it. It was converter-local when introduced in v0.1.151,
    which is exactly why only Schema Conversion got the distinction while Evaluation kept
    calling an `AUTO_INCREMENT` key a risk — the two screens contradicted each other about
    the same key for 20 releases. One shared enum makes that class of drift impossible
    rather than merely fixed once. `core.converter` re-exports it, so existing imports work.
  - **A recommendation no longer inflates an object's effort estimate.** Effort answers
    "how much work must I do to migrate this", and optional throughput advice is not work
    the migration requires. A table needing only a foreign-key workaround (`SIMPLE`, under
    two hours) was reported as `MEDIUM` (two to six) purely because it *also* had an
    `AUTO_INCREMENT` key — and since MySQL tables overwhelmingly do, this inflated the
    estimate for the most common table shape there is. Measured against a real 7-table
    schema, two tables moved from `MEDIUM` back to `SIMPLE`. An object whose findings are
    *all* advisory now carries no effort at all. The advice still shows what taking it
    would cost ("effort if you take it"), so the choice stays informed.
  - Advisory findings render in the calm info-blue treatment — `RECOMMENDED` badge, a
    `Note` caption instead of `Risk` — on the screen, in the text export (`[RECOMMENDED]`)
    and in the HTML export (info-blue cell, deliberately outside the green/amber/red
    severity ramp). Findings default to `LOSS`, so every other rule is untouched and a
    report persisted before this change renders exactly as before.

### Fixed

- **Three more dead KO manual links.** `ko/11-customer-faq.md` still pointed at
  `10-conclusion.md` with English anchors, which the v0.1.166 sweep of 16 links missed.
  Every cross-chapter anchor in `docs/manual/` now resolves.

## v0.1.172

### Fixed

- **Evaluation no longer presents an `AUTO_INCREMENT` key as a defect.** It read
  *"AUTO_INCREMENT column 'id' produces monotonic keys that cause hot partitions in Aurora
  DSQL"* under an amber **Risk** heading — but such a key converts cleanly and works
  correctly: nothing is dropped and no query returns a different answer. Moving to a
  UUID/random or cached-identity key buys **insert throughput**, because DSQL stores rows
  in primary-key order so a monotonic key concentrates writes on one partition. The text
  now leads with what is true of the table ("converts cleanly and works as-is") and marks
  the change as optional, matching the correction Schema Conversion already made in
  v0.1.151 (`ConversionNoteKind.RECOMMENDATION`) — that pass missed this rule, so the two
  screens contradicted each other about the same key.

## v0.1.171

### Changed

- **The compatibility chart now ranks object kinds by size, largest first.** Ordering by
  trouble-share put a single unsupported `TRIGGER` above two hundred tables, so a stub bar
  floated on top of the long ones — which reads as a broken chart rather than as a
  priority. Bars now step down in length (`TABLE`, `PROCEDURE`, …), and each bar still
  carries its own red segment and its "*n*% need attention" caption, so nothing about the
  severity signal was lost. The HTML export is built from the same aggregation and so
  reorders identically; a test now pins the two orders together.
- **Each Evaluation finding labels its problem and its fix as two distinct blocks.** The
  risk was a bare sentence and the recommendation a fainter one below it, marked only by a
  small arrow — the pair read as a single wrapped paragraph, and the fix was easy to skim
  past. Both now sit on their own tinted panel with a leading glyph and caption: amber
  **Risk**, green **Recommendation**, matching the amber = be-aware / green = resolved
  tones used across the app. The text and HTML exports already labelled the two (`Risk:` /
  `Fix:`, and their own table columns), so this brings the screen up to the level the
  exports were already at.

## v0.1.170

### Changed

- **The Evaluation chart now bars objects by compatibility, not by effort, and the exported
  report follows the screen.** The bar sat directly above a classification summary and a
  list whose badges read Auto-converted / Review needed / Unsupported, yet split its own
  segments into Simple / Medium / Significant actions — two vocabularies for one picture,
  so answering "how much of my schema actually moves?" meant translating between them. The
  stack is now the three classifications in that order, so a bar reads left-to-right from
  "moves by itself" to "cannot move", and per-kind rows are ordered most-blocked first.
  The HTML export renders the same aggregation with the same labels and colors, retitled
  **Compatibility by object kind** to match, and its per-bar caption now reads
  "*n*% need attention" (everything not auto-converted). Effort is unchanged and still
  reported — in its own summary badges, in the filters, and per object.
- **Each expanded Evaluation finding is now a bordered card behind a single indent
  spine.** Findings were separated by flat rules, so with two objects expanded the blocks
  ran together and it was not obvious which object a given finding belonged to. The spine
  plus card is the same containment idiom the Schema Conversion object tree uses.
- **The HTML report gives each finding its own table row.** The previous revision put a
  list of risks in one cell beside a list of fixes in another, which still asked the reader
  to count list positions to pair them. Each finding is now a row carrying its own rule id,
  classification and effort, with the object and kind cells spanning the group; the filter
  controls hide a whole group together and the counter still counts objects.

## v0.1.169

### Changed

- **Evaluation now lists each risk with its own fix, instead of one run-on sentence.** An
  object commonly trips several independent rules — a foreign key, an `AUTO_INCREMENT`
  key, a case-insensitive collation, an `ENUM` column and an `ON UPDATE` timestamp are
  five separate decisions with five separate remedies. Every rule's text was joined into a
  single **Risk** paragraph and a single **Recommendation** paragraph, so the report became
  unreadable exactly when it had the most to say, and matching the *n*-th risk to the
  *n*-th fix was left to the reader. Each matched rule is now its own block — carrying its
  own rule id, classification and effort — in the Evaluation screen, the text export, and
  the HTML report (where the two columns become aligned lists). A per-concern
  classification badge also makes it visible when one finding is `UNSUPPORTED` while the
  rest are `MANUAL`; the row header shows only the governing class, which used to hide
  that. The joined `risk`/`recommendation` strings are still populated for back-compat and
  flat exports, and a report persisted before this change falls back to rendering them.

## v0.1.168

### Changed

- **The published ECR Public default now points at `0.1.167`.** That is the image a fresh
  CloudFormation deploy pulls, so it has to track the shipped version — it carries the
  column-`DEFAULT` preservation and the three DDL-rejection fixes from v0.1.166/v0.1.167,
  without which a new deployment would silently produce schemas the cluster rejects (or
  accepts while dropping every default).

## v0.1.167

### Fixed

- **`ON UPDATE CURRENT_TIMESTAMP` made the generated `CREATE TABLE` fail.** v0.1.166 began
  emitting column defaults, but SQLAlchemy's MySQL reflection folds the `ON UPDATE` clause
  *into* the default — `datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`
  reflects as the single string `"CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"` — so it
  was passed through verbatim and the target rejected it with *`syntax error at or near
  "ON"`*. On the most common audit column there is, that turned a missing default into a
  failed conversion. The root cause is fixed at the source: column defaults now come from
  **`information_schema.COLUMN_DEFAULT`**, which keeps the `ON UPDATE` half in `EXTRA`
  where `auto_update_timestamp` already reads it (and the assessor already reports it
  MANUAL — DSQL has neither an `ON UPDATE` clause nor triggers).
- **Two more defects the same reflection path caused.** MySQL 8 reports an *expression*
  default parenthesized, so `DEFAULT (uuid())` arrived as `"(uuid())"` and was misread as a
  literal; and a `bit(1) DEFAULT b'1'` was **silently dropped** by the reflection regex.
  `information_schema` reports both correctly. Expression-vs-literal is now decided by
  MySQL's own `DEFAULT_GENERATED` flag rather than inferred from quoting, which could not
  tell the literal string `'CURRENT_TIMESTAMP'` from the function call.
- **`bigint unsigned AUTO_INCREMENT` keys failed under the identity strategy.** An unsigned
  integer key maps to `DECIMAL(20,0)` to preserve its range, and DSQL identity columns must
  be `BIGINT` — so 6 of the 11 tables in the reference schema were rejected with *`identity
  column type must be bigint`*. `DECIMAL` is now widened along with the narrow integer
  types; an identity sequence is BIGINT-bounded on DSQL anyway, so no generatable value is
  lost.
- **A `DATETIME` default of `CURRENT_TIMESTAMP` now pins to UTC.** `DATETIME` maps to a
  no-timezone `timestamp`, and the loader deliberately normalizes migrated rows to naive
  UTC; a bare `CURRENT_TIMESTAMP` default would have inherited the session `TimeZone`
  instead, so rows written by the application after cut-over could disagree with the
  migrated ones by hours.

### Changed

- **One supported inventory shape, no heuristics.** The converter reads defaults only in
  the form `introspector.enrich_columns` produces (unquoted value + `DEFAULT_GENERATED`
  flag), which every MySQL source goes through unconditionally. The quoting-based fallback
  was removed rather than left to guess. The verification script now enriches too — without
  it, it was exercising a code path the app never takes, and duly reported failures that
  only existed there.
- **Rarer literal/target mismatches deliberately have no special-case branch.** A
  bit-string default on an integer target, a binary default on `bytea`, MySQL's
  `0000-00-00` zero date: none occurs in a real schema, and each would add a code path plus
  tests for a case nobody hits. They fall through to the general rule, where a rejection is
  loud at conversion time rather than silent.
- **The manual now documents default handling** (EN/KO/JA) — chapter 2's "what the
  conversion does for you" list was silent on defaults, and chapter 4's constraint table
  now records that DSQL *does* support them while `ON UPDATE CURRENT_TIMESTAMP` is
  unreproducible.

### Added

- **`scripts/verify_conversion_on_dsql.py` is now published** (it was excluded by the
  `scripts/*` ignore rule) and documented in `scripts/README.md` as a customer-facing
  read-only check: convert your own schema and find out what Aurora DSQL rejects *before*
  migrating. Its synthetic matrix grew to 53 cases, including every default form above.

## v0.1.166

### Fixed

- **Column `DEFAULT` values were dropped during Schema Conversion, silently.** Aurora DSQL
  supports column defaults (`DEFAULT default_expr` is in its documented `CREATE TABLE`
  grammar, and literals, expressions, `CURRENT_TIMESTAMP`/`now()`, `gen_random_uuid()` and
  `NOT NULL DEFAULT` were all confirmed on a live cluster) — the converter simply never
  emitted them: `ColumnDef.default` was populated by the introspector and read by nothing.
  Migrated rows were unaffected (the loader writes explicit values), but rows the
  **application** writes after cut-over were not: MySQL accepts an `INSERT` that omits a
  `NOT NULL` column *with* a default, and the target rejects that same `INSERT` with a
  not-null violation. Every one of the 22 defaulted columns in the reference schema is
  `NOT NULL`, so this was not a corner case. Defaults are now carried across, with three
  translations that a pass-through would get wrong: `tinyint(1) DEFAULT '1'` becomes
  `DEFAULT TRUE` (`DEFAULT 1` on a boolean is a hard error on DSQL), the `AUTO_INCREMENT`
  column gets no default (an identity column carrying one is rejected), and a generated
  column gets none (its value is computed). A default that genuinely cannot be translated
  (MySQL `UUID()`, or a `tinyint(1)` default outside 0/1) is dropped **with a warning** that
  spells out the post-cut-over consequence — previously nothing was reported at all.
- **Identity primary keys produced DDL that Aurora DSQL rejected outright.** Two separate
  defects, both on the `IDENTITY_WITH_CACHE` strategy and both invisible to the tests
  because they assert on generated DDL *text*:
  - `CACHE 100` — DSQL requires `CACHE` to be stated explicitly and accepts only `1` or
    `>= 65536` (*"CACHE (100) must be greater than or equal to 65536 or equal to 1"*). Now
    65536, the smallest cached value, which is also the point of the strategy.
  - an `INT` identity column — DSQL sequences are BIGINT-only (*"datatype integer not
    supported, identity column type must be bigint"*), and MySQL `int AUTO_INCREMENT` is the
    most common primary key there is, so this broke the typical table. Narrow integer
    identity columns are now widened to `BIGINT` (lossless).
- **`DECIMAL` past Aurora DSQL's precision ceiling failed to apply.** MySQL allows
  `DECIMAL(65,30)`; DSQL caps precision at 38 and scale at 37. The spec is now clamped —
  with a warning, since clamping loses range — and a reduced precision drags an over-large
  scale down with it (`scale > precision` is itself an error).

### Added

- **`scripts/verify_conversion_on_dsql.py`** — applies the converter's own output to a live
  Aurora DSQL cluster and reports anything the cluster rejects. This is the check that was
  missing: the unit tests assert on generated DDL text (so a snapshot happily pinned the
  broken `CACHE 100`), and the end-to-end run exercises one hand-built schema that contains
  none of the shapes above. It sweeps a 49-case synthetic matrix over the *long tail* of the
  MySQL dialect (`SET`, `BIT`, spatial, wide `DECIMAL`, quoted/empty/negative literal
  defaults, generated columns, …) plus, optionally, every table in a real source schema —
  under every primary-key strategy, since two of the three defects were only reachable
  through a non-default one. Read-only against the source; on the target it creates and
  drops tables in a scratch schema. Exits non-zero, so it can gate a release.

## v0.1.165

### Fixed

- **The published ECR Public image — what a new deploy actually pulls — was 130 releases
  stale.** `deploy/cloudformation.yaml` defaults `ContainerImageUri` to
  `public.ecr.aws/.../mysql-dsql-migrator:<tag>` so a normal deploy needs no image build,
  but that tag still read `0.1.34` (2026-07-02) while the app was at `0.1.164`. Anyone
  deploying from the template got a July 2 build — without the Query Converter rename,
  the Settings dialog, the CDC security-group fix, or anything else since. Publishing to
  ECR Public is an opt-in extra step (`PUBLIC_IMAGE_URI=…` on the build script) that is
  easy to skip, and nothing checked the result. `0.1.164` is now published and the
  default points at it, and a test asserts the default stays on the shipped major.minor
  line and within 20 patch releases of it — with the republish command in the failure
  message. The default is also asserted to be a pinned numeric tag, never `latest`, so a
  redeploy of "the same template" cannot silently change images.

## v0.1.164

### Changed

- **The sidebar footer is now a single "Settings" entry that opens a tabbed dialog.**
  Performance tuning, Diagnostics and the activity-log download used to sit in the
  sidebar as two inline expansion panels plus a button — which squeezed a nine-field form
  into the ~16rem sidebar column, and made opening one panel shove the others around.
  They are also all the same kind of thing (app-wide runtime settings, none of them part
  of the migration flow), so they now live behind one gear row. The dialog groups them as
  **Performance / Diagnostics / Activity log** tabs, because you come here to change one
  category, not to read all three. The body is built once, so a half-typed value survives
  closing and reopening, and the dialog is persistent with an explicit close so an
  outside click cannot discard it. Each panel is bounded (`max-height` + scroll) but not
  padded to a common height — a two-control panel no longer renders a screen of empty
  space. With the extra room, the copy also says what each section actually affects
  instead of repeating the same "live, app-wide" caveat three times.

## v0.1.163

### Changed

- **The Query Converter's SQL editor can be resized by dragging its bottom-right
  corner.** A long statement no longer has to be scrolled inside a fixed box. This
  required dropping Quasar's `autogrow`: it rewrites the textarea's inline height to the
  content height on every input event, so it undid a manual drag the moment you typed —
  the two cannot coexist. The editor now starts at the same tall default, has a
  `max-height` so a very large paste cannot push the Convert button off-screen, and keeps
  a drag applied even while typing (verified in a real browser). The resize grip also had
  to be pulled out to the field's corner: Quasar insets field content by 12px
  horizontally, which left the browser-drawn grip under the rounded border, rendering as
  a half-clipped mark.

## v0.1.162

### Changed

- **The "Query validation" tool is now called "Query Converter".** The old name reused
  *Validation*, which is Step 4's own name for a completely different job — comparing
  **migrated data** by exact `COUNT(*)`, checksums and per-table PK reconciliation — so
  an optional side tool read as a repeat of a workflow step. It also named the screen
  after a secondary action: conversion is the one thing this screen always does (the
  target test needs a verified target connection, and the AI review / AI DBA tuning are
  further opt-ins on top), so the title now names the core action. It pairs with Step 2's
  **Schema Conversion** — schemas there, queries here. The caption still reads
  *"Convert & test app queries"*, so the narrower title hides nothing. The manual is
  updated in all three languages; chapter filenames are unchanged (they are linked from
  many places), and the code keeps its `query_playground` module name, noted in that
  module's docstring so the two spellings are not mistaken for two screens.

## v0.1.161

### Fixed

- **Tooltips flickered and could not be read while a background job was running.** A
  Quasar tooltip is a *child* of the element it is attached to, so re-rendering a region
  destroys the element the pointer is over — Quasar closes the tooltip, and it only
  reopens on a fresh hover. Both of the sub-second polls did an unconditional full
  re-render on every tick, which meant the tooltip was recreated 2–3 times a second:
  - **Validation's "Cancel validation"** (0.5 s poll). Only three things actually change
    during a run — the progress label, the progress bar, and the cancel/stopping state —
    so the panel is now built once and the poll updates those in place (`set_text` /
    `set_enabled` / `set_value`) and re-arms its own timer, the way the Connect step
    already gates its Next button. A terminal status still re-renders, since the whole
    screen changes to the result view.
  - **Query playground's "Test on target"** (0.4 s poll). Nothing in the probing branch
    changes between ticks (a spinner plus fixed text), so the poll now waits for the
    probe to finish and re-renders exactly once, when the verdict actually needs drawing.
  `render_notice` returns its header/body labels so a polled region can swap the wording
  in place; existing callers that draw a static notice are unaffected.

### Notes

- The same pattern still exists on the slower polls — Full Load progress (1.5 s) and CDC
  monitoring (5 s). They are far less disruptive at those intervals and are left for a
  separate change.

## v0.1.160

### Fixed

- **The source-change check now works on RDS MySQL, where it always read
  "unavailable".** Drift was judged by GTID only, but RDS MySQL 8.0 cannot enable GTID
  — so on the tool's primary supported source every run reported *"could not be
  determined (GTID unavailable)"* and the section could never answer its own question.
  The watermark already records the binlog `file:position` (and CDC already resumes
  from it), so the coordinate needed was being collected and then ignored. Drift is now
  judged by GTID when both sides have one and otherwise by binlog `file:position`, and
  the report records which basis was used. The comparison tests equality rather than
  ordering, which is what makes it correct across a log rotation (the position restarts
  in each new file, so a later file can hold a smaller offset) and treats a coordinate
  that moved backwards — a restored source, `RESET MASTER` — as changed rather than
  clean.

### Changed

- **The section reads through the migration type instead of stating a raw fact.** An
  advancing source is the normal steady state under live CDC, but the panel said "the
  source has advanced since the snapshot" regardless of migration type, which reads as
  a problem. Now: with CDC it is `info` ("expected — CDC is replicating them; drain to
  zero lag before the final check"); without CDC it is `warning` and says plainly that
  those rows are **not** on the target and cutting over now would lose them; no change
  is `success`; and an undeterminable result stays `info` rather than alarming.
- **Plainer heading and detail.** "Drift since snapshot" was jargon twice over —
  "drift" is a replication term and "snapshot" is the tool's internal name for the
  watermark — so the section is now **"Source changes since the comparison"**. The raw
  coordinate pair moves into a collapsed "Technical detail" block (its values cannot be
  read as "how far behind"; a GTID is not a distance) and leads with the coordinate
  that actually produced the verdict, naming *why* when GTID is off — instead of
  putting two "unavailable" rows at the top and burying the evidence that was used.

## v0.1.159

### Changed

- **Validation's "Objects to validate" shortcuts now match every other object picker.**
  Schema Conversion and Data Migration render their "Select all"/"Unselect all" with the
  same treatment — primary + `done_all` for the affirmative action, grey + `remove_done`
  for the clearing one — but Validation's "Include all"/"Exclude all" carried neither the
  color nor the icon, so beside those screens they read as a different app. They now use
  the shared convention, and a test asserts it against the other screens' source so the
  three cannot drift apart again. Gating is unchanged: both stay disabled while a run is
  in flight, and each is enabled only when it would actually change something.

## v0.1.158

### Changed

- **"Cancel validation" now says what it is waiting for.** The cancel is cooperative and
  is only polled at two points — before each table, and every few thousand merged rows
  inside a PK reconciliation — so a `COUNT(*)` or checksum already executing on a large
  table has no interruption point and runs to completion first (minutes), as does every
  table being compared concurrently. The screen showed only "Stopping…" next to an
  unchanged "Comparison in progress — safe to leave running" panel, so a cancel that was
  in fact winding down correctly looked like a click that had been ignored. The label now
  reads *"Stopping… waiting for the in-flight table comparisons to finish."*, the panel
  switches to explaining the wind-down (what is skipped, why an in-flight query cannot be
  interrupted, and that no partial report is produced), and the button keeps its own name
  instead of relabelling itself "Stopping…" — which duplicated the status label and left
  nothing naming the requested action. The determinate progress bar is hidden while
  stopping, since it tracks tables *completing* and would keep advancing against the
  "Cancelling" message. Behavior is unchanged: this is honest feedback, not a new stop
  mechanism, and validation remains read-only throughout.

## v0.1.157

### Fixed

- **Data Migration showed "Success" the moment CDC start was pressed — before any data
  streamed.** The step (and its badge on both the stepper header and the in-screen status
  chip) is promoted to Done once CDC is live, which also unlocks Validation. But it was
  gated on the same signal that latches the CDC *inputs* — and that signal deliberately
  fires the instant Start is pressed (so the start point / table set can no longer be
  edited), while the connectors are still coming up on MSK Connect (~10–20 min) and no
  row has reached the target. So the header read Success mid-start. The promotion now
  uses a separate, narrower signal — connectors actually detected, or the cdc-stack phase
  is `running` — so "Success" means data is genuinely flowing. The input-locking latch is
  unchanged (it still fires at Start, as it should), and a finished Full Load still marks
  the step Done as before.

## v0.1.156

### Fixed

- **A stray apostrophe in the CDC template made every CDC deploy fail — and left the
  failed stack needing manual cleanup.** The inline HTTPS-egress rule on
  `ConnectorSecurityGroup` described itself as reaching S3 "via the *customer's* own NAT".
  EC2 accepts only `a-zA-Z0-9` and `. _-:/()#,@[]+=&;{}!$*` in a security-group **rule**
  description — the apostrophe is not in that set, and the set is narrower than the
  free-form text allowed in `Parameters` and resource descriptions elsewhere in the same
  template, so it read as perfectly normal prose. The result (observed on
  `mysql-dsql-cdc-stack-0729`) was `ConnectorSecurityGroup CREATE_FAILED - Invalid rule
  description`, which rolled the stack back — and the rollback itself then hit
  `ROLLBACK_FAILED`, because the two `CustomPlugin` resources were still `CREATING` and
  MSK Connect refuses to delete a plugin in that state. So a single character cost a
  manual stack cleanup rather than a simple retry. The description is reworded, and two
  tests now validate every security-group rule description in the template — inline rules
  and standalone `AWS::EC2::SecurityGroup{Ingress,Egress}` resources alike — against EC2's
  character set and its 255-character limit, so the next one cannot reach a deploy.

## v0.1.155

### Fixed

- **"Deploy CDC infrastructure" no longer looks ready before you enter a VPC ID.** VpcId
  is the one deploy input the tool cannot infer — subnets/NAT, the plugin S3 bucket, the
  DSQL cluster ARN, the source host and its credentials secret are all resolved at deploy
  time — but it was validated only in the submit path. So the button appeared enabled,
  clicking it opened the confirmation dialog (which runs a VPC network diagnosis and a
  cost estimate), and only after clicking Deploy did a toast say *"Enter your VPC ID."*
  The button is now disabled until the field is filled, with a one-line hint saying what
  is missing, and it enables as soon as you enter the ID. The gate is updated in place
  rather than by re-rendering the form, so the field you are typing in is never recreated
  under the cursor and the first Deploy click is not swallowed — which matters because the
  next move after entering the ID is to click Deploy, and a click on a still-disabled
  button is silently lost. A field holding only whitespace still counts as empty, matching
  the submit-path check exactly, and an unmet prerequisite check still takes precedence,
  so only one blocking reason is shown at a time.

### Changed

- **The sidebar Connect item now shows whether you are actually connected.** Its icon
  reflected only whether Connect was the selected view, so a session whose credentials
  had been dropped by an app restart (they are never persisted — Property 7) looked
  exactly like a healthy one, and nothing hinted that Connect had to be revisited before
  anything could run. The icon now carries the connection state: a green link with
  "Connected" when both source and target are verified, an amber broken link with
  "Reconnect to resume" when restored progress needs re-verification, and the neutral
  grey link when a fresh session simply has not connected yet. Amber rather than red is
  deliberate — the data is intact and re-entering credentials fixes it, so per the design
  system's severity calibration it is a recoverable warning, not a blocking error, and it
  matches the existing amber reconnect banner and diagram badge that describe the same
  state. The icon is driven by the same signal as that banner, so the two cannot disagree.

## v0.1.154

### Fixed

- **"Deploy CDC infrastructure" was blocked after a finished Full load + CDC run.** The
  CDC prerequisite gate added in v0.1.145 demanded a CDC-mode report, but those reports
  live in process memory only — they are deliberately never persisted, and the Full Load
  clears them when it starts. So the normal flow (run the CDC prerequisites → let the
  load finish → deploy) hit *"Run the CDC prerequisite checks first"* about checks the
  user had just run. The gate now also accepts the durable signal recorded when the load
  started: a Full Load can only have STARTED once the CDC-superset checks passed. A
  report that is present but FAILING still blocks (a live signal), a Full-load-only pass
  still does not excuse the CDC gate, and a session that never checked is still blocked.
  Both CDC lifecycle gates (Deploy infrastructure and Start CDC) are covered.

## v0.1.153

### Fixed

- **"Stop Full Load" could hang forever, and said it was almost done while it did.**
  Observed live: the screen sat on *"Stopping… finishing the current batch."* with no
  progress — the job stayed `RUNNING`, four worker processes idled at 0% CPU, and the
  row count had not moved. It was a deadlock, not a slow shutdown: the progress drain
  stopped consuming, the workers filled the IPC queue and parked inside a blocking
  `queue.put`, and **there they could no longer reach the code that polls the cancel
  event** — so cancellation could never be observed. The parent then waited in
  `as_completed(futures)` with no timeout. Three fixes, each closing one link:
  - Worker progress is sent with `put_nowait` and a full queue is dropped. Progress is
    telemetry — the counters are deltas the next flush re-accrues, and the authoritative
    totals come from the worker's return value — so losing a message costs a slightly
    stale progress bar. Blocking cost liveness.
  - The cleanup sentinel is non-blocking too; on the `finally` path a full queue could
    otherwise wedge the very teardown meant to unwind the job.
  - The parent now waits in slices with a bounded grace period after a cancel. If the
    workers do not wind down in time it stops waiting, tears the pool down, and marks
    the unfinished tables retryable (the load is idempotent) instead of hanging.
- **The stop message no longer overstates what is happening.** "Stopping… finishing the
  current batch" read as a promise the tool could not keep. It now says it is waiting for
  the in-flight batches, and the tooltip explains that an unresponsive worker is torn
  down after a grace period with its tables left retryable.

## v0.1.152

### Fixed

- **"Records per page" on the Full Load progress table now sticks.** The per-table
  progress table is rebuilt on every ~1.5 s poll tick while a load runs, and only the
  *page* was carried across that rebuild — the rows-per-page was hardcoded at 10. So
  raising it was undone by the very next tick: the setting appeared to do nothing, and
  the select snapping back made the table look like it was refreshing itself. The
  poll-surviving holder now carries `rowsPerPage` as well, including Quasar's "All"
  option (`0`), and a shrinking table still clamps the page instead of leaving you on an
  empty one.

## v0.1.151

### Fixed

- **A CDC teardown on customer-supplied subnets no longer strands a billable MSK
  cluster.** The offset-seeder Lambda answers CloudFormation with an HTTPS PUT to S3, so
  the connector security group must still permit 443 when the custom resource is
  deleted. That rule was made **inline** on the security group for exactly this reason
  (an inline rule cannot be deleted while the Lambda's ENI references the SG) — but it
  was gated on the stack owning its own network. On a BYO-subnet deploy the SG fell back
  to the *standalone* `ConnectorHttpsEgress` resource, which CloudFormation deletes in
  parallel with the custom resource. Observed on `mysql-dsql-cdc-stack-0727`: the egress
  rule was gone before the seeder's `Delete` ran, its response timed out three times
  (5 min each), and the stack landed in `DELETE_FAILED` — leaving an **ACTIVE MSK
  Serverless cluster billing**. The inline rule is now created on **both** network
  modes, and the redundant standalone resource is removed so it cannot reintroduce the
  race.

## v0.1.150

### Fixed

- **A half-deleted CDC stack is no longer offered for "Attach", and no longer goes
  silent.** After a teardown that ended in `DELETE_FAILED`, the Data Migration step
  showed an inviting **"Attach to &lt;stack&gt; (DELETE_FAILED)"** button. Attaching to
  such a stack cannot work — its resources are partly gone, so nothing can stream — and
  the button buried the fact that actually mattered: the leftover **Amazon MSK / NAT was
  still billing** with no session tracking it. Discovered stacks are now split by
  status: failed / rolled-back / deleting ones get an **error** notice naming the
  billing risk and telling the user to finish the delete, with **no** attach button;
  only healthy stacks are attachable.
  - The cross-view teardown banner no longer clears itself on a *job* that finished
    while the *stack* is still broken. A `DELETE_FAILED` outcome — or a job record lost
    to an app restart — used to clear the marker and hide the banner entirely. It now
    also consults the last probed stack status, so leftover billable infrastructure
    stays visible and actionable.

- **A view's source DDL is now formatted instead of one endless line.** MySQL's
  `SHOW CREATE VIEW` returns the whole definition on a single line prefixed with server
  bookkeeping (`ALGORITHM=`, `DEFINER=`, `SQL SECURITY`), and it was shown raw — an
  unreadable wall of text, and unusable in the side-by-side diff, where the target side
  *is* pretty-printed so the two could never line up. The source is now re-rendered with
  sqlglot in MySQL dialect and the server metadata is stripped (it has no bearing on the
  conversion, and round-tripping `DEFINER=\`user\`@\`host\`` turned its backticks into
  double quotes — invalid MySQL shown to the user). An unparseable definition is still
  shown verbatim, which is exactly when the operator needs to see it as-is.
- **The object browser is locked while a schema apply runs.** The apply worker is handed
  a fixed object list when it starts, so re-ticking mid-run could not change what it
  writes — it only desynchronized the screen from the target. Worse, "Generate DDL" or
  "Reset all" during a run would swap or discard the DDL the in-flight apply is
  executing. The tree, the bulk buttons, the filter, the source refresh, Generate and
  Reset are all disabled with an explanation while the apply is in progress.

### Changed

- **The object browser's two panels now line up.** "Select all" / "Unselect all" moved
  onto the **Source (MySQL)** header row (beside the refresh) and the primary-key legend
  moved below the tree. Both used to sit above the source tree, pushing it down while
  the target tree started right after its filter — so the side-by-side comparison read
  as visibly misaligned.

- **A failed "drop & replace" now says how to fix it, instead of repeating the
  database's dangerous hint.** Replacing a table that a view still selects from failed
  with the raw driver error — `cannot drop table … because other objects depend on it
  … HINT: Use DROP ... CASCADE`. That hint is the wrong advice here: cascading would
  silently delete a view this tool may not be able to recreate. The apply already
  pre-drops every view **in the selection** before recreating tables, so a blocking
  view simply was not selected (typically created by an earlier apply). The failure now
  names the blocking view and says to select it in the object browser and re-run — the
  pre-pass then drops it first and its own apply unit recreates it — while explicitly
  steering away from `DROP ... CASCADE`. Dependency failures are no longer OCC-retried
  either: a dependency is hard state, not a transient conflict.

### Changed

- **Schema Conversion now separates recommendations from real conversion gaps.**
  Everything was listed under **"Conversion warnings"** with the same amber `MANUAL`
  badge, so throughput advice looked like a defect: a kept `AUTO_INCREMENT` key
  converts perfectly and works — moving to a UUID/random or cached-identity key is a
  *performance* suggestion for DSQL's partitioning, not a problem to fix. It sat right
  next to "foreign key constraints were removed from the DDL", which genuinely dropped
  something. Conversion notes now carry a `kind` (`LOSS` / `RECOMMENDATION`) and the UI
  renders two sections: **Conversion warnings** (something could not be carried over or
  changed meaning — keeps the MANUAL/UNSUPPORTED severity) and **Recommendations**
  (calm info-blue `RECOMMENDED` badge, with a line clarifying the conversion is
  complete). The per-object header counts them separately too, so a table whose only
  note is advice no longer reads as "Review needed · 1 warning".
  - Notes default to `LOSS` — what every note historically meant — so only the
    AUTO_INCREMENT key notes opt into `RECOMMENDATION`. The composite-key note stays a
    `LOSS`: it really does change what the application must key on.
  - The AUTO_INCREMENT messages were reworded to lead with what happened ("the integer
    key was kept and converts cleanly") instead of with "causes hot partitions", which
    described a risk as though it were a failure.
- **The primary-key picker uses AWS-style tiles instead of a segmented control.**
  Keep source PK vs Composite key is a design decision with lasting consequences (a
  composite key changes every query, join and upsert, and DSQL keys are immutable once
  created), so each option now gets a Cloudscape "Tiles" card explaining the trade-off
  — the pattern AWS uses for consequential choices, where a segmented control is for
  switching views. Added `radio_tiles` to `ui/design.py` as the single source of truth
  for that look.
- **The source/target DDL diff now uses the AWS Console code-surface treatment.** Every
  changed line was filled with a solid red or green wash — and because a heterogeneous
  MySQL→DSQL conversion rewrites nearly every line, that painted the whole panel. It
  read as an error report rather than a review surface, and the saturated fill competed
  with the monospace text. The code area is now **neutral** (white surface, quiet
  header) and the change is carried by a narrow **`+` / `−` status gutter** plus a
  barely-there row wash (a `-50` shade at 40% alpha). Color is no longer the only
  signal, so the diff stays legible in a monochrome screenshot and for a colorblind
  reader. Only the side that actually changed is marked, so a rewritten line is one
  before/after pair instead of two loud blocks. The tokens moved into `ui/design.py`
  (`CODE_*` / `DIFF_*`) as the single source of truth.
- **The "Recommendations" explanation is a tooltip, not standing text.** The
  "optional tuning suggestions, not problems to fix" line is now on a help glyph beside
  the heading — the `RECOMMENDED` badge and the heading already carry the message, and
  this block repeats for every object.

## v0.1.149

### Fixed

- **Evaluation no longer opens with a migration type the user never chose.** With the
  Migration plan step retired (v0.1.147), the journey header's migration-type banner
  started rendering on every step — but `migration_type` always answers, because
  full-load-only is its *default*. So the very first step greeted the user with
  "Migration type: Full load only" and its full description, presenting an untouched
  default as a settled decision three steps before the choice is even offered.
  (Under the retired step this could not happen: the choice came first.) The session
  now tracks whether the type was **explicitly chosen**, and the banner appears only
  from that point on — the steps before it show just the progress stepper.
  - Confirming the tile that is already selected now counts as a choice. The type has
    a default, so clicking "Full load only" is how a user confirms it; the selector
    previously bailed out on "no change" and left that user with no banner. The
    sub-step reset stays scoped to a real change, so confirming disturbs nothing.
  - The flag is persisted, so a reconnect keeps the banner for a session that had
    already chosen — and does **not** invent a choice for one that had not. Older
    snapshots restore as "not chosen", which is the safe direction.

## v0.1.148

### Changed

- **A notice reporting a live background operation now shows an animated spinner and
  an "In progress" badge.** The cross-view CDC banner ("Deleting '<stack>' in the
  background (~15–45 min)…") carried only a static info icon, so a 15–45 minute
  teardown looked like an inert message — there was no way to tell it was still
  moving rather than stalled. `render_notice` gains a `busy` flag that swaps the
  static glyph for a tone-colored spinner and pins an **In progress** badge beside the
  header; the running teardown/stop banner, and the CDC-infrastructure deploy notice
  on Data Migration's Prerequisites sub-step, both use it. A **failed** teardown stays
  static and keeps its Retry/Dismiss actions — a spinner there would wrongly imply
  work is still happening.

## v0.1.147

### Changed

- **The workflow is now five steps: the "Migration plan" step is retired.**
  `Connect → Evaluation → Schema Conversion → Data Migration → Validation → Cut
  over`. The step asked one question — "Include CDC?" — at the moment of *minimum*
  information: nothing consumed the answer for three steps, and Evaluation (which
  detects, for example, cascading foreign keys that CDC can never replicate) had not
  run yet. It also duplicated a decision Data Migration already owns: the same CDC
  choice existed there as the three-way migration-type selector, which was never
  locked, so the type was decided twice. Everything the step did now lives where it
  is actionable:
  - the **migration type** is chosen on Data Migration, after the compatibility
    report tells you what you are dealing with;
  - the **CDC infrastructure deploy** is offered on Data Migration's Prerequisites
    sub-step (v0.1.146), which still precedes the Full Load — so the ~15–20 minute
    MSK create overlaps the snapshot instead of being front-loaded before Evaluation.
  - Connect now advances straight to **Evaluation**, and the duplicate "Include CDC?"
    control is gone.
- **The migration-type banner now appears on every step.** The retired step was the
  one screen that had to suppress it (its two-value "Include CDC?" control read as
  conflicting with the three-value banner), so the journey header is finally
  identical everywhere.
- **"Start over" now warns about orphaned CDC infrastructure in more cases.** The
  caution required the migration type to *still* name a CDC mode, which was a hole:
  the type is freely switchable, so someone who deployed MSK and then switched back
  to Full-load-only got no warning and could silently leave a billing cluster behind.
  Entered infrastructure inputs — or a non-default stack name, which a fresh session
  never re-discovers — are now enough on their own.

### Fixed

- **An unreadable persisted session no longer breaks the page.** The SQLite session
  store parsed its payload with no error handling, and both `SessionSnapshot` and
  `WorkflowState` are `extra="forbid"` — so a snapshot written by a newer build (or
  naming a field since removed) raised out of the page build and locked the user out
  of the tool entirely, rather than just losing the restored progress. It now warns
  and starts fresh, matching the S3 store.

### Compatibility

- `WorkflowStep.MIGRATION_PLAN` and `WorkflowState.migration_plan` are **kept** as
  back-compat only (like the older `data_migration` alias). Removing the field would
  make every already-persisted snapshot that names it fail to validate. All 19
  snapshots in the reference session store still load unchanged.
- A session **parked on the retired step** is redirected to Evaluation on restore
  (8 of those 19 were), instead of silently falling back to the Connect screen.
- The presentation decks (`docs/tech-talk-*`, `docs/full-load-cdc-slides-*`) have
  been removed from the repository; `docs/` now holds only the user manual and the
  UI screenshot.
- The README/deployment screenshot is now a **static PNG** (`docs/demo-ui.png`),
  recaptured on the five-step UI. The previous animated GIF baked the retired
  six-step sidebar — and a stale version chip — into its frames.

## v0.1.146

### Added / Changed

- **CDC infrastructure can now be deployed from the Data Migration step, so the
  ~15–20 minute MSK create overlaps the Full Load.** The deploy form previously lived
  only inside the CDC sub-step — which, for a Full Load + CDC migration, is reached
  only *after* the snapshot finishes. So the wait was serialized: the load ran, and
  only then did ~15–20 minutes of provisioning start. It is now offered at the bottom
  of the **Prerequisites** sub-step, which still precedes the Full Load, with copy
  that says explicitly the deploy runs in the background and the snapshot should be
  started now. The deep CDC sub-step form stays available (a session can still arrive
  at CDC with nothing deployed).
  - Prerequisites is the right anchor, not the migration-type tiles: running the
    checks is what pins and locks the confirmed table set, which the connector's
    table list and the topic partition plan both need.
  - The section adapts to the situation: not deployed → the form; deploying → live
    progress and "start your Full Load now"; already deployed → a short "ready,
    nothing to do here"; found under another name → attach instead of paying for a
    second MSK cluster. It renders **nothing** until the account-wide discovery has
    reported, so a fresh-deploy form can never appear before the duplicate-cluster
    guard is populated.
- **The prerequisite checks now record the exact table set they covered.** The picker
  locks as soon as a report exists, so that set *is* the migration scope — but when
  the user never touched the picker it was only implied by the default, leaving the
  stored selection empty. Anything reading it then resolved to "no tables": a CDC
  deploy started before any Full Load watermark exists produced an empty connector
  table list and a uniform topic-partition plan.
- **An in-flight CDC infrastructure deploy is now visible from every screen.** It is
  the one CDC operation the user is meant to walk away from, but the cross-view banner
  covered only stop/delete — so after leaving Data Migration there was no sign it was
  still running, and a user could sit and wait on it. The banner now also reports a
  running deploy and repeats that the Full Load is not blocked.
- **Evaluation's CDC-specific foreign-key finding is now surfaced where CDC is
  chosen.** The assessment already detects foreign keys with automatic `ON
  DELETE`/`ON UPDATE` actions: MySQL applies those to child rows inside InnoDB, so
  they never reach the binary log, CDC cannot see them, and DSQL (no foreign keys)
  cannot re-perform them — the child rows are silently left behind on the target. The
  finding's own guidance begins "Before starting CDC", yet it appeared only in the
  Evaluation report, read *before* the user knew whether CDC was in scope. Selecting a
  CDC migration type now names the affected tables inline.

### Fixed

- **The CDC infrastructure deploy's progress estimate no longer under-reports the
  wait.** The `ensure_bucket` and `upload_plugins` stages carried no estimate even
  though they upload ~43 MiB of connector plugins, so the total ETA — the user's only
  signal during the deploy — was short by roughly a minute on a cold start.

## v0.1.145

### Fixed

- **Adding CDC after a Full-Load-only run no longer skips the CDC prerequisite
  checks.** The prerequisite report is intentionally not persisted, so the run guard
  excuses an absent report once a load has run — that is what lets a reconnected user
  re-run a finished Full Load. But the excuse was not scoped to the mode that
  actually cleared the gate, so on the "start Full-Load-only, add CDC later" path the
  tool inherited the Full Load pass for **CDC** mode: Prerequisites collapsed as
  "done" and the CDC sub-step opened with the **binary-log format never verified**. A
  source on `STATEMENT`/`MIXED` (or without `binlog_row_image=FULL`) can never be
  streamed, so this was only discovered as an undiagnosed connector failure ~26
  minutes into a billable create. The mode that gated the run is now recorded (and
  persisted), and a switch that needs different checks asks for them. Older snapshots
  lack the field and keep the previous lenient behavior, so a reconnect is never
  hard-blocked.
- **The sidebar's Data Migration Run guard now agrees with the on-screen guard.** It
  called the guard without a mode, silently defaulting to Full Load — so for a CDC
  migration type the sidebar Run button appeared enabled while the in-content button
  (correctly gated on the CDC superset) was disabled. The mode is now derived from
  the selected migration type in both places.

### Added

- **Deploy CDC infrastructure and Start CDC now have their own prerequisite gate.**
  Both actions previously relied on the sub-step order (Prerequisites → Full Load →
  CDC) to guarantee the checks had run — an implicit guarantee that only held because
  the migration type was chosen early. Both now explicitly require the CDC-mode
  checks to have run with **`BINLOG_ROW_FORMAT` passing**, and explain what to fix
  (on RDS, a parameter-group change plus a reboot) before any billable
  infrastructure is created. The gate deliberately ignores unrelated required
  failures (e.g. a per-table target-schema check), which the Full Load guard already
  reports — one problem is not surfaced twice.
- **Start CDC warns when the snapshot's binary log has already been purged.** The
  watermark is captured at Full Load **start**, so a long load plus the ~15–20 minute
  infrastructure create plus the connector create all elapse before Debezium reads
  it. If the source purged that log in the meantime the gapless hand-off is
  impossible, and the only correct recovery is a fresh snapshot. A single read-only
  `SHOW BINARY LOGS` now runs before the connectors are created and names the missing
  log, the oldest one still retained, and the retention command to raise it — instead
  of failing ~26 minutes later with an undiagnosable `CREATE_FAILED` (MySQL error
  1236). It is a **warning, not a block** (starting with a known gap can be
  deliberate), and it stays silent whenever the answer is unknown — no watermark, a
  manual start position, or the statement/privilege unavailable.

## v0.1.144

### Fixed

- **Deploying CDC infrastructure no longer masquerades as a live CDC stream.** The
  ~15–20 minute infrastructure deploy (`create_stack`: MSK Serverless, networking,
  plugins, IAM) creates **no connectors** — the template gates both on
  `HasBootstrapServers`, which the infra pass leaves blank — so nothing is streaming
  while it runs. But the "is CDC streaming?" predicate counted **any** in-flight CDC
  lifecycle job, including the infra deploy, so starting a deploy (e.g. from the
  Migration plan step) and then opening Data Migration made the tool behave as if a
  pipeline were live:
  - **Data Migration was promoted to `Success`**, unlocking **Validation with zero
    rows loaded** — and because that promotion never downgrades, the bogus status was
    **persisted** and survived a restart.
  - **Start Full Load was disabled** with a misleading "CDC is streaming — stop CDC
    first" tooltip, and the table picker was frozen.
  - A **"Drop & reload" re-run silently became an append**: the DROP is suppressed
    while a live sink is writing the target, so the reload skipped over stale rows
    ("0 new + N already there") instead of refreshing them.
  - **Applying schema was blocked** on the Schema Conversion step — a dead end, since
    Data Migration (the only place CDC can be stopped) is prerequisite-locked behind
    it.
  A `kind="infra"` job is now excluded: only a **connector-level** operation
  (Start / Stop / Delete CDC) or an actually-streaming pipeline counts. Detected
  connectors and stack phase `running` still win, so every CDC-live safety gate is
  unchanged.
- **The prerequisite "Check" button is no longer disabled for the whole
  infrastructure deploy.** A second, independent path disabled it: the panel treated
  every CloudFormation `*_IN_PROGRESS` status as a live migration operation, so
  `CREATE_IN_PROGRESS` kept the read-only checks unavailable for the full ~15–20
  minutes — exactly when the user should be running them. Only the first deploy uses
  `create_stack` (Start / Stop CDC go through `update_stack` →
  `UPDATE_IN_PROGRESS`), so `CREATE_IN_PROGRESS` is now recognized as
  "infrastructure provisioning, nothing streaming yet" and leaves the checks
  available. `UPDATE_IN_PROGRESS` / `DELETE_IN_PROGRESS` still count as live
  operations.

Together these let the ~15–20 minute MSK create **overlap** the Full Load instead of
serializing after it — the deploy runs in the background while the snapshot loads.

## v0.1.143

### Fixed

- **A failing post-load index no longer fails a fully-loaded table.** Secondary
  indexes are created by `CREATE INDEX ASYNC` **after** every row is written, and the
  error propagated out of the import — so a table whose data was **completely loaded**
  was marked `FAILED`, which also **blocked the Validation gate** on a table with
  nothing missing. Re-running did not help: the usual cause (DSQL's 24-index limit) is
  not transient, so the run hit the same error every time.
  - An index failure is now **isolated**: the data load reports success (`failures=0`,
    every row present) and the failure is returned separately as
    `BatchedImportResult.index_failures`.
  - **One bad index no longer stops the rest.** Each DDL is attempted independently,
    so the remaining indexes are still created (previously the first failure aborted
    the loop).
  - Reported as its own **info**-toned block in the Full Load result — *"Indexes not
    created (N) — the data loaded completely"* — kept apart from failures and from
    quarantined rows, since no data is missing. The error log names which index and
    why, and says the load does not need re-running.
  - Applies to the multiprocess load path as well, so a run does not behave
    differently depending on the worker mode.

## v0.1.142

### Added

- **Evaluation now checks Aurora DSQL's per-table index limit** (`TOO_MANY_INDEXES`).
  DSQL allows **24 indexes per table** (MySQL allows 64), and the **primary-key index
  counts toward that budget** — verified against a live cluster, where the 24th
  `CREATE INDEX` on a table that already had a PK failed and `pg_indexes` then showed
  24 rows including the PK. A migrated table can therefore carry at most **23
  secondary indexes**, which is what the source's reflected index list is compared
  against.
  - Caught at planning time because the failure otherwise surfaces at the worst
    moment: secondary indexes are created by **post-load** `CREATE INDEX ASYNC`, so
    the limit is hit only **after Full Load has written every row** — turning a
    multi-hour load into a failed table that a re-run cannot fix (the limit is not
    transient).
  - Classified **MANUAL**: unused/redundant indexes are common, so the fix is usually
    to drop a few (the finding points at `sys.schema_unused_indexes`) rather than
    redesign. The message names both counts, the exact error (`54000`), and when it
    would have fired.

- **Evaluation now flags foreign keys whose cascade CDC cannot replicate**
  (`FK_CASCADE_CDC_GAP`). MySQL performs `ON DELETE/UPDATE CASCADE` (and `SET NULL` /
  `SET DEFAULT`) **inside the InnoDB engine**, so the resulting child-row changes are
  never written to the binary log — the same reason cascaded actions don't fire
  triggers. Debezium reads the binary log, so a CDC stream cannot see them, and
  Aurora DSQL has no foreign keys to re-perform the cascade: the child rows are left
  behind on the target **with no error and no warning**. (MySQL bug #32506, closed as
  documented behavior — it affects every binlog-based CDC tool, not just this one.)
  - The referential actions are now captured during introspection
    (`ForeignKeyDef.on_delete` / `on_update`), read from information the source
    reflection already returns — no extra source query.
  - Classified **MANUAL** (not UNSUPPORTED): the table migrates fine, but the cascade
    has to move into the application — which DSQL requires anyway, since it has no
    foreign keys. The finding names the concrete action, explains why CDC misses it,
    and points at the interim safety net (Validation's orphan-record check plus
    quiescing source writes before the final comparison).
  - `RESTRICT` / `NO ACTION` are **not** flagged: they only reject the parent change,
    so they never produce an unlogged child write.

## v0.1.141

### Fixed

- **CDC lifecycle actions now record their OUTCOME in the activity log, not just
  "started".** Deploy infrastructure / Start CDC / Stop CDC / Delete infrastructure
  each take minutes to tens of minutes, but only the submit was logged — nothing
  recorded whether the action succeeded, failed, or how long it took. The audit trail
  therefore could not answer the question that matters most at cut-over: *did the
  Stop actually succeed, and when?* Connector-state transitions were the only proxy,
  and those are written by the UI poller, so an action completing while the operator
  was on another screen was never logged at all (recovering a Start CDC duration from
  the log required guessing from later poll lines).
  - Each lifecycle job body is now wrapped so the outcome is logged **from the job
    thread**, independent of what the UI is showing: `success` with the elapsed time,
    `failure` with the elapsed time plus the error, or `info` for a cooperative
    cancel (`run_cdc_*` returns normally when cancelled, so the job handle — not an
    exception — distinguishes "stopped early" from "finished").
  - A failure is still re-raised, so the JobManager keeps marking the job `FAILED`.
  - The `core` deployer is untouched: `core` deliberately has no activity-log
    dependency, so the logging stays in the UI layer (mirroring Full Load).
  - Known gap: if the process dies mid-action, that job keeps only its `started`
    line (the JobManager reconciles it to `FAILED` on restart without logging an
    activity event).

## v0.1.140

### Fixed

- **A failed source read no longer holds its MySQL connection open while the retry
  waits.** The source row streams are generators that dispose their engine in their
  own `finally`, so an abandoned one keeps its connection until it is closed or
  garbage-collected — and the raising frame keeps it referenced. The v0.1.139 retry
  therefore waited out the whole failover backoff (up to 60s) with the dead
  connection still open, then opened another one to re-read. At 16 tables × 8 shards
  that **doubles the source connection count exactly when a just-promoted Aurora
  writer is most fragile**, risking `1040 Too many connections` — which would have
  failed the table outright.
  - `migrate_table` now closes the row streams it created when a load raises, so the
    connection is released as the exception leaves.
  - The retry's backoff wait moved OUT of the `except` block, so the traceback (and
    with it the failed attempt's frames and generator) is dropped before waiting.
  - Verified end-to-end: the connection is now disposed *before* the wait starts.

### Added

- **`Too many connections` on the source is now retried, with its own advice.** MySQL
  1040 / 1203 are self-inflicted and self-clearing (a failover makes every reader
  reconnect at once; slots drain as readers finish), so they are classified as
  transient. The operator hint differs from the failover one, because waiting is not
  the fix: it names `FULL_LOAD_TABLE_PARALLELISM` / `FULL_LOAD_READER_SHARDS` and the
  source's `max_connections`.
- **A clamped reader-shard count now says so.** Concurrent source readers are capped
  at 32 (`table_parallelism × reader_shards`); when that ceiling reduces the
  configured shard count, the log states the old and new values and why, instead of
  silently loading with fewer readers than requested — which looked like the setting
  had no effect.

## v0.1.139

### Added

- **Full Load now survives a source Aurora failover.** A writer promotion (patching,
  an instance replacement, an AZ event) closes every open MySQL connection, so a
  multi-hour load would meet one — and previously the table in flight simply failed
  and waited for someone to press Re-run. Such a table is now **re-read
  automatically** (3 attempts by default, 15s → 30s → 60s backoff to let DNS
  re-point at the promoted writer).
  - The retry deliberately **re-reads the table from a fresh consistent snapshot**
    rather than resuming the dead read at its last primary key. Resuming would splice
    two different MySQL snapshots into one table, leaving it consistent as of no
    single point in time — and the gapless Full Load → CDC handoff depends on each
    table being consistent as of the run's watermark. Already-written rows are skipped
    by the idempotent load, so a retry costs re-read I/O but never duplicates rows.
    (Reader sharding shrinks even that cost: each shard already holds its own
    snapshot, so only the affected shard re-reads.)
  - Only **connection-level** failures retry (MySQL 2013/2006/2003/2002/2055/1053/
    1077/1079/1927 and socket timeouts). A data or schema error fails immediately, as
    before — retrying it would only add delay before the same failure.
  - Applies to the multiprocess load path (the default at scale) as well as the
    single-process one, so a run does not behave differently per worker mode. A retry
    correctly stops treating the target as freshly-emptied, so the re-read cannot
    collide with rows its own failed attempt already wrote.
  - A user **Stop** is honored during the backoff wait, not after it.
  - Tunable: `DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_ATTEMPTS` (1 = off, the previous
    behavior) and `DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_BACKOFF_SECONDS`.

### Changed

- **A dropped source connection now explains itself.** When the retries are
  exhausted, the per-table error no longer reads as a bare
  `OperationalError: (2013, 'Lost connection to MySQL server during query')`. It now
  states that this is usually an Aurora failover, that nothing on the source was
  changed (the load only reads it), and that re-running is safe because the load is
  idempotent and resumes by primary key — filling only what is missing.

## v0.1.138

### Fixed

- **A fully-loaded Full Load table can no longer report as incomplete because the
  source ESTIMATE overcounted.** Per-table `Progress` and the completeness verdict
  divided by / compared against the watermark's scan-free `information_schema` count.
  That estimate comes from InnoDB index sampling and errs in *both* directions, so
  whenever it overcounted, a table the loader had streamed to exhaustion showed e.g.
  **"91%" and counted as mismatched** — implying rows were lost when none were.
  - A `DONE` table is now **100%** by definition: the export streams the table by PK
    keyset until exhausted, so finishing *is* the completeness evidence — it does not
    depend on the estimate agreeing.
  - `complete` reports `True` for a finished table unless the shortfall exceeds the
    estimate's sampling tolerance, so a genuinely truncated load is still flagged
    (and a few-percent discrepancy no longer is).
  - Loading **more** rows than the estimate predicted (the common undercount case) is
    now stated as normal in the Rows tooltip, with the percentage, instead of being
    silently hidden by the 100% cap.

### Changed

- **The Full Load table is explicit that its source figure is approximate.** The
  column header now reads **Rows (target / source est.)** with a new ⓘ tooltip
  explaining the sampling error, why a target exceeding the source is normal, and
  that a finished table is 100% because the loader exhausted it — not because the two
  numbers match. Validation (step 4) remains the exact comparison.

- **The CDC status table no longer flags healthy tables as "target ahead".** Its
  Source rows figure is a scan-free `information_schema` **estimate** (so a large
  production source is never `COUNT(*)`-scanned), but the consistency verdict was
  subtracting the exact target `COUNT(*)` from it and treating any difference as an
  anomaly. InnoDB derives that estimate from index sampling and routinely
  *undercounts* by several percent, so a perfectly healthy target legitimately
  exceeds it — on a live 11-table schema **8 tables showed an amber "target ahead"
  badge** with zero quarantined rows and every stream caught up.
  - The `"target ahead"` verdict is **removed**. A target exceeding an estimated
    source count is the normal case, not an anomaly.
  - Verdicts now lean on the signals that are actually exact and cheap: the DLQ, the
    time-based `ReplicationLagMs`, and the `MAX(pk)` leading edge. A shortfall
    against an *estimate* is only escalated to "rows missing" when it exceeds the
    sampling tolerance, so genuine data loss is still reported while statistics
    noise is not.
  - Equality claims are gated on an **exact** source count (`counts_comparable`);
    `in_sync` now returns "not determinable" rather than a false negative when the
    source figure is an estimate.

### Changed

- **The CDC table is explicit that Source rows is approximate.** The column header
  now reads **Source rows (est.)** with a new ⓘ tooltip explaining the sampling error
  and pointing to Validation (step 4) for the exact comparison; the per-cell `(est.)`
  suffix is gone (it now marks only the unusual *exact* case). The Consistency
  tooltip and the "How to read this table" legend state that green means "nothing
  looks wrong", not a proven exact match.

## v0.1.137

### Added

- **Fast-sweep "verified by row count only" tables can now be deep-checked in place.**
  The footnote that lists tables the fast sweep passed on row count alone previously
  only advised turning Fast sweep off and re-running everything. It now offers
  **Deep-check N count-only table(s)**, which re-compares just those tables with the
  checksum / record reconciliation the run skipped and merges the results into the
  existing report — the same per-table mechanism v0.1.136 added for failing tables.
  This is the one *passing* case where re-validating is genuinely useful, since those
  tables were never proven row-for-row identical.
  - The action is withheld when it would be a no-op: in a `ROW_COUNT`-mode report
    with no reconciliation there is no deeper check to run, so the honest "turn off
    Fast sweep and re-run" advice stands instead of a button that repeats the
    identical count comparison.
  - Otherwise-passing tables still get no re-check button; the affordance appears
    only where it adds a check (failing tables, or count-only fast-sweep tables).

## v0.1.136

### Added

- **Re-check an individual table in Validation instead of re-running everything.**
  When a table fails on row count or checksum, each entry under "Tables needing
  attention" now has a **Re-check** action (plus **Re-check all N tables** for the
  whole failing set). It re-compares only those tables and **merges** the fresh
  result into the existing report, so every other table's verdict — and the overall
  cut-over go/no-go — is kept and updates on its own: fix the last failing table and
  the verdict flips to "Ready for cut-over" without an hour-long full re-run.
  - The re-check reproduces the **original run's options** (comparison mode,
    reconciliation, orphan check) read back from the report itself, so the merged
    report stays internally consistent — and a report restored after a reconnect is
    re-checkable too. The fast sweep is forced **off** for a re-check: the table is
    already known to differ, so its checksum/reconciliation is exactly what should run.
  - The report states the mixed as-of plainly: **"N table(s) re-checked at &lt;time&gt; —
    newer than the rest of this run"**, listing the tables, since the verdict now
    covers two vintages. The disclosure survives a reconnect.
  - A re-check runs on top of the completed step (the step stays **Done**, the report
    stays on screen) with an inline "Re-checking…" state on the affected rows. It
    shares the single validation job slot, so "Re-run validation" is disabled while a
    re-check runs and vice versa — a full re-run can never orphan a re-check or clear
    the report it is about to merge into.
  - A re-check that cannot start (e.g. the short-lived DSQL target token expired since
    the report was produced) reports as its own **"Could not re-check those tables"**
    notice and leaves the existing report untouched — never as "Validation failed".

## v0.1.135

### Fixed

- **Validation no longer false-reports "data differs" for JSON columns after CDC.**
  MySQL `JSON` maps to a Postgres `json` column and the checksum compared raw text:
  MySQL renders a spaced canonical form (`{"k": "v"}`) while a CDC-written row holds
  Debezium's compact serialization (`{"k":"v"}`) — logically-equal data, different
  text, so CDC-touched rows with JSON failed the checksum (Full-Load rows matched).
  JSON is now excluded from the checksum (like FLOAT/DOUBLE); row counts and every
  other column still validate. This was the cause of spurious `customers` / `products`
  / `suppliers` checksum failures.

### Changed (checksum cross-engine hardening)

- **Source MySQL sessions are pinned to UTC** (`SET time_zone='+00:00'` on every
  source engine: connection test, introspection, validation, Full Load stream). MySQL
  `TIMESTAMP` is stored UTC but read in the session's zone; without this a non-UTC
  server/client zone would make `TIMESTAMP` columns drift versus the target's UTC
  rendering in the checksum. (`DATETIME` is a wall-clock and was unaffected.)
- **Validation skips migration-excluded columns** (e.g. the CDC oversized-LOB
  exclusion): a column that was never written to the target is dropped from the
  checksum instead of always "differing" (PK columns are never dropped).

## v0.1.134

### Changed

- **While CDC is streaming, actions that can't apply are now visibly disabled (greyed),
  not just tooltip-warned:**
  - **Start / Re-run Full Load** is now **disabled** during live CDC (it previously
    stayed clickable with only a warning) — running it would collide with the stream.
    The tooltip/hint say to Stop CDC first to re-enable it.
  - **CDC start point** was already read-only when locked but didn't *look* locked —
    the radio choice and the manual GTID/binlog inputs are now clearly **greyed
    (muted + not-allowed cursor)** to match the "Locked" badge.

## v0.1.133

### Changed

- **Inserts / Updates / Deletes cells are now just the coloured count** — the leading
  glyphs (＋ / ✎ / − ) are removed; the column header + green/blue/red colour already
  identify the op. Their header ⓘ tooltips are trimmed to a single plain sentence.

## v0.1.132

### Changed

- **Per-table CDC monitor now has separate Inserts / Updates / Deletes columns**
  (DMS-style), replacing the single combined "Changes since Full Load" cell. Each is
  a **cumulative running total** of what CDC has applied since it started streaming,
  colour-coded (green inserts / blue updates / red deletes).

### Fixed

- **I/U/D counts no longer flicker ("appears then disappears").** The applied-ops read
  is best-effort, so a flaky/empty poll (CloudWatch throttle/timeout, or tables
  momentarily empty) used to overwrite the stored counts with an empty map and blank
  the columns. The counts are cumulative (monotonic), so the poll now **merges** a
  non-empty read into the last-known values and **never wipes** on an empty read —
  the counters stay put and only increase.
- **The per-table header ⓘ tooltips (Stream lag, Consistency, …) no longer close mid-
  hover.** The table used to fully re-render every ~5s poll, tearing down the tooltip.
  The table element + its header tooltips are now built **once** and only the row data
  is swapped **in place** each poll, so a tooltip stays open while you read it.
- **Clearer Stream lag / Consistency explanations** in both the header tooltips and the
  legend (plain-language wording instead of the terse metric definitions).

## v0.1.131

### Fixed

- **Stream lag panel no longer disappears after a session restore of a drained
  pipeline.** The live lag trend is an in-memory rolling buffer that is not persisted,
  so a reconnect re-seeds it from CloudWatch `ReplicationLagMs` — but that metric is
  event-driven, so once the source is quiesced (caught up) there are no recent
  datapoints to seed from, and the chart (which needs ≥2 points) hid the entire panel:
  the operator saw no stream-lag signal at all after reconnecting. The panel now shows
  a **"Caught up — no replication lag in the recent window"** line whenever CDC is live
  but there is no trend to plot, so the metric is always present; it only fully hides
  before streaming starts.

## v0.1.130

### Changed

- **Decluttered the Validation screen text.** The 5-line intro is trimmed to a single
  sentence, and the three status notices (No export watermark / CDC still streaming /
  Comparison in progress) keep their headers but have their bodies cut to the one
  actionable line each — so a combined state (no watermark + CDC active + running) no
  longer stacks into a wall of text. The notices stay (they carry real conditional
  state), just concise.

## v0.1.129

### Changed

- **Change flow reads "idle" once the pipeline drains, absorbing the source
  connector's heartbeat floor.** The source (Debezium) connector never fully goes
  silent — `heartbeat.interval.ms=300000` emits a heartbeat every 5 min, so
  `SourceRecordPollRate` idles at a small floor (~0.03/s on the CloudWatch average)
  rather than 0. The idle threshold was `0.01/s`, so that heartbeat residual kept the
  change-flow line showing "streaming" even after the source was quiesced. Raised the
  threshold to `0.1/s` — above the heartbeat floor, far below any real change traffic
  (typically ≥1/s). The rule still requires BOTH the source-poll AND sink-send rates
  below the threshold, so a stalled sink (source still producing, sink not sending) is
  never mislabelled idle — it correctly stays "streaming".

## v0.1.128

### Fixed

- **Stream lag no longer freezes at the last value after the pipeline drains.** The
  `ReplicationLagMs` metric is event-driven (the sink emits a datapoint only when it
  applies a change), so once the source is quiesced for cut-over the pipeline stops
  emitting — but the reader kept returning the last datapoint still inside its 15-min
  window as the "current" lag, so the Stream lag chart/column sat flat at e.g. 1068 ms
  for up to ~15 minutes even though the source-poll / sink-send rates had correctly
  dropped to idle. The reader now treats a most-recent datapoint older than a freshness
  cutoff (~3 min) as absent, so a drained pipeline reads as **caught up** and the chart
  drops to 0 shortly after the source goes quiet. Reader-side fix (no sink redeploy).

### Changed

- **Decluttered the Data Migration / CDC screens: verbose standing explanations moved
  to hover ⓘ tooltips (or dropped when redundant).** The always-on help paragraphs
  read as noise once the screen is familiar, so the guidance now lives a hover away
  and the views are quieter:
  - **Stream lag** chart caption → an ⓘ next to the title (the title + `lag (ms)` axis
    carry the basics).
  - **Tables to migrate** — the "why only tables (not views/triggers/routines)"
    paragraph → ⓘ on the title; the "Locked — re-run prerequisite checks…" line →
    folded into the lock-icon tooltip; the pre-selection blurb trimmed to
    `Pre-selected: N table(s) already on the target — untick any to skip.`
  - **CDC start point** — the "where streaming begins / Automatic is gapless"
    paragraph → ⓘ on the title; the "CDC has started — locked…" line → folded into
    the **Locked** badge tooltip.
  - **Stop CDC** — the standing "connectors are streaming… Stop removes only the
    connectors…" paragraph removed (the live status shows streaming; the impact is
    already spelled out in the Stop confirmation dialog), with a short reassurance
    tooltip on the button.
  - **Change flow** — the "whether changes are still streaming / watch it drop to
    idle for cutover" paragraph and the "CloudWatch, ~last few min" provenance note
    → folded into one ⓘ on the "Change flow" header, leaving just the state line +
    the source/sink rate gauges.

## v0.1.127

### Changed

- **Per-table CDC monitor now shows a DMS-style change breakdown (I/U/D).** The
  "Net rows since Full Load" column is replaced by **"Changes since Full Load"** —
  three live counters per table: **inserts** (green `add`), **updates** (blue
  `edit`), and **deletes** (red `remove`). This makes UPDATE traffic visible for the
  first time: the old net-rows figure summed inserts − deletes and skipped updates
  entirely, so an update-heavy table looked idle. Still scan-free (no `COUNT(*)`):
  the DSQL sink now emits three CloudWatch metrics — `InsertsApplied` /
  `UpdatesApplied` / `DeletesApplied` (namespace `MysqlDsqlMigrator/CDC`, dimensions
  `Stack` + `Table`) — in place of the single `NetRowsApplied`, and the control plane
  sums each over the window. Net rows stays derivable (inserts − deletes) where still
  needed. Requires the rebuilt sink plugin (`PLUGIN_VERSION` v21 → v22), so a
  **Delete + Deploy** of the CDC infra is needed to pick it up.

## v0.1.126

### Changed

- **CDC Live-status polish (readability + less noise):**
  - **Change flow** rate gauges no longer overflow the Pipeline health card (fixed-
    width bars + inner padding), and the rates are labelled **`rec/s`** (change-event
    records per second — `SourceRecordPollRate` / `SinkRecordSendRate`) instead of a
    bare `/s`.
  - **Connectors** show a colour-coded state **badge** (green "Running", etc.) again
    for at-a-glance health, kept on the compact one-line-per-connector layout.
  - **"CDC behavior & limits"** reference section is **collapsed by default** — it is
    info-only and long, so it no longer adds noise on every visit.
  - The **"Runs on the … cdc-stack"** orientation banner shows **only before the
    cdc-stack is deployed**; once it exists (or the phase is still resolving) it is
    hidden, so it doesn't repeat on every visit or flash on a reconnect.

## v0.1.125

### Fixed

- **The CDC per-table status view (and its live metrics) no longer comes up empty
  when you reconnect to an already-running CDC pipeline.** The per-table set — which
  also scopes the scan-free CDC metrics (net rows, stream lag, and the live lag
  chart) — was derived *only* from a Full Load job's chunks, so a session with no
  Full Load job (reconnected to a running pipeline, or a CDC-only run) showed an
  empty table and no lag/chart even while the pipeline was actively streaming. It now
  falls back to the tables reconciled from the live stack's config.

## v0.1.124

### Changed

- **The Stream lag chart is now a live, in-place time series** (previously it was
  redrawn from scratch on every 5s poll, which flickered). The chart element persists
  and updates in place, so the line extends continuously like a CloudWatch graph. X is
  a **time** axis; Y is lag in **milliseconds**. Its data is a hybrid rolling series —
  seeded from CloudWatch's 1-minute history (so it survives a page reload) then
  extended each ~5s poll with the current worst-across-tables lag (caught-up shown as
  0), bounded to ~15 min. It moved to its own persistent "Stream lag" panel at the top
  of Live status.
- **Change flow (source poll / sink send) is now visual** — two labelled bar gauges on
  a shared scale instead of a plain text line, so you can see at a glance whether the
  sink is keeping up with the source (matched bars) or falling behind.
- **Status badges are unified across the Full Load and CDC statistics tables** — both
  now use the same outline chip with title-case labels. (The Full Load "Status" badge
  was a solid, uppercase chip; it now matches the CDC table's outline style and the
  design system's status-chip convention.)
- **The Live-status "Connectors" list is now minimal** — one compact line per
  connector (status icon + role name + a muted detail; the raw connector id moved to
  a hover tooltip), replacing the previous two-line id + outline-badge treatment.

## v0.1.123

### Added

- **Stream lag over time — a trend line chart in the CDC "Pipeline health" card.**
  The per-table "Stream lag" column shows the *current* lag, but a snapshot can't
  tell you whether the stream is catching up or falling behind — which is exactly
  the cut-over question. The chart plots the **worst end-to-end lag across tables per
  1-minute bucket over the trailing ~15 min** (seconds behind, from the sink's
  `ReplicationLagMs` metric): flat near zero means caught up and safe to cut over; a
  rising line means the pipeline is falling behind. It reuses CloudWatch datapoints
  the per-table read already fetched (no extra state, survives a page reload) and the
  in-app ECharts component (no new dependency). Resolution is ~1 minute (CloudWatch
  Period), so it's a trend, not a per-second readout.

## v0.1.122

### Fixed

- **A failed CloudFormation delete no longer silently strands the CDC
  infrastructure.** The in-VPC seeder Lambda's CloudFormation response now
  **retries** its response PUT (bounded, ~4 attempts) instead of giving up after
  one. A single failed PUT during teardown previously left CloudFormation with no
  response, so it waited its own ~1h custom-resource timeout and the whole cdc-stack
  landed in `DELETE_FAILED` — leaving MSK/NAT billing. Retrying rides out a transient
  S3-gateway egress hiccup while ENIs/routes settle. (Takes effect on freshly
  deployed CDC infrastructure; `PLUGIN_VERSION` bumped to v21.)

### Added

- **The teardown banner now recovers from a `DELETE_FAILED`.** When a CDC teardown
  ends in CloudFormation `DELETE_FAILED`, the persistent banner switches from
  "in progress" to an actionable **"CDC teardown failed — action needed"** state
  (error styling) with a one-click **Retry cleanup** — which re-runs the delete,
  retaining the stuck resource so the rest (MSK/NAT) is removed — and a **Dismiss**.
  The retry works even after Start over has reset the session: the region / deploy
  role / profile it needs are saved with the durable teardown marker.

## v0.1.121

### Fixed

- **A CDC infrastructure teardown now stays visible until it finishes.** When you
  Start over and choose "Delete all CDC infrastructure" (or "Remove connectors,
  keep infrastructure"), the teardown runs in the background while the session
  resets to a fresh Connect screen. Previously nothing indicated it was still
  running, so you couldn't tell whether MSK/NAT were still billing or the
  infrastructure was already gone. A persistent banner now shows on **every**
  screen (Connect included) — "CDC infrastructure teardown in progress…" — and
  clears itself automatically the moment the teardown completes. It also covers a
  teardown started from the CDC step's Delete/Stop buttons, so navigating to
  another step no longer hides it.
- **Start over can no longer race an in-flight CDC teardown.** Resetting was
  already blocked while a stop/delete ran, but a brief window right after
  Start over → delete — before CloudFormation flipped the stack to
  `DELETE_IN_PROGRESS` — could let a second reset slip through and fire a duplicate
  teardown. A durable teardown marker that survives the session reset now closes
  that window.

### Changed

- Start over now **warns** (instead of silently proceeding) when a CDC
  infrastructure deploy or Start CDC job is still running. The reset is still
  allowed — that work is re-discoverable and blocking it would trap a user escaping
  a stuck run — but you're told it keeps running in the background.

## v0.1.120

### Changed

- **CDC Start now creates the source and sink connectors in ONE parallel pass**,
  roughly halving connector-creation wall time. Previously Start ran a serial
  two-pass update — create the source connector, wait for it to reach RUNNING (so
  Debezium auto-created the per-table topics), then create the sink — because a sink
  that starts before its topics exist hits an empty-partition-assignment race. The
  cdc-stack's start-prep custom resource (the seeder Lambda, generalized) now
  **pre-creates the per-table sink topics up front** on every start — with the
  deterministic `<prefix>.<db>.<table>` names and partition count the tool already
  computes — so both connectors depend only on the pre-created topics (not on each
  other) and deploy concurrently. The seeder still seeds the connect-offsets record
  only on a gapless Full-Load handoff (watermark present); topic pre-creation is
  unconditional so CDC-only starts benefit too. Start progress collapses from six
  source-then-sink steps to a single "Waiting for connectors (source + sink)" step;
  per-connector state remains visible in the live connector chips.

## v0.1.119

### Fixed

- **A sharded single large table now loads successfully instead of being marked
  FAILED.** The PK-range shard worker built its result with
  `rows_skipped=result.rows_skipped`, but `BatchedImportResult` has no such
  attribute (it exposes `conflicts`). Every shard raised `AttributeError` at its
  return, was caught, and reported `FAILED` with `rows_loaded=0` — so a big single
  table (which the engine splits into one shard per core) was marked FAILED even
  though all its rows had loaded. Only the sharded path was affected; an unsharded
  table maps `rows_skipped = result.conflicts` correctly, which is why multi-table
  loads (one worker per table, unsharded) were unaffected. The shard worker now maps
  `rows_skipped` from `conflicts` too.
- **A sharded table's failure now records every failed shard's status/rows/message
  to the error log**, not only shards that carried a message — so "one or more
  shards failed" is always diagnosable (previously a shard that failed without a
  message left no cause).

## v0.1.118

### Fixed

- **The `measure_performance` harness now dumps the per-table/-shard/-batch error
  records on a failed run.** A sharded table marks itself `FAILED` when any shard
  fails, but the shard's actual reason is written only to the error log; the perf
  run printed "one or more shards failed" with no cause. It now prints each
  `DATA ERRORS` entry (table/chunk, code, message) alongside the `FAILURE REASON`,
  so a failed run — including a late single-shard failure on a large single-table
  load — is diagnosable from its logs alone.

## v0.1.117

### Fixed

- **DSQL's 10-schema-per-cluster limit now surfaces as an actionable error.** When
  the target cluster is already at its hard cap of 10 schemas, a `CREATE SCHEMA` for
  the migration's schema fails with `program_limit_exceeded` (SQLSTATE 54000,
  "more than 10 schemas not allowed") — even with `IF NOT EXISTS`, because DSQL
  checks the limit before the existence check. This is a hard limit (retrying never
  clears it), so it is translated immediately into a clear message telling the user
  to free a schema (`DROP SCHEMA ... CASCADE`) or use another cluster, instead of an
  opaque driver error. It is deliberately not routed through the OCC/transient retry.
- **The `measure_performance` harness now prints the job's failure reason.** A
  failure that propagated out of `run_full_load` (e.g. the pre-pass schema/DDL error
  above, before any table worker ran) was stored only as the JobManager's captured
  exception; the run printed `status=FAILED` with every table `PENDING` and no
  reason. It now logs `FAILURE REASON: <exception>` so a failed perf run is
  diagnosable from its logs alone.

## v0.1.116

### Fixed

- **Every replace table is now DROP+recreated once, serially, before the parallel
  data load starts** — closing a startup DDL storm at maximum parallelism. Each
  table worker used to recreate its own target inside its process, so at high
  table-parallelism all workers issued `CREATE SCHEMA` / `DROP` / `CREATE` against
  the shared schema catalog at once. DSQL runs one DDL per transaction under
  optimistic concurrency, so those concurrent catalog writes conflict with OC001
  (`SQLSTATE 40001`, "schema has been updated by another transaction") and could
  exhaust the DDL retry budget, failing a table before a single row loaded. The
  DROP+recreate (metadata-only) now runs in the existing pre-pass for **all**
  replace tables, not just sharded ones; workers load into the already-empty target
  without re-running the DDL (they derive the same post-load `CREATE INDEX ASYNC`
  DDLs from the applied conversion). This makes a max-parallelism Full Load start
  deterministically instead of racing the catalog.

## v0.1.115

### Fixed

- **The per-table DROP+recreate connection is now retried on a transient connect
  failure**, closing the last gap that could fail a table during a connection
  storm. In a max-parallelism Full Load (table-parallelism 16, 20 tables), the
  four queued tables start only when the first sixteen finish — which they do
  nearly together, so all four open fresh DSQL connections at once and trip
  DSQL's ~100 new-connections/second limit. `recreate_table` (and the other DDL
  connect paths in `schema_applier`) opened that connection **outside** any retry,
  so the resulting `ConnectionTimeout: connection timeout expired` failed the
  whole table with **0 rows loaded, before a single batch ran** (no OCC retry, no
  give-up log — the failure was outside the batch loop the earlier fixes hardened).
  The connection open is now wrapped in the same transient-connection retry the
  batched loader's pool leases already use, so the connect rides out the storm.
- The transient-connection classifier moved to `core/target_connection.py`
  (`is_transient_connection_error`) so **every** DSQL connect/execute path shares
  one definition — the batched loader's pool leases and the DDL connects alike.
  `batched_import` keeps a back-compat alias.

## v0.1.114

### Changed

- **The OCC/connection retry loop now logs its retries and give-ups**, so a
  batch failure is diagnosable directly instead of inferred from timing.
  `with_occ_retry` was silent; it now logs each retry at DEBUG (attempt N/max, the
  error type + SQLSTATE, and the backoff delay) and, when the budget is exhausted,
  a WARNING with the **attempt count, total elapsed time, and the last error +
  SQLSTATE**. That WARNING is the direct evidence needed to tell apart *"the retry
  budget was too small"* from *"the transient storm lasted longer than the budget"*
  from *"the error wasn't retryable"* — e.g. `occ-retry gave up after 30 attempts
  over 131.4s; last=ConnectionTimeout sqlstate=None`. Purely additive logging; no
  behavior change to the retry itself.

## v0.1.113

### Changed

- **Full Load's per-batch retry budget is now more patient (10 → 20) and
  operator-tunable**, so a batch rides out a longer transient DSQL connection
  storm instead of failing the table. The budget (`occ_max_attempts`) is shared by
  OCC (`40001`) conflicts and the transient connection retries added in
  v0.1.110/112; at high parallelism a connection storm at a load transition (many
  tables finishing → a burst of reconnects) can outlast the old 10-attempt (~20s)
  budget and exhaust it, failing a table with `ConnectionTimeout` even though the
  error was correctly classified as retryable. Raised the default to 20 (~70s of
  exponential-backoff retrying) — a large-scale load runs for hours and will meet
  such a blip — and exposed it as `DSQL_MIGRATOR_FULL_LOAD_OCC_MAX_ATTEMPTS`
  (1–100) for environments that need more. Each retry still leases a fresh
  connection and replays the idempotent batch, so this only adds patience, never
  duplicates.

## v0.1.112

### Fixed

- **Full Load now retries ANY no-SQLSTATE connection error, not just known
  message signatures.** v0.1.110 taught the loader to retry connection drops that
  carry no SQLSTATE, but matched them by a fixed list of libpq/OpenSSL message
  substrings. Under a high-parallelism connection storm (many tables finishing at
  once → hundreds of concurrent connections), DSQL surfaces the drop in *varying*
  forms — "SSL error: unexpected eof", "Network is unreachable", and
  **"connection timeout expired"** — and any message the list didn't contain
  slipped through as a permanent failure (a 1 TB run at 512 connections lost
  tables to `connection timeout expired`). The classifier now treats **any
  psycopg `OperationalError`/`InterfaceError` with `sqlstate=None`** as a transient
  connection failure (a genuine data/constraint error always carries a SQLSTATE),
  gated on the exception type so the tool's own no-SQLSTATE structural errors are
  still never retried. The message-signature list is kept only as a fallback for a
  wrapped/re-raised error whose type was lost.

## v0.1.111

### Fixed

- **DSQL connections are now pinned to IPv4, so a reconnect in an IPv4-only
  network can't fail on the endpoint's unreachable IPv6 address.** Aurora DSQL
  endpoints are dual-stack (A + AAAA records). In an IPv4-only VPC (e.g. an ECS
  task with no IPv6 egress), a reconnect that libpq routes to the IPv6 (AAAA)
  address fails with *"connection to server at … failed: Network is
  unreachable"*. That normally stays hidden — until a transient DSQL event (e.g.
  a brief `XX000 server unavailable`) forces many reconnects at once, at which
  point the IPv6 attempts fail an in-flight Full Load even though IPv4 is
  perfectly reachable (observed: a 1 TB in-VPC load lost tables to IPv6
  `Network is unreachable` right after a DSQL blip). `DsqlConnector.connect` now
  resolves the endpoint's IPv4 address and passes it as `hostaddr` (the DNS name
  stays as `host` for TLS SNI / certificate verification), so every connect and
  reconnect stays on the reachable address family. It falls back to the previous
  host-based resolution when no IPv4 is available (an IPv6-only environment is
  unaffected). Covers all DSQL connections — Full Load, Validation, and probes.

## v0.1.110

### Fixed

- **Full Load now recovers from a mid-query connection drop that carries no
  SQLSTATE (e.g. a TLS teardown), instead of failing the whole table.** The
  batched loader is designed to retry a transient connection drop by leasing a
  fresh connection and replaying the idempotent batch — but `_is_transient_connection_error`
  only recognized drops the server reported with a **SQLSTATE class `08`**. When
  the TLS socket is severed mid-query the server never sends an error code, so
  psycopg raises an `OperationalError` with `sqlstate=None` and a libpq/OpenSSL
  message like *"SSL error: unexpected eof while reading"* / *"server closed the
  connection unexpectedly"*. Those were **mis-classified as permanent** → not
  retried → the batch (and the whole table) failed. This bit hardest under high
  write parallelism (many concurrent connections → DSQL severs some at peak
  pressure): an in-VPC 1 TB load at `table_parallelism=16 × batch_parallelism=32`
  (512 connections) lost 16/20 tables near completion to `SSL error: unexpected
  eof`. The classifier now also treats a **no-SQLSTATE connection-lost error**
  (matched by libpq/OpenSSL drop signatures) as transient, so the loader
  reconnects and retries — the Full Load analogue of the CDC sink's transient
  reconnect. A real data/constraint error (which always carries a SQLSTATE) and a
  structural error with no SQLSTATE that isn't a connection drop are unaffected
  (still surface, never retried forever).

## v0.1.109

### Changed

- **CDC per-table status: "How to read this table" is far easier to scan, and
  each tricky column now explains itself in place.** The legend was a wall of
  small gray bullets where the column name was buried in prose and the
  consistency colors were only described in words. It is now a quiet bordered
  panel of **definition rows** — each term matches a table header, so the mapping
  is obvious — and the Consistency entry renders the **real badge chips**
  (`consistent` / `replicating…` / `rows missing` / `data quarantined`) in the
  exact same colors as the table cells, instead of naming the colors. In addition,
  the three non-obvious column headers (**Net rows since Full Load**, **Stream
  lag**, **Consistency**) now carry an **ⓘ tooltip** with a one-line explanation,
  so help is available right where the eye is. Added a reusable `definition_row`
  to the design system (single source of truth) for the legend layout.

## v0.1.108

### Fixed

- **Skewed CDC workloads no longer serialize a hot table on one sink task —
  Kafka topic partitions are now allocated proportionally to table size.** The
  scaling default spread partitions uniformly, which assumes write load is even
  across tables; when there are many tables (≥ the sink-parallelism cap) it
  collapsed to **1 partition per topic**, and a 1-partition topic can be consumed
  by at most one sink task. So when a few "hot" tables carried most of the writes
  (e.g. a sysbench run hitting 4 of 9 tables), each hot table was streamed by a
  single task while the rest sat idle — pure throughput loss (DSQL was near idle).
  The tool now reads scan-free per-table row-count estimates (the Full Load
  watermark's, or a fresh `information_schema` estimate if CDC infra is deployed
  before Full Load) and gives the larger tables **more partitions** via Debezium
  `topic.creation` groups (2 or 4 partitions for hot tables; 4 is the per-table
  ceiling, where a single table's gain flattens as concurrent DSQL upserts
  contend), so a hot table streams across several tasks in parallel. It is a
  no-op under even load and falls back to the previous uniform default when there
  is no size signal or an explicit `DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS` override
  is set. Partition counts are fixed at topic creation, so this is decided at CDC
  infra deploy; ordering is unaffected (Debezium keys each record by primary key,
  so a given key always lands on one partition). Requires a fresh CDC infra
  deploy to take effect (existing topics' partition counts are immutable).

## v0.1.107

### Changed

- **Evaluation "Objects by importance" filter is now two clear, category-based
  dropdowns instead of one confusing mixed control.** The old segmented control
  mixed a derived "Needs attention" bucket with per-classification values on one
  axis, which read ambiguously (e.g. "Needs attention" vs. "Review needed"). It
  is replaced by two AWS-Console-style filter dropdowns — **Classification**
  (Automatic / Review needed / Unsupported) and **Estimated manual effort**
  (Simple / Medium / Significant) — the same color-coded categories the summary
  badges already show. The two filters combine (AND), and a **Clear filters**
  link appears when any filter is active. Added a reusable `filter_select` /
  `filter_bar` to the design system (single source of truth) so the dropdowns
  match the Cloudscape "filtering" look.

## v0.1.106

### Fixed

- **CDC infrastructure deploy now self-heals when MSK Serverless rejects an
  auto-selected subnet's availability zone.** MSK Serverless supports only a
  subset of a region's AZs and offers no API to list them, so when the deploy
  auto-selects one NAT-egress subnet per AZ it can pick a subnet in an
  unsupported AZ (e.g. `ap-northeast-2d`), making `MskCluster` fail with
  `CREATE_FAILED … unsupported availability zones: [ap-northeast-2d]` and the
  whole stack roll back. The deployer now detects that specific failure, parses
  the rejected AZ(s) from the stack event, deletes the rolled-back stack,
  re-selects connector subnets with those AZ(s) excluded, and retries the create
  automatically (bounded, so a genuinely stuck deploy still stops). If excluding
  the unsupported AZ(s) leaves fewer than two NAT-egress AZs, it stops with a
  clear message naming the excluded AZ(s) instead of looping. No new inputs —
  the user still supplies only a VpcId.

## v0.1.105

### Added

- **Accurate, time-based CDC replication lag — replacing the imprecise `MAX(pk)`
  "Stream lag".** The old per-table "Stream lag (newest)" compared `MAX(pk)` on each
  side: a count of PK units (not time), insert-only (blind to UPDATE/DELETE lag), and
  only for single-column integer PKs. The DSQL sink now reads each change's **source
  commit time** (Debezium `source.ts_ms`) and emits a per-table **`ReplicationLagMs`**
  CloudWatch metric = apply-wall-clock − source commit time (the worst lag per
  offset-commit window, in milliseconds). The migration monitor's **"Stream lag"**
  column now shows a real time value ("8.5s behind", "2m 10s behind", "caught up"),
  read live and scan-free — accurate for any PK type and reflecting update/delete lag,
  not just the newest insert. It falls back to the `MAX(pk)` leading-edge check
  ("N behind (PK)") only when the time metric is unavailable (older plugin) or the
  counts weren't refreshed. Emission is strictly best-effort (never affects
  replication) and reuses the v18 metric plumbing/IAM (`cloudwatch:PutMetricData`,
  `metrics.stack`) — no new IAM.
- Requires the rebuilt connector plugin (`PLUGIN_VERSION` → `v19`) and a CDC
  re-deploy to take effect; until then the column uses the `MAX(pk)` fallback.

## v0.1.104

### Fixed

- **Query Playground "Test on target" now resolves unqualified table names instead
  of failing with `relation "orders" does not exist` (42P01).** A query written
  against a MySQL database uses unqualified table names (`SELECT * FROM orders`), but
  the migration maps each MySQL database to a same-named PostgreSQL **schema**
  (`ecommerce_demo`), so on DSQL the tables live in that schema — not the default
  `public` search_path the probe ran under, so every unqualified reference was
  rejected. The probe now sets `search_path` to the source database's schema (then
  `public`) before the `EXPLAIN` / dry run, mirroring the MySQL execution context so
  the converted query validates against the migrated tables. No effect when the
  source connection specified no database (search_path unchanged).

## v0.1.103

### Fixed

- **Validation no longer gets stuck on "In progress" with a locked "Re-run
  validation" button when a run finishes while you're on another step.** If you
  clicked Re-run and then navigated away (e.g. to the Data Migration step to Stop
  CDC) while the run was in flight, the poll timer that flips the step to `DONE` was
  torn down with the page — so when the run finished in the background and you
  returned to Validation, the step reconciled to `DONE` *inside the content render*,
  too late for the workflow shell (the step-header badge + Re-run button had already
  drawn the stale "In progress" state, and nothing re-rendered them). Now, whenever
  the in-content reconcile changes the persisted status (finished-while-away or the
  v0.1.102 reconnect case), it schedules a one-shot refresh so the shell re-renders
  with the reconciled status — the completed report shows with an enabled Re-run
  button. (The follow-up render sees `DONE`/`NOT_STARTED`, so it never loops.)

## v0.1.102

### Fixed

- **Validation no longer gets stuck on "In progress" with a permanently-locked
  "Re-run validation" button after a reconnect.** If the browser reconnected right as
  a validation finished (or the session was saved mid-run), the step was restored as
  `IN_PROGRESS` while its completed report was also restored — but the validation job
  id is not persisted, so no live job could ever flip it to `DONE`. The in-content
  reconcile to `DONE` ran too late (after the workflow shell had already drawn the
  stale "In progress" badge + disabled Re-run button, and nothing re-rendered the
  shell), leaving the step showing a finished report under an "In progress" header
  with Re-run locked forever. Session restore now reconciles the step to `DONE` when
  it reads `IN_PROGRESS` but a completed report is present (a report proves the run
  finished) — before the shell renders — so the completed result shows with an
  enabled Re-run button. A genuinely in-flight run has no report (it is cleared at
  run start), so this never hides a live run.

## v0.1.101

### Fixed

- **After deleting the CDC infrastructure, a reconnected session now shows the
  "Deploy CDC infrastructure" action again instead of getting stuck on the old
  "Infrastructure deleted" log.** On reconnect the session restore was re-applying a
  *completed* CDC lifecycle job's link (so the finished delete's stage log kept
  rendering) and the *stale* connector names from before the teardown (so the card
  could misclassify the pipeline) — with no path to redeploy. Restore now skips both
  the finished-job link and the stale connector names when the last CDC action was a
  teardown (`delete` / `stop`), so the card is driven by the fresh read-only AWS
  phase probe: **absent → Deploy CDC infrastructure**, **infra → Start CDC**. The
  stack identity is still restored so the probe knows which stack to check; an
  in-flight teardown is reflected by the probe's live stack status, not a stale job.

## v0.1.100

### Added

- **Durable S3 job store — an interrupted Full Load AND the per-table migration
  monitor now survive a Fargate redeploy.** The JobManager's job state lived in a
  SQLite file on the task's **ephemeral `/tmp`**, so an app redeploy (ECS task
  replacement) wiped it: an interrupted Full Load couldn't resume, and the
  per-table migration monitor — which is keyed to the Full Load job — went **blank**
  after a deploy (the S3 session store kept only the `job_id` linkage, not the job
  itself). New `S3JobStore` persists each job snapshot as a JSON object under
  `jobs/` in the tool's **managed plugin bucket** (same bucket as the session store,
  auto-provisioned — no extra setup), so job/resume state survives a task
  replacement. Wired on Fargate via `DSQL_MIGRATOR_JOB_STATE_BUCKET` → the managed
  bucket; local dev keeps the on-disk SQLite store (both satisfy the `JobStore`
  protocol, so the JobManager is unchanged).
- **Scale-safe writes (no PUT storm).** The Full Load drain persists on every
  progress tick, which is cheap for local SQLite but would flood S3 on a large
  table. Since only chunk/job **status transitions** matter for resume (a non-`DONE`
  chunk is re-run whole; sub-chunk progress is display-only and an interrupted chunk
  is reconciled to `FAILED` on reload), `S3JobStore` PUTs immediately on a status
  signature change and throttles pure-progress writes to ≤ 1 PUT / 5 s — bounding
  PUTs to ~the number of status transitions regardless of row count. Best-effort (an
  S3 error never breaks the live migration); no new IAM (the task role's existing
  bucket `/*` grant covers the `jobs/` prefix). Template + code only — no
  connector/plugin change; takes effect on the next app redeploy.

## v0.1.99

### Fixed

- **The per-table net-rows monitor now works in single-database mode, not just
  cluster mode.** The DSQL sink always emits the `NetRowsApplied` metric's `Table`
  dimension **schema-qualified** (`db.table`, e.g. `ecommerce_demo.orders`), but in
  single-database mode the tool addresses tables by **bare** name (`orders`) — so the
  monitor's exact-dimension CloudWatch lookup missed and the "Net rows since Full
  Load" column silently fell back to the `COUNT(*)`-based figure. The reader now
  `ListMetrics`-discovers the `Table` dimension values the sink actually published for
  the stack and matches each requested table by exact name, else by an **unambiguous
  bare** table name — so the scan-free column works in both cluster (already-qualified)
  and single-database (bare) naming, without assuming the qualification scheme.
  Ambiguous bare matches (the same table name under two schemas) are skipped rather
  than risk misattributing rows (that table falls back to the COUNT). Grants the app
  task role `cloudwatch:ListMetrics` (Resource `*` — the API has no resource-level
  scoping). Reader + IAM only (no connector/plugin change): a deploy updates the role
  and ships the reader — no plugin rebuild or CDC re-deploy needed.

## v0.1.98

### Added

- **Per-table "Net rows since Full Load" is now scan-free — sourced from a sink
  metric, not a `COUNT(*)`.** The DSQL sink connector now emits a per-table
  `NetRowsApplied` CloudWatch metric (namespace `MysqlDsqlMigrator/CDC`, dimensions
  `Stack` + `Table`): each commit records inserts − deletes (an insert is +1, an
  update 0, a delete −1), so summing the metric gives the net rows CDC has applied
  to each table since it started streaming. The per-table migration-status monitor
  reads this on the existing ~5 s CDC poll and shows it directly, so the "Net rows
  since Full Load" column no longer needs any `COUNT(*)` on the source or target —
  it stays light and never scans the (potentially billion-row) source. While CDC is
  streaming the per-table table now re-renders on that poll (reading the stored
  metric, no network), so the column updates **live** instead of only when you click
  "Refresh source/target counts" (which still runs the exact source/target
  `COUNT(*)` — those columns are unchanged). Emission is strictly best-effort in the
  sink (a metric failure never affects replication or offset commits), and the
  column falls back to the old `target − Full Load` figure when the metric is
  unavailable (older plugin, or the sink not yet emitting). The figure is a live
  progress monitor, not the authoritative reconciliation: it can slightly over-count
  if Kafka Connect redelivers an already-applied batch (at-least-once), so the exact
  source-vs-target verdict remains Validation (Step 4).
- Requires the rebuilt connector plugin (`PLUGIN_VERSION` → `v18`) and a CDC
  re-deploy to take effect; until then the monitor uses the `COUNT(*)`-based
  fallback. Template change grants the sink's connector-execution role
  `cloudwatch:PutMetricData` scoped by a namespace condition; the app task role's
  `cloudwatch:GetMetricData` (added in v0.1.97) reads it back.

## v0.1.97

### Fixed

- **The live CDC pipeline-health throughput now actually populates.** The UI reads
  the connectors' `AWS/KafkaConnect` CloudWatch metrics (`SinkRecordSendRate`,
  `SourceRecordPollRate`, running/errored task counts) for the change-flow panel,
  but the app task role was never granted `cloudwatch:GetMetricData` — so every read
  failed with `AccessDenied` and was swallowed best-effort, leaving the throughput
  showing blank/unknown. Grant `cloudwatch:GetMetricData` (Resource `*` — the API
  has no resource-level scoping) so the panel shows real send/poll rates. This is a
  lightweight, **source-scan-free** CDC-activity signal; it also readies the role to
  read the per-table net-rows custom metric added next. Template-only IAM change —
  a deploy updates the role (no image rebuild).

## v0.1.96

### Fixed

- **Start/Stop CDC no longer fails with `AccessDeniedException` on
  `kafkaconnect:ListConnectors`.** The CDC-deploy role granted `ListConnectors` but
  scoped it to a connector ARN (`connector/mysql-dsql-cdc-*/*`) — yet `ListConnectors`
  is an **account-level** list operation, authorized against `.../v1/connectors`, so
  the ARN scope granted nothing. The deployer lists connectors to read source/sink
  state during the two-pass Start CDC (and Stop), so that read hit AccessDenied and
  the operation errored ("could not read … state"). It became visible once v0.1.86
  made the connector-state read raise (instead of silently returning `None`).
  `ListConnectors` is now granted on `Resource: "*"` in its own statement (matching
  the task role's discovery grant); the other connector operations stay scoped to the
  `mysql-dsql-cdc-*` family. Requires an app-stack deploy to update the role (no image
  rebuild).

## v0.1.95

### Fixed

- **Hardened the S3 session store so a snapshot serialization error can't break the
  UI.** In `S3SessionStateStore.save()` the `model_dump_json()` serialization ran
  just outside the `try`/`except` that guarantees the store never raises to its
  caller, so a (dormant, but possible) serialization failure could escape and break
  the live UI request that persists session state. Moved the serialization inside
  the guard so `save()` honors its best-effort contract in all cases — no behavior
  change on the normal path. (Surfaced by the v0.1.93 change's own adversarial
  review.)

## v0.1.94

### Fixed

- **Stopping CDC no longer reports a false "Stack operation timed out".** Stop CDC
  blanks `MskBootstrapServers`, which removes the connectors — but it also used to
  tear down the in-VPC offset-seeder Lambda, and reclaiming that Lambda's Hyperplane
  ENIs takes ~20–40 min, well past the control plane's 10-minute stop wait. So the
  stop reported a failure even though the connectors were already removed (CDC was
  actually stopped) and the stack reached `UPDATE_COMPLETE` on its own minutes
  later. The cdc-stack template now keeps the seeder Lambda (+ its role) deployed
  across a stop via a new `DeploySeederFunction` condition (gated on the seeder key
  + watermark, independent of `MskBootstrapServers`); only the fast
  `OffsetSeedResource` invoker is removed on stop. Stop cleanup is then just the two
  connectors + the invoker (all quick), so the stack settles well inside the
  timeout; the VPC-Lambda ENI teardown now happens only on a full stack delete
  (whose timeout already accommodates it). Takes effect once the updated template is
  deployed — i.e. from the next Start CDC.

## v0.1.93

### Added

- **Durable per-session resume across a redeploy (S3-backed session store).** A
  reconnecting browser resumes its per-session workbench (workflow progress, the
  Step-1 Evaluation result, Schema Conversion choices, the CDC start point / adopted
  stack) instead of re-running Evaluation. That snapshot previously lived in a local
  SQLite file on the container's **ephemeral** disk, so a Fargate **task
  replacement** (any redeploy) wiped it — the operator had to redo Evaluation after
  every deploy. A new `S3SessionStateStore` (implementing the existing
  `SessionStateStore` protocol) writes each non-secret snapshot to the tool's managed
  plugin bucket (`mysql-dsql-migrator-plugins-<account>-<region>`, auto-provisioned —
  no new parameter or customer setup) under a `sessions/` prefix, so resume now
  survives a redeploy. Selected automatically on the Fargate deploy via a new
  `DSQL_MIGRATOR_SESSION_STATE_BUCKET` (the template points it at the managed
  bucket); local dev keeps the SQLite path. Non-secret state only (Property 7 — the
  source DB password is re-entered on Connect); persistence is best-effort (a
  transient S3 error is logged and never breaks the UI). The task role gains
  `s3:DeleteObject` for session delete/prune.

## v0.1.92

### Fixed

- **Adopting an existing CDC pipeline now reconciles its table set, so the CDC
  step reflects the running pipeline instead of "no tables selected".** When a
  session attaches to a pre-existing cdc-stack ("Attach to &lt;stack&gt;", e.g.
  after a session reset) — or the pipeline was otherwise started out of band —
  the session held no Full Load watermark and no in-session table selection, so
  the CDC step showed "Select at least one table before starting CDC", built its
  config preview from an empty set, and could not populate the per-table status,
  even though the pipeline was actively replicating. The render-time stack probe
  now reads the live stack's `TableIncludeList` (the source connector's
  `table.include.list`, i.e. each table's name) and reconciles it onto the
  session; `_cdc_tables_for_config` uses it as a final fallback (after an
  in-session watermark or selection). So an adopted/out-of-band pipeline resolves
  exactly which tables it is replicating — the "select a table" warning clears,
  the config preview and per-table status reflect reality — while a normal
  in-session Full Load → Start CDC flow is unchanged. Re-adopting a different
  stack clears the previous reconciled set (the fresh probe repopulates it).

## v0.1.91

### Fixed

- **The CDC "Deploy log" no longer snaps shut every few seconds while a
  lifecycle job runs.** The live CDC panel re-renders on a ~5s poll to stream
  new deploy-log lines and connector status. The "Deploy log" expansion's
  open/closed state was held in a local variable of the panel's render function,
  so each full re-render recreated it as collapsed — a log the operator expanded
  to watch a Start/Stop/Deploy would close itself a few seconds later. The
  open/closed state now lives on the session-scoped migration state, so it
  survives every level of re-render (both the inner refreshable and the outer
  panel poll) and stays open until the operator closes it.

## v0.1.90

### Fixed

- **The "CDC is running, can't apply schema" block on Schema Conversion is now
  actionable instead of a dead end.** When a CDC pipeline is already streaming into
  the target, applying schema conversion is (correctly) blocked — the sink is
  writing those tables and DDL is not replicated, so a REPLACE would drop or
  corrupt them. Previously this only surfaced as a transient toast shown when you
  clicked Apply, telling you to "stop CDC first" — but Data Migration (the only
  place CDC can be stopped) is prerequisite-locked behind Schema Conversion, so
  there was no way forward from that screen. Schema Conversion now shows a
  **persistent warning notice** at the top of the step whenever CDC is live,
  explaining that the target schema is **already applied** (CDC is streaming to it)
  and offering the one safe path: **"Skip conversion & continue to Data
  Migration"** — which both proceeds and unlocks Data Migration, where CDC can be
  stopped if the schema genuinely needs to change. The on-Apply toast now carries
  the same actionable guidance (Skip to continue, or stop CDC in Data Migration to
  change the schema).

## v0.1.89

### Fixed

- **The "attach to existing CDC infrastructure" banner now appears on the Migration
  Plan step, where CDC is actually chosen.** v0.1.88 added the banner but only on
  the Data Migration step's migration-type selector; the **Migration Plan** step is
  a separate screen (where you answer "Include CDC? — Yes, keep in sync"), and it
  did not surface the banner — so selecting keep-in-sync there still dropped you
  into the fresh "deploy CDC infrastructure" flow (and the "already exists" error)
  for a pipeline that already exists. The Migration Plan's CDC-infrastructure
  section now shows the **"Attach to &lt;stack&gt;"** banner when an existing
  `mysql-dsql-cdc-*` pipeline is discovered under a different stack name (the phase
  probe that already runs on that step also populates the discovery). Attaching
  points the session at that stack; the next probe recognizes it as deployed and
  shows "CDC infrastructure ready". The Data Migration surfacing is kept as well.

### Added

- **Existing CDC infrastructure is now surfaced on the Migration Plan, not only
  deep in the CDC step.** The previous release added account-wide CDC discovery and
  an "attach to existing" choice, but that affordance only rendered inside the
  active CDC sub-step — which a session reset makes hard to reach (you must pass the
  earlier steps first). Now, the moment the plan includes CDC, a banner beside the
  migration-type choice names any existing `mysql-dsql-cdc-*` pipeline with an
  **"Attach to &lt;stack&gt;"** action, so you adopt it right where you are —
  without navigating to the CDC sub-step and hitting a duplicate-deploy risk. The
  discovery already runs at plan time (it is gated on the plan including CDC); this
  just surfaces its result where the user is. Attaching remains read/attach-only,
  and deploying a deliberate second pipeline (a different stack-name suffix) is
  still available from the CDC step — so this is a choice, never a hard block.

### Added

- **The CDC screen now discovers existing CDC infrastructure and offers to attach
  to it, instead of blindly re-deploying.** The tool tracked which CDC stack a
  migration uses in session state only; a single-task app restart (an ECS/Fargate
  task replacement) loses that, so a reconnected session defaulted to a fresh
  "deploy CDC infrastructure" flow even when a CDC pipeline was already running
  under a different stack name — risking a second, billable Amazon MSK cluster. The
  CDC step now scans the account for `mysql-dsql-cdc-*` stacks and, when one exists
  that the session doesn't target, surfaces it with a primary **"Attach to
  &lt;stack&gt;"** action (fresh deploy is de-emphasized behind an expansion).
  Attaching re-reads the pipeline's live state from AWS (running / provisioning /
  infra), so a running pipeline lands straight on its monitoring view; it never
  mutates the stack or connectors — starting over remains the explicit **Stop CDC**
  (connectors only, keeps MSK) or **Delete CDC infrastructure** path. Requires the
  CDC-deploy role to have `cloudformation:ListStacks` (added to the app stack);
  discovery is best-effort and simply shows nothing if the grant is absent.

### Fixed

- **CDC no longer stalls silently when it can't read a connector's state; it
  surfaces the cause.** When starting CDC, the tool waits for the source connector
  to reach `RUNNING` before it requests the sink connector. That wait read the
  connector state through a helper that swallowed **every** error (credential
  expiry, throttling, a transient network blip) to `None` — indistinguishable from
  "still creating" — so a read failure made the wait log "creating…" forever: the
  sink was never requested, the deploy appeared stuck, and no error was shown.
  Recovering then required restarting the app task, which on Fargate wipes the
  in-progress session (all workflow steps had to be redone). Now the state read
  **propagates** errors; the `RUNNING`-wait tolerates a few consecutive transient
  read failures and then fails with the **actual cause**, and fails **immediately**
  on a non-recoverable credential/authorization error with a "retry Start CDC"
  hint. A genuinely-absent connector still reads as `None` (unchanged), so normal
  "still provisioning" polling is unaffected.

## v0.1.85

### Fixed

- **CDC failed to deploy: the privileged CDC-deploy role was missing CloudWatch
  alarm permissions.** v0.1.84 added a per-connector CloudWatch alarm (on
  `ErroredTaskCount`) to the CDC stack, but the app's `cdc-deploy` role was not
  granted `cloudwatch:PutMetricAlarm` / `DeleteAlarms` / `DescribeAlarms`. Starting
  CDC therefore failed while creating the alarm with an `AccessDenied` error, and
  the CDC stack rolled back (its rollback then also failed on
  `cloudwatch:DeleteAlarms`), so no connectors were created. The role now has the
  scoped alarm permissions (alarm ARNs in the CDC stack family). **Redeploy the
  app-stack to pick up the permission, then retry Start CDC** (no new image build
  is required — this is an IAM-only template change).

## v0.1.84

### Fixed

- **CDC sink survives a transient DSQL connectivity blip instead of dying
  (connector rebuilt, `PLUGIN_VERSION` v17).** On a transient failure — OCC
  retry budget exhausted, or a connection torn down by DSQL's 1-hour idle close,
  IAM-token expiry, or an MSK Connect worker recycle — the sink re-raised a plain
  `ConnectException`, which Kafka Connect's `WorkerSinkTask` treats as **fatal**:
  it kills the task, the offset never advances, and CDC stalls until a human
  restarts the connector. The sink now throws `RetriableException` for these
  transient cases, which `WorkerSinkTask` catches and redelivers (pause + retry
  the same batch) so the pipeline self-heals across a reconnect. Apply is
  idempotent, so replaying the same offsets is safe. The transient-vs-permanent
  classification is unchanged; a genuine poison row still goes to the DLQ.
- **Gapless resumability on a low-traffic source: the Debezium source connector
  now sets `heartbeat.interval.ms`.** Debezium only advances its committed binlog
  offset when it emits a record. If the captured tables are idle while other
  tables churn the binlog, the committed offset can fall behind the live binlog
  head; if source binlog retention then purges past it, a restart cannot resume
  (a gap → forced re-Full-Load). A periodic heartbeat keeps the offset advancing.
  `heartbeat.action.query` is deliberately not set — it would write to the
  read-only source; emitting the heartbeat record is enough for MySQL.

### Added

- **CloudWatch alarms surface a failed CDC connector automatically.** Each
  connector (Debezium source, DSQL sink) now has an alarm on the
  `AWS/KafkaConnect` `ErroredTaskCount` metric, so a task that errors out is
  visible without a human watching the console — previously, recovery waited
  entirely on someone noticing a FAILED connector, and a long gap could exceed
  source binlog retention. The alarms are always created (visible in CloudWatch);
  set the new optional `AlarmNotificationTopicArn` parameter to an SNS topic ARN
  to also be notified. No SNS wiring is required to deploy (the default is empty).

## v0.1.83

### Fixed

- **AI assist works outside US regions: the default Bedrock model is now a
  region-agnostic `global.*` profile.** The code-level default model id was
  `us.anthropic.claude-sonnet-4-6`, a US-geography cross-region-inference profile
  that `InvokeModel` rejects from a non-US region. An operator deploying in, e.g.,
  ap-northeast-2 (Seoul) who enabled AI assist and left the model id blank (the
  natural path) got a failure. The default is now
  `global.anthropic.claude-sonnet-4-6` (reachable from any commercial region),
  matching the CloudFormation template's own recommendation. Set `BEDROCK_MODEL_ID`
  to override. (Found by a region-portability audit; the deploy templates, region
  derivation, STS/token region, and S3 endpoint/LocationConstraint handling were
  all already region-correct — only this code default was US-locked.)

## v0.1.82

### Fixed

- **AI assist: expired/invalid AWS credentials now give an actionable message.**
  An expired-session or invalid-signature error (`ExpiredTokenException`,
  `InvalidSignatureException`, `InvalidClientTokenId`, …) was misclassified as a
  generic "unavailable"/"unknown", telling the user only that the workflow
  continues without AI — with no hint to re-authenticate. Such errors are now
  classified as `ACCESS_DENIED` on both the suggestion and "Verify AI access"
  paths, and both messages now mention re-authenticating if credentials/session
  expired.
- **Cluster-wide schema read: cross-schema foreign-key targets are now
  schema-qualified.** When reflecting an entire cluster (multiple schemas), a
  table name was qualified `schema.table` but its foreign key's referenced table
  stayed unqualified, so a downstream orphan-check / DDL query resolved the parent
  against the search_path (or a wrong same-named table in another schema). The FK
  target is now qualified with the FK's own `referred_schema` (or the reflected
  schema for a same-schema FK), matching how table names are qualified.

### Changed

- **AI assist hardening.** The Bedrock client now sets bounded connect/read
  timeouts (10s / 60s) so a hung connection can't leave an "AI is writing…" /
  "Verifying…" state spinning forever (a stalled socket surfaces as a
  classified network/timeout error). "Verify AI access" now also catches an
  error while *building* the client (e.g. no resolvable region) and reports it
  as an actionable result instead of letting the exception reach the UI. The
  persistent AI-status line in the connection screen now carries its verdict
  severity via the design-system palette instead of plain gray text.
- **Source overview: report the Aurora writer's instance class, not a reader's.**
  For an Aurora cluster endpoint the source-metadata lookup now resolves the
  writer via `DescribeDBClusters` (`IsClusterWriter`) instead of taking an
  arbitrary cluster member, so an asymmetric writer/reader topology no longer
  mislabels the source capacity (best-effort; falls back to the first member).
- **Schema apply: `CREATE SCHEMA` self-heals a duplicate-object race.** A `42P07`
  on schema creation is now absorbed as `CREATED` (the schema is present),
  matching the table/view/index self-heal path, instead of a spurious `FAILED`.

## v0.1.81

### Fixed

- **Evaluation: `TINYINT(1)`, `BIT(n)`, and `YEAR` are no longer reported as
  fully auto-compatible.** The compatibility assessor had no rule for these three
  types, so a table whose only notable column was one of them was classified
  `AUTO` / `COMPATIBLE` with zero findings — even though the schema converter maps
  all three to a *different* DSQL type with changed semantics (`MANUAL`), and a
  `TINYINT(1)` value outside `{0,1}` aborts Full Load. Evaluation therefore showed
  "fully compatible, no risk" for a table that could fail at load, contradicting
  the assessor's own "nothing is silently treated as compatible" guarantee. New
  `TINYINT_BOOLEAN` / `BIT_TYPE` / `YEAR_TYPE` rules now surface each as `MANUAL`
  with the specific risk, matching the converter's classification.

### Changed

- **Evaluation: spatial columns are now `MANUAL`, not `UNSUPPORTED`.** Spatial
  types (`GEOMETRY`, `POINT`, `POLYGON`, …) were classified `UNSUPPORTED` with a
  "substitute or redesign the column" recommendation, implying the table was
  blocked. But the converter already auto-substitutes each spatial column to
  `bytea` (raw WKB bytes preserved end-to-end through Full Load and CDC), so the
  table migrates. The new `SPATIAL_TYPE` rule reclassifies these as `MANUAL`
  (review whether raw `bytea` suffices; spatial operators/indexes are lost),
  which no longer sends users to redesign a table the tool already migrates.

## v0.1.80

### Changed

- **UI: statuses use the design-system palette instead of ad-hoc glyphs/colors.**
  A design-system consistency pass across the Data Migration and Evaluation
  screens:
  - The Full Load "CDC is streaming" warning card and the CDC consistency /
    stream-lag columns no longer embed literal `✓`/`✗`/`⚠` glyphs (a
    tofu-box risk on fonts lacking them). Severity is carried by the existing
    colored notice box / status badge; the health-table legend was reworded to
    describe the colored badges rather than glyphs.
  - Busy buttons (Fetch current position, Start CDC, Apply to target) now show
    the in-progress state by disabling + swapping the label (e.g. "Applying…")
    instead of the Quasar `loading` prop the design system forbids.
  - Warning/disruptive cues use the design system's amber rather than orange
    (Stop CDC buttons, the score gauge, effort/conflict badges).
  - Removed a dead, never-rendered `_format_complete_cell` helper.

## v0.1.79

### Fixed

- **App shell: a step-render crash now shows a red error notice, not a blue
  info one.** The top-level "step could not be displayed" fallback called
  `render_notice(tone="negative")`, but `negative` is not a defined notice tone,
  so it silently fell back to the calm blue `info` styling for what is actually
  the most alarming state in the app (an unhandled rendering exception). It now
  uses `tone="error"` (red), matching the severity.

### Housekeeping

- Open-source release hygiene: removed an internal session handoff note and
  internal author/repo identifiers from the talk decks, and replaced dangling
  citations of internal (unpublished) design/spec documents in the connector
  sources, CloudFormation template, and CDC read-models with inline summaries.
  Added `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, and `SECURITY.md`.

## v0.1.78

### Fixed

- **Schema Conversion: `DOUBLE(M,D)` now emits valid DSQL DDL.** A MySQL
  `DOUBLE(M,D)` column (e.g. `DOUBLE(10,2)`) fell through the type mapper (it parses
  to sqlglot kind `DOUBLE`, which the `UDOUBLE`/`FLOAT` special cases both miss),
  so it rendered as a two-argument `FLOAT(10, 2)`. PostgreSQL/DSQL `double precision`
  takes no arguments, so this was a syntax error that failed the **entire** table's
  `CREATE TABLE` at apply time. `DOUBLE(M,D)` now maps to a plain `double precision`
  (the `(M,D)` display spec carries no storage meaning), matching the existing
  `FLOAT(M,D) -> real` handling.
- **Validation: large `BIGINT UNSIGNED` / `DECIMAL` values no longer produce false
  checksum mismatches.** The PostgreSQL-side `to_char` numeric mask provided only 18
  integer digit positions, but the MySQL side renders via `CAST(... AS DECIMAL(65,
  scale))` and `BIGINT UNSIGNED` is stored as `numeric(20, 0)`. Any integer magnitude
  at or above ~10^18 (e.g. `18446744073709551615`) overflowed the mask, so `to_char`
  emitted the overflow indicator (`####...`) instead of the digits — making a
  byte-identical value report a **checksum MISMATCH** and potentially blocking
  cut-over on a false alarm. The mask now spans the full 65-digit `DECIMAL(65,0)`
  integer range.

## v0.1.77

### Fixed

- **CDC survives a source reboot without manual intervention.** When the source
  RDS/Aurora instance rebooted (maintenance patch, failover, instance-class change),
  the Debezium source connector hit a retriable binlog error, restarted once, failed
  the restart with "Error reading MySQL variables: Communications link failure"
  (source still booting), and — because `errors.retry.timeout` defaulted to `0` (no
  retry) — Kafka Connect **killed the task permanently** ("will not recover until
  manually restarted"), a silent stall (`SourceRecordWriteRate=0`) needing a
  Stop/Start to recover. The source connector now sets `errors.retry.timeout=600000`
  (10 min) + `errors.retry.delay.max.ms=60000` — mirroring the sink — so it keeps
  reattempting across the reboot window and resumes from the committed binlog offset
  once the source is back (gapless, no human intervention). Observed and fixed after
  a 2→8 vCPU source scale-up reboot on 2026-07-08.

### Changed

- **CDC: sink MCU is now sized separately from the source (`SinkMcuCount`).** The
  sink became CPU-bound once the per-row round-trips were removed (plugin v16: ~80%
  CPU / ~21,000 rows/s at 4 MCU), while the single-task source has spare CPU. A new
  `SinkMcuCount` CFn parameter (default 4) lets the sink scale independently;
  `ConnectorMcuCount` now applies to the source only. Measured: sink 4→8 MCU took
  throughput ~21,000 → ~26,200 rows/s and CPU 80% → ~34%.

## v0.1.76

### Changed

- **CDC sink: fetch parameter metadata once per statement (plugin `v16`) — ~9.7×
  sink throughput.** `bind()` called `getParameterMetaData()` for every change
  event; on pgjdbc that is a server-side Parse/Describe round-trip, so the sink was
  issuing roughly one read-only transaction *per applied row* — confirmed by DSQL's
  `ReadOnlyTransactions` metric sitting at ~115,000/min (≈ 60× the write rate) while
  `OccConflicts` was flat 0. That hidden round-trip, not server-side write
  contention, was the real ceiling — it was cancelling most of the v13/v15 batching
  gains. The metadata is identical for every row of a given SQL, so it is now
  fetched once per prepared statement and passed into `bind()`. Measured DSQL apply
  rate rose from ~1,925 to **~18,672 rows/s** (8 partitions/tasks); read-only
  transactions dropped ~150× and sink CPU rose 10% → ~65%. Sink-jar change only
  (`PLUGIN_VERSION` → `v16`).

## v0.1.75

### Changed

- **CDC sink: multi-row INSERT rewrite (plugin `v15`) — +30% sink throughput.** The
  sink's JDBC URL now enables pgjdbc `reWriteBatchedInserts=true`, so a batch of
  single-row `INSERT`s is collapsed into one multi-row
  `INSERT ... VALUES (..),(..) ON CONFLICT ..` statement — turning N execute
  round-trips into 1. Because DSQL is latency-bound, this lifted measured sink
  throughput from ~1,500 to ~1,925 rows/s (8 partitions/tasks), cross-checked by
  the DSQL apply rate. To make the rewrite safe, `applyChunkBatched` first dedupes
  each same-SQL run to one row per primary key (last image wins — idempotent,
  order-preserving); without it a rewritten multi-row `ON CONFLICT` would reject a
  duplicate conflict key ("cannot affect row a second time"). Sink-jar change only
  (`PLUGIN_VERSION` → `v15`).

## v0.1.74

### Changed

- **CDC connector scaling is now inferred, not hardcoded.** The tool computes the
  per-table topic partition count, sink `tasks.max`, and MSK Connect MCU count from
  the number of captured tables (`compute_cdc_scaling_defaults`) and passes them at
  cdc-stack create. It picks the smallest partitions-per-topic that brings total
  sink parallelism (`partitions × tables`) up to a ceiling of 8 — e.g. 1 table → 8
  partitions, 4 tables → 2 each, ≥8 tables → 1 each — because the sink is
  DSQL-write-latency-bound and scales sublinearly past that point. Partition count
  is set at create because it is irreversible (a topic's partitions can only be
  raised). Previously `topic.creation.default.partitions` was hardcoded to `4`; it
  is now the `TopicDefaultPartitions` CFn parameter. Advanced operators can override
  the inference with `DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS`,
  `DSQL_MIGRATOR_CDC_SINK_TASKS_MAX`, and `DSQL_MIGRATOR_CDC_MCU_COUNT`. Manual §7.2
  (CDC) documents the model.

## v0.1.73

### Changed

- **CDC source throughput tuning (plugin `v14`).** After the v0.1.72 sink batching,
  the bottleneck moved to the Debezium source (~2,000 rec/s at ~12% CPU —
  produce/queue-bound, not binlog-parse-bound). New CFn parameters expose the
  source pipeline knobs so a redeploy can widen it: `SourceMaxBatchSize` (8192) and
  `SourceMaxQueueSize` (32768) drain more binlog events per streaming iteration, and
  `SourceProducerBatchSize` (256 KiB), `SourceProducerLingerMs` (20), and
  `SourceProducerCompression` (`lz4`) enlarge and compress the Kafka produce batch.
  The producer knobs are set as `producer.*` in the **source worker config** — MSK
  Connect rejects per-connector `.override.` keys — so the immutable worker config
  is renamed via a `PLUGIN_VERSION` bump to `v14` (no connector JAR changed).

## v0.1.72

### Changed

- **CDC sink throughput: batched apply (plugin `v13`).** The DSQL sink connector
  now coalesces each maximal run of *consecutive* change events that render to the
  same SQL into a single JDBC `executeBatch()` instead of a per-row
  `executeUpdate()`. DSQL is latency-bound — each statement is a distributed
  round-trip, and the sink task was observed running at ~5% CPU / ~550 rec/s
  (round-trip-bound, not compute-bound). Collapsing per-row round-trips into
  batched sends is the primary throughput lever. Apply **order is preserved**:
  only contiguous identical-SQL events group, so an upsert followed by a delete on
  the same PK still applies in arrival order, and a run breaks on any
  table/column-set/kind change. Poison-row isolation, OCC retry, and idempotent
  replay are unchanged (a permanent failure still falls back to record-by-record
  apply). Bumps `PLUGIN_VERSION` to `v13`.
- **CDC sink `consumer.max.poll.records` now defaults to 3000.** New
  `SinkMaxPollRecords` CFn parameter (default 3000, set in the sink worker config).
  The Kafka default (500) capped how many records reach one `put()` call — and
  thus how many the connector can batch into one ≤3000-row DSQL transaction —
  leaving the batched apply under-filled. Matching it to the transaction limit lets
  a full poll fill one round-trip.
- **CDC throughput defaults raised for large-scale sources:** `ConnectorMcuCount`
  4, `SinkTasksMax` 4, and per-table `topic.creation.default.partitions` 4, so the
  sink can consume a data topic across 4 partitions in parallel out of the box
  (effective sink concurrency is capped by the partition count). The app stack also
  now allows 8/16 vCPU task sizes.

### Fixed

- **CDC: non-GTID sources reliably fall back to file:position mode.** Debezium is
  now told to exclude all GTIDs (`gtid.source.excludes=.*`) and not filter DML on a
  missing GTID (`gtid.source.filter.dml.events=false`), so a source with
  `gtid_mode=OFF` (e.g. RDS MySQL where GTID can't be enabled) captures changes via
  binlog file:position instead of producing zero records.

### UI

- **Start CDC gives immediate feedback:** the button shows a loading state and a
  toast on click, rather than appearing unresponsive while the deploy request is
  in flight.
- **Interrupted CDC stages show a FAILED icon** once the job has ended, instead of
  remaining stuck on an in-progress spinner.

## v0.1.71

### Fixed

- **CDC: `SnapshotMode` now actually reaches the CloudFormation template.**
  v0.1.70 computed the correct mode in Python but the cdc-stack template had
  `snapshot.mode: recovery` hardcoded — the fix never reached the deployed
  connector. Added a `SnapshotMode` CFn parameter and wired it through
  `build_cdc_stack_params` / `build_cdc_infra_params`. Start CDC now also passes
  the updated template (not `UsePreviousTemplate`) so new parameters are
  recognized by stacks deployed before this version. This is the real fix for
  the "Could not find existing redo log information" connector failure.

- **CDC: source DB port is now read from the session's source config.** Previously
  always defaulted to 3306, causing connector timeout failures when the source
  runs on a non-standard port.

## v0.1.70

### Fixed

- **CDC: `snapshot.mode` now correctly uses `schema_only` for new connectors.**
  Previously hardcoded to `recovery`, which requires a pre-existing schema-history
  topic. Now `recovery` is used only when a real Full Load watermark (with binlog
  coordinates) exists; all other cases — manual start, session reset, CDC-only
  flow — use `schema_only`. Eliminates the "Could not find existing redo log
  information" connector failure.

- **CDC: pre-flight subnet NAT egress check prevents 10-minute silent failures.**
  MSK Connect assigns private IPs only — subnets without NAT gateway egress
  cannot reach Secrets Manager. Both user-supplied and auto-discovered subnets
  are now verified before deploy submission. Also re-verifies discovered subnets
  at deploy time to catch race conditions (e.g. another stack's NAT deleted
  between diagnosis and deploy).

- **CDC: prerequisites button locked during CDC deploy/start.** The Check button
  was only disabled during Full Load; now it's also disabled while a CDC stack
  operation is in flight.

## v0.1.69

### Added

- **CDC: "Fetch current position" button in Manual start-point mode.** When no
  Full Load watermark is available (CDC-only flow), the Manual start-point form
  now includes a "Fetch current position" button that queries `SHOW MASTER STATUS`
  on the source and auto-fills the GTID and binlog fields. Eliminates the need to
  manually run SQL on the source and copy-paste coordinates.

## v0.1.68

### Changed

- **Full Load: multi-process parallelism (GIL bypass).** Tables now load in
  separate OS processes via `ProcessPoolExecutor`, giving each table (or shard)
  its own Python GIL and its own CPU core. Large tables with a single integer
  primary key are automatically split into PK-range shards across multiple
  processes. All work units — whole-table workers and shard workers — share one
  bounded pool. Measured on ECS Fargate 8 vCPU:
  - 4 tables mixed (tp=8): **34,800 rows/s** at CPU 561% (was 12,277 at 110%)
  - Single 33.6M-row table sharded (tp=8): **51,000 rows/s** at CPU 777%
  - 200GB table estimate: **~2.5 hours** (was ~46 hours, **18× faster**)
  - Backward-compatible: test doubles automatically use the thread fallback.

## v0.1.67

### Changed

- **Full Load single-table throughput optimizations (GIL-aware).** Five changes
  that compound to reduce GIL hold time and network round-trips:
  1. MySQL keyset page size raised from 1,000 to 5,000 rows — 5× fewer source
     round-trips per table (the dominant bottleneck).
  2. `build_insert_statement` SQL template cached per batch shape — eliminates
     ~40,000 object allocations per batch (99.99% cache hit on large tables).
  3. `_iter_batches` byte estimation made lazy — samples the first row of each
     batch and only checks per-row near the 8 MiB budget, eliminating 90%+ of
     `_estimate_row_bytes` calls for normal-width tables.
  4. `_flatten_params` converted to list comprehension (~40% faster in CPython).
  5. `convert_row` passthrough fast path — columns that need no type conversion
     (int, varchar, numeric, text) skip `convert_value` entirely via a
     precomputed frozenset lookup.

### Fixed

- **"Retry unfinished tables" button now gives immediate visual feedback.**
  The button shows "Checking target…" with a hourglass icon and disables
  itself while probing the target, then shows a toast on retry start. Previously
  the slow probe ran without visible feedback so the UI felt unresponsive.
- **Per-object "Apply to target" in Schema Conversion now detects existing tables**
  and shows a Replace/Skip dialog (previously silent SKIP due to unwired
  existence checker; now resolved from the target inventory).
- **"Keep integer PK" renamed to "Keep source PK"** — the label was misleading
  for tables with non-integer primary keys.
- **"Apply converted to target" renamed to "Apply all to target"** — clearer.

## v0.1.66

### Changed

- **Migration overview diagram redesigned as a single unified panel.** The three
  separate bordered cards (Source / Migration Tool / Aurora DSQL) are now
  borderless column segments inside one shared surface. Status indicators use a
  lighter dot + text pattern (Cloudscape "StatusIndicator") instead of bordered
  chip badges, flow connectors are simpler dashed arrows with plain text captions,
  and the overall chrome is significantly reduced while preserving all information
  (endpoint, engine, region, connection state). Adds a reusable
  `render_status_dot` component to the design system (`ui/design.py`).

## v0.1.65

### Changed

- **Applying a single object that already exists now asks how to handle it,
  instead of silently skipping.** In Schema Conversion (Step 2), clicking a
  per-object **Apply to target** for a table that already exists on the target
  (and whose DDL you did not edit) previously just reported `SKIPPED` and left the
  target unchanged — easy to miss, and there was no way to change your mind from
  that button. It now opens a **Replace / Skip / Cancel** dialog so the choice is
  explicit at the moment you apply. This matters when you *revert* a choice — e.g.
  switch a table back from a composite key to the integer key: SKIP would have left
  the old composite table in place, whereas Replace drops and recreates it with the
  new DDL. (Editing an object's DDL, or the global REPLACE mode, still routes
  through the existing destructive-replace confirmation.)
- **The per-object Apply to target button now shows it is working.** While the
  apply runs (a target round-trip that can take a moment, or waits on the
  confirmation dialog), the button switches to a disabled, loading spinner state
  and returns to normal when the apply finishes — so a slow apply no longer looks
  like a dead click. The busy state is always cleared, even if the apply fails.

## v0.1.64

### Added

- **Opt-in per-table composite primary key (write hot-partition fix).** Aurora
  DSQL stores rows in primary-key order, so a monotonic `AUTO_INCREMENT` key
  funnels every insert into one partition — a write hot partition that caps
  throughput. Schema Conversion (Step 2) now offers a per-table **primary key**
  picker: keep the integer key (default, unchanged) or switch to a **composite
  key** that prepends a high-cardinality column you choose (e.g.
  `(customer_id, id)`) so writes spread across partitions. The source MySQL schema
  is never changed — only the DSQL target key. The tool only offers NOT NULL,
  non-key columns as the leading column, validates the result against DSQL's key
  limits (≤ 8 columns, ≤ 1 KiB), and emits a `CREATE UNIQUE INDEX ASYNC` on the
  original key so its uniqueness is preserved. A notice at selection time spells
  out the consequence: after cutover the application's queries, joins, and upserts
  must use the new composite key, and the leading column must be immutable.
  - **Full Load** loads a composite table correctly: the idempotent
    `INSERT ... ON CONFLICT` now keys on the **target** primary key (previously it
    always used the source key), so a changed key no longer mismatches the target
    constraint. Appending into an existing table whose target key differs is
    refused with a clear message (reload fresh to apply the new key first).
  - **CDC** replicates a composite table with no connector/plugin change: the
    Debezium source is re-keyed via `message.key.columns` so each change record's
    key matches the target composite key, and the sink's idempotent upsert/delete
    apply against it unchanged. CDC start refuses to proceed only if a composite
    key column was also chosen for LOB exclusion (it must be captured to build the
    key), with an actionable message.

## v0.1.63

### Changed

- **Full Load can read a large table with multiple concurrent readers (reader
  range sharding).** The single keyset reader is CPU-bound (per-row type
  conversion) and tops out near one core, so a big table's read is now optionally
  split into K disjoint primary-key ranges streamed concurrently, all feeding the
  one write pool. Off by default (`DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS=1`); only
  applies to a table with a single **integer** PK and at least
  `DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS` (default 1,000,000) estimated rows —
  composite/non-integer PKs and smaller tables always use one reader. Bounded so
  total source readers (`table_parallelism × shards`) stay within a safe ceiling.
  Sharding is **not** applied on a clean replace load (plain INSERT, no CDC), whose
  single consistent snapshot must be preserved; it is limited to the idempotent
  existing-data/CDC path where the watermark + idempotent re-load make per-shard
  snapshot skew safe. No change to resumability, OCC handling, or the write side.

## v0.1.62

### Changed

- **Full Load reads ahead of the write pool (bounded prefetch queue).** The source
  reader now fills a bounded queue from a dedicated background thread, so reading
  page N+1 overlaps writing page N instead of the two running serially. Memory
  stays bounded (the queue is capped at ~2× the write parallelism), the load order
  is unchanged (batches still map to fixed PK ranges), and the reader thread is
  joined on stop/cancel so nothing leaks. On by default; a measurement seam
  (`DSQL_MIGRATOR_FULL_LOAD_PREFETCH=0`) can disable it to reproduce the previous
  path for A/B benchmarking. No change to load correctness or resumability.

## v0.1.61

### Changed

- **Simpler Full Load progress table.** Trimmed from 9 columns to 6 so it reads at
  a glance and stops wrapping: "Rows on target" and "Source rows" are merged into
  one **Rows (target / source)** column with large counts abbreviated
  (`1.18M / 33.6M`) and the exact figures + new/already-there breakdown moved to a
  hover tooltip; the **Errors** column is folded into **Attempts** (e.g. `5 · 1 err`);
  the redundant **Complete** column is dropped (Status + Progress already show it);
  and the **Time** header no longer wraps. Small counts still show in full with
  thousands separators.

## v0.1.60

### Changed

- **Prerequisite checks can't be re-run while a Full Load is in progress.** You
  could previously go back to the Prerequisites step and click "Check" mid-load.
  It was harmless (the checks are read-only and never touch the running job — a
  fresh result only applies to the *next* run), but pointless and confusing: a
  newly-failing check would show a red "blocked" verdict while the load kept
  running, and it added avoidable read load on the source. The Check button is now
  disabled while a Full Load is IN_PROGRESS, with a short note explaining the
  checks apply to the next run and don't affect the running load — matching how
  the migration-type selector already locks during a run. Stop the load to re-run
  checks.

## v0.1.59

### Changed

- **Full Load "Failure details" is cleaner and no longer shifts on long errors.**
  Removed the per-row "Reload" button from each failure — retrying is now driven
  solely by the single "Retry unfinished tables" checklist below, so there's one
  consistent way to retry instead of two competing controls. Each failure row now
  has a stable layout (table name + wrapping error message on the left, the
  "AI Assist" action pinned to the right) so a long error message no longer pushes
  the buttons onto a second line or misaligns them between rows. Quarantined-row
  entries keep their own "Reload" (a quarantined table is DONE, not "unfinished",
  so the retry checklist doesn't cover it).

## v0.1.58

### Fixed

- **The Full Load progress table no longer jumps back to page 1 while a load is
  running.** The per-table table refreshes every ~1.5 s, and each refresh rebuilt
  it from scratch — so paging to page 2+ snapped you back to page 1 on the next
  tick. The chosen page is now preserved across refreshes (and clamped so a
  shrinking table can't strand you on a now-empty page).

## v0.1.57

### Fixed

- **Tables left PENDING by a failed run can now be retried (were stranded).** If a
  Full Load ended in failure *before* some tables were even attempted, those
  tables stayed `PENDING` (not `FAILED`) — and the recovery UI keyed only off
  `FAILED` chunks, so it showed no "Retry" action and the only escape was a full
  "Re-run Full Load". Recovery now targets every **unfinished** table (FAILED *or*
  PENDING): the retry row appears whenever a terminated run has unfinished tables,
  the button reads "Retry unfinished tables (N)", and the checklist lists each one
  with its reason (its error, or "Not loaded yet — the previous run ended first."
  for a PENDING table). Already-loaded (DONE) tables are still kept and never
  re-run needlessly. (This is the recovery path for the v0.1.56 crash: after
  updating, click Retry unfinished tables to resume the PENDING ones.)

## v0.1.56

### Fixed

- **"Drop & reload" no longer crashes the whole run with a `SchemaApplier`
  TypeError.** Choosing Drop & reload for a table whose dependent views had to be
  dropped/recreated raised `TypeError: SchemaApplier.__init__() missing 1 required
  positional argument: 'introspector'`, which aborted the entire Full Load and
  wiped the per-table progress view (showing only "Migration failed"). The
  dependent-view pre-drop/recreate now uses introspector-free DDL helpers
  (`drop_object` / `recreate_table`) instead of constructing a `SchemaApplier`
  incorrectly, so a clean reload succeeds. Additionally, the optional view
  pre-drop/recreate passes are now defensive: any unexpected failure there is
  logged and skipped rather than failing the run — so a view-handling problem can
  never again wipe your Full Load progress (a table that truly can't drop still
  surfaces as a normal per-table failure you can act on).
- **Full Load step no longer errors when CDC is live and the run is startable.**
  Fixed a `NameError` (a stale `cdc_live` reference) that broke the Full Load
  step's render in the specific case where CDC is streaming and the Start/Re-run
  button is enabled.

## v0.1.55

### Changed

- **"Retry failed tables" now lets you pick which failed tables to retry.** The
  retry dialog lists each failed table with a checkbox (all pre-checked) and its
  failure reason, so you can uncheck the ones you're not ready to retry yet (e.g.
  a source value you haven't fixed, or a dependency you haven't resolved) and
  retry only the rest. Confirming retries just the checked subset — and the
  read-only "already has data" probe and the Append-vs-Drop choice are scoped to
  that subset too. Confirm is disabled when nothing is checked. Retrying all (the
  common case) is unchanged: leave everything checked and confirm. The per-table
  "Reload" shortcut is unchanged.

## v0.1.54

### Changed

- **"Retry failed tables" and per-table "Reload" now offer the same Drop-vs-Append
  choice as Start.** Previously the choice was only made on the initial Start Full
  Load and a retry silently reused it, so you couldn't switch to a clean reload
  after a failed append (short of a full Start over). Retry and Reload now run the
  same read-only probe (scoped to just the tables being retried) and open the same
  confirm dialog, so you pick **Append** or **Drop & reload** at retry time too.
- **The Drop & reload choice now spells out that it uses your edited schema.** The
  dialog notes that a drop & reload recreates each table from your **applied Schema
  Conversion — including any edits you made there** — and rebuilds its secondary
  indexes after loading, so it's clear a schema change is honored on a clean
  reload (it already was; this just makes it visible).

## v0.1.53

### Fixed

- **Full Load now lets you choose Drop-vs-Append for tables that already have
  data — and a retry keeps that choice.** Previously, if a selected target table
  already held rows, the tool decided for you (DROP+recreate on the first run),
  and a **retry silently reverted to append**, reporting "0 new + N already there"
  over stale data — so a failed load could look clean without actually refreshing
  anything. The Start Full Load dialog now asks, once for the run: **Append**
  (keep existing rows, load only the missing ones — idempotent, the default) or
  **Drop & reload** (DROP and recreate each table first, for a clean load). The
  choice is stored, so **retry and per-table Reload follow the same behavior**
  instead of quietly changing it.
- **"Drop & reload" no longer fails when a view depends on the table.** A
  dependent view (e.g. `customer_order_summary`) used to block `DROP TABLE` with
  `DependentObjectsStillExist`, leaving the old rows in place. The drop path now
  drops the dependent views first (a run-level pre-pass, since a view can span
  several tables loaded in parallel) and **recreates them after the load**, so a
  clean reload succeeds and your views survive — without a blunt `DROP … CASCADE`.
  Suppressed while CDC is streaming (a DROP would race the live sink).

## v0.1.52

### Added

- **AI Assist on each failed Full Load table.** Every table in the Full Load
  "Failure details" list now has an "AI Assist" button next to "Reload". It opens
  the AI chat drawer and explains that specific failure's likely cause and how to
  fix it — grounded in the actual error text (e.g. a `DependentObjectsStillExist`
  drop conflict from a dependent view, or a transient `InternalError_: server
  unavailable`) **and in this migration's situation**: the migration type
  (Full-Load-only vs Full Load + CDC), whether the table was a DROP+recreate of an
  existing target, and whether CDC is already streaming. So the guidance is
  specific to your migration, not generic, and points at the right recovery
  (fix a schema dependency, fix a source value, or just Reload a transient). Opt-in
  — the button is enabled only when AI Assist is turned on at Connect; otherwise it
  shows a disabled affordance pointing there. Reuses the existing chat-drawer /
  Bedrock stack (no new credentials path).

## v0.1.51

### Fixed

- **Prerequisites section no longer collapses when you click "Check" in a
  reconnected session.** After an app restart you may need to (re-)run the
  prerequisite checks. Expanding the Prerequisites section and clicking Check
  used to collapse it immediately — the click triggers a re-render, and the
  section only stayed open when it was the "active" sub-step, which after a
  reconnect is a later step. It now stays expanded while it is the actionable
  section (its checks are running, or it still blocks the run), so the running
  spinner and the results remain visible.

## v0.1.50

### Changed

- **Schema Conversion object browser matches the "Tables to migrate" styling.**
  The Step 2 source/target browsers now use the same look as the Step 3 table
  picker: white, bordered scroll panels and connector-less trees. Each source
  table leaf shows the same primary-key indicator (green check when the table has
  a primary key, amber warning when it has none, which Aurora DSQL requires) with
  a legend under the filter. Views/triggers/routines carry no PK indicator (they
  have no primary key). Selection and DDL-generation behavior is unchanged.

## v0.1.49

### Fixed

- **"Tables to migrate" filter now works, and the primary-key icons have a
  legend.** The name filter box above the table tree rendered but did nothing —
  it wasn't bound to the tree — so typing filtered nothing; it's now wired to the
  tree's filter (typing narrows to matching table leaves). Added a small legend
  under the header explaining the per-table icons: a green check means the table
  has a primary key, an amber warning means it has none (which Aurora DSQL
  requires).

## v0.1.48

### Changed

- **"Tables to migrate" picker: back to the schema tree, with the modern
  styling kept.** Reverted the flat data table (v0.1.47) to the schema → Tables →
  leaf object-browser tree, but wrapped in the same AWS/Cloudscape styling — a
  name filter, Select all / Unselect all, and a live "N of M selected" counter
  above a white, bordered scroll panel. Each table leaf now shows a small primary-
  key indicator (a green check, or an amber warning when the table has no primary
  key, which Aurora DSQL requires); other metadata columns from the table view
  were dropped to keep the tree light. The PK indicator is a client-side Quasar
  slot, so it adds no per-node work. Selection behavior and the locked (dimmed,
  non-interactive) state are unchanged.

## v0.1.47

### Changed

- **"Tables to migrate" picker restyled as a compact AWS Console (Cloudscape)
  data table.** Step 3's table picker was a schema → Tables → leaf tree with a
  checkbox at every level. It's now a flat, sortable data table with a single
  checkbox column and one row per table, showing more at a glance: schema,
  column count, whether the table has a primary key (a green check, or an amber
  warning when absent since DSQL requires one), secondary-index count, and a
  "exists"/"new" target status chip. A name filter and a live "N of M selected"
  counter sit above it. Fewer checkboxes, higher information density, same
  selection behavior — the ticked set still drives Full Load / CDC / prerequisite
  checks, and the picker still locks (dimmed, non-interactive) once checks have
  run or CDC is live.

## v0.1.46

### Changed

- **Clearer "re-run prerequisites" message after an app restart.** If you had
  already cleared the Data Migration prerequisites but hadn't started the Full
  Load yet, an app restart used to gate the run behind the same blunt "Run the
  prerequisite checks first" prompt shown to a first-time user — reading as if
  your progress was lost. The checks still must re-run (they're read-only, and
  the source connection is re-established on reconnect so a stale result can't be
  trusted), but the message now names the situation: "Reconnected — re-run the
  prerequisite checks to resume. They're read-only and quick; your progress
  wasn't lost, but the results aren't kept across an app restart." A genuine
  first-time user still sees the original prompt. Detected from the persisted
  active sub-step (only reachable once checks passed), so the two cases can't be
  confused.

## v0.1.45

### Changed

- **Performance-tuning control restyled as a compact AWS Console (Cloudscape)
  form.** The sidebar "Performance tuning" panel no longer stacks four bare
  number inputs. It now opens with a one-line info Alert (applies to the next
  run; live/app-wide, resets on restart; connections ≈ tables × batches), then
  lays the knobs out as grouped form fields under "Full Load" / "Validation"
  section subheaders. Each knob is a single dense row — label, an info glyph
  whose tooltip carries the longer description, the allowed range, and a
  bounded number input — so the whole panel stays tight in the narrow sidebar.
  The knob metadata (group / label / description / range) all lives in
  `config.py` so the UI and the validation messages share one source of truth.
  No behavior change to what the knobs do.

## v0.1.44

### Fixed

- **"Start Full Load" can't be double-clicked into two confirm dialogs.** Opening
  the confirm runs a ~1–2 s read-only probe (which target tables already hold data)
  before the dialog appears; a fast double-click used to open the dialog twice. The
  handler now drops a second click while the probe is in flight (re-entrancy guard)
  and shows a busy cue — the clicked button disables and reads "Checking…" with an
  hourglass icon, restoring when the dialog opens. Applies to both the initial
  Start and the terminal Re-run buttons.

## v0.1.43

### Changed

- **Deploy-log timestamps now show the `UTC` zone.** Each CDC deploy/teardown log
  line reads `HH:MM:SS UTC - …` (was zone-less `HH:MM:SS - …`), making it
  unambiguous and consistent with the downloaded activity log, CloudWatch, and
  CloudFormation events — all UTC.

## v0.1.42

### Fixed

- **CDC stack-name field alignment.** The fixed `mysql-dsql-cdc-` prefix is now
  rendered inside the input via Quasar's `prefix` prop (baseline-aligned with the
  typed suffix, like `$` before an amount) instead of as a separate left label that
  floated out of line with the field's own label. A one-line helper below shows the
  resulting full stack name.

## v0.1.41

### Changed

- **CDC stack-name field is now suffix-only, so a custom name can't be silently
  rejected.** The mandatory `mysql-dsql-cdc-` prefix is shown as a fixed, read-only
  addon and you edit only the suffix (e.g. `orders` → `mysql-dsql-cdc-orders`).
  Previously, typing a name without the prefix (e.g. `abcde`) was rejected and
  reverted to `mysql-dsql-cdc-stack` with a warning — confusing, since the prefix is
  required by the deploy role's IAM scope. Now `abcde` simply becomes the valid
  `mysql-dsql-cdc-abcde`; only an illegal-charset suffix is rejected.

## v0.1.40

### Changed

- **"Start over" shows a "Checking…" busy state while it probes CDC.** Opening
  Start over runs a ~1–2 s read-only AWS probe (to decide whether to offer the CDC
  stop/delete tiles); the button now disables and swaps to "Checking…" with an
  hourglass icon during that probe, then restores when the dialog opens — a visible
  cue that also prevents a double-open. (Label/icon swap, matching the app's busy
  idiom, not Quasar's `loading` prop which artifacts on flat buttons.)

## v0.1.39

### Fixed

- **Start over no longer warns about "CDC keeps billing" when CDC is already gone.**
  If a fresh live probe confirms no CDC infrastructure exists (e.g. you just
  finished deleting the stack), the Start-over dialog no longer shows the
  "resetting does not delete CDC infrastructure — MSK/NAT keep billing" caution,
  which was misleading about infra that is already torn down. The warning still
  appears when the probe is inconclusive (a hedge) and, of course, when CDC really
  is deployed (that path shows the stop/delete tiles instead).

## v0.1.38

### Changed

- **The CDC card shows a clear "being deleted" state during teardown.** While the
  cdc-stack is `DELETE_IN_PROGRESS`, the pipeline card previously read as a vague
  "Busy" / "cdc-stack needs cleanup — wait for the current operation". It now shows
  a **"Deleting…"** badge and a reassuring notice — *"CDC infrastructure is being
  deleted (~15–25 min — the in-VPC Lambda's network interfaces take time to detach);
  MSK / NAT billing stops once it completes"* — and keeps polling so it flips to
  "Not deployed" on its own. A settled-but-stuck stack (`ROLLBACK_COMPLETE` /
  `DELETE_FAILED`) still shows the "needs cleanup — delete then redeploy" guidance.
  (New pure helper `cdc_unstable_message` drives badge + notice from one place.)

## v0.1.37

### Fixed

- **"Start over" no longer races an in-flight CDC teardown.** After choosing to
  stop/delete the CDC pipeline during Start over, the CloudFormation stack is
  `DELETE_IN_PROGRESS` for ~15–25 min — during which the header "Start over" button
  stayed clickable, and because the reset had already wiped the session, a second
  attempt no longer recognized the running teardown (confusing, and for a custom
  stack name a risk of orphaned MSK/NAT billing). Start over is now **blocked while
  a CDC stop/delete is actually in flight**: the dialog explains that a teardown is
  running and offers only Close (no RESET). Detection is narrow — a live
  `*_IN_PROGRESS` stack status or a PENDING/RUNNING stop/delete job — so a settled
  but stuck stack (`ROLLBACK_COMPLETE` / `DELETE_FAILED`) can still be reset and
  cleaned up. The `run_cdc_delete` already-deleting backstop is unchanged.

## v0.1.36

### Added

- **Runtime performance tuning from the UI.** A new **Performance tuning** control
  in the sidebar footer (next to Diagnostics) lets an operator retune the four Full
  Load / Validation parallelism knobs (`FULL_LOAD_TABLE_PARALLELISM`,
  `FULL_LOAD_BATCH_PARALLELISM`, `FULL_LOAD_BATCH_ROWS`, `VALIDATE_MAX_WORKERS`)
  **between runs without a redeploy or restart** — the loader and validator re-read
  the config on every run, so a value set here applies to the next Full Load /
  Validation. Each field is bounded by the same limits as the config (single source
  of truth), app-wide (single-task app), and resets to the deploy/startup values on
  restart. Set the task-definition `environment` for values you want to persist;
  use this control to experiment live.

## v0.1.35

### Fixed

- **AI assist now deploys in non-US regions (e.g. Seoul / ap-northeast-2).** The
  `BedrockModelId` deploy parameter accepted only `us.` inference profiles, and the
  task-role `bedrock:InvokeModel` scope was derived by splitting on `"us."` and
  hard-coded to the US member regions (us-east-1/2, us-west-2) — so AI assist could
  not be enabled outside the US (a non-`us.` id was rejected at parameter
  validation, and the derived IAM scope was wrong for other geographies). The
  parameter now also offers `global.` profiles (portable to any region), the
  foundation-model id is derived by splitting on `"anthropic."` (present in every
  `us.`/`global.`/`apac.` profile id), and the foundation-model ARN is scoped
  region-agnostically (region `*`, exact model id) instead of enumerating per-geo
  member regions. Still least-privilege — the `*` is only the region field; the
  model id stays exact and the resource is never a blanket `*`.
- **CDC deploy no longer opens `0.0.0.0/0` egress to the source DB by default.** At
  CDC-infra deploy the tool now auto-discovers the source DB's security group (RDS
  `DescribeDBInstances`, read-only) and scopes the connector's egress-to-source
  rule to it, so the stack stops falling back to an open source-port egress on
  every UI deploy. Best effort — a non-RDS host or missing `rds:DescribeDBInstances`
  leaves it empty (documented fallback, unchanged).
- **CDC sink log corrected + dead in-memory S3 CSV export removed.** The sink's
  `start()` advisory now states a permanently-rejected record with no DLQ **fails
  the task** (the actual behavior), not "logged and skipped"; and an unreachable,
  whole-file-in-memory S3 CSV export path was deleted (the shipping path streams
  page-bounded). No behavior change to the live data path.

### Changed

- **Default container image bumped to `0.1.34`.** The app-stack default
  `ContainerImageUri` still pointed at `0.1.31` while the shipped release was newer,
  so a fresh deploy ran a stale image.

### Docs

- **Japanese (日本語) manual + docs**, with a 3-way English / 한국어 / 日本語
  language switcher across the manual, README, deployment guide, and changelog.
- **Natural-Korean pass** over the Korean manual (fluency + terminology
  consistency), a rewritten testing chapter, and a new measured-results section in
  the performance chapter.
- **Architecture diagrams as PNGs** embedded in the README (the full topology is
  click-to-enlarge); the editable `.drawio` sources are no longer shared.
- **Deployment guide**: the AWS CLI example now enables AI assist inline
  (`EnableAiAssist` / `BedrockRegion` / `BedrockModelId`); Apache-2.0 `LICENSE`
  copyright line filled; internal working documents removed from the repo.

## v0.1.34

### Added

- **AI DBA query tuning in the Query Playground.** After a converted `SELECT`
  passes "Test on target", a new **Tune with AI DBA** action opens the shared AI
  chat drawer and rewrites the query for Aurora DSQL efficiency — grounded on the
  query's REAL captured EXPLAIN plan and DPU cost, and on Aurora DSQL's own
  execution model (the primary key *is* the table, filter pushdown through the
  three filter layers, `Full Scan` vs. `Index`/`Index Only Scan`, and DPU as the
  cost unit). It explains what it changed and why it is cheaper on DSQL, and is
  explicitly steered away from vanilla-PostgreSQL tuning advice that does not
  apply to DSQL. Each proposed rewrite has a **Test rewrite on target** action
  that re-runs it read-only on the target and has the AI report the measured
  before/after DPU improvement in the same chat. Opt-in (AI off by default),
  advisory only — nothing is auto-applied, and the measured DPU (not the model's
  prose) is the proof of improvement.

## v0.1.33

### Fixed

- **"Start over" now reliably offers to tear down a deployed CDC pipeline,
  regardless of which step you were on.** The reset dialog decides whether to show
  the stop/delete choices from the detected CDC deployment, but that detection was
  only refreshed when the CDC step had been opened — so starting over from another
  step (or a reconnected session) could fall back to a passive "resetting does not
  delete CDC infrastructure" warning with no teardown action. Start over now runs a
  read-only AWS probe when it opens, so it reflects the real deployed state.
- **Teardown is offered for CDC resources in ANY state, not just running ones.** A
  connector that is failed/still provisioning, a stuck or rolled-back cdc-stack, or
  an infrastructure-only stack (the MSK cluster + NAT with no connectors yet) all
  still bill — but were not always offered for teardown. Existence, not health, now
  drives the offer, matching the CDC step (which already exposes Delete for a
  stuck/unstable stack).
- **A custom cdc-stack name is named explicitly in the Start-over warning.** If you
  deployed CDC under a custom stack name (the CDC step's "Advanced — CDC stack
  name", e.g. for a second parallel migration), a fresh session cannot re-discover
  it (it reverts to the default name). The warning now names the exact stack so you
  know precisely what to delete (in the tool or the AWS console).
- **Deleting CDC infrastructure no longer submits a doomed delete against a stack
  that is mid-operation.** If a CloudFormation operation was still running, the
  delete raced it and could fail opaquely. Delete now stops with a clear
  wait-and-retry message when an operation is in flight (and, if a deletion is
  already underway, simply waits for it) — while still deleting stable, failed, and
  rolled-back stacks as before.

## v0.1.32

### Fixed

- **Validation checksums no longer false-mismatch on NULL-bearing rows.** The
  per-row checksum joined columns with a `'\0'` NUL sentinel for NULLs, but that
  byte renders differently on each engine (a single NUL on MySQL vs. the two-char
  string `0x5C30` under PostgreSQL's `standard_conforming_strings`, DSQL's
  default), so any row containing a NULL hashed differently on source and target
  and was reported as a spurious difference. The sentinel is now the plain text
  `<NULL>` (also avoiding NUL, which is invalid in PG text), so identical data
  hashes identically on both engines.
- **Validation checksums now agree on binary and BIT columns.** MySQL rendered
  `BINARY`/`VARBINARY`/`BLOB` (and spatial) as raw bytes while the target side
  used hex, and `BIT` was compared as raw bits vs. an integer — both produced a
  guaranteed cross-engine mismatch even when the stored data was identical. Binary
  columns are now hashed as lower-case hex on both sides (`LOWER(HEX(…))` on MySQL
  to match PG `encode(…, 'hex')`), and `BIT(n)` is compared as its integer value
  (`CAST(… AS UNSIGNED)` vs. `::text`).
- **Out-of-range MySQL `TIME` values now fail loudly instead of corrupting the
  target column.** MySQL `TIME` spans `-838:59:59..838:59:59`, but a DSQL `time`
  column only holds `00:00:00..23:59:59.999999`. A value outside that range had no
  `time` representation and would silently bind as an interval (or a non-time text
  cell), corrupting the column. Full Load now raises a clear `ValueConversionError`
  naming the column and value and pointing to the fix (remap the target type to
  `interval`/`text` in Schema Conversion, or restrict the source values), matching
  the existing `TINYINT(1)`-out-of-range guard — data is never silently mangled.

## v0.1.31

### Fixed

- **Validation is now reachable during a CDC-only run (no more "Complete Data
  Migration first").** The Data Migration step only ever reached DONE via a
  finished Full Load, so a CDC-only plan — or a reconnected session that never ran
  Full Load locally — left Validation permanently locked even though CDC was
  actively replicating to the target. When CDC is streaming, the Data Migration
  step is now treated as DONE for downstream gating (new pure
  `data_migration_step_after_cdc`; only promotes, never downgrades a terminal
  DONE/FAILED).

### Known issues

- **Object browser can still show "everything selected" (locked) for a
  reconnected CDC-only session.** When CDC is live but this session has no Full
  Load watermark and no locally-confirmed table selection (e.g. reconnected after
  starting fresh from Connect), the tool cannot resolve the real streamed table set
  from local state and the locked browser falls back to the target-existing
  default. Fully fixing this needs reading the deployed connector's actual
  table set (`describe_connector`) off the event loop during CDC status discovery —
  tracked as a follow-up. (v0.1.30 already fixed the common case where the
  watermark/selection is known.)

## v0.1.30

### Fixed

- **Data Migration object browser no longer shows "everything selected" while CDC
  is live.** When the picker is locked (CDC streaming), a reconnect fell back to the
  generic "everything on the target" default and ticked every table — misrepresenting
  what CDC is actually replicating (and frozen, so it couldn't be corrected). The
  locked browser now reflects the REAL streamed set (the CDC connectors' table set,
  from the Full Load watermark / confirmed selection) instead of the target-existing
  default.
- **Schema Conversion "Apply to target" is now blocked while CDC is running.**
  Applying schema during live CDC — especially a destructive REPLACE, which DROPs and
  recreates the table — would corrupt or truncate the tables the sink is actively
  writing (Debezium does not propagate DDL), risking data loss / a broken pipeline.
  Both the bulk apply and the per-object inline apply now stop with a warning telling
  the operator to stop CDC first. (Guarded by a CDC-status probe injected from the
  app; when unavailable, apply is unaffected.)

## v0.1.29

### Added / Changed

- **Schema Conversion: one-click copy for the Source and Target DDL.** Each DDL
  block now has a copy-to-clipboard icon — on the side-by-side Source/Target diff
  (per-side, in the header bar) and on the non-editable view/trigger/routine preview
  (next to each "Source DDL" / "Target DDL" label). A positive toast confirms the
  copy; if the browser clipboard is unavailable (e.g. non-HTTPS) it falls back to a
  calm "select and copy from the block" note.

## v0.1.28

### Fixed

- **CDC teardown auto-recovers a `DELETE_FAILED` stack blocked by the offset-seeder
  Lambda's leftover ENIs.** The offset-seeder runs in-VPC (it must — MSK Serverless
  bootstrap is VPC-private, so nothing outside the VPC can produce the gapless seed
  record), and a VPC Lambda leaves AWS-managed hyperplane ENIs behind that AWS
  reclaims only asynchronously (minutes to tens of minutes). While they linger,
  deleting the connector subnets / security group fails and the whole stack lands
  in `DELETE_FAILED` — previously a dead-end that required manually deleting the
  ENIs and re-running delete-stack from the CLI (hit repeatedly this session), while
  MSK/NAT kept billing. `run_cdc_delete` now detects `DELETE_FAILED`, deletes the
  leftover *detached* (`available`) ENIs pinning the failed subnets/SG, and
  re-issues the delete (retaining anything still stuck) so teardown completes.
  In-use ENIs (still being reclaimed) are left alone; best-effort throughout.
  (This is the practical resolution of the offset-seeder ENI known-issue: the
  Lambda cannot move out of the VPC, so the tool now heals the teardown instead.)

## v0.1.27

### Fixed

- **CDC deploy auto-recovers a wedged `UPDATE_ROLLBACK_FAILED` cdc-stack instead of
  dead-ending.** A connector `UpdateConnector` that fails leaves the connector
  not-RUNNING, and CloudFormation's own rollback then also fails on that resource
  ("only valid for RUNNING"), parking the stack in `UPDATE_ROLLBACK_FAILED` — a
  state from which no further update can be submitted (previously it required a
  manual `continue-update-rollback` from the CLI). `discover_stack` now detects
  that state and continues the rollback while skipping the stuck resource(s), so
  the stack returns to `UPDATE_ROLLBACK_COMPLETE` and the next Start/Retry proceeds.
  Best-effort: if the recovery call itself errors, the normal "not a stable state"
  error is surfaced.

## v0.1.26

### Fixed

- **CDC UI: surface the "no tables selected" guard, and stop retries snapping back
  to Prerequisites.** Following the v0.1.25 backend guard, the CDC step now shows a
  clear "select at least one table" notice (instead of the config preview crashing
  or a deploy failing minutes later at connector-create), and Start CDC blocks with
  the same message before submitting a job. The early "provision infrastructure"
  deploy still allows an empty selection (`build_sink_config(..., allow_empty=True)`)
  because it creates no connector yet — `SinkTopics` is filled at Start CDC.
- **CDC sub-step no longer collapses to Prerequisites on a retry / re-render once
  connectors are deployed.** The active-sub-step resolver had nothing persisting
  "cdc", so any re-render (a CDC retry, a reconnect) fell back to
  full_load/prerequisites and yanked the user off the live CDC view. When the plan
  includes CDC and connectors exist, the CDC sub-step is now pinned and persisted.

## v0.1.25

### Fixed

- **CDC start now fails fast when no tables are selected, instead of deploying a
  broken sink.** `build_sink_config` raises if the table list is empty: a Kafka
  Connect sink requires a non-empty topic list, so an empty selection produced
  `SinkTopics=""` and MSK Connect rejected the connector at `POST /connectors`
  with an opaque HTTP 400 minutes into the deploy (see v0.1.24 notes). The guard
  turns that into an early, actionable error ("select at least one table") before
  any slow/billable deploy is attempted. (The *source* config is unchanged — an
  empty `table.include.list` is valid there and means "all tables".)

## v0.1.24

### Fixed

- **CDC connector deploy: complete the CdcDeployRole / task-role IAM so a connector
  actually reaches RUNNING.** Creating an MSK Connect connector exercises a chain of
  permissions that were incrementally missing; each one failed the connector CREATE
  (or left the UI stuck) until added. Verified end to end against a live cdc-stack —
  the Debezium source connector now reaches RUNNING. The additions:
  - `ec2:CreateNetworkInterface` / `DescribeNetworkInterfaces` / `DeleteNetworkInterface`
    on **CdcDeployRole** — MSK Connect places the connector's ENIs using the *caller's*
    credentials (confirmed via CloudTrail: `CreateNetworkInterface` invoked by
    `kafkaconnect.amazonaws.com` but authorized against the deploy role), not the
    connector's ServiceExecutionRole or the MSK Connect service-linked role. (The ENI
    grant mistakenly added to the cdc-stack `ConnectorExecutionRole` was removed —
    the service execution role does not need it.)
  - CloudWatch Logs *delivery* actions (`logs:CreateLogDelivery`, `ListLogDeliveries`,
    `PutResourcePolicy`, …) on CdcDeployRole — the connector enables CloudWatch worker-
    log delivery, set up via the vended-logs delivery API using the deploy role; without
    them the connector went to FAILED with `InvalidInput.WorkerLogsError` and no worker
    logs were ever written.
  - `kafkaconnect:DescribeConnectorOperation` / `ListConnectorOperations` on
    CdcDeployRole, scoped to **both** the `connector/*` and `connector-operation/*`
    ARN shapes — UpdateConnector is asynchronous and its poll is authorized against
    either ARN; a CDC retry rolled the stack back without both.
  - `kafkaconnect:ListConnectors` / `DescribeConnector` on the **task role** itself —
    the app polls connector state to drive the CDC UI (and to advance from the source
    pass to the sink pass). Without it the AccessDenied was silently swallowed and a
    connector that was actually RUNNING showed "creating…" forever.
- **DSQL sink connector reaches RUNNING — the full source→MSK→sink→DSQL pipeline is
  now verified end to end.** The sink had been failing `POST /connectors` with HTTP
  400 once IAM/infra was complete; root cause was an **empty `SinkTopics`** parameter
  (a Kafka Connect sink requires `topics`/`topics.regex`, so a blank value is
  rejected at registration). `SinkTopics` was empty because the two-pass Start never
  populated it (see the UI known-issue below); with it set to
  `<TopicPrefix>.<db>.<table>` the sink connector creates and runs.

### Known issues

- **UI: "Retry CDC" can reset the view to Prerequisites without running the deploy,**
  the source→sink two-pass does not resume after a long stack cleanup, and a Start
  that skips table selection leaves `SinkTopics`/`TableIncludeList` empty (the source
  tolerates it — captures all tables — but the sink then fails `POST /connectors`
  with HTTP 400). A follow-up UX/guardrail pass should block a CDC start when no
  tables are selected and surface the empty-topics condition before deploy rather
  than at connector-create time.
  _Update: the empty-table start is now blocked and the CDC view is kept on retry
  (v0.1.26); the two-pass resume after a long cleanup is the remaining piece._

## v0.1.23

### Added / Changed

- **The "before you start CDC" notice is friendlier and better-timed.** It now
  shows which tables will stream right at the Start button (e.g. "Will stream 3
  tables: …"), so "finalize your selection" is verifiable at a glance instead of
  asking the user to scroll up. The MSK-capacity caution is a calm info tip on the
  first start after a fresh deploy (the happy path — no alarm), and only escalates
  to a warning once connectors have actually existed before (a prior start/stop or
  a restored run), which is when repeated create/delete really begins consuming
  MSK's non-reclaimed capacity. Wording is plain-language ("MSK's limited capacity
  that isn't freed up again") instead of "partition quota … exhaust … force a full
  teardown".

## v0.1.22

### Fixed

- **CDC connector deploy no longer fails with "Access denied for operation
  'AWS::KafkaConnect::Connector'".** `kafkaconnect:CreateConnector` has no
  resource-level support (the connector ARN doesn't exist at create time), but the
  CdcDeployRole scoped it to a `connector/mysql-dsql-cdc-*` ARN, so the
  DebeziumSourceConnector CREATE was denied. It (plus create-time `TagResource`) is
  now granted on `Resource: "*"`, like the sibling CreateCustomPlugin /
  CreateWorkerConfiguration; the other connector operations stay scoped.
- **CDC connector deploy no longer fails with "not authorized to perform
  ec2:CreateNetworkInterface".** MSK Connect assumes the connector's
  ServiceExecutionRole to place the connector's ENIs in the connector subnets, but
  that role (`ConnectorExecutionRole` in cdc-stack) lacked the EC2 network-interface
  permissions. Added the MSK-Connect `EC2NetworkAccess` set
  (`ec2:CreateNetworkInterface` / `DescribeNetworkInterfaces` / `DeleteNetworkInterface`
  + attach/detach/permission, `Resource: "*"`), so the connector can create/clean up
  its ENIs. (These two were latent — earlier CDC failures stopped before the connector
  CREATE stage, so the connector had never actually been created before.)

### Added / Changed

- **After a Full-load-only run completes, the Full Load step now suggests CDC.** A
  Full-load-only migration has no CDC phase (no "Continue to CDC" button), so when
  it finishes an info notice explains how to add continuous replication: change the
  migration type to "CDC only" (streams from this Full Load's watermark onto the
  already-loaded target, no re-snapshot), noting the CDC infrastructure may need
  deploying first.

## v0.1.21

### Added / Changed

- **Migration Plan now asks a single "Include CDC?" question instead of the full
  three-way migration-type tiles.** The step's only durable effect is whether CDC
  streaming infrastructure (MSK, ~15-20 min) is provisioned early, so it asks
  exactly that (Yes / No) rather than overstating the commitment — the type is
  freely changeable on Data Migration, and Full Load always runs. No →
  `FULL_LOAD_ONLY`, Yes → `FULL_LOAD_AND_CDC`; the finer Full Load + CDC vs
  CDC-only choice stays on the Data Migration step (re-selecting Yes no longer
  clobbers a CDC-only choice). The underlying `migration_type` enum, sub-steps,
  prerequisites, and session snapshots are unchanged.
- **The "Migration type:" banner is hidden on the Migration Plan step** (still
  shown on every later step for continuity). On the plan step the "Include CDC?"
  control is the source of truth, so a three-value banner ("Full load + CDC")
  above the two-value decision was redundant and read as conflicting.

## v0.1.20

### Fixed

- **Aurora DSQL connection no longer times out on an IPv4-only Fargate task.** The
  DSQL cluster endpoint is dual-stack (DNS returns both an A and an AAAA record),
  but a Fargate task on an IPv4-only subnet/ENI (no IPv6 CIDR, no IPv6 SG egress)
  has no route to the IPv6 address. glibc could return the AAAA first, so the
  driver (psycopg/libpq) blocked on the unreachable IPv6 until `connect_timeout`,
  surfacing in the UI as "Connection failed: connection timeout expired" even
  though IPv4:5432 was reachable. The container image now prefers IPv4 for all
  outbound name resolution (`/etc/gai.conf`: `precedence ::ffff:0:0/96 100`), so
  `getaddrinfo` returns the reachable IPv4 address first and the connection
  succeeds. Harmless on a genuine dual-stack task (IPv4 is simply tried first).
- **CDC source-secret re-provisioning no longer fails with AccessDenied after a
  teardown.** The task role's `provision-cdc-source-secret` policy was missing
  `secretsmanager:RestoreSecret`, but the upsert restores a same-named secret that a
  prior teardown scheduled for deletion (recovery window) before writing the new
  value. Re-provisioning the CDC source secret after a delete now succeeds; the
  action stays scoped to the `mysql-dsql-migrator/cdc/*` prefix.

### Added / Changed

- **Deploy guide + stack-details form clarifications.** "Specify stack details"
  now leads with a required-fields table and a one-line self-signed certificate
  command; the desktop-browser access combo (`AlbScheme=internet-facing` + public
  `AlbSubnetIds` + `AllowedIngressCidr=<your-ip>/32`) is called out; and
  `HttpsEgressCidr` is documented as "keep the `0.0.0.0/0` default" (tighten only
  with PrivateLink). `ServiceSubnetIds` guidance notes you may reuse the ALB
  subnets + `AssignPublicIp=ENABLED` when the VPC has no private/NAT subnets.

## v0.1.19

### Fixed

- **Validation no longer shows a completed run as "in progress" (then "not
  started" on refresh).** The IN_PROGRESS→DONE flip is driven by a poll timer that
  only runs on the Validation screen, so navigating away mid-run (e.g. to Data
  Migration) left the step stuck IN_PROGRESS after the job finished, and the
  orphaned-status reconcile then discarded the completed report as "not started".
  Now, when a run actually finished (a report exists) but the step is a stale
  IN_PROGRESS with no live job, it reconciles to **DONE** and shows the report.

### Added / Changed

- **CDC lifecycle + connector state-transition activity logging.** Control-plane
  actions (deploy / start / stop / delete CDC infrastructure) and connector
  RUNNING/FAILED transitions are now appended to the activity log as discrete
  milestones (de-duplicated; continuous lag/throughput stays in the live panel, not
  the log).
- **Cut over: the "Steps to cut over" 1–6 runbook is larger and easier to read**
  (the critical guidance was too small) — scoped to the cut-over runbook only.
- **Deploy guide: a complete teardown order.** The Teardown section now lists the
  full decommission sequence — remove the costly **cdc-stack** first (via "Start
  over → Delete all CDC infrastructure", or a manual `delete-stack`), then the
  app-stack, then the build-stack, and verify no `mysql-dsql-*` stacks / Route 53
  records / build bucket remain — so no resources or cost are left behind.

## v0.1.18

### Fixed

- **A Full Load re-run now drops + recreates the confirmed tables before CDC has
  started, even in the "Full load + CDC" pattern.** The DROP+recreate was disabled
  whenever the pattern was Full-load-+-CDC (so a "Re-run all tables" before CDC
  started merged idempotently instead of reloading fresh, leaving prior rows as
  "already there"). The suppression is now gated on CDC **actually streaming**: a
  re-run before CDC starts drops + recreates the confirmed tables (clean reload),
  and only an actively-streaming CDC pipeline forces the safe idempotent
  `SKIP_EXISTING` load (no DROP) to avoid racing the live sink. The Start-Full-Load
  confirmation only shows the "will be DROPPED" warning when the drop will actually
  happen (CDC not live). (Re-loading without a DROP never duplicates rows — it is
  `INSERT ... ON CONFLICT (PK) DO NOTHING` — but it could leave rows deleted from
  the source; a clean reload removes that ambiguity.)

## v0.1.17

### Fixed

- **The "Start / Re-run Full Load" confirmation dialog no longer vanishes after a
  few seconds.** It was built inside the periodically re-rendered content and
  opened via a one-shot flag, so the ~1.5 s progress-poll re-render tore it down
  right after it appeared. It is now created and opened in the top-level client
  context on demand, so it stays up until you Confirm or Cancel.

## v0.1.16

### Fixed

- **A Full Load re-run no longer reverts a customized target schema.** The
  per-object **edited target DDL** (e.g. a `TINYINT(1)` → `smallint` remap) is now
  persisted in the durable session snapshot and restored on reconnect/restart.
  Previously the edit lived only in memory, so after a restart a "Re-run all
  tables" recreated the table from the deterministic conversion (e.g. reverting
  `smallint` back to `boolean`) and the out-of-range value failed to load again.
  The re-run's DROP+recreate now uses the customized DDL.

> Note: restoration matches by session id, so set `DSQL_MIGRATOR_STORAGE_SECRET`
> to keep the session (and its edits) stable across restarts. A container
> redeploy uses fresh ephemeral storage, so re-apply the edit after one.

## v0.1.15

### Fixed

- **Schema Conversion: "Apply to target" now reliably shows its REPLACE confirmation.**
  The confirmation dialog was built inside the per-object editor's (nested) slot, so
  it often never rendered as a page overlay — the button looked unresponsive. It is
  now created in the top-level client context and always appears.
- **Schema Conversion: a slow apply no longer crashes with "parent slot deleted".**
  Post-await UI feedback (notify / refresh) now re-enters the originating client and
  is best-effort, so a slot torn down during a slow apply can't raise.
- **The UI version (top-right) now reflects the real released version.** `__version__`
  is read from the installed package metadata instead of a hardcoded value, so each
  built image shows its true version.

### Added / Changed

- **Schema Conversion & Data Migration: Select all / Unselect all** in both object
  browsers for fast bulk selection.
- **Schema Conversion: "Generate DDL for selected" locks after generating** and
  re-enables after "Reset all", so a regeneration is always obvious (a second click
  no longer silently re-runs the same scope).
- **Data Migration: clearer pre-selection caption** — states how many tables are
  pre-selected and why (already present on the target), with the Select all/Unselect
  all controls.
- **Quarantined rows are reframed, not treated as a table failure.** A table that
  loaded but had to permanently drop a row a hard DSQL limit rejects (e.g. a value
  over the ~1 MiB per-value limit) is shown as "Done — quarantined" (amber), separate
  from real, retryable failures (red).
- **Per-table Reload.** Re-run Full Load for exactly one table (even a DONE one) —
  e.g. after fixing an oversized source value so a previously-quarantined row loads —
  keeping the other tables as-is.
- **Accept quarantined rows & continue (CDC override).** When a Full Load is
  incomplete ONLY because of permanently-quarantined rows, you can acknowledge the gap
  and unblock CDC without re-running; the gap is still reported in Validation. A
  retryable real failure still blocks (the override can never mask a recoverable
  failure).

## v0.1.14

### Fixed

- **Schema Conversion: an edit now reliably applies via REPLACE (it was sometimes
  still skipped).** v0.1.13 gated the auto-REPLACE on a UI-side existence check that
  could be stale or unavailable, so an edited object could still come back
  "SKIPPED — already existed; left unchanged". Applying an edited object now always
  routes through the REPLACE confirmation (REPLACE's `DROP ... IF EXISTS` safely
  handles an object that does not exist yet), so the edit lands once confirmed.
- **Schema Conversion: applying no longer collapses the open Generated-DDL panels.**
  The post-apply re-render now preserves each expansion's open/closed state per
  object instead of folding everything.

### Notes

- UI fix; ships in the `:0.1.14` image.

## v0.1.13

### Changed

- **Schema Conversion: applying an EDITED object that already exists now uses
  REPLACE (with confirmation) instead of silently skipping.** Previously, after
  editing a converted DDL (e.g. remapping a column's type) and clicking "Apply to
  target" in the default SKIP mode, an already-existing target object was left
  untouched -- the edit silently did not take effect, and the only feedback was a
  brief SKIPPED toast (it looked like "nothing happened"). The per-object Apply now
  detects an edit to an existing object and routes it through the REPLACE
  confirmation dialog ("DROP and recreate …"), so the change actually lands once
  confirmed. A non-edited existing object is still skipped (idempotent); an edited
  object that does not yet exist is created normally.

### Notes

- UI/behavior change; ships in the `:0.1.13` image.

## v0.1.12

### Changed

- **DSQL-unsupported source columns are now PRESERVED as `bytea` -- never blocked
  or silently NULLed -- across BOTH Full Load and CDC.** A table with a MySQL
  spatial column (geometry/point/…) previously failed Schema Conversion entirely
  (an UNSUPPORTED, read-only comment placeholder). Now:
  - **Schema Conversion** maps the spatial column to `bytea` and produces a real,
    editable `CREATE TABLE` (classified MANUAL with a "preserved as raw bytes
    (WKB)" note). You can still edit it to `text` (WKT), drop the column, or keep
    `bytea`.
  - **Full Load** reads the column via `ST_AsBinary(col)` -> WKB bytes -> `bytea`.
  - **CDC**: the custom DSQL sink converts Debezium's geometry logical type
    (`io.debezium.data.geometry.Geometry`/`Geography`/`Point`) to its WKB bytes ->
    `bytea` -- the **same bytes** Full Load writes (FL/CDC parity; SRID dropped on
    both paths, plain WKB). An unexpected shape is bound as-is so it fails loudly
    to the DLQ -- it is never silently NULLed.
  - The shared write contract (`converter.DSQL_WRITE_CONTRACT_CASES`) records
    geometry -> `bytea` so the Full Load (Python) and CDC (Java) write paths stay
    in lockstep.

### Notes

- The DSQL sink connector plugin must be rebuilt/republished for the CDC geometry
  handling to take effect on a live pipeline; it ships with the next image + plugin
  build.

## v0.1.11

### Changed

- **Full Load value conversion now follows the applied target schema.** The value
  converter previously re-derived each column's target type from the *source*
  MySQL type, so a column remapped in Schema Conversion (e.g. `TINYINT(1)` ->
  `smallint` instead of `boolean`) was ignored and a non-0/1 value failed the whole
  table. Full Load now converts each value to match the *applied* target type
  (parsed from the converted/edited DDL), so a remapped `smallint`/`integer` column
  loads non-0/1 values as integers; a genuine boolean column is unaffected.
- **A fresh/replace re-load preserves a custom-remapped target schema.** The
  fresh-load recreate step now DROPs+recreates from the applied (edited) DDL rather
  than a deterministic re-derivation, so a user remap is not silently clobbered on a
  full re-load.

### Fixed

- The boolean value-conversion conflict message now guides the user to remap the
  column's target type to `smallint`/`integer` in Schema Conversion (now effective)
  and retry the table, instead of only suggesting a source-side change.

### Notes

- No new container image is published yet (batched with v0.1.10). Locally, restart
  the UI to pick it up; on ECS it ships with the next image build.

## v0.1.10

### Fixed

- **Schema Conversion preview: an object that cannot be auto-converted is labeled
  "Unsupported" and shows no "Apply to target" button.** A table with a specific
  placeholder (e.g. MySQL spatial types) was previously shown as just
  "N warning(s)", stayed editable, and offered an Apply button (which would
  no-op / SKIP). The preview now (1) surfaces the conversion severity
  ("Unsupported" / "Review needed") in the object header, and (2) treats **any**
  non-`CREATE` placeholder -- not only the generic not-converted note -- as
  not-auto-converted: shown read-only with the redesign reason and the
  AI-suggestion option, and never offered for apply. Complements v0.1.9, which
  already SKIPs such objects on the apply path.

## v0.1.9

### Fixed

- **Schema Conversion: a table that cannot be auto-converted is now SKIPPED, not
  FAILED.** Applying a table the converter could not auto-convert -- e.g. one with
  MySQL spatial/geometry columns, which Aurora DSQL has no type for -- produced a
  confusing `SchemaApplyError: target DDL must be a CREATE TABLE/VIEW/MATERIALIZED
  VIEW/INDEX statement`, because the converter emits a comment placeholder (not a
  `CREATE`) for it. Such a table is now reported **SKIPPED** with the redesign
  reason (matching its assessment) and is never sent to the applier; the other
  selected tables apply normally.

## v0.1.8

### Fixed

- **CDC offset-seeder (gapless Full Load -> CDC handoff) can now deploy.** When CDC
  is deployed with a Full Load watermark (`SeedOffset`), the cdc-stack creates an
  in-VPC offset-seeder Lambda plus its own IAM role, invoked by a custom resource.
  The assumed `CdcDeployRole` lacked the permissions to do this, so the deploy would
  fail with `AccessDenied` and roll back. Added to `CdcDeployRole`:
  - `lambda:*` lifecycle (`CreateFunction`/`DeleteFunction`/`InvokeFunction`/…) on
    `function:mysql-dsql-cdc-*`;
  - broadened the IAM role-management scope from `*-ConnectorExecutionRole-*` to
    `role/mysql-dsql-cdc-*` so it also covers the auto-named offset-seeder role;
  - `iam:PassRole` to `lambda.amazonaws.com` (in addition to MSK Connect).
- **CDC infrastructure: MSK Serverless cluster creation.** Creating the MSK
  Serverless cluster validates the VPC under the caller's credentials, so the
  assumed `CdcDeployRole` also needs `ec2:DescribeVpcAttribute` (and
  `ec2:DescribeAvailabilityZones`); without them the deploy failed with `You are
  not authorized to perform DescribeVpcAttribute` and rolled back.
- **CDC infrastructure: connector role creation + rollback cleanup.**
  `logs:DescribeLogGroups` (which CloudFormation calls to resolve a LogGroup `Arn`
  for `!GetAtt`) has no resource-level support, so it is now its own statement scoped
  to the account/region log groups rather than pinned to the connector log group;
  and the MSK Serverless cluster delete requires `kafka:DeleteCluster` (there is no
  `DeleteClusterV2`) -- without it rollback/teardown left the cluster orphaned.
- **Removed dead Glue Schema Registry permissions** from the deploy role: the
  pipeline uses the built-in JSON converter (since v0.1.5) and creates no Glue
  registry, so the `glue:*` grants were unused.

### Notes

- Deploy-template only (app-stack IAM); **no container image change** — the published
  `:0.1.7` image is unchanged and remains the default.

## v0.1.7

### Fixed

- **CDC infrastructure now deploys (cdc-stack).** Deploying the cdc-stack via the
  assumed `CdcDeployRole` failed and rolled back due to missing IAM permissions and
  a template bug. Fixed:
  - `CdcDeployRole` IAM: stage the oversize template in the plugin bucket
    (`s3:PutObject`/`GetObject`); MSK Connect plugin + worker-configuration tag
    permissions (`kafkaconnect:TagResource`/`ListTagsForResource`/`UntagResource`)
    with `Resource: "*"` for the create actions (which have no resource-level
    support); and VPC endpoint permissions (`ec2:CreateVpcEndpoint`, …).
  - `cdc-stack.yaml`: removed an invalid `!GetAtt ConnectorS3Endpoint.PrefixListId`
    (`AWS::EC2::VPCEndpoint` has no such attribute), and shortened a security-group
    rule description to satisfy EC2's <256-char / restricted-charset rule.

### Changed

- Default `ContainerImageUri` -> the published `:0.1.7` image.

> Note: the CDC **infrastructure** path is validated end-to-end; connector start
> ("Start CDC") and the offset-seeder (watermark/gapless handoff) paths are being
> hardened separately.

## v0.1.6

### Fixed

- **CDC infrastructure deploy works on the published image.** The cdc-stack
  CloudFormation template (`deploy/cdc-stack/cdc-stack.yaml`), which "Deploy CDC
  infrastructure" reads at runtime, was not bundled in the container image (the
  Dockerfile did not copy it and `.dockerignore` excluded `deploy/`), so a clean
  image failed with "Could not read the cdc-stack template". The template is now
  bundled in the image.

### Changed

- Default `ContainerImageUri` bumped to the published `:0.1.6` image (so a fresh
  deploy includes the CDC-template fix).

## v0.1.5

### Changed

- **CDC deploy cost estimate is shown per hour, not per month**, matching the
  tool's temporary (cut-over duration) use of the CDC pipeline. **Glue is removed**
  from the listed cost drivers — the pipeline does not use Glue.

## v0.1.4

### Fixed

- **Schema Conversion no longer blanks on unsupported spatial types.** A table
  using a MySQL spatial type (`POINT`, `LINESTRING`, `POLYGON`, …) previously
  raised a `sqlglot` `ParseError` that aborted the entire Schema Conversion step.
  The failure is now isolated per table: the affected table is classified
  `UNSUPPORTED` with a clear reason (naming the spatial column) and the remaining
  tables still convert.
- **"Deploy CDC infrastructure" button on the Migration plan step now works.** The
  click was a silent no-op because the async confirm-dialog/deploy handlers were
  invoked without `await` (the coroutine was never awaited). The handlers are now
  awaited, so the confirmation dialog opens and the deploy starts.

### Changed

- **app-stack networking guardrail.** `AllowedIngressCidr` guidance is clarified
  (internet-facing ALB → set your own public IP as `x.x.x.x/32`), and a new
  `SourceReachabilityRequired` rule requires at least one of
  `SourceDbSecurityGroupId` / `SourceDbCidr` so the task always has egress to the
  source DB (prevents a silent "can't connect to source" after deploy).
- **AI assist model selection.** `BedrockModelId` is now a curated Anthropic
  dropdown, and the task role's `bedrock:InvokeModel` scope is auto-derived from
  the chosen model. `BedrockModelArns` becomes an optional override.
- **`CertificateArn` test path documented.** The deployment guide (EN/KO) was
  tidied: clearer optional sections, and the public-IP / test-cert prerequisites
  are surfaced up front.

## v0.1.3

- Prior published baseline (ECR Public image `:0.1.3`).
