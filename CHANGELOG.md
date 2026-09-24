# Changelog

_Language: **English** | [한국어](CHANGELOG.ko.md) | [日本語](CHANGELOG.ja.md)_

All notable changes to this project are recorded here. This project follows
[semantic versioning](https://semver.org/) (patch releases for bug fixes).

## v0.1.514

### Fixed

- **"Re-snapshot every table" was ignored by the Deploy dialog, so the continuation was a
  dead end again.** Choosing that route flipped the prerequisites to `Can proceed` and
  demoted the failing row to a recommendation -- and then Deploy CDC infrastructure blocked
  with a red panel claiming "the connector is configured with
  publication.autocreate.mode=disabled … the Debezium task would die immediately", which is
  FALSE for that route (it is the one where the CONNECTOR's own database user creates the
  publication), and told the operator to "start the migration over as Full load + CDC" --
  discarding the choice they had just made. The verdict function was never wrong: v0.1.509
  gave the Deploy dialog this gate but called the probe with `[], None` and no
  `force_initial`, so it graded a decision it had not been told about, against an empty table
  list and an absent watermark. Fixed at the root rather than at the call site: a single
  shared gate now derives all three inputs from the very functions the connector config
  builder uses (`_cdc_tables_for_config`, `_cdc_resume_signal`), and every surface that gates
  on those objects -- the Deploy dialog, the Start dialog and the worker backstop -- goes
  through it. `pg_replication_objects_blocker`'s contract is to MIRROR
  `build_pg_source_config`'s two decisions exactly, and that is only checkable if the mirror
  lives in one place: this is the second time the same defect shipped on a second surface
  (v0.1.507 fixed the Start path; v0.1.509 reopened it on Deploy). A test now asserts by AST
  that no other function even REFERENCES the raw probe -- deliberately over references rather
  than call syntax, because the bug passed it to `run.io_bound` as a value, which a textual
  search for a call would not have seen.
- **The Start dialog graded publication coverage against every table in the source instead
  of the captured set.** Found while consolidating the above: it passed the whole inventory,
  so a publication covering exactly the tables this migration replicates could still be
  reported as a coverage gap because of an untouched neighbouring table in the same database.

### Changed

- **The prerequisite results table wraps its cells, so the longest remediation is readable.**
  "Detail / remediation" is by far the widest column -- a failed PostgreSQL
  replication-objects check carries ~800 characters of detail plus remediation -- and
  Quasar's QTable holds every cell on ONE line unless told otherwise (it adds
  `q-table--no-wrap`, which is what carries `white-space: nowrap` onto each cell). Measured
  in a real browser at a 1304px-wide card: the cell was 4656px and `.q-table__middle`
  scrolled (scrollWidth 5082), so the row read "The connector does n…" and the rest was
  behind a horizontal scrollbar -- the guidance the operator most needs was the least
  visible. With wrapping the cell is 1092px, there is no horizontal scrollbar, and the row
  grows from 48px to 112px: height the page can scroll, which is the right trade against
  hidden remediation. Same fix as v0.1.505 made for the per-table status card, and the design
  system now records both mechanisms and when each applies.
- **The Deploy dialog's MSK-seed note is one line.** Two of its three sentences were design
  rationale -- why the whole VPC range is admitted rather than the task's subnet, and that
  what the app may then do on the cluster is still gated by `kafka-cluster` IAM. Both true,
  neither a decision input, and padding a billable confirmation with justification makes the
  panels that do inform the decision (the cost estimate and the network plan) harder to
  find. It now states the range, the VPC, the port and the purpose.

## v0.1.513

### Changed

- **The CDC prerequisite guidance is ONE decision block instead of one problem stated four
  times.** A PostgreSQL "Full load only -> CDC only" continuation rendered seven stacked
  things under the checks, and a single fact -- the publication does not exist -- carried
  four different severities on the way down: a red `Blocked` badge, a red "1 failed" badge
  on Source Configuration, an amber "a gapless handoff is no longer possible" notice, and a
  red "Resolve the failed prerequisite(s)" line, with two blue info panels wedged in
  between, so "is this serious, or is this reading material?" had no answer. It also took
  ~11 lines of prose to reach the point that there are exactly two options. Now one amber
  box states the situation once and carries BOTH routes inside it as peers, each with its
  own button and a one-line summary of what it leaves behind; the costs and mechanics
  collapse behind "What this does, and what it costs" so only the reader who wants the
  derivation pays for it. (This repository had already recorded the same failure mode once,
  in `stale_error_notice`: "three verdicts at once, and the user cannot tell which is
  true.")
- **The RECOMMENDED route is now a button the tool executes, not an instruction.** It said
  "Change the migration type above and run the Full Load again" while the only clickable
  action on screen sat on the route it explicitly does not recommend -- visual pull and
  stated advice pointing opposite ways. "Start over as Full load + CDC" is now the primary
  button: it switches the migration type, re-runs the (read-only, seconds) prerequisite
  checks for the new type, and opens the Full Load. It navigates ONLY when the re-graded
  report can actually proceed, because the explanation for a block lives on the panel the
  operator would have been moved off.
- **The run-guard line no longer restates the verdict underneath its own solution.** Reading
  order was inverted (here is how to fix it, then "you must fix it") and it duplicated two
  badges that already carried the verdict. When a route choice is on screen the line says
  only what the empty primary-action slot means: "Pick one of the two routes above to
  continue."
- **A blocked prerequisite names the action it blocks.** "Resolve the failed
  prerequisite(s) before running: ..." is ambiguous on a screen where the Full Load, a
  billable ~5-minute CDC infrastructure deploy and Start CDC can all "run". It now reads
  "before deploying the CDC infrastructure or starting CDC" for CDC -- the deploy is the
  first thing the failure stops, and the one that costs money -- and "before running the
  Full Load" for the load.
- **The "Full load only" tile now discloses what a PostgreSQL source gives up by picking
  it.** v0.1.511 addressed this from the other side (the "Full load + CDC" tile says its CDC
  phase can be started later), but an operator reading only the cheap,
  no-extra-infrastructure tile still had no way to know it is the one choice that forfeits a
  gapless CDC handoff permanently -- and that is exactly why the reporter picked it. The tile
  now says so, where the decision is made: nothing retains WAL during the load and a
  replication slot cannot be created at a past position, so adding CDC afterwards means
  re-snapshotting every table or starting over as "Full load + CDC". MySQL is unaffected -- a
  CDC-only start seeds the connector from the watermark's binlog coordinates, so its handoff
  stays gapless as long as the binlogs are retained.

### Fixed

- **The prerequisite panel's re-snapshot route recorded only half the decision, so the job
  died after being paid for.** That route's own copy promises the connector will create the
  publication this load never had, but the button (v0.1.510) set only the start mode
  ("manual", i.e. `snapshot.mode=initial`) and NOT
  `publication.autocreate.mode=filtered`. Half a decision un-gated everything: the
  re-graded checks passed, the ~5-minute billable infrastructure deploy was allowed, and the
  Start CDC dialog -- whose own probe mirrors `force_initial` and therefore raised no
  objection -- started both connectors, after which the Debezium task died on "Publication
  autocreation is disabled". The Start CDC dialog's version of the same escape always set
  both knobs; this one now does too. The recommended route additionally CLEARS both, so an
  operator who takes the re-snapshot route and then changes their mind does not carry
  `snapshot.mode=initial` + `autocreate=filtered` into the very run whose entire point is to
  stream from the slot the tool creates at the snapshot point.

## v0.1.512

### Fixed

- **A restored session built a MySQL schema converter for a PostgreSQL source.** Two engine
  reads on the Data Migration screen took the source engine from `source_config` with a MySQL
  fallback, and a RESTORED session (reconnect, task replacement) records the engine WITHOUT a
  `source_config` — so it fell back for exactly the operator who reconnects, which is why it
  went unnoticed. Measured consequence: a PostgreSQL `timestamp` converts to `TIMESTAMPTZ`
  instead of `TIMESTAMP` (a timezone-semantics change) and `bit(1)` to `SMALLINT`, and every
  PostgreSQL conversion warning is dropped — including the array / range "unsupported in
  Aurora DSQL" notes and the 1 MiB per-value ceiling.

  No behaviour changes with this release, and that was verified rather than assumed: the two
  values these call sites actually consume — the CDC composite re-key columns with
  foreign-key ownership, and the target primary key for the recreate disclosure — were
  identical under both converters across nine primary-key shapes including a `bytea` key, no
  key, a composite key, and a table with a foreign key. The fix is worth making anyway,
  because that agreement is luck: the moment an engine difference reaches the re-key choice,
  the CDC record key would be wrong and the sink would upsert on the wrong columns, silently.
  Both reads now use `session_source_type`, as every other PostgreSQL-aware surface does, so
  one rule holds for every engine read instead of two with different reasons for being right.

## v0.1.511

### Changed

- **The continuation guidance now leads with "start over as Full load + CDC" instead of
  re-snapshotting.** Both routes re-read every source table, so the re-read is not what
  distinguishes them — but re-running the load (a) uses the tool's bulk loader rather than the
  streaming pipeline, (b) creates the replication slot BEFORE the load so everything from the
  snapshot point on is streamed, and (c) can DROP each target table first, which is the only
  way to clear rows deleted on the source since the first load. The re-snapshot route leaves
  those rows on the target forever. Same cost, strictly better result — so it is the
  recommendation, and re-snapshot is the secondary option for when re-running the load is not
  acceptable.
- **The "CDC only" tile stops promising two things PostgreSQL cannot do.** Its blurb said
  "start from a prior watermark or an external start position": the first is true only when
  that watermark carries the slot a Full-load-+-CDC run created, and the second does not exist
  at all — `_cdc_resume_signal` DISCARDS the operator's start override for PostgreSQL, because
  Debezium PostgreSQL resumes only from a replication slot's committed position and cannot be
  told to start from an arbitrary WAL LSN. The tile is deliberately NOT disabled: CDC-only is
  valid and gapless whenever an earlier run recorded its slot, and job state including the
  watermark is persisted and reloaded on startup, so a later session sees it — and disabling it
  would remove the re-snapshot continuation as well. The "Full load + CDC" tile now also says
  what it uniquely offers: the slot is created before the load, and the CDC phase can be
  started later. This blurb is re-rendered as the Data Migration step BANNER, so a false clause
  stood above every screen of the step, not just the tile.

### Fixed

- **Deploying the CDC infrastructure after switching the tile to "Full load + CDC" no longer
  bills for a cluster the load has not provisioned for.** Switching the type after a finished
  Full-load-only run re-grades the existence prerequisite to SKIP — its detail says "Full Load
  creates them for this run" — so the gate un-gated Deploy while the objects still did not
  exist. MSK Serverless was created and Start CDC then refused. The cost gate now keys on the
  WATERMARK rather than the tile, which is precise: a fresh combined run has no watermark, so
  the recommended deploy-before-load flow is untouched; a run whose load provisioned carries a
  slot; and a load that finished WITHOUT one is exactly the state that must be gated, whichever
  tile is selected now. This matters because it is the trap on the route this release
  recommends.
- **A restored session showed a PostgreSQL operator the MySQL tile semantics.** The
  migration-type selector read the engine from `source_config` with a MySQL fallback, and a
  reconnected session records the engine WITHOUT a `source_config` — so every engine-aware
  string above would have been defeated for exactly the operator who reconnects. It now uses
  `session_source_type`, as every other PostgreSQL-aware surface does.

## v0.1.510

### Fixed

- **A PostgreSQL "Full load only" run could not continue to CDC at all — the lossless route
  shipped dead, and then both buttons that led to it were disabled.** v0.1.507 added
  "Re-snapshot every table instead" so a CDC-only start with no publication could still
  proceed losslessly, but the Start-CDC worker backstop calls the same blocker right before
  creating anything and that blocker refused an absent publication REGARDLESS of intent. So
  the button un-disabled Start and the job then raised "Start CDC blocked". v0.1.509 then
  correctly disabled Deploy on the failing prerequisite, and the only surface that could
  record the re-snapshot decision was a dialog behind that button — leaving no route at all.
  The blocker now takes the connector's two intents explicitly, mirroring
  `build_pg_source_config`: `publication.autocreate.mode=filtered` excuses an absent
  publication, and `snapshot.mode=initial` excuses a missing slot. A publication that EXISTS
  but omits tables or narrows its publish list still blocks — Debezium's `filtered` mode does
  not alter an existing publication.
- **An unfinished Full Load's watermark was honoured as a gapless resume point, silently
  dropping the never-loaded tables.** Nobody reported this and it is the most serious defect
  in the chain. The watermark is attached BEFORE the first table loads, so a FAILED or partial
  run leaves one byte-identical to a successful run's — same LSN, same slot name — and nothing
  in the CDC-start path read the job's status. Starting CDC then selected `snapshot.mode=never`
  and the tables that never loaded got no baseline and no backfill: their rows were permanently
  absent from the target. Switching the type to "CDC only" even removed the "retry the failed
  tables first" hint, because that substep disappears. The slot is now suppressed on any job
  that is not `DONE`, which routes such a start to a full re-snapshot (lossless) and self-heals
  — a retry carries the original watermark forward, so the gapless resume returns once the load
  completes.
- **An invalidated replication slot graded as usable**, turning a recoverable re-snapshot into
  a dead Debezium task. Both probes now reject `wal_status='lost'`. Live-verified on
  PostgreSQL 16.15: with `max_slot_wal_keep_size=0` and enough churn the slot reached
  `wal_status=lost` while the source kept committing — so an over-run slot is an invalidated
  OPTION, not a source outage.

### Added

- **The continuation is offered beside the prerequisite that fails, not in a dialog behind a
  disabled button.** "Re-snapshot every table" records the decision and re-runs the
  (read-only, seconds) checks, so ONE re-graded report clears every gate at once instead of
  leaving a red FAIL next to an enabled primary button. It is offered only for the failures a
  re-snapshot genuinely repairs, keyed on an explicit flag rather than by matching message
  text. The copy discloses both real costs: the source tables are read again, and a row
  DELETED between the load and the CDC start is in neither the snapshot nor the stream, so it
  stays on the target and Validation reports it as an extra row.

So "finish a Full Load, then continue to CDC" now works on PostgreSQL by two routes, and the
difference is stated rather than discovered. Gapless with no re-read requires deciding BEFORE
the load — a logical replication slot cannot be created at or rewound to a past position — and
the tool already implements that decision as the "Full load + CDC" type, whose CDC phase can be
started later. After a Full-load-only run the remaining lossless route is the re-snapshot.
MySQL is unaffected in every mode: its binary log retains history with no consumer.

## v0.1.509

### Fixed

- **"Deploy CDC infrastructure" ignored a failing CDC prerequisite, so a cluster nobody
  could use started billing.** v0.1.508's new row ("CDC's publication and replication slot
  exist on the source") is `required` and sets `can_proceed=False`, and its own remediation
  says "Fix this BEFORE deploying the CDC infrastructure — MSK Serverless and both
  connectors are billed from creation". But the deploy gate only ever consulted two things:
  that a report exists, and that the engine's change-stream check passed. So the button the
  remediation referred to stayed enabled: the MSK Serverless cluster was created and started
  billing (~$1.4–2.0/hour), and Start CDC — correctly gated since v0.1.507 — then refused,
  leaving a cluster that could never be used. The gate now also blocks on that row's FAIL,
  as a peer of the change-stream check: both mean streaming cannot work against this source
  at all, and both must be known before anything is paid for.

  Deliberately still NOT gating on `can_proceed`, which would also take in per-table Full
  Load failures the Full Load guard already owns. The row's GRADING draws the line instead,
  and it was already right: a missing SLOT is a WARN (such a start re-snapshots, so it costs
  a re-read, not correctness), unread facts are INFO, and the whole row SKIPs in
  "Full load + CDC" where the objects are supposed to be absent. Verified by execution over
  all five states that the gate fires on exactly one of them.
- **The disabled button now explains the way out.** Its tooltip and the notice carry the
  failing row's own detail and remediation, so the route — including where the re-snapshot
  option is — is visible without going back to the prerequisite table. The notice header
  also stops saying "Run the CDC prerequisite checks first" once the checks have run and one
  of them failed.

## v0.1.508

### Added

- **A prerequisite row for whether CDC's publication and replication slot actually EXIST.**
  "Source user can create the CDC publication" checks a PRIVILEGE, so it passed green while
  the object was absent -- which is how a green pre-flight came to be followed by a
  guaranteed deploy failure. The two are different questions with different remedies (grant
  vs. run the load / re-snapshot), so they are now separate rows, side by side. It grades
  coverage, not just existence: a publication that omits a selected table, or that narrows
  its INSERT/UPDATE/DELETE list, FAILS -- each would leave the connector reporting RUNNING
  while replicating nothing for those tables or those change types. A missing SLOT is only a
  WARN, because such a start re-snapshots and so costs a re-read, not correctness. Unread
  facts stay UNKNOWN and cannot gate anything.

  The row is gated on the PROVISIONER's own predicate, not on the migration type: in
  "Full load + CDC" the objects are supposed to be absent beforehand (the Full Load creates
  them), so the row SKIPs there and can never block the one path that is gapless.

### Changed

- **Deploying the CDC infrastructure for a "CDC only" run is refused when the source has
  nothing to stream from**, before the ~5-minute billable MSK Serverless create rather than
  after it. Asymmetric by design: a "Full load + CDC" run is left alone, because deploying
  the infrastructure before the load is the flow the tool itself recommends and the objects
  legitimately do not exist yet. It cannot strand anyone -- the Start-side check re-probes
  rather than remembering this verdict, so provisioning in between simply clears it.
- **A Full-load-only PostgreSQL run no longer recommends the broken path in the tool's own
  voice.** The post-load invite said to set the type to "CDC only" because it "streams from
  this Full Load's watermark onto the already-loaded target (no re-snapshot)" -- false on
  both counts for PostgreSQL -- and offered a one-click jump to do it. It now explains that
  no slot was created, that a gapless handoff needs the slot to exist BEFORE the load, and
  what the two honest routes cost. MySQL's wording is unchanged and remains true.
- **The "CDC only" tile no longer implies it will arrange a publication.** Its PostgreSQL
  requirements now say the publication and slot must already exist and that this mode does
  not create them; the "Full load + CDC" tile says the tool creates them at the snapshot
  point. The Full-load-only prerequisite PREVIEW says it too -- that report is the last
  moment the gapless choice is still available.
- **A FAILED connector's troubleshooting line names its CloudWatch log group** instead of
  promising a link that was never rendered anywhere in the UI.
- README and the cdc-stack template no longer describe the gapless handoff
  source-agnostically: the mechanism differs by engine, and the PostgreSQL half is what the
  source-agnostic wording got wrong.

## v0.1.507

### Fixed

- **A PostgreSQL "CDC only" start after a "Full load only" run killed the Debezium task,
  and the tool let you pay for the infrastructure first.** The CDC publication and
  replication slot are created by exactly one thing -- the Full Load's provisioning step --
  and only when that run is part of a CDC plan. A Full-load-only run creates neither, so the
  connectors deployed against objects that did not exist and the source task died on
  "Publication autocreation is disabled". The tool already detected this and only WARNED, so
  a billable MSK Serverless cluster and both connectors were created first. Start CDC now
  PROBES the source (a privilege-free catalog read) and blocks before the spend, in the Start
  dialog and again on the worker thread just before anything is created -- the second check
  covers "Retry CDC", which submits the job with no dialog. It fails closed only on "the
  catalog says absent", never on "I could not ask".
- **A worse, silent bug behind it: the tool asked to resume from a replication slot it had
  never created.** `snapshot.mode` was chosen from the watermark's WAL LSN alone, and a
  Full-load-only watermark carries an LSN with no slot -- so a CDC-only start was configured
  `snapshot.mode=never`, i.e. resume from a slot of unknown position. Today's loud task-kill
  was the only thing preventing that: had the objects existed (created by hand, or left by an
  earlier session) the connector would have started and silently skipped every change
  committed since the load, which the default ROW_COUNT validation cannot detect. The mode is
  now chosen from the RECORDED SLOT, not the coordinate: no recorded slot means `initial`.
- **The start-point card promised gaplessness with no slot in existence.** Same root cause in
  the UI: it tested the LSN alone, so a Full-load-only watermark rendered "Automatic --
  gapless from the replication slot (recommended)", a positive Ready badge and a green
  "Start point set" line. It now requires the recorded slot too. (Same bug shape as the MySQL
  GTID-only post-mortem already documented beside it.)
- **A PostgreSQL connector failure is diagnosed instead of left in CloudWatch.** Every
  existing log signature was MySQL/MSK-flavoured, so a PostgreSQL task death produced the
  content-free "<connector> entered FAILED state." and the operator had to go open the MSK
  Connect log group. Two PostgreSQL signatures were added, and the worker log is now
  consulted on TIMEOUT as well as on FAILED -- MSK Connect exposes no task-level state and no
  last-task exception, so a connector whose only task died at startup can otherwise sit in
  CREATING until the budget expires with the cause visible nowhere in the tool.

### Changed

- **When the objects are missing, the block offers a one-click way out instead of a dead
  end.** "Re-snapshot every table instead" makes Debezium create its own slot, snapshot the
  selected tables and then stream from that slot -- there is no window between the snapshot
  and the stream, so nothing is lost, and the target load is idempotent so re-reading a row is
  safe. When the publication is also missing, the CONNECTOR's database user creates one over
  exactly the captured tables; the tool itself still never writes to your source. The cost is
  reading the source tables again.

  There is deliberately no "accept the gap and stream from now" option. The window starts at
  Full Load START, so it spans the whole load plus all think time; the tool cannot tell you
  how many rows or which tables are affected; and the default validation mode would then
  certify the result clean. An acknowledgement nobody can evaluate is a click-through, not
  consent.

Gaplessness on a PostgreSQL source is a decision made BEFORE the load: "Full load + CDC"
creates the slot at the snapshot point, so the handoff has no gap and no re-read. After a
Full-load-only run that option is gone -- a logical replication slot cannot be created at,
or rewound to, a past position (verified live on PostgreSQL 16 and 18: the function takes no
LSN argument, CREATE_REPLICATION_SLOT rejects one, slot_advance only moves forward, and
START_REPLICATION silently clamps) -- so the remaining gapless route is to re-snapshot.
MySQL is unaffected in every mode: its binlog retains history with no consumer, so nothing
needs provisioning.

## v0.1.506

### Fixed

- **A PostgreSQL "Deploy CDC infrastructure" log no longer reports MySQL artifacts as if
  they mattered.** The upload stage pushed all four bundled artifacts on every deploy, so
  the log opened with `cdc-plugins/debezium-mysql-plugin.zip: already up to date` and
  `offset-seeder-lambda.zip` on a PostgreSQL run — neither of which a PostgreSQL cdc-stack
  can reference (`DebeziumSourcePlugin` is conditional on `IsMySqlSource`, and
  `DeploySeederFunction` ANDs `IsMySqlSource`). The stage is now engine-aware: a PostgreSQL
  deploy uploads the PostgreSQL source plugin and the shared DSQL sink, a MySQL deploy
  uploads the MySQL source plugin, the sink and the offset-seeder Lambda, and one line
  states what is not being uploaded and why. As a side effect a first PostgreSQL deploy no
  longer transfers the 31.3 MiB MySQL plugin it can never use, and a first MySQL deploy no
  longer transfers the 21.2 MiB PostgreSQL one.
- **An upload result now names only artifacts it actually uploaded.** A key is returned as
  empty when its object was skipped, so a cdc-stack parameter can no longer point at an
  object that is not in the bucket — which is also what keeps the template's
  `Fn::Equals [LambdaSeederS3Key, ""]` truthful for a PostgreSQL stack, with no
  engine-awareness needed in the parameter patcher.

The engine is derived from the CloudFormation parameters the deploy is about to submit
(`EngineType`), not passed in by the caller, so the uploaded set cannot disagree with the
stack being created; an unknown engine falls back to uploading everything, because the
failure mode of guessing wrong is a plugin whose file is missing.

## v0.1.505

### Fixed

- **The Per-table migration status table no longer grows a horizontal scrollbar inside its
  card.** Quasar sets `white-space: nowrap` on every table header and cell, so the
  eleven-column table's multi-word headers ("Source rows (est.)") and long
  `schema.table` names pinned it wider than the card and the inner region started
  scrolling. The headers and the table-name column may now wrap. Both halves were needed:
  allowing the headers to wrap reclaims most of the width, but `word-break` on the name
  column does nothing while `nowrap` is still in force -- a break opportunity cannot be
  taken if wrapping is forbidden -- and that single omission left the name column pinned
  at full width with the scrollbar intact at every width tested. The numeric columns keep
  `nowrap`, because a thousands-separated figure has no break opportunity anyway and reads
  badly split. Measured in a real browser against the columns, rows and cell slots the app
  actually renders: a 1100px card went from 59px of horizontal overflow to 0, a 1000px card
  from 159px to 0, an 850px card from 309px to 0, with zero clipped cells at every width.
  Below roughly an 820px card the numeric columns hit a floor no wrapping can shrink, and a
  scrollbar there is preferred over hiding data.

## v0.1.504

### Fixed

- **PostgreSQL CDC still could not be started from the Fargate deployment: the app read the
  wrong IP address for itself.** v0.1.503 resolved the network the cdc-stack must admit on
  MSK port 9098 by UDP-`connect()`ing a socket to a link-local address and reading back the
  address the kernel bound. On an `awsvpc` ECS task that is the WRONG interface -- the task
  has a separate link-local interface for the ECS metadata/credentials endpoint, and being
  on-link THERE is precisely what selects it -- so the app reported `169.254.172.2` instead
  of its ENI's private IP. It returned a well-formed wrong answer, not "unknown". The
  consequence was two false refusals, not one: the deploy dialog blocked with "the tool could
  not find this app's network interface in vpc-…" while the same dialog's Network panel
  described that very VPC correctly, and Start CDC ALSO refused -- which is why setting
  `DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR` did not work around it either. On Fargate there was no
  supported configuration that could start PostgreSQL CDC. The app now asks the ECS task
  metadata endpoint for its own ENI address when it runs on ECS, and -- more importantly --
  REJECTS any address that can never belong to a VPC network interface (link-local, loopback,
  multicast, reserved) instead of acting on it, so an unknown answer is now honestly unknown.
  Start CDC additionally honours an explicitly configured `DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR`
  over its own address check, since an operator who set it has attested to this host's network.

### Changed

- **An unresolvable network now warns instead of blocking the deploy, and says which step
  failed.** Four different causes -- no own address, a denied or throttled
  `ec2:DescribeNetworkInterfaces`, no interface with that address in the entered VPC, and an
  address outside the VPC's CIDR blocks -- all rendered one identical message whose only
  advice ("deploy the CDC infrastructure into the VPC this app runs in") was circular for an
  app already in that VPC. Each now has its own message naming its own remedy, and the deploy
  is no longer blocked: the cdc-stack's 9098 ingress is a CIDR rule with an empty default, so
  deploying without it and adding a TCP 9098 inbound rule to the cdc-stack's
  `ConnectorSecurityGroup` afterwards is a working path -- whereas the block left no path at
  all. A deployment whose TEMPLATE declares it cannot reach MSK is still blocked, because no
  security-group rule can add a missing egress rule or a missing task-role policy.
- **The network resolution is no longer silent.** `core/ec2_metadata.py` had no logger at all
  and three separate places swallowed the reason, so diagnosing this needed a one-off Fargate
  task. The resolved address, a discarded address and a failed AWS lookup are now logged --
  at `WARNING` for the give-ups, so they are visible at the deployed default `INFO` level.

App code only: an existing stack needs just the image (`ContainerImageUri`), not a
full-template update.

## v0.1.503

### Fixed

- **PostgreSQL CDC could not be started at all from the Fargate deployment.** A PostgreSQL
  cdc-stack is always created `SeedMode=External`, which means the APP -- not the in-VPC
  seeder Lambda MySQL uses -- creates the CDC Kafka topics and seeds the start offset itself,
  over the MSK Serverless IAM endpoint on port 9098. The Fargate app stack granted neither
  half of what that needs: its task security group declares an explicit egress list (443 +
  5432), which suppresses the default allow-all, so 9098 was denied at the source; and the
  task role held no data-plane `kafka-cluster` permissions at all. The cdc-stack's matching
  ingress rule already existed but was never armed, because the `HostSubnetCidr` that gates
  it was only ever set by the EC2 host's user-data and was structurally empty on Fargate.
  The visible failure was a `KafkaTimeoutError` after the MSK Serverless cluster had already
  been created and started billing. Now the app stack ships the `MskEgress` rule (with its
  own `MskEgressCidr` parameter -- deliberately NOT `HttpsEgressCidr`, which operators are
  told to narrow to their NAT/PrivateLink range, while the MSK bootstrap resolves inside the
  CDC VPC) and a `cdc-external-seed` task-role policy, and the app registers the VPC CIDR
  block containing its own address as the cdc-stack's admitted network -- the VPC block, not
  the task's subnet, because the single task is placed into a LIST of subnets and a
  replacement may land in another one, which would silently stale a subnet-scoped rule.
  **An existing Fargate stack needs a FULL-TEMPLATE update**
  (`--template-file deploy/cloudformation.yaml`), not an image-only `ContainerImageUri`
  override: the task-role grant has no code substitute. MySQL is unchanged on every
  deployment, and the in-VPC EC2 deployment is unaffected.
- **A CDC infrastructure deploy that could never be seeded is now refused up front instead
  of costing a cluster.** Previously the whole cdc-stack was created -- MSK Serverless
  included, 5-20 minutes and billable -- and only then did Start CDC fail with a raw
  `KafkaTimeoutError`. The Deploy dialog now checks the deployment's capability and the
  network the stack would admit before the spend, disables Deploy with the remedy named, and
  re-checks on the click (an EC2 describe that succeeded when the dialog opened can since be
  throttled) so nothing is created for a doomed configuration -- not even the source-secret
  upsert. This half is pure app code, so **it takes effect on an already-deployed stack**:
  without the template update the deploy is refused in seconds rather than after 20 minutes.
  Start CDC likewise refuses. The old "Start PostgreSQL CDC from inside the VPC" warning is
  gone: it was factually wrong on Fargate (the task IS in the cdc-stack VPC -- it was simply
  not admitted and held no IAM), it fired even on a correctly equipped stack, and it warned
  and then proceeded into a guaranteed failure. The engine keeps one fail-open backstop: at
  seed time it refuses only on a certainty -- the deployed stack admits a network this host
  is not in -- which also covers an adopted stack, where local config says nothing.
- **The CDC `VpcId` is derived from the source database, and a mismatch warns before the
  deploy.** The VPC comes off the same `DescribeDBInstances` response the tool already reads
  for the source security group, so there is no extra API call and no extra IAM. It fills only
  an EMPTY field -- an operator's own value is never overwritten -- and shows its provenance
  rather than substituting silently. Entering a VPC that is not the source's now warns (not
  blocks: peering, Transit Gateway and PrivateLink make it legitimate) and says what to
  confirm, instead of surfacing ~20 minutes into an MSK deploy that leaves a stack to tear
  down. Best-effort throughout: a non-RDS host, a cross-account endpoint or a missing
  `rds:DescribeDBInstances` leaves the field blank and stays silent rather than guessing.

### Security

- The new task-role `kafka-cluster` grants are scoped tighter than the seed's create/describe
  permissions: `ReadData`/`WriteData` are limited to the `-debezium-source-offsets` topic
  alone, because that is the only topic the seed reads or writes. The broader
  `topic/<family>/*/*` form would have let this long-lived, ALB-fronted role consume the full
  replicated row content of every cdc-stack in the account. No consumer-group action is
  granted at all -- the seed uses `consumer.assign()` and joins no group.

## v0.1.502

### Changed

- **A PostgreSQL source no longer lists MySQL's binlog/GTID checks at all.** They were kept
  as SKIP rows to satisfy a mode-symmetry rule -- a check id present in the weaker mode but
  absent in the stronger one reads as an oversight -- and v0.1.498 corrected their wording
  from "Not applicable for this MODE" (which reads as "they WILL apply under CDC") to "...for
  this source ENGINE". But symmetry is about the two MODES of one engine, and it still holds
  now that they are absent from BOTH PostgreSQL modes. What was left was simply another
  engine's requirements on a PostgreSQL operator's screen: three rows that can never apply,
  beside the nine that do. The PostgreSQL counterpart of the retention risk is its own check
  (`SLOT_WAL_RETENTION`), so nothing is lost. MySQL is unchanged.

## v0.1.501

### Fixed

- **"Accept quarantined rows & continue" was undone within a tick, leaving no way forward.**
  The Full Load panel showed "complete -- with an accepted gap" and the table showed
  `Done · 3 dropped`, while the sidebar said Data Migration **Failed** and Validation stayed
  locked. The acceptance really was applied -- and then the live poll wrote the JOB's status
  back over it. The job is terminally FAILED (its rows really were dropped) and the
  acceptance is an operator decision recorded on the SESSION, so the job status can never
  express it; the poll re-arms on every refresh and wrote `mapped` unconditionally, so the
  decision was reverted about a second after it was made, `set_error` re-recorded the
  "Migration failed" banner, and the Validation gate -- which reads this step -- stayed shut.
  A dead end: re-running drops the same rows again, so the only escape was Start over.
  The poll now honours the acceptance before writing, still gated on the incompleteness
  being quarantine-ONLY so a real retryable failure is never waved through by a stale flag.
  Dates to v0.1.350, so every deployed version has it.

## v0.1.500

The six findings the v0.1.499 audit left unadjudicated, re-verified against a real
PostgreSQL 16 and fixed. Every fix mutation-checked.

### Fixed

- **A blank-padded `CHAR(n)` key made an idempotent resume re-write the whole table.**
  PostgreSQL pads `bpchar` on OUTPUT while a MySQL `CHAR` source value arrives TRIMMED (MySQL
  strips trailing blanks on retrieval). The server compares bpchar blank-insensitively, so its
  own `WHERE (code) IN (%s)` DOES match the stored row -- but the Python-side set compare then
  saw `'AB        '` against `'AB'` and concluded the key was absent. Live-confirmed: the
  verbatim compare matched NONE of two present keys. Keys are now normalised by the type the
  SELECT itself reports, so no new plumbing is needed; a text/varchar key is untouched, where
  trailing spaces are data.
- **A psycopg error raised CLIENT-side was retried as if a connection had dropped.** The
  no-SQLSTATE arm treated anything from the psycopg module as transient, and psycopg raises
  `ProgrammingError`/`DataError` with no sqlstate for a value it cannot adapt or a statement it
  cannot render. Reachable from real SOURCE DATA, not just a tool bug: a MySQL TEXT column
  holding a `0x00` byte raises `DataError("PostgreSQL text fields cannot contain NUL (0x00)
  bytes")`, so the batch burned its whole retry budget with backoff and then failed with an
  opaque message, instead of failing at once. Narrowed to CONNECTION-level errors --
  deliberately NOT their shared `DatabaseError` base, which would re-admit both.
- **A partitioned source table had no row estimate at all.** `pg_class.reltuples` is 0/-1 on a
  partitioned PARENT because the rows live in its partitions -- and the parent is exactly what
  the migration selects, so the tables most likely to be huge showed no progress denominator
  and no ETA for hours. The estimate now sums the LEAF partitions via `pg_partition_tree`,
  still catalog-only. `FILTER`, not `COALESCE`: with no leaf analysed it stays unknown rather
  than collapsing to a confident and wrong 0.
- **"Indexes not created" reassured the operator about a missing CONSTRAINT.** A failed
  `CREATE INDEX` is deliberately not a table failure -- the data is complete -- but "no data was
  lost and you do not need to re-run" was true of the data and false of the constraint: a
  failed UNIQUE index means the target now accepts duplicates the source forbids, and a
  COMPOSITE_KEY table's post-load pass can contain ONLY such an index. The uniqueness bit is
  carried from the DDL (the only place that knows it) and the notice splits on it, warning that
  it must be created BEFORE cut-over.
- **The shared-snapshot anchor could die mid-load and take the snapshot with it.** A sharded
  PostgreSQL read pins one connection `idle in transaction` so every shard can
  `SET TRANSACTION SNAPSHOT` to it -- and with a bounded pool the last shard may import that id
  hours later. An idle timeout or `idle_in_transaction_session_timeout` ends the transaction,
  after which later shards read their OWN snapshot and the table is consistent as of no single
  point in time, with nothing said. The parent now pings the anchor once a minute from the loop
  it already runs, and a lost anchor stops the shards instead of letting them diverge.
- **The quarantine safety cap was multiplied by the shard count.** `MAX_QUARANTINE_RECORDS` is
  enforced per importer, and a sharded table builds one importer per PROCESS, so K shards could
  drop K x 1000 rows before the safety net fired. A shared counter would need IPC, so the one
  budget is divided among the table's shards instead.

## v0.1.499

A Full Load audit, executed against a real PostgreSQL 16 rather than read. Eight defects that
lose, corrupt or misreport data, plus four reporting defects and a wrong-database trap. Every
fix was mutation-checked: the defect was re-introduced and the new test confirmed to fail.

### Fixed

- **A source-drop retry could skip rows it never wrote, and still report success.** The
  resumable unit was the batch ORDINAL: `index <= high-water` means "this PK range is
  committed" only while batch *i* denotes the same rows -- and the retry deliberately opens a
  NEW snapshot (its own docstring explains that resuming the dead one would splice two points
  in time, and promises the cost is only re-read I/O because `ON CONFLICT` skips what is
  already written). The ordinal skip broke that promise: it does not re-write, it skips by
  count. Reproduced -- with batches 0-2 committed (ids 1..6) and the application deleting
  id=1 before the retry, the re-read's batch 3 became `[8, 9]` and **row 7 was never
  written**, with `failures=0` and the table DONE. Because `_iter_batches` also cuts on a
  payload-byte budget, a plain UPDATE that changes one already-loaded row's SIZE is enough --
  no insert or delete needed. The watermark is now discarded before every re-read; a shard
  reopening one SHARED PostgreSQL snapshot keeps it, because there the row sequence really is
  stable.
- **"Exclude column & reload" did nothing on a sharded table.** The shard worker does not go
  through `migrate_table`, so it never applied the operator's oversized-LOB exclusions: the
  column was read and written anyway, every >1 MiB value was rejected and the whole ROW
  quarantined again -- the opposite of the intent, which is to keep the row and drop the
  column -- and past the quarantine cap the load aborted. The recovery was therefore a no-op
  on exactly the tables large enough to be sharded.
- **A PostgreSQL DOMAIN bypassed the faithful read.** `format_type` reports a domain as the
  DOMAIN's name, so the text-cast rule (json/jsonb/interval) matched nothing -- while
  PostgreSQL still describes the result with the BASE type's OID, so psycopg applied the base
  loader and produced the exact loss the cast prevents. Live-confirmed: a domain over jsonb
  turned the JSON literal `null` into SQL NULL, and a domain over interval turned
  `2 years 3 mons` into `820 days`, silently, with the table reported DONE.
- **The tool's own remodel advice was unimplementable on the data path.** It tells the
  operator exactly what to remodel a DSQL-unsupported PostgreSQL type to, and the loader even
  parses the applied DDL's types -- but `PostgresValueConverter` discarded them, so every
  value still arrived in the SOURCE type. Live-confirmed: `money` -> numeric was rejected
  22P02 (psycopg returns `'$12.34'`), `text[]` -> jsonb was rejected 22P02, and **`bit` ->
  bytea silently stored the ASCII digits of `'10101010'`** instead of the byte `0xAA`. The
  read now follows the applied target type (textual targets take the canonical source text,
  money casts to numeric, an array reads `to_jsonb`), and the bit -> bytea advice is withdrawn
  because the loader cannot produce it faithfully.
- **A date/time value PostgreSQL stores happily could kill the whole worker.** `infinity`, a
  year past 9999 and a BC date load into `datetime` only by raising `psycopg.DataError` --
  out of the streaming cursor, so not a per-row quarantine: the worker dies and the table
  fails with a driver message ("timestamp too large (after year 10K)") naming no column.
  These columns are now read as text, which handled all three faithfully on PostgreSQL 16.
- **The identity-sequence sync discarded its own results when its connection dropped.** It
  held ONE connection across every table with no reconnect -- the only post-load pass without
  one -- and this pass runs at the END of a load that can take hours, when a short-lived DSQL
  IAM token is most likely dead. Measured on a real backend kill: the function RAISED, 5 of 6
  tables were left at `nextval=1` with `max(id)=11`, and even the recorded success was thrown
  away. It now reconnects per table and keeps what it has. Separately, a failed `SELECT MAX`
  was swallowed as a silent no-op (so the table vanished from the log while the same entry
  promised "the first insert after cut-over cannot collide"), and `sql.Identifier(*name.split("."))`
  split on EVERY dot, so a table whose bare name contains one addressed a relation that does
  not exist.
- **One exception killed the thread that carries Stop and liveness.** The Full Load progress
  drain's loop body was unguarded, and `handle.update()` writes the job store on every
  message. After it died, a user Stop never reached the workers (`cancel_event` is set only
  there) and a healthy run was reaped by the 900s stall watchdog blaming an unresponsive
  connection -- while `drain.join(timeout=5)` noticed nothing.
- **A plain INSERT that stored fewer rows than it was sent counted them as "skipped".** Under
  the NONE mode there is no `ON CONFLICT` clause, so there is nothing to conflict with: a
  shortfall is lost rows, which is precisely what that mode exists to rule out. It now fails
  the batch loudly.
- **Quarantined rows could disappear on a resume, and a failed shard discarded its siblings'.**
  `import_rows` cleared its quarantine sink on every call, so an attempt that skipped the
  already-committed batches reported `quarantined=0` -- the table logged SUCCESS, no primary
  key reached the error log, and the Validation gate never fired. Records now carry across a
  resumed attempt, deduped by primary key. Separately, the per-row evidence loops sat in the
  all-shards-clean branch, so one FAILED shard threw away every other shard's quarantine
  records and index failures.
- **"Accept quarantined rows" was a sticky, unscoped flag.** Consenting to a 3-row gap
  auto-accepted every later run's gap, of any size, in any table -- so a load that dropped
  thousands completed as a success nobody had agreed to, with the banner still saying "you
  accepted that gap". The accepted row count is recorded, and a larger gap re-asks.
- **An unknown row estimate is no longer reported as 0.** PostgreSQL 14+ reports
  `reltuples = -1` for a never-analyzed table and `autovacuum_analyze_threshold` defaults to
  50, so EVERY small PostgreSQL table is affected, not just freshly-loaded ones. The dialect
  maps that to `None` on purpose; `or 0` threw the distinction away and the panel read
  "5 / 0" -- a target apparently ahead of its source.
- **Quarantine advice named only the one recovery that cannot work.** All three surfaces said
  "fix the source value", impossible on a read-only source and impossible in principle when
  the value is legitimately over 1 MiB -- the usual cause. They now also name
  "Exclude column & reload", the tool's own answer. The sharded path's per-table log line was
  bare ("12 rows newly loaded") with no mention of the quarantine that caused its FAILURE, and
  now shares the single-path wording.
- **An evaluation over ZERO tables warns instead of scoring an empty schema.** Zero migratable
  tables is almost never a finding -- it is the wrong database, easiest to hit on PostgreSQL
  (one database per connection, and Aurora always provides an empty `postgres` beside the real
  one) and worst when the database and the schema share a name. The notice names the
  DATABASE/schema confusion and lists the other databases on the server.

### Internal

- Bounded memory was MEASURED and the standing O(rows) suspicion refuted: retained memory is
  flat across 4k / 20k / 40k rows (~0.05 bytes/row, ~0.5 MB extrapolated to 10M), peak traced
  memory tracks one page x prefetch depth rather than row count, and `SKIP_EXISTING`'s key set
  is rebuilt per batch. OCC retry, wait timeouts and lock ordering were checked and are sound.
- Three of this release's own new assertions were VACUOUS on first write and were caught by
  mutation-checking them: a whole-module string count also matched the parameter's own default;
  a function-wide search also matched an adjacent call; and an `inspect.getsource` grep matched
  the explanatory COMMENT rather than the emitted message. Each was replaced with a test that
  drives the real code and asserts on its output.

## v0.1.498

Five defects that share one shape: a value read **once, too early**, then reused as if it were
still current. Four were found by sweeping for that pattern after the first one turned up; the
fifth is the mirror image of a bug fixed in v0.1.491 in one place but not its twin.

### Fixed

- **Schema Conversion converted a PostgreSQL source with the MySQL dialect, and applied it.**
  The screen read the source engine once, in the page builder — but `build_page` runs every
  screen at page load, *before* the user has connected anything, and nothing re-enters that
  builder afterwards (the sidebar re-invokes only the returned content callable). So on the
  normal first-session flow a PostgreSQL user got: a `Source (MySQL)` pane, backticked
  `AUTO_INCREMENT` source DDL, a primary-key tile naming a mechanism their database does not
  have, and — the part that leaves the tool — **MySQL-converter target DDL written to DSQL by
  Apply**. Only a browser hard refresh recovered, and nothing said so.
  - Verified against real PostgreSQL 16: a PG expression default is re-quoted as a string
    literal, so `DEFAULT 'pending'::text` became the literal 15-character text and
    `DEFAULT now()` became `DEFAULT 'now()'` — which PostgreSQL **freezes to a constant at
    `CREATE TABLE` time**, so every row inserted after cut over gets the same migration-time
    timestamp. Both are accepted silently: no error, no warning, and Validation cannot catch
    it (it compares migrated rows, not future defaults). Three PG-only warnings
    (array/`inet` unsupported, the 1 MiB per-value cap) disappeared as well.
  - The engine and the converter are now resolved per render/action, the converter rebuilt only
    when the engine actually changes, and the conversion memo keyed on the converter too —
    without that last key the labels correct themselves while Apply keeps writing the
    MySQL-dialect DDL, which is the harder half to notice. An injected converter (tests) still
    wins and is never rebuilt. Same shape as `query_playground`'s click-time resolver, where
    this exact bug class was already fixed in v0.1.491 — in the screen that does *not* write
    to the target.
- **The AI DBA's general chat answered from whatever was true when the panel first opened.**
  The panel caches the streamer for the whole scope, so opening it on the Connect screen froze
  the engine word, the current step, the migration fact summary, and the Bedrock config for
  the rest of the session: MySQL advice (binlog, `AUTO_INCREMENT`) for a PostgreSQL migration,
  "Current step: Connect" with no facts while the chip above read e.g. `Validation`, and
  replies still coming from the previously selected model. The grounding is now resolved per
  **send**; `_refresh_model_line` already re-read the model for display on open, and the
  answer now does the same.
- **Start over silently dropped the CDC source-secret CMK.** It is deployment config, threaded
  onto the state once at screen-build time — and `reset_in_place` re-runs `__init__` on the
  *same* object (deliberately, so the builders' captured closures stay valid), so the value was
  wiped with no builder left to re-supply it. The next CDC deploy then created the tool-managed
  secret under the account's default `aws/secretsmanager` key instead of the operator's CMK,
  with no UI signal — invisible until someone audited the secret's encryption key.
  `cdc_deploy_role_arn`, set on the adjacent line, was already preserved; its twin was not.
- **After Start over, CDC posted no activity events at all.** The dedupe markers were keyed on
  `id(migration_state)` in a module-level dict, on the stated assumption that "Start over
  rebuilds the migration state" — it does not, it resets that same object in place, so the id
  never changed and the stale markers survived. A fresh CDC run therefore announced no
  "CDC streaming started", no schema drift and no DLQ growth, so neither the assistant nor the
  activity feed ever learned the stream had started or that records were being dead-lettered.
  The markers now live **on the state**, which `__init__` clears for free (and this drops a
  module dict that leaked an entry per stream per session).
- **Full-load-only prerequisites listed the wrong engine's checks for a PostgreSQL source.**
  The Full-Load branch appended MySQL's `BINLOG_ROW_FORMAT` / `BINLOG_RETENTION` / `GTID_MODE`
  with no engine split, so a PG report was byte-identical to a MySQL one. The point of leaving
  the CDC checks visible as SKIP is to preview what switching to CDC will additionally require
  — so this previewed *another engine's* requirements while hiding the operator's own six
  (`wal_level`, replication role, slot headroom, WAL retention, writer, REPLICA IDENTITY),
  which were generated only under CDC. Those six are now previewed, and the MySQL rows say
  **"Not applicable for this source engine"** instead of "for this mode" (which reads as
  "these will apply once you switch to CDC" — PostgreSQL has no binary log in any mode). The
  wording is corrected in CDC mode too. This is the mirror of v0.1.491's D-1 fix, which
  aligned the two *modes* but left engine fitness untouched.

Also, from a review of the PostgreSQL-source **CDC** prerequisite gate — ten defects, each
reproduced against a real PostgreSQL 16 before being fixed:

- **One unreadable catalog silently disarmed every later check, and the report then said
  "can proceed".** The probe's ~13 statements share one connection, so one transaction:
  swallowing a failure without a rollback left it aborted, so every *subsequent* statement
  failed with `25P02` and became `None` too. Because the object returned is still a
  `PostgresCdcFacts` (not `None`), the blocking "readiness could not be verified" FAIL could
  not engage — each check degraded itself to a non-blocking INFO. Measured with `pg_class`
  revoked: **3 of 9 facts survived before the fix, 9 of 9 after.** A report that had
  correctly blocked on a REPLICA IDENTITY of `nothing` flipped to proceeding with nothing
  verified. Each statement is now isolated, and facts with *no* CDC-critical value read are
  routed to the blocking FAIL like a probe that returned nothing at all.
- **Partitioned tables passed the gate, then broke writes on the source.** The migration
  selects the partitioned parent (the children are dropped from the inventory), but a
  publication on a parent expands to the **leaves** and PostgreSQL enforces and logs the
  *leaf's* identity. Live-verified: with the parent at `REPLICA IDENTITY FULL` and one leaf
  at `NOTHING`, `UPDATE` through the parent still fails — naming the leaf — and `ALTER TABLE`
  on the parent does not propagate. The check now grades the partitions and names the
  offending one.
- **`REPLICA IDENTITY USING INDEX` whose index was dropped read as usable.** PostgreSQL does
  not reset `relreplident` when that index goes away: the table keeps reporting `'i'` while
  behaving exactly like `NOTHING`, so every `UPDATE`/`DELETE` is refused once it is published.
  `pg_index.indisreplident` is now consulted. A non-PK identity index is a new WARN — its
  before-image carries only that index's columns, so the key the target upserts on arrives
  NULL.
- **An UNLOGGED table aborted the whole publication** with a raw driver error; it is now
  caught per-table and named (`TABLE_REPLICABLE`).
- **Nothing checked that the source user can CREATE the publication.** That needs CREATE on
  the database plus ownership of every published table — neither is the REPLICATION right the
  gate already checks, so a correctly-granted least-privilege user passed and then failed at
  the first step of CDC, after the Full Load had run (`PUBLICATION_PRIVILEGE`).
- **Unknown facts no longer render as confident green.** `is_in_recovery` and the two
  privilege flags defaulted to `False`, so an unreadable source produced
  "Source accepts writes (pg_is_in_recovery=false)" on a *required* check — failing open on
  the one fact that decides whether a slot can exist. They are tri-state now.
- **`max_replication_slots`/`max_wal_senders` of 0 now blocks.** A configured zero is not a
  full pool: nothing can be freed, so the old WARN's "drop an unused slot" could not work,
  and since Full Load takes its snapshot through the same slot the run cannot even begin.
- **The walsender-exhaustion warning was dead in the setup the tool recommends.** Outside
  `pg_monitor`, `pg_stat_activity` masks `backend_type` for other backends, so the count came
  back 0 and the WARN could never fire. It reports unknown when it cannot see.
- **A CDC-only start is warned that its publication and slot must already exist** — Full Load
  creates them, and the connector runs with `publication.autocreate.mode=disabled`.
- **The panel no longer drops the measurement.** It rendered `remediation or detail`, so every
  FAIL/WARN row lost the observed value — the WAL-retention row told the operator to raise a
  cap whose current value it never showed. `SLOT_WAL_RETENTION` was also missing from the
  CDC-only set, so a CDC-handoff check was tagged "Full load + CDC".

### Internal

- The regression tests for the Schema Conversion defect **build the real screen and render it**.
  All ten prior references to that builder were `inspect.getsource` string assertions, which is
  why a run of releases aimed at the PostgreSQL path could ship with the path unreachable and CI
  green — the buggy line still *mentioned* the right helper, it just read it at the wrong time.
- The prerequisite suite now pins the mode-symmetry rule **per engine** in one test, so this
  class of bug cannot recur a third time from a new branch.
- Two test bugs found and fixed while verifying: a dedupe-isolation line popped a bare job id
  from a dict whose keys were tuples (so it never matched anything), and the v0.1.491 D-1 test
  asserted a check id must be *absent* from Full Load, contradicting the rule it asserted two
  lines above.
- The PostgreSQL CDC probe and its production caller had **no tests at all** — replacing the
  probe with `return None` passed the entire suite, as did flipping `REPLICA_IDENTITY` and
  `SOURCE_IS_WRITER` to non-blocking, and dropping the table names the probe is given. Every
  check's correctness rested on facts nobody verified. All three are pinned now, and each new
  assertion was mutation-checked: the defect was re-introduced and the test confirmed to fail.

## v0.1.497

A review of v0.1.496 raised seven items. Six are real and fixed here; one was a methodology
warning that did not apply to this repo's checks. **Item 1 is mine: I applied two remedies
that were offered as alternatives.**

### Fixed

- **A genuinely oversized column no longer reads "Ready", and the two engines agree again.**
  R-5 offered a choice — downgrade the grade OR filter by CHECK — and v0.1.495 took the first
  while v0.1.496 took the second, so both landed. With the false positives gone the downgrade
  was no longer buying anything and was actively harmful: a `bytea` holding 1,114,112 bytes
  read `AUTO` / **100 "Ready"** while an identical MySQL `longblob` read `MANUAL` / **57
  "Moderate effort"** — the one unrecoverable DSQL limit, graded opposite ways by engine.
  - Restored by SPLITTING on what the type declares, rather than reverting: `bytea` /
    `json` / `jsonb` are a **LOSS** (choosing one is the intent to store something large, and
    every member of MySQL's set is likewise deliberately-large), while an unbounded
    `text` / `varchar` stays a **RECOMMENDATION** (PostgreSQL's idiomatic spelling for an
    ordinary short string). Measured both ways first: a plain reversion would have put an
    18-table schema of ordinary `text` columns back into per-table manual work, which is what
    the v0.1.495 downgrade was for. Now `bytea` = 57 both engines, 18 ordinary `text` tables =
    100 "Ready".
- **Two holes in the v0.1.496 CHECK predicate, both of which HID a real risk.** Verified
  against real PostgreSQL:
  - It matched an `EQ` node ANYWHERE in the parsed tree, so
    `status = ANY (ARRAY['a','b']) OR body IS NOT NULL` excluded `status` (the OR lets a row
    satisfy the CHECK via the other branch) and `NOT (other = ANY (...))` excluded `other`
    (the negation permits everything else). Only a top-level AND-conjunct bounds a column now.
  - It treated a **`NOT VALID`** CHECK as a bound. Such a constraint was never checked against
    the rows already stored, so a multi-megabyte value written before it was added is still
    there — and Full Load reads exactly those rows. The flag was not even captured;
    `CheckConstraintDef.not_valid` now reflects it from SQLAlchemy's `dialect_options`.
- **The engine-wiring call site is tested, and the remaining sites now honour a restored
  session.** Deleting the renderer's `source_type` argument left the ENTIRE suite green
  (measured: 4009 passed) — the three mutations quoted in the v0.1.495 changelog all targeted
  `_models.py`, not this second link, so that claim overstated the coverage. Also,
  `session_source_type` had reached only 2 of the session-based sites; the watermark panel,
  `_cdc_source_type` and Schema Conversion's `_src_type` still read `source_config` directly
  and so ignored the restored hint. The last one mattered most: on a resume it converted a
  PostgreSQL inventory with the MySQL dialect and put AUTO_INCREMENT back in the PK picker.
  A test now fails if any of them regresses to a raw read.
- **Evaluation reports a NON-primary-key sequence column** (`NON_KEY_SEQUENCE`). Schema
  Conversion warned that such a column reaches the target with neither identity nor default,
  while Evaluation said nothing — the same contradiction v0.1.496 closed for the KEY column,
  one column over. Graded a LOSS rather than advice: for the key the operator gets a choice at
  Schema Conversion, for a non-key column the generation is simply gone.
- **An aggregate no longer blanks every routine body in its schema.** `pg_get_functiondef`
  raises `WrongObjectType` for an aggregate or window routine, and that error ABORTS THE
  TRANSACTION — so one aggregate made every SUBSEQUENT object-detail lookup on the same
  connection fail too, each reported with the false "the object may have been dropped or you
  may lack permission". Confirmed live: asking for the aggregate first turned a healthy
  neighbouring function's body into `None`, while a fresh connection returned it. The lookup
  is now restricted to `prokind IN ('f','p')`. The review reported the symptom for aggregates
  only; the transaction-abort blast radius was wider.
- **A schema-qualified argument type no longer cross-matches an overload.** The
  bare-last-segment arm was computed on the whole name, so the tail of `app.f(b geo.point)` is
  `point)` — which also ends `app.f(a geo.point)`. Asking for one overload returned the other,
  making the v0.1.495 claim that "a supplied signature still matches exactly" false in that
  case. Both sides are now compared with the signature stripped.
- **The drift JSON no longer says `"drifted": false` when nothing was comparable.** Added a
  `determinable` sibling field rather than making `drifted` nullable, so a consumer reading
  `drifted` as a bool keeps working and one wanting the truth has a field to read. Derived
  from `basis`, which already encodes it.
- **The manual's `OVERSIZED_LOB` grade is accurate again** in all three languages, including
  that a CHECK-limited column is not flagged at all.

### Not applicable

- The review warned that a mutation check run from a `/tmp` copy is invalid because pytest
  imports the installed package. True in general, but not what happened here: these checks
  mutate files IN PLACE under `src/` (an editable install, so the edit is what runs) and
  restore from a backup afterwards. The decisive evidence is that the mutations FAILED — a
  mutation that was not seen would pass. The two that unexpectedly PASSED were investigated
  rather than accepted, and both turned out to be the check's own fault (one vacuous
  assertion, one string that never matched).

## v0.1.496

Two inaccuracies a workshop Evaluation surfaced on a real PostgreSQL schema.

### Fixed

- **A `text` column a CHECK limits to a literal set is no longer reported as a 1 MiB risk.**
  `products.status`, `orders.status` and `product_media.media_type` — each restricted by a
  CHECK to 3-5 short values — were all reported as "no length limit … can exceed 1 MiB". Two
  things were wrong: the statement is false (such a column cannot hold an oversized value),
  and it buried the genuine one, since the same table's `content` (a `bytea` actually holding
  1,114,112 bytes) was listed at the same grade.
  - **The tool already had the answer.** It reads and DISPLAYS those CHECK expressions in the
    same report (the dropped-CHECK note quotes each name and expression); only the LOB rule
    looked at the type alone. No new probe — Evaluation, Schema Conversion and the UI's
    exclusion offer now share one table-aware predicate, so a CHECK-limited column is also
    no longer OFFERED for exclusion (excluding it would drop a column for nothing).
  - Parsed with sqlglot, not pattern-matched: PostgreSQL rewrites `IN (...)` into
    `= ANY (ARRAY[…::text])`, so the stored form is not what the user typed. A CHECK that
    merely mentions the column (`notes <> ''`) bounds nothing and is correctly ignored. A
    `length(col) <= n` CHECK does bound it but is deliberately NOT claimed — judging "small
    enough" needs a byte-versus-character call against the 1 MiB cap, so such a column is
    still reported, which is the safe direction.
  - The **severity** half of this was already fixed in v0.1.495 (re-rated to
    `RECOMMENDATION`, so the readiness score is back to 100 "Ready" on a `text`-only schema
    rather than 57). This fixes the false statement itself.
- **Evaluation now says WHO generates the primary key, not just that it could be faster.**
  For a source whose keys are `AUTO_INCREMENT` / `GENERATED AS IDENTITY`, Evaluation said the
  key "converts cleanly and works as-is" and framed everything as "optional, for throughput
  only". Under the DEFAULT conversion the target column is a plain integer with **no
  identity**, so the application must supply the value on every insert — an app-code change,
  which Schema Conversion does state ("IMPORTANT: Aurora DSQL will NOT auto-generate this
  key") while the go/no-go artifact did not. The two steps disagreed about the same key.
  - **Engine-independent**: the MySQL rule carried the same wording, so a MySQL migration was
    equally under-informed.
  - Still calibrated as ADVICE, not a failure — the v0.1.151 correction that stopped this
    reading like a defect stands. The throughput half remains explicitly optional; what was
    added is the decision the operator has to make.

### Reviewed and NOT changed

- The DSQL rule that an identity column must be `bigint` is real but CONDITIONAL and already
  handled: it applies only if the operator picks the Server-generated (IDENTITY) strategy, and
  the converter then widens `integer` to `BIGINT` itself (verified by execution). Under the
  default (Keep) no identity is created, so `integer` is correct. Nothing to fix; the
  recommendation now mentions the widening so the choice is informed.
- The identity CACHE constraint (1 or >= 65536) has no user-reachable violation: the converter
  always emits `CACHE 65536`.

## v0.1.495

A verification pass over v0.1.491-493 confirmed 20 of 26 filed items resolved, and found
**14 defects the fixes themselves introduced**. All are fixed here. Two were self-inflicted
regressions in shipped behaviour; one is a case where the reasoning I used to justify a
v0.1.493 change condemns another part of the same change.

### Fixed

- **An extension-created schema no longer takes the user's objects with it.** v0.1.493 added
  a SCHEMA-level exclusion to `list_schemas` as "belt-and-braces" over the per-object
  filters. It is strictly COARSER than them: a schema is a container, so excluding PostGIS's
  `tiger_data` or pg_cron's `cron` also excluded whatever the user put inside — and people do
  (the TIGER loader's own tables live there). That table vanished from Evaluation, could not
  be listed for Full Load, and Validation reported MATCH over a set that silently excluded
  it, with nothing to explain any of it. Removed; the per-object `deptype='e'` filters
  already empty such a schema of the extension's own objects while keeping the user's.
  - The same commit justified requiring `classid` on the ground that **a missing finding is
    strictly worse than an over-reported one**. That argument applies against this layer, and
    a test now asserts `list_schemas` does *not* filter.
- **PostgreSQL routine bodies are no longer always empty.** Putting the identity arguments
  into a routine's name (so overloads are distinguishable) broke the definition lookup, which
  still matched `proname = :name` — `order_count()` can never equal `order_count`. Every
  PostgreSQL function and procedure returned an empty body through the AI object-detail tool,
  with a *false* reason blaming a dropped object or a missing privilege. Dot-splitting was
  wrong for a second reason too: a schema-qualified argument type (`app.f(a app.hstore)`) has
  three parts. Now matched against all four renderings a name can arrive in, so an overload
  asked for by signature returns its own body.
- **The friendly name works again in the AI object-detail tool.** Same root cause: only the
  full name or the last dot-segment matched, so `order_count` — what an operator or the model
  actually says — returned `not_found` for an object Evaluation had just listed. A supplied
  signature still matches exactly, so one overload never resolves to another.
- **A validation RE-CHECK is no longer reaped as stalled.** The v0.1.491 liveness fix covered
  the checksum helpers but not this sibling call path: the re-check passed no `on_page`, so
  the Validator's page hook was inert and the watchdog failed the job at 900 s — reporting a
  read-only comparison with the *Full Load* wording "the load appears to have stalled", while
  the comparison itself completed. Re-checking one large mismatched table is exactly what the
  button is for. **Engine-independent: MySQL sources were affected too.**
- **A non-primary-key `GENERATED AS IDENTITY` column no longer loses its value generation
  silently.** The v0.1.492 fix keyed on a `nextval(` default, but a PostgreSQL identity column
  has **no `pg_attrdef` default at all** — so only the legacy `serial` spelling was covered and
  the PG10+ *recommended* one stayed silent. `attidentity` is now recorded for every column,
  not only the key, via a new `ColumnDef.identity`.
- **`OVERSIZED_LOB` is advice, not per-table manual work.** Adding `text` to the PostgreSQL
  set made the rule fire on nearly every real schema, because `text` is PostgreSQL's idiomatic
  string type — a plain schema read "Moderate effort" purely because a column has no length
  limit, with no evidence any value approaches 1 MiB. Re-rated to `RECOMMENDATION`, matching
  its sibling rule. That is the same unusable-signal shape v0.1.493 fixed for extension
  objects, reintroduced one release later in a different place.
- **The paste-ready `ALTER TABLE … ADD CONSTRAINT … CHECK` now quotes identifiers.** It is the
  only in-tool route to re-creating a dropped CHECK, so for a mixed-case or spaced name the
  remedy itself was a syntax error — while the `CREATE TABLE` beside it quoted correctly.
- **The Schema Conversion primary-key picker speaks the source's language.** v0.1.491 fixed
  the converter warnings and Evaluation but not the picker, where the decision is actually
  made: a PostgreSQL operator was told about AUTO_INCREMENT, and the "no alternative applies"
  hint explained the absence in MySQL terms.
- **The drift DETAIL text is engine-correct.** The summary line was made source-neutral in
  v0.1.491; the detail a PostgreSQL operator reads on the sign-off report was not, so it
  explained the verdict with two coordinates their database does not have. It now says why
  drift is deliberately not computed from a WAL LSN.
- **The oversized-LOB pre-tick can no longer disagree with the offer.** The reverse map was a
  second, hand-kept copy of PostgreSQL type knowledge, already missing `bpchar`/bare `varchar`,
  and it stripped the length modifier — so a bounded `character varying(50)` fell into the
  `text` bucket and could be pre-ticked in a dialog whose confirm NULLs the column for every
  row. Both halves are now gated on the single `is_pg_oversized_lob_type` predicate.
- **A restored session keeps its engine.** The new PostgreSQL-aware call sites read only
  `source_config`, which a snapshot without connection coordinates leaves `None` — so right
  after a restore the pre-load LOB panel claimed "no oversized LOB columns" for a schema full
  of `text`/`bytea`, the pre-tick was empty, and the watermark panel labelled MySQL rows. One
  `session_source_type` helper now consults the restored hint, as `ui/connect.py` already did.

### Added

- **One finding for a source database collation Aurora DSQL does not match**
  (`PG_DATABASE_COLLATION`). The v0.1.491 per-column capture deliberately ignores the
  collation named `default` — that is not a collation, it is "this database's default" — which
  hid the ORDINARY case: on a stock RDS/Aurora PostgreSQL every text column inherits
  `en_US.utf8` while DSQL runs `C`. Measured on the live cluster: DSQL reports
  `datcollate = 'C'`, so `'Bob' < 'alice'` is TRUE and `ORDER BY` yields `A, B, a, b`; the same
  queries on `en_US.utf8` give FALSE and `a, A, b, B`. Ordering and range predicates change
  after cut over while every row count and checksum still matches. Worded not to overstate it:
  equality and UNIQUE enforcement are unchanged.

### Testing

- **The load-bearing line behind the quarantine-recovery button now has a real test.** Its
  only guard was an AST assertion that the `source_type` keyword *appears* — which a
  hard-coded `SourceType.MYSQL` would also satisfy — and it guarded a different call site than
  the one that can regress. I had recorded that a behavioural test was impossible because the
  helper was a closure; the honest fix was to stop making it a closure. It is now the
  module-level `make_lob_candidates_for(inventory, session)` factory, and the test drives the
  shipped body. It fails under all three ways the wiring can regress.
- The shared PostgreSQL test double gained `.first()`, without which the single-value catalog
  probes hit their own best-effort `except` and returned `None` — a probe test would have
  passed for the wrong reason.

### Unchanged (reported, and correct as shipped)

- The "no slot in this run" branch of the CDC slot-health fix is unreachable in the running
  app — which its own docstring already states as defensive-only, so there is nothing to fix.
- `is_pg_oversized_lob_type` treating a bare `character`/`char` as unbounded is unreachable:
  PostgreSQL's `format_type` always renders a modifier-less char as `character(1)`. Those two
  spellings are dropped as dead weight, with the reason recorded.

## v0.1.494

### Security

- **Upgraded the transitive `anyio` dependency to 4.14.2**, closing two advisories Dependabot
  raised against the shipped lockfile:
  - `CVE-2026-63374` / `GHSA-82r6-8w77-94w6` — **critical** (CVSS 4.0 score 9.3).
    `TLSStream` encoded host names with IDNA 2003, so a hijacked connection to an
    **internationalized** domain could be validated against a certificate obtained for the
    IDNA-2003 spelling of that name — TLS certificate spoofing.
  - `CVE-2026-64847` / `GHSA-5p39-cfhj-2xmp` — medium (6.8). anyio process-pool workers are
    started with `stderr` on a pipe that is never drained, so a worker writing enough to
    `stderr` fills the pipe and wedges the awaiting call.
- **Practical exposure here was low, and the patch is applied anyway.** `anyio` reaches this
  project only under the NiceGUI/Starlette/uvicorn web stack and `httpx`; the tool's own
  outbound TLS goes to AWS endpoints through botocore/urllib3 and to the source database
  through pymysql/psycopg, none of which use `anyio`'s `TLSStream`, it connects to AWS and
  database endpoints by ASCII host name, and it uses no anyio process pool. A critical
  advisory against a library inside the published image is worth shipping a patch for
  regardless of reachability.
- Lockfile only — no first-party code changed. The full suite passes and the UI was booted
  on 4.14.2 to confirm the web stack is unaffected.

## v0.1.493

A fix-request note reported that PostgreSQL introspection reports an extension's functions
as the user's objects. Confirmed, and it is worse than reported: **an extension's own tables
were migration targets, so their rows were actually loaded into the target.** The note's own
proposed fix for triggers is also inert and is replaced.

### Fixed

- **An installed extension no longer wrecks the Evaluation verdict.** `_pg_collect_routines`
  filtered on the schema only, so every function an extension provides was collected and
  rated `UNSUPPORTED / SIGNIFICANT` — advising the operator to "reimplement as a LANGUAGE
  SQL function" a C function they did not write and cannot reimplement. Measured live on a
  textbook `CREATE EXTENSION pgcrypto;` (no `SCHEMA` clause, so it lands in `public`, which
  the tool always sweeps) over a **one-table database with zero user functions**: 37 objects
  / 36 UNSUPPORTED and a readiness score of **2/100 "Significant effort"**, against 1 object
  / 0 UNSUPPORTED and **57/100** for the same user schema without the extension.
  - Nothing warned, and there was no provenance to filter on downstream: `ObjectRef` has no
    such field, and `pg_depend` / `pg_extension` / `deptype` appeared **nowhere** in the
    package.
  - The phantom count did not stay in Evaluation: the same `inventory.routines` feeds the
    Schema Conversion object tree and its "N stored routines are not shown" notice, N bogus
    conversion warnings, and the AI briefing — where it is passed as "authoritative facts
    you MUST respect and never contradict".
- **An extension's own TABLES and VIEWS are no longer migrated.** The most consequential
  part, and not in the note: `get_table_names` / `get_view_names` apply no extension filter,
  so PostGIS's `spatial_ref_sys` / `geometry_columns`, pg_partman's `part_config` and the
  like arrived as ordinary tables — and because the default selection is *all* tables, Full
  Load **wrote their rows to the target**, Schema Apply was handed a view whose body calls a
  C function, and Validation then compared objects the user never created. Dropped in place
  during enrichment, exactly as `_pg_apply_partitioning` already drops partition children.
- **A schema an extension created is no longer treated as the user's.** `list_schemas`
  excluded only real system schemas, so PostGIS's `topology` / `tiger` / `tiger_data` and
  pg_cron's `cron` were enumerated as user schemas and everything inside them flowed into
  the migration.
- **Extension-owned triggers, materialized views and foreign tables no longer produce bogus
  findings.** `NOT tgisinternal` does not cover this: it marks only SYSTEM-generated
  constraint triggers, so a trigger an extension creates in its install script reads as the
  user's.
- **Overloaded functions are distinguishable in the report.** The name came from `proname`
  alone, discarding the signature, so PostgreSQL overloads produced rows nothing could tell
  apart — and because findings are bucketed by object name, each duplicate row repeated
  every sibling's concerns, rendering 3 overloads as **9** table rows. Names now carry the
  identity arguments (`crypt(text, text)`), which needs no extra privilege and renders a
  zero-argument routine as `sync_all()`.

### Added

- **One finding per installed extension** (`PG_EXTENSION_UNSUPPORTED`, MANUAL/MEDIUM). This
  is the pairing that makes the filtering above honest: Aurora DSQL provides no user
  extensions, so application SQL calling `crypt()`, `ST_Contains()` or
  `uuid_generate_v4()` has a real incompatibility. Before, that fact was technically
  present but unusable (76 rows); simply removing those rows would have replaced an
  unusable signal with **no** signal. Now it is one line per extension that says which one
  and what to do, and the risk text states that the extension's own objects were excluded so
  nobody goes looking for them.

### Corrected (the note's proposed fix for triggers does nothing)

- The note proposed one shared `pg_depend` fragment with only `classid` swapped. For
  **triggers that is a silent no-op**: a trigger created by an extension's install script is
  *not* recorded as an extension member — verified against a purpose-built extension, its
  only `pg_depend` rows are `'a'`→its table and `'n'`→its function — so a
  `pg_trigger`-keyed test is always false. It has to key on the trigger's **table**. Keying
  on the trigger's *function* would be wrong in the other direction: a genuine user trigger
  calling contrib `moddatetime` would silently vanish from the report. The shared fragment is
  therefore parameterised by `(classid, oid expression)`, and a test asserts the trigger
  query does **not** mention `pg_trigger` — it fails if the note's version is applied.
- `classid` is required rather than decorative: `pg_depend.objid` "references any OID
  column", so an object is identified by the pair `(classid, objid)` and OIDs are not unique
  across catalogs. Omitting it risks silently dropping a real user object — a missing
  finding, strictly worse than the over-reporting being fixed.
- The filter is object-level, so a user function that merely LIVES in an extension's schema
  is still reported (live-verified). `deptype='e'` is the same line `pg_dump` draws, which
  makes the tool's notion of "the user's objects" match it.

## v0.1.492

A fix-request note reported that a PostgreSQL source's oversized `text`/`bytea` values pass
all three gates in silence and that the post-quarantine recovery button disappears. All of
it is confirmed, plus one more instance of the same root cause. **One of the note's own
conclusions was backwards and is corrected here.**

### Fixed

- **A PostgreSQL migration gets its post-quarantine recovery button back.** The
  "Exclude column & reload" action's candidate list came from
  `lob_exclusion_candidates(inventory)` with no `source_type`, so it fell back to the MySQL
  default and matched `mediumtext`/`longblob` against a PostgreSQL inventory's `text`/`bytea`
  — nothing matched, and the renderer's "no candidates → do not offer a dead button" early
  return then hid the only in-tool route forward for every PostgreSQL source. The other two
  callers of that function pass the engine; this one, added later, never picked it up.
  - **The loss is the RECOVERY ROUTE, not the data.** The oversized value is loudly
    reported either way (per-row quarantine, the amber completeness banner, the Validation
    deficit). What a PostgreSQL operator lacked was the fix: the pre-load exclusion panel is
    locked once a load has committed and its own text points at *Start over*, which discards
    Evaluation, Schema Conversion (including hand/AI-edited DDL) and the CDC inputs. The
    per-table banner meanwhile advises "fix the source value and reload", impossible on a
    read-only source.
  - PostgreSQL is strictly more exposed to this than MySQL: `text`/`bytea` are **unbounded
    by default**, whereas a MySQL user has to choose `mediumtext`/`longblob` deliberately.
  - Pinned by an AST assertion on the call site. A behavioural test cannot reach it — the
    helper is a closure inside the screen builder, and every existing test of this button
    injects it as a lambda, which is exactly why the defect survived (one of them even pins
    the "empty → no button" branch it exploits).
- **Evaluation and Schema Conversion no longer stay silent about oversized PostgreSQL
  columns.** The manual documents the Evaluation `OVERSIZED_LOB` flag as the signal for
  this and names PostgreSQL explicitly, yet the rule could not fire: it was excluded from
  the PostgreSQL rule set, and merely registering it would have found nothing forever
  because it matches MySQL type NAMES while a PostgreSQL inventory holds `format_type`
  spellings. Schema Conversion suppressed its counterpart too — oversized-LOB was the only
  entry in that engine-branched tuple with no `_pg_*` pair. Added `PgOversizedLobRule` and
  `_pg_oversized_lob_warning`, both reading one shared predicate now in `core.assessor`, so
  Evaluation, Schema Conversion and the UI's exclusion offer cannot disagree about which
  columns are at risk.
  - This is the one DSQL limit whose breach **cannot be undone by reloading** — the value
    does not fit — which is why silence at the go/no-go step mattered.
- **`json` / `jsonb` are now covered, and an unbounded `varchar` with them.** See below: the
  reasoning that excluded them was wrong.
- **The exclude dialog pre-ticks the right column for a PostgreSQL source.** The quarantine
  reason names the DSQL *type*, and PostgreSQL → DSQL is the identity for these types, so
  the MySQL map (`mediumblob`→`bytea`) matched nothing and the dialog opened with nothing
  selected. Kept as a separate PostgreSQL map rather than merged into the MySQL one: MySQL
  has its own `text` type, and although it cannot reach this helper today, merging would
  make that exclusion load-bearing for correctness. A `jsonb` reason no longer also matches
  the `json` entry.
- **The manual no longer claims the capture-stage exclusion is "driven by the Evaluation
  `OVERSIZED_LOB` flag".** Tracing `column.exclude.list` shows no such dependency for
  *either* engine: the exclusion is an opt-in card on the Data Migration / CDC step reading
  the same type set. Reworded in all three languages (the `DsqlSinkTask` Javadoc carries the
  same sentence and is deliberately left alone — editing connector source would require a
  `PLUGIN_VERSION` bump and a Delete + Deploy cycle on live CDC stacks for a comment).
- **A restored or refreshed PostgreSQL session no longer exports a report titled "MySQL to
  Aurora DSQL".** Found by sweeping for the note's root cause (an engine default the caller
  does not pass): four sites rebuild `EvaluationResult` and dropped `source_type`, so after
  clicking Generate in Schema Conversion, either Refresh-browser button, or any app restart,
  the Step-1 deliverable was mislabelled. The three in-place rebuilds now use
  `dataclasses.replace`, so the next field added cannot repeat this.

### Corrected (the note's conclusion was backwards)

- **`json`/`jsonb` are NOT exempt from the 1 MiB cap.** The note argued the code was right
  to exclude them and the manual was the looser statement, citing a measurement: a
  1,117,396-character text value reported `pg_column_size` 4400, i.e. it compressed. That
  observation is true and settles nothing:
  - DSQL auto-compresses `text`/`varchar`/`bpchar` **as well as** `json`/`jsonb`, so
    compression cannot discriminate between them. If it justified dropping `json` it would
    equally justify dropping `text`, leaving the set empty.
  - The documentation puts the 1 MiB limit on `bytea`, `text`, `json` and `jsonb`, and for
    `json`/`jsonb` states it applies to the **compressed** size — which relocates the cap,
    it does not remove it.
  - This is the same mistake v0.1.468 spent a release reverting: a compression probe does
    not overrule a documented quota. Acting on the note as written would have loosened
    customer-facing docs in three languages.
  So the code comment was wrong and the manual was right. The set is now
  `{text, bytea, json, jsonb}` plus an unbounded `character varying` (length-aware, so
  `varchar(50)` is untouched), and the corrected reasoning is recorded beside it.

### Known and deliberately not changed

- The CDC sink's pre-write 1 MiB guard measures the **raw** byte length, while the
  documented cap for `json`/`jsonb` and `text` is on the **compressed** size. A highly
  compressible document between those two points is dead-lettered although DSQL would have
  accepted it. It is visible (DLQ depth, Validation's missing count), not silent. Closing it
  means letting DSQL arbitrate — attempt the write and dead-letter on `54000` — which is a
  deliberate connector design change requiring a `PLUGIN_VERSION` bump and a Delete + Deploy
  infrastructure cycle, so it belongs in its own decision rather than folded in here.

## v0.1.491

A PostgreSQL-source path audit reported 17 findings. All 17 were re-verified against the
code and then attacked by three independent skeptics each (is the reading right, does it
reproduce, is the proposed fix right). **16 stand and are fixed below; one was refuted and
the current behaviour pinned instead.** Three of the audit's own conclusions were wrong and
are corrected here rather than implemented as written.

### Fixed

- **A column excluded from the migration is no longer missing from the Validation report.**
  `run_validation` strips the operator's excluded columns from each `TableDef` *before* the
  Validator runs — which is what makes the comparison correct, since a column with no target
  data would otherwise mismatch on every row. But `checksum_excluded_columns` is then
  computed from that already-stripped list, so the column appeared in neither the comparison
  nor the "columns not compared" disclosure: the report printed
  `Data identical: yes (N/N tables matched)` with no trace that a column holds no data on
  the target at all. The exclusion is now stamped onto the finished report
  (`TableValidationResult.migration_excluded_columns`) and stated in the text/JSON report and
  the readiness panel.
  - **Engine-independent — MySQL was affected identically.** `_apply_column_exclusions` has
    no engine branch.
  - The verdict is deliberately untouched: skipping the column is right, only the silence
    was wrong. Reported in every mode, and worded as "holds no data on the target" rather
    than "not compared", because the reason and the remedy differ from the FLOAT/JSON case.
- **A long CHECKSUM validation can no longer be reaped while healthy.** `_target_checksum`
  accepted an `on_page` liveness hook but did not forward it to the keyset loop, and
  `_source_checksum_for` had no such parameter — so for a single-column-PK table (the common
  case) *neither* side beat the watchdog for the whole scan, which is the longest stretch of
  a run with no other statement in between. This is exactly what `set_page_hook` exists to
  prevent.
  - The obvious test for this is vacuous: the row-count helpers beside these already
    forwarded the hook, so "did the hook fire during validate()?" passes with the defect
    fully present (confirmed by reverting the fix — it still passed). The regression test
    drives the two checksum helpers directly.
- **Evaluation no longer stays silent about three PostgreSQL conditions Schema Conversion
  warns about.** The v1 rule set excluded them as "MySQL-specific", but the distinction that
  matters is whether a rule reads a MySQL *type string* or a *structural field*:
  - `numeric(40,10)` read AUTO at Evaluation even though `clamp_pg_numeric` will silently
    clamp it at Schema Conversion — `_DECIMAL_BASES` already contains `numeric`, so the
    shared rule was correct for PostgreSQL all along and is simply registered now.
  - A PostgreSQL **generated column** read "compatible" at Evaluation while the converter
    warns it becomes an ordinary column that drifts on the first write. It was not even
    listed in the exclusion docstring.
  - A **serial / identity** key got the hot-partition recommendation at Schema Conversion but
    not at Evaluation.
  - The latter two get PG-worded rules rather than the shared MySQL classes, whose text says
    "MySQL generated columns" and "AUTO_INCREMENT" — a list-only fix would have put a MySQL
    feature in front of a PostgreSQL operator.
- **A non-primary-key `serial` / identity column no longer loses its sequence default
  silently.** `pg_column_default_sql` discarded every `nextval(...)` with no warning on the
  premise that "the primary-key strategy governs it" — true only for the key column, since PG
  enrichment sets `auto_increment_column` for a primary key only. Any other sequence column
  reached the target with neither identity nor default and nothing said so, so the first
  INSERT that omits it writes NULL instead of the next number. Keyed on the primary key, not
  on `auto_increment_column`, so an un-enriched inventory cannot produce a wrongly-worded
  note about its own key column.
- **The dropped-CHECK note now shows the expression and the statement to re-add it.** It
  said the expression "is shown in Evaluation" — untrue, Evaluation lists constraint *names*
  — and told a PostgreSQL operator about "a MySQL CHECK expression". The expression was
  captured all along (`CheckConstraintDef.expression`) and simply never displayed. The note
  now prints each constraint with its expression plus a ready-to-run
  `ALTER TABLE … ADD CONSTRAINT … CHECK (…) NOT VALID`.
  - Automatically re-emitting the CHECK is deliberately **not** done: DSQL supports CHECK,
    but a source expression can use functions DSQL does not accept, and carrying constraints
    over is a post-load pipeline (as foreign keys are) that needs its own design and live
    verification — not a side effect of a wording fix.
- **PostgreSQL CDC now checks WAL retention, the analog of the MySQL binlog-retention
  check.** CDC resumes from the slot created at the Full Load snapshot point, and a finite
  `max_slot_wal_keep_size` lets the source discard that WAL and invalidate the slot — the same
  silent Full-Load-to-CDC gap the MySQL check exists to catch, and it was not checked at all.
  WARN-only with the same calibration (`-1` = unlimited = PASS; unknown = INFO). Read from
  `pg_settings`, not `SHOW`, which renders a unit-suffixed string.
  - `BINLOG_RETENTION` also reappears as a SKIP in PostgreSQL CDC mode: it was present as a
    SKIP in the *weaker* Full Load report and absent from the stronger one.
- **The replication-slot health panel no longer vanishes when the stack name drifts.** It
  re-derived the slot name from the mutable stack name, while every other PostgreSQL path
  (`dispatch_source_config`, `_drop_pg_source_replication`) prefers the recorded/deployed
  name — their comments say why. Since the derived name embeds a hash of the stack name, an
  attach/rename/restore pointed the health read at a slot that never existed, so the whole
  WAL-pressure panel silently disappeared for a live, healthy slot. The deployed `PgSlotName`
  is now read from the same describe the phase probe already performs.
- **The Query Playground reads the source engine's dialect.** It hard-coded the MySQL
  parser. For a PostgreSQL migration that is not cosmetic: `"user_name" || '@' || domain`
  parses as MySQL into `'user_name' OR '@' OR domain` — a **different query**, still labelled
  AUTO ("no review needed"), which the Test button would then run against the real target.
  PostgreSQL-only syntax (`@>`, `::jsonb`) instead failed to parse, fell to `OTHER`, and so
  could not be tested at all. The MySQL-only rewrites (`ON DUPLICATE KEY UPDATE`,
  `JSON_UNQUOTE`) are skipped for a PostgreSQL source, and a parse error names the right
  engine.
- **Four PostgreSQL prerequisite checks are filed under the right heading.**
  `_PREREQ_CATEGORY_BY_CHECK` mapped 12 of the (then) 18 check ids and the lookup falls back
  to "Schema & Tables", so `wal_level`, the replication role, slot headroom and the
  writer check — all server settings/privileges — were filed as per-table readiness, leaving
  "Source Configuration" nearly empty on a PostgreSQL CDC run. Gating never depended on the
  category, so nothing functional was wrong. A test now asserts the map covers the enum,
  which is how five checks slipped in unnoticed.
- **The primary-key strategy notes no longer advise a PostgreSQL operator about
  AUTO_INCREMENT.** The shared DSQL-constraint phase runs for a PostgreSQL source too
  (enrichment sets `auto_increment_column` for a serial/identity key), so all three strategy
  messages named a MySQL feature the database does not have. The MySQL strings stay
  byte-identical — a committed conversion snapshot pins them.
- **The watermark panel trusts the engine it knows instead of guessing.** It inferred the
  engine from `bool(watermark.wal_lsn)`, but a PostgreSQL Full-Load-only LSN read is
  best-effort (`None` without the privilege), so a PostgreSQL run whose LSN came back empty
  rendered MySQL binlog/GTID/server-UUID rows and reported an "unavailable binlog coordinate"
  for a source that has no binlog.
- **A non-default PostgreSQL column collation is now captured and disclosed.**
  `_reflect_tables` hard-codes `collation=None` (SQLAlchemy does not supply it) and only the
  MySQL enricher filled it, so the collation warning could never fire for a PostgreSQL
  source. A column collated case-/accent-insensitively landed under DSQL's default
  collation, changing `=`, `LIKE`, `ORDER BY` and UNIQUE semantics while every row count and
  checksum still matched. Any non-default collation is reported, because a PostgreSQL
  `collname` is an arbitrary name whose sensitivity cannot be read off the string the way
  MySQL's `_ci` suffix can.
- **The text report no longer claims "Drifted: no" with nothing to compare.** `drifted`
  defaults to `False` when no coordinate was comparable, which the report printed as a flat
  all-clear on the one line an operator reads to confirm a pre-cut-over write freeze held —
  and that is the state of *every* PostgreSQL run, whose MySQL probes are deliberately
  skipped to keep the shared snapshot intact. Determinability is now derived from `basis`,
  exactly as the UI already does, and the undetermined message is source-neutral.
  - The audit's proposed fix — read the WAL LSN and treat an advance as drift — is **not**
    implemented, and should not be: an idle PostgreSQL source advances its LSN on its own
    (autovacuum, checkpoints, `wal_level=logical` bookkeeping), so that comparison would
    report drift on a source nobody wrote to. Making the existing report honest is the
    correct fix; a real PostgreSQL drift signal needs a quiescence-safe coordinate.
- **`timetz` stays offset-agnostic when the applied target types are unavailable.** The
  `timetz` arm sat inside `if applied:`, so a table whose applied types could not be resolved
  fell back to a raw `::text` render on both sides. The CDC sink stores `timetz`
  UTC-normalized (Debezium `ZonedTime`) while Full Load keeps the source offset, so every
  CDC-written row false-MISMATCHed — a blocking cut-over verdict on correct data. An applied
  type still wins, so a deliberate remap away from `timetz` is honoured.
  - The audit's stated trigger (a reconnect empties `target_type`) is wrong and was verified
    not to happen: the conversion state object always exists by the time Validation reads it,
    and a session restore rehydrates it. The reachable route is a PK strategy whose DDL
    clause does not parse.

### Unchanged (reported, and the current behaviour is correct)

- **`jsonb` stays in the checksum.** The audit asked for `jsonb` to be excluded alongside
  `json`. It must not be: PostgreSQL stores `jsonb` decomposed and re-serializes it through
  `jsonb_out` on read, so both ends emit the same canonical text whatever wrote the row —
  the CDC sink's compact form included. `json` keeps its bytes verbatim, which is why it has
  no byte-identical cross-engine form. Broadening the check would have dropped the type
  customers actually use out of the only value-level verification there is. A test already
  pinned the distinction; the reasoning is now recorded beside it so it is not "fixed" again.

## v0.1.490

### Fixed

- **A CDC teardown can no longer delete a NEWER deployment's source credentials.** The teardown
  polls the stack every 30 s while the UI re-probes every 5 s, so for up to ~30 s after the stack
  vanishes the UI can already offer a fresh deploy — whose `ensure_source_secret` upserts the same
  deterministic secret name. The still-running cleanup then scheduled THAT secret for deletion
  (7-day recovery), the deploy succeeded, and Start CDC failed minutes later with the Debezium
  source unable to read its credentials — surfaced as a generic "&lt;connector&gt; entered FAILED
  state", with no self-heal (the Start pass never re-upserts the secret and the connector reads it
  by name). The cleanup now deletes only a secret **not written since the teardown began**.
  - **Freshness, not ownership.** A tag or the ARN cannot discriminate here: the racing upsert is
    from the same tool for the same stack name, so any ownership marker matches on both sides. Only
    "was this written after I started tearing down?" separates them.
  - **Fails safe.** If the last-changed time cannot be read, the secret is left alone and the log
    says so: skipping a cleanup costs a lingering secret an operator can delete, while deleting the
    wrong one costs a pipeline that cannot start and gives no reason. Callers that pass no
    timestamp keep the old behaviour with no extra API call.
  - Verified before implementing that this guard cannot silently fail open: `secretsmanager:
    DescribeSecret` is **allowed** on both live TaskRoles (`iam simulate-principal-policy`
    against `mysql-dsql-migrator` in us-east-1 and `mysql-dsql-migrator-seoul` in ap-northeast-2),
    which mattered because both stacks were created from older template revisions.

## v0.1.489

### Fixed

- **The table picker no longer tells you to start a deletion that is already running.**
  `cdc_infra_prep_state` folds EVERY non-stable stack status into `"ready"`, so a stack being
  torn down reached the CDC-infrastructure clause and its remedy read "delete the CDC
  infrastructure on the CDC step first" — impossible to comply with, and it then unlocked
  ~15–25 minutes later with no explanation. The lock's own premise does not hold there either: it
  exists because the stack OWNS each table's immutable Kafka topic partition count, and a stack on
  its way out owns nothing. Narrowed to the `DELETE_IN_PROGRESS` literal (and an in-flight delete
  job), **not** generalised to "any teardown": Stop CDC is an UPDATE that leaves MSK, the topics
  and that partition plan in place, so unlocking there would let a table be added and then streamed
  forever on a single partition — the exact harm the lock prevents.
- **The oversized-LOB exclusion panel can no longer freeze with no reason given.**
  `lob_exclusion_lock`'s phase tuple lacked `"unstable"` — the phase for every non-stable status —
  so it returned "not locked, no reason" while the panel's caller ORs it with the selection lock
  and still rendered the card locked, with nothing explaining why. An adjacent comment names that
  as the thing that must never happen, and its own sibling set already included `"unstable"`. Fixed
  in two places: the tuple closes today's instance, and the call site now falls back to the
  selection lock's reason so the NEXT contributor to that OR cannot reopen the class.

## v0.1.488

### Added

- **A run the operator STOPPED now writes a terminal line.** `_finalize_run` returned silently
  on a cancelled run, so the audit log held `run started` and nothing else — the same
  dangling-start shape the last two releases closed for aborted runs, but for the one case where
  the operator knows what happened and a reader six weeks later does not. `run stopped` at
  `WARNING` (nothing broke and the loaded tables are kept, but a partial target is a fact someone
  must act on) with how many of how many tables loaded and that a re-run fills only the rest.
- **The stop / cancel REQUEST is recorded too.** Both stops are cooperative, so minutes can pass
  between the click and the run ending; without the request line the log cannot tell an
  operator-requested stop from a crash that happened to end the run at the same moment. Full Load
  logs `stop requested`, Validation logs `cancel requested` (noting that no verdict is produced for
  the skipped tables and that validation is read-only, so nothing on the target changed). Both live
  in the click handler, never in the poll helpers that run every half-second.
- **A job reaped by the stall watchdog is audited.** The watchdog flips a RUNNING job to `FAILED`
  after its silence timeout and did so with NO event at all — so a run reaped for an unresponsive
  source or target left a `run started` line and nothing else, with no operator present to know
  why. `core` must not import the activity log, so the reaper now announces each reap through an
  injected listener that the app wires to a `job stalled` line (`SYSTEM`, because the watchdog
  reaps validation jobs too). A listener that raises can never break the reap.
- **Changes to the activity log's own recording are recorded.** `activity log level changed` and
  `activity log mirror changed`, so a reader comparing two runs can tell the quieter one was
  configured that way rather than idle. Turning the CloudWatch mirror OFF logs at `WARNING` — on
  ECS that mirror is what makes the trail survive a task replacement — and the line is emitted
  BEFORE the handler is removed, or it would be the one event the sink it just removed never
  carried, leaving the CloudWatch copy ending with no explanation.

## v0.1.487

### Fixed

- **A run-level abort inside the table loop now writes a terminal line too.** v0.1.484 bracketed
  only the pre-loop phase, and I attributed the remaining case to a "multiprocess relay path" —
  that attribution was an inference, and it was wrong. Tracing the real failure shows the same
  process, one frame further in: `run_full_load` → `_migrate_tables_in_parallel` → `recreate` →
  `SchemaApplyError`, raised by the loop's own run-level DROP+recreate pre-pass when a target
  foreign key still referenced a table being replaced. That raise skips `_finalize_run`, so
  nothing wrote a terminal line — the identical dangling `run started` the previous fix was
  supposed to end. The bracket now covers the loop, gated on nothing having finished: once tables
  HAVE loaded, their per-table `load table` lines plus `_finalize_run`'s `run incomplete` already
  tell the story, and a second run-level line would be the duplicate reporting this log has been
  cleaned of before.
  - Verified by reproducing the real failure against a live Aurora MySQL source and Aurora DSQL
    target (a blocking foreign key re-created on the target on purpose), not by inference: the
    line appears, names the blocking constraint, states that no rows were written, and is one
    line at `FAILURE`.

## v0.1.486

### Fixed

- **`DeletesApplied` counted one source DELETE twice.** Debezium emits BOTH an `op=d` envelope
  and a TOMBSTONE for the same deleted row (`tombstones.on.delete` defaults on), and the sink's
  applied-ops counter keyed only on "is this a delete?" — so it incremented for each. Measured live
  on `ecommerce.order_items`: 3 inserts / 1 update / 1 delete at the source reported
  `InsertsApplied 3`, `UpdatesApplied 1`, `DeletesApplied 2`. That contradicts the metric's own
  definition ("how many deletes it applied"): a tombstone applies nothing new — it is the same
  idempotent DELETE by key, and one row is removed — and it broke comparison against the source's
  DML counts, where only deletes came out at exactly double. The tool's own per-table migration
  status table showed the doubled figure too.
  - `ChangeEvent` now carries a tombstone flag, set only on the keyed-null-value path, and
    `recordOps` skips it. **The APPLY path is unchanged** — dropping the tombstone from the apply
    would break the key-only case and the log-compaction contract, so it is still applied exactly
    as before; only the counting changed. Ordering is unaffected because `recordOps` runs after the
    apply, and `ReplicationLagMs` is unaffected because a tombstone carries no `source.ts_ms` and
    the lag gate already skipped it.
  - A `sourceTsMs == 0` proxy was rejected: the code's own comment notes an `op=d` envelope can
    also omit `ts_ms`, so that test would silently stop counting real deletes.
  - Connector plugin `PLUGIN_VERSION` is `v41`. The sink ZIP's content changed, so a live
    cdc-stack needs **Delete + Deploy infrastructure** to pick it up — Start CDC alone does not
    re-register the plugin.

## v0.1.485

### Changed

- **Raising the log level now actually helps you troubleshoot a migration.** The Settings tab's
  level switch did take effect — it just had almost nothing to change. All it did was attach a
  stacktrace to failure events that pass an exception, and only **3 of the 61** log call sites did,
  so a participant who flipped to DEBUG while stuck on a schema apply, a CDC deploy or a foreign-key
  pass saw a byte-identical log. Two things changed:
  - **The switch now raises the data-path loggers too.** The traces that answer "which data was
    moving when it broke" already existed — one DEBUG line per keyset export page and per import
    batch, naming the table, the PK range in flight, rows attempted / inserted / skipped, the
    conflict mode and the OCC retry count — but they hang off the `dsql_migrator` package logger,
    settable only by `DSQL_MIGRATOR_LOG_LEVEL` **at start-up**, which a workshop participant cannot
    do. One switch now moves both. Those lines go to the container log (stdout → CloudWatch Logs on
    ECS); the downloadable activity file stays the audit trail.
  - **Six more failure sites forward their exception**, so DEBUG attaches a stacktrace where
    failures actually happen: the CDC lifecycle wrapper (a failed deploy / start / stop / teardown —
    the action an operator is most likely to be stuck on, which had the exception in hand and
    dropped it), both identity-sequence syncs, and all three foreign-key apply failures.
  - The tab's description now says what DEBUG turns on and where those lines land, instead of
    "DEBUG adds failure stacktraces to the activity log".

### Security

- **A natural-key PK value can no longer appear in the DEBUG trace.** The export/import trace names
  the PK **range** in flight. That was reachable only via an env var plus a restart, so its own
  docstring called a natural-key primary key "the operator's risk" — but the Settings switch now
  enables it with one click, which would have put an email or account-number range a click away.
  The range now uses the same withholding rule as a quarantined row's primary key: surrogate keys
  (integer / bool / UUID) shown, anything else `<withheld>`.

## v0.1.484

### Added

- **A run that dies before the table loop now writes a terminal audit line.** Everything from the
  watermark capture through the CDC replication slot and the view / foreign-key pre-drops is
  bracketed: on a failure it logs `run failed` (`FAILURE`) naming the reason and stating that no
  rows were written, then re-raises unchanged. Observed live three times in one session — a target
  foreign key that blocked the DROP+recreate, and a PostgreSQL replication slot that could not be
  created because `wal_level` was `replica` (twice). Each had a nameable cause, every chunk stayed
  `PENDING`, and the activity log held a `run started` line with no terminal line at all. The
  reason is reduced to its first line by `log_activity`, so a driver message cannot carry row
  values into it (Property 7).
  - **Scope, stated because it is not the whole gap:** this covers the IN-PROCESS pre-loop phase.
    A failure relayed from a multiprocess table worker is a different path and still produces no
    run-level line; the MySQL foreign-key case above reached the job that way.

## v0.1.483

### Security

- **A quarantined row's PRIMARY KEY VALUES no longer reach the log.** The loader rendered every
  key column with `!r` (`core/batched_import.py`), so a natural-key primary key — an email, an
  account number, a national id — was written verbatim into the downloadable error log, the
  activity log and its CloudWatch mirror. It was a Property 7 violation in the very function whose
  next line is careful to keep the driver's row-value dump out of the same record. Column NAMES are
  always kept (without them the row cannot be located at all); VALUES only for a surrogate key
  (integer / bool / UUID), everything else renders `col=<withheld>`. This is the rule the CDC side
  already applies to a dead-lettered record's primary key.
- **A raw driver message can no longer carry row values into the log.** Several call sites
  interpolate an exception directly (`detail=f"Apply failed: {exc}"`), and psycopg keeps the
  server's `DETAIL:` / `Failing row contains (...)` lines in `str(exc)` — the offending row's
  column values. `log_activity` now reduces every `detail` to its first line (where the actionable
  message lives), collapses whitespace so one event stays one line, and caps the length. Enforced
  centrally rather than per caller so a future call site cannot reintroduce it.
- **A failed job's persisted error is redacted, as its docstring always claimed.**
  `JobManager._mark_failed` stored `f"{type(exc).__name__}: {exc}"` and then persisted the record
  to the job store, so the raw text — including any row-value dump — outlived the process. It is
  now first-line-only.

### Added

- **The cut-over acknowledgement records the referential-integrity outcome it signs off on.**
  Reading only the activity log, an auditor could not answer the question the Cut over screen
  exists to settle: did the target go live with its foreign keys enforced? The waiver click wrote
  nothing at all, the acknowledgement never mentioned foreign keys, and an ABSENT apply line was
  ambiguous across four situations — no foreign keys in the schema, applied, never run, or waived.
  "Cut over acknowledged" now names which of the four it was (with the applied/outstanding counts,
  and any orphan skips or failures), plus whether the identity sequences were advanced. Choosing
  "Cut over without enforced foreign keys" is its own `WARNING` line: it is the one decision on
  that screen that cannot be reconstructed afterwards.
- **Start CDC records the resume point.** Its audit detail was the stack name alone, so a
  downloaded log showed THAT CDC was started but not from where — while the connector's own
  offsets are consumed and the deploy log is per-action and ephemeral. It now states the coordinate
  through the same precedence the Full Load watermark line uses (GTID → `binlog file:pos` → WAL
  LSN), so the two lines in one log agree instead of spelling it two ways, plus the start mode, a
  forced initial snapshot, and the count of columns excluded from replication. With no watermark it
  says so explicitly — that the stream starts from the source's CURRENT position and any change
  written before that moment is NOT replicated — because that is the case a reader must be able to
  spot, and an absent coordinate only implies it.
- **The per-object "Apply to target" button logs what it applied.** It passed no result callback to
  `run_schema_apply` while the bulk path recorded every object — and this is the path that force-
  REPLACEs an EDITED object, dropping and recreating the table. The most destructive single-object
  action was the one that left no record. Both paths now share one logger, so the line reads the
  same whichever button was used.
- **`ActivityStatus.WARNING`** — the calibrated middle the four existing statuses lacked: nothing
  failed, but the event is not routine (a waived invariant, an operator stop, a log sink turned
  off). `INFO` buried exactly the line a reader needs to find; `FAILURE` would claim a break. Maps
  to `logging.WARNING`.

### Changed

- **The activity-log tab offers DEBUG and INFO only.** Every non-failure audit event is `INFO`, so
  selecting `WARNING` or `ERROR` could not filter noise — it could only discard the audit trail,
  which is never what an operator wants from that tab.
- **A cut-over foreign-key pass is logged as a cut-over event.** The shared audit line is
  parameterised by where the pass ran: at cut over it files under `validation` as "apply foreign
  keys (cut-over)" and says so in the detail. For a CDC migration this pass runs ONLY at cut over
  (the post-load pass is a no-op there), so recording it under `full_load` with no marker made the
  most important pre-repoint fact read as something that happened back during the load — while the
  identity-sequence sync standing beside it on the same screen already logged "(cut-over)" under
  `validation`. Still exactly one line per pass; only its category and label vary.

## v0.1.482

### Fixed

- **A Start over wiped the multi-stack CDC teardown queue it had just written, so a teardown of
  several stacks announced that billing had stopped after only the first one.** The queue's only
  writer runs in the SAME synchronous handler as the session reset (the dialog calls the CDC
  teardown, then the reset), so it was written and wiped in one event — every time. That made
  `teardown_queue_progress`, `next_unfinished_teardown` and `advance_cdc_teardown` dead code in
  production, reachable only from tests that seeded the queue by hand: the banner never said
  "1 of 3", never advanced to the next stack, and the completion notice named only the tracked
  one — while the rest were still deleting and still billing for MSK / NAT. The queue and the
  finished-teardown record now survive the reset. Both fields' docstrings had claimed they did.
- **A CDC teardown no longer blocks the prerequisite checks — and through them, Full Load.** The
  gate lumped `DELETE_IN_PROGRESS` in with a live connector operation and explained it as "A
  migration operation is in progress". With no prerequisite report,
  `full_load_run_guard_reason` then refuses to start a load, so a ~15–45 minute teardown silently
  gated the main flow behind a reason that was false — and it cleared with no explanation when the
  stack finally vanished. A teardown is now excluded for exactly the reason the infrastructure
  create already was: nothing streams, no connector exists and no load is running, so re-running
  the checks is what that time is for. Both halves of the gate needed it — the status, and the
  in-session delete job, whose `kind="delete"` makes `cdc_streaming_started` answer True.
  **Stop CDC (`UPDATE_IN_PROGRESS`) keeps blocking**: it leaves MSK, the topics and the immutable
  partition plan in place, so it really is a live pipeline operation.
- **Prerequisites no longer paints a deleting or failed cdc-stack green.** Its "ready" state folds
  EVERY non-stable CloudFormation status (only `CREATE_COMPLETE` / `UPDATE_COMPLETE` /
  `UPDATE_ROLLBACK_COMPLETE` / `IMPORT_COMPLETE` are stable), so a stack being torn down — or stuck
  in `ROLLBACK_COMPLETE` / `CREATE_FAILED` / `DELETE_FAILED` — was announced in a green success box
  with a positive "Ready" badge as "already deployed, so there is nothing to provision here", while
  the CDC card one sub-step below, reading the same state, correctly said "CDC infrastructure is
  being deleted". One screen contradicting itself. The badge, tone and copy now come from the raw
  status through the same pure helper the CDC card uses, and the section re-polls while the status
  can still change on its own. The fold itself is unchanged and still hides the deploy form — a
  `CreateStack` against a same-named deleting stack is rejected anyway — and no new action is added
  here, because Delete already lives one sub-step below.
- **A delete that is progressing normally is no longer diagnosed as a failed one.** Both the
  "leftover infrastructure needs cleanup" panels and the app-wide teardown banner treated the
  in-flight statuses (`DELETE_IN_PROGRESS`, `ROLLBACK_IN_PROGRESS`, `UPDATE_ROLLBACK_IN_PROGRESS`)
  as failures, because they share the un-attachable bucket with the genuinely stuck ones. The
  panels said "a previous teardown did not finish" and recommended `Retain resources` in the
  CloudFormation console — which would abandon the very MSK / NAT resources the delete was about to
  remove cleanly, the opposite of the stated goal of stopping the cost. The banner said "CDC
  teardown failed — action needed" for a teardown that was simply still working. Both now split the
  two cases and describe the in-flight one as in progress, at warning rather than error severity.
- **The teardown failure banner reports the status it observed instead of asserting
  `DELETE_FAILED`.** It named that status for every failed teardown, including a job that timed out
  or was reconciled after a restart with the stack in `ROLLBACK_COMPLETE` — sending the operator to
  hunt a status the console never showed. It now echoes the observed status, or says plainly that
  the last reported state is not a completed delete when none was read.
- **A failed CDC deploy now reports the CloudFormation reason it already had.** The notification
  read `job_error` only to ask whether a restart had interrupted the job, then discarded it in
  favour of a generic per-kind sentence recommending a second Delete — so a stack that failed for a
  nameable reason (a quota, a subnet with no egress, an IAM gap) showed boilerplate. The real cause
  is now appended.
- **A lost job record no longer freezes a poll forever.** The Full Load and Validation pollers are
  one-shot chains that re-arm only by rendering again, so `except JobNotFoundError: return` killed
  the chain for the rest of the session: the load card stayed "In progress" with a spinner, the
  foreign-key card froze on its counter, and Validation sat at IN_PROGRESS with a Cancel button that
  could never resolve — only a restart triggers the reconcile in `session_persistence`. Both now
  either re-arm (when the foreign-key pass is still running) or do a terminal reconcile and one
  full refresh. The CDC deploy poller's lost-job path likewise now refreshes the whole card, which
  re-probes the stack, instead of refreshing a region that early-returns to empty. An AST test
  guards the whole class: no one-shot poller's `JobNotFoundError` handler may bare-return.
- **The AI panel's chat tick is cancelled, not just deactivated.** nicegui's timer loop reads
  `active` only to decide whether to invoke the callback — the loop keeps waking on the interval
  until `cancel()` runs. All three exit paths only deactivated, leaking one 8.33 Hz asyncio task per
  chat turn, each retaining the full reply through its closure, on the same event loop that serves
  the UI, for as long as the conversation lived.

### Note

- A reported crash from "bare repeating `ui.timer`" was **investigated and refuted**, so nothing
  changed for it. The premise — that nicegui's `Timer._run_in_loop` captures the slot context
  outside the loop — is true of `nicegui/timer.py`, but `ui.timer` instantiates the ELEMENT
  subclass, which overrides `_handle_delete()` to cancel and `_should_stop()` to include
  `is_deleted`: tearing down the anchor cancels the timer, so there is no next tick and no
  exception (confirmed on the pinned nicegui with a live browser probe over ~58,000 create/destroy
  cycles). The investigation found the opposite hazard instead — `_run_in_loop` catches a raising
  tick and keeps polling while a one-shot chain does not — which is the lost-job fix above.

## v0.1.481

### Added

- **Reload specific tables — a scoped re-load for tables that finished cleanly.** The per-table
  "Reload" button lives only on the quarantine card, so a table that loaded with no dropped rows
  had no scoped control at all, and the table picker locks once a load has run ("Use 'Start over'
  to migrate a different set of tables"). That made the normal recovery after re-applying ONE
  table's schema — a REPLACE drops and recreates it EMPTY — either a full "Re-run Full Load" over
  every table (a whole-source re-read) or Start over, which discards the evaluation, the conversion
  edits and the CDC inputs. A finished run now offers **"Reload specific tables…"**, opening the
  same confirm checklist the retry path uses, scoped to the tables that loaded and starting with
  **nothing ticked** (pre-checking would arm a full re-load from a button that says "specific").
  The engine already supported the scoped run — the same path the quarantine Reload takes, keeping
  the run's ORIGINAL watermark so a later CDC start is still gapless — so this only adds the
  missing entry point. Disabled while CDC streams, and while either connection is unverified, with
  the reason in the tooltip.

### Fixed

- **A dropped TARGET table is now reported as its own, more severe condition.** When the table the
  CDC sink writes to disappears — a Schema Conversion REPLACE, or an out-of-band `DROP`, under a
  live stream — DSQL answers `42P01`, which the sink's transient test (`40001` / `08*` / `57*` / a
  null state) does not match. So it is permanent: every event for that table is dead-lettered with
  its offset committed, no retries, and no replay. But `42P01` was not in the drift map, so the
  most destructive failure of all was also the quietest: no banner, and under 50 records not even
  an amber DLQ badge. `42P01` / `3F000` now map to a new `MISSING_TABLE` kind — the one
  target-side kind — which takes the error tone (rows are being lost as it renders, so "be aware"
  is the wrong severity), its own header ("Target table missing — rows are being lost") instead of
  the source-change wording that does not apply, and a distinct recovery notice: the stream cannot
  heal it, so stop CDC, confirm the table exists, reload just that table, and let Validation prove
  the gap is closed. The `add-column` "Fix target schema…" action stays out of it — an `ALTER …
  ADD COLUMN` cannot fix a missing table.
- **Schema Conversion no longer goes silent about CDC after a restart.** The guard that blocks
  applying schema while CDC streams reads `cdc_controller` / `cdc_stack_phase`, and NEITHER is
  snapshotted — only `cdc_connector_names` is, and all three are written solely by a render of the
  Data Migration CDC sub-step. So after a UI restart an operator who went straight to Schema
  Conversion saw nothing at all while the pipeline was still running on AWS, and a destructive
  REPLACE proceeded behind only the generic confirm. That unchecked-but-recorded state is now
  surfaced ("CDC may still be streaming — this session hasn't checked") with the danger, the check
  to run, and a statement that the notice is stale if CDC was already torn down. It **warns rather
  than blocks**, deliberately: connector names left behind by an out-of-band teardown would
  otherwise trap the operator forever (a screen was once observed warning "CDC is streaming" for a
  stack deleted twelve hours earlier). No AWS call is added — this screen re-renders on every
  refresh.

## v0.1.480

### Fixed

- **A Schema-Conversion edit was never persisted, so a task restart could silently revert it.**
  `capture_session_snapshot` has always recorded `edited_target_ddls`, `preserve_foreign_keys` and
  `ticked_node_ids` — but the dirty-check that decides whether to WRITE a snapshot
  (`session_signature`) did not look at any of them, so an edit-only change left the signature
  unchanged and the save was skipped entirely. The per-table primary-key picker stores its answer
  nowhere else, so choosing a server-generated IDENTITY key (or turning FK preservation off) was
  lost on a Fargate task replacement unless some unrelated state happened to change too — and
  because the cut-over "Sync identity sequences" action is derived from that DDL, it then vanished
  from the runbook. The signature now includes a cheap term for all three (key names + a hash of
  the pairs; never the DDL text, so the per-poll cost stays a dict walk and a large inventory is
  still not re-serialized on every refresh).
  - A reported diagnosis of the same symptom — that a restart empties the evaluation/conversion
    state the cut-over gates count — was **not** the cause and is not what changed: the snapshot
    does restore the inventory, the edited DDLs and the preserve-FK flag, and a real
    capture → JSON → restore round trip onto fresh state keeps both cut-over counts intact. A new
    test pins exactly that, since nothing covered it before.
- **The cut-over runbook could render empty and read as "nothing left to do".** Both gate counts
  are 0 both when there is genuinely nothing to apply AND when the Step 1/2 schema inputs are not
  loaded in this session, and 0 hides the action — so "done" and "can't tell" looked identical on
  the one screen that decides whether referential integrity is enforced. The two zeros are now
  distinguished: the runbook shows a warning naming the missing input and the recovery, and the
  finish gate returns a new `"unknown"` state that soft-blocks "I've cut over" (the explicit
  "cut over without enforced foreign keys" opt-out still clears it, so it is not a trap). The
  count helpers themselves are untouched — 0 for a schema with no foreign keys, or for the default
  integer-PK strategy, is correct and stays pinned. No target-catalog probe was added: the count
  must match what the FK pass will actually render from the source inventory plus your conversion
  choices, which a `pg_constraint` read cannot reconstruct, and this screen re-renders on every
  refresh so it must never scan the target.
- **The cut-over "I have frozen source writes and CDC has drained to zero lag" tick no longer
  clears itself.** The gate's entire memory was a per-render local with no bound value, so any
  `refresh()` — including the foreign-key apply's own completion refresh, and the refresh on a
  click while a pass was running — redrew the checkbox unchecked and re-disabled the button.
  That landed on exactly the remediation loop the screen recommends ("resolve the orphan rows,
  then click Apply foreign keys again — it is idempotent"). Applying foreign keys does not unfreeze
  the source, so the attestation is still true afterwards; it is now latched on `ValidationState`,
  and a later un-tick still re-disables the button. Deliberately NOT snapshotted — a claim that
  writes are frozen must not survive into a session restored hours later — and cleared on every
  event that could make it stale: a new verdict, a re-run, a target-schema REPLACE, and Start over.
- **The Bedrock preflight's fallback model was unreachable in the one case it exists for.** The
  fallback was tried only for `MODEL_NOT_ENABLED`, but Bedrock reports "this model is not available
  for this account" as `AccessDeniedException` — so the chain was skipped and the operator was told
  to add a `bedrock:InvokeModel` permission that was already correct (measured: a `global.` profile
  denied for the account while a different model answered on the same credentials). A denial whose
  code is authorization-ambiguous (`AccessDeniedException` / `AccessDenied` /
  `UnauthorizedException`) now reaches the fallback; credential-identity denials (expired token,
  bad signature) still return immediately, because another model cannot fix those. Classification
  is still by error **code** only — no message body is ever read (Property 7) — and the two cases
  are told apart by the *outcome*: if the fallback answers, the configured model is un-granted or
  outside this deployment's `BedrockModelArns` scope (both named in the message); if it is denied
  too, the verdict stays the configured model's original `ACCESS_DENIED` reason.
  - **Companion:** a preflight that passes on a fallback now adopts that model. Previously it
    reported success while leaving the denied model configured, so every real call still failed —
    the check proved a working model existed and then nothing used it. The status line, the toast
    and the dropdown all name the substitution, and verification resets to unverified, so nothing
    claims green.
  - The operator-facing IAM comment in `deploy/cloudformation.yaml` and the `.kiro` spec's error
    mapping asserted the same refuted premise and are corrected.

### Removed

- `target_lag_seconds` (MSK Connect controller) is deleted. It was exported but had no call site
  anywhere in the app: per-table "Stream lag" uses `ReplicationLagMs` and the headline uses the
  connector state's `lag_seconds`, so the only thing it did was suggest to a reader that some
  path computes lag from the target's `max(ts)`. Its five tests go with it — they guaranteed the
  behaviour of code nothing ran.

## v0.1.479

### Removed

- **The "Ask AI DBA about cut over" section is gone from the Cut over step.** Its copy promised
  a GO/HOLD verdict and repoint recipe "grounded on your real validation, CDC and identity-sync
  state" — and that grounding did exist (target coordinates, the last validation verdict with its
  matched/missing/extra counts, the migration path, source drift, and the identity-sync outcome
  with its `23505` risk). What it never carried was the **foreign-key state**, which is the fact
  that decides GO/HOLD on this screen: cut over is where a CDC migration applies its deferred
  foreign keys. A chat that sounds authoritative about a cut over while blind to that is worse
  than not offering one, so the section was removed rather than patched.
  - Nothing is left orphaned: `cutover_ai_facts` (the grounding builder) and
    `AssessmentStrategist.stream_cutover_chat` (its only entry point) are removed too, along with
    the three now-unused AI parameters on `build_cutover_screen` and the arguments `app.py`
    passed for them. Dead code that only a reader has to reason about is not a saving.
  - The Cut over step still posts to the **activity feed** (`ai_post_event`); only the chat went.
  - The AI assistant remains available everywhere else it was — Evaluation, Schema Conversion,
    the quarantine explainer, and the CDC/DLQ surfaces are untouched.

### Tests

- 3896 green. The test that pinned the button's presence is replaced by its inverse — the section
  and BOTH grounding helpers must stay gone, so it cannot drift back in half-wired — and it
  strips comment lines, so the comment explaining the removal does not trip it. Three mutations
  checked, each caught: re-adding the section, and leaving either helper behind as dead code.

## v0.1.478

### Changed

- **Removing the foreign keys that block Start CDC now happens IN the Start CDC dialog, which
  stays open.** Clicking **Remove foreign keys** used to drop them, close the dialog, and tell
  the operator to "reopen Start CDC to continue" — turning a two-click job into four and
  discarding the pre-flight results (the binary-log resume probe and the connection check) that
  had just been computed for them. The dialog now replaces the block notice with
  *"N foreign key(s) removed — ready to start"* and **enables Start CDC in place**, so the next
  click is the one the operator came for.
  - A PARTIAL removal keeps Start **locked** and says which state they are in ("N removed, M
    failed"). A surviving enforced foreign key would make the sink dead-letter out-of-order
    child rows (`23503`) permanently and silently, so this must not become a soft warning.
  - **Cancel** now refreshes the CDC card when a removal happened: the drop is real even if the
    operator then backs out, and the close used to do that refresh implicitly.

- **Published `0.1.477` to all three registries and deployed it**, and repointed the
  `ContainerImageUri` default to it.
  - `mysql-dsql-migrator` (us-east-1): `app:58` -> `app:59`, 1/1, ALB target `healthy`.
  - `mysql-dsql-migrator-seoul` (ap-northeast-2): `app:104` -> `app:105`.

### Tests

- 3899 green (+2). The new tests DRIVE the real dialog through a NiceGUI double and click
  Remove, rather than grepping its source: they assert the dialog is not closed, that Start
  becomes enabled, and that a partial failure leaves it disabled. Four mutations checked, each
  caught. Source-grepping would not have been enough — while making this change the Cancel edit
  landed in the WRONG dialog (`_open_cdc_infra_dialog`, which has an identical button row) and
  only the undefined-globals guard caught it.

## v0.1.477

### Fixed

- **The CDC infrastructure deploy no longer tells the operator to expect 15-20 minutes for a
  ~4-minute job.** "Stack creation submitted — this provisions MSK (~15-20 min)" dated from when
  the template provisioned a *provisioned* MSK cluster. With MSK Serverless two real runs
  finished in **3m34s** and **4m07s** (the second is the `deploy CDC infrastructure … completed
  in 4m 7s` line in a real activity log). A 3–5× over-estimate is not a harmless margin: the
  operator leaves the screen, and the deploy that needs them to press **Start CDC** next sits
  idle. The progress label and the submitted-stack line now say *usually ~5 min* — a typical,
  not a promise, since a different region or VPC can be slower.
  - The same stale figure appeared in two more places the operator reads (the Prerequisites
    "you'll deploy it first" hint and the Start-over provisioning banner); fixing only the
    deploy log would have moved the surprise rather than removed it. A sweep test now fails if
    that figure reappears in operator-visible copy.
  - The teardown estimates (`~15–45 min`, dominated by the native ENI detach) are left alone —
    there is no measurement for them, and inventing one is the mistake being corrected here.

### Changed

- **Published `0.1.476` to all three registries, deployed it, and repointed the
  `ContainerImageUri` default.** `0.1.476` is the first image carrying the load-mode audit lines
  (APPEND vs REPLACE, and the new `retry started`).
  - `mysql-dsql-migrator` (us-east-1): `app:57` -> `app:58`, 1/1, ALB target `healthy`.
  - `mysql-dsql-migrator-seoul` (ap-northeast-2): `app:103` -> `app:104`, same image.

### Tests

- 3897 green (+2). Three mutations checked, each caught: restoring the stale figure in the stage
  label, in the submitted-stack log line, and in one of the other operator-visible surfaces.

## v0.1.476

### Added

- **The activity log now records whether a load APPENDED or REPLACED.** `run started` said only
  "7 table(s) selected", omitting the run's most consequential choice: a **replace** DROPs and
  recreates the target table — taking its foreign keys with it — while an **append** keeps every
  existing row and inserts only the missing ones. Afterwards a reader of the audit trail could
  not tell which had happened to a table's rows. The mode is named per run, and because the
  choice is per table a mixed run is reported as such ("REPLACE for 1 of 3 (ecommerce.b) …
  APPEND for the rest").
- **A retry now has a run-level line at all.** `run_full_load_retry` logged the watermark and
  the excluded columns but nothing to open the run — so the most common single-table replace,
  the per-table **Reload** and **"Exclude column & reload"** (which force a replace for that
  table), left no record that those rows had been dropped and reloaded rather than added to. It
  is named `retry started` so a scoped re-run is distinguishable from a fresh one, and it names
  the retried tables and their mode.

### Changed

- **Published `0.1.475` to all three registries, deployed it to both live app stacks, and
  repointed the `ContainerImageUri` default.** A fresh `git clone` deploy previously defaulted to
  `0.1.472`, which predates every activity-log and CDC fix in v0.1.474–475 (the false
  `connector … running` audit lines, the duplicated foreign-key summary, and the connector read
  that could not tell "gone" from "no permission"). The live release gate passed before each
  build.
  - `mysql-dsql-migrator` (us-east-1): `app:56` -> `app:57`, 1/1, ALB target `healthy`.
  - `mysql-dsql-migrator-seoul` (ap-northeast-2): `app:102` -> `app:103`, 1/1, 4096 CPU /
    8192 MiB and the Cognito user pool (with its user) untouched.

### Tests

- 3895 green (+2). Five mutations checked. The first pass MISSED one — dropping the
  "is it in this selection?" filter, so a leftover replace target from another run coloured the
  verdict — because the assertion only looked for "APPEND", which the partial-replace text also
  contains ("APPEND for the rest"). Tightened to require the ABSENCE of "REPLACE".

## v0.1.475

### Fixed

- **A connector read that FAILS no longer reads as "there are none" — v0.1.474's clearing made
  that case worse, and this corrects it.** `list_connectors()` folds every error into `[]`
  (fail-closed by design), so a task missing `kafkaconnect:ListConnectors` looks exactly like a
  deleted stack. v0.1.474 then started clearing the remembered connector names on an empty
  listing — right for a stack that really is gone, and **wrong for a permission error**: the
  tool would report a LIVE, streaming pipeline as absent. That is the more dangerous of the two
  lies. An operator who believes nothing is streaming may edit the CDC inputs, or press
  **Apply foreign keys** — and applying them while the sink streams out-of-order child rows
  dead-letters those rows (`23503`), which is a hazard the tool documents against itself.
  - `list_connectors_checked()` now returns `(read_succeeded, connectors)`. Both discovery
    branches clear ONLY on a successful empty read; a failed read leaves the last known names
    untouched — **unknown, not asserted either way** — and logs the failure.
  - A controller without the new method (an injected double, or anything predating it) is
    treated as a successful read, so nothing that used to clear silently stops clearing.

| | before v0.1.474 | v0.1.474 | now |
|---|---|---|---|
| stack really deleted | reported *streaming* ❌ | *absent* ✅ | *absent* ✅ |
| no `ListConnectors` permission | *streaming* (accidentally right) | reported *absent* ❌ | *unknown*, names kept ✅ |

### Tests

- 3893 green (+3). Four mutations checked. The first pass MISSED one — folding the controller's
  failure back into success — because every test used its own double and the real
  `MskConnectController` error path had no coverage at all; a test against the real controller
  was added and the mutation is now caught.

## v0.1.474

### Fixed

- **An empty connector listing no longer fails OPEN, so "CDC is streaming" cannot outlive the
  connectors.** Discovery stores the MSK Connect controller, then returned on an empty live
  listing *without writing the names* — and `cdc_pipeline_live` is
  `controller is not None AND cdc_connector_names`, so it stayed **true on leftover names**.
  After a restart those names come from the session snapshot, restored with no AWS
  confirmation at all (gated only on the last UI action not being delete/stop, so a stack
  deleted from the console leaves them). Observed live: the **Schema Conversion** screen warned
  *"CDC is streaming to the target — the schema is already applied"* for a cdc-stack that had
  been `DELETE_COMPLETE` for twelve hours, while the Data Migration panel — from the SAME
  discovery pass — correctly said *"No cdc-stack is deployed yet"* with the chip on
  `CDC: NOT_STARTED`. That warning reads the predicate with no AWS call of its own, so nothing
  else could ever correct it. An empty listing now clears both the names and the running-names;
  the controller is still kept, because a later deploy needs it.
- **The activity log recorded connectors as `running` that nobody had seen change.**
  `_log_cdc_connector_transitions` promises "only a CHANGE from the last-seen state is logged",
  but its baseline is never initialised, so a session's FIRST observation was treated as a
  transition. Live, a fresh page render wrote two `SUCCESS [cdc] connector … running` lines for
  a stack deleted twelve hours earlier — and the activity log is an **audit trail**, so an
  investigation starts from that false claim. The first observation now seeds the baseline
  silently; a genuine transition after it still logs.
- **One foreign-key pass produced two contradictory audit lines.** An unconditional `INFO`
  inside the pass and a conditional `SUCCESS` from its caller fired 0.3 ms apart for the same
  event, disagreeing on action name, status and wording (`orphan rows` vs `orphaned rows`,
  capitalisation, full stop). Now one line, emitted by the pass — which is deliberate, not the
  obvious de-duplication: the **cut-over** action calls that pass directly and logs nothing of
  its own, so moving the line to the caller would have silently deleted cut over's only record
  (visible in the real log as a lone `foreign keys applied` with no companion). The surviving
  line takes the caller's named action and SUCCESS/FAILURE status, and drops "post-load" since
  the same pass runs at cut over.
- **"migration type selected" is filed under the type that was chosen.** The category was fixed
  at `full_load`, so picking **CDC only** was logged as `[full_load]` — filtering the log by
  area hid the decision that defines a CDC migration.

### Changed

- **The deferred foreign-key pane in Generated DDL is two lines tall, with an expand button.**
  It reused the DDL comparison panes' class, whose `min-height: 8rem` gave a two-statement
  `ALTER TABLE … ADD CONSTRAINT` list the same tall box as a 30-line `CREATE TABLE` — so the one
  section Schema Apply does not even run dominated the panel. Same height for one constraint or
  twenty; it scrolls, and the full list is one click away.

### Tests

- 3890 green (+8). Fifteen mutations checked across the four fixes, each caught — including
  returning without clearing the names, clearing the names but keeping the stale running-names,
  logging a first observation again, seeding without recording the baseline, re-adding the
  caller's duplicate line, and restoring the `orphaned rows` spelling. Two existing tests had
  encoded the defects as expected behaviour (a first observation logs; the wrapper logs) and
  were corrected.

### Known gaps

- The two false `running` log lines are no longer written, but **where their state came from is
  not established**: `connector_states` has one producer, a live `list_connectors` filter, and
  an empty result makes the logger bail — so a fresh process cannot produce them from the code
  as written. Not invented into a fix.
- ~~`list_connectors()` swallows every error, so "gone" and "cannot call
  `kafkaconnect:ListConnectors`" are indistinguishable.~~ **Fixed in v0.1.475** — and it had to
  be, because this release's own clearing made that case WORSE (see below).

## v0.1.473

### Changed

- **Published `0.1.472` to all three registries, deployed it to both live app stacks, and
  repointed the `ContainerImageUri` default to it.** A fresh `git clone` deploy previously
  defaulted to `0.1.469`, which predates the Schema-Conversion REPLACE fixes (the foreign-key
  pre-drop, v0.1.471, and the stale-applier invalidation, v0.1.472). The live release gate
  passed against a real cluster before each build. No app-code change in this release.
  - ECR Public + us-east-1 private + ap-northeast-2 private all on `0.1.472`.
  - `mysql-dsql-migrator` (us-east-1): task definition `app:55` -> `app:56`, 1/1 running,
    ALB target `healthy`.
  - `mysql-dsql-migrator-seoul` (ap-northeast-2): `app:101` -> `app:102`, 1/1 running, with
    4096 CPU / 8192 MiB and the Cognito user pool (and its user) untouched.
  - Both updates were image-only (`--use-previous-template`, one overridden parameter of 26,
    the rest `UsePreviousValue`, the list generated from each deployed stack's own parameters).

## v0.1.472

### Fixed

- **A second apply in the same session failed with `relation "x" already exists` instead of
  replacing.** The applier answers *"does this object already exist?"* from a name snapshot
  taken ONCE when it is built, and the screen caches it for the whole session so a per-object
  click does not re-browse the entire target catalog. But an apply CHANGES the target, which
  makes that snapshot wrong about the objects it just wrote: apply to an empty target, then
  apply again as REPLACE without leaving the screen, and the stale snapshot still reports the
  tables absent — so the `DROP` is skipped and every object fails on the `CREATE`. Nothing in
  the error pointed at the actual remedy, **"Refresh target"**, which was the only thing that
  cleared it. The cached applier is now dropped after an apply that actually wrote to the
  target, so the next one re-browses.
  - Invalidated only when something came back CREATED. A run that skipped everything changed
    nothing, so its snapshot is still true and the browse would be exactly the cost this cache
    exists to avoid.
  - Both apply paths do it — the bulk run and the per-object **"Apply to target"** button,
    which is the one that mutates the target on every single click.
  - Found while verifying v0.1.471's foreign-key pre-drop: the first harness run failed 6/6
    with `already exists`, which was the harness reusing one applier — the same shape as the
    UI's cached one, and the reason that run was reported INVALID rather than as a pass.

### Verified live

- Against a real Aurora DSQL cluster (local MySQL source, 2 tables): one applier built against
  an empty target created both; **reusing** it for a REPLACE failed both with
  `relation "..." already exists`; a **fresh** applier replaced both cleanly. So the defect and
  the fix are both demonstrated, not inferred.

### Tests

- 3886 green (+2). Five mutations checked, each caught: never invalidating, dropping it from
  either apply path, invalidating AFTER the results are recorded (a render in between could
  re-cache the stale snapshot), and invalidating unconditionally.

## v0.1.471

### Fixed

- **Schema Conversion's destructive REPLACE could not recreate a table this migration's own
  foreign keys referenced.** Reported live: with 6 preserved foreign keys applied, editing two
  primary keys and re-applying all 7 objects as Replace reported *created: 6, failed: 1* —
  `ecommerce.categories` could not be recreated because `ecommerce.products.fk_products_category`
  still depended on it. The apply pre-dropped the selection's **views** for exactly this
  reason but not its **foreign keys**, while Full Load's "drop & reload" path had always
  pre-dropped them. Both destructive paths now agree: the pre-drop uses the SAME pure selector
  (`foreign_keys_blocking_replace`), so only constraints from this migration's own conversion
  whose parent is being recreated are touched — a hand-made constraint is never dropped — and
  each drop is idempotent and reported to the activity log.
  - **The pre-drop lives inside `run_schema_apply`, beside its view sibling.** Wiring it into
    the bulk caller alone left the per-object **"Apply to target"** button reproducing the
    failure verbatim — and that is the likelier route straight after a key edit, because an
    edited object forces REPLACE even in global SKIP mode.
  - A pre-drop failure is now **reported** (logger + activity log), not swallowed. Silently
    skipping it left the operator with the recreate failure plus a hint telling them to
    re-run, which would fail again for the same unreported reason.
  - The selector no longer reads the live "preserve foreign keys" toggle: unticking it empties
    the generated FK DDL without removing anything from the target, so the pre-drop selected
    nothing exactly when constraints were still live and still blocking.
  - It stamps job liveness. The whole pass runs before the first per-object callback, so on a
    schema with many foreign keys it was silence measured against the 900 s stall window.
- **The failure text asserted the tool had not created the constraint it had just created.** It
  said *"This migration did not create it, so the tool will not remove it"* on the strength of
  the reload path pre-dropping owned constraints — a premise that never held for this path. It
  now states only what it can know, and is short enough that the durable record's 200-character
  detail keeps the **action** rather than cutting it mid-clause.
- **Both later steps kept claiming referential integrity was in place after a REPLACE dropped
  it.** DSQL drops a table's foreign keys with the table and nothing cleared the verdicts, so
  Data Migration rendered a green *"N applied — referential integrity is in place on the
  target"* with its Apply button withdrawn, and cut over's finish gate stopped blocking with
  nothing enforced. A confirmed REPLACE of any table now clears both. It is keyed on *a table
  is being replaced*, not on what the pre-drop dropped: the selector is parent-side only, so
  replacing a **child** destroys its own foreign keys while producing no pair at all.
- **The REPLACE confirmation dialog now names the foreign keys it will drop.** They sit on
  tables the operator did **not** select, so a dialog listing only the selection understated
  what the confirmation authorises (Property 12), and nothing re-creates them afterwards.
- **Removed three more claims that the tool re-creates foreign keys automatically** — the Full
  Load pre-drop's own activity detail, the apply hint's fallback, and a `validator` docstring.
  Nothing has done that since v0.1.461; they are the explicit "Apply foreign keys" action.

### Tests

- 3884 green (+11), including a new `tests/test_schema_conversion_apply.py`. Seven mutations
  checked, each caught — removing the seam from `run_schema_apply`, running it for an
  unconfirmed REPLACE, letting a seam failure fail the apply, dropping the dialog disclosure,
  leaving `proceed_without_foreign_keys` standing, restoring the ownership claim, and making a
  pre-drop failure silent again (that last one exposed a missing assertion on the first pass).
- The structural test that only grepped the screen for a call site is gone: it was satisfied by
  the bulk path alone, which is precisely how the per-object regression hid.

### Known gaps

- ~~Not yet verified live end-to-end.~~ **Verified live** against a real Aurora DSQL cluster
  (local MySQL source, 6 tables / 5 preserved foreign keys): with all 5 applied, a confirmed
  REPLACE of every object reported **`failed: 0`**, where the *identical* apply without the
  pre-drop failed on `fkdb.categories` with *"the foreign key fkdb.products.fk_products_category
  still depends on it"* — the reported failure, reproduced as a negative control so the pass
  means something. The pre-drop removed exactly the 5 constraints this migration owns, and the
  confirmation text named all 5 as sitting on tables the operator did not select. Scratch schema
  and container torn down.
- Pre-existing and out of scope: a REPLACE invalidates nothing ELSE about the superseded run,
  so the Full Load panel above still reads "complete — all N tables loaded every source row".

## v0.1.470

### Changed

- **Published `0.1.469` to all three registries and repointed the `ContainerImageUri`
  default to it.** A fresh `git clone` deploy previously defaulted to `0.1.465`, which carries
  connector plugin **v39** — the reverted per-type value guard — and none of the foreign-key
  card fixes. `0.1.469` carries plugin **v40** (the documented flat 1 MiB per column) plus
  v0.1.467–469's progress, stopped-pass and cut-over fixes. The live release gate passed
  against a real cluster before each build. No app-code change in this release.
  - ECR Public + us-east-1 private + ap-northeast-2 private all on `0.1.469`.
  - The two live app stacks are **not** updated by this — they still run `0.1.465` until a
    stack update is run explicitly.

## v0.1.469

### Fixed

- **A STOPPED foreign-key pass reported itself as a clean success.** "Stop applying" breaks
  the engine's loop, so the foreign keys it never reached land in no bucket at all: stopping
  after 2 of 6 returns `(2, 0, 0)`. The card was toned and worded from those buckets alone, so
  it announced a GREEN *"Foreign keys: 2 applied — referential integrity is in place on the
  target"* while four constraints did not exist, and withdrew the Apply button. Nothing clears
  the result, so that screen was the end of the road: **Start over** was the only way out.
  Present since v0.1.461; v0.1.467 only made it visible (before, the card needed a manual page
  reload to change at all). The card now derives what is still outstanding
  (`total − applied − skipped − failed`), tones and words it accordingly
  (*"2 of 6 applied … 4 were NOT reached"*), and **keeps the Apply action so the pass can be
  resumed**. The same test catches a pass that DIED: `_apply_foreign_keys` swallows the
  exception and returns `(0, 0, 0)`, which used to render as a green "0 applied".
  - The count provider no longer returns 0 once a result exists — it reports the run's TOTAL,
    which is the denominator the card needs. Zeroing it made the fix a no-op in exactly the
    branch that needed it.
  - It also no longer fails OPEN: that count is advisory (its provider swallows any error, and
    it is 0 by design for a CDC run), so the denominator falls back to the total the pass
    itself publishes on its progress — otherwise an unavailable count restored the original
    false green and printed "2 of 0 applied".
- **Resuming a stopped pass was invisible and unstoppable.** `_fk_apply_running()` reports
  False while a result exists and nothing cleared it, so keeping the Apply button (above) first
  exposed a resume path where the new job reported *not running* for its whole duration: the
  stale summary re-rendered with no spinner, no per-FK progress and no "Stop applying" (that
  branch owns the cancel), the poll never re-armed, and the still-enabled button could submit a
  **second concurrent pass** whose job id displaced the first — leaving it uncancellable. The
  action now clears the previous result **before** submitting (ordering matters: clearing after
  submit can wipe a fast worker's fresh result).
- **A reload that replaces a table no longer leaves a stale "N applied".** DSQL drops a table's
  foreign keys with the table, and nothing cleared the result — so after the tool's own
  recovery advice ("Exclude column & reload", which forces a replace for that table) the card
  went on claiming referential integrity was in place for constraints that no longer existed. A
  fresh load clears it outright; a retry clears it only when it will actually replace a table
  (an append-only retry leaves the constraints intact).
- **Cut over no longer claims the load applied the foreign keys for you.** Its Full-Load-only
  copy said this schema's foreign keys *"were applied automatically at the end of the load"* —
  untrue since v0.1.461, so an operator who never ran the action, or who stopped it, was told
  referential integrity was already in place. It now names the explicit **"Apply foreign keys"**
  action and says plainly that integrity is NOT enforced if it was never run, stopped part-way,
  skipped or failed.
- **Cut over's "Apply foreign keys" can no longer start two concurrent passes.** It submitted a
  job and then reported nothing at all — no spinner, no count — for a pass that runs an
  O(child rows) orphan pre-gate per foreign key, so on a real schema an operator reasonably
  concluded the click had not registered and clicked again, each click costing another full
  pass. It now records its job and refuses to start a second while one is live, and publishes
  per-foreign-key progress.

### Tests

- 3872 green (+13). Eighteen mutations checked across the two rounds, each caught — including
  toning from the buckets alone, withdrawing the action on a stopped pass, removing the
  denominator fallback, dropping the result-clear (or moving it after submit), clearing the
  result on an append-only retry, removing the cut-over guard or its progress reporter, and
  leaving a stale cut-over job id (which would refuse every later click for the session).

### Known gaps

- Cut over still has **no truth source** for whether the foreign keys are actually enforced: the
  false claim is gone and the idempotent apply is offered, but confirming the real state needs a
  target-catalog probe, which is deliberately left for a separate change.
- The cut-over screen installs no poll timer, so its new progress is visible on a refresh rather
  than live.
- v0.1.464's per-value guard remains **unverified live** (needs an MSK pipeline).

## v0.1.468

### Fixed

- **Reverted v0.1.464's and v0.1.467's claim that Aurora DSQL does not cap a `text` value at
  1 MiB. It does — the claim was wrong, and this restores the documented limit everywhere.**
  [Aurora DSQL's quotas page](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html)
  states it plainly and it applies to **every type**: *Maximum size of a column that's not
  part of an index — 1 MiB — `54000` — `ERROR: maximum column size exceeded`* (a row is
  capped at 2 MiB, a write transaction at 10 MiB, and a protocol message at 10 MiB,
  `08P01 FATAL: invalid message length`).
  - **How the error was made.** A live probe inserted `"x" * n` into a `text` column and read
    back `length(t)`; it reported success at 2 / 4 / 6 / 8 / 9.5 MiB, and that was taken as
    evidence that the documented 1 MiB did not apply to text. The payload was the problem:
    PostgreSQL TOAST **compresses**, a run of one character collapses to almost nothing, and
    the cap is on the **stored** size — so the probe never approached the limit while
    `length()` kept reporting the logical size. The connection dropping at exactly 10 MiB was
    the **message-size** limit, not the transaction limit. A documented quota should not have
    been overruled by a probe at all; the probe was not even measuring the right quantity
    (`pg_column_size`, not `length`) with the right data (incompressible, not repeated).
  - **What is restored:** the CDC sink's pre-write guard is a flat 1 MiB again for every type
    (`PLUGIN_VERSION` v39 → **v40**, artifact rebuilt), and all 91 statements across the
    manual, the UI strings and the READMEs are back to the 1 MiB limit.
  - **What is kept:** the limitations table (en/ko/ja) now cites the quotas page and states
    the figures exactly — 1 MiB per non-index **column**, 2 MiB per **row**, 10 MiB per write
    transaction — including the row limit our docs never stated.
  - **Impact while v39 shipped (images 0.1.464 / 0.1.465):** a 1–8 MiB text value was handed
    to DSQL instead of being dead-lettered before the write, so DSQL rejected it (`54000`) and
    the sink dead-lettered it anyway. Same outcome, reached the slow way, one wasted write
    attempt per oversized row. **No data was lost or wrongly dropped in either version.**
- **`scripts/cdc_dlq_demo.py` never actually produced a DLQ entry** (local script). Its
  oversized `full_description` repeated a readable phrase, with a comment reasoning that "the
  point is size, not incompressibility" — but the cap is on stored bytes, so 1.2 MiB of
  repeated text compressed well under it and the row replicated normally. The payload is now
  drawn from the full printable-ASCII set, the default is 1.6 MiB (≈1.39 MiB compressed), and
  `--desc-mib` is validated against the **compressed** size instead of the logical one.

### Unaffected

- v0.1.467's "Apply foreign keys" progress fixes (per-FK reporting, providers instead of
  values frozen at page render, and polling while the FK job runs) are unrelated to the limit
  and stand as shipped.

## v0.1.467

### Fixed

- **"Apply foreign keys" showed `0 of N done` for the whole pass, then kept spinning after
  every constraint was already on the target.** Reported from a workshop: the card appeared
  correctly, was polled for 110 s, never moved, and only a manual page reload turned it into
  "6 applied" — so the "it looks stuck, click it again" that the explicit action was
  introduced to end was still there, for minutes on a real schema. Three distinct causes:
  - **No per-FK progress was ever published.** The action wrote only `(0, total)` before
    submitting and `(total, total)` in a `finally`. The pass now takes an `on_progress`
    reporter and calls it once per foreign key as it settles. This is deliberately NOT the
    existing `heartbeat`: since v0.1.456 that fires on every orphan-count page and every
    index poll, many times per FK, so counting it would have produced nonsense.
  - **The card's inputs were frozen at page render.** It is drawn inside the step's
    `@ui.refreshable` live region, but `running` / `progress` / `applied` / `pending` were
    passed as *values* evaluated at the call site — so refreshing the region replayed the
    state from before the button was ever clicked. They are providers now, re-read on every
    refresh. This, not the missing counter, is why completion never appeared.
  - **Nothing re-armed the poll.** The re-arm was gated on the *load* job, and the FK pass is
    a separate job that starts only once the load is terminal. The live region now polls while
    either is live, and the poll refreshes only that region while the FK job runs (a full page
    rebuild every 1.5 s would have been the alternative), doing its single full refresh on the
    first tick after it settles.
  - The `finally` that forced `(total, total)` is gone: it reported a pass cut short by **Stop
    applying** as complete, and the card reads progress only while the job runs, so the write
    was never displayed anyway. The engine's own final report counts settled foreign keys, so
    a stopped pass now shows its true partial count.
- **Corrected 91 statements across the manual, the UI and the READMEs that told the reader
  Aurora DSQL caps any single value at ~1 MiB.** Measured live 2026-09-19: that is true only
  for `bytea`; a `text` value stores intact at 9.5 MiB and is bounded by the 10 MiB
  per-write-transaction limit. v0.1.464 fixed the sink and the limitations table, but the
  claim was repeated in ~60 more places — the MySQL/PostgreSQL type tables ("a value > ~1 MiB
  is rejected" on the `TEXT` row), the CDC three-band DLQ table (which said every 1–8 MiB
  value is dead-lettered, when text in that band now applies normally), the test-scenario
  matrix, and customer FAQ Q24. All three languages. Changelog entries were left untouched as
  the historical record.

### Tests

- 3856 green (+9). Eight mutations checked, each caught: removing the per-FK report, forcing
  the final count to the total (the Stop misreport), letting a reporter error escape, dropping
  the reporter in the wrapper's degrade chain, re-arming the poll on the load only, reverting
  the call site to frozen values, removing `_call_or`'s fallback, and letting the poll fall
  into its full-refresh branch while the FK job runs.

### Known gaps

- v0.1.464's per-type value guard is still **unverified live** — it needs an MSK pipeline, and
  none has been stood up since. Unit + mutation only.
- The local `scripts/cdc_dlq_demo.py` can no longer produce an oversized-value DLQ at all: its
  1.2 MiB `longtext` now replicates normally, and `product_media`'s only blob is excluded at
  capture. Its docstring now says so and points at schema drift as the reliable trigger.

## v0.1.466

### Changed

- **Published `0.1.465` to all three registries, deployed it to both live app stacks, and
  repointed the `ContainerImageUri` default to it.** The v0.1.465 notes above describe
  publishing `0.1.464`; what actually shipped and now runs in production is `0.1.465`, which
  carries the same CDC per-value size-guard fix plus that release's own metadata. Publishing
  the newer tag was the deliberate choice: the Seoul stack pulls from its own
  ap-northeast-2 private ECR and had to be built anyway, and building the current tree under
  the older tag would have produced an image tagged `0.1.464` whose UI reported `0.1.465`
  (the displayed version comes from installed package metadata, not `pyproject.toml`) — the
  exact tag/version drift this repo has been bitten by before.
  - ECR Public + us-east-1 private + ap-northeast-2 private all on `0.1.465`; the live
    release gate passed against a real cluster before each build.
  - `mysql-dsql-migrator` (us-east-1): task definition `app:53` -> `app:54`, image
    `0.1.463` -> `0.1.465`, 1/1 running, ALB target `healthy`.
  - `mysql-dsql-migrator-seoul` (ap-northeast-2): `app:99` -> `app:100`, same image bump,
    1/1 running, 4096 CPU / 8192 MiB and the Cognito user pool (with its user) untouched.
  - Both updates were image-only (`--use-previous-template`, one overridden parameter out of
    26, the rest `UsePreviousValue`), and the stack events confirm only the task definition
    and the ECS service changed -- no load balancer, listener, security group or user pool.

## v0.1.465

### Changed

- **Published the container image at `0.1.464` and repointed the `ContainerImageUri`
  default** in `deploy/cloudformation.yaml` to
  `public.ecr.aws/z0q0i9j0/mysql-dsql-migrator:0.1.464`, so a fresh `git clone` deploy pulls
  the image carrying the CDC per-value size-guard fix (v0.1.464) instead of the build that
  still dead-letters `text` values DSQL accepts. Published to ECR Public and the us-east-1
  private ECR; the release gate passed against a live cluster first. No app-code change.

## v0.1.464

### Fixed

- **CDC dead-lettered `text` values that Aurora DSQL accepts — permanent, silent row loss.**
  The sink's pre-write size guard applied a flat 1 MiB ceiling to every `String` and `byte[]`
  value alike, so a 2 MiB `longtext` row went to the DLQ instead of the target. Measured live
  on a real cluster (2026-09-19): a `text` value stores **intact at 1 / 2 / 4 / 6 / 8 / 8.5 /
  9 / 9.5 MiB** and fails only at 10 MiB -- the per-write-transaction limit, where the server
  severs the connection rather than returning an error -- while `bytea` rejects anything over
  1048576 bytes (`ProgramLimitExceeded: datatype limit greater than 1048576 bytes not
  supported for bytea`, SQLSTATE 54000). The flat cap was calibrated in June 2026, when text
  *did* behave that way; DSQL has since raised it, and the guard was never revisited.
  - **Why it mattered beyond the dropped rows:** Full Load reacts to the real DSQL error
    instead of pre-checking a size, so it migrated a 2 MiB `longtext` **fine** while CDC
    dead-lettered the same value. A Full Load + CDC migration therefore diverged silently on
    exactly that column -- the tool's own Validation would later report the difference with no
    explanation for it.
  - The guard is now per type: `byte[]` at 1 MiB (DSQL's real `bytea` cap) and `String` at the
    chunk byte budget, declared **as** `MAX_BATCH_BYTES` rather than a second 8 MiB literal so
    tuning one cannot strand the other. `binary.handling.mode=bytes` is what makes the Java
    type a sound discriminator: a blob arrives as `byte[]`, text/json as `String`.
  - The dead-letter reason now carries the **measured size and the limit breached**
    (`body (2097152 bytes > 1048576)`). It previously named only the column, so a DLQ reader
    could not tell a marginal value from a 9 MiB one, nor which ceiling applied.
  - Plugin artifact rebuilt; `PLUGIN_VERSION` v38 -> v39. A running cdc-stack keeps its current
    plugin until Delete + Deploy infra.
- **Three harness scripts died at import and had done so for weeks** -- one of them a
  committed, documented entry point (`scripts/run_full_load.py`; the other two,
  `run_fullload_resume_harness.py` and `verify_fullload_edgecases.py`, are local-only).
  All three imported
  `dsql_migrator.ui.data_migration._engine`, a private submodule the v0.1.346-351 refactor
  split into `_full_load_engine`. Every invocation raised `ModuleNotFoundError`, and
  `run_full_load.py` is listed in `scripts/README.md`. All four import sites
  now use the **package**, whose re-exports are the stable surface, so a future split cannot
  break them again.
- **The oversized-LOB picker no longer implies one ceiling for both types.** It labelled the
  checkboxes "Columns that can exceed DSQL's 1 MiB per-value limit" while listing blob *and*
  text columns, pointing the user at the wrong box; it now names each type's real bound. The
  manual's limitations table (en/ko/ja) is corrected the same way.

### Verified live

- **Review item 5 re-verified end to end on the current build** (the workshop item that could
  not be checked at the time, because the oversized column had been excluded up front so
  nothing ever quarantined). A real MySQL source with three 1.5 MiB `longblob` rows loaded
  into the real Seoul cluster: DSQL rejected exactly those three (54000), the two fitting rows
  landed (confirmed by `COUNT(*)` on the target), and the rejection became a **durable
  per-chunk `rows_quarantined=3`**. Rendering the panel from that job with an **empty error
  log** -- precisely a restored session -- still offers **"Exclude column & reload"**, keyed on
  the durable count rather than the in-memory messages. The unit suite covers this with a
  double; what a double cannot establish is that a real DSQL rejection becomes a per-chunk
  count at all, which is the seam every defect found this session sat on.

### Tests

- 3847 Python green (+41 new), 107 Java green (+6 new). Nine mutations checked, each caught:
  re-applying the flat 1 MiB cap to `String` (the exact bug -- caught by the 2 MiB-text case),
  loosening `bytea` to the String ceiling, dropping the size/limit from the dead-letter reason,
  unbinding the String ceiling from the chunk budget, and restoring the broken `_engine`
  import path.
- New `tests/test_scripts_imports_resolve.py` resolves every `dsql_migrator` name the
  `scripts/` harnesses import, statically -- the scripts are never executed, so nothing
  connects to a database. `tests/test_no_undefined_globals.py` could not have caught this: it
  walks the `dsql_migrator` package, and `scripts/` is outside it.

## v0.1.463

### Fixed

- **BLOCKER: "Apply foreign keys" destroyed the Full Load's own result and blocked the CDC
  handoff.** The action was submitted with `migration_state.job_id = job_manager.submit(...)`
  -- copied from the retry path, where the new job legitimately IS the load. Here it made the
  Full Load panel read the FK job instead, so the moment the operator ran the action the tool
  itself had told them to run: the per-table rows became "No data available", "7/7 tables
  settled" became 0/0, the green completion summary vanished, and the **export watermark** was
  replaced by "the consistency point is captured when the migration starts". CDC then reported
  "No usable Full Load watermark in this session", leaving a choice between CDC and
  referential integrity. A page refresh did not recover it. The action now has its own job
  slot (`fk_apply_job_id`) and never writes the load's.
  - The data was never at risk -- only the job record the panel and the CDC seed read.
- **The outstanding-foreign-keys notice is `warning`, not `error`.** It renders directly under
  a green "Full Load complete", where a red card reads as "my load failed" -- which is how a
  workshop participant read it. The load did succeed; foreign keys are the next required step,
  which is exactly what `warning` means in this design system.
- **The running action now shows progress and can be stopped.** Clicking it previously gave no
  confirmation, no spinner and no count until it finished -- while the card itself warns that
  each constraint "reads the whole child table". On a real schema an operator assumes it hung
  and clicks again. It now shows "Applying foreign keys — N of M done" with a spinner and a
  "Stop applying" button, and the start button is withdrawn while it runs.
- **Schema Conversion no longer describes foreign keys as automatic.** Three notices said they
  are "(re)created at the end of Full Load" / "as a post-load ALTER TABLE … ADD CONSTRAINT
  pass", which reads as something the tool does by itself. All three now name the
  **"Apply foreign keys"** button and say the step is explicit, and why (the orphan pre-check
  reads every child table).

### Tests

- 3806 green. Five mutations checked, each caught: re-clobbering the load's job slot (both
  from the action and from the setter), restoring the error tone, removing the running state,
  and a notice dropping the button's name.

## v0.1.462

### Fixed

- **v0.1.461's "Apply foreign keys" action stored the wrong shape, so the Data Migration step
  raised `TypeError` on the next render after clicking it.** `_apply_foreign_keys` returned
  only the FAILED count (an int -- all the load needed for one summary clause), while the new
  action stored it as the `(applied, skipped, failed)` triple the step renders; the renderer
  then called `list(<int>)`. Since v0.1.461 removed the automatic apply, that button is the
  only path to referential integrity in Step 3 -- so this would have shipped a step that
  breaks the moment it is used. The function now returns the full triple (the action is its
  only caller).
  - **Found by live verification, not by the suite.** A Fargate run against the real Seoul
    source + DSQL target showed `result=0` while three constraints had actually appeared on
    the target -- the contradiction that gave it away. Each piece was unit-tested; the SEAM
    was not, so `test_the_fk_action_result_renders` now carries what the action STORES into
    the renderer, and reverting to the int fails two tests.

### Verified live (Seoul Fargate, against real Aurora MySQL + Aurora DSQL)

The new v0.1.461 flow, end to end:

| check | result |
| --- | --- |
| the load completes and creates NO constraint | PASS |
| the step is told how many are outstanding | PASS (2 of 2) |
| the action applies them, and they exist on the target | PASS (`(2, 0, 0)`) |

## v0.1.461

### Changed

- **Foreign keys are now applied by an explicit "Apply foreign keys" action on the Data
  Migration step, instead of automatically as the last act of the load.** The orphan
  pre-check that gates each foreign key reads the whole child table, and it ran AFTER every
  row was already written -- so "Full Load" stayed unfinished for many minutes with nothing
  left to load. Measured on this schema (15 foreign keys over 15.46M child rows): **28.0 min
  serial, 6.9 min at the measured concurrency**. The load now reports complete when the DATA
  is complete.
  - This also makes both migration types one flow: a CDC migration has always applied its
    foreign keys from a button at cut over (they must not exist while the sink streams
    out-of-order rows -- it dead-letters an FK violation, SQLSTATE 23503). The Full-Load-only
    path was the odd one out.
  - The orphan pre-check itself is unchanged and still mandatory. It cannot be replaced by
    DSQL's own `ALTER TABLE ASYNC ... VALIDATE CONSTRAINT`, because a FAILED validation is
    unobservable (live-verified: `convalidated` stayed false for 240s with a real orphan --
    indistinguishable from "still running" -- no `sys.jobs` row ever appeared, and a
    synchronous `VALIDATE CONSTRAINT` is `FeatureNotSupported`).
  - **A forgotten click is the risk this buys, so it is guarded three ways:** the run summary
    names the outstanding count and the action to take; the step shows an `error`-tone notice
    ("N foreign key(s) not yet applied — referential integrity is NOT in place until you
    apply them"), not a quiet button; and the cut-over runbook already offers the same action
    for any pending foreign key, whatever the migration type.
- **The orphan pre-gate fan-out is 16, measured rather than chosen.** A live sweep over the
  same 15 FKs: serial 1680.5s | 8-way 461.2s (3.64x) | **16-way 414.0s (4.06x)** | 24-way
  415.7s (4.04x). It saturates at 16 and 24 is fractionally worse -- the cluster doing the
  anti-join is the limit, not client concurrency. Every arm applied all 15 with zero failures.

### Known residual

Applying the foreign keys on the Data Migration step does not suppress the cut-over runbook's
offer of the same action (the two screens keep separate state). Re-applying is safe -- a
duplicate `ADD CONSTRAINT` is treated as success (SQLSTATE 42710) -- but it pays the orphan
pre-check again.

## v0.1.460

### Changed

- **The foreign-key pass's orphan pre-gates now run concurrently (bounded at 8), so the pass
  is no longer serial over every foreign key.** After v0.1.459's 27x query fix the pre-gate
  still took 28.0 min end to end for a 15-FK / 15.5M-row schema, and it is the whole cost of
  the pass --
  the `ADD CONSTRAINT ... NOT VALID` DDL itself measures 0.18s. The DDL stays SERIAL (DSQL
  takes one DDL per transaction); only the read fans out, each worker on its own short-lived
  connection.
  - Bounded rather than one connection per FK: this pass runs at **cut over** for a CDC
    migration, alongside a draining sink, on a live cluster.
  - Purely an optimisation: a pre-gate that fails or is missing falls through to the existing
    serial check, so the verdict and its error handling are unchanged. A failure is recorded
    as an exception, never as "0 orphans" -- reporting zero would add a constraint over
    violating rows.

### Measured, end to end (this schema: 11 tables, 15 foreign keys, 15.46M child rows)

| foreign-key pass | wall clock | applied |
| --- | --- | --- |
| anti-join, serial (v0.1.459) | **1680.5s / 28.01 min** | 15/15 |
| anti-join, 8-way pre-gate (v0.1.460) | **472.9s / 7.88 min** | 15/15 |

Concurrency gain **3.55x**, both arms in one run on one build, both applying all 15.

A note on method, because it changed a conclusion: the 6-minute figure first quoted for the
serial pass was EXTRAPOLATED from a single page's latency, and it was 4.5x optimistic --
per-page timing ignores accumulated overhead and the rising cost of deeper keyset positions.
On that projection the 8-way run (7.7 min) looked like a regression and was nearly reverted.
Only the end-to-end A/B showed it is a 3.55x win. Projections are not measurements.

### Why the pre-gate could not simply be removed (measured live)

Aurora DSQL only accepts `ADD CONSTRAINT ... NOT VALID`, which enforces new writes but does
not check existing rows (both confirmed: a bad insert raised `ForeignKeyViolation`, while a
pre-existing orphan was accepted). The natural idea is to let DSQL validate server-side with
`ALTER TABLE ASYNC ... VALIDATE CONSTRAINT` and skip the tool's own scan. That was measured
and **rejected**, because a FAILED validation is unobservable:

| observation | with a real orphan present |
| --- | --- |
| `pg_constraint.convalidated` | stayed `false` for 240s -- indistinguishable from "still running" |
| `sys.jobs` | no row ever appeared for the VALIDATE |
| synchronous `VALIDATE CONSTRAINT` | `FeatureNotSupported` -- no synchronous verdict exists |
| success case, for contrast | `convalidated` flipped `true` after 330s on a 3M-row child |

Success is observable; failure is not. Dropping the pre-gate would leave a constraint sitting
unvalidated over violating rows with no signal, which is worse than a slow check -- so the
tool's own count stays the verdict, and concurrency is the lever.

Also measured and rejected as levers, so neither is a guess: the page size (identical ms/row
from 5k to 250k; 500k exceeds DSQL's 300s transaction limit), the `array_agg` keyset-boundary
trick (0.21s per page on its own; `max()` genuinely has no uuid overload), a DISTINCT-key
anti-join (**5x slower** on a high-cardinality FK) and a semi-join-and-subtract (**6x
slower**).

## v0.1.459

### Changed

- **The foreign-key pass's orphan pre-gate is ~27x faster, for byte-identical results.**
  The paged orphan count used `COUNT(*) FILTER (WHERE ... NOT EXISTS ...)`, which makes the
  correlated subquery a per-row scalar expression -- so the planner probed the parent table
  once per child row instead of matching the two in bulk. The page is now pinned in a
  `MATERIALIZED` CTE and read twice: once as a `LEFT JOIN` anti-join for the count, once for
  the keyset boundary. The window stays UNFILTERED, which is the correctness property the
  FILTER existed to protect (a filtered window would skip PK ranges and under-count).
  - Measured on a live ap-northeast-2 cluster over a 100k-row window of a 3M-row child:
    **0.59 ms/row -> 0.022 ms/row**. For an 8.5M-row schema with 15 foreign keys
    (15.5M child rows scanned in total) the serial pass measures **28.0 min end to end**
    (a per-page extrapolation had suggested ~6 min -- see the note below). All three candidate
    formulations returned exactly the same `(orphan_count, last_pk, row_count)` as the
    shipped query.
  - This is the pre-cut-over gate, so the cost landed where it hurts most: for a Full-Load
    migration it blocks the end of Step 3, and for a CDC migration it runs at **cut over**,
    after the source is already frozen. The same paged query backs Validation's optional
    orphan check, which gets the same speedup.
  - Ruled out by measurement first, so the change is not a guess: the page size is NOT the
    lever (identical ms/row from 5k to 250k, and 500k exceeds DSQL's 300s transaction
    limit), and neither is the `array_agg` keyset-boundary trick (0.21s per page on its
    own). `max()` genuinely has no uuid overload, so `array_agg` stays.

## v0.1.458

### Fixed

- **v0.1.456's liveness report in the foreign-key pass was at the wrong level, so a healthy
  run could still be reaped.** The pass reported once per FOREIGN KEY, but that is not where
  it spends its time: the orphan pre-gate pages a large child at 5000 rows per round trip --
  roughly 600 round trips for a 3M-row child -- so a SINGLE foreign key left the pass silent
  for minutes. The pre-gate now reports liveness per PAGE, the same shape used for
  Validation's bounded scans.
  - **Found by live verification, not by the suite.** A real Full Load of 11 tables / 8.5M
    rows with foreign keys preserved (15 of them), run in Seoul Fargate under a deliberately
    short 90s stall watchdog, was reaped mid-pass. Instrumenting the run located it exactly:
    `_apply_foreign_keys` started at +411.0s, the last liveness report was at +412.3s, and
    the pass never returned. The review that produced v0.1.456 had flagged this cost
    ("each orphan pre-gate pages the child at 5000 rows per round trip"); the fix simply put
    the report a level too high.

### Tests

- 3795 green. Two mutations checked, each caught: removing the per-page report, and stopping
  the pass from threading its heartbeat into the pre-gate.

## v0.1.457

Completes the sweep started in v0.1.453: state keyed by an identifier that a legitimate
user action changes underneath it. v0.1.453 fixed the Full Load half of the job-id problem;
this fixes the CDC half and the remaining stragglers.

### Fixed

- **A Full Load retry made every recorded CDC dead-letter unreadable.** `cdc_error_log_key`
  returned `migration_state.job_id` live, and a retry ("Retry unfinished tables" or the
  per-table "Reload" -- both supported WHILE CDC streams) replaces it. So every CDC surface
  re-keyed mid-stream: the DLQ card fell to a grey "0 quarantined / No records
  quarantined", its Download and Ask-AI-DBA buttons disappeared, the per-table consistency
  badges flipped to "consistent", and the schema-drift banner cleared -- for records that
  were still there under the previous job id. The key is now PINNED for the life of the
  migration state (Start over builds fresh state, so the pin never outlives a migration),
  and the per-stack fallback for CDC-only sessions is unchanged.
- **The DLQ card's "the Full Load also set N rows aside" cross-reference vanished after a
  retry** -- it read the bare key instead of resolving the retry lineage, so the two halves
  of the same pre-cut-over screen disagreed. It now resolves the lineage, as does the AI
  DBA's `list_failed_full_load_tables`, which was handing the model `"error": ""` for
  exactly the tables the retry did not re-run while the screen beside it showed the reason.
- **CDC dead-letters were re-written to the durable activity log on every controller
  rebuild.** The only dedup was the controller's in-memory cursor, and the controller is
  rebuilt on Start over, on app restart and for each new browser session -- so each rebuild
  re-read the blind 6 h look-back window and re-audited every record. N quarantines became
  2N, 3N lines and the log could no longer answer "how many rows did the pipeline drop?".
  The read cursor now lives on the session (a rebuilt controller resumes where the previous
  one stopped) and the durable write is idempotent per record identity.
- **`_CDC_ANNOUNCED` was wrong in both directions.** A retry changed its key, so every CDC
  event was re-announced to the AI feed (a stream that appears to start twice); and the
  stack-derived fallback key is IDENTICAL across migrations, so after Start over a fresh
  CDC-only session inherited the old markers and announced NOTHING at all. Now keyed by the
  migration state's identity as well as the pinned key.
- **The cached MSK Connect controller was never invalidated when the AWS profile or target
  region changed**, so CDC monitoring kept reading the old identity: connector health, DLQ
  depth and the per-table CDC columns stayed stale or empty (reads fail closed) and the
  profile fix the operator had just applied appeared to do nothing. Read-only surface only
  -- Deploy/Start/Stop build their clients at click time -- but silently wrong. Now dropped
  when `(region, aws_profile)` changes; a directly injected controller adopts the current
  identity rather than being discarded.
- **The controller's DLQ read cursor was not keyed by log group**, so attaching to another
  CDC stack reused the previous pipeline's cursor and the adopted pipeline's earlier
  dead-letters were never surfaced -- its DLQ card read "0 quarantined" for a pipeline that
  had quarantined rows. Cursor and seen-id set are now per log group.
- **After a retry completed the load, only the RETRIED tables got their identity sequence
  advanced.** The first attempt was incomplete so it synced nothing (by design), and the
  retry narrowed the sync to its own subset -- so every table that succeeded on the FIRST
  attempt kept its DSQL sequence at its start value while its rows already occupied those
  values, and the first application insert after cut over could fail with a duplicate key
  on a table the operator had no reason to suspect. The retry now syncs every table the
  completed run loaded.

### Changed

- The multiprocess sharding rule is now a pure predicate (`_sharding_is_consistent` /
  `_table_may_shard`) asserted by BEHAVIOUR. Its previous guard asserted the source-text
  substring `"_shardable_ok = bool(migrator._inputs.cdc_coexisting)"`, which stayed a
  PREFIX of the widened condition (`... or _shared_snapshot`) -- so it kept passing while
  no longer guarding anything, and the comment above it still claimed an invariant the code
  had dropped (a PostgreSQL non-CDC REPLACE does shard now, safely, on one exported
  snapshot). No behaviour change; the rule is unchanged and now stated once.

### Tests

- 3793 green. Five mutations checked, each caught. One of them initially SURVIVED -- the
  controller-identity test only exercised a path where the mutant was equivalent -- so the
  test was strengthened to assert the contract it documented.

## v0.1.456

The stall watchdog fails a job that has not refreshed its liveness clock for 900 s, but
only Full Load's progress drain ever refreshed it -- so most long-running work was silent
for its entire duration and was reaped **while perfectly healthy**. Nine such spans are
closed here, plus the two blocking-wait defects behind them.

Design note: liveness is reported at REAL UNIT BOUNDARIES (a completed page, object,
table, foreign key, or one slice of a deliberate wait), never from a background timer. A
timer ticking on its own would stamp liveness for a genuinely wedged job too, which is
exactly what the watchdog exists to catch. The mirror of this is v0.1.454's fix in the
other direction: a purely diagnostic read must NOT stamp liveness.

### Fixed

- **A validation run longer than 15 minutes was reported as a stall failure.** Progress
  went only to UI state, never to the job, so the whole run -- exact `COUNT(*)` + MD5
  checksum per table plus the trailing identity resync -- was one silence gap. The reap
  cannot be undone (`_mark_done` will not overwrite FAILED), so the operator saw a green
  MATCH report under a red "Failed" badge with Cut over refusing to open. Liveness is now
  reported per table AND per bounded page: a single 10M-row CHECKSUM table is thousands of
  paged round trips and exceeds the window on its own, so all six paging scans (source and
  target counts, both checksums, the PK reconcile streams, the orphan count) report each
  page through a new optional `Validator.set_page_hook` seam.
- **Full Load's entire post-load tail was silent** -- view recreate, the foreign-key pass
  and the identity resync all run AFTER the progress drain is joined. That tail is
  unbounded: up to 300 s per distinct still-building parent index (four such parents is
  1200 s) plus an O(rows) orphan pre-gate per FK. A fully successful load was therefore
  marked FAILED, which also gates Validation shut. The pass now reports liveness per
  foreign key and per poll of a building index, **and honours a stop** instead of running
  the rest of the pass after the user clicked Stop.
- **Full Load's pre-load DDL pass was silent too** -- the serial DROP+recreate of every
  "drop & reload" table is one fresh DSQL connect plus DDL each, and it runs BEFORE the
  drain thread that owns liveness starts. A few hundred tables crossed the window before a
  single row loaded. Now reports per recreated table.
- **A deliberately throttled Full Load was reaped as "unresponsive".** The source-load
  governor's pause reported liveness only on the pause/resume TRANSITION, so a sustained
  pause -- the whole point of the `max_source_threads_running` guard -- looked identical to
  a dead worker. It now reports once per wait slice, on both the in-process and sharded
  worker paths, via a liveness-only marker that does not disturb the drain's paused-reader
  count.
- **Full Load liveness was row-count based, not time based.** Progress flushed only once
  10 000 rows accumulated, on the assumption that 10 000 rows always land inside the
  window -- false for wide/LOB-heavy rows (a batch caps at ~8 rows when values approach
  DSQL's ~1 MiB limit), and a table with FEWER than 10 000 rows total never flushed
  mid-table at all, making the silent span the whole table load. A 30 s time floor now
  flushes as well.
- **The Schema Conversion apply, Evaluation, and cut-over "Apply foreign keys" jobs never
  reported liveness either.** Each is unbounded in its own way -- a fresh IAM-token/TLS
  DSQL connection per DDL statement, per-table source reflection of a very large schema,
  and the same O(rows) orphan pre-gates as the Full Load FK pass. All three now report at
  their per-object / per-phase / per-FK boundaries.
- **One wedged write batch could block Stop forever.** The DSQL connection set
  `connect_timeout` only -- no socket read timeout and no keepalives -- so a socket
  blackholed mid-statement (a NAT/ENI idle timeout, a dropped ENI) left `cursor.execute`
  waiting on the OS default; the drain never completed and a thread pool, unlike the worker
  processes, cannot be terminated. The connection now enables TCP keepalives and
  `tcp_user_timeout`, mirroring what the PostgreSQL source engine already sets. Keepalives
  rather than `statement_timeout` on purpose: they probe only when nothing is in flight, so
  a healthy-but-slow statement (a large keyset checksum page) is never cut short while a
  DEAD peer is detected and raised into the existing reconnect/OCC retry. **Live-verified
  against a real DSQL cluster** -- an unsupported connection option would break every
  connect while leaving the unit suite green (this is how v0.1.438 shipped).
- The batch drain's `wait(FIRST_COMPLETED)` is now a series of short bounded waits instead
  of one uninterruptible block. It still does not return until a batch completes, so the
  bounded-parallelism guarantee is unchanged.

### Tests

- 3786 green. Five mutations checked, each caught: silencing the heartbeat primitive,
  dropping validation's per-page seam, stopping a pager from calling its hook, removing the
  FK pass's per-FK report, and disabling the governor's per-slice report.

## v0.1.455

### Fixed

- **v0.1.454's cancel teardown terminated no worker at all.** It called
  `pool.shutdown(wait=False, cancel_futures=True)` and only then read `pool._processes` to
  terminate the wedged children -- but CPython's `Executor.shutdown` ends with
  `self._processes = None` **unconditionally** (both `wait` values), so the loop iterated an
  empty mapping. Stop still returned promptly (an incidental side effect of the same
  shutdown clearing the manager thread), but the worker processes were left running: each
  kept its source connection and could commit its one in-flight batch after the user had
  stopped the load. The children are now captured BEFORE shutdown.
  - The unit test hid it: its hand-rolled pool double kept `_processes` populated after
    `shutdown`, so a teardown that terminated nothing reported success. The double now
    matches CPython, and a companion test drives a **real** `ProcessPoolExecutor` with
    wedged children and asserts they are actually dead -- the third time in this file that
    a double or a source-text assertion has covered for a live defect.

## v0.1.454

Four defects found by probing test blind spots -- places where a test was structurally
unable to catch a class of bug (a frozen clock, or an assertion on a value the test itself
injected). Two further areas probed the same way turned out CLEAN and got the missing
assertion instead.

### Fixed

- **Stop never completed if a worker was wedged.** The sliced cancel wait correctly noticed
  its grace period expiring and abandoned the wait -- but `break` fell straight out of
  `with ProcessPoolExecutor(...)`, whose `__exit__` calls `shutdown(wait=True)` and so
  returns only once every RUNNING work item finishes. It therefore blocked on exactly the
  worker the grace period had just given up on (`future.cancel()` cannot stop an
  already-running task), leaving the job in RUNNING with the UI on "finishing the current
  batch" while the log already claimed the pool was torn down. The abandon path now stops
  accepting work and terminates the worker processes, so the following `shutdown` returns
  at once. The pre-existing test asserted substrings of `inspect.getsource` and never ran
  the wait, so it passed against the block.
- **A diagnostic memory sample counted as progress, so a wedged Full Load was never
  reaped.** The memory-pressure sampler read the in-progress table names through
  `handle.update` -- the WRITE path -- and `JobManager.apply_update` unconditionally
  refreshes `last_progress_at`, the stall watchdog's liveness clock. A Full Load whose
  worker was stuck but whose memory kept creeping therefore sat in RUNNING indefinitely
  with a frozen row count and no terminal affordance. `JobHandle` gained `snapshot()`, a
  read that does not touch liveness, and the sampler uses it.
- **The validator's reconnect budget did not cover the reconnect.** In
  `_ReconnectingCursor.execute` the retry `try:` began AFTER `_live()` (the DSQL connect
  factory) and `.cursor()`, so the one failure the budget documents itself as existing for
  -- "a fresh reconnect can transiently hit DSQL's new-connection rate limit" -- escaped
  after 1 of 4 attempts and 0.5 of 3.0s of backoff. That table was reported as errored with
  a raw driver message and the cut-over gate shut on a transient event the retry was meant
  to absorb.
- **An uncapped source-retry backoff could get a healthy job reaped as "stalled".**
  `base * 2**(attempt-1)` had no clamp (unlike the house pattern in `core/occ.py`), and
  neither wait loop reported liveness -- so raising the retry budget produced a wait longer
  than the 900s stall window, and the watchdog failed a job that was deliberately waiting,
  blaming "an unresponsive source/target connection" for the tool's own chosen pause. The
  delay is now capped (`_SOURCE_RETRY_MAX_DELAY_SECONDS`, kept under the stall window) and
  each wait slice sends an explicit heartbeat -- from a child process via the progress queue
  (`_HEARTBEAT`), which the drain turns into a liveness stamp that changes no job state.

### Tests

- Regression coverage for all four, each mutation-checked (7 mutations, each caught):
  reverting the pool teardown, making it wait, putting the sampler back on the write path,
  removing the backoff clamp, removing the heartbeat, moving the reconnect back outside the
  retry, and making `snapshot()` stamp liveness.
- Two areas probed and found CLEAN -- correct behaviour, no coverage -- now guarded:
  - The CDC deploy/delete wait deadlines DO fire (verified against loops whose fakes never
    settle). But both waits heartbeat every poll, which deliberately defeats the stall
    watchdog, so the deadline is the ONLY thing that can end a wedged wait -- and
    `_monotonic_deadline`/`_deadline_passed` had no direct test, so a sign or units error
    there would have shipped green as a job that never ends.
  - The `SourceLoadGovernor` TTL cache DOES expire. Its only TTL test froze the clock, so
    "caches within the window" was indistinguishable from "caches forever" -- verified by
    mutation: a cache-forever mutant leaves that test passing.

## v0.1.453

### Fixed

- **The FK pass logged "the index was still building ... waited 0.0s" for every foreign
  key.** The elapsed-time counter started BEFORE the first catalog probe, and that probe
  is a DSQL round trip (IAM token + TLS), so its own latency landed in the reported wait
  and made it non-zero even when nothing was waited for. The caller keys its log line on
  that value being non-zero, so a normal run emitted one line per FK -- self-contradictory
  (a 0.0s wait that was nonetheless "still building"), and the opposite of the silence
  this log was introduced to keep. In a live 6-FK run 5 of the 6 lines were spurious. The
  timer now measures the POLLING wait only: the probe runs first and a state other than
  `building` returns immediately with exactly `0.0`. A genuine wait (the same run's 32.6s)
  still logs.
  - Every pre-existing test for this helper injected `monotonic=lambda: 0.0` -- a FROZEN
    clock -- and the call-site test monkeypatched the helper to return a hardcoded
    `("valid", 0.0)`, so no test ever ran the real timer. The new tests let it run.
- **Hardening in the same pass, from an adversarial review of the fix above:**
  - A transient catalog read mid-wait no longer aborts it. `unique_index_state` swallows
    every exception and returns `None`, and the loop condition (`while state ==
    "building"`) treated that as "no longer building" -- so one blip ended a 300s wait
    after a single poll and the `ADD CONSTRAINT` raced the very index build being waited
    for. An unknown reading is now skipped (we already know it WAS building) and the last
    known state is kept, so the failure reason stays actionable instead of degrading to
    the generic "apply it manually".
  - One stuck parent index is waited for ONCE per pass, not once per referencing foreign
    key. The wait is per FK but the index is shared, so N FKs onto one parent cost N x the
    300s budget and emitted N near-identical log lines, each poll opening a fresh DSQL
    connection.
  - The log no longer prints a Python literal to an operator ("then it was None"); each
    state renders as a phrase that says what it means for the foreign key.
  - The helper's return annotation said `Optional[str]` while all three returns are
    `(state, waited_seconds)` tuples.
- **A retry blanked the quarantine reason for every table it did not re-run -- and took
  that table's recovery action with it.** The error log is keyed by job id and a retry runs
  under a NEW one (the job manager refuses to reuse an id), while the retry seeding carries
  non-retried chunks forward WITH their quarantined-row count. So those tables rendered a
  dropped-row count and no reason, and because v0.1.452's "Exclude column & reload" is
  keyed on those records, it disappeared for exactly the table that still needed it --
  fixing one oversized-LOB table stranded the others, leaving Start over as the only
  escape. `MigrationJob` now carries `table_error_job_ids` (table -> the job id whose
  records are authoritative), seeded on retry: re-run tables are deliberately absent so
  they get a clean slate, every other table keeps the id that recorded it, and repeated
  retries compose. Kept self-contained rather than a parent link, so resolution needs no
  ancestor lookup and survives job pruning; the error log stays append-only and its
  storage Protocol is unchanged.
- **The "N row(s) quarantined and ACCEPTED" audit line undercounted on a retry**, by every
  table the retry did not re-run -- understating what the operator was agreeing to
  permanently drop. It now counts across the same lineage.
- **The recovery action no longer depends on the error log at all.** The quarantine panel
  also keys on the durable per-chunk `rows_quarantined`, so a restored session -- whose
  in-memory error log is gone -- still offers "Exclude column & reload" instead of hiding
  the card entirely.

## v0.1.452

### Added

- **Recover from an oversized-LOB quarantine without starting over: "Exclude column &
  reload" on the quarantine card.** A value over Aurora DSQL's ~1 MiB per-value limit is
  dead-lettered and the table finishes `DONE` with quarantined rows, but the **Oversized
  LOB columns (optional exclusion)** tick boxes lock once a load has run — so the only way
  to exclude the offending column was **Start over**, discarding the whole session. The
  quarantine card now offers the recovery directly: it opens a picker listing that table's
  LOB columns, pre-ticking the one whose source type matches the DSQL type named in the
  quarantine reason (`bytea` -> `mediumblob`/`longblob`, `text` -> `mediumtext`/`longtext`),
  and on confirm records the exclusion, drops and recreates just that table, and reloads
  it. The confirmation spells out the impact: the column becomes NULL for every row, the
  exclusion applies migration-wide (including CDC capture), and the table is dropped and
  recreated.
- The action is offered but **disabled with the reason as its tooltip** when the excluded-
  column set is already baked into a CDC pipeline (a sink streaming, or a CDC stack in
  `infra` / `running` / `provisioning` / `partial` / `unstable`) or when no connection is
  verified — a silently missing control reads as a missing feature, and the reason is the
  actionable part. It is not rendered at all for a table with no excludable LOB column.

### Fixed

- **A missing import could no longer ship as a runtime `NameError` in a rendered page.**
  `tests/test_no_undefined_globals.py` now imports every module in the package and scans
  each function's bytecode for `LOAD_GLOBAL` names that resolve in neither module globals
  nor builtins. This is the class of defect that took v0.1.448 down; a successful module
  import does not prove a call site resolves.

## v0.1.451

### Fixed

- **The Full Load confirm dialog could fail to open at all (it raised
  `UnboundLocalError`).** The append/drop radio is created only when the probed tables
  hold rows AND CDC is not streaming — replace is disabled while a sink streams — but its
  change handler was attached under the looser "tables hold rows" condition. So with CDC
  streaming and the probed table already holding target rows, `reload_choice` was never
  bound and BUILDING the dialog raised, so the dialog never appeared. Reachable from the
  per-table **Reload** and **Retry unfinished tables**. The handler's guard now matches
  the radio's creation condition exactly.

## v0.1.450

### Fixed

- **Drop & reload was blocked by the foreign keys the tool created itself, and blamed a
  view.** DSQL refuses `DROP TABLE orders` while `order_items`' foreign key still
  references it, and a `Full load only` run creates exactly those FKs in its post-load
  pass — so choosing **Drop & reload** afterwards hit a wall built by the tool. The
  recreate path now removes them first and the existing post-load pass puts them back,
  the same pre-drop/recreate pairing the dependent-VIEW pass already used. Only FKs from
  this migration's own conversion whose PARENT is being replaced are touched (derived from
  the same `foreign_key_ddls` the apply pass uses, so a hand-made constraint is never
  dropped, and no catalog read is needed); an FK that merely lives ON a replaced table
  needs no action because `DROP TABLE` takes its own constraints with it. Each removal is
  logged, naming the constraint and that the post-load pass re-creates it.
- **The failure message named the wrong cause and gave an impossible instruction.**
  `dependent_objects_hint` parsed only `view|materialized view … depends on`, so DSQL's
  real DETAIL (`constraint fk_order_items_orders on table order_items depends on table
  orders`) fell through to a fallback that blamed "usually a view" and told the user to
  "select the dependent object in the object browser" — impossible for a foreign key,
  which is not an item there, leaving no way out of the screen. It now names the
  constraint and its table and offers an action that exists (drop it yourself, or choose
  Append), and the unparseable fallback no longer asserts the blocker is a view: it names
  both causes with the remedy for each.

## v0.1.449

### Fixed

- **BLOCKER: the Data Migration step could not render at all on 0.1.448.** That release
  added a `lob_exclusion_lock(...)` call to `ui/data_migration/__init__.py` without
  importing the helper, so every render raised `NameError: name 'lob_exclusion_lock' is
  not defined` and the Full Load screen showed only "Data Migration could not be
  displayed". A missing global is raised only when the function RUNS, so the module still
  imported cleanly and nothing caught it before the image reached three registries and both
  live app stacks. 0.1.448 should not be used; upgrade to 0.1.449 (or stay on 0.1.447).

### Added

- **A guard for the whole class of bug that shipped it** (`tests/test_no_undefined_globals.py`).
  After importing each module it scans every function's bytecode for `LOAD_GLOBAL` — the
  opcode that reads a module-level or builtin name — and asserts each one resolves. That
  catches any call to a helper that was never imported, in any module, including nested
  functions and comprehensions, with no NiceGUI double, no rendering and no new dependency.
  Verified against the real defect: with the import removed it fails with
  `build_data_migration_screen.<locals>.content: lob_exclusion_lock`. Function-local
  imports (the repo's deliberate lazy-import pattern) compile to `LOAD_FAST`, so they are
  unaffected.
  - The regression test that was supposed to cover the 0.1.448 change used
    `inspect.getsource`, which reads source TEXT and so passed with the import missing. It
    now resolves the name through the module namespace. This was the SECOND time in two
    releases that a test exercised a helper but not its call site — v0.1.446's own commit
    message recorded the first — which is why the check is now mechanical and package-wide
    rather than per-change.

## v0.1.448

### Fixed

- **The foreign-key index wait left no trace, so v0.1.446's fix could not be verified in
  the field.** `_wait_for_referenced_unique_index` polled while the referenced unique index
  was still building but logged nothing at all — no activity entry, no logger call. On a
  successful run "the wait held the FK back until the index was ready" and "the race
  happened to be won" were therefore indistinguishable: the gap between the last table
  load and the FK pass measured ~5.4s either way, and the interval was empty in both the
  activity log and CloudWatch. A wait that actually happens is now recorded as one INFO
  entry naming the elapsed time and what it waited on; when the index was already valid
  (the normal case) nothing is logged, so an ordinary run gains no noise. The failure
  detail also names the elapsed wait, which is what lets an operator tell "the index build
  is slow, retry shortly" from "there is no such index" — the two raise the same SQLSTATE.
- **A frozen oversized-LOB tick box on the Full Load screen now explains itself.** The
  screen passed `lock_reason=None`, and the panel only renders text `if locked and
  lock_reason`, so the boxes were frozen with NO explanation — the "silent frozen box reads
  as a bug" state that panel's own comment forbids. The table picker's wording does not
  cover it either: it says to use 'Start over' "to migrate a different set of tables",
  while a user who forgot to exclude a column is trying to change the COLUMNS, so it does
  not read as applying to them even though the way out is the same. The screen now uses the
  LOB-specific `lob_exclusion_lock` reason, which was already written for exactly this case
  (it explains that the excluded columns are fixed for this migration, why changing them
  would leave loaded rows inconsistent with what CDC captures, and that 'Start over' is the
  way to change them).

## v0.1.447

### Changed

- **Scoping the source to a single MySQL database no longer loses that database name on
  the target.** With `Database` set on Connect, inventory names stayed UNQUALIFIED
  (`orders`), so the conversion emitted no `CREATE SCHEMA` and the table was created in
  DSQL's default `public` schema. Cluster-wide mode (blank `Database`) has always
  qualified (`ecommerce.orders`), so the same source produced two different target
  layouts depending on a field whose own hint describes SCOPE, not naming — and nothing
  disclosed the difference. Single-database mode now qualifies too, so
  `ecommerce.orders` on MySQL becomes `ecommerce.orders` on DSQL. This fixes four
  problems at once:
  - an application querying `ecommerce.orders` keeps working after cut over (it had to be
    rewritten for `public.orders`);
  - migrating two databases no longer collides — both used to land in `public`, so any
    shared table name overwrote the other;
  - **CDC in single-database mode was structurally broken.** The Java sink has always
    derived its target as `<source db>.<table>` (`DebeziumEvents.resolveTable`, whose own
    comment warns that dropping the schema "would silently route streamed changes to
    `public` … splitting one table across two schemas"), while the control plane fed it
    bare names: Debezium's `table.include.list` matched nothing, `SinkTopics` named topics
    Debezium never writes, and any record that did flow was written to `ecommerce.orders`
    on a target that only had `public.orders`. Every existing E2E/soak ran with qualified
    names, so this path was never covered;
  - a foreign key's PARENT is now qualified together with its child. Qualifying only the
    child left `ecommerce.orders` referencing a bare `customers`, which resolves through
    the target's default `"$user", public` search_path — so the orphan pre-gate raised
    42P01 and **every foreign key was skipped**, or, on a cluster still holding tables
    from an earlier bare-name run, it silently bound a stale `public.customers` and
    enforced integrity against dead data.
- **A session saved before this change still restores.** Old snapshots hold bare names, so
  the restored selection would have failed to resolve (`TableSelectionError`), the
  per-object conversion edits would have orphaned, and the Validation report would have
  named tables that no longer exist. Bare names are brought to the current qualified
  vintage on restore (inventory + FK parents, selection, edited DDLs, validation report);
  an already-qualified snapshot and one with no recorded source database are untouched.
  Tables an older build put in `public` are deliberately NOT reconciled — that placement
  was the defect, not state worth preserving.

## v0.1.446

### Fixed

- **The post-load foreign-key pass raced `CREATE INDEX ASYNC` and silently dropped the
  foreign key on a composite-primary-key parent.** A table Full Load has to RECREATE (a
  composite-PK re-key is the common reason) gets its secondary indexes only AFTER the
  load, as `CREATE INDEX ASYNC` — which returns immediately while DSQL builds in the
  background. The FK pass ran seconds later and never waited, so an FK whose parent's
  single-column uniqueness comes ONLY from one of those async indexes failed with
  `SQLSTATE 42830` (*there is no unique constraint matching given keys for referenced
  table*). Being a race the same procedure could pass on one run and lose an FK on the
  next (observed: `6 applied, 0 failed` then `5 applied, 1 failed` with 5.3s between the
  last table load and the failure). No data was lost, but the migration finished
  "successfully" with referential integrity missing. The pass now waits for the referenced
  UNIQUE index to become valid before adding the constraint.
  - DSQL raises the SAME 42830 whether the required index is MISSING or merely still
    BUILDING, so the fix distinguishes them via `pg_index.indisvalid` (new
    `unique_index_state()`, live-verified: `indisvalid` is false while building and flips
    true when ready). **building** waits (bounded, generous — the build scales with the
    table); **absent** returns immediately because waiting is pointless; a stuck build
    gives up at the budget rather than hanging the run.
- **A foreign key that could not be applied now says WHY.** The activity log — the artifact
  an operator downloads and pastes into a runbook — recorded only the fixed string "could
  not be created automatically; apply it manually", while the real exception went solely to
  the module logger (CloudWatch showed one bare warning line, no driver message). The entry
  now carries the SQLSTATE, the driver's own message, the cause in operator terms (index
  still building vs no unique index at all), and the exact `ALTER TABLE … ADD CONSTRAINT`
  statement to re-run.
- **`run completed` no longer reads as a clean SUCCESS when a foreign key is missing.** The
  summary said only "N table(s) loaded", so a reader who scans just that line missed the
  shortfall (which is how the failure got mistaken for user error). It now appends
  "N foreign key(s) NOT applied" when any failed; a clean run's wording is unchanged.

## v0.1.445

### Fixed

- **The v0.1.444 CDC foreign-key gate did not fire in the flow it was built for — it
  failed OPEN.** The bare-name branch of the target lookup filtered on
  `pg_table_is_visible(c.oid)`, which depends on the connection's `search_path`, and the
  tool's DSQL connections use the default `"$user", public` (only the Query Playground
  ever sets a search_path). A target table in any other schema therefore read back as
  `[]` — which the reader documents as "read the catalog, this table genuinely has none"
  — so the gate saw a clean target and started streaming with the foreign keys still
  enforced. That is the original silent-data-loss bug unchanged: out-of-order child rows
  get `SQLSTATE 23503` and are dead-lettered permanently. Live-confirmed: the same table
  returned `['fk_child_parent']` when asked by qualified name and `[]` when asked by bare
  name, flipping the gate from blocking to open. A bare name is now resolved across every
  USER schema (system schemas excluded, the idiom this module already uses elsewhere), so
  the answer no longer depends on session state, and a name that is constrained in
  SEVERAL user schemas reports `None` (ambiguous → blocks) instead of guessing or merging
  results. A table that genuinely has no constraints still reports `[]`, so an ordinary
  CDC start is not gated.
  - The sibling readers (`target_primary_key_columns`, `target_primary_keys`,
    `target_required_columns_without_default`) share the `pg_table_is_visible` idiom but
    are NOT affected in the same way: a miss maps them to `None` (unknown), never to a
    positive "there are none". They do lose information for a bare name outside the
    search_path, which is recorded as a separate follow-up rather than changed in a fix
    release, because their callers drive Full Load recreate/append decisions.

## v0.1.444

### Fixed

- **A `Full load only` run no longer leaves foreign keys that make a later CDC stream
  silently discard rows.** Full Load's post-load pass applies AND validates the preserved
  foreign keys unless one of three CDC signals is set, and all three are unset for a
  `Full load only` run — at load time nothing knows CDC will follow. If the user then
  switched the same session to `CDC only` and started streaming, the sink applied change
  records across several tasks with no parent-before-child ordering, so a child row could
  arrive before its parent, be rejected with `SQLSTATE 23503`, and be **dead-lettered
  permanently**: 23503 is not in the sink's transient set, so there is no retry, the task
  keeps running and offsets advance. The rows were lost with nothing failing loudly, and
  because it is a race the same run could pass (`Quarantined 0`) or lose rows. This also
  contradicted the tool's own documented model ("re-created on Aurora DSQL only at cut
  over, never during replication"). Starting CDC now checks the target first and refuses
  while any enforced foreign key is present:
  - the Start CDC dialog reads the target's `pg_constraint` off the event loop, names
    exactly which constraints block, explains why, and disables Start;
  - a **Remove foreign keys** action drops only the constraints THIS migration's own
    conversion renders (matched on the `(table, constraint name)` pair — never a
    constraint the user created and the tool could not put back), logging each one to the
    activity log; cut over's existing idempotent "Apply foreign keys" step re-creates
    them once the stream has drained;
  - the check is repeated inside the job body immediately before anything is created, so
    the dialog-less **Retry CDC** path is covered too;
  - detection **fails closed**: an unreadable catalog counts as blocking, because
    reporting "no foreign keys" for a target we could not read would open the gate on
    exactly the case it exists for.

## v0.1.443

### Changed

- **Composite-key warning now reads as one severity.** v0.1.442 corrected the stale "Not yet
  supported with CDC." claim but replaced it with a reassurance ("CDC handles this
  automatically"), which sat oddly inside an amber warning whose header is an imperative
  ("Queries must use the new composite key after cutover") and diluted the action the user has to
  take. The same fact is now stated as the obligation it implies -- keep every key column in
  capture, because change records are keyed on the composite key, so excluding one stops
  replication for that table. The notice stays `warning` (not `info`): unlike the IDENTITY
  option's advisory "expect gaps and loose ordering", choosing a composite key creates a
  MANDATORY application change plus a hard DSQL immutability constraint, and ignoring either
  breaks the application after cutover.

## v0.1.442

### Fixed

- **Composite-key warning no longer claims CDC is unsupported.** Choosing "Composite key" in
  Schema Conversion ended its warning with "Not yet supported with CDC." — true when the option
  shipped in v0.1.64, but composite-key CDC has worked since: `composite_key_columns_for_cdc()`
  tells Debezium to key each change record on the target composite key
  (`message.key.columns`), so the sink's `ON CONFLICT` / `DELETE` match without any sink change.
  The notice now states the real constraint: it works automatically as long as no key column is
  dropped from capture by the column exclude list (the one precondition CDC start already gates).
- **AI Assist no longer claims to be working when Amazon Bedrock is denying every request.**
  The app modelled AI *intent* but never AI *capability*: `ai_assist.enabled` (the opt-in) was
  persisted and restored, while the only record of a successful Bedrock preflight was a
  page-local dict inside the Connect card. Every AI affordance and the journey-header diagram
  read the preference alone, so a resumed session painted a green "AI assist: On" chip and live
  AI buttons while each reply failed with "access to Amazon Bedrock InvokeModel was denied".
  AI capability is now session state (`ai_verified`) with a single source of truth,
  `ai_availability()`, that the whole UI consults:
  - the header chip has three honest states -- green **"AI assist: On"** only after a clean
    preflight, neutral **"AI assist: On (unverified)"** when enabled but never checked, and
    amber **"AI assist: unavailable"** once a denial is observed (the same "reconnect" semantic
    the source/target nodes use for restored-but-unverified endpoints);
  - a real `ACCESS_DENIED` reply now **records** the failure instead of discarding it, so the
    header and every step's AI button self-correct after the first failed call, and the per-step
    affordances go disabled-with-a-reason rather than offering an action that can only fail.
    Transient failures (throttle / network) deliberately do NOT latch;
  - editing the toggle, the Bedrock model, the region, **or the AWS profile** now invalidates a
    prior verdict, so a green "Verified" badge cannot survive switching to a profile without
    `bedrock:InvokeModel`.
- **A restored session no longer silently changes its AWS credential identity.** `aws_profile`
  (a profile *name*, never a credential) was not part of the session snapshot while
  `ai_assist.enabled=True` was, so a session whose AI worked under a named profile came back
  invoking Bedrock through the environment credential chain -- the likeliest source of the
  AccessDenied above. It is now persisted, restored first, and part of the dirty-check signature.
- **Turning AI Assist off is saved immediately.** Nothing on the Connect screen invoked the
  snapshot hook, so switching AI off and reloading before navigating re-applied the stale AI-ON
  snapshot and silently re-enabled it.
- **The "Verified" badge no longer disappears just because you navigated away.** The preflight
  verdict lived in a closure that was re-created on every Connect render; it now lives on the
  session, so leaving Connect and returning keeps a verified state.

## v0.1.441

### Added

- **Full Load: "Reader shards per large table" is now a Settings knob (was env-only).** A large
  single-integer-PK table was read by ONE keyset reader that is CPU-bound (per-row type
  conversion) and tops out near one core, so a migration dominated by one huge table stalled near
  single-core speed. Reader range-sharding (K concurrent readers over disjoint PK slices) already
  existed but was only settable via `DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS` — invisible to a
  browser operator. It is now exposed in the Settings "Full Load" group alongside Tables in
  parallel / Batches per table / Rows per batch (default `1` = off; range 1–8). Measured **~3.8× at
  K=4** on an 8M-row load. Applies only to a single-integer-PK table with at least the shard
  minimum rows; raises source read concurrency (total source readers = tables-in-parallel × this),
  clamped under the source connection ceiling; takes effect at the next Full Load run.

## v0.1.440

### Changed

- **Source-agnostic wording.** Now that the tool migrates from PostgreSQL sources as well as
  MySQL, neutralized the user-facing text that still implied a MySQL-only source: CloudFormation
  parameter labels/descriptions (e.g. "Source MySQL port" → "Source DB port [3306 for MySQL /
  5432 for PostgreSQL]", "MySQL source (optional)" → "source (MySQL/PostgreSQL, optional)"), the
  CLI `--help` description, cut-over UI strings ("… the source database to DSQL"), the CDC source
  secret's description, and assorted comments/docstrings. Correct MySQL-specific references are
  left as-is (the MySQL→PostgreSQL Query Converter, the binlog/GTID CDC stub, MySQL-JDBC error
  signatures, MySQL grant names, the MySQL branch of source-aware conditionals). Deployed-resource
  names (the plugin bucket, the CDC secret name, the app/build CloudFormation stacks, the ECR image
  repo), the `mysql-dsql-migrator` CLI command, and the Python distribution name are intentionally
  unchanged — renaming those is a coordinated migration that would disrupt live deployments, so it
  is deferred.

## v0.1.439

### Changed

- **Source-neutral CDC stack naming (`dsql-cdc-`), backward-compatible.** The CDC
  CloudFormation stack prefix was `mysql-dsql-cdc-`, which is misleading now that the tool
  migrates from PostgreSQL sources too. The canonical prefix is now `dsql-cdc-` (default
  stack name `dsql-cdc-stack`), and the legacy `mysql-dsql-cdc-` prefix is still fully
  accepted so **existing deployments keep working**: stack-name validation accepts either
  prefix, account-wide CDC-stack discovery matches both (so an already-deployed
  `mysql-dsql-cdc-*` stack is still found and offered for adoption rather than orphaned), a
  persisted session name is restored as-is, and both app-stack and EC2-host `CdcDeployRole`
  IAM policies scope every resource ARN to *both* families. New stacks are minted with the
  neutral prefix; the suffix-only UI field now shows `dsql-cdc-`.

### Fixed

- **Dev/E2E harness: `scripts/compare_rows.py` now supports a PostgreSQL source** (via
  `SOURCE_TYPE=postgres`) — it was MySQL-only (PyMySQL + `constraint_name='PRIMARY'` PK
  detection + backtick quoting), so a PG-source cut-over check misreported "SOURCE MISSING".
  It now connects with psycopg, detects the PK via portable standard SQL
  (`constraint_type='PRIMARY KEY'`, correct on both engines), and quotes identifiers per
  engine. Also fixed two latent bugs surfaced by this: `.env` values overrode explicit
  environment variables (now os.environ wins, matching the other harnesses) and the source
  password bypassed that precedence. `scripts/cdc_consistency_check.py` inherits the fix and
  no longer backtick-quotes against a PG source. (Harness only — the shipped, dialect-aware
  Validation was always the authoritative PG cut-over verdict and is unaffected.)

## v0.1.438

### Fixed

- **Regression: the DSQL target connection failed outright (`lc_numeric` not supported).**
  v0.1.435 added `-c lc_numeric=C` to the Aurora DSQL connection's startup options to make
  the Validation checksum's decimal point locale-independent. But **DSQL rejects `lc_numeric`
  as a startup/session parameter** (`FATAL: setting configuration parameter "lc_numeric" not
  supported`), so *every* DSQL connection through the tool failed — Test target connection,
  Full Load, Validation, and Cut over all broke against a live cluster. The unit suite missed
  it because those tests use an in-memory DSQL double that doesn't validate connection
  options; caught by a live-connectivity E2E probe. The option is now removed (TimeZone /
  DateStyle / IntervalStyle are still pinned — DSQL accepts those, live-verified). It was also
  unnecessary: the checksum's numeric mask already uses a literal `.` (not `to_char`'s
  locale-aware `D`), so source and target agree on the decimal point without it. The
  PostgreSQL *source* connection still pins `lc_numeric=C` (real PostgreSQL supports it).

## v0.1.437

### Fixed

- **Cut over: failure-surfacing and gating fixes for MySQL and PostgreSQL sources** (found by
  a review — the identity/sequence-sync and FK-apply *mechanisms* are correct and
  engine-consistent, verified on live PostgreSQL 16; these fixes stop the cut-over screen
  from reading "ready / done" when it isn't):
  - **A failed identity-sequence sync is no longer painted green "nothing to advance".** A
    connection/token/catalog failure during the "Sync identity sequences" step was swallowed
    to an empty result and shown as success — a false-ready that leads to a post-cut-over
    duplicate-key collision. Such a failure now surfaces as the red "do not cut over yet"
    notice (and an unreadable table is reported, not silently skipped).
  - **The cut-over readiness gate now consumes the record-reconciliation warning.** A default
    row-count run where record reconciliation was requested but ran on no table (all
    non-integer/composite PK) presented a clean "ready to cut over"; it is now a soft-block
    directing you to re-run in CHECKSUM (CHECKSUM mode, which value-compares every row, stays
    clean).
  - **"Apply foreign keys" at cut over now requires confirming CDC has drained.** For a CDC
    migration the button is disabled until you confirm source writes are frozen and the
    stream has drained — applying enforced FKs mid-stream would dead-letter out-of-order sink
    writes (`SQLSTATE 23503`).
  - **"I've cut over to DSQL" is now gated on the deferred foreign keys being applied.** For a
    CDC migration with preserved FKs pending, the final acknowledgement is soft-blocked (with
    a "proceed without enforced foreign keys" opt-out) so a target is not marked live with FK
    enforcement silently missing. A Full-Load cut over also now surfaces/re-offers any FKs
    skipped or failed during the load.
  - Smaller fixes: a stale "identity sync failed" notice no longer persists across a fresh
    validation run; the "Sync identity sequences" button now shows a pending count and hides
    when nothing needs advancing (symmetric with the FK button); a failed automatic
    identity-sync during a validation run is now surfaced on the results panel (not only the
    activity log); and the AI-assistant cut-over facts report the real number of advanced
    sequences (was always "0 tables").

## v0.1.436

### Fixed

- **CDC pipeline fixes for MySQL and PostgreSQL sources** (found by a review of the Debezium
  source configs, the custom Java DSQL sink, and the control-plane; the pipeline core —
  idempotent upsert, per-type conversion, transient/DLQ classification, delete/tombstone,
  gapless handoff — was already sound and engine-consistent):
  - **PostgreSQL CDC start no longer silently skips its topic/offset setup on a
    non-in-VPC host.** A PostgreSQL cdc-stack is always created `SeedMode=External`, but
    Start passed the host's seed-mode config — so on a host not configured External the
    in-process topic/offset prep was skipped and PostgreSQL CDC never streamed. Start now
    derives `SeedMode` from the source engine (PostgreSQL ⇒ External) to match the infra,
    and warns loudly when the host cannot reach MSK.
  - **PostgreSQL re-keyed tables (a `COMPOSITE_KEY` conversion) no longer silently lose
    DELETEs.** When the CDC re-key prepends a non-PK column, PostgreSQL `REPLICA IDENTITY
    DEFAULT` publishes only the source PK in the DELETE before-image, so the sink could not
    build the re-keyed delete and it was dropped. Those specific tables now get `REPLICA
    IDENTITY FULL` at publication creation (tightly allow-listed to exactly that statement),
    with a pre-start heads-up. MySQL is immune (its binlog before-image is the full row).
  - **PostgreSQL UPDATE of an unchanged TOASTed non-string value (e.g. `numeric`) no longer
    overwrites the target with the unavailable-value placeholder.** Debezium's
    `ReselectColumnsPostProcessor` is enabled on the PostgreSQL source connector to re-query
    an unavailable value by primary key before emitting the after-image (a genuine
    `SET col = NULL` is preserved; the sink is unchanged).
  - **The DSQL sink now caps a write transaction by BYTES, not just row count.** A wide /
    JSON-heavy 3000-row chunk could exceed Aurora DSQL's 10 MiB/transaction limit and
    collapse to a slow one-row-per-transaction fallback; the sink now also splits a chunk
    when it would cross ~8 MiB, mirroring Full Load. (Requires a **cdc-stack Delete + Deploy**
    to take effect — MSK Connect custom plugins are immutable; the bundled sink plugin is
    rebuilt and `PLUGIN_VERSION` is bumped to v38.)
  - **The MySQL "Start CDC" button in automatic mode now requires a seedable binlog
    file:position** (not just any coordinates), so a GTID-only watermark steers the user to
    Manual / granting `REPLICATION CLIENT` rather than enabling a start that cannot seed.

## v0.1.435

### Fixed

- **Validation (Step 4) fixes for MySQL and PostgreSQL sources** (found by a review that
  reproduced the checksum/count/reconcile/orphan SQL on live PostgreSQL 16; the checksum
  value-rendering parity and orphan MATCH-SIMPLE semantics were already sound and
  engine-consistent):
  - **A row-count-only run where record reconciliation was requested but applied to NO
    table no longer reads as a clean, record-verified pass.** Reconciliation only runs on a
    single-column integer PK; an all-UUID / all-composite-PK schema (common on PostgreSQL)
    skipped it for every table, yet the report said "reconciliation was turned off" and the
    cut-over verdict stayed green — so in the default row-count mode a
    same-count-but-different-rows divergence was a **silent false match** that could release
    cut over. The run now distinguishes "off" / "ran" / "requested but inapplicable for
    all", and in row-count mode the last case downgrades the verdict to a warning ("row
    counts match, but records are unverified — re-run in CHECKSUM mode"). CHECKSUM mode
    stays green (it value-compares every row) with an honest caveat.
  - **The orphan / FK-integrity pre-gate is now keyset-paged** for a single-column-PK child
    (like COUNT/checksum), so a billion-row child no longer risks the ~300s single-
    transaction limit; the unbounded single scan remains only as the composite/missing-PK
    fallback. The paged count filters the orphan predicate while paging over the full PK
    window, so the boundary advances over non-orphan rows too (verified on live PG16).
  - **The downloadable text report no longer over-claims "Data identical" in row-count
    mode** (the default) — it says "Row counts match" and reserves "Data identical" for
    CHECKSUM mode, matching the UI; it also footnotes tables that ran but could not be
    reconciled.
  - **Checksum numeric rendering is now locale-independent** — the `to_char` mask used a
    locale-sensitive decimal point (`D`), a latent false-mismatch if the DSQL connection's
    `lc_numeric` ever differed from the source; it now uses a literal `.`. The DSQL target
    connection also pins `DateStyle`/`IntervalStyle`/`lc_numeric` (not just `TimeZone`),
    matching the source, so DATE/INTERVAL/NUMERIC checksum text renders identically
    regardless of any role/engine default.

## v0.1.434

### Fixed

- **Full Load data-path fixes for MySQL and PostgreSQL sources** (found by a review that
  ran the keyset reader + batched loader; the core keyset + batched-load layers were
  already sound and engine-consistent — these are residual issues):
  - **PostgreSQL `interval`/`jsonb` PRIMARY KEY: keyset pagination silently SKIPPED or
    DUPLICATED rows.** The keyset `ORDER BY` resolved to the same-name text-cast SELECT
    alias (text order) while the `WHERE` keyset boundary compared the native type — the two
    orderings disagreed, so pages were mis-ordered. `ORDER BY` now table-qualifies each
    primary-key reference to force the native column, matching the boundary (proven on a
    live PostgreSQL 16: the old code returned only 2 of 4 interval-PK rows; the fix returns
    all, exactly once).
  - **PostgreSQL sharded "Drop & reload" (REPLACE) loads created NO secondary indexes.**
    The multiprocess sharded path (PostgreSQL shared-snapshot) recreated the empty target
    but discarded the index DDLs, and the sharded load never ran the post-load index pass —
    so a large PG table reloaded across reader shards silently ended up with no secondary
    indexes. They are now created once, after all of a table's shards succeed, via a bounded
    `create_indexes` pass (identical `CREATE INDEX ASYNC` / retry behavior to the
    non-sharded path).
  - **Batch byte cap under-counted multi-byte text** (CJK / emoji): batch size was estimated
    in code points, not UTF-8 bytes, so a text-heavy batch could exceed Aurora DSQL's
    10 MiB/transaction limit. It is now counted as UTF-8 bytes so the batch flushes below the
    cap.
  - **In-process source-drop retry re-wrote from the start.** The resume watermark was never
    persisted when the source read raised (e.g. an Aurora failover mid-load), so a retry
    re-wrote already-committed batches. The completed-prefix watermark is now persisted even
    on the raising path — the source re-read still fails safe; only the redundant re-writes
    (OCC / round-trips) are spared.

## v0.1.433

### Fixed

- **Schema Conversion: generated-DDL / DEFAULT-value defects for MySQL and PostgreSQL
  sources** (found by a review that ran the converter and validated every emitted DDL with
  sqlglot, Aurora DSQL `dsql_lint`, and a live PostgreSQL 16). Type *spellings* were already
  correct; these DEFAULT / identity / interval issues — most **rejected at apply** or
  **silently lossy**, with no Schema-Conversion warning — are fixed:
  - **PostgreSQL `interval(N)`** was emitted as the unparsable `INTERVAL 6` (DSQL rejects at
    apply). The precision modifier is now stripped for *all* interval spellings, not only
    fields-qualified ones → `INTERVAL`.
  - **MySQL `BIT` column with a `b'…'`/`x'…'`/`0x…` DEFAULT** emitted an integer column with
    a bit-string-literal default DSQL rejects. The literal is now parsed to its integer
    value (`b'101'` → `5`, `0xff` → `255`).
  - **PostgreSQL `serial` / `IDENTITY` primary key** silently lost its auto-generation — the
    primary-key strategy never applied because the PG path never set the identity column.
    `enrich` now flags a `nextval(…)` / `attidentity` PK column, so IDENTITY/UUID/KEEP
    strategies (and the hot-partition recommendation) apply as they do for MySQL.
  - **MySQL zero/integer temporal defaults** (`'0000-00-00'`, `'0000-00-00 00:00:00'`,
    `DEFAULT 0` on a date/timestamp column) emitted DDL DSQL rejects — now dropped with a
    MANUAL warning.
  - **MySQL `BINARY`/`VARBINARY` hex defaults** (`0x…`) emitted a `bytea` default DSQL
    rejects — now emitted as a byte-identical `'\x…'` literal (a bit-string on `bytea` is
    dropped with a warning).
  - **PostgreSQL source dropped ALL column DEFAULTs** while MySQL preserved them (divergent
    DDL + silent loss). `build_pg_source_ddl` now carries the source default (the
    `read="postgres"` re-parse normalizes it to DSQL-valid SQL, e.g. `now()` →
    `CURRENT_TIMESTAMP`), excluding generated columns and the identity `nextval`.
  - **> 24 secondary indexes** were all emitted (the 24th+ `CREATE INDEX ASYNC` then failed
    at apply, error 54000). The excess is now **omitted** and named in the warning (matching
    the > 8-key-column omit policy), so the applied script completes.
  - **A generated `COMPOSITE_KEY` unique-index name over 63 bytes** (silently truncated by
    DSQL) is now included in the identifier-length warning.
  - **MySQL `UTC_DATE()` default** mapped to a session-timezone-dependent expression
    (off-by-one under a non-UTC session) — now instant-based (`(now() AT TIME ZONE 'UTC')::date`).

## v0.1.432

### Fixed

- **Foreign-key referential-action edge cases** (found by a cross-engine review of the FK
  re-creation path — the core `ADD CONSTRAINT ... NOT VALID` path is consistent across
  MySQL and PostgreSQL sources; these are residual edges):
  - **A PostgreSQL 15+ column-list referential action** (`ON DELETE SET NULL (col)` /
    `SET DEFAULT (col)`) was silently downgraded to `NO ACTION` — the parenthesised column
    subset failed the accepted-action check and fell through. The subset (which Aurora DSQL
    / PostgreSQL-16 cannot express) is now stripped and the base action (`SET NULL` /
    `SET DEFAULT`) is emitted instead of being dropped.
  - **`RESTRICT` now renders identically regardless of source engine.** A MySQL source can
    never recover `RESTRICT` (`SHOW CREATE TABLE` omits it, so it already reads as
    `NO ACTION`), while a PostgreSQL source preserved it — so a logically identical foreign
    key differed by source. `RESTRICT` is now normalized to `NO ACTION` for both engines
    (functionally equivalent on the `NOT VALID` DSQL target: immediate vs deferred check).
  - **The cut-over "pending foreign keys" count no longer over-states.** It counted every
    source FK, including FKs on tables that convert to **UNSUPPORTED** (never applied); it
    now counts the same rendered `ADD CONSTRAINT` DDLs the apply pass iterates, so the
    number matches what is actually created.

## v0.1.431

### Fixed

- **MySQL-source introspection edge cases** (the same review, applied to the MySQL path —
  which was already robust; these residual issues are fixed):
  - **A composite index mixing a plain column with an expression key-part** (MySQL 8.0.13+,
    e.g. `UNIQUE KEY (tenant_id, (lower(email)))`) **was silently emitted as a narrower
    index over the plain columns only** — for a UNIQUE index that changes the uniqueness
    semantics (and can fail the async build). SQLAlchemy drops the expression key-part on
    reflection; `enrich_index_types` now compares the reflected column count against the
    true key-part count from `information_schema.STATISTICS` and flags such an index as an
    expression index (warned, and NOT emitted as a wrong narrower constraint).
  - **A table-/database-scoped SELECT grant now raises a non-blocking notice.** The
    privilege check passed on any `SELECT` token, but `SHOW FULL TABLES` / `SHOW SCHEMAS`
    are privilege-filtered, so tables/databases outside a scoped grant were silently
    absent from the inventory. A new non-blocking `SELECT_GRANT_SCOPE` check warns when
    SELECT is scoped (not `*.*`), asking the operator to confirm the user can see every
    object to migrate.
  - **SELECT granted only via a MySQL 8.0 role no longer hard-blocks Full Load** — the
    privilege check downgrades to a non-blocking warning (plain `SHOW GRANTS` cannot expand
    a role's privileges) instead of a false "SELECT missing" failure.
  - **A single view that cannot be `SHOW CREATE`'d no longer aborts the whole
    introspection** — the failing view is skipped (best-effort) so the rest of the
    inventory still assembles.
  - **The 63-byte identifier-length warning now also checks the table and schema name**
    (not just column/index names), since Aurora DSQL / PostgreSQL truncates any identifier
    over 63 bytes (a possible silent collision).

## v0.1.430

### Fixed

- **PostgreSQL introspection no longer silently drops or mis-reads schema objects.** A
  review (prompted by the PostgreSQL 17/18 verification) found a cluster of PG-source
  introspection defects, all previously **silent** — a wrong/incomplete `SourceInventory`
  with no error or warning — now fixed:
  - **User schemas named like `pgapp`/`pgdata` were silently skipped.** `_user_schemas`
    relied on SQLAlchemy's `get_schema_names()`, which filters `nspname NOT LIKE 'pg_%'`
    with an **unescaped** `_` (matches any single char), so any schema starting with `pg`
    + a character was dropped — its tables never introspected or migrated. A new
    `SourceDialect.list_schemas` hook has the PostgreSQL dialect enumerate `pg_namespace`
    with an **escaped** underscore, excluding only real system/temp schemas.
  - **`enrich` ignored the reflected schema → cross-schema column bleed.** The
    `pg_attribute` read was not scoped to the schema being reflected, so in a multi-schema
    source with same-named tables a table could pick up **another schema's** column types /
    generated-column flags (last-wins). Now scoped to the reflected schema.
  - **Triggers, functions and procedures were never collected** (PG `enrich` always
    returned empty) → Evaluation reported "0 triggers / 0 routines" even when the source
    had them. Now read from `pg_trigger` / `pg_proc` so TriggerRule/ProcedureRule flag them.
  - **Declarative partitioned tables were double-migrated** — `get_table_names` returns the
    partitioned parent **and** every partition child as independent tables. The parent is
    now flagged partitioned (PartitionedTableRule fires) and the children are removed (the
    parent's scan already returns their rows), so partition data is not represented twice.
  - **Materialized views and foreign tables were silently omitted** (relkinds `m`/`f`, in
    neither `get_table_names` nor `get_view_names`). They are now surfaced (via
    `ViewDef.unsupported_kind` + a new `UnsupportedRelationRule`) as **UNSUPPORTED** —
    Aurora DSQL has neither — instead of vanishing from the inventory; `convert_view` no
    longer downgrades a materialized view to a plain view.
  - **Partial and non-btree indexes were silently degraded** — a partial UNIQUE index
    became a **FULL** unique index (can reject valid data / fail the async build) and a
    GIN/GiST/BRIN/hash index was emitted as plain btree. `IndexDef` now carries the
    predicate (`where`) and access method (`method`), and Schema Conversion emits a
    MANUAL/LOSS warning rather than silently changing the index.
  - **Cross-schema foreign keys were mis-qualified to the child's schema** when the parent
    was search-path-visible (SQLAlchemy returns `referred_schema=None`). The referenced
    schema is now resolved from `pg_constraint.confrelid` (search-path-independent).
  - **`probe_grants` missed SELECT granted via role membership** → a false "SELECT missing"
    that blocked Full Load. It now checks effective privilege via `pg_has_role`.

## v0.1.429

### Fixed

- **PostgreSQL 18 VIRTUAL generated columns are now detected and flagged (previously
  silent).** `PostgresSourceDialect.enrich` marked a column generated only when
  `pg_attribute.attgenerated == 's'` (STORED). PostgreSQL 18 adds VIRTUAL generated
  columns and makes VIRTUAL the **default** kind for a keyword-less `GENERATED ALWAYS AS
  (expr)`, so on a PG18 source the common generated-column form (`attgenerated = 'v'`) was
  classified as an ordinary column: Schema Conversion emitted **no** MANUAL/LOSS warning,
  and the column was created as ordinary and materialized by Full Load with no signal that
  nothing maintains it afterward (and that CDC does not replicate it). enrich now treats
  both `'s'` and `'v'` as generated, so the existing `_pg_generated_column_warning` fires
  for VIRTUAL columns exactly as for STORED; the warning wording is generalized to cover
  both kinds and now states that CDC does not maintain generated columns (VIRTUAL columns
  cannot be logically replicated at all). Only affects PostgreSQL 18 sources (13–17 have
  only STORED generated columns).

### Documentation

- **Manual: PostgreSQL 17/18 source support.** `00-before-you-begin` now states PG
  **13–18** are supported, and `06-limitations §6.2` documents the three PG17/18-specific
  edges surfaced by a compatibility audit: `interval` `infinity`/`-infinity` (new in PG17)
  is quarantined by Full Load and caught by Validation on the PostgreSQL-16-compatible DSQL
  target; PG18 VIRTUAL generated columns; and the bundled Debezium PostgreSQL connector's
  tested matrix topping out at PG16 (17/18 CDC is best-effort — validate end to end, and
  confirm `idle_replication_slot_timeout=0` on PG18).

## v0.1.428

### Added

- **Full Load now warns before Start/Reload when a selected target table is missing.**
  Previously the pre-Start probe only checked which target tables had rows (for the
  Drop-vs-Append choice) and treated a **missing** table the same as an empty one — so a
  table deleted on DSQL (or never created because Schema Conversion wasn't applied) was
  not surfaced; an append load then failed it per-table, and if the operator started CDC
  the table became a standing gap CDC never backfills. The confirm dialog now runs a
  distinct existence probe (`tables_present`) and, for selected tables that are missing
  **and won't be (re)created by this run** (i.e. append mode, not a recreate candidate),
  shows a warning naming them + the fix: apply Schema Conversion, or choose "Drop &
  reload" to (re)create them from the applied conversion. Non-blocking (a Drop & reload /
  recreate-candidate run still creates them), and skipped when the target probe couldn't
  read the target.

## v0.1.427

### Fixed

- **Foreign keys are no longer applied at Full-Load end for a MySQL Full Load + CDC
  migration — they now defer to cut over, as intended.** The post-load FK pass was gated
  only by `cdc_coexisting` (MySQL connectors-first / SKIP_EXISTING) and `cdc_stack_name`
  (PostgreSQL slot handoff). A MySQL **Full-Load-first → binlog-watermark handoff** (the
  "Automatic" CDC start point, where CDC starts *after* the load) has neither signal set
  at load end, so the pass created and enforced the foreign keys immediately — and once
  CDC began streaming, the out-of-order sink dead-lettered every child row whose parent
  had not yet arrived (`SQLSTATE 23503`, e.g. `order_items`/`inventory` → `products`). A
  new source-agnostic `is_cdc_migration` input (derived from
  `migration_type == FULL_LOAD_AND_CDC`) now forces FK deferral for **every** CDC
  migration, so foreign keys are created only at cut over after the stream drains.

## v0.1.427

### Fixed

- **Full Load Reload / Retry is no longer clickable after a session restore with no live
  connection.** These actions re-read the source and write the target, so they need a
  live, verified connection — specifically the in-memory source password, which is never
  restored (Property 7). The gate keyed on `has_source()`/`has_target()` (config present),
  but since v0.1.414 a restore re-hydrates the source config (so `has_source()` is True)
  while leaving it **unverified with no password** — so the gate stopped firing and the
  per-table **Reload** button was never gated at all. A click then just failed at connect.
  Both the recovery buttons and the per-table Reload now gate on the **verified** state
  (`connection_ready()` — tested live this session), showing a "reconnect first" hint
  until the source and target are re-verified on the Connect step.

## v0.1.426

### Fixed

- **The "CDC infrastructure teardown in progress" banner now clears once the stack is
  actually gone.** The persistent teardown banner keyed only on the delete JOB's status,
  and the "is the stack gone?" self-heal ran only on the *failed* path — so if the delete
  job hung after the stack had already been deleted (a wedged post-stack cleanup thread,
  or a long-lived tab whose self-poll chain broke), the "Deleting… (~15–45 min)" banner
  pinned over a stack that no longer existed. The running banner now also does a throttled
  (~30s) best-effort live check and, on a definitive does-not-exist, completes the
  teardown — replacing the banner with the dismissable "CDC infrastructure deleted" notice
  and clearing the marker.

## v0.1.425

### Changed

- **Lowered the "Start CDC" time estimate to match reality (~5 min, was ~20–26 min).**
  The two dominant start stages (`stack_connectors`, `connectors_running`) were each
  estimated at 13 min — a hangover from the old source-then-sink two-pass. Source and
  sink now deploy in parallel and live runs finish in ~5 min, so each is lowered to ~3
  min, and the "provisioning" notice now says ~5 minutes instead of "10–20 minutes".

## v0.1.424

### Fixed

- **"Start over" no longer breaks CDC on the deployed stack.** The CDC deploy-role ARN
  (from the deployment's `DSQL_MIGRATOR_CDC_DEPLOY_ROLE_ARN` env — the `CdcDeployRole`
  the ECS task assumes for CloudFormation/MSK) is set once at screen-build time, but
  Start over's in-place reset re-ran the state's `__init__` and cleared it, and the
  builder never re-runs — so after a Start over every CDC operation (even the read-only
  state probe) fell back to the bare task role, which lacks `cloudformation:DescribeStacks`,
  and failed with `AccessDenied` (surfaced as "CDC state not determined yet"). The reset
  now preserves the deploy-role ARN across Start over, exactly as it already preserves the
  session binding and an in-flight teardown marker — it is deployment config, not
  per-session state.

## v0.1.423

### Changed

- **Lowered the CDC infrastructure-deploy time estimate in the UI copy too** (deploy
  dialog, infra-inputs panel, "deploying in the background" notice, redeploy note, and
  the start toast) from ~10–15 / ~15–20 min to ~5 min, matching v0.1.422 and the ~2–5 min
  live infra creates. MSK Connect **connector bring-up** (~10–20 min) and MSK **teardown**
  (~15–25 min ENI reclamation, up to ~20 min) estimates are unchanged — those are accurate.

## v0.1.422

### Changed

- **Lowered the CDC infrastructure-deploy time estimate** shown on the CDC pipeline
  progress. The `stack_create` (MSK Serverless) stage hint was 9 min, but live deploys
  now finish the whole infra create in ~2–5 min (CREATE_COMPLETE observed at ~2 min), so
  the per-stage hint over-reported the wait. Lowered to ~5 min; it remains a ballpark, so
  a slower AWS run simply overruns the hint rather than misleads.

## v0.1.421

### Fixed

- **CDC step: a failed CDC-state probe no longer leaves you silently stuck on "CDC
  state not determined yet".** The read-only `describe_stacks` probe that reads the live
  CDC state re-raises on an unexpected AWS error (expired/insufficient credentials,
  missing `cloudformation:DescribeStacks`, wrong region, throttle), but the caller
  swallowed it — so the card stayed "undetermined" with a misleading "session was
  restored" message, no Deploy/Start action, and re-verifying the target just re-ran the
  same failing probe. The probe now records and logs the failure; the notice names the
  real cause (probe **failed** vs. genuinely **not run yet**) and shows the AWS error;
  and a **"Re-check CDC state"** button re-runs the probe in place so recovery no longer
  requires navigating away.

## v0.1.420

### Fixed

- **Schema Conversion: "Apply all … to target" is now disabled until you Generate the
  DDL.** The in-card bulk-apply button was only disabled while an apply was already
  running — never gated on whether "Generate DDL for selected" had been run — so it was
  clickable before any DDL was generated (and after Start over, which clears the
  generated scope). It now stays disabled with a "Generate the DDL first" hint until the
  reviewed DDL exists, and the handler is guarded so it can never apply an un-reviewed
  scope. (The sidebar Run keeps its own ticked-scope behavior.)

## v0.1.419

### Fixed

- **The AI DBA no longer stops at "Let me pull the details…" without answering.** On a
  tool-using turn (e.g. the header's **"What's next?"** briefing) the model sometimes
  ended its turn with only an intent-to-look-it-up preamble and no actual tool call, so
  the tool loop delivered that preamble as the entire reply. The tools instruction now
  forbids ending a turn with only a preamble, and the tool loop detects that punt and
  nudges the model once to actually call the tools and answer — so the briefing returns
  the real, tool-backed answer instead of a dangling "Let me check…". A genuine
  no-tool answer is unaffected (no extra round).

## v0.1.418

### Fixed

- **PostgreSQL-source correctness sweep — 14 places that still behaved as, or read
  as, MySQL when the source was PostgreSQL are fixed.** A repo-wide audit found the
  data path was already engine-dispatched, but a tail of UI text, guidance, and a few
  behaviors were still MySQL-only. Notably:
  - **Two functional bugs:** (1) a PostgreSQL column **DEFAULT** was silently dropped
    from the converted DDL with no warning — now every dropped default raises a MANUAL
    conversion warning (a serial/identity `nextval` is skipped, as MySQL
    `AUTO_INCREMENT` already is); (2) **PostgreSQL stand-alone CDC** (Manual re-snapshot)
    could not be started — the Start CDC button stayed disabled though the start-point
    card showed "Ready"; the button now mirrors the card's PG gate.
  - **Schema Conversion source panels** now render a PostgreSQL source in PG syntax
    (double-quoted identifiers, exact PG types, `CREATE INDEX`, no `AUTO_INCREMENT`),
    and a PG **view** is parsed/pretty-printed in the PostgreSQL dialect instead of being
    mangled through MySQL; the CDC **schema-drift "Fix target schema… (ADD COLUMN)"**
    recovery now reads PG columns and maps PG types instead of erroring.
  - **Full Load watermark panel** shows the captured **WAL LSN** / replication slot /
    publication for a PG source instead of rendering the binlog/GTID fields as
    "unavailable"; the combined prerequisite panel no longer mis-labels PG CDC-only
    checks (wal_level, replica identity, …) as "Full Load: Blocked".
  - **Evaluation:** a multi-**schema** PostgreSQL source is no longer flagged as
    "spans N databases" with MySQL consolidation advice (DSQL supports schemas and the
    tool migrates them automatically); the exported HTML report is titled by the real
    source engine; and the shared index/key-limit rules no longer cite MySQL's 64/16
    limits or `sys.schema_unused_indexes`.
  - **Text/guidance:** the oversized-LOB exclusion now detects PostgreSQL `text`/`bytea`
    (not just MySQL LOB types); the validation drift verdict, CDC/param/sink previews,
    Connect port default on a restored PG session, and various tooltips/blurbs are now
    engine-aware or engine-neutral. (The AI-DBA prompts were already source-aware as of
    v0.1.415.)

## v0.1.417

### Changed

- **Settings: the "Diagnostics" and "Activity log" tabs are merged into one
  "Activity log" tab, and "Mirror to stdout" is renamed "Mirror to CloudWatch Logs".**
  The log level, the CloudWatch mirror and the download are all the same subject — the
  one activity/audit log (how much is recorded, where it is streamed, how to pull it) —
  so they now live on a single tab instead of being split across two. The mirror toggle
  is named by its destination ("CloudWatch Logs") rather than the opaque "stdout"
  mechanism; its description keeps the detail (streams to the container's stdout, which
  the ECS awslogs driver forwards to CloudWatch — a durable copy that survives task
  replacement). Manual §7.2 updated to match.

## v0.1.416

### Added

- **"Start over" now writes a `session reset` entry to the activity log.** The reset
  wipes the session workbench, the saved snapshot and the AI panel transcript, and can
  trigger a CDC teardown (logged separately) — but the reset itself previously left no
  trace. The durable, process-wide activity/audit log (which is intentionally **not**
  cleared by a reset, so it survives to record that the reset happened) now shows a
  `[system] session reset (success)` line with the time it occurred.

## v0.1.415

### Fixed

- **The AI DBA now knows the migration's source engine (MySQL vs PostgreSQL) and
  answers in the right dialect.** Every AI-DBA grounding prompt previously hard-coded
  "MySQL", so a PostgreSQL-source migration was mis-grounded — the assistant would talk
  about MySQL binlog, `AUTO_INCREMENT`, and MySQL syntax even when the source was
  PostgreSQL. All prompts (object/schema/query chats, the validation, Full Load, CDC,
  cut over, and connection-error assistants, and the batch assessment) are now
  parameterized on the real source engine, taken from the session's source type. The
  CDC/validation/cut-over recovery guidance also picks the source-specific detail
  (PostgreSQL logical replication vs MySQL binlog; `SERIAL`/`IDENTITY` sequences vs
  `AUTO_INCREMENT`; rollback direction). The target is always described as Amazon
  Aurora DSQL (PostgreSQL-compatible), so converted/reimplemented SQL is still written
  in DSQL's PostgreSQL dialect.

## v0.1.414

### Changed

- **Published `0.1.414` to all three registries and repointed the `ContainerImageUri`
  default** (`0.1.413 → 0.1.414`); both deployed app stacks (us-east-1
  `mysql-dsql-migrator` + Seoul `mysql-dsql-migrator-seoul`) updated to `0.1.414`.
- **The source connection endpoint now survives a reconnect / instance restart.** On
  restore, the Connect form pre-fills the source **host / port / database / username**
  (previously only the source engine kind was restored, so those fields came back
  blank), mirroring how the target connection already restores. Only the **password**
  is re-entered — it is never persisted (Property 7), and the source stays
  **unverified** until you re-test, so the nav gate still requires a live re-check.
  `SourceConnectionConfig` is by design safe to serialize (the password lives only in
  the in-memory `SecretValue`), so persisting these non-secret coordinates matches the
  target's behavior. Older snapshots (no source coordinates) fall back to the prior
  engine-hint-only restore.

## v0.1.413

### Changed

- **Published `0.1.413` to all three registries and repointed the `ContainerImageUri`
  default** (`0.1.412 → 0.1.413`); both deployed app stacks (us-east-1
  `mysql-dsql-migrator` and Seoul `mysql-dsql-migrator-seoul`) updated to `0.1.413`.
- **The Evaluation AI DBA can now read the ACTUAL source body of an unsupported stored
  procedure / function / trigger / event on demand** and propose a concrete
  application-side reimplementation, instead of name-only guidance. It fetches the
  definition through the in-memory, read-only source connection — MySQL
  `SHOW CREATE …`, PostgreSQL `pg_get_functiondef` / `pg_get_triggerdef` — size-capped,
  injection-safe (MySQL identifier allow-list; PostgreSQL bound parameters), and falls
  back to name/kind when the source isn't connected in this session.
- **Made the AI DBA tools PostgreSQL-source-aware:** `get_full_load_status` now surfaces
  the PostgreSQL WAL-LSN watermark alongside the MySQL binlog/GTID fields, and
  engine-specific tool descriptions are neutral so the model doesn't assume a MySQL
  source for a PostgreSQL migration.

### Fixed

- **"What's next?" re-runs the briefing after an intervening question.** A composer
  follow-up keeps the panel on the "readiness" scope, so the scope-id dedupe suppressed
  the re-ask; it now re-runs on every click except a true back-to-back (nothing asked
  since the last briefing).

## v0.1.412

### Changed

- **On Connect, the Bedrock model/region settings and the "Verify AI access" button
  are now disabled until "Enable AI Assist" is switched on.** With AI Assist off the
  app never calls Bedrock, so configuring a model or running the access preflight had
  no effect; the switch is now the single gate and the Bedrock section reads as
  contingent on it (the controls re-enable live when you flip the switch).
- **Refreshed the AI Assist description on Connect** to match the current assistant:
  a Bedrock-backed AI DBA that spans the whole migration — per-object guidance and
  conversion suggestions for MANUAL/UNSUPPORTED objects, query-lint and validation
  help, and a cut-over repoint recipe with a GO/HOLD verdict — plus an always-available
  AI chat (previously described only "per-object guidance, conversion suggestions, and
  the AI chat").
- **The AI Assist section badge turns green "Verified" on a clean "Verify AI access"**
  and reverts to "Optional" on any edit (model / region / toggle), mirroring Connect's
  per-connection Verified badge.
- **The Connect "Model ID" is now a dropdown of the supported Anthropic models**
  (`SUPPORTED_BEDROCK_MODELS`, seeded from `BedrockModelId`) instead of a free-text
  field, removing the "typo / wrong id → `MODEL_NOT_ENABLED`" trap. A deploy-configured
  model outside the curated set is preserved as an extra option, and a test keeps the
  list in sync with the CFN `BedrockModelId` AllowedValues.
- **Published the `0.1.412` image to all three registries and repointed the
  `ContainerImageUri` default** (`0.1.411 → 0.1.412`). Both deployed app stacks were
  updated to `0.1.412`: us-east-1 (`mysql-dsql-migrator`) and Seoul
  (`mysql-dsql-migrator-seoul`).

### Fixed

- **Start over now resets the AI DBA chat.** The persistent AI panel captured the
  session's conversation object by reference, but Start over replaced it with a new
  object — so the panel kept showing (and appending to) the old transcript. Start over
  now resets the conversation IN PLACE and wipes the panel's rendered transcript, so a
  fresh journey opens on an empty AI DBA chat.

## v0.1.411

### Changed

- **Published the `0.1.411` image to all three registries and repointed the
  `ContainerImageUri` default** in `deploy/cloudformation.yaml` (`0.1.409 → 0.1.411`),
  so a fresh `git clone` deploy pulls the image with the FK-note Alert box (v0.1.410)
  and the advisory-only Evaluation classification fix (v0.1.411). The deployed Seoul
  stack (`mysql-dsql-migrator-seoul`, ap-northeast-2) was updated to the `0.1.411` image.

### Fixed

- **Evaluation no longer over-counts "Review needed" tables in the report / chart /
  summary.** A table whose ONLY findings were advisory (a preserved foreign key or an
  AUTO_INCREMENT throughput note — `RECOMMENDATION`, which nonetheless carried a
  `MANUAL` classification) was rolled up as `MANUAL` by the assessor, so it read as
  **Review needed** in everything derived from the item classification (the exported
  HTML/JSON/text report, the chart, the difficulty summary, the migration score) while
  the on-screen object list correctly showed it as **Recommended** — e.g. 7 tables
  marked Review needed in the report vs 5 in the list. An object whose findings are all
  advisory is now classified **AUTO** (mirroring the effort rule, which already excluded
  advisory findings), so every surface agrees; the advice is still surfaced as a
  `RECOMMENDED` concern (nothing is silently marked compatible).

## v0.1.410

### Changed

- **The foreign-key deferral note at the "Preserve foreign keys" toggle is now an
  Alert box (`render_notice`, info tone) instead of loose gray caption text,** matching
  the design system (a status/guidance message is a bordered notice with a leading icon
  and bold header, never plain colored text). Same wording — foreign keys are created
  after Full Load (at cut over for CDC), not at Schema Apply, by design.

## v0.1.409

### Changed

- **Published the `0.1.409` image to all three registries and repointed the
  `ContainerImageUri` default** in `deploy/cloudformation.yaml`
  (`0.1.406 → 0.1.409`), so a fresh `git clone` deploy pulls the image with the
  Bedrock provider-scope (v0.1.407), foreign-key legibility (v0.1.408), and
  preserve-toggle persistence (v0.1.409) changes. The deployed Seoul stack
  (`mysql-dsql-migrator-seoul`, ap-northeast-2) was updated to the `0.1.409` image.

### Fixed

- **The "Preserve foreign keys" choice now survives a reconnect / instance restart.**
  It was not persisted in the session snapshot, so a restored session silently
  reverted to preserve-by-default — meaning a user who chose to STRIP foreign keys
  could have them re-created by the post-load / cut-over FK pass after a crash or
  Fargate task replacement. The toggle is now captured and restored with the rest of
  the (non-secret) workbench state (older snapshots restore as preserve = True,
  matching prior behavior). The deferred foreign keys themselves were already
  crash-safe: the FK `ADD CONSTRAINT` DDL is re-derived from the persisted source
  inventory and applied idempotently, so a crash/restart resumes and the FK pass
  re-runs. (Note: source credentials are never persisted (Property 7), so if the
  session snapshot is lost entirely the schema must be restored by reconnecting and
  re-running Evaluation — it cannot be re-read automatically.)

## v0.1.408

### Changed

- **Made deferred foreign keys legible in Schema Conversion (no behavior change to
  the deferred-apply design).** The generated target DDL used to intermix the
  post-load `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY … NOT VALID` lines with the
  editable `CREATE` statements, so a user would apply and then find zero foreign keys
  on the target ("why is the FK missing?"). Now:
  - the editable target-DDL box (and its diff) shows only the `CREATE
    SCHEMA/TABLE/INDEX` statements, and the foreign keys appear in a distinct,
    read-only **"Foreign keys — applied after Full Load, not at Schema Apply"**
    section (the stored edited DDL is kept whole by recombining on edit, so the
    post-load FK pass still receives them);
  - a caption at the **"Preserve foreign keys"** toggle and an info notice in the
    apply results state plainly that foreign keys are (re)created after Full Load (at
    cut over for a CDC migration), not by Schema Apply;
  - a **Full-Load-only** run now records a named `apply foreign keys` step in the
    activity log (`N applied, S skipped, F failed`), mirroring the CDC cut-over
    "Apply foreign keys" action, so the foreign keys are visibly (re)created at load
    end instead of appearing to have gone missing.

## v0.1.407

### Changed

- **The Bedrock `InvokeModel` IAM scope is now provider-wide (any Anthropic model)
  instead of the single deploy-time `BedrockModelId`,** so an operator can switch the
  Connect form's Model ID to another Anthropic model **without redeploying** to widen
  the policy. When `BedrockModelArns` is not overridden, the task role's
  `bedrock:InvokeModel`/`InvokeModelWithResponseStream` is scoped to
  `inference-profile/*.anthropic.*` (this account, any region) plus
  `foundation-model/anthropic.*` (any region). The provider stays pinned to
  `anthropic.` — non-Anthropic / image / Marketplace models are never invokable and
  it is never a blanket `*` — because the app only speaks the Anthropic Messages body.
  `BedrockModelId` is now just the default the UI opens with (still access-gated by
  your account), and `BedrockModelArns` still lets an operator narrow the scope or
  allow a non-Anthropic model. **Note:** this does not bypass account-level Bedrock
  model access — each model must still be enabled in your account/region (otherwise
  the call fails `MODEL_NOT_ENABLED`, not `AccessDenied`).

## v0.1.406

### Changed

- **Published the `0.1.406` image to all three registries and repointed the
  `ContainerImageUri` default** in `deploy/cloudformation.yaml` to
  `public.ecr.aws/z0q0i9j0/mysql-dsql-migrator:0.1.406`, so a fresh `git clone`
  deploy pulls the image that includes the Schema-Apply foreign-key fix below.
  The deployed Seoul stack (`mysql-dsql-migrator-seoul`, ap-northeast-2) was
  updated to the `0.1.406` image.

### Fixed

- **Schema Apply no longer rejects an edited table that has a preserved foreign key.**
  Editing a table's target DDL in Schema Conversion (e.g. changing its primary key)
  and clicking Apply failed only for the edited tables with
  `SchemaApplyError: target DDL must be a CREATE TABLE/VIEW/MATERIALIZED VIEW/INDEX
  statement`. The edited-DDL path (`override_apply_objects`) split the edited script
  and applied every statement — including the post-load `ALTER TABLE … ADD CONSTRAINT
  … FOREIGN KEY … NOT VALID` line the preview renders — which the applier's
  CREATE-only parser rejects. It now excludes that FK `ALTER` from the Schema Apply
  units, exactly as the non-edited path (`build_apply_objects`) omits
  `foreign_key_ddls`; the foreign key is still (re)created by the post-load FK pass
  (driven by `applied_table_conversions`), and removing the FK line from the edit
  still drops it. A regression introduced with foreign-key preservation (v0.1.400).

## v0.1.405

### Changed

- **Republished the container image at `0.1.404` to all registries and repointed the
  `ContainerImageUri` default** in `deploy/cloudformation.yaml` to
  `public.ecr.aws/z0q0i9j0/mysql-dsql-migrator:0.1.404`, so a fresh deploy pulls the
  image that includes the PostgreSQL Full Load + CDC + foreign-key fixes (v0.1.401–404).
  Image published to ECR Public and the us-east-1 / ap-northeast-2 private ECRs.

## v0.1.404

### Fixed

- **Start CDC now enables for a PostgreSQL Full Load watermark.** The Start CDC readiness
  gate keyed only on `CdcResumePoint.has_coordinates()` (a MySQL GTID / binlog
  `file:position`). A PostgreSQL Full Load records a **WAL LSN** (and no binlog/GTID), so
  the gate stayed false and the **Start CDC** button was disabled with "Set the CDC start
  point above first" — even though the start-point card showed the LSN as set (it keys on
  the PostgreSQL `can_resume_from_lsn()`). The gate now accepts either coordinate type, so
  a gapless PostgreSQL Full Load → CDC handoff can start. MySQL is unchanged.

## v0.1.403

### Fixed

- **The Data Migration → CDC step no longer crashes for a PostgreSQL source.** After a
  PostgreSQL Full Load, rendering the CDC source-config card accessed `config.start_gtid`
  (and `start_binlog_file`/`start_binlog_pos`) — MySQL GTID/binlog fields that a
  `PostgresSourceConfig` does not have — raising `AttributeError` and replacing the step
  with an error card ("An unexpected error occurred while rendering this step"). Those
  MySQL-only fields are now read defensively (via `getattr`), and the PostgreSQL start
  point (logical-replication `slot.name` / `publication.name`) is shown instead. MySQL is
  unchanged.

## v0.1.402

### Fixed

- **A PostgreSQL Full Load + CDC migration can now deploy the CDC infrastructure from
  the UI.** The "Deploy CDC infrastructure" / Start CDC gate required the MySQL
  `BINLOG_ROW_FORMAT` prerequisite to have passed — the source change-stream check — but
  a PostgreSQL prerequisite report carries no binlog check (its equivalent is
  `WAL_LEVEL_LOGICAL`, i.e. `wal_level=logical`), so the gate always fired and left the
  **Deploy CDC infrastructure** button disabled for every PostgreSQL source. The gate now
  keys on whichever engine's change-stream check the report carries (`BINLOG_ROW_FORMAT`
  for MySQL, `WAL_LEVEL_LOGICAL` for PostgreSQL), with an engine-neutral message. MySQL
  behavior is unchanged.

## v0.1.401

### Fixed

- **PostgreSQL Full Load + CDC no longer applies foreign keys at the end of the initial
  Full Load** (which would make the subsequent CDC stream dead-letter FK violations,
  SQLSTATE 23503). The post-load FK apply is deferred to cut over for **every** CDC
  migration, but the deferral was gated only on `cdc_coexisting` — MySQL's
  CDC-live-during-load signal, which is `True` mid-load. PostgreSQL uses a **Full-Load-first**
  gapless handoff (CDC starts *after* the load), so `cdc_coexisting` is `False` during the
  initial PG load and the foreign keys were applied too early. The deferral now also
  triggers on `cdc_stack_name` (set only for a PostgreSQL Full Load + CDC run), so PG CDC
  foreign keys wait for the cut-over apply like MySQL's. MySQL behavior is unchanged, and a
  PostgreSQL **Full-Load-only** migration still applies foreign keys at end of load.

## v0.1.400

### Added

- **Aurora DSQL now supports enforced foreign keys** (announced 2026-08-27), so the tool
  **preserves foreign keys** instead of stripping them. Schema Conversion renders each
  source FK as a post-load `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY` statement
  (`TableConversion.foreign_key_ddls`) — kept OUT of `CREATE TABLE` so the concurrent,
  cross-table bulk load stays order-independent — with a strip option
  (`SchemaConvertOptions.preserve_foreign_keys=False`) for the app-enforced-integrity case.
- **Full Load applies preserved foreign keys as a run-level post-pass** after the data
  load, each `ADD CONSTRAINT … NOT VALID` as its own single-DDL autocommit transaction
  with OCC (40001) retry and reconnect (new `schema_applier.apply_foreign_key`,
  duplicate-tolerant). DSQL only accepts an `ADD CONSTRAINT` that is `NOT VALID` (it
  enforces every new write immediately without scanning existing rows); the loaded rows
  are then confirmed via the async `ALTER TABLE ASYNC … VALIDATE CONSTRAINT`
  (`schema_applier.validate_foreign_key`).
  An **orphan pre-gate** (reusing Validation's orphan query) skips — with an actionable
  per-FK activity-log entry — any foreign key whose child rows reference a missing parent,
  instead of letting the `ALTER` fail opaquely; the pre-gate count is itself OCC-retry-safe,
  so a transient serialization failure (OC001/40001) from a live CDC sink at cut over is
  not mistaken for a failed pre-check, and the shared pre-gate probe reconnects on a
  mid-pass connection drop rather than failing every remaining foreign key. Each rendered
  FK DDL is paired to its metadata **by constraint name**, so editing the Schema-Conversion
  target script to drop or reorder a foreign-key line keeps the pre-gate and the
  `VALIDATE` targeting the constraint actually being added. For a **CDC migration** the apply is
  **deferred to cut over** (foreign keys must not exist while the sink streams
  out-of-order rows — it dead-letters an FK violation, SQLSTATE 23503).
- **Schema Conversion has a "Preserve foreign keys" toggle** (on by default) that strips
  the foreign keys from the conversion and Full Load when turned off; the **target preview
  now shows the foreign-key DDL**, and the edited-target-script re-parser preserves an
  edited `ADD CONSTRAINT … FOREIGN KEY`.
- **The Cut over runbook adds an "Apply foreign keys" action** for a CDC migration: after
  the final drain (and before repointing) it re-creates the deferred foreign keys on
  Aurora DSQL, orphan-gated and idempotent, and reports how many were applied / skipped
  (orphan rows) / failed.

### Changed

- The Evaluation **foreign-key finding is now advisory** (RECOMMENDED, no required effort)
  rather than a MANUAL "not supported — remove it" gap; it carries the runtime caveats
  (extra reads on referenced/referencing DML, retryable 40001 conflicts, and `CASCADE` /
  `SET NULL` / `SET DEFAULT` counting toward the 3000-row transaction limit). Rule id
  `FK_UNSUPPORTED` → `FK_PRESERVED`.
- The cascade-over-CDC finding + banner keeps its (still-real) warning but is reworded: an
  un-replicated source cascade now surfaces as a **blocking orphan at the cut-over FK
  apply** (caught by the Validation orphan check), not silent divergence.
- Corrected the now-false "Aurora DSQL has no foreign keys / enforce in the application
  layer" guidance across the converter, assessor, validator, and the AI grounding
  constants; the query linter no longer flags `FOREIGN KEY` as an anti-pattern
  (`FOREIGN_KEY_DEPENDENCY` is retained for back-compat but no longer emitted); and
  `CLAUDE.md`'s Aurora DSQL domain-constraints section was updated.

## v0.1.399

### Changed

- **Republished the ECR Public image at `0.1.398` and repointed the `ContainerImageUri`
  default to it**, so a fresh-clone default deploy runs the engine-neutral UI (the
  `0.1.398` header rename + source-aware PostgreSQL wording). The Seoul demo stack was
  updated to `0.1.398` as well.

## v0.1.398

### Changed

- **Engine-neutral UI for a PostgreSQL source.** The in-app header is now **Aurora DSQL
  Migration Tool** (was "MySQL to Aurora DSQL Migration Tool"), and the remaining
  MySQL-only UI copy is now source-aware so a PostgreSQL source no longer sees MySQL
  wording: the CDC migration-type requirements and least-privilege setup (PostgreSQL
  logical replication — `CREATE ROLE … REPLICATION` + a `pgoutput` publication — vs
  MySQL binlog ROW mode + `REPLICATION` grants), the CDC connector / "types converted"
  labels, the **Cut over** runbook, and the **Schema Conversion** source pane (its label
  and syntax highlighting follow the source engine). The source **Port** field already
  defaulted per engine (3306 / 5432). (The Query Converter stays MySQL-only for now — its
  conversion engine is MySQL-specific.)

## v0.1.397

### Changed

- **Republished the ECR Public image at `0.1.396` and repointed the
  `ContainerImageUri` default to it**, so a fresh-clone default deploy runs a
  PostgreSQL-capable image (the default previously lagged at `0.1.393`, before PostgreSQL
  source support).
- **README (en/ko/ja) now documents PostgreSQL as a supported source throughout** —
  intro, workflow, prerequisites (RDS/Aurora PostgreSQL 13–16), quick start, and the
  AWS-services / CDC notes — and the "simple architecture" diagram shows a MySQL /
  PostgreSQL source with a `binlog / WAL (CDC)` path.

## v0.1.396

### Added

- **PostgreSQL is now a supported migration source (RDS / Aurora PostgreSQL → Aurora
  DSQL).** The Connect screen has a source-engine selector (MySQL / PostgreSQL), and a
  PostgreSQL source runs the full journey — Evaluation, Schema Conversion, Full Load,
  Validation, and CDC — through a source-dialect adapter (`core/source_dialect`).
  PostgreSQL → DSQL is near-identity (both PostgreSQL-16 wire): Schema Conversion passes
  types through and flags the DSQL-unsupported ones (arrays, geometric / network / xml /
  money / bit / range / tsvector / enum / composite / domains) with a faithful remodel
  target; Full Load streams by keyset (sharded REPLACE reads use a shared exported
  snapshot); Validation reuses one PostgreSQL-16 checksum renderer on both ends. Verified
  live end-to-end on real Aurora PostgreSQL (Full Load + Validation, CHECKSUM byte-identical).
- **PostgreSQL CDC (Debezium `pgoutput` → MSK → DSQL sink).** Logical-replication slot +
  publication lifecycle, WAL-retention health surfacing, gapless Full-Load → CDC handoff,
  and typed sink binds (uuid / timetz-UTC / interval / timestamptz / jsonb / bytea /
  numeric) with TOAST partial-upsert. A dedicated Debezium-PostgreSQL connector plugin
  ships alongside the MySQL one.

### Fixed

- **CHECKSUM validation no longer false-mismatches `BIGINT UNSIGNED` on modern MySQL
  sources.** The DSQL-target checksum applied PostgreSQL's unconstrained-`numeric` scale-6
  rule to any parenthesis-less type, but MySQL 8.0.19+ / 8.4 / Aurora MySQL 3 report
  `BIGINT UNSIGNED` as the paren-less `bigint unsigned` (stored as `numeric(20,0)`, an
  integer). The target side gained `.000000` while the unchanged MySQL source side rendered
  scale 0, so every such row diverged despite byte-identical data. The scale-6 rule is now
  gated to a genuine PostgreSQL source. (MySQL 5.7's `bigint(20) unsigned` was unaffected.)
- **A bare PostgreSQL `numeric`/`decimal` (no declared precision) now raises a Schema
  Conversion warning.** Aurora DSQL stores such a column at its default `numeric(18,6)` and
  rounds values beyond 6 fractional digits; this was previously silent (only a *declared*
  precision above 38/37 was flagged) and Validation compares at scale 6, so the loss could
  pass unnoticed. Conversion now names the column and the `numeric(18,6)` cap.

## v0.1.395

### Fixed

- **Checksum validation now supports wide tables (> ~100 columns).** The per-row
  checksum token was `MD5(CONCAT_WS('|', <every rendered column>))`; on Aurora DSQL /
  PostgreSQL a function call is capped at **100 arguments** (`FUNC_MAX_ARGS`), so a
  table with ≥ 100 checksummed columns failed CHECKSUM validation on the **target**
  with *"cannot pass more than 100 arguments to a function"* — even though tables up
  to 255 columns are otherwise supported. The token now **nests MD5s**: it hashes
  each group of ≤ 96 rendered columns, then hashes the group hashes, applied
  **identically on both engines** so equal data still hashes equally. Tables within
  the limit emit the original flat SQL unchanged (no re-checksum of narrow tables).
  Verified against a live **Aurora DSQL** cluster: a flat 200-argument `concat_ws`
  errors, while the nested form succeeds.

## v0.1.394

### Changed

- **Republished the app image as `0.1.393` and repointed the default to it.** The
  `0.1.393` image (MySQL 8.4 + 5.7 source-compatibility fixes) was pushed to ECR Public
  (`z0q0i9j0`) and both private ECRs (us-east-1, ap-northeast-2), and
  `cloudformation.yaml`'s `ContainerImageUri` default was repointed `0.1.390 → 0.1.393` so
  a fresh `git clone` deploy pulls the 8.4/5.7-ready build.

## v0.1.393

### Fixed

- **MySQL 5.7 source: temporal function defaults were mis-rendered as string literals.**
  The converter tells an *expression* default from a *literal* using MySQL's
  `DEFAULT_GENERATED` `EXTRA` flag — which only exists on **MySQL 8.0.13+**. On a **5.7**
  source the flag is absent, so a temporal function default like `CURRENT_TIMESTAMP` /
  `CURRENT_TIMESTAMP(6)` was treated as a literal and quoted (`DEFAULT
  'CURRENT_TIMESTAMP(6)'`), making the target `CREATE` fail with *"invalid input syntax
  for type timestamp"*. Introspection now also recognizes temporal function defaults
  (`CURRENT_TIMESTAMP[(n)]`, `NOW()`, `LOCALTIME[STAMP]`, `UTC_TIMESTAMP`) on
  `datetime`/`timestamp` columns as expression defaults when the flag is absent —
  **scoped to temporal columns** so a genuine string literal is never misclassified, and
  8.0+ keeps using the authoritative flag. Verified end-to-end against a live **MySQL
  5.7.44** source (Full Load of `DATETIME(6)`/`TIMESTAMP` columns now creates + loads
  correctly; the binlog watermark is captured via the `SHOW MASTER STATUS` fallback added
  in `0.1.392`).

## v0.1.392

### Fixed

- **MySQL 8.4 source: binlog watermark capture (gapless Full Load → CDC handoff).**
  `SHOW MASTER STATUS` was **removed in MySQL 8.4**, so on an Aurora/MySQL 8.4 source the
  watermark captured an empty binlog `file:position` — silently degrading the gapless
  Full Load → CDC handoff (CDC could not resume from the watermark and fell back toward a
  from-now start, risking a data gap). The watermark capture, the CDC **"Fetch current
  position"** action, and the validator's binlog-drift read now issue **`SHOW BINARY LOG
  STATUS`** (MySQL 8.2+, including 8.4) and **fall back to `SHOW MASTER STATUS`** on
  ≤ 8.0.x servers that don't recognize the newer form — one code path covering 8.0.x
  through 8.4+ with no version probe. Verified end-to-end against a live **Aurora MySQL
  8.4.7** cluster (Full Load + watermark now returns real coordinates).

## v0.1.391

### Changed

- **Republished the `0.1.390` app image and repointed the default to it.** The `0.1.390`
  image (CDC applied-ops windowing fix + the `0.1.388/389` AI-reliability fixes) was
  pushed to ECR Public (`z0q0i9j0`) and both private ECRs (us-east-1, ap-northeast-2),
  and `cloudformation.yaml`'s `ContainerImageUri` default was bumped `0.1.387 → 0.1.390`
  so a fresh git-clone deploy pulls it.

## v0.1.390

### Fixed

- **CDC per-table Inserts/Updates/Deletes now count only events applied AFTER the Full
  Load watermark, not prior CDC runs.** The "Changes since Full Load" column summed the
  sink's `InsertsApplied`/`UpdatesApplied`/`DeletesApplied` CloudWatch metrics over a
  fixed 14-day trailing window, so a clean Full-Load→CDC run with no post-watermark
  writes still showed non-zero counts — those were leftover ops from earlier demo/test
  CDC runs on the same stack still inside the window. CDC start now pins the applied-ops
  window to the Full Load watermark (`cdc_ops_window_start`, persisted across reconnect;
  falls back to CDC-start time for CDC-only/manual), and the poll windows the metric read
  from it — so a fresh gapless run correctly reads 0 until real change traffic flows.

## v0.1.389

### Fixed

- **AI DBA no longer fails with "AI reply unavailable / could not be parsed" on
  reasoning-heavy turns (e.g. the header "What's next?" briefing, Query Converter
  "Tune").** The Claude 5 models do extended thinking by default and thinking tokens
  count against `max_tokens`, so a multi-tool / heavy turn could spend the whole budget
  on thinking and stop with no answer text. Raising the cap (v0.1.382) only reduced the
  frequency. Since these replies are grounded (tool results + system context carry the
  facts) and kept concise, extended thinking is now **disabled** for every AI-DBA
  generation (`thinking: {type: disabled}` on the chat, tool-chat, and guidance bodies),
  so the full budget goes to the answer and the empty-thinking failure can't occur.

## v0.1.388

### Changed

- **Republished the `0.1.387` app image and repointed the default to it.** The
  `0.1.387` image (LOB-panel scoping fix + the `0.1.385/386` prereq work) was pushed
  to ECR Public (`z0q0i9j0`) and both private ECRs (us-east-1, ap-northeast-2), and
  `cloudformation.yaml`'s `ContainerImageUri` default was bumped `0.1.384 → 0.1.387`
  so a fresh git-clone deploy pulls it. (Updating an already-deployed stack to the
  new image is a separate, explicitly-confirmed step.)

## v0.1.387

### Fixed

- **The "Oversized LOB columns" panel now lists only the SELECTED schema's/tables'
  columns, not every schema's.** Picking one schema in Schema Conversion only
  pre-ticks the Data Migration picker; `migration_state.selection` stays the empty
  "= all" default until the picker is touched, so the panel — which filtered on that
  raw selection — fell through to listing LOB columns from all schemas. The panel now
  filters on the resolved **effective selection** (the same set the load uses), via a
  new pure `scope_lob_candidates` helper. When an effective selection is supplied it
  filters strictly (empty = nothing to migrate = no candidates); the legacy raw-
  selection fallback is kept for callers that can't resolve it (CDC-only panel).

## v0.1.386

### Added

- **CDC prerequisites now check the source's binary-log retention** — a new
  `BINLOG_RETENTION` check (CDC mode only; SKIP for Full Load). CDC resumes from the
  binlog file:position / GTID watermark captured at the Full Load snapshot, so if the
  source's binlog is purged before CDC starts the handoff has a silent data gap. The
  check reads the RDS `binlog retention hours` (from `mysql.rds_configuration`; an unset
  value = aggressive purge) with a self-managed fallback (`binlog_expire_logs_seconds` /
  `expire_logs_days`, where `0` = purging disabled = safe), and WARNs (non-blocking, per
  Property 14) with remediation (`CALL mysql.rds_set_configuration('binlog retention
  hours', 168)`) when retention is under 24h; unknown → advisory INFO. This closes the
  classic RDS CDC gotcha the gate previously missed. Full Load's source checks are
  unchanged (reachability + SELECT grant + per-table PK) — nothing to add there.

## v0.1.385

### Changed

- **Republished the app image at `0.1.384` and repointed the default to it.** The
  `0.1.384` image (AI DBA diagnostics tools + token-budget fix + Query Converter schema
  picker) was pushed to ECR Public (`z0q0i9j0`) and both private ECRs (us-east-1,
  ap-northeast-2), and `cloudformation.yaml`'s `ContainerImageUri` default was bumped
  `0.1.382 → 0.1.384`, so a fresh git-clone deploy pulls the fixed image instead of the
  stale one. (Updating an already-deployed stack to the new image is a separate,
  explicitly-confirmed step.)

## v0.1.384

### Added

- **AI DBA can now diagnose Full Load / CDC failures, not just report status.** Three new
  read-only tools (in `ui/ai_tools.py`): `get_cdc_pipeline_diagnostics` answers "why is CDC
  not streaming / why did the deploy fail" from cached state (CFN stack phase, per-connector
  RUNNING/FAILED, confirmed sink stall, the failed deploy stage + the deploy log's
  already-diagnosed error tail, and the Full Load→CDC watermark handoff);
  `get_prerequisite_verdicts` returns the Full Load/CDC prerequisite checks (binlog, GTID,
  MSK, IAM, PKs…) with PASS/FAIL/WARN + remediation and `can_proceed`; `list_cdc_dlq_samples`
  returns a sample of the actual DLQ error messages per (table, SQLSTATE). `get_full_load_status`
  and `list_failed_full_load_tables` also gained per-table `rows_skipped` / `rows_quarantined` /
  `attempts` detail. All cached/local (no extra AWS calls per question) and credential- +
  row-data-free — the failing row's PK value is deliberately excluded (Property 7).

## v0.1.383

### Changed

- **Extracted the AI DBA's read-only tools out of `ui/app.py` into their own
  `ui/ai_tools.py`.** The tool schemas, the system-prompt hint, and the ~400-line
  `_ai_tool_execute` body now live in one cohesive module; `build_ai_tool_executor(...)`
  is a factory that takes the page's stores + Full Load rate/ETA helper and returns the
  executor (a factory, so the module needn't import app's globals — no circular import).
  Behavior-preserving move (the tool logic is unchanged); `build_page` just wires it. The
  executor is now unit-testable on its own (new `tests/test_ai_tools.py`), which it wasn't
  as a closure. Sets up adding more tools (Full Load / CDC failure diagnostics) cleanly.

## v0.1.382

### Fixed

- **AI DBA replies no longer fail with "AI reply unavailable / could not be parsed"
  on reasoning-heavy turns (e.g. Query Converter's "Tune with AI DBA").** The current
  models (Claude 5 family) use extended thinking, and thinking tokens count against
  `max_tokens`; the chat/guidance cap was 1536, small enough that a single turn could
  spend the whole budget on thinking and stop (`stop_reason=max_tokens`) with no answer
  text — which surfaced as an unparseable reply. The generation budgets are now
  generous (8192) so thinking plus a full answer fit; the concise `_RESPONSE_STYLE`
  still keeps visible answers short, so normal replies don't get longer.

### Changed

- **Token budgets are keyed by call shape, not per screen/feature.** Now that the AI
  DBA is one persistent app-wide panel, every interactive turn (object guidance,
  grounded chat, tool-calling chat, CDC error chat) shares a single `_CHAT_MAX_TOKENS`
  — one place to tune the chat budget, nothing per-screen to keep in sync. The only
  other budgets are the genuinely different shapes: the one-shot assessment report and
  the bare permission probe (`max_tokens=1`, output ignored). Internal only.

## v0.1.381

### Changed

- **Query Converter's "Test on target" now resolves unqualified tables against the
  right schema, and lets you pick it.** A converted query like `SELECT ... FROM
  "orders"` was being tested under the default `public` search_path, so a table
  migrated into a `<database>`-named schema failed with `relation "orders" does not
  exist (SQLSTATE 42P01)`. The test now resolves the schema robustly — the connected
  source database (a MySQL DB maps to a same-named PG schema) when the target has it,
  else the target's sole user schema — and adds an optional **"Test against schema"**
  picker (populated in the background from the target's user schemas, defaulted to the
  inferred one) so you can retarget when a table was migrated under a different schema.
  On a `42P01` rejection the raw error is now followed by an actionable hint (which
  schema it tested against and how to fix it). Rather than force a schema up front
  (Convert needs none), the schema is inferred and only surfaced as an override.

## v0.1.380

### Changed

- **The AI DBA panel is now drag-resizable, and its default width is narrower.** You can
  drag the panel's left edge to set any width; a persistent grip on that edge (which
  highlights on hover) marks it as draggable, with an `ew-resize` cursor. The width you
  choose is remembered and a later browser resize no longer overrides it. The auto
  (default) width dropped from 40% to 30% of the viewport so it pushes the Tool UI less on
  first open, still clamped to a 360–660 px readable range; a dragged width is clamped only
  to the window (keeping at least 200 px for the Tool UI). UI-only.

- **Redesigned the migration overview diagram as three independent, explicitly-labeled
  cards** (Source → Migration Tool → Target) with neutral-grey borders and header bands,
  joined by grey dashed connectors, replacing the single unified panel. The source/target
  cards show the engine identity, the endpoint as a copy-to-clipboard "code label", and
  inline detail rows ordered Endpoint → Instance type → Region; the blue-accented Migration
  Tool card has a cog tile, a "Convert · Load · Validate" header tagline, and the current
  stage. The journey stepper below the diagram is slimmed. Adds a reusable
  `render_status_pill` (Cloudscape StatusIndicator) to the design system as the single
  source of truth for the tinted status badge. UI-only — the diagram's data derivation
  (`build_migration_diagram`) is unchanged.

- **Removed the duplicate per-step status badge** on the Evaluation and Schema Conversion
  screens (the shared step header already shows it); the Data Migration status stays, since
  it is phase-aware (Full Load / CDC). Minor Data Migration option-card copy clarification.

## v0.1.378

### Security

- **Rebuilt the DSQL sink connector plugin with PostgreSQL JDBC (pgjdbc) 42.7.11 → 42.7.12**
  to clear a newly-published Dependabot advisory — the pgjdbc silent channel-binding
  authentication downgrade (CVE-2026-54291, affecting 42.7.4–42.7.11, so the prior 42.7.11
  bump was itself in range). Sink jar only (`PLUGIN_VERSION` v34 → v35); the Debezium
  plugin and offset-seeder zip are unchanged. Low real risk (the downgrade needs a MITM
  position and reliance on `channelBinding`; the sink uses TLS to the trusted Aurora DSQL
  target). Done via the full rebuild flow — a Dependabot manifest-only PR would have left
  the committed shaded jar at 42.7.11. Picking up the new plugin needs a cdc-stack
  redeploy (Delete + Deploy infra).

## v0.1.377

### Security

- **Rebuilt the DSQL sink connector plugin with PostgreSQL JDBC (pgjdbc) 42.7.3 → 42.7.11**
  to clear the last Dependabot alert — the pgjdbc SCRAM-auth CPU-exhaustion DoS
  (CVE-2026-42198). Sink jar only (`PLUGIN_VERSION` v33 → v34); the Debezium plugin and
  offset-seeder zip are unchanged. Low real risk here (the DoS needs a malicious
  PostgreSQL server; the sink connects only to the trusted Aurora DSQL target), but
  bumped for a clean advisory surface. Picking up the new plugin needs a cdc-stack
  redeploy (Delete + Deploy infra) — a running stack keeps its current plugin until then.

## v0.1.376

### Security

- **Bumped two transitive web-stack dependencies to clear Dependabot alerts** (both
  pulled by NiceGUI / FastAPI, not first-party code): **aiohttp 3.14.1 → 3.14.3**
  (fixes the HTTP-response-parser out-of-bounds read CVE-2026-69244 plus two moderate
  WebSocket issues) and **starlette 1.3.0 → 1.6.0** (fixes the `request.form()` DoS
  CVE-2026-54283). Both are within NiceGUI/FastAPI's allowed ranges and the suite stays
  green. The remaining pgjdbc SCRAM-DoS alert on the connector is tracked separately —
  it needs a sink-jar rebuild and requires a malicious PostgreSQL server, which our
  trusted Aurora DSQL target rules out.

## v0.1.375

### Fixed

- **Stripped the maintainer's local build path (`/Users/<user>/...`) from the committed
  `offset-seeder-lambda.zip`.** The vendored packages' `.dist-info/RECORD` manifests
  carried absolute `.pyc` cache paths from the machine the zip was built on, embedding a
  personal handle in a shipped artifact. Removed those manifest lines only (457 across 10
  RECORD files); every package file and the Lambda sources (`seeder.py`, `cfnresponse.py`)
  are byte-identical, so the Lambda's behavior is unchanged. No `PLUGIN_VERSION` bump (the
  immutable MSK Connect plugins are untouched).

## v0.1.374

### Changed

- **The Fargate `ContainerImageUri` default now points at the freshly published
  `:0.1.373` ECR Public image** (was `:0.1.358`), so a default deploy pulls the current
  build. The image is still hosted in an individual account's ECR Public registry
  (alias `z0q0i9j0`) as an INTERIM measure — noted in `cloudformation.yaml`, to be
  migrated to an AWS Labs / team-owned account.

## v0.1.373

Public-release readiness pass ahead of open-sourcing under AWS Labs
(`github.com/awslabs/dsql-migration-toolkit`).

### Removed

- **The internal-only "temporary GitLab SSH" clone path is gone from the EC2 from-source
  template** (`cloudformation-ec2.yaml`): the `DeployKeySsmParam` parameter, the
  `HasDeployKey` condition, the read-deploy-key IAM policy, the port-22 `GitSshEgress`
  rule, and the user-data SSH-deploy-key branch. It was pre-publication scaffolding for
  cloning from an internal Git host and is dead for public users. Source acquisition is
  now public-HTTPS `git clone` (default) or an S3 source tarball (restricted network /
  your own local copy) only — no deploy key, no port-22 egress.

### Changed

- **The EC2 template's `SourceRepoUrl` default now points at the public repo**
  (`https://github.com/awslabs/dsql-migration-toolkit.git`) instead of a personal fork.
- **`cloudformation.yaml` top-level description corrected** — it no longer calls building
  and pushing an image a prerequisite (the `ContainerImageUri` default is the published
  ECR Public image, no build), and it now states job/session state is kept in the managed
  S3 bucket (durable across a task replacement), not on ephemeral storage.
- **READMEs** retitled to "DSQL Migration Toolkit" (the repo is source-agnostic ahead of
  future non-MySQL sources; the tool today migrates MySQL → DSQL), and the Quick Start
  `git clone` now uses the real public URL. The pip package and CLI stay
  `mysql-dsql-migrator`.
- **`THIRD-PARTY-NOTICES.md` now enumerates all three bundled plugin artifacts** — it had
  covered only the Debezium jars. Added the shaded dependencies of the custom DSQL sink
  plugin (PostgreSQL JDBC / pgjdbc 42.7.3 BSD-2-Clause; AWS SDK for Java 2.x 2.31.0
  Apache-2.0; transitive Netty / Reactive Streams) and the vendored Python packages in
  the offset-seeder Lambda (boto3, botocore, s3transfer, jmespath, python-dateutil, six,
  urllib3, click, kafka-python, aws-msk-iam-sasl-signer-python).
- **`pyproject.toml`** gained `[project.urls]` (Homepage / Repository / Issues → the
  awslabs repo) and an `authors` entry.
- Minor: completed the `.gitignore` "committed on purpose" note (it omitted
  `offset-seeder-lambda.zip`) and fixed a stale PNG path in it; genericized a personal
  handle used as test-fixture data; and dropped an internal "Property 7" spec reference
  from the customer setup manual (EN/KO/JA).

## v0.1.372

### Changed

- **The "these settings are not permanent" notice moved to the bottom of the Settings
  modal** (below the tuning tabs) instead of sitting between the title and the tabs. As a
  banner above the tabs it competed with the header; as a closing footer note it still
  registers the caveat — values apply app-wide, take effect without a redeploy, and
  revert to the deploy-time defaults on restart (set the `DSQL_MIGRATOR_*` env var to make
  one stick) — without pushing the tabs down.

## v0.1.371

### Changed

- **"Start over" and the version label moved from the header to the left-drawer footer**
  (below Settings). "Start over" is destructive — it can also tear down the CDC
  infrastructure — so it no longer sits one click from the "AI DBA" button where it
  invited mis-clicks. It now renders as a list row matching the "Settings" row (icon +
  "Start over" + "Reset this session" caption); the confirm dialog still spells out the
  full impact. The header keeps only the AI controls ("What's next?" / "AI DBA").
- **The AI DBA side panel width is now responsive** — clamped to roughly 40% of the
  browser width (min 360 px, max 660 px) and recomputed on resize, instead of a fixed
  660 px that crushed the tool UI on a narrow browser. Wide screens keep the comfortable
  660 px; a narrow window gives the panel proportionally less so the migration UI stays
  usable.

## v0.1.370

**AI DBA now spans the whole migration** — every step, not just Evaluation — as one
consistent, on-demand, tool-grounded assistant.

### Added

- **AI wired across every step.** CDC gets dead-letter-queue and schema-drift diagnosis
  chats, a `get_cdc_status` tool (streaming / lag / DLQ depth + poison tables + SQLSTATEs
  / detected drift), and CDC activity events (streaming started, drift detected, DLQ
  growing, sink stalled). Cut over gets a grounded repoint-recipe / "is it safe to cut
  over?" GO-HOLD chat (connection-string translation, IAM-token generation/refresh,
  sslmode, OCC retries, identity-sequence timing, rollback asymmetry). Full Load gets
  per-failed-table AND per-quarantine AI help. Validation's mismatch chat and Schema
  Conversion's per-warning chats are now tool-wired.
- **Six new read-only tools** (15 total): `get_source_object_detail`,
  `list_unsupported_objects`, `list_failed_full_load_tables`, `get_cdc_status`,
  `list_validation_mismatches`, `get_schema_apply_result` — so the assistant NAMES the
  specific blocking objects/tables instead of citing counts. Schema/counts/verdicts
  only, never row values or credentials (Property 7).
- **Reimplementation guidance** for the triggers / stored routines / scheduled events
  DSQL cannot convert: an on-demand "Ask AI DBA how to reimplement these" action names
  each object and gives a per-kind path (application logic, or an external scheduler).
- **"Use as target DDL" footer** on the conversion chat: adopt an AI-proposed DDL block
  (denylist-checked) straight into the editable target → Apply, closing the loop
  chat-natively.
- **Live Full Load monitor card** pinned in the AI panel (done/failed tables + failed
  names + rows/sec + ETA), which keeps updating as you navigate to other steps; plus a
  visual schema-apply summary event (Created/Skipped/Failed).
- **"What's next?" briefing** in the header — a proactive, tool-grounded read of what to
  do next and the top risks (including CDC DLQ/drift/stall and standing gaps CDC won't
  backfill before cut over).

### Changed

- The assistant is branded **"AI DBA"** consistently everywhere (header, panel, every
  chat scope), and its accent palette now lives in the design system as the single
  source of truth.
- **All AI is on-demand and consistent across steps** — removed the only auto-fired AI
  turn (Generate no longer auto-opens a chat; its detail is one click away on the
  banner), so no step spends a model call without an explicit click.
- Removed the unreachable Step-2 approve/reject "AI suggestion" review UI (it was never
  populated from the screen); the AI-DDL path is the chat's "Use as target DDL" loop.

### Fixed

- **Cost safety:** turning AI Assist off on the Connect screen mid-session now
  immediately inerts the panel — the composer dies and no further model turn can fire.
- **Property 7:** a failed Full Load table's error message is now sanitized
  (`safe_error_message`, dropping the driver `DETAIL: Failing row contains (...)` line)
  before the new `list_failed_full_load_tables` AI tool can relay it, so a table-level
  failure can never carry a row's column values to the model.
- **The composer no longer locks** after navigating during a streaming reply (the
  streaming timer is anchored to the persistent panel), nor after a restart that
  restored an object-scoped chat (it falls back to the general assistant).
- **Query Converter "Test rewrite on target":** never executes a data-modifying CTE
  (`WITH … (DELETE/INSERT/UPDATE) …` is refused — the re-test runs EXPLAIN ANALYZE, which
  would otherwise write to the target), applies the source `search_path` so a rewrite
  with unqualified names is not falsely rejected, and adds caveats (proves cost/runs-OK
  not row-set equivalence; a suggested index was not created).
- A Schema Conversion per-warning AI icon now seeds the chat with THAT specific warning
  rather than a generic "walk me through converting this table".

## v0.1.369

### Fixed

- **An Evaluation interrupted by an app restart no longer spins at "Starting
  evaluation… 0%" forever.** The restart restored the step as ``IN_PROGRESS`` but the
  background job did not survive, so the poll had nothing to advance. The screen now
  reconciles a stale ``IN_PROGRESS`` step whose job is gone back to ``NOT_STARTED``
  (with a calm "interrupted by a restart — run it again" note), so the spinner stops
  and the Run button returns. Evaluation only reads, so nothing was lost.

## v0.1.368

The AI assistant grew from a per-object Q&A box into an **agentic, migration-aware helper**.

### Added

- **Tool-using AI (in-process function calling, not an MCP server).** The general chat —
  and each per-object/finding chat — can call read-only, credential-free tools to answer
  specific questions from the migration's REAL data: list converted tables, show an
  object's converted DSQL DDL, list/inspect assessed objects by classification, the
  validation verdict, Full Load/CDC status, plus LIVE target-DSQL reads (existing tables,
  a table's schema, a table's row count). Results render as Markdown tables/code. Tools
  return only schema/counts/verdicts — never row values or credentials (Property 7).
- **Activity feed.** Major actions are mirrored into the panel as deterministic timeline
  events: Evaluation (with a visual Automatic/Review/Unsupported breakdown bar), Schema
  Conversion generate + apply, Full Load start, Validation, Cut over, connection tests,
  and step transitions. An unseen-count badge shows on the collapsed reopen tab.
- **Connection-error AI help.** A failed connection test is auto-recorded and offers a
  one-click "Ask AI to help fix this" that diagnoses from the exact entered values.
- **Per-finding AI guidance** in Evaluation (a compact icon on each review finding),
  a **Stop** button for a streaming reply, and a **connected-model chip** under the composer.

### Changed

- **A real chat surface:** roomy multi-line composer (Enter sends, Shift+Enter newline;
  per-message cap raised 1000→4000 for pasted DDL), full-width replies, a wider panel,
  a character counter, and design-system components for the activity chip + a segmented
  distribution bar (single source of truth in `ui/design.py`).
- **Context-grounded, never generic.** Every chat is grounded on injected deterministic
  cross-step context (source/target coordinates + versions, migration type, assessment /
  schema-apply / Full Load / CDC / validation state, and the connected model) and a shared
  response-style directive: answer from the provided facts/tools with scannable Markdown,
  not textbook advice. A per-object chat can now also answer wider-migration questions
  via its tools instead of declining.
- **Durable transcript.** The AI conversation now survives an unexpected app restart / crash
  (persisted in the session snapshot, bounded to recent messages), not just a browser
  refresh; "Start over" still clears it.

### Fixed

- Activity events posted from background job threads now render live (they were silently
  dropped), via a UI-loop drain timer; a restored-open panel no longer leaves the composer
  disabled with no way to start a chat.

## v0.1.367

### Changed

- **The AI panel's composer is now a roomy, multi-line chat box instead of a single-line field.**
  A follow-up is often a pasted DDL/SQL snippet or a few sentences, so the input is now an
  auto-growing textarea: it starts a few lines tall and grows with what you type, capped so a long
  paste scrolls inside the box rather than pushing the send button off-screen. Enter sends the
  message and **Shift+Enter** inserts a new line (the newline suppression is done client-side, so a
  plain Enter never leaves a stray blank line).
- **AI replies now use the panel's full width, and the repeated ✨ avatar icon beside each reply
  is gone.** The per-reply avatar column only ate horizontal space and narrowed every answer; each
  reply is now a full-width card, so long guidance (and code blocks) read with more room.

## v0.1.366

### Changed

- **Retired the old per-screen AI chat drawer — the persistent panel is now the sole AI chat
  surface.** With every screen deep-linking into the app-wide panel (v0.1.361/365), the fallback to
  `ai_chat_drawer.build_chat_drawer` was dead in production, so it was removed from all five screens
  and the `build_chat_drawer` dialog builder was deleted (its module keeps only the pure,
  screen-agnostic helpers the panel reuses — markdown segmentation, the `ChatStreamer` contract,
  guardrail constants). Two screens whose openers were previously gated only by the drawer's
  presence (Schema Conversion, Query Converter) now gate explicitly on AI-assist-enabled, so an
  AI-off session never opens the panel through them. No user-facing behavior change (the app already
  used the panel everywhere).

## v0.1.365

### Changed

- **All four remaining AI chats (Schema Conversion, Query Converter, Validation, Full Load) now open
  into the persistent AI panel, the Evaluation intro banner was removed, and the "10 questions per
  conversation" cap is gone.** Each screen's per-object/-table/-topic AI button now deep-links into
  the app-wide panel (`open_ai_scope`) so the conversation lives across the whole journey — with a
  graceful fallback to the old per-screen drawer only when the panel isn't wired (e.g. tests), so no
  behavior change to existing tests. The redundant Evaluation intro paragraph ("Analyze the source
  against the target … The report can be downloaded.") was removed — the journey header, the "Ready
  to evaluate" card, and the report itself already convey it.
- **Guardrail rearchitecture: the arbitrary per-conversation turn cap (`MAX_CHAT_TURNS = 10`) is
  removed.** The relevance guardrail is now purely the domain-scoped system prompt (every chat is
  pinned to this MySQL→Aurora DSQL migration and politely declines/steers back on anything
  off-topic), and cost is bounded by CONTEXT, not turns: the strategist trims each request's
  transcript to a character budget, so an arbitrarily long conversation still costs a bounded amount
  per turn. A per-message input length cap and the single-in-flight-turn lock remain; replies stay
  advisory and credential-free (Property 7). (A hard denied-topics layer via Amazon Bedrock
  Guardrails remains available as optional future hardening.)

## v0.1.364

### Added

- **The AI panel can now be used from the header to "ask anything about this migration," not only
  from a screen's object button — with a migration-only guardrail.** Opening the panel with no
  object scope activates a GENERAL assistant grounded on the current step (`MigrationContext`), so
  the composer is immediately usable. Its system prompt carries the SAME guardrail as the per-object
  chats: it is pinned to this MySQL→Aurora DSQL migration (schema conversion, data migration/CDC,
  validation, cut over, DSQL behavior) and politely declines + steers back on anything off-topic;
  it is credential-free (Property 7) and bounded (turn/length caps). The general streamer is
  reconstructable, so the composer stays usable for it after a browser refresh (a screen-specific
  scope's live streamer is not, and stays disabled until re-opened from that screen). A screen
  deep-link still takes priority over the general scope.

## v0.1.363

### Changed

- **The AI panel now has a visible right-edge "expand" tab when collapsed, and its header control
  reads as "hide" rather than "close."** When the panel is closed (and AI is enabled), a small
  pinned `AI` tab sits on the right edge so it is obvious how to reopen it (not only the header
  button); it hides while the panel is open. The header dismiss control is now a right-collapsing
  chevron (tooltip "Hide the AI assistant") instead of an X, since the panel is only hidden — the
  conversation persists and either the tab or the header button brings it back.

## v0.1.362

### Changed

- **The header AI-panel toggle is now a labeled "AI assistant" button, not a bare icon**, so it is
  obvious it opens/closes the side panel (a lone `auto_awesome` glyph gave no hint of what it did).
  It matches the "Start over" button's treatment and its tooltip now reads "Open or close the AI
  assistant panel."

## v0.1.361

### Added

- **The persistent AI assistant panel is now live in the app shell, and the Evaluation screen's AI
  guidance opens into it.** Second increment of the AI-experience overhaul: `build_workflow_sidebar`
  builds the panel once (a right drawer that survives content refreshes), adds an always-present
  `auto_awesome` **header toggle** to open/close it anytime, and shows a baseline context chip ("where
  you are"). The panel is handed back to `app.py`, which routes screen AI buttons + a per-step
  `MigrationContext` through it. Evaluation's per-object "AI guidance" now deep-links into the panel
  (`open_scope`) so the conversation persists across the journey, with a graceful fallback to the
  old per-screen drawer when the panel isn't wired (e.g. tests) — so no behavior/test churn for the
  other screens yet. Schema Conversion, Query Converter, Validation, and Full Load still open the
  per-screen drawer; migrating them and retiring the old drawer lands next.

## v0.1.360

### Added

- **(Internal, not yet user-visible) Foundation for a persistent, app-wide AI assistant panel.**
  First increment of the AI-experience overhaul (moving from five per-screen chat dialogs that reset
  on every open to one always-available panel whose conversation persists across the whole journey):
  a session-backed conversation model (`AiConversation` / `AiScope` / `MigrationContext` on
  `SessionConnectionState.ai_conversation`) and the `ui/ai_panel.py` component — a Cloudscape-styled
  right drawer that renders FROM the session (so the transcript + open/closed state survive closing/
  reopening the panel, navigating between steps, and a browser refresh), streams replies, and
  deep-links per subject via `open_scope` (divider on scope change; the streamer is grounded on the
  current scope's turns only, while the full transcript stays visible as scrollback). Credential-free
  (Property 7); advisory/deterministic-first. NOT yet wired into the shell or the screens — that
  cutover (header toggle, right drawer, migrating the five screens, retiring the old drawer) lands in
  a follow-up. No user-facing behavior change in this release.

## v0.1.359

### Changed

- **Fresh-clone default image bumped `0.1.344` → `0.1.358`.** The `ContainerImageUri` default in
  `deploy/cloudformation.yaml` now points at the freshly republished public image
  (`public.ecr.aws/z0q0i9j0/mysql-dsql-migrator:0.1.358`), so a `git clone` + deploy pulls the
  current build instead of a stale one. The `0.1.358` image was published to all three registries
  (public `z0q0i9j0`, private `us-east-1`, private `ap-northeast-2`) and the deployed Seoul stack
  (`mysql-dsql-migrator-seoul`) was updated to it. Drift guard (app patch − default patch, must stay
  in 0–20) is now 1.

## v0.1.358

### Changed

- **The Full Load source-load throttle (v0.1.357) is now a runtime-tunable Settings knob and its
  paused state shows in the progress caption.** Two follow-ups that make the opt-in governor usable
  on Fargate (where the container's environment is fixed at deploy and the app is reachable only
  through the web UI):
  - **Settings-tab knob.** `full_load_max_source_threads_running` is now a runtime tuning knob
    (Settings → Full Load), so an operator can enable / adjust / turn it off from the browser — the
    only config path on Fargate — with the change applied at the next Full Load run / "Retry failed
    tables" (no redeploy), exactly like the other Full Load knobs. To fit the integer-knob machinery
    the field changed from `Optional[int]` (unset = off) to `int` with **0 = off** (the default);
    the env key is unchanged and 0/unset both mean "no throttle". Bounds 0–10000.
  - **Progress-caption hint.** While a table's read is paused by the governor, the Full Load progress
    caption appends "— paused on source load (N table(s): source Threads_running over the configured
    ceiling)", so a deliberately throttled read is never mistaken for a hang. The workers report each
    pause/resume transition to the progress drain (table name only — Property 7), which keeps a
    per-table paused-reader count on the job (`MigrationJob.throttled_tables`, additive/defaulted);
    the count rides the existing in-memory job snapshot and does not change the durable job
    signature, so throttle oscillation triggers no extra S3 writes.

## v0.1.357

### Added

- **Opt-in Full Load source-load throttle (`full_load_max_source_threads_running`, unset = OFF)**
  (from the perf/stability + migration-tool reviews; gh-ost `--max-load` style). Full Load could
  previously only bound its source read pressure with the STATIC reader caps
  (`full_load_table_parallelism` × `full_load_reader_shards` × `full_load_batch_parallelism`) — up to
  ~128 concurrent readers — with no PROACTIVE back-off on a healthy-but-loaded production source.
  When this ceiling is set, each reader now PAUSES before fetching its next page while the source's
  global `Threads_running` exceeds it, and resumes when the metric recedes (a sliced, Stop-responsive
  wait polled at the same between-pages point as the cooperative cancel — never mid-page). Because
  the migration's own readers count toward `Threads_running`, this effectively caps the source's
  active-query concurrency at roughly the ceiling, protecting a live-serving source. It is
  **pause-only** (never fails the load), **fail-open** (a failed `SHOW GLOBAL STATUS` read is treated
  as "don't throttle", so a broken status read can't stall the migration), and the metric is
  TTL-cached (~2 s) so the extra status query is negligible even across many readers. Unset by
  default → no throttle, zero overhead. IMPORTANT: set it ABOVE your application's steady-state
  `Threads_running` plus the headroom you want the migration to use — too low will stall the read.
  The paused/resumed state is logged (WARNING on pause, INFO on resume); surfacing it in the UI is a
  future wire-up via the governor's `on_state_change` hook. Config key:
  `DSQL_MIGRATOR_FULL_LOAD_MAX_SOURCE_THREADS_RUNNING`.

## v0.1.356

### Changed

- **A resumed (append / CDC-coexisting / sharded) Full Load no longer retains per-batch resume
  state that grew with the row count — it keeps a compact per-shard high-water instead** (from the
  perf/stability review). The batched loader's in-process source-drop retry uses an *ephemeral*
  resume job (created per table / per shard reader, never persisted) to skip keyset ranges already
  committed on a retry. On the SKIP_EXISTING path that job was passed from row 1, and the loader
  retained one `_BatchOutcome` per batch **and** appended one `ChunkState` per batch to it — so a
  billion-row unsharded table accumulated hundreds of thousands of objects (× table parallelism),
  contributing to the silent Fargate OOM the memory-pressure logger warns about. Because batches
  stream in keyset order (batch `index` maps to a stable PK range), a completed *contiguous* prefix
  means every range up to that index is committed, so the resume point a retry needs is just the
  highest contiguously-completed batch index **per shard** — one int per shard, not one object per
  batch. The loader now folds outcomes into its running aggregate (as the non-resume path already
  did) and advances a compact `_BatchResumeTracker` (new `MigrationJob.resume_batch_watermark`);
  the skipped-batch count is a single counter rather than an id list. Out-of-order completions are
  absorbed by a small reorder buffer, and a FAILED batch *seals* its shard (freezing the frontier
  and dropping the buffer) so a mid-shard failure cannot re-grow memory — a retry re-runs everything
  past the frontier idempotently. Resume converges to the same target as before; the only behavior
  change is that a batch completed *after* a gap is re-run on resume (idempotent under
  `INSERT ... ON CONFLICT`) rather than individually skipped. The durable per-**table** job (what
  the UI shows and what survives a restart) is unchanged; the new field is additive and defaulted
  so already-persisted job snapshots still deserialize.

## v0.1.355

### Changed

- **Validation now computes the CHECKSUM over bounded keyset PK pages instead of one whole-table
  `SELECT SUM(...)` scan, so a large table can actually produce a checksum on Aurora DSQL** (from
  the perf/stability review). The per-table checksum was the last unbounded full scan on the
  validation path (row counts and PK reconciliation are already keyset-paged): a single
  `SELECT SUM(md5-prefix) FROM table` over a big table exceeds DSQL's hard ~300s per-transaction
  limit, so the checksum could never complete and CHECKSUM-mode validation of a large table was
  effectively impossible. The per-row checksum token is now factored out (`_mysql_row_token` /
  `_pg_row_token`, shared by the whole-table checksum, the row-diff sample, and the new page
  builders so they can never drift) and, for a single-column primary key, each engine sums the
  token over one `WHERE pk > :last ORDER BY pk LIMIT N` page at a time and accumulates the per-page
  sub-sums in Python. Because the token is per-row and `SUM` is order-independent, and a
  single-column PK partitions the rows into disjoint covering pages, the accumulated total equals
  the whole-table checksum exactly — while every statement stays well under the 300s limit and
  memory stays at one page. The DSQL side composes with the v0.1.353 reconnecting proxy (a page
  that hits the ~1h connection age limit replays from its last PK). The MySQL side is paged the
  same way so both engines accumulate identically (each returns the integer total, so a paged side
  is never compared against a whole-scanned side) and no single unbounded `SUM` statement runs; it
  stays within validation's existing per-table `START TRANSACTION WITH CONSISTENT SNAPSHOT`, so
  paging does not change what snapshot the source checksum sees. A composite/missing primary key
  keeps the whole-table single-scan fallback (a documented residual, like the composite-PK
  `COUNT(*)`); MySQL has no per-statement limit so its fallback is safe.

## v0.1.354

### Changed

- **Full Load reader sharding now parallelizes tables with a COMPOSITE primary key whose leading
  column is an integer, not just single-integer-PK tables** (from the migration-tool architecture
  review; scope chosen after an adversarial design/verification pass). Previously only a single
  integer PK could be range-sharded, so a large table with e.g. a `(tenant_id, id)` composite PK
  loaded single-threaded — contradicting the tool's billion-row-scale stance. `shardable_int_pk`
  is generalized to `shardable_leading_int_pk`: it shards on the LEADING PK column whenever that
  column is an integer, for both single and composite PKs, and `keyset_stream` now applies the
  leading-column range bound (`>= :lo AND < :hi`) for composite keys too (ANDed with the existing
  5.7-safe lexicographic disjunction cursor, which is unchanged). Correctness is provable and was
  adversarially verified: integer columns are collation-free, so MIN/MAX arithmetic yields interior
  boundaries strictly increasing in MySQL's numeric order → the K shard ranges are disjoint and
  covering, and because the band is on the leading value only, every row sharing a leading value
  co-locates in one shard (a composite key is never split). Boundary derivation stays scan-free
  (one `MIN`/`MAX` on the leading column). A NON-integer leading column (string / UUID / decimal /
  temporal / binary) intentionally stays a single reader: those orderings are collation-dependent
  (a case/accent-insensitive collation's order ≠ byte order, and design verification found real
  overlap/gap/crash hazards in string/timestamp boundary schemes), and the single-reader fallback
  is always correct — just not parallel. Single-integer-PK behavior is unchanged.

## v0.1.353

### Fixed

- **Validation now reconnects and resumes when its target DSQL connection ages out, instead of
  permanently erroring the table and blocking cut-over** (found by the perf/stability review).
  Aurora DSQL force-closes a connection at its ~1-hour maximum connection duration. Validation
  opened ONE target connection per comparison unit (one per table in the parallel path, one for
  all tables in the serial path) and reused it for that unit's entire count + checksum + keyset
  PK reconcile + orphan reads, with no reconnect — so a validation that ran past ~1h (a single
  billion-row table's keyset count/reconcile, or a multi-table run in the serial mode) hit the
  connection drop, recorded the table as errored, and — because reconcile restarts with no
  reconnect — that table could never pass, blocking the pre-cut-over gate. This is the same
  aged-connection class already hardened on the WRITE paths (`schema_applier._run_ddls_reconnecting`
  and the batched loader's pool, v0.1.344) but the validation READ path lacked it. The target
  connection is now wrapped in a transparent reconnecting proxy: on a connection-level transient
  error (SQLSTATE class 08 / no-SQLSTATE, via `is_transient_connection_error`) it discards the dead
  connection, reconnects (re-minting a short-lived IAM token, bounded retries + short backoff), and
  re-runs the in-flight statement. All target reads are read-only/idempotent and the keyset pagers
  carry their own `WHERE pk > :last` bound, so a mid-page drop resumes from the last PK rather than
  rescanning; a real query/constraint error still propagates unchanged.

### Changed

- **Data-migration package layout made consistent + honest (no behavior change), from the
  architecture review.** The package was a hybrid (per-feature `_cdc_*` slice for CDC, per-layer
  for the rest) with two misleading names. Renamed for a consistent, honest convention:
  `ui/data_migration/_engine.py` → **`_full_load_engine.py`** (it is the Full Load backend engine;
  now symmetric with `_full_load_ui.py`) and `ui/data_migration/_status.py` → **`_cdc_status.py`**
  (it is ~all CDC; the generic name hid that). All importers (src + tests) were updated; no
  re-export shim, since these are private submodules. Documented the intentional layout in the
  package `__init__` docstring — CDC is a larger subsystem so it legitimately spans more files
  (`_cdc_ui`/`_cdc_monitoring`/`_cdc_state`/`_cdc_status` + `core/cdc*`), Full Load is a tight
  pipeline (`_full_load_engine`/`_full_load_ui` + `core/exporter`/`batched_import`), and
  `_models`/`_state` are the shared layer coupled by the Full Load → CDC watermark handoff — so
  file count tracks intrinsic complexity, not forced symmetry. Deliberately did NOT relocate the
  `full_load_error_*` readers or `_current_job` out of `_cdc_status`: the FL and CDC error-log
  views share `is_cdc_error_record` (moving them would fragment that cohesion and pull a CDC
  predicate into the FL module), and `_current_job`'s primary user is `_cdc_status` itself; the
  module docstring now states this scope honestly instead.

## v0.1.351

### Changed

- **Maintainability refactor (no behavior change), from the refactor-opportunity backlog.**
  Split `ui/data_migration/_cdc_ui.py` (5415 → 3695 lines) into three modules in two steps:
  (1) the pure CDC state/phase predicates (`cdc_streaming_started`, `cdc_pipeline_live`,
  `cdc_monitoring_visible`, `cdc_teardown_badge`, `cdc_infra_deploy_in_flight`,
  `_cdc_is_streaming`, `_CDC_POLL_INTERVAL_SECONDS`) → new `_cdc_state.py`; then (2) the
  ~1600-line post-start monitoring / DLQ / status render panels
  (`_render_migration_table_status`, `_render_cdc_live_monitoring`, `_render_cdc_dlq_panel` +
  sub-panels, the schema-drift banner, `lob_exclusion_lock`, the CDC-handling panel, …) →
  new `_cdc_monitoring.py`, which imports the predicates from `_cdc_state` (not back from
  `_cdc_ui`). The predicate split had to come first: it removes the back-edge that would
  otherwise make `_cdc_ui` ↔ `_cdc_monitoring` a circular import. `_cdc_ui.py` re-exports all
  moved names, so every consumer/test import resolves unchanged. One-directional throughout
  (`_cdc_state` → `_status`; `_cdc_monitoring` → `_cdc_state`/`_models`/`_status`/`core`/`ui`).
  No behavior change.

## v0.1.350

### Changed

- **Maintainability refactor (no behavior change), from the refactor-opportunity backlog.**
  Extracted the Full Load render cluster out of `ui/data_migration/__init__.py`
  (5386 → 3384 lines) into a new sibling `ui/data_migration/_full_load_ui.py` — the watermark
  and load-status renderers, the ~830-line `_render_full_load_step`, the per-cell/tooltip
  formatters, the quarantine helpers, and the progress/completeness/error-log renderers (plus
  the small `format_selected_workloads` headline). Mirrors the existing `_cdc_ui.py` split and
  is re-exported at the bottom of the package `__init__`, so `dm.<name>` and every
  consumer/test import resolve unchanged. One-directional (the new module imports from
  `_engine`/`_models`/`_status`/`_cdc_ui` + shared `core`/`ui`, never back from `__init__`).
  One test's monkeypatch was repointed to the new module (the moved probe resolves its
  `DsqlConnector`/`tables_with_rows`/`target_primary_keys` deps in the new namespace). No
  behavior change.

## v0.1.349

### Changed

- **Maintainability refactor (no behavior change), from the refactor-opportunity backlog.**
  Extracted the NiceGUI-agnostic schema-apply engine out of `ui/schema_conversion.py`
  (5077 → 3977 lines) into a new `ui/schema_conversion_apply.py`: the apply contracts
  (`ApplyMode`/`ApplyObject`/`SchemaApplier` protocols), the OCC-retrying orchestration
  (`run_schema_apply` + the `build_apply_objects` / composite-/identity-conversion family),
  the AI-assisted conversion-unit builders, `split_sql_statements`, and the `DsqlSchemaApplier`
  adapter + `default_applier_factory`. This is pure domain/apply logic with no NiceGUI
  dependency (the most heavily unit-tested part of the file); the screen builder and render
  helpers stay. One-directional (the new module imports only stdlib + `core.*` + `ui.ai_assist`,
  never back). `schema_conversion.py` re-exports every moved name, so all consumer/test imports
  resolve unchanged. No behavior change.

## v0.1.348

### Changed

- **Maintainability refactor (no behavior change), from the refactor-opportunity review.**
  Split the stateful AWS/CloudFormation wrapper out of `core/cdc_deployer.py`
  (2196 → 1450 lines) into a new `core/cdc_stack_deployer.py`: the `CdcStackDeployer` class,
  its `CdcStackDiscovery`/`CdcDeployError` data types, the `build_cdc_stack_deployer` factory,
  and the `_stack_absent_error` / `_parse_unsupported_azs` (+ `_UNSUPPORTED_AZ_RE`) helpers it
  uses. The `run_cdc_*` deploy orchestration (`_StageDriver`, stage seeders,
  `_wait_stack_settles`, the connector-log diagnostics) stays in `cdc_deployer.py` and depends
  on the wrapper one-directionally (the new module never imports the orchestration). All moved
  names are re-exported from `cdc_deployer.py`, so every UI/test import
  (`from dsql_migrator.core.cdc_deployer import CdcStackDeployer`, `build_cdc_stack_deployer`,
  …) resolves unchanged. No behavior change.

## v0.1.347

### Changed

- **Maintainability refactor (no behavior change), from the refactor-opportunity review.**
  Extracted the Start-over confirmation dialog and the CDC-teardown/lifecycle banner UI out
  of `ui/workflow.py` (2213 → 1703 lines) into a new `ui/start_over.py`:
  `_start_over_cdc_warning`, `_open_start_over_dialog`, `_cdc_teardown_banner_copy`,
  `_render_cdc_teardown_banner` (and the `_TEARDOWN_BANNER_POLL_SECONDS` constant they use).
  These are a self-contained teardown-safety UI concern — each takes the NiceGUI `ui` module
  and its callbacks as explicit parameters, so the new module has no back-dependency on
  `workflow.py` (imported one-directionally by `build_workflow_sidebar`). `workflow.py`
  re-exports them, so `from dsql_migrator.ui.workflow import _start_over_cdc_warning` (used by
  tests) resolves unchanged. `_render_reconnect_banner` deliberately stayed (it depends on a
  core notice helper). No behavior change.

## v0.1.346

### Changed

- **Maintainability refactor (no behavior change), from the refactor-opportunity review.**
  Two safe, behavior-preserving structural changes:
  - Extracted the pure cross-engine SQL builders out of `core/validator.py` (2066 → 1528
    lines) into a new `core/validation_sql.py` — the checksum/PK-token/keyset-page builders,
    the `_checksum_kind`/quoting helpers, `build_orphan_count_sql`, and
    `integer_pk_column`/`single_pk_column`. These are pure `TableDef`/`ColumnDef` → SQL
    functions with no connection/thread/run state; isolating them makes the most
    independently-tested logic easy to find. `validator.py` re-exports them, so
    `from dsql_migrator.core.validator import build_mysql_checksum_sql` (and every other
    consumer/test import) resolves unchanged.
  - The two lazy S3-client builders (`JobStore._s3`, `SessionStateStore._s3`) now build
    through the shared `aws_session.build_session` factory instead of re-implementing the
    `boto3.Session(profile) or boto3.Session()` selection, so they share the one credential
    context every other AWS client uses. Behavior is identical.

## v0.1.345

### Changed

- **The default `ContainerImageUri` now points at the freshly republished `0.1.344` ECR
  Public image** (was `0.1.338`). The `0.1.344` image was republished to all three registries
  (ECR Public `z0q0i9j0`, and the private us-east-1 / ap-northeast-2 repos), so a fresh
  `git clone` deploy pulls a current build with the post-load index-creation fix and the
  Validation review fixes (0.1.340–0.1.344) baked in, instead of the older `0.1.338` default.
  A republish is not required for every patch (the drift guard tolerates being a few patches
  behind); this refreshes it after a batch of behavior fixes.

## v0.1.344

### Fixed

- **Post-load index creation now recovers from an aged-out / dropped DSQL connection instead
  of spuriously failing every remaining index** (found by the connection / UI-wiring review).
  A large-table Full Load can run past Aurora DSQL's ~60-minute maximum connection duration,
  after which DSQL force-closes a pooled connection with no SQLSTATE. `_create_indexes` held a
  SINGLE leased connection for the whole `CREATE INDEX ASYNC` loop and wrapped each DDL in OCC
  retry with the default predicate (SQLSTATE 40001 only), so a mid-loop connection drop was
  (1) not retried — it propagated on the first attempt — and (2) swallowed by the per-index
  handler, which then kept reusing the SAME dead connection for every remaining DDL. The result
  was that ALL of a table's secondary indexes were reported failed even though the data was
  complete and a reconnect would have built them (non-data-loss, but the operator had to rebuild
  indexes by hand). The loop now leases a FRESH connection inside each retried unit and treats a
  transient connection drop as retryable (`retryable=_is_retryable_load_error`) — mirroring the
  data path (`_execute_insert`) — so a dropped/aged connection is discarded and the index builds
  on a new one. A genuinely bad index DDL (e.g. DSQL's 24-index limit) is still isolated and
  surfaced as before.

## v0.1.343

### Fixed

- **Validation now discloses the columns it did NOT value-compare, and its docstrings no
  longer overclaim** (found by the Validation review). FLOAT/DOUBLE and JSON columns have
  no byte-identical cross-engine text form, so the CHECKSUM omits them — a difference
  confined to a NON-KEY float/double/json value is therefore undetected by any mode (an
  in-place value edit leaves the row count unchanged and reconciliation compares
  primary-key presence, not values). The module docstring previously claimed a CHECKSUM
  match "means the data itself is equal" and that float equality was "covered by the
  row-count comparison and reconciliation" — false for a non-key column — with no operator
  disclosure. Now each table records the omitted columns
  (`TableValidationResult.checksum_excluded_columns`) and both the text report and the
  cut-over readiness panel surface them, so a "Data identical" result reads as "every
  column EXCEPT these was value-compared." The exclusion itself is unchanged (there is no
  sound byte-identical cross-engine float/JSON comparison).

## v0.1.342

### Fixed

- **Validation's CHECKSUM now honors a Schema-Conversion target-type remap instead of
  false-mismatching it** (found by the Validation review). The per-column checksum render
  was classified by the SOURCE type, so after the operator applied a non-default target
  remap the tool itself recommends — e.g. keeping a `TINYINT(1)` as `smallint` for values
  outside 0/1 — the source rendered `'true'/'false'` while the `smallint` target rendered
  `'0'/'1'`, and **every row of a correctly-migrated column false-mismatched** in CHECKSUM
  mode (a false alarm that can block cut-over). The Validator now resolves the APPLIED
  target types from the Schema-Conversion result (converter vocabulary) and renders each
  column by how it was actually stored, falling back to the source-derived mapping when
  the applied types aren't available (e.g. after a reconnect). Source-based spatial/BIT
  handling is unchanged.

## v0.1.341

### Fixed

- **Validation now scales to very large target tables — the target row count is bounded,
  not a single `COUNT(*)`** (found by the Validation review). Aurora DSQL enforces a hard
  300-second transaction limit, but Validation ran one `SELECT COUNT(*)` per table (every
  table, every mode) — a full-table scan in a single transaction that, past a few minutes
  of rows, failed with `transaction age limit of 300s exceeded`, so a large table could
  never report MATCH. It was worst for a UUID/`varchar` PK (Aurora DSQL's recommended
  shape), which also skips the keyset reconcile, leaving no path to confirm the table. The
  target count now comes from the exact total the keyset reconcile already streams (integer
  PK), or is keyset-paged directly for **any** single-column PK (int/uuid/varchar/binary) —
  each page a bounded sub-300s statement, memory one page. A composite/missing PK still uses
  `COUNT(*)` (a documented residual). The source count is unchanged (watermark snapshot /
  live). Note: CHECKSUM mode's full-table checksum scan is a separate, still-unbounded
  residual (a follow-up), so the default ROW_COUNT + reconcile path is now fully bounded
  but CHECKSUM mode can still time out on a very large table.

## v0.1.340

### Fixed

- **Validation no longer mishandles a NULL in a nullable boolean (`TINYINT(1)`) column in
  CHECKSUM mode** (found by the Validation review). The MySQL-side checksum rendered a NULL
  boolean as `'true'` — `NULL = 0` is UNKNOWN, so the `CASE` fell through to `ELSE` — while
  the target renders a NULL boolean as SQL NULL (the shared `~N` sentinel). That both
  (a) FALSE-MISMATCHED a correctly-migrated NULL→NULL row (a false alarm that can block a
  clean cut-over on correct data) and (b) FALSE-MATCHED a source-NULL vs target-TRUE row
  (hiding a genuine value corruption — a soundness hole in the exact case CHECKSUM mode
  exists to catch). The MySQL render now returns SQL NULL for a NULL boolean (an `IS NULL`
  guard), routing it through the same sentinel as every other type so both engines agree.

## v0.1.339

### Changed

- **Republished the app container image at `0.1.338` to the private ECR (us-east-1 and
  ap-northeast-2) and ECR Public, and pointed the CloudFormation `ContainerImageUri`
  default at `0.1.338`** (was `0.1.334`). A fresh `git clone` deploy now pulls an image
  built with the v0.1.335–338 fixes (schema-conversion warnings + bytea-in-key, the
  ZEROFILL checksum fix, and the `BIT(1)`/`BIT(64)` CDC sink fixes) instead of the
  4-releases-old default. No application code change.

## v0.1.338

### Fixed

- **CDC no longer corrupts a `BIT(64)` value above 2^63** (found by the tricky-schema CDC
  E2E, right after the `BIT(1)` fix let those rows replicate at all). The sink's
  `bitsToLong` accumulated Debezium's little-endian `Bits` bytes into a **signed** `long`,
  so a `BIT(64)` value ≥ 2^63 wrapped negative (`2^64-1` → `-1`) and landed as `-1` in the
  `numeric(20,0)` target while Full Load stored the correct unsigned value — a silent
  CDC value corruption. It now accumulates into a `BigInteger` and returns a `Long` for
  `BIT(≤63)` (integer target) or a `BigDecimal` for a `BIT(64)` value above `Long.MAX_VALUE`
  (numeric target). Rebuilds the DSQL sink plugin (`PLUGIN_VERSION` v32→v33).

## v0.1.337

### Fixed

- **CDC no longer drops every row from a table with a `BIT(1)` column** (found by the
  tricky-schema CDC E2E on a live cluster). Debezium serializes MySQL `BIT(1)` as a plain
  Kafka boolean (no logical schema name), but the schema converter maps `BIT(n)` to a DSQL
  integer (smallint), so the sink bound a boolean into a smallint column and quarantined
  **every** change event for such a table to the DLQ (`SQLSTATE 42804 column "b1" is of
  type smallint but expression is of type boolean`) — silent CDC data loss. `DsqlSinkTask`
  now coerces a boolean bind to `0`/`1` when the target column is an integer type (read
  from `ParameterMetaData`), the mirror of the existing `TINYINT(1)`→boolean coercion, so
  Full Load and CDC agree. Rebuilds the DSQL sink plugin (`PLUGIN_VERSION` v31→v32); a CDC
  infra Delete+Deploy (or a fresh deploy) is required to pick up the new plugin.

## v0.1.336

### Fixed

- **Validation no longer false-mismatches a MySQL `INT ZEROFILL` column.** The per-column
  CHECKSUM rendered a ZEROFILL integer on the source with `CAST(col AS CHAR)`, which
  applies the ZEROFILL display padding (e.g. `00042`), while the migrated target holds a
  plain integer (`42`) — so a correctly-migrated ZEROFILL column was reported as a data
  mismatch in Validation. The source side now renders the plain numeric value (`col + 0`,
  which drops the display attribute), matching the target. Found by the tricky-schema
  conversion E2E on a live Aurora DSQL cluster.

## v0.1.335

_Schema-conversion fidelity fixes, found by applying the converter's own output to a
live Aurora DSQL cluster over a comprehensive "tricky schema" (BINARY/VARBINARY/spatial
keys, no-PK, composite/>8-col/>255-col, reserved/unicode/over-long identifiers, CHECK,
generated columns, expression indexes, comments, partitioning, collations)._

### Fixed

- **`bytea` columns in a key now surface loudly instead of producing DDL Aurora DSQL
  silently rejects.** DSQL rejects a `bytea` column in any key ("datatype bytea is not
  supported in a key", verified live). A BINARY/VARBINARY primary key (which maps to
  `bytea`) makes the `CREATE TABLE` fail, and a secondary or spatial index on a `bytea`
  (or geometry) column makes the post-load `CREATE INDEX ASYNC` fail — yet the converter
  emitted them anyway, and the only note it produced (the 1 KiB key-size recommendation)
  wrongly said "the DDL itself applies fine." Now a `bytea` **primary key** is reported
  UNSUPPORTED (re-key to text/uuid/hash before migrating); a `bytea` **secondary index**
  is **skipped** rather than emitted as a doomed statement, and reported; the spatial-index
  note is corrected to say a SPATIAL index on a geometry column cannot be created and is
  not emitted; and the misleading key-size note is suppressed for a `bytea` key.
- **A table with no primary key is described accurately.** Aurora DSQL actually *accepts*
  a `CREATE TABLE` with no primary key (verified live), so the warning no longer claims
  DSQL "requires" one — it now explains the real blocker: the migrator's Full Load reads
  rows by primary-key keyset and CDC replicates by primary key, so a PK-less table cannot
  be migrated (still UNSUPPORTED; add a primary key first).

### Added

- **Schema Conversion now warns about four previously-silent drops**, so the conversion
  screen no longer looks clean while quietly losing something:
  - **Over-long / colliding identifiers** — MySQL allows 64-character names; PostgreSQL
    and DSQL truncate to 63 bytes. Two column names that share the first 63 bytes collide
    and the `CREATE TABLE` is rejected ("column specified more than once") — reported
    UNSUPPORTED; a single over-63-byte name that only truncates is a recommendation.
  - **Source CHECK constraints** — DSQL supports CHECK, but the converter does not re-emit
    an arbitrary MySQL CHECK expression, so it was dropped with no note on the conversion
    screen (only Evaluation flagged it). Now warned there too.
  - **Functional / expression indexes** (`KEY ((LOWER(email)))`) — reflection keeps only
    plain column key-parts, so an all-expression index was dropped entirely with no note.
    Now warned (recreate it as a DSQL expression index after the load).
  - **Table / column COMMENTs** — now captured from the source and reported as dropped
    (cosmetic; re-add with `COMMENT ON` if you rely on them).

## v0.1.334

_Batches several CDC-review fixes under one patch (the ECR Public image is republished
once for the batch)._

### Fixed

- **Ordinary CDC poison rows falsely raised a "source schema change detected" banner.**
  `classify_schema_drift` mapped any class-22 SQLSTATE (`22001` string-too-long, `22003` numeric
  out-of-range, `22007` bad datetime, `22P02` bad text) to a `TYPE_CHANGE` drift — but those are
  per-**value** rejections that ordinary bad data raises with no source DDL change at all, so a
  single oversized/out-of-range quarantined row was reported as "the source changed a column's
  type," steering the operator toward a schema reconciliation that was not the real problem. Drift
  classification is now restricted to the three **structural** SQLSTATEs (determined by the row's
  column set / a column's declared type, which a source DDL change produces): `42703` → ADD
  COLUMN, `23502` → DROP COLUMN, `42804` (datatype_mismatch) → TYPE CHANGE. A class-22 value error
  stays an ordinary quarantine (still counted in DLQ depth, not surfaced as drift).

- **Every CDC status poll re-discovered the CloudWatch metric dimensions ~5× and re-paged
  `ListMetrics` every ~5 s.** `applied_ops_by_table` (3 op metrics) plus `replication_lag_by_table`
  and `replication_lag_series` (the same `ReplicationLagMs` metric, twice) each ran the bounded
  `ListMetrics` pagination on every poll — 5 discovery passes per tick for data that changes only
  when a new table starts emitting, inflating CloudWatch cost/latency with the poll rate and
  risking throttling (which blanks the monitor). `_list_metric_dimensions` is now memoized per
  `(stack, metric)` for a short TTL, collapsing the passes within a poll and the re-discovery
  across polls to one pagination per metric per window; the live `GetMetricData` data reads stay
  uncached so the numbers remain current.

- **The DLQ view missed pre-existing quarantines on a late/headless attach.** The first CloudWatch
  DLQ read looked back only the trailing hour, so a CDC run that had been dead-lettering for longer
  than that before the operator opened the UI (e.g. a headless `run_e2e_migration.py` stream)
  silently omitted the earlier quarantines — and once the read cursor advanced forward they could
  never be picked up. The first-read look-back is now widened (`_DLQ_INITIAL_LOOKBACK_SECONDS`) to
  capture a realistic late-attach backlog; subsequent polls still read incrementally from the
  cursor (each read is `limit`-bounded, so history is not re-scanned every poll).

- **Clicking "Start CDC" ran blocking AWS/file work on the UI event loop, freezing every
  browser session on Fargate.** The Start-CDC confirm handler performed a deploy-role
  `AssumeRole` (deployer build), an STS `GetCallerIdentity`, a ~50 KiB template read, and a config
  load synchronously on the single asyncio loop that serves all sessions — so the click stalled
  every session for the round-trip (longer under STS throttling). All of that setup now runs inside
  the submitted background job body (worker thread), matching the sibling infra-deploy path; the
  event loop only builds the pure params and submits the job.

- **The CDC monitor re-scanned the entire (uncapped) DLQ error log 4–5× on every ~5 s poll.**
  The depth badge, per-table chips, drift banner, and record list each called `cdc_dlq_records`,
  which copied the whole append-only error log and re-ran the per-record CDC/Full-Load predicate —
  O(records) work repeated 4–5× per tick and growing unbounded exactly during a drift storm when
  the log is largest (the opposite of the project's bounded-work stance). `cdc_dlq_records` is now
  memoized on the log's append-only record count (via a new O(1) `ErrorLogStore.count`), so the
  copy + filter runs only when a new record actually arrives; an unchanged count hands back the
  cached view, and a reset drops the count to 0 and invalidates it.

- **A connector CREATE_FAILED rollback surfaced an opaque message instead of the diagnosed
  cause.** On a connector failure the stack rolls back and the connectors-pass wait raises before
  the per-connector RUNNING-waits (which diagnose on real names) are reached — so its own rollback
  diagnosis is the only chance to surface the cause. But it was handed a synthetic
  `"<src> + <sink>"` pseudo-name that matched no CloudWatch log stream (`connector_log_tail`
  matches a stream by substring), so the worker-log diagnosis was always dead on this primary
  failure path and the operator got the opaque "Stack operation ended in
  'UPDATE_ROLLBACK_COMPLETE'". The wait now takes the list of REAL connector names and scans each
  one's worker log, so a known failure (source unreachable, access denied, partition-quota
  exhaustion, a plugin-packaging defect) surfaces as actionable guidance.

- **CDC dropped the sub-second precision of a MySQL `TIME(1–6)` value (Full Load ↔ CDC
  divergence).** The sink's `DebeziumTypeConverter.microsToTime` built the value with
  `java.sql.Time.valueOf(LocalTime)`, and `java.sql.Time` holds only hour/minute/second — the
  JDK contract discards the sub-second field — so a CDC-applied fractional `TIME` landed
  truncated (`12:34:56` for a source `12:34:56.789012`), while the Full Load path (Python
  `datetime.time(microsecond=…)`) kept the microseconds. The two write paths therefore disagreed
  and Validation flagged a mismatch on every fractional-`TIME` row. The converter now returns a
  `java.time.LocalTime`, which pgjdbc binds to a `time`/`time(n)` column with full microsecond
  precision (and is timezone-independent). Rebuilds the DSQL sink jar; connector plugin
  `PLUGIN_VERSION` → `v31` (a `PLUGIN_VERSION` bump requires Delete + Deploy of the CDC infra to
  take effect).

- **The CDC cost estimate understated MSK Connect ~3×.** `estimate_cdc_hourly_cost` assumed a flat
  "two connectors at 1 MCU each" ($0.22/hr), but the defaults deploy 2 (source) + 4 (sink) = 6 MCU,
  so it understated MSK Connect ~3× and the whole hourly figure ~30–40% — the opposite of the tool's
  cost-awareness principle. The MSK Connect component now derives from the actual source + sink MCU
  counts × the per-MCU rate, so the estimate tracks the real deploy (and a sink resize).

- **A CDC dead-letter caveat rendered as loose red text instead of a design-system notice.** The "a
  zero count is expected while the sink is stalled" caveat (a stalled sink never reaches a record to
  quarantine, so a zero depth is not evidence nothing was lost) was a bare `text-red-700` label; it
  now uses `render_notice(tone="warning", …)` so the box + amber border + leading icon carry the
  severity, per the design system.

- **The CDC deploy-role session now auto-refreshes its credentials, so a long deploy no longer
  expires mid-flight (H5 root-cause prevention).** `build_assumed_role_session` returned static
  1-hour `sts:AssumeRole` credentials, but the deployer reuses one session for a whole operation
  that can exceed an hour (each connector RUNNING-wait ≤45 min; an AZ-retry delete + recreate passes
  an hour) — so the credentials expired mid-flight and every CloudFormation read threw
  `ExpiredToken` (the case v0.1.333's fail-fast surfaces). The production path now builds botocore
  `RefreshableCredentials` that re-run `AssumeRole` before expiry (chaining off the base identity),
  keeping the long op supplied with valid credentials; the injected-`session_factory` test path
  stays static (botocore-free), preserving the unit-test contract. The refreshable path uses
  botocore internals and can't be exercised end-to-end offline — validate it against a live >1 h
  deploy before relying on it; the deployer's fail-fast still surfaces any expiry cleanly regardless.

## v0.1.333

### Fixed

- **A CDC deploy/start that outran the 1-hour assumed-role credentials reported a false
  "Stack operation timed out." and stranded a billable half-built stack.** The deployer runs a
  single `sts:AssumeRole` session (static 1 h credentials) for the whole operation, but a run can
  exceed an hour (each connector RUNNING-wait is up to 45 min; an AZ-retry delete + MSK recreate
  passes an hour). Once the credentials expired, every CloudFormation read raised `ExpiredToken`,
  which `stack_status` swallowed to `None`; the stack-settle wait read that `None` as "transient,
  keep polling" and burned the entire timeout before wrongly reporting a timeout — even when the
  stack had actually reached `CREATE_COMPLETE`. The wait now reads status through a raising probe
  (`stack_status_checked`) and fails fast on a terminal credential/authorization error with an
  actionable cause ("credentials may have expired … the stack operation may still be running in
  AWS — check the console, then retry"), while still tolerating a few transient read blips —
  mirroring the connector RUNNING-wait's existing handling. `stack_status` keeps its best-effort
  swallow for other callers. (Root-cause prevention — refreshable deploy-role credentials so a
  long op never expires mid-flight — is tracked as a follow-up.)

## v0.1.332

### Fixed

- **The CDC replication-lag monitor blanked entirely once a migration tracked more than 500
  tables.** `applied_ops_by_table` batches its CloudWatch `GetMetricData` queries into ≤500-query
  requests (the API cap), but `replication_lag_by_table` and `replication_lag_series` issued a
  single unbatched call. Past 500 tables that call raised `ValidationError`, which the methods'
  broad `except` swallowed to `{}` / `[]` — blanking the per-table "Stream lag" column **and** the
  "Stream lag over time" trend chart for the *entire* pipeline (not just the overflow tables), so
  the operator faced the cutover decision with no replication-lag signal at all. Both lag methods
  now batch into ≤500-query requests and merge across batches (by id for the per-table current
  lag; by timestamp bucket, MAX, for the trend), matching `applied_ops_by_table` so the lag
  surface scales to a large table set.

## v0.1.331

### Fixed

- **The "Fix target schema…" ADD COLUMN drift recovery was a silent no-op (dominant-case
  recovery unreachable).** The DLQ log parser keyed each dead-lettered record on the *bare*
  table name (`orders`), but the drift recovery's `information_schema` reads need the
  db-qualified `db.table` — a bare name splits to `schema='orders', name=''` and matches zero
  rows, so `plan_add_columns` produced an empty plan and the dialog reported "the target already
  has every source column" while the missing column was never added and the table kept
  dead-lettering. `_table_from_topic` now returns the db-qualified `db.table` (the last two
  topic segments), which also makes the DLQ per-table surface consistent with the CloudWatch
  monitor's `Table` dimension, the connector's `table.include.list`, and the target's
  schema-qualified tables (all already `db.table`). Also guards the recovery dialog: an empty
  source-column read now reports an explicit "could not read source columns for '<table>'" error
  instead of the false "already up to date".

## v0.1.330

### Fixed

- **A non-seedable watermark (GTID-only) selected `snapshot.mode=recovery` with no offset to
  recover from — a broken CDC start.** `build_source_config` chose `recovery` whenever the
  watermark had a binlog file *or* a GTID, but the gapless handoff resumes from a seeded
  `connect-offsets` entry that the seeder writes only when binlog `file`+`pos` are BOTH present
  (`CdcResumePoint.can_seed_offset`). A watermark with a GTID set but no binlog coordinates —
  reachable when `SHOW MASTER STATUS` is restricted (no `REPLICATION CLIENT`) while
  `@@GLOBAL.gtid_executed` still reads — therefore got `recovery` while the seeder was skipped,
  so the source task either failed at start (nothing to recover) or silently resumed from the
  *current* binlog, losing every change made during the Full Load window. The mode gate now uses
  the same `can_seed_offset()` precondition as the seeder: a non-seedable watermark falls back to
  `schema_only` (which starts the connector cleanly). Gapless-from-Full-Load was never achievable
  for such a watermark; the UI surfaces that separately.

## v0.1.329

### Fixed

- **CDC gapless-handoff offset seed could make Debezium skip rows at the resume point (silent
  loss).** The read-modify-write seed (the production path: it reads the connector's live
  offset for the no-clobber guard, then overrides `file`/`pos` to the Full Load watermark) left
  the live offset's `row`/`event` skip counters intact. A watermark position is always an event
  boundary (`row=0`/`event=0`), but the live offset can carry a non-zero `row`/`event` (the
  connector stopped mid multi-row event) that is meaningful only at its OLD position — so
  carrying it onto the watermark position made Debezium skip that many rows/events in the first
  event after the resume, rows present in neither Full Load nor CDC. Both the app builder
  (`core/cdc_offset_seed.build_source_offset`) and the vendored in-VPC Lambda copy
  (`deploy/cdc-stack/lambda/seeder._build_source_offset`) now reset `row`/`event` to `0` when
  they override the position (`server_id` is source identity, not a skip counter, so it is left
  as-is). Rebuilds the offset-seeder Lambda zip; connector plugin `PLUGIN_VERSION` → `v30`
  (a `PLUGIN_VERSION` bump requires Delete + Deploy of the CDC infra to take effect).

## v0.1.328

### Fixed

- **A source-drop retry mid-Full-Load re-streamed and re-probed every already-loaded batch of
  a huge table (batch-level resume was dormant).** `import_rows` can skip already-committed
  keyset ranges via a resume job, but the workers never passed one — so an in-process retry
  (e.g. an Aurora failover partway through a billion-row table) re-did the whole table. Each
  worker now reuses a per-table (per-shard) resume job across its source-drop retries, so a
  retry skips the batches the prior attempt already committed. Wired ONLY where it is safe:
  the append/`SKIP_EXISTING` path and the sharded path, whose targets are NOT recreated on a
  retry so committed rows persist; the single-table replace/`NONE` path still withholds it (a
  retry recreates the empty target, so skipping would silently lose data). In-process only —
  a full "Retry failed tables" still recreates and restarts the table. Also removes the
  `SKIP_EXISTING` re-probe waste a resume otherwise pays on the overlapping prefix.

## v0.1.327

### Fixed

- **Full Load retained one batch-outcome object per batch for the whole table load.** The
  importer accumulated a `_BatchOutcome` per batch (~500k objects for a billion-row table) and
  aggregated them only at the end. Outcomes are now folded into a running aggregate as each
  batch resolves, so loader memory stays bounded regardless of row count. The per-batch list
  is retained only when a job is passed (batch-level resume / small tables), where per-batch
  chunk state is still upserted exactly as before — the reported result totals are unchanged.

## v0.1.326

### Fixed

- **Full Load worker payload and progress accuracy (multiprocess path).** Two more Full Load
  review fixes for the ProcessPool path:
  - *Each worker submission pickled the whole SourceInventory + every table's converted DDL,
    giving O(tables^2) serialization/IPC.* A worker migrates one table, so its inputs are now
    slimmed to that table's conversion and an empty inventory (never read in the worker path),
    making a many-thousand-table migration O(tables).
  - *Every submitted table was marked IN_PROGRESS at submission time,* so with a bounded worker
    pool a large run showed far more tables "in progress" than were actually running and
    inflated per-table elapsed/ETA. A worker now signals when it actually begins its table and
    the parent marks the chunk IN_PROGRESS then (idempotent across a table's shards); a
    dropped start signal is covered by the first live progress message.

## v0.1.325

### Fixed

- **Full Load review fixes (correctness, efficiency, and bounded memory).** Follow-up to the
  Full Load data-path review:
  - *A NONE-mode (clean-load) batch could be FALSELY quarantined on a retry.* When a plain
    INSERT committed but the ack was lost to a transient drop, the OCC replay hit a 23505 and
    the whole already-present batch was binary-split and quarantined as phantom data loss. A
    NONE-mode 23505 now recovers idempotently via SELECT-existing + insert-missing.
  - *A systematic data error could quarantine unboundedly.* A whole-column failure (e.g. every
    value over DSQL's 1 MiB per-value limit) binary-split every batch down to single rows and
    appended a quarantine record per row without bound. Retained records are now capped and,
    past the cap, the load fails loudly so the systematic cause is fixed instead of silently
    dropping most of the table.
  - *A composite-PK keyset export could degrade to a full table scan per page.* The row-value
    cursor `(a, b) > (?, ?)` only uses the PK index on MySQL 8.0.14+; on a 5.7-compatible
    source (RDS MySQL 5.7 / Aurora MySQL 2) it full-scanned every page (O(n^2)). The cursor is
    now the explicit index-friendly expansion `(a > ?) OR (a = ? AND b > ?) OR ...`, which uses
    the PK index on every version (same bind params, lexicographically identical).
  - *SKIP_EXISTING rebuilt its INSERT statement on every batch.* The primary append / resume /
    CDC-coexist load path now reuses the cached statement (like the other modes) instead of
    rebuilding a ~40k-placeholder statement per batch.

## v0.1.324

### Fixed

- **A transient connection failure while creating a pooled load connection could permanently
  shrink the pool and deadlock the whole Full Load.** `_ConnectionPool.lease` pulled a slot
  token from the queue and then created the connection OUTSIDE the try/finally that refills
  it, so a factory exception (DSQL's new-connection rate limit under a burst, or an IAM-token
  mint blip) lost the slot for good. Because a connect failure is classified transient and
  retried, repeated blips drained every slot until the next lease's untimed `get()` blocked
  forever -- wedging every worker thread with no error and no progress. The slot is now
  refilled on a create failure before the error is re-raised, so the OCC retry re-leases and
  creates a fresh connection. Adds a test for the create-failure path.
- **Refreshed the ECR Public default image tag from `0.1.303` to `0.1.314`** (the newest build
  actually published to `public.ecr.aws/z0q0i9j0/mysql-dsql-migrator`), so a new customer's
  default CloudFormation deploy no longer pulls a build that had fallen 21 releases behind.

## v0.1.323

### Fixed

- **A misconfigured DSQL endpoint (a host that does not resolve) was retried the full OCC
  budget before failing.** The Schema Applier classified every no-SQLSTATE connect error as
  transient (a classifier shared with the batched loader), so a typo'd / deleted cluster
  endpoint burned the whole retry budget per table before surfacing. The applier now fails
  fast on the unambiguous permanent host-resolution signatures (getaddrinfo NXDOMAIN: "could
  not translate host name", "name or service not known", etc.) while still retrying a
  transient DNS blip ("temporary failure in name resolution") or a refused connect (a
  rebooting cluster). Scoped to the applier -- the shared connection classifier is unchanged.

## v0.1.322

### Fixed

- **A string-column DEFAULT that looked numeric/boolean/null was emitted unquoted, silently
  changing the value on the target.** `_quote_default_literal` decided quoting from the
  literal's shape alone, but `information_schema.COLUMN_DEFAULT` returns string defaults
  UNQUOTED — so a `VARCHAR`/`CHAR`/`TEXT` (incl. ENUM/SET) column whose default was a string
  like `'00000'`, `'007'`, `'NULL'` or `'TRUE'` converted to a bare `DEFAULT 00000` /
  `DEFAULT NULL` / `DEFAULT TRUE`. The DDL applied cleanly, but PostgreSQL/DSQL then parsed
  `00000` as integer 0 → text `'0'`, dropped the 4-char string `'NULL'` to SQL NULL, and
  stored `'true'` — so every post-cut-over INSERT that omitted the column got the wrong
  default, with no error raised. Quoting now keys off the mapped (textual) target type, not
  the literal shape: a textual target always single-quotes the literal. Numeric/date/boolean
  targets are unchanged (a number still emits bare).
- **Schema Conversion review fixes (correctness, safety, and efficiency).** A focused review
  of the Schema Conversion path (converter, applier, and UI) fixed:
  - *A backslash in a literal default was re-interpreted on the MySQL re-parse.*
    `_quote_default_literal` doubled only quotes, so `C:\tmp` became `C:<TAB>mp` and a value
    ending in a backslash aborted the whole-table parse; backslashes are now doubled too.
  - *A destructive REPLACE could silently lose an object.* DSQL commits each DDL immediately,
    so a committed `DROP` followed by a failed `CREATE` left the object gone under a generic
    "apply failed"; the applier now surfaces an explicit "dropped but not recreated" error.
  - *Transient connection failures were retried only while opening the connection, not while
    executing.* The REPLACE / `recreate_table` / drop paths now reconnect and replay the whole
    idempotent unit on a mid-execute transient error, not just the connect (a 40001 is still
    absorbed per-statement).
  - *A destructive REPLACE of an unquoted mixed-case name dropped the wrong relation.* DSQL
    folds an unquoted `CREATE TABLE Orders` to `orders`; the existence key and the `DROP` now
    fold to match, so REPLACE targets the relation the server actually created.
  - *An UNSUPPORTED object with an approved AI SCHEMA suggestion was applied and reported
    twice.* Apply units are now deduped by object name (the AI/edited unit wins).
  - *The full-schema conversion was recomputed on every render (including the 0.5s apply
    poll).* It is now memoized per source inventory, so it runs at most once per inventory.
  - *Each inline "Apply to target" click rebuilt the applier and re-browsed the whole target
    catalog.* The applier is now reused per target connection; a "Refresh target" invalidates it.
  - *The per-object existence checker went stale after a "Refresh target".* It is now rebuilt
    whenever the target snapshot changes (an explicitly injected checker is still respected).
  - *A column with no default still re-parsed its type.* `_resolve_column_default` now returns
    early for a column with no default, skipping the wasted sqlglot parse.

## v0.1.321

### Fixed

- **A quarantined row's values could reach CloudWatch Logs; the sink now strips them.**
  The DSQL sink built its DLQ quarantine reason from `SQLException.getMessage()`, and pgjdbc
  builds that from `ServerErrorMessage.toString()` — which appends the server's `DETAIL`
  field. For a not-null violation DETAIL **is the failing row**
  (`Failing row contains (42, alice@example.com, …)`); for a unique violation it is the
  conflicting key value. That reason is `log.warn`'d, so it landed in the connector's
  CloudWatch log group, where the row is not otherwise present. The Kafka DLQ record was
  never the problem — its value already *is* the row — and it still receives the unmodified
  exception.

  `DsqlSinkTask.safeCauseMessage()` now walks the cause chain, and for a server error takes
  the **primary message only**, plus the constraint name when the server named one (an
  identifier, not a value). Client-side failures (connection closed, token expiry) have no
  server message and are passed through unchanged. Two unit tests pin it, including a
  precondition assertion that the *raw* driver message really does leak the row — so the test
  fails if pgjdbc ever stops appending DETAIL and the guard becomes dead code.

  Stated precisely, because the previous comment over-claimed: this bounds the exposure, it
  does not eliminate it. For a few SQLSTATEs the server puts the offending literal in the
  primary message itself (`22P02: invalid input syntax for type integer: "abc"`), so at most
  one value can still appear. A full failing row can no longer reach the log.

  Two comments that asserted the stronger, false claim are corrected in the same change:
  the Javadoc beside the quarantine builder and the `parse_dlq_log_message` docstring in
  `core/cdc_dlq.py`.

  **Deployment**: sink-jar change, so `PLUGIN_VERSION` is bumped to `v29`. MSK Connect custom
  plugins are immutable, so an existing CDC deployment needs **Delete CDC infra → Deploy CDC
  infra** to pick it up; Start CDC alone will not re-register the plugin.

## v0.1.320

### Fixed

- **The CDC deploy role could no longer be used to become account administrator.**
  `iam:AttachRolePolicy` was granted on `role/mysql-dsql-cdc-*` with no restriction on
  *which* policy, and the same role holds `iam:CreateRole`, `iam:PassRole` (to Lambda) and
  `lambda:CreateFunction`/`InvokeFunction`. That is a complete escalation chain: create
  `mysql-dsql-cdc-<anything>`, attach `AdministratorAccess`, pass the role to a Lambda this
  role may also create, invoke it. Attach/Detach now sit in their own statement conditioned
  on `iam:PolicyARN` equal to `AWSLambdaVPCAccessExecutionRole` — the **only**
  `ManagedPolicyArns` value in `deploy/cdc-stack/cdc-stack.yaml`. Verified with
  `iam:SimulateCustomPolicy`: attaching that policy is **allowed**, while
  `AdministratorAccess` and `PowerUserAccess` are **implicitDeny**, and `iam:CreateRole` is
  unaffected.

  Worth stating plainly, because it is the uncomfortable part: this path predates the
  scoping work, and **v0.1.319 removed the scanner signal for it** — `CKV_AWS_109`
  ("permissions management without constraints") stopped firing once the wildcards were
  scoped, while the escalation remained. A test now pins the condition, since no rule will.
  Residual, knowingly deferred: `iam:PutRolePolicy` can still write an inline policy of any
  content onto a `mysql-dsql-cdc-*` role, and IAM offers no policy-content condition for it
  — only `iam:PermissionsBoundary`, which means giving the cdc-stack roles a boundary and
  conditioning role creation on it across both stacks.

- **Two reads CloudFormation makes on every cdc-stack deploy are no longer denied.**
  CloudTrail on a live stack shows `ec2:DescribeSecurityGroupRules` (once per
  `AWS::EC2::SecurityGroupIngress`/`Egress` resource) and `ec2:DescribeNetworkAcls` returning
  `Client.UnauthorizedOperation`, invoked by `cloudformation.amazonaws.com`, on both create
  and delete. Neither was ever granted. The deploy survives because CloudFormation falls back
  to the `Authorize*` response for the rule ids, but every customer's trail collected
  AccessDenied entries and the deploy depended on that undocumented tolerance. Both are
  read-only and neither has a resource-level form, so they join the `Ec2NetworkReads`
  statement.

- **The CDC workload generator validates the schema name it interpolates.**
  `scripts/cdc_workload_customers_sample_new.py` takes `--schema` (or
  `CDC_WORKLOAD_SCHEMA`) and interpolates it into every statement through its `_q()` helper.
  It was missed by the v0.1.315 sweep because that sweep grepped for f-strings **at the
  `execute()` call**, and this script builds the SQL in a helper first — a reminder that the
  right sweep follows the data, not the syntax. Row values were already bound; the schema,
  table and PK-column identifiers now go through `_common.validate_identifier()`.

### Changed

- Corrected two claims in the v0.1.319 notes above: the wildcard-statement count went from
  **8 to 6 on the CDC deploy role** (12 to 10 counting the task role's four read-only
  statements, which did not change), not 12 to 6; and the ALB drop-invalid-headers attribute
  satisfies **`CKV_AWS_131`**, not `CKV_AWS_103` — the latter is the listener's TLS-version
  check, satisfied by `SslPolicy`. The template comment carried the same mislabel.

### Docs

- The VPC and its subnets must belong to the deploying account: RAM-shared (cross-account)
  subnets are **not** supported, because v0.1.319 scoped the deploy role's EC2 permissions to
  `${AWS::AccountId}` resources, so creating the connector's network interface in a shared
  subnet fails with `AccessDenied` (confirmed by simulating against a foreign-owner subnet
  ARN). Stated in `deploy/DEPLOYMENT.md` and §1 of the manual in all three languages.

## v0.1.319

### Changed

- **The privileged CDC deploy role no longer holds EC2 network writes on `Resource: "*"`.**
  A content security review flagged the role for unconstrained write and
  permissions-management access (`CKV_AWS_111` / `CKV_AWS_109`), and the template's own
  comment claimed EC2 create/delete "has no ARN-level conditions". **That claim was
  wrong.** AWS's machine-readable service reference lists resource types for every one
  of those 28 actions, so they are now pinned to the nine types the cdc-stack actually
  creates or touches — `security-group`, `security-group-rule`, `subnet`, `route-table`,
  `vpc`, `natgateway`, `elastic-ip`, `vpc-endpoint`, `network-interface` — in this
  account and region. The ten `ec2:Describe*` calls that genuinely have no
  resource-level form moved to their own read-only statement, and
  `ec2:DescribeVpcAttribute` (which does take an ARN) stayed with the scoped writes.

  Two smaller scopings came out of the same audit: `kafkaconnect:Delete`/`Describe`
  for custom plugins and worker configurations now target
  `custom-plugin|worker-configuration/mysql-dsql-cdc-*/*` instead of `"*"` (only their
  *create* actions, and the tagging the CFN handlers do during create, have no ARN to
  match), and `cloudwatch:DescribeAlarms` targets the same `alarm:mysql-dsql-cdc-*`
  family the alarm writes already used.

  Verified rather than assumed: `iam:SimulateCustomPolicy` returns **allowed** for all
  28 EC2 actions against an ARN of the type they create, and **implicitDeny** for the
  same action in another account, another region, another resource type
  (`ec2:DeleteVpc`, `ec2:RunInstances`), a plugin outside the cdc family, or another
  account's alarm. Wildcard statements on this role drop from 8 to 6 (12 to 10 counting
  the task role's four read-only ones, which are unchanged) — and the six that
  remain are pinned by a test as an explicit allowlist, each with the reason its API
  has no resource-level form, so a new `"*"` statement fails the suite. A second test
  keeps `cloudformation.yaml` and `cloudformation-ec2.yaml` from drifting, since both
  grant this same role.

  A specific VPC or subnet ARN still cannot be pinned: the operator supplies the VPC as
  a cdc-stack parameter long after this role is created, so account+region+type is the
  narrowest form available at role-creation time. Deliberately omitted resource types
  (`ipam-pool`, `ipv4pool-ec2`, `ipv6pool-ec2`, `internet-gateway`, `vpn-gateway`) are
  only evaluated for parameters this stack never passes; using one in `cdc-stack.yaml`
  means adding its ARN here, or the deploy fails with `AccessDenied`.

## v0.1.318

### Changed

- **The CDC prerequisite probe binds the variable names it queries instead of
  formatting them into the statement.** `SHOW GLOBAL VARIABLES WHERE Variable_name IN
  (...)` built its list by string-formatting `_CDC_VARIABLES` into the SQL. Nothing
  external can reach that list — it is a module constant — but the names are *values*,
  and unlike a schema or table name (which cannot be a bind parameter at all) a value
  can simply be bound. It now is, leaving the statement text a plain literal. A test
  pins one placeholder per entry in `_CDC_VARIABLES`, so adding a variable cannot
  silently drop it from the query. No behavior change: the same four variables are read.

## v0.1.317

### Changed

- **The cloud-build stack encrypts its ECR repository with KMS and scopes the ECR Public
  publish grant to one repository.** Two content-security-review findings on
  `deploy/codebuild.yaml` that were fixable rather than arguable. The repository was on
  ECR's default AES256; `EncryptionType: KMS` with no `KmsKey` uses the AWS managed
  `aws/ecr` key, so there is no CMK to create and no monthly charge. And the
  `ecr-public:*` publish statement sat on `Resource: "*"` even though those actions take a
  repository ARN — the same action names in the private `ecr:` block beside it were already
  scoped — so a release build can now push the release image and nothing else. Only the
  login-token APIs (`ecr:GetAuthorizationToken`, `ecr-public:GetAuthorizationToken`,
  `sts:GetServiceBearerToken`) remain account-wide, because they have no resource-level
  form; what such a token can *do* is bounded by the scoped grants above.
  `tests/test_deployment_artifacts.py` pins both, including the exact set of actions
  allowed to stay unscoped.

  **Updating an existing build stack will fail**, by CloudFormation's rules rather than
  ours: `EncryptionConfiguration` is immutable, so it tries to replace a repository whose
  name is fixed. Delete the build stack and redeploy it — it holds no state
  (`EmptyOnDelete: true`) and the next build re-pushes the image. The app stack and its
  `ContainerImageUri` are untouched. The ECR Public repository must share the private
  repository's name, which is the convention `buildspec.yml` already documents
  (`public.ecr.aws/<alias>/mysql-dsql-migrator`).

## v0.1.316

### Changed

- **Both `hashlib.md5` call sites now pass `usedforsecurity=False`.** Each one compares an
  S3 **non-multipart ETag**, which *is* the object's MD5 — so the digest is a protocol
  requirement, not a security primitive: `_local_md5` in `core/s3_provision.py` is how the
  tool decides a connector plugin is already uploaded and skips re-uploading it. The flag
  states that intent to the interpreter, keeps the call working on a FIPS-enabled build,
  and clears the high-severity finding a content security review raises on every
  `hashlib.md5` call site. The digest value is unchanged (verified identical), so the
  skip-upload check still matches an existing object.

## v0.1.315

### Changed

- **Content-security-review hardening: the ALB now drops malformed HTTP headers, and
  the HTTPS listener pins a TLS 1.3 policy.** Both were silent AWS defaults. An ALB
  forwards invalid header fields straight to the target — the HTTP desync /
  request-smuggling vector, where the ALB and the app disagree about where one request
  ends — and an HTTPS listener with no `SslPolicy` inherits `ELBSecurityPolicy-2016-08`,
  which still negotiates TLS 1.0/1.1. `deploy/cloudformation.yaml` now sets
  `routing.http.drop_invalid_header_fields.enabled=true` on the load balancer and
  `SslPolicy: ELBSecurityPolicy-TLS13-1-2-2021-06` on the listener (that policy keeps
  TLS 1.2 as the fallback, so ordinary browsers still connect). The app itself is
  unchanged and the workbench sends only ordinary browser + WebSocket headers, so
  nothing user-visible changes; an existing stack picks both up on the next update.
  `tests/test_deployment_artifacts.py` pins them, since neither shows up in normal use
  and nothing else would notice them being removed.

- **The command-line scripts validate every SQL identifier they interpolate.** A
  schema, table or column name cannot be passed as a bind parameter, so
  `scripts/compare_rows.py`, `scripts/cdc_consistency_check.py` and
  `scripts/verify_conversion_on_dsql.py` have to build
  ``... FROM `{schema}`.`{table}` `` — and, in the last one,
  `DROP`/`CREATE SCHEMA "{scratch}"` — by interpolation, while taking those names from
  `--table` / `--schema` / `--scratch-schema` on the command line. Every interpolated
  name (including the PK column and target schema read back out of
  `information_schema`, and the table name reflected from the source) now goes through
  one shared `_common.validate_identifier()` allowlist — `[A-Za-z_][A-Za-z0-9_]*`,
  deliberately stricter than what MySQL/PostgreSQL themselves accept — which raises
  before any SQL is issued. PK *values* were already bound as parameters and stay
  bound. The shipped app is unaffected: its own source reads go through the
  introspector's bound queries.

### Docs

- Softened an absolute claim in the CDC chapter (§4.3, all three languages): the
  gapless Full Load → CDC handoff is now described as **designed to ensure** no missed
  changes and no duplicates, rather than guaranteeing it — the mechanism is unchanged,
  the wording just no longer promises an outcome for every source.

## v0.1.314

### Added

- **A source `ADD COLUMN` detected as schema drift can now be applied to the target
  from the DLQ panel, with the exact DDL shown for approval first.** `v0.1.313` only
  *detected* drift — the operator then had to work out which columns were missing and
  hand-write the `ALTER TABLE`. The "Source schema change detected" band now carries a
  **Fix target schema…** action for `add-column` drift: it reads the source's and the
  target's column lists, renders one `ALTER TABLE ... ADD COLUMN` per missing column
  using the same MySQL→DSQL type mapping the schema conversion uses, and shows the
  statements verbatim in a confirmation dialog. Nothing is applied until that dialog is
  confirmed (Property 6 — the tool still never mutates the target schema on its own),
  and each statement runs in its own transaction because Aurora DSQL rejects two DDL
  statements in one transaction (`multiple ddl statements not supported in a
  transaction`, verified live).

  Deliberate limits: each column is added **NULLable with no default** — `NOT NULL`
  without a default is rejected on a populated table, and a fabricated default would
  invent data for the rows CDC already applied, so existing rows read NULL until they
  are backfilled. A source type the converter cannot map is **skipped and named**, never
  approximated, and a lossy mapping (e.g. a spatial type → `bytea`) shows its warning in
  the dialog. The action is offered for `add-column` **only**: a source `DROP COLUMN` or
  an incompatible type change can rewrite or destroy target data, so those stay
  alert-only. Applying the DDL does not replay the rows already dead-lettered — the
  dialog and the notification both point at per-table Reload for that backfill.

## v0.1.313

### Added

- **CDC now detects and surfaces a source schema change (DDL) instead of leaving it
  as an opaque rising dead-letter count.** CDC does not propagate DDL: when the
  source runs `ALTER TABLE`, the target is unchanged and the first row written under
  the new schema is rejected by Aurora DSQL and quarantined to the DLQ. The sink now
  prefixes each quarantine log line with `sqlstate=<state>`, and the control plane
  classifies that SQLSTATE into a drift kind — `42703` → a column was **added** at the
  source (the target lacks it), `23502` → a column was **dropped** at the source (the
  target still requires it), `42804` / class `22xxx` → a column's **type changed**
  incompatibly. The DLQ panel gains a "Source schema change detected" banner naming
  the affected table(s), what changed, and the manual runbook (apply the matching
  change to the target schema, then per-table Reload to backfill the set-aside rows;
  stop CDC first for a drop/retype). **Detection only** — the tool never auto-alters
  the target (Property 6, no silent schema mutation), and an ordinary poison row
  (no drift SQLSTATE) is not surfaced as drift. This bumps `PLUGIN_VERSION` to `v28`
  (sink jar change, the `sqlstate=` prefix); the Debezium/seeder artifacts are
  unchanged, and a clean stream shows no banner.

## v0.1.312

### Fixed

- **The "CDC teardown failed" banner no longer lingers after the stack is actually
  gone.** A `DELETE_FAILED` teardown (e.g. leftover Lambda ENIs pinning the connector
  security group) freezes both the background job record and the cached stack status
  at "failed", and the banner does not self-poll — so if an operator finished the
  cleanup out of band (terminated the blocking resource, re-ran the delete from the
  CLI/console), the actionable banner stayed up forever with no way to clear it from
  some views. The banner getter now does one best-effort, read-only
  `DescribeStacks` check (reusing the same probe behind the restored-session
  re-verify notice) whenever it would show "failed"; **only** when CloudFormation
  definitively reports the stack does-not-exist does it settle the marker and show the
  dismissable "CDC infrastructure deleted" completion notice. A still-present stack
  (any state, including `DELETE_FAILED`) or any ambiguous/errored read leaves the
  failed banner untouched, so a genuinely still-billing failure is never hidden.

## v0.1.311

### Fixed

- **The `SeedMode=External` CDC path now actually deploys against CloudFormation, and
  the EC2 host wires it end-to-end.** Two gaps from the v0.1.307/v0.1.310 work:
  (1) `run_cdc_start` sent the lowercase `"external"` as the `SeedMode` parameter, which
  CloudFormation rejects against the template's case-sensitive `AllowedValues`
  `["Lambda","External"]` (the unit tests used a fake deployer that skips validation, so
  it went unnoticed) — it now sends `"External"`. (2) The CDC **infra-create** pass never
  carried `SeedMode` or `HostSubnetCidr`, so a UI-driven deploy from the EC2 host created
  the stack in Lambda mode (making the in-VPC seeder Lambda, only to delete it on the
  first Start) and never opened MSK port 9098 for the in-process seed. `build_cdc_infra_params`
  now emits both at create time (mapping the lowercase config value to the capitalized
  template token), sourced from a new `DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR` config key that
  the EC2 user-data auto-resolves from the host's own subnet. Result: on the Lambda-free
  host, "Deploy CDC infra" creates a `SeedMode=External` stack (no seeder Lambda) that
  admits the host on 9098; Fargate/local default to Lambda with no ingress (unchanged).

## v0.1.310

### Changed

- **The "EC2 + MSK only" deploy mode now runs the app FROM SOURCE — no Docker, no
  ECR.** For customers who cannot use a container registry, `deploy/cloudformation-ec2.yaml`
  no longer pulls a container image; instead the host installs `git` + `uv`, obtains
  the app source, runs `uv sync --extra cdc-external`, and starts the app as a
  `systemd` service (`dsql-migrator.service`) — reached the same way (SSM
  port-forward), with the same retained-EBS state and `SeedMode=External`. Source
  acquisition is a new `SourceMode` parameter: `git` (default — `git clone` the
  public repo over HTTPS, or an internal Git host over SSH with a read-only deploy key from
  SSM via the new `DeployKeySsmParam`, which auto-drops its IAM grant + port-22 egress
  once the repo is public) or `s3` (download + extract a source tarball from
  `SourceS3Uri` — the simple way to run a local working copy before the repo is
  public). The `ContainerImageUri` parameter and all Docker steps are removed. The
  Fargate app-stack (`deploy/cloudformation.yaml`) is unchanged and still image-based;
  the Dockerfile keeps `--extra cdc-external` (inert unless `SeedMode=External`).

## v0.1.309

### Fixed

- **The CDC stack failed to deploy** with `Template error: YAML aliases are not
  allowed in CloudFormation templates`. The v0.1.307 sink-connector split used a YAML
  anchor/alias (`&DsqlSinkProps` / `*DsqlSinkProps`) to share one body between the
  Lambda and External sink variants. PyYAML resolves anchors fine (so the structural
  unit tests passed), but **CloudFormation rejects any anchor/alias**, so a real
  `create-change-set` / deploy against the cdc-stack was blocked. The shared body is
  now duplicated verbatim into the two variants (the only difference is the External
  variant's omitted `CdcStartPrepResource` dependency), a test asserts the two bodies
  stay byte-identical, and a new guard scans every deploy template's raw text to
  reject any YAML anchor/alias so this deploy-blocker cannot recur.

## v0.1.308

### Added

- **A Lambda-free "EC2 + MSK only" deployment mode** for customers who cannot use AWS
  Lambda, built on the `SeedMode=External` machinery from v0.1.307. A new deploy
  template `deploy/cloudformation-ec2.yaml` runs the whole control plane (the web UI +
  Full Load engine + in-process CDC seed) on a **single EC2 instance inside the CDC
  VPC**, reached over **SSM port-forward** — no ALB, ACM certificate, or Cognito. State
  (the job / session SQLite databases) lives on a **retained EBS volume** so it survives
  instance replacement, instead of the S3 stores the Fargate stack uses. Because that
  host runs inside the VPC it reaches MSK directly, so **"the host is the mode"**: the
  EC2 host's user-data sets `DSQL_MIGRATOR_CDC_SEED_MODE=external` (a new config key) and
  runs CDC Lambda-free, while the existing Fargate/local deployments leave it unset and
  stay on the in-VPC seeder Lambda — unchanged. Supporting bits: a new
  `DSQL_MIGRATOR_CDC_SEED_MODE` config key threaded into Start CDC (default `lambda`); an
  additive, condition-gated `HostSubnetCidr` parameter on the CDC stack that admits the
  host to MSK on port 9098 by subnet CIDR (empty default → no rule, so existing deploys
  are unchanged); and the container image now bakes the `cdc-external` extra
  (`kafka-python` + the MSK IAM SASL signer, both pure-Python and inert unless
  `SeedMode=External`). The Fargate app-stack, the Lambda-mode default, and
  `PLUGIN_VERSION` are all untouched.

## v0.1.307

### Added

- **`SeedMode` for the CDC stack — an opt-in, Lambda-free ("EC2 + MSK only") way to
  do the CDC Kafka prep.** The gapless Full Load → CDC handoff needs three Kafka
  steps done in-VPC before the connectors are created (pre-create the compacted
  offset topic + per-table data topics + DLQ topic, and seed the connect-offsets
  record). Today an in-VPC seeder Lambda does this. A new `SeedMode` parameter
  (default `Lambda` = today's behavior, unchanged) adds an `External` mode in which
  the deploying app does the prep **in-process** over the MSK IAM bootstrap before
  the connectors are created — so a customer who cannot use AWS Lambda can run CDC
  from an in-VPC host. In `External` mode the stack omits the seeder Lambda, its
  role, and the `CdcStartPrepResource` custom resource, and selects a sink-connector
  variant without the `CdcStartPrepResource` dependency; the two sink variants share
  a single body via a YAML anchor so they cannot drift. The in-process Kafka client
  (`kafka-python` + the MSK IAM SASL signer) is an **optional extra**
  (`pip install ".[cdc-external]"`), so the default install and container image are
  unchanged. **Everything is behind the default:** with `SeedMode=Lambda` (the
  default) no app-side seed runs, no `SeedMode` is sent, and the set of deployed
  resources is identical to before — this release only adds the machinery. The
  security-group / IAM / VPC-co-location wiring that lets a real host reach the
  cluster on port 9098 lands separately with the EC2 host. `PLUGIN_VERSION` is
  unchanged (no connector/seeder artifact changed).

## v0.1.306

### Internal

- **Extracted the pure CDC Kafka-prep decision logic into one canonical app module**
  (`core/cdc_kafka_prep.py`), the foundation for an upcoming Lambda-free ("EC2 + MSK
  only") minimal deployment option. The in-VPC offset-seeder Lambda
  (`deploy/cdc-stack/lambda/seeder.py`) has always duplicated three pure helpers
  (`parse_partitions_map`, `binlog_seq`, `offset_already_at_or_past`) and the
  topic-shaping decision inline; those now have a single, unit-tested home app-side
  (plus a `TopicSpec` / `plan_topics()` planner), and the offset-record builders are
  re-exported from `core/cdc_offset_seed.py` (kept as the one canonical builder). This
  change is **app-side only**: the Lambda source, the committed connector/seeder ZIPs,
  and `PLUGIN_VERSION` are untouched, so **every existing deployment behaves
  identically**. A drift-guard test now asserts the Lambda's inline copies stay
  behaviorally identical to the canonical module in both directions, and a new
  packaging guard asserts the committed offset-seeder ZIP still embeds the current
  on-disk Lambda sources.

## v0.1.305

### Added

- **The activity log can now be sent to CloudWatch Logs on ECS**, via a
  new deploy parameter `EnableActivityLogCloudWatch` (default off). The activity log is
  a rotating file on the task's ephemeral `/tmp`, so it is lost when a Fargate task is
  replaced (a redeploy, crash, or rebalance). When this is enabled the app also mirrors
  each event to stdout as a JSON line, which the container's `awslogs` driver forwards
  to the stack's existing CloudWatch log group (`/ecs/<stack>-mysql-dsql-migrator`) —
  a durable, queryable copy that survives task replacement. No extra IAM or log group
  is needed (the task already logs to CloudWatch). The local rotating file and the
  runtime toggle in the app's Diagnostics panel are unchanged; this just lets the
  mirror be turned on from the start of a deployment rather than only at runtime (a
  runtime toggle resets on restart).

## v0.1.304

### Fixed

- **The CDC sink no longer dies when it dead-letters an oversized row.** When the sink
  quarantines a change event that DSQL rejects on its 1 MiB per-value limit, that
  record is itself larger than 1 MiB — but the DLQ Kafka topic was being auto-created
  by Kafka Connect at the broker's ~1 MiB default, so the DLQ produce failed with
  `RecordTooLargeException` and the sink **task died** (`Stopped — a task is not
  running`), unable to either apply *or* quarantine the poison row. Every event behind
  it on that partition then stalled. (Seen live: a `product_media.full_description`
  row over the limit stopped the whole sink.)

  The offset-seeder custom resource (`CdcStartPrepResource`) now **pre-creates the
  sink DLQ topic** at the same `max.message.bytes` as the data topics (4 MiB by
  default, up to the 8 MiB MSK Serverless max), so a dead-lettered 1–4 MiB record has
  somewhere to land and the sink isolates the poison row and keeps streaming. The
  seeder's `CreateTopic` IAM scope is widened to cover the DLQ topic name (it is not
  under the Debezium topic prefix). Requires a CDC-connector redeploy (plugin `v27`)
  to take effect; an already-running stack can be unblocked by raising the existing
  DLQ topic's `max.message.bytes` directly.

## v0.1.303

### Added

- **CDC DLQ quarantine logs now record the failed row's primary key**, so a
  migration engineer can find and fix the exact source row a poison event came
  from. This matters because a quarantined event is **never retried automatically**
  — the sink does not re-consume the Kafka offset and Debezium does not re-emit the
  binlog event, so the row is permanently set aside until someone re-touches it at
  the source (e.g. shrinks an oversized LOB and `UPDATE`s it, producing a fresh
  binlog event). The Kafka offset in the log cannot be mapped back to a source row;
  the primary key can. (Events *after* a quarantined one keep replicating — a
  permanent failure isolates only the poison row and the offset advances past it.)

  To respect Property 7 (no sensitive row values in logs/reports), PK **column
  names** are always shown, but PK **values** appear only for surrogate keys
  (integers and UUIDs). A natural-key value that may be sensitive — e.g. an email
  or account-number primary key — is withheld and rendered `email=<withheld>`; the
  decision is made per column for composite keys. The PK rides inside the existing
  quarantine reason string (like the SQL template already does), so it surfaces in
  the DLQ panel and the durable activity log with no change to the reader. Requires
  a CDC-connector redeploy (plugin `v26`) to take effect.

## v0.1.302

### Fixed

- **The CDC DLQ depth / "Quarantined records" surface always read empty, even when
  poison records were being written to the dead-letter queue.** The Fargate task role
  could read connector worker logs (`logs:DescribeLogStreams` + `logs:GetLogEvents`),
  but the DLQ read (`MskConnectController.dlq_errors`) uses `logs:FilterLogEvents`
  against the same `/msk-connect/<cdc-stack>-cdc` log group — and that action was not
  granted. At runtime the call got `AccessDenied`, which the reader swallows
  (`except Exception: return []`, fail-closed by design), so the UI reported a DLQ
  depth of zero regardless of what CloudWatch actually held. Verified against the live
  Seoul deployment with an IAM policy simulation: `logs:FilterLogEvents` on
  `/msk-connect/mysql-dsql-cdc-stack-cdc` evaluated to `implicitDeny` while the two
  already-granted actions were `allowed`.

  `logs:FilterLogEvents` is now added to the `ReadConnectorWorkerLogs` statement in
  the deploy template. The resource scope (`/msk-connect/mysql-dsql-cdc-*:*`) already
  covered the CDC log group, so only the missing action was added — no change to the
  ARN pattern. Requires a stack update to take effect on an existing deployment.

## v0.1.301

### Fixed

- **The "Sink stalled" warning no longer fires on a healthy pipeline that has just
  finished a burst of writes.** The signal added in `0.1.296` compared the source and
  sink rates on a SINGLE poll, but both are CloudWatch averages over a trailing window:
  the moment a burst ends, the source still carries its residual (plus the Debezium
  heartbeat, which is why the idle threshold is `0.1` and not `0`) while the sink has
  legitimately gone quiet with nothing left to apply. That transient is indistinguishable
  from a real stall, and it raised the red "changes are NOT reaching DSQL" alert — plus a
  `FAILURE` line in the durable activity log — on a pipeline that was replicating
  correctly (seen live while generating CDC demo load; target row counts kept rising and
  no `Commit of offsets timed out` ever appeared).

  The divergence must now hold for **3 consecutive polls** (~10–15 s at the ~5 s CDC
  poll) before it is reported: `sink_stalled` stays as the per-poll observation, and a
  new `sink_stall_confirmed` is what the UI and the activity log act on. Detection is
  unaffected — a real stall is permanent, because an ejected consumer never rejoins by
  itself — so waiting a few polls costs nothing. The streak resets as soon as the sink
  sends anything, and also when a rate becomes unknown (an unreadable metric is a
  monitoring failure, not a data one).

## v0.1.300

### Changed

- **The app stack's default image is now `0.1.299`**, published to all three registries
  (ECR Public plus the private `us-east-1` / `ap-northeast-2` repos). `0.1.298` and
  `0.1.299` only touched `deploy/cloudformation.yaml`, so no image had been built for
  them and the default still pointed at `0.1.297` — code-identical, but it left the
  deployed tag two releases behind the project version. Keeping the tag equal to the
  release removes the "which code is actually running?" question that has already cost
  time twice here (a Seoul stack 27 releases behind, and a UI showing a version 7
  releases stale).

## v0.1.299

### Fixed

- **Cognito login failed with a bare `500` on `/oauth2/idpresponse`: the ALB had no
  outbound HTTPS, so it could never complete the OIDC token exchange.** With
  `EnableCognitoAuth=true` the ALB itself calls the Cognito hosted UI
  (`/oauth2/token`, `/oauth2/userInfo`) from its own ENIs. But declaring *any*
  `SecurityGroupEgress` on the ALB security group replaces the default allow-all
  egress, and the template declared only "ALB → task on the app port" — so the login
  flow got as far as the callback and then died with `500 Internal Server Error` and
  **no app log at all**, because the request never reaches the app. (Hit live on the
  first real Cognito deploy, at the "set a new password" step.) The template now adds
  an `AlbHttpsEgress` rule (443 to `HttpsEgressCidr`), created only when Cognito is
  enabled — a no-auth ALB needs no outbound internet. Pinned by a test.

## v0.1.298

### Changed

- **The app stack's default image is now `0.1.297`** (was `0.1.287`, ten releases
  behind). `ContainerImageUri` defaults to the published ECR Public image so a normal
  deploy needs no build — which also means a stale default silently shipped old code to
  anyone who deployed without overriding it, including every fix from `0.1.288` onward.

## v0.1.297

### Fixed

- **The CDC sink's CloudWatch monitor no longer runs on the offset-commit path, where it
  could time out the commit it was only meant to observe.** Kafka Connect calls
  `SinkTask.flush()` *inside* the offset commit and bounds that commit by
  `offset.flush.timeout.ms`; the sink emitted its per-table metrics there
  **synchronously**. `PutMetricData` is a network call — and its first invocation also
  resolves credentials and endpoints, egressing through the cdc-stack's NAT gateway with
  no monitoring VPC endpoint — so a slow CloudWatch could consume the whole commit budget
  and surface as a repeating `Commit of offsets timed out`: a best-effort monitor
  degrading replication, which is exactly backwards. `flush()` now hands the window to a
  single daemon thread and returns immediately. At most one emission is in flight, so a
  slow CloudWatch cannot build a backlog of windows — and nothing is lost by skipping
  one, because each counter is read-and-cleared atomically, so skipped counts roll into
  the next window. `stop()` still emits the final window inline (teardown is not the
  commit path, and those counts would otherwise die with the daemon thread). Guarded by a
  new Java test that fails if the emission goes back to being inline.

  `PLUGIN_VERSION` → `v25` (sink jar rebuilt; the Debezium plugin and seeder zip are
  unchanged). It also carries the v24 worker-config change, so one **Delete + Deploy of
  the CDC infrastructure** picks up both.

## v0.1.296

### Fixed

- **A stalled CDC sink is now reported instead of being rendered as healthy.** With the
  source producing changes and the sink applying none, three separate panels asserted
  an all-clear — the exact reason the tester could not tell replication had stopped
  without querying target row counts:
  - **Pipeline health** said "Streaming — changes are flowing". That verdict came from
    `idle`, which is False whenever *either* rate is non-zero, so a source-only rate
    read as healthy. The divergence was already on screen (two bars, "Source poll 5.00
    rec/s" / "Sink send 0.00 rec/s") — nothing interpreted the gap. A new
    `sink_stalled` signal now names it: "Sink stalled — changes are NOT reaching DSQL",
    plus an error notice saying the connector can still report `RUNNING` while this
    happens, to check the sink log for repeating `Commit of offsets timed out`, and not
    to cut over until the send rate recovers. `idle` is deliberately unchanged — it
    gates the cut-over "drained" judgement and must keep reading False here.
  - **Stream lag** showed the green "Caught up — no replication lag in the recent
    window." The sink emits `ReplicationLagMs` only while applying, so a *dead* sink
    produces no datapoint and got the strongest possible all-clear. The same input now
    renders "No lag data because the sink is applying nothing — this is a stall, not
    being caught up."
  - **Dead-letter queue** rendered a green success band at depth 0. A stalled sink never
    reaches a record to quarantine, so 0 means "nothing was attempted", not "nothing was
    lost" — the tone is downgraded and the panel says a zero count is expected during a
    stall.

  The stall also writes a durable activity-log event (`sink stalled` FAILURE with both
  rates, and a `sink recovered` SUCCESS on the way back), fired on the state
  **transition** only so the few-second poll cannot flood the log. No new AWS calls: all
  of this is derived from metrics already being read.

## v0.1.295

### Fixed

- **The CDC sink no longer stalls permanently while reporting `RUNNING`: its consumer
  poll/session timeouts are now sized for a slow DSQL apply instead of left at Kafka's
  defaults.** A tester saw replication stop for good ~15–20 min after Start CDC — no
  new rows in DSQL — while the connector stayed `RUNNING` and the log repeated only
  `Commit of offsets timed out`. Root cause: the sink hands one `SinkTask.put()` up to
  `SinkMaxPollRecords` (3000) records, and when any row in a chunk hits a permanent SQL
  error the connector deliberately re-applies that chunk **row by row, one DSQL
  transaction each** (so the poison row is isolated and healthy rows still land). At
  ~50 ms per round-trip that single call runs ~450 s — past Kafka's
  `max.poll.interval.ms` default of 300000 (5 min) — so the group coordinator **ejects
  the consumer** and revokes its partitions, after which offsets can never be
  committed. Because the sink runs `errors.tolerance=all` and Connect only *warns* on a
  failed commit, nothing ever fails the task: it logs `Commit of offsets timed out` on
  a loop with the connector still `RUNNING` and replication dead. The sink worker config
  now sets `consumer.max.poll.interval.ms=900000`, `consumer.session.timeout.ms=60000`,
  `consumer.heartbeat.interval.ms=20000` and `offset.flush.timeout.ms=120000` (Connect's
  5 s default was also far too tight, since the commit path calls the task's `flush()`,
  which emits monitor metrics to CloudWatch over the NAT gateway). All four are exposed
  as cdc-stack parameters, and a test now pins them so they cannot silently regress.

  The template previously concluded that `consumer.*` tuning was undeployable on MSK
  Connect. That is only true of the per-*connector* `.override.` form (MSK Connect
  excludes `connector.client.config.override.policy`) — the **worker**-level allowlist
  explicitly includes `consumer.max.poll.interval.ms`, `consumer.session.timeout.ms` and
  `consumer.heartbeat.interval.ms`. The two comments saying otherwise are corrected.

  `PLUGIN_VERSION` goes to `v24` with **no artifact change**: `SinkWorkerConfiguration`
  is custom-named with the plugin version and immutable, so the new settings can only
  reach a deployed stack behind a new token — which means this fix needs a **Delete +
  Deploy of the CDC infrastructure**, not just Start CDC. Manual §7.2 documents the
  timeouts, why they differ from Kafka's defaults, and that lowering
  `SinkMaxPollRecords` bounds one `put()` at no throughput cost.

## v0.1.294

### Fixed

- **The 1 KiB key-size budget is now estimated from declared column widths and warned
  about, and the byte estimate no longer counts every string column as 255 bytes.**
  Aurora DSQL limits a primary key — and each secondary index — to **1 KiB combined**
  (error 54000 `key size too large`). Unlike the 8-column cap this is enforced on the
  **value at `INSERT`/`UPDATE`**, not on the DDL, so a `varchar(2000)` key creates fine
  and fails only for rows whose value is actually too long. Two problems:
  - Nothing warned about it. A key whose declared widths could not possibly fit was
    discovered per row, mid-migration: a too-large PK value quarantines that row in Full
    Load (dead-letters it in CDC), and a too-large index value fails the post-load
    `CREATE INDEX ASYNC`. Schema Conversion now emits a **MANUAL recommendation** naming
    each at-risk key and its worst-case size. It never blocks — a wide declared type
    holding short values migrates fine, so refusing to convert it would be a false alarm.
  - The estimator assumed a **255-byte per-column key cap that Aurora DSQL does not
    document** (it indexes `varchar` up to 65,535 bytes and `char` up to 4,096). That was
    wrong in both directions: it under-counted one wide column (a `varchar(2000)` key
    scored 255, so the composite-key picker allowed a key that cannot fit) and
    over-counted several narrow ones (5 × `varchar(10)` scored 1,275 and was rejected
    though it cannot exceed ~200 bytes). It now uses the **declared length at 4 bytes per
    character** (utf8mb4's worst case), falling back to the full budget for an unbounded
    `text`. The prefix-index warning, which cited the same non-existent 255-byte limit,
    now cites the documented 1 KiB budget and error code.

## v0.1.293

### Fixed

- **A key over Aurora DSQL's 8-column limit is now caught in Evaluation and Schema
  Conversion, instead of failing at apply — or, for an index, after Full Load had
  written every row.** MySQL allows up to 16 columns in an index; DSQL allows **8**
  (error 54011 `more than 8 column keys are not allowed`). Nothing checked this: the
  8-column constant existed only inside the optional composite-key picker's validation,
  so a source with a 9–16-column key passed Evaluation clean, converted clean, and then
  failed — a wide **primary key** rejected the `CREATE TABLE` (nothing migrated), and a
  wide **secondary index** failed its post-load `CREATE INDEX ASYNC`, i.e. at the worst
  possible moment, after a multi-hour load. Now:
  - Evaluation gains a `TOO_MANY_KEY_COLUMNS` rule — **UNSUPPORTED** when the primary key
    is over the limit (no data can land) and **MANUAL** when only secondary indexes are —
    naming each offending key with its column count, the error it would hit, and *when*.
  - Schema Conversion emits a matching note and **omits** the wide `CREATE INDEX ASYNC`
    rather than shipping DDL guaranteed to fail after the load, so the applied script is
    one that can succeed and the operator is told which indexes were left out.
  - The 24-indexes-per-table budget now counts only the indexes actually emitted, so a
    table that is over the raw count solely because of skipped wide indexes no longer
    warns about an overflow the applied script cannot hit.

  Manual §6.1 (all three languages) documents both limits — the 8-column key cap and the
  24-index-per-table cap, which was also undocumented.

## v0.1.292

### Changed

- **The deploy guide and the CloudFormation parameters now tell you how to size the
  Fargate task, instead of leaving the defaults to be discovered by an OOM kill.** The
  `ContainerCpu` / `ContainerMemory` defaults (`512` / `1024` MiB) are sized for
  evaluation, but nothing at deploy time said so — and `ContainerMemory` is a Fargate
  **hard limit**: exceed it and the kernel kills the task with no app log. The template's
  parameter descriptions (and console labels) now state that memory is a hard limit and
  which workload each size fits, and `DEPLOYMENT.md` (+ `.ko` / `.ja`) gains a **Task
  sizing** section with the valid CPU↔memory pairings and per-workload recommendations:
  0.5–1 vCPU / 1 GiB for evaluation, 2–4 vCPU / ≥ 2 GiB for a real Full Load, ≥ 8 GiB
  when loading large LOBs or raising parallelism. Manual §7.2 (`07-performance-and-tuning`,
  all three languages) replaces its understated memory note with the same OOM-aware
  guidance and links to that section. Resizing is a redeploy — Fargate does not scale a
  running task's memory.

## v0.1.291

### Added

- **Full Load now logs container memory pressure, so an ECS Fargate OOM kill is
  diagnosable instead of silent.** A tester hit an OOM kill mid-Full-Load: memory sat
  steady, then climbed to the task's hard limit in about a minute and the kernel killed
  the task — with NO app log (an OOM kill is not a graceful shutdown), leaving only a
  CloudWatch metric spike and an ELB "Request timed out". The multiprocess load's memory
  is the whole cgroup (parent + all worker processes) and can climb toward the limit when
  the per-worker read-ahead queue and in-flight write batches meet wide / oversized-LOB
  rows — none of which was metered. The parent's progress-drain thread now samples the
  container's memory cgroup (`/sys/fs/cgroup/memory.current` + `memory.max`, cgroup v2
  with a v1 fallback; a silent no-op off-Fargate, e.g. local/macOS — no new dependency):
  it logs an `INFO` line at each new memory high-water (so the run's peak is always
  captured) and, when usage crosses ~80% of the limit, a `WARNING` naming the tables
  currently loading and the remedies (lower `full_load_table_parallelism` /
  `full_load_batch_parallelism`, exclude oversized-LOB columns, or redeploy with more
  memory). The 80% crossing is also recorded on the **durable activity log** (a
  `[full_load] memory pressure` event) so it surfaces in the UI activity timeline and the
  downloadable report and **survives the task** — an OOM kill tears down the app and its
  CloudWatch worker log, but the activity log is persisted. Value-free (Property 7); the
  warning re-arms only after memory recedes below ~70%, so a run hovering near the
  threshold does not flood the log.

### Changed

- **A wrong VpcId in the CDC-infrastructure deploy now gets a message that points at the
  VPC ID, instead of misdirecting toward the region.** VpcId is the one value the tool
  cannot infer, so a typo is a common mistake — and it is already caught early (before
  any billable CloudFormation create) by the pre-flight network diagnosis, which resolves
  subnets from the VpcId. But a nonexistent VPC and a real-but-empty VPC both produced the
  same "No subnets found in VPC 'X'. Ensure the VPC is in the same region…" text, which
  implies the VPC exists and sends a user who simply mistyped the id to check the wrong
  thing. `diagnose_cdc_network` now distinguishes the two: when a VpcId resolves to no
  subnets it checks whether the VPC exists (`describe_vpcs`) and returns either "VPC 'X'
  was not found in this account and region — check the VPC ID (a typo is the usual
  cause)…" or "VPC 'X' exists but has no subnets. Add subnets (in >=2 AZs…)". A genuinely
  uncertain lookup (a permissions error / throttle on `describe_vpcs`) is treated as
  "exists" so an unrelated API failure can never masquerade as "VPC not found" — that case
  falls through to the real-VPC message and the blocking submit path still surfaces the
  API error itself. (The pre-flight gate, the deploy-time failure surfacing, and the
  `ROLLBACK_COMPLETE` delete-and-redeploy recovery were already in place — this only
  sharpens the most common wrong-VpcId message.)

### Changed

- **The CDC-infrastructure deploy time estimate is corrected from "~15-20 minutes" to
  "~10-15 minutes" to match observed runs.** Repeated live deploys finished the whole
  `infra` create (MSK Serverless provisioning-dominated) in ~10 minutes, but the deploy
  confirmation dialog, the prerequisite/overlap guidance, the provisioning banners and
  the redeploy prompt all quoted ~15-20 minutes — an over-estimate that made the wait
  read as longer than it is. All operator-facing copy now says ~10-15 minutes, and the
  per-stage progress ETA model (`_CDC_STAGE_ETA_SECONDS["infra"]["stack_create"]`) is
  lowered from 18 to 9 minutes so the stage hints and total ETA agree with the dialog.
  It remains a ballpark (AWS provisioning varies by account/region/time), so a slower
  run simply overruns the hint rather than being misreported. Teardown estimates
  (~15-25 min, a different code path with its own measured basis) are unchanged.

### Fixed

- **The oversized-LOB exclusion can no longer be edited after a Full Load already loaded
  data under it — closing a silent split-brain in the `full_load_only` → `cdc_only`
  path.** The exclusion is a single migration-wide selection shared by Full Load and CDC.
  On the Full Load screen it correctly locks once the load has run (`selection_lock_reason`'s
  `has_job or status is DONE`). But when a `full_load_only` migration completed with a
  column excluded (rows loaded with that column `NULL`) and the operator then switched the
  migration type to `cdc_only` to add replication — a path the tool itself suggests after a
  full-load-only run — the card moved to its CDC-step home, whose lock
  (`lob_exclusion_lock`) only checked CDC-streaming / infra-deployed state and had **no**
  "a Full Load already committed under this set" clause. So in the pre-deploy window the
  operator could **un-exclude** the column: CDC would then capture it for rows changed
  after the snapshot while the already-loaded rows stayed `NULL` (silent partial data) —
  or, symmetrically, **add** an exclusion for a column the load populated, so CDC dropped
  its updates and the target went stale. `lob_exclusion_lock` now takes a
  `full_load_committed` signal (the `FULL_LOAD` step is `DONE` or a job exists — both
  survive the type switch) and locks the card in **both** tick directions when a load has
  committed, naming *Start over* as the only correct re-scope (deleting the CDC stack does
  not release it — the load really ran against this set). A genuine fresh `cdc_only` run
  with nothing loaded is unaffected (still editable until deploy), and a single
  `full_load_and_cdc` run was never affected (its card stays on the Full Load screen,
  locked). Adds a regression test for the committed-load lock.

### Changed

- **The deploy template's default `ContainerImageUri` is bumped to the current release
  (`public.ecr.aws/z0q0i9j0/mysql-dsql-migrator:0.1.287`).** The default had drifted many
  releases behind the published image, so a fresh `aws cloudformation deploy` with no
  override pulled a stale app. It now points at the current published ECR Public tag.

## v0.1.286

### Added

- **The activity log now records the journey's key decisions and verdicts across every
  stage, not just the Full Load data path.** An audit of what the downloadable
  `migration_activity.log` captured found that the stages proving and concluding a
  migration were largely absent — the log recorded the load, but not whether the result
  was validated or that the operator signed off. Four gaps are closed:
  - **Validation verdict (Step 4).** A validation run now logs a `[validation] validation
    started` event (mode + table count) and a `[validation] validation completed` verdict
    — `MATCH`/`MISMATCH`, the mode (ROW_COUNT vs CHECKSUM), how many tables matched vs
    mismatched (plus errored / missing / extra where reconciled), the failing tables, and
    whether it is ready for cut-over. Logged `SUCCESS` on a clean match, `FAILURE` on a
    mismatch, so a no-go reads loud. Previously only the identity-sequence re-sync was
    logged; the verdict itself lived only in the UI.
  - **Cut-over acknowledgement (Step 5).** Clicking "I've cut over" now logs a
    `[validation] cut over acknowledged` event naming the release state the operator
    signed off on — a clean match, or an explicitly ACCEPTED gap (rows permanently
    dropped and knowingly migrated without). The migration's conclusion was previously
    unrecorded.
  - **Schema apply run summary (Step 2).** In addition to the existing per-object lines,
    a schema apply now logs `[schema_conversion] schema apply started` and
    `schema apply completed` with a roll-up — "N of M object(s) applied (C created, S
    skipped), F failed" — mirroring Full Load's run started/completed bracketing.
  - **Assessment start + migration type.** Evaluation now logs a `STARTED` "run
    assessment" event (it previously logged only success/failure), and the migration-type
    choice (Full Load only / CDC only / both) is logged as `[full_load] migration type
    selected` at the point it is chosen — only on an actual change, so a refresh never
    re-logs it.

  All events are value-free (counts, modes, and table names — never a row value,
  Property 7).

### Added

- **The Full Load watermark (the gapless CDC-handoff consistency point) is now recorded
  in the activity log.** The watermark pins the exact source position the snapshot
  reflects — the coordinate a later CDC catch-up resumes from — but it was persisted only
  on the in-memory job record, so the downloaded `migration_activity.log` (the artifact
  teams attach to a change ticket) had no record of which source point-in-time the
  migration captured. A Full Load now emits a `[full_load] watermark captured` event at
  `INFO` right after the snapshot: the GTID when present (else the `binlog_file:position`,
  else a plain "no coordinate available" when binary logging is off/restricted), the
  snapshot UTC timestamp, the number of tables counted, and whether those counts are
  approximate `information_schema` estimates. A retry logs the original watermark it
  resumed against too, so the audit trail is complete across retries. The event carries
  only a log position and a timestamp — never a row value (Property 7).

### Added

- **An excluded oversized-LOB column is now recorded in the activity log — an intentional
  data omission belongs in the migration's audit trail.** Ticking a column in the
  "Oversized LOB columns" card drops that column's data from the Full Load (it arrives
  `NULL` on the target), but nothing was written to the activity log, so the downloaded
  `migration_activity.log` had no record that a column was left out on purpose — a
  governance gap, since the log is the artifact teams attach to a change ticket. Row-level
  quarantine was already logged; column-level exclusion now is too. A Full Load run (and a
  retry, scoped to the tables it re-ran) now emits one `[full_load] column excluded` event
  per excluded column at `INFO` (an expected, user-chosen omission — not a fault), naming
  the `table.column` and stating the column is left `NULL` on the target. The affected
  table's `load table` line also echoes it — e.g. `15 rows newly loaded (1 column
  excluded: content)`. The event names only the column (no row values — Property 7).

### Changed

- **A DSQL target-side Full Load failure now explains what to do next, not just the raw
  driver text (audit finding U6).** The per-table failure path only appended a
  source-side hint (dropped connection, too-many-connections), so a *target* error — an
  optimistic-concurrency budget exhaustion (`40001`), a per-table structural limit
  (`54000`, e.g. >24 indexes), or a constraint / data rejection (`23xxx` / `22xxx`) —
  reached the error log, activity log, and inline per-table message as a bare driver
  message. A new `target_error_hint` keys off the SQLSTATE (falling back to recognizable,
  already value-free message text) and appends the same "what happened / what to do next"
  guidance the source path gives — e.g. "OCC retries exhausted → lower parallelism/batch
  size and re-run; the load is idempotent". No row values are ever surfaced (Property 7).
- **The `KEEP_INTEGER` primary-key recommendation now warns that DSQL will not
  auto-generate the key after cut-over (audit finding U5).** Keeping the integer key from
  an `AUTO_INCREMENT` column converts cleanly, but Aurora DSQL puts no identity/default on
  it — so an application that relied on the database generating the key will fail or
  collide on insert after cut-over. The Schema Conversion message now says this plainly
  and points to the "Server-generated (IDENTITY)" strategy for callers that want DSQL to
  fill the key, instead of only offering the throughput note.

## v0.1.282

### Fixed

- **A source `CHECK` constraint is no longer silently dropped — it is now surfaced in
  Evaluation (audit finding).** The introspector never reflected CHECK constraints, so a
  table whose only DSQL-relevant feature was a CHECK was classified `AUTO` / "no
  compatibility issues" while the constraint was lost on the target. CHECK constraints
  are now reflected (`TableDef.check_constraints`, via SQLAlchemy's
  `get_check_constraints`) and a new `CHECK_CONSTRAINT_DROPPED` assessment rule flags
  such a table `MANUAL`, naming the constraint(s) and recommending re-creating the CHECK
  on the target by hand (where its expression is DSQL-compatible) or enforcing it in the
  application. The converter deliberately does not auto-translate an arbitrary CHECK
  expression (a MySQL expression can use functions/operators that differ in DSQL, so a
  blind copy risks invalid DDL) — the goal here is completeness: never drop a
  source-enforced constraint without surfacing it (Property 8).

## v0.1.281

### Fixed

- **Schema-conversion accuracy fixes (audit findings C11, U1, U2, U3).**
  - *C11 — `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` timezone.* A MySQL `TIMESTAMP` maps to
    DSQL `timestamptz`, but the `CURRENT_TIMESTAMP` default was wrapped as
    `now() AT TIME ZONE 'UTC'` — a naive value a `timestamptz` column re-interprets in
    the session TimeZone, shifting a defaulted insert by the session's UTC offset. The
    naive-UTC wrapper now applies ONLY to the DATETIME → plain `timestamp` target;
    `timestamptz` keeps the plain instant.
  - *U1 — `TINYINT(1) UNSIGNED` mislabelled.* Evaluation flagged it as "mapped to
    boolean" (warning of a 0/1-load failure that never happens), but the converter maps
    it to `smallint`. The assessor now excludes the unsigned form, matching the
    converter.
  - *U2 — `TIME` range.* MySQL `TIME` is a duration (−838:59:59..838:59:59); DSQL `time`
    is a time-of-day. An out-of-range value failed per-row mid-load with no earlier
    signal. Schema Conversion now warns up front (remap to interval/text if the column
    stores durations), matching the ENUM/BIT/YEAR pattern.
  - *U3 — 24-index cap off by one under COMPOSITE_KEY.* The per-table index-limit warning
    ignored the extra UNIQUE index the COMPOSITE_KEY strategy adds, so a table with 23
    source indexes converted with a composite key produced 25 indexes (> DSQL's 24) yet
    passed the pre-apply check and failed the extra `CREATE INDEX ASYNC` after the load.
    The warning now counts the conversion-added index.

## v0.1.280

### Fixed

- **The CDC offset-seeder no longer rewinds an advanced connector at the binlog
  filename rollover (audit finding C9).** The no-clobber guard that skips re-seeding a
  connector already at/past the watermark compared binlog file names
  *lexicographically*. MySQL binlog suffixes are zero-padded but WIDEN at rollover
  (`mysql-bin.999999` → `mysql-bin.1000000`), where a string compare inverts
  (`'1000000' < '999999'`) — so a genuinely-advanced connector was mis-classified as
  behind and rewound on a re-deploy, replaying already-streamed changes. The guard now
  compares the parsed numeric binlog sequence (falling back to lexicographic only for an
  unparseable suffix). The offset-seeder Lambda zip is rebuilt and `PLUGIN_VERSION` is
  bumped to `v23` accordingly (a `PLUGIN_VERSION` bump requires a Delete + redeploy of
  CDC infrastructure to take effect).

### Notes

- Audit finding C10 (composite-key re-keying could duplicate a row if the leading
  column is mutated) needed no code change: the composite-key strategy already emits a
  UNIQUE index on the original primary key, so a mutating update surfaces as a
  unique-violation / DLQ event rather than a silent duplicate, and the immutability
  requirement is already warned at opt-in time.

## v0.1.279

### Fixed

- **The Full Load batch byte-cap now holds for a size-skewed batch (audit findings
  C7/P1).** A batch is split so a single write transaction stays under DSQL's 10 MiB
  limit. The splitter sampled only the FIRST row of each batch and, if that
  extrapolation was under budget, skipped the per-row byte check for the rest of the
  batch — so a batch whose first row was tiny (empty/NULL text) but whose later rows
  were large (BLOB/JSON) accumulated far past the cap. It was recovered downstream by a
  costly recursive split, but the advertised cap was silently unenforced. The splitter
  now keeps a true running byte sum (every row estimated once and added) and flushes the
  moment the next row would exceed the budget; a single oversized row still forms its
  own batch. Memory stays bounded (one batch at a time).

## v0.1.278

### Fixed

- **Validation soundness: several ways a "match" could be reported over non-identical
  data are closed or disclosed (audit findings C1–C6, U4).**
  - *Checksum token collisions (C4).* Each column value is now escaped (`~`→`~~`,
    `|`→`~|`) before it is joined with the `|` separator, so a value containing `|` can
    no longer shift a delimiter across a column boundary (`CONCAT_WS('|','a|','b')` and
    `CONCAT_WS('|','a','|b')` both used to yield `a||b`). The NULL sentinel is now the
    un-forgeable `~N` (a real escaped value can never produce it), closing the old
    literal-`<NULL>`-vs-SQL-NULL collision. The escaping is byte-identical on both
    engines and backslash-free (a backslash scheme diverged between MySQL and PG
    before).
  - *DATETIME timezone (C3).* The plain-`timestamp` (DATETIME) checksum term is now
    rendered directly instead of via `AT TIME ZONE 'UTC'`, which is NOT a no-op on
    `timestamp without time zone` (it converts through the session TimeZone and shifts
    the wall-clock); `timestamptz` still uses it. The DSQL connection also pins
    `TimeZone=UTC` so the comparison no longer depends on an unpinned session default.
  - *Honest labels/disclosure (C5, C1/C2, C6, U4).* The readiness check is labelled
    "Row counts match" in ROW_COUNT mode (it does not compare column values) and only
    "Data identical" in CHECKSUM mode, which now also discloses that FLOAT/DOUBLE and
    JSON columns are not value-compared. The PK-reconciliation check is renamed "No
    missing or extra records" (it verifies the key set, not row-value equality), and the
    composite/non-integer-PK footnote states "row count only" in ROW_COUNT mode instead
    of overstating "count/checksum".

## v0.1.277

### Fixed

- **`FLOAT UNSIGNED` / `FLOAT(M,D) UNSIGNED` columns no longer abort the whole table's
  conversion.** sqlglot's MySQL dialect cannot parse `float unsigned` as a standalone
  type, so a table containing one fell to the "could not auto-convert" placeholder (no
  `CREATE TABLE`) — while Evaluation still rated it AUTO/compatible, a contradiction the
  operator could not diagnose (audit finding B1). Unsigned-ness is not representable on an
  approximate numeric and carries no storage meaning, so it is now stripped and the column
  maps to `real` — the same treatment `DOUBLE UNSIGNED` already gets. Integer `UNSIGNED`
  (range-widened) and `DECIMAL UNSIGNED` are untouched.
- **MySQL prefix indexes (`KEY (col(N))`) are now surfaced instead of silently becoming a
  full-column index that fails after the load.** Aurora DSQL has no prefix-index
  equivalent, so the converter indexes the whole column; a variable-length column whose
  full value exceeds DSQL's ~255-byte index-key limit then fails `CREATE INDEX ASYNC`
  *after* the table and its data are already loaded, with no earlier signal (audit finding
  B2). The prefix length is now reflected from the source (`IndexDef.prefix_lengths`) and
  Schema Conversion emits a warning naming the index and column, so the operator can
  confirm the values fit — or replace it with an expression index on a bounded substring —
  before a multi-hour load. The index DDL is still emitted (the warning is advisory).

## v0.1.276

### Fixed

- **Full Load no longer reader-shards a table that has nothing to reconcile a torn read
  — closing a cross-shard torn-read data-loss window on the production path.** Reader
  sharding splits a large table's read into K disjoint PK ranges streamed concurrently,
  each opening its OWN independently-timed `START TRANSACTION WITH CONSISTENT SNAPSHOT`.
  If the source is written to during the load, a multi-row source transaction can be torn
  across shards (one row in shard A's snapshot, its sibling not yet in shard B's). That is
  only safe when a CDC stream will reconcile the post-snapshot write; a clean **replace**
  (plain INSERT, no CDC) or a **non-CDC append** has nothing to reconcile it, so it must
  be read by a single reader (one snapshot = one point-in-time cut). The single-process
  path guarded the replace case but still allowed sharding a non-CDC append, and — more
  seriously — the **multiprocess path (the production default whenever table parallelism
  > 1)** decided sharding purely on "has a single integer PK", ignoring both the
  replace/CDC state and the `full_load_reader_shards` off-switch/ceiling, so it sharded
  replaces and non-CDC appends outright (audit finding D1, plus C12). Both paths now shard
  **only** when the load is CDC-coexisting, and the multiprocess planner derives its shard
  count from the clamped `full_load_reader_shards` (bounded by the source-connection
  ceiling) instead of the worker-pool budget. Non-sharded loads are unchanged.

## v0.1.275

### Fixed

- **A failed identity-sequence sync is no longer reported to the operator as success.**
  Before cut-over the tool advances each `GENERATED BY DEFAULT AS IDENTITY` sequence past
  the migrated rows (`RESTART WITH max+1`) so the application's first insert can't hit a
  duplicate key. That `ALTER` is a DDL under DSQL's optimistic concurrency (can raise
  40001 / concurrent-DDL) and the IAM token can expire mid-session — but a failure was
  swallowed (`except: pass`) and became indistinguishable from "this table has no
  server-generated key", so the cut-over runbook painted it with the green *"Done — no
  server-generated key needed advancing."* line. An operator could then repoint the
  application onto a lagging sequence and hit the exact duplicate-key outage the sync
  exists to prevent (audit finding D2). `sync_identity_sequences` now returns a distinct
  per-table result — `int` (advanced) / `None` (nothing to do) / `str` (the `RESTART`
  **failed**, with a value-free reason) — split by a new `partition_identity_sync`
  helper. Every caller (the cut-over button, the validation auto-sync, and the Full Load
  / accept-quarantine post-load sync) now surfaces a failure: the runbook shows an
  **error** notice ("Identity sequence sync failed — do not cut over yet", with the retry
  / manual-`RESTART WITH` remedy) instead of the success line, and the activity log
  records it at `FAILURE`. The identity-sync setup-exception messages are also run through
  the value-free sanitizer (Property 7), consistent with v0.1.274.

## v0.1.274

### Fixed

- **Full Load error logging no longer writes the failing row's column values to disk
  (Property 7).** A driver (psycopg) error's `str()` keeps the server `DETAIL:` /
  `Failing row contains (...)` line, which carries the offending row's column values
  (e.g. a duplicate email, a token). That raw text was stored verbatim in the
  quarantine record, the per-table failure message, the durable NDJSON activity log,
  and (mirrored) CloudWatch. It is now passed through a single-line sanitizer
  (`safe_error_message`) at every load-failure site — the quarantine record, the batch
  outcome's `first_error`, and the per-table failure handler — which keeps the
  actionable primary message (`duplicate key value violates unique constraint "…"`)
  and the SQLSTATE while dropping the value-bearing `DETAIL` line. The prior
  `" ".join(str(exc).split())` collapse did NOT help — it merely folded the DETAIL
  line onto one line with the values intact. The DEBUG-only activity-log stacktrace is
  fixed the same way: it now keeps the (value-free) stack frames plus a
  `Type: first-line` tail instead of `format_exception`'s value-bearing message line.

## v0.1.273

### Changed

- **The oversized-LOB exclusion now explains why it is locked once CDC infrastructure
  is deployed, instead of freezing silently.** Previously, once the cdc-stack existed
  (including after a Stop CDC, which keeps the stack and its committed MSK offset), the
  exclusion tick-boxes were disabled with no message — a stopped-CDC operator saw a
  frozen box and no reason, which reads as a bug. The lock itself is correct and stays:
  changing which columns are excluded on a pipeline that has already streamed would
  leave already-migrated rows inconsistent with rows processed after (the resume offset
  survives a Stop), so the exclusion is fixed for the life of the infrastructure. The
  box now names that reason and the only safe remedy — delete the CDC infrastructure and
  redeploy with the new set — mirroring how the table picker locks on the same stack
  phase. The reason renders in a neutral tone (a deployed pipeline is an expected state,
  not a warning). The CDC start point and table selection behaviors are unchanged.

## v0.1.272

### Added

- **Oversized-LOB column exclusion is now migration-wide — Full Load honors it too,
  not just CDC.** The opt-in "Oversized LOB columns" control (which drops a MySQL
  `mediumtext`/`longtext`/`mediumblob`/`longblob` column whose values can exceed Aurora
  DSQL's ~1 MiB per-value limit) previously fed only CDC capture (Debezium
  `column.exclude.list`). It is now a single, migration-wide selection: a ticked column
  is dropped from **both** the Full Load INSERT column list and CDC capture, so the two
  data paths can never disagree across the gapless handoff (a column excluded from one
  but not the other would leave silent partial data). For any migration that includes a
  Full Load, the card now renders on the Full Load screen **right after table selection
  and before the prerequisite check**, so the operator can exclude a column before the
  load that would carry it. CDC-only keeps the card in the CDC sub-flow (with its
  `column.exclude.list` preview). The exclusion is applied by dropping the column from
  the effective table before streaming — the exporter's `SELECT` list and the importer's
  `INSERT` list both derive from it — so the source is never read for the column and the
  target is never written; the target schema still keeps the column (recreated from the
  applied DDL), which simply takes its default/NULL. A **primary-key column is never
  excluded**, and the **loadability prerequisite now evaluates the post-exclusion column
  set**: excluding a column that is `NOT NULL` with no default on the target correctly
  fails the pre-load gate (it can no longer be filled) instead of failing every batch
  mid-load. The card stays **editable right up until the load is committed** (it locks
  on the same points as the table picker — a Full Load has started, CDC is streaming, or
  CDC infra is deployed — not merely because the read-only checks have run); if the
  exclusion is changed after the checks pass, the Run button blocks with a "re-run the
  prerequisite checks" prompt naming the newly-excluded column, so a stale pass can never
  start a load against an unchecked column set (the same asymmetric guard the table
  picker uses — un-excluding a column is never blocked). The default is unchanged —
  nothing is excluded unless you tick it, and an oversized single value is still
  quarantined per-row.

### Fixed

- **The "Start Full Load" confirmation dialog now opens on the first click.** It was
  built and opened in the same client update, so Quasar's `QDialog` never saw the
  `false→true` transition it needs to animate open and the first click was silently
  dropped — only a second click (with the element already registered) showed it. The
  open is now deferred one tick, so the dialog appears on the first click.
- **The "Change migration type" jump link now has the same outlined border as the other
  navigation buttons** (Back / Next / Continue), instead of rendering flat and
  border-less.
- **The pre-"Start Full Load" target probe is much faster (fewer DSQL connections).** It
  read each selected table's real primary key on its own connection, so an N-table
  selection opened N+1 connections — and every DSQL connect mints an IAM token and does a
  (cross-region) TLS handshake (~1s each), which is what made the confirm dialog take
  several seconds to appear. The primary keys are now read for all tables over a **single
  connection** (new `target_primary_keys`), cutting the probe to 2 connections regardless
  of table count. Measured against a 7-table schema in Seoul: the PK read dropped from
  ~9.5s to ~2.8s.

## v0.1.271

### Changed

- **The pre-cut-over identity-sequence sync is now an explicit "Sync identity sequences"
  button in the Cut-over runbook, not an automatic action on render.** v0.1.270 ran the
  sync as a side-effect of the Cut-over screen rendering — viewing the page issued
  `ALTER TABLE … RESTART WITH` to the target without any click, which is a poor design
  (a read/view causing a write) and out of step with this step's principle that cut-over
  is the operator's explicit act. The runbook now shows a **"Sync identity sequences"**
  button, placed just before the repoint step, that the operator clicks after the final
  drain/reload and before repointing. It advances every identity key past the current
  target `MAX(pk)` (idempotent, safe to re-click), runs in the background so the click
  never blocks, and shows the outcome (which sequences advanced, or "nothing needed
  advancing"). This keeps the safety net v0.1.270 aimed for — reaching the runbook and
  pressing one button covers rows CDC delivered after the last Validation, with no
  dependence on remembering to re-validate — while removing the render-time target write.
  (v0.1.270 was never deployed; this supersedes it.)

## v0.1.270

### Fixed

- **Opening the Cut-over runbook now re-syncs identity sequences one last time, so a
  clean cut-over no longer depends on remembering to re-validate first.** Validation
  already re-syncs a `GENERATED BY DEFAULT AS IDENTITY` sequence past the current
  `MAX(pk)` (v0.1.266), but only when the operator runs it after the final CDC drain.
  Rows CDC delivered *after* that last validation would leave the sequence lagging, and
  the app's first insert after cut-over would collide (duplicate key, SQLSTATE 23505) —
  the counts/checksums still match, so it stays silent until after cut-over. The Cut-over
  step now runs a best-effort identity-sequence re-sync when its runbook opens (release
  clean or accepted) — the last point the tool controls before the operator repoints the
  app, and the point at which the target `MAX(pk)` is final. It runs once per verdict (a
  re-validation re-arms it), in the background over the validated tables, keyed off the
  current `MAX(pk)`; the target catalog's `is_identity` filter skips non-identity tables.
  Reaching the runbook is now sufficient — no dependence on running Validation again.
  (Chosen over syncing at the "I've cut over" acknowledgement, which fires *after* the
  app is already live and so would be too late to prevent the first collision.)

## v0.1.269

### Fixed

- **Accepting a quarantine gap now also syncs the identity sequences — closing the hole
  v0.1.268 left for the accept-after-load flow.** v0.1.268 synced identity sequences when a
  load *ran* with the gap already accepted, but the normal workshop flow is the reverse:
  the load runs first (nothing accepted yet, so `_finalize_run` sees it as incomplete and
  skips the sync), the operator then clicks **"Accept quarantined rows & continue"**, which
  marked the step done and unblocked CDC but never triggered a sync. So a `GENERATED BY
  DEFAULT AS IDENTITY` key was left at `nextval` = 1 while migrated ids were already present,
  and the app's first insert after cut-over collided (duplicate key, SQLSTATE 23505) —
  reproduced in a live event even on 0.1.268. The accept action now submits a background
  identity-sequence sync over the migration scope, keyed off the current target `MAX(pk)`
  (quarantined rows are permanently dropped, so it is final); the target catalog's
  `is_identity` filter skips non-identity tables. Together with the load-time sync
  (clean/accepted-at-load) and the validation re-sync (v0.1.266), every path that completes
  a load now advances the sequence.

## v0.1.268

### Fixed

- **Identity sequences are now synced after an accepted-quarantine Full Load, not only
  after a perfectly clean one.** The post-load identity-sequence sync (which advances a
  `GENERATED BY DEFAULT AS IDENTITY` key past the loaded `MAX(pk)`) ran only on the
  fully-clean completion path. If any row was quarantined — permanently dropped, e.g. a
  value over DSQL's ~1 MiB per-value limit — the run finished via the "quarantine
  accepted" path, which returned **without** syncing, leaving the sequence at its start
  (`nextval` = 1). After cut-over the application's first auto-insert then collided with a
  migrated id (duplicate key, SQLSTATE 23505). Quarantined rows are permanently dropped
  and never backfilled, so the current `MAX(pk)` is final and syncing off it is correct —
  the sync gate is now `real_failed == 0` (accepted-quarantine included) rather than
  strictly clean. A genuinely partial/failed load (`real_failed > 0`) still skips the sync,
  since a later retry can still add rows. This complements the v0.1.266 re-sync on
  validation: previously an accepted-gap load that skipped validation and went straight to
  cut-over lost that safety net; now the sync happens at load time regardless.

## v0.1.267

### Fixed

- **Converting a `bigint unsigned` AUTO_INCREMENT key to a server-generated IDENTITY now
  warns that its range is narrowed, instead of doing it silently.** Aurora DSQL identity
  columns must be `bigint`, but `bigint unsigned` maps to `numeric(20,0)` to preserve its
  full `0..2^64-1` range — so making it an identity narrows it to `bigint`'s `0..2^63-1`.
  Newly generated ids are unaffected, but any *existing* source value above 2^63-1
  (9223372036854775807) would no longer fit and that row would fail Full Load with a
  numeric-out-of-range error (SQLSTATE 22003) — a real, silent-until-load failure. The
  conversion now emits a distinct LOSS warning naming the exact threshold and the safe
  alternatives (keep the source PK, or use a UUID key), shown in the Schema Conversion
  "gaps" section rather than mixed in with throughput advice. The lossless widenings
  (`int`/`bigint`/`int unsigned` → `bigint`) are unchanged and emit no such warning, and
  keeping the source PK leaves `bigint unsigned` as `numeric(20,0)` with its full range.

## v0.1.266

### Fixed

- **Identity (AUTO_INCREMENT) sequences are now re-synced on validation, closing a
  post-cut-over duplicate-key gap on Full Load + CDC migrations.** For a `GENERATED BY
  DEFAULT AS IDENTITY` key, both Full Load and CDC insert *explicit* ids — and an explicit
  id does not advance the identity sequence. Full Load already re-synced the sequence at
  load time, but CDC keeps inserting afterwards, so by cut-over the sequence again lagged
  the real `MAX(pk)` and the application's first auto-insert after cut-over could fail with
  a duplicate key (SQLSTATE 23505). This was the worst failure shape — row counts and
  checksums MATCH, so Validation passed clean, and it only surfaced after cut-over once the
  source was frozen. A full validation run now re-runs the idempotent
  `ALTER TABLE … ALTER COLUMN … RESTART WITH max(pk)+1` for every identity table (via the
  target catalog's `is_identity`, so non-identity tables are untouched) — validation is the
  step operators reach right before cut-over, after CDC has drained, which is exactly when
  the target `MAX(pk)` is final. When any sequence is advanced the result shows an
  "Identity sequences advanced for cut-over" notice with the new `RESTART WITH` values, and
  the CDC cut-over runbook's final "re-run validation" step now states that this is what
  advances them. The comparison itself stays read-only; the sequence advance is a separate,
  reported target write that never fails the report.

## v0.1.265

### Added

- **The Schema Conversion primary-key picker now offers "Server-generated (IDENTITY)"
  for AUTO_INCREMENT tables.** The converter already implemented the identity strategy
  (`BIGINT ... GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)`) end-to-end — including the
  post-Full-Load identity-sequence sync — but it was reachable only by hand-editing the
  target DDL. For a single-column AUTO_INCREMENT key the picker now shows a third tile
  that applies it in one click, with a caveat that the key gains gaps / loose ordering
  (each DSQL node draws its own value block) while Full Load still keeps the source ids.
  This is the faithful port of AUTO_INCREMENT and keeps the data path intact (`BY DEFAULT`
  lets the loader insert the source's own ids). The tile is offered only where it applies
  (a single-column AUTO_INCREMENT key) and the choice is stored in the same edited-DDL
  field the composite-key choice uses, so it is resume-safe with no separate state.
- UUID is intentionally **not** offered as a picker tile: the UUID strategy retypes the
  key column to `uuid`, which the tool's Full Load cannot populate from the source's
  integer ids (an int→uuid insert fails), so surfacing it here would steer a data
  migration into a broken load. It remains reachable via manual DDL edit for a
  schema-only / greenfield case.

## v0.1.264

### Changed

- **The Validation "Validating" card no longer shows a "Migration type" row.** Validation
  is a pure source-vs-target comparison — the engine (`validator.validate`) takes no
  migration type and behaves identically however the rows arrived (Full Load vs CDC), so
  the type never changed what this screen does. Worse, a session records only the
  last-chosen type, so a Full Load → CDC-only run displayed just "CDC only", which
  misrepresented what was actually migrated. The type is already conveyed on the Data
  Migration step and the migration-type banner, so it was removed here. (The cut-over
  runbook and drift verdict still branch on whether CDC is in use — that logic is
  unchanged.)

## v0.1.263

### Changed

- **Removed two small between-section captions on the Validation result that read as
  noise.** (1) The "For a definitive zero-loss verdict, quiesce the source first" alert
  no longer appears in the gap-recovery card — whether the source drifted since the
  snapshot, and the freeze-source-writes / let-CDC-drain-then-re-validate guidance, is
  the whole subject of the dedicated "Source changes since the comparison" section, so
  carrying it in the recovery card too was a cross-section duplicate. (2) The "Changing
  options applies on the next run — use Re-run (top right) to apply them." line under the
  options block is gone: the options block is plainly a pre-run config area and the
  Re-run button is always visible. (3) The "Completed in Xs" run-duration caption under
  the verdict is gone. The greyed-out "Options apply to the next run." note shown while a
  run is in flight stays — it explains why the toggles are inert mid-run.

## v0.1.262

### Fixed

- **The Validation per-table results table now shows the match / mismatch / ERROR badge in
  the Result column instead of always showing a dash (—).** The Result column sorts
  failures-first on an integer key (`result_sort`), but the colored-badge slot was reading
  that column's field value — the integer — as if it were the `{text, color}` badge payload.
  An integer has no `.text`, so every row fell through to the "—" placeholder. Result now
  has its own cell slot that reads the badge payload off the row directly, while the column
  keeps sorting on `result_sort` (failing/errored rows still sort to the top). The
  row-count and checksum columns are unchanged.

## v0.1.261

### Changed

- **The Cut-over readiness card no longer repeats the same explanation three times.** When
  the gap is fully explained, the card's lead-in already says every "Heads-up" item below
  is the rows the migration reported dropping — yet the "Data identical" and "No mismatched
  records" checks each re-appended "…is exactly the N rows dropped during the migration —
  already reported, not new data loss". The per-check tail is now suppressed when the
  lead-in covers it (fully explained), leaving each check with just its numbers. A
  partially-explained run has no lead-in, so the checks keep the tail there to carry the
  cause themselves. Cross-section repetition (verdict, per-table, etc.) is left as-is —
  that redundancy keeps each section independently readable.

## v0.1.260

### Changed

- **The "quiesce the source first" caveat no longer promises a clean match on a
  permanent gap.** The caveat (shown under live source drift) applies to both recovery
  branches, but its tail — "re-validate — a clean match then truly means no data was
  lost" — is only true for an unexplained, loadable gap. For a fully-explained gap those
  rows exceed a permanent Aurora DSQL limit and stay absent whatever you do, so freezing
  and re-validating can never reach a clean match; the promise contradicted the same
  card's "these rows can't be stored as-is — shrink the value or accept the gap". The
  tail is now branch-aware: fully-explained → "re-validate to confirm no other rows
  drifted in — the explained gap will remain until you shrink those values or accept it";
  unexplained → the original clean-match wording, which is the right goal there. The
  drift gate itself is unchanged.

## v0.1.259

### Changed

- **The recovery section's title and icon now match its content for an acceptable gap.**
  v0.1.258 branched the section's body on whether the gap is fully explained, but the
  heading stayed "How to recover" with a wrench icon. For a fully-explained gap the rows
  can't be stored at all and accepting them is a legitimate final choice, so "recover" /
  wrench framed a decision as a repair — fighting the verdict banner and the section's own
  "shrink the value or accept the gap" body. The fully-explained case now uses the Cut over
  step's exact heading and icon, "Acknowledge the known gap" / fact_check, so the two
  screens speak with one voice; a genuinely unexplained (loadable) gap — including the
  mixed case where one table is explained and another is a real loss — keeps "How to
  recover" and the wrench.

## v0.1.258

### Fixed

- **The "How to recover" section sent an acceptable (fully-explained) gap through the wrong
  fix.** When every missing row is one the migration already reported dropping — a value
  over a permanent Aurora DSQL limit (e.g. the ~1 MiB per-value cap) — re-running Full Load
  just isolates it again, yet the recovery section rendered "Re-run Full Load + CDC to
  backfill the gap. The Full Load only fills missing rows" plus the Stop-CDC → reload →
  resume runbook. That is false for these rows and contradicted the verdict banner directly
  above it. The section now branches on whether the gap is fully explained: for a
  permanently-quarantined gap it says the rows can't be stored as-is and gives the two real
  paths — reduce the source value below the limit (e.g. move a large object to Amazon S3)
  then reload, or accept the gap and cut over — and omits the reload runbook. A genuinely
  unexplained (loadable) gap, including the mixed case where one table is explained and
  another is a real loss, still gets the reload runbook.
- **The verdict banner body carried the same "reload reaches a full match" implication.**
  Its remediation now leads with "reloading alone will not help, since DSQL still cannot
  store the original values" before pointing to reducing the source value or accepting the
  gap, matching the recovery section and the Cut over step.

## v0.1.257

### Changed

- **The "quiesce the source first" caveat in the validation recovery section now shows
  only when the source has actually drifted.** "How to recover" is the no-go section: the
  outstanding issue is a real, unexplained mismatch, and the fix is the ordered
  Full-Load-reload steps. Advising the reader to freeze the source belongs here only when
  live drift means part of the mismatch may be in-flight (so a reload could chase a moving
  target). The notice used to render unconditionally — with an `info` fallback even when
  nothing had drifted — adding a generic cut-over aside to a screen about fixing a concrete
  gap. That fallback is dropped; the source-changes section and the Cut over step already
  carry the quiesce guidance for the no-drift case, so this removes a duplicate, not the
  advice.

## v0.1.256

### Changed

- **Validation verdict no longer calls an acceptable gap "blocked".** When every
  difference is exactly the rows the migration already reported dropping, the tool
  classifies the state as *acceptable* (cut-over proceeds once the gap is acknowledged),
  yet the amber verdict header read "Cut-over blocked only by rows dropped during the
  migration". "blocked" is a red-tier, full-stop word — it contradicted the tool's own
  gate and read as more severe than the actual red "Not ready" verdict, and it clashed
  with the Cut over step, which calls the same state "Every difference is explained". The
  header now names the decision — "Every difference is explained — accept the gap or fix
  the source and reload" — matching the Cut over step so the two screens read as one
  situation. The green "Ready for cut-over" and red "Not ready for cut-over" headers are
  unchanged.
- **The Cut-over readiness panel now leads with a line tying it back to the verdict.**
  Since v0.1.255 the panel renders last (after the evidence), so a "Heads-up" row could
  read as a new, weaker signal. When the whole difference is the known dropped rows, the
  panel now opens with "Same conclusion as the verdict above — nothing unexplained. Each
  'Heads-up' item below is the same rows the migration already reported dropping, not a
  new problem." (Shown only when nothing is unexplained.)

## v0.1.255

### Changed

- **Validation section order now leads with the evidence, not the roll-up.** On a no-go the
  page showed "How to recover" and then the "Cut-over readiness" checklist *above* the
  comparison results that justify them, so the fix and the summary arrived before the "why".
  The verdict (top banner) and its recovery advice stay together as the headline answer, but
  the readiness checklist — which is summarised from the evidence — now renders after it:
  verdict → how to recover → tables needing attention → per-table results → orphan records →
  source changes → **cut-over readiness** → export. Reading top-to-bottom now answers "can I
  cut over?", "what do I do?", "why?", then the final tally.

## v0.1.254

### Fixed

- **The migration-type banner showed "CDC only" on Validation/Cut over after a Full Load.**
  The banner rode the "one journey" header on every step, but a session has a single
  migration type — so the guided post-Full-Load path (run Full load only, then switch to
  "CDC only" to stream onto the loaded target) left later steps asserting "Migration type:
  CDC only" with a blurb reading "no Full Load in this session", contradicting what the user
  had just done, on screens with no way to correct it. The banner now appears only on the
  Data Migration step, right beside the selector that owns the choice, where the label is
  always current (Full load only while that runs, CDC only once switched). The journey
  stepper still carries cross-step continuity on every step.

## v0.1.253

### Fixed

- **The seeder-ENI wait during a CDC delete still left the log silent for ~18 minutes.**
  v0.1.251 added the reporting but logged only when the interface count *changed*, so an
  observed teardown went "MskCluster DELETE_COMPLETE" (02:46) → nothing → "Seeder network
  interfaces released." (03:05). The operator learned what was happening only after it
  finished — the same "looks frozen" symptom the reporting was meant to cure. The wait is
  now re-reported every ~2 minutes while the count is unchanged, carrying elapsed minutes
  ("Waiting for AWS to reclaim 2 seeder network interfaces — 6 min so far"), so a long
  reclamation reads as progressing. Rapid polls inside that window still emit one line, so
  the stack events are not drowned.

## v0.1.252

### Fixed

- **Deleting the CDC infrastructure left the pipeline looking like it was still streaming.**
  Pressing "Delete CDC infrastructure" started the teardown, but CloudFormation does not
  remove the connectors instantly — so discovery kept reporting both as RUNNING for the
  whole ~20 minute delete. The card body said "Deleting infrastructure" while a green
  **"Streaming"** badge sat next to it, and the Live status panel, its dead-letter queue,
  and the per-table migration status table all stayed on screen showing replication figures
  for a pipeline being dismantled. A teardown the operator asked for now outranks a
  connector state that is only true for another minute: the badge reads "Deleting…" (or
  "Stopping…" for Stop CDC, which leaves the MSK cluster behind), and the three monitoring
  views hide until the teardown finishes. They share one visibility predicate so they cannot
  diverge, and a Start CDC in flight still shows them — that ramp is when they matter most.

## v0.1.251

### Added

- **The CDC infrastructure delete now reports seeder network-interface reclamation in the
  deploy log.** The longest part of a teardown is AWS releasing the in-VPC offset-seeder
  Lambda's ENIs before the MSK cluster can go — ~15-20 min during which CloudFormation emits
  no events, so the log looked frozen. The delete wait now polls those ENIs (read-only) and
  logs on change: "Waiting for AWS to reclaim N seeder network interface(s)…" while any
  remain, then "Seeder network interfaces released." once they clear — so the quiet stretch
  shows what it is actually waiting on. The app stack's CDC deploy role gains
  `ec2:DescribeNetworkInterfaces` (read-only) for this; no CDC-infrastructure redeploy is
  needed.

## v0.1.250

### Fixed

- **CDC infrastructure delete showed a precise ETA it kept overshooting.** The stage-progress
  card read "est. ~5 min remaining", but a delete is dominated by AWS reclaiming the in-VPC
  seeder Lambda's ENIs before the MSK cluster can go — unpredictable, and measured well past
  the estimate — so a 4x-short countdown read as a stuck UI. Delete now shows an honest upper
  bound ("can take up to ~20 min") with no per-stage ETA hints; the countdown stays for the
  other operations, whose timings are stable.

## v0.1.249

### Changed

- **Per-table Consistency column: linked it to the Refresh button that fills it.** The
  Consistency verdict is computed from the source/target row counts and high-water PKs,
  which come only from the explicit "Refresh source/target counts" action (a source-scanning
  COUNT(*) that is deliberately never auto-polled). Until it is pressed, every row's
  Consistency reads "refresh to check" — a prompt that pointed at a button the user had to
  connect to on their own. The info notice above the table now names that link before the
  first read ("the Consistency column reads 'refresh to check' until you press 'Refresh
  source/target counts'") and drops the prompt afterwards. Reviewed whether the column and
  button are worth keeping at all: the column is a live cut-over signal distinct from
  Validation's exact check, and it is the button that feeds it — the two are one feature, so
  both stay, with the relationship now made explicit rather than removed.

## v0.1.248

### Fixed

- **CDC step visuals: the "Live status" section and the collapsible panels were unboxed.**
  Before CDC started, "Live status" rendered as a borderless header above an empty chart and
  a loose grey "appears once connectors are detected" line — dead space that did not match
  the bordered cards around it. The whole section is now hidden until CDC has started
  (matching the per-table table), and once shown, the still-ramping placeholder is a proper
  bordered info notice. Separately, the collapsible panels (Connector configuration, the
  cdc-stack parameter file, Delete CDC infrastructure, Infrastructure inputs, the deploy
  log, and the others) drew no border, so they read as unstyled headers next to every carded
  section; they now share one `EXPANSION_PANEL_CLASSES` border token from the design system.

## v0.1.247

### Fixed

- **The migration type stayed switchable while CDC infrastructure was being created.** It
  was excluded on the grounds that the create makes no connectors and streams nothing, but
  that is not the same as the choice being free: it is a ~15-20 min run provisioning a
  billable Amazon MSK cluster, and its progress view and "Delete CDC infrastructure"
  control both live on the CDC sub-step — which switching to "Full load only" removes
  outright, leaving the cluster building in the account with no progress, no completion
  signal and no teardown control on screen. The tool was also inconsistent with itself: the
  oversized-LOB exclusions already lock during an infrastructure create. The type now locks
  for the duration of that run, naming the cost and where the controls are, and unlocks
  again when it finishes (idle infrastructure is a trade-off the user still owns).
- **The CDC step's "Per-table migration status" table appeared before CDC started.** It
  showed up as soon as the CDC sub-step did — including throughout the infrastructure
  create — where every CDC column (Stream lag, Quarantined, Inserts/Updates/Deletes,
  Consistency) is necessarily empty because no connector exists yet, so it read as "CDC is
  running and replicating nothing". It now appears once CDC is actually started, matching
  the live-status panel above it, and from the moment Start CDC is pressed rather than only
  once the connectors finish their ~10-20 min ramp. Nothing is lost: the Full Load's own
  per-table table stays on the Full Load step.

## v0.1.246

### Fixed

- **"Accept quarantined rows & continue" left the red "Migration failed" banner on screen.**
  Accepting the gap is the operator resolving that exact error — it marks Full Load DONE and
  the step then reports "Full Load complete — with an accepted gap" — yet the raw
  `FullLoadIncompleteError` stayed pinned above it. One screen carried three verdicts at once
  (failed / complete-with-gap / DONE) and re-flagged as a problem something the operator had
  already dealt with, including the instruction to "choose 'Accept quarantined rows &
  continue'" they had just followed. The banner is now hidden once the gap is accepted, and
  stays hidden across a migration-type switch (a resolved error is not carried-over context
  either). Nothing is lost: the accepted-gap notice already names the dropped row count, the
  affected tables and that Validation still reports the gap, and the error log still lists
  every dropped row by primary key. Before acceptance the banner is unchanged — that is a
  live failure and stays loud.

## v0.1.245

### Added

- **A jump link on the post-Full-Load "want CDC next?" notice.** The notice told the user
  to "change the migration type above", but after a Full Load that selector is at the top
  of a long page and usually off screen — so the one action the notice asks for was left
  for the user to go hunt. It now offers a "Change migration type" link that scrolls
  straight to the selector and briefly rings it, so the right control is unmistakable on
  arrival. The copy points at the link instead of naming a direction. Purely
  navigational: the type is still changed by deliberately clicking a tile.

## v0.1.244

### Fixed

- **"Generate DDL for selected" judged target existence from a stale snapshot.** Each
  diff's "'x' already exists on the target — choose SKIP or REPLACE (destructive)" warning
  was answered from the cached target inventory, which issues no SQL and is only filled by
  Evaluation's browse or the manual "Refresh target" button. So after emptying the target,
  Generate still warned about objects that were gone and pushed the user toward a
  destructive choice for nothing; the reverse case was worse, since an object created
  since the snapshot drew no warning at all and produced an unexpected SKIP. Generate now
  re-reads the target catalog first — read-only, off the UI thread, and silent (no toast,
  no double render) — so every verdict reflects the live target without the user having to
  remember a refresh step. A failed re-read does not block generation: the DDL diff is
  still produced, and the apply path independently re-checks existence against a live
  browse, so a stale verdict could never have misrouted the actual DDL.

## v0.1.243

### Fixed

- **The CDC step's per-table "Quarantined" column still counted Full Load quarantines.**
  v0.1.241 filtered the dead-letter card and v0.1.242 the Full Load log, but this column
  was missed — so one screen showed two contradictory numbers for the same session: the
  card read "0 quarantined" while the column right above it read 3. It now shares the
  card's filter, so the two always agree. Also filtered the Full Load activity log's
  per-table failure reasons, which could otherwise attribute a dead-lettered row's reason
  to a table in a FULL_LOAD log line. Every remaining raw read of the shared error log was
  audited: the two that stay unfiltered are the reads immediately feeding the filters
  themselves, and the quarantine counters were already safe (they key on a message prefix
  only the Full Load writers emit).

## v0.1.242

### Fixed

- **The Full Load error log counted dead-lettered CDC rows as Full Load failures.** The
  mirror of v0.1.241, pointing the other way: CDC records under the Full Load's job id
  whenever one ran, so an unfiltered read turned 3 Full Load quarantines plus 2
  dead-lettered rows into "Download Full Load error log (5 errors)" and put the CDC rows
  in the file. At cut-over that reads as "the Full Load lost 5 rows" when it lost 3. The
  per-table failure reason had the same flaw — because only the latest message per table
  is kept, a dead-lettered row could supply the "why" for a table the Full Load had loaded
  fine. The Full Load surfaces (count, per-table rows and reasons, the quarantine list,
  and the download's label *and* file contents) now report its own records only, so the
  two screens partition the error log exactly: Full Load + CDC adds up to the whole log,
  with nothing lost and nothing double-counted.

## v0.1.241

### Fixed

- **The CDC dead-letter queue card counted Full Load quarantines as dead-lettered
  records.** The DLQ panel's error-log key is the Full Load job id whenever one ran, so
  both sources shared one key and batch-loader quarantines appeared under "Dead-letter
  queue (poison records)" — with copy claiming they were "isolated to the DLQ (the
  pipeline keeps running)" for rows that never entered a stream and were set aside hours
  before any connector existed. Full Load has no DLQ. The damaging case is a user who has
  just excluded an oversized LOB column: they saw a non-zero quarantine count and
  concluded the exclusion had failed, when a zero CDC count is precisely the proof that it
  worked. Every DLQ surface — the depth badge, the per-table chips, the record table, and
  the download's label *and* file contents — now reports CDC-sourced records only, all
  through one filter so the count can never disagree with the rows beneath it. The Full
  Load's own quarantines are not hidden: the panel cross-references them in a neutral line
  pointing at the Full Load section, since rows that never reached the target still matter
  at cut-over.

## v0.1.240

### Fixed

- **The Data Migration badge stayed on "CDC: IN_PROGRESS" after Stop CDC or an
  infrastructure Delete.** Detecting connectors promoted the CDC workflow step
  NOT_STARTED → IN_PROGRESS, but nothing ever moved it back, so once the connectors were
  gone the badge still claimed CDC was running — and because the workflow is persisted,
  the stale value came back on every session restore. The step now tracks whether the
  connectors actually exist, in both directions: a teardown drops it to NOT_STARTED,
  which is the honest resting state (nothing is streaming, and Start CDC is on offer
  again). CDC still has no terminal DONE — it is continuous replication that ends only
  by an explicit Stop/Delete. A FAILED recorded elsewhere is left alone rather than
  being relabelled by a routine discovery pass.

## v0.1.239

### Fixed

- **Deleting the CDC infrastructure took ~24 minutes, almost all of it spent not
  deleting the MSK cluster.** Two IAM roles scoped their cluster-level MSK grants with
  `Fn::GetAtt: [MskCluster, Arn]`. CloudFormation reads that as a dependency and deletes
  in reverse, so the cluster had to wait for every role naming it — including the
  offset-seeder's, whose in-VPC Lambda leaves ENIs that AWS takes ~15-20 minutes to
  reclaim. A measured teardown sat 18m30s on the seeder before the cluster's own delete
  (93 seconds) even started, with the UI showing "Deleting infrastructure" throughout.
  Nothing in those policies is needed at teardown — the connectors are already gone and
  IAM is only evaluated at call time — so the roles now build the cluster ARN by name
  (`Fn::Sub`, with a wildcard for the UUID suffix AWS appends). Identical authorization,
  but the cluster and the seeder tear down in parallel. Creation order is unchanged: the
  connectors still `DependsOn` the cluster.
- **The moment a teardown finished, the CDC card offered the deploy form again.** After
  ~20 minutes of waiting for a billable MSK cluster to be removed, the answer the
  operator wants is "it's gone" — not a 20-line BYO-VPC form implying the tool is about
  to rebuild it. The card now confirms the deletion (and that it stopped costing money,
  and that the migration's data is untouched), then offers rebuilding as an explicit
  opt-in that states what saying yes costs (~15-20 min, billable). A first-ever deploy
  is not gated — there the form is the next step — and a second teardown asks again
  rather than reusing the first answer.

## v0.1.238

### Fixed

- **The oversized-LOB exclusion tick boxes stayed clickable after the choice was already
  committed.** They were never locked at all: a tick registered and the state really
  changed, but `column.exclude.list` is baked into the cdc-stack's `ColumnExcludeList`
  parameter at infrastructure-create time and handed to the source connector at Start
  CDC — so any later change was silently discarded while the box claimed otherwise. The
  boxes are now genuinely disabled — greyed *and* click-blocked, so the appearance and
  the behaviour agree. The two transient cases also say why, because the operator may be
  mid-decision and needs the remedy named: submitted with the stack while infrastructure
  is being created, or handed to the connector once CDC started (stop CDC to change it).
  Deployed infrastructure locks silently — that is the normal state of a CDC run, and the
  greyed-out boxes already convey that the choice is closed.
- **After an app restart the CDC pipeline card came back blank and offered a fresh deploy
  form.** The card treated "not yet probed" and "no stack exists" as the same state. The
  read-only AWS probe that reads the live CDC state needs a target region and returns
  silently without one, and a restored session does not trust its old connections until
  re-verified — so the phase was unknown while a real pipeline was streaming, and the card
  invited a duplicate, billable MSK cluster. The two states are now distinguished: when
  the state has genuinely not been determined, the card says so, states that any running
  pipeline is unaffected, and names the one action that recovers it (re-verify the target
  connection) instead of showing a deploy form.

## v0.1.237

### Fixed

- **The migration type could still be switched while CDC connectors were being
  created.** Toggling it then locked a moment later, once the connectors appeared. Two
  lock conditions existed and both miss an in-flight connector start: the connectors do
  not exist yet (so the discovered-connectors check is empty and the stack phase is not
  yet `running`), and on a CDC-only plan the `full_load` step is not `IN_PROGRESS`
  either. The start point and table set are committed the moment Start CDC is pressed,
  so the choice now freezes then — using the same `cdc_streaming_started` signal the
  table picker already locks on. An infrastructure create deliberately does NOT lock:
  `create_stack` provisions MSK, networking and plugins but makes no connectors, so
  nothing is committed and nothing streams for the ~15-20 minutes it runs, and the
  operator can still change the plan. Also fixed alongside it: the lock explanation was
  recomputed separately without the job manager, so the new case would have disabled the
  tiles while showing no reason at all — the disabled state and its explanation now come
  from one evaluation and cannot disagree.

## v0.1.236

### Fixed

- **Start / Re-run Full Load appeared to need a second click.** On any plan that
  includes CDC, an account-wide CDC discovery fires ~0.05s after the Data Migration
  screen renders, reads AWS on a worker thread, and used to call the screen's full
  refresh unconditionally when it returned. That rebuilt every widget, so a click landing
  in the window between render and refresh went to an element that no longer existed and
  was silently dropped. The refresh now happens only when discovery actually changed
  something — on a revisit it usually finds the same stack and connectors, so the rebuild
  bought nothing. A real change still refreshes, so the duplicate-MSK adopt guard appears
  as soon as it is known. Full load only was never affected (discovery does not run).
- **A restored CDC-only session showed "CDC: DONE" with CDC never having run.** The
  badge's status came from the single `full_load` workflow step that every migration type
  shares, and the whole workflow is persisted and restored — so a session that had once
  completed a Full Load came back labelled "CDC" carrying that Full Load's DONE. Naming
  one phase while showing another's value is worse than the bare "DONE" the label
  replaced in v0.1.231. For CDC only the badge now reads the independently-maintained
  `cdc` step, so it moves between NOT_STARTED and IN_PROGRESS — which is what CDC does,
  since continuous replication has no completion and ends only via an explicit
  Stop/Delete. Display only: the `full_load` step remains the Validation gate.
- **The CDC infrastructure card rendered twice on the CDC step.** v0.1.235 added a call
  to the prep section there, but the step's lifecycle card already renders the same
  BYO-VPC deploy form (or the adopt choice) whenever the stack is absent, so the
  identical form appeared twice. The prep section is the *extra* entry point — offered
  under Prerequisites only so the ~15-20 min MSK create can overlap a Full Load — and it
  stays suppressed for CDC only, which has no Full Load to overlap.

## v0.1.235

### Changed

- **For CDC only, the CDC infrastructure card now lives in the CDC step instead of
  Prerequisites.** Its Prerequisites placement exists so the ~15-20 min MSK create can
  overlap the Full Load — a reason that does not apply to CDC only, which has no Full
  Load to overlap. Keeping it there split one continuous task across two sections: the
  operator provisioned under Prerequisites, then had to find Start CDC in a different
  section. It now sits ahead of the start-point decision, since nothing downstream can
  run without the stack. Full load + CDC is unchanged: it keeps the Prerequisites
  placement and the overlap that motivates it. The card renders in exactly one place per
  migration type — never both, which would show a billable deploy form twice.

## v0.1.234

### Fixed

- **The CDC section collapsed right after the operator acted on CDC infrastructure,
  hiding the next action.** The infrastructure card sits at the bottom of
  Prerequisites, so deploying or deleting a cdc-stack happens there — but the CDC
  section only opens when the active sub-step is `cdc`, and nothing moved it. Two cases
  hit this: **CDC only with infrastructure ready**, where "CDC infrastructure is ready"
  appeared under Prerequisites while Start CDC sat inside a collapsed CDC section; and
  **immediately after submitting a teardown**, which bounced the view back to
  Prerequisites mid-operation. An existing pin already handled the live-CDC case but was
  gated on connectors existing, which neither of these has yet. The pin now also fires
  while an infrastructure create/teardown is in flight, and for a CDC-only session whose
  infrastructure is ready. Deliberately not for Full load + CDC on readiness alone: a
  finished Full Load keeps its results on screen and the operator advances with
  "Continue to CDC", so pinning there would yank the snapshot's row counts and watermark
  out of view.
- **The ready notice told CDC-only operators to start streaming "after the Full Load".**
  There is no Full Load in that plan, so it read as an unmet prerequisite. It now points
  at Start CDC on the CDC step.

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
