// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * G25: a unique-index hold's 5-minute budget does not run while Aurora DSQL cannot answer, and a
 * held change shows as replication lag while it waits.
 *
 * Budget: while Aurora DSQL answers XX000 (now always redelivered, G22) or a connection drops,
 * neither the held change nor the change that frees its value can apply -- Connect pauses every
 * partition while it redelivers -- so before G25 an outage longer than the budget dead-lettered
 * a held primary-key-change insert on the first try after recovery, and the old row's delete then
 * applied: the row was gone, the v52 loss G23 had fixed. The budget now counts from the hold's
 * first rejection or from the task's last transient failure, whichever is later.
 *
 * Lag: ReplicationLagMs was recorded only for applied rows, so a table whose change was held read
 * as "caught up" for up to 5 minutes. The held head's lag is now recorded when the hold starts,
 * on every failed try and at every offset commit (flush), with no op counted.
 *
 * A held change's own try that Aurora DSQL does not answer no longer fails the put (G25), so
 * Connect no longer logs it: the sink logs it itself, and backs the next try off (500 ms doubling
 * to 10 s) instead of reconnecting every 500 ms for as long as the outage lasts.
 */
package dev.dsqlmigrator.connect;

import static dev.dsqlmigrator.connect.HoldHarness.P0;
import static dev.dsqlmigrator.connect.HoldHarness.P1;
import static dev.dsqlmigrator.connect.HoldHarness.P2;
import static dev.dsqlmigrator.connect.HoldHarness.SECOND;
import static dev.dsqlmigrator.connect.HoldHarness.change;
import static dev.dsqlmigrator.connect.HoldHarness.create;
import static dev.dsqlmigrator.connect.HoldHarness.delete;
import static dev.dsqlmigrator.connect.HoldHarness.row;
import static dev.dsqlmigrator.connect.HoldHarness.task;
import static dev.dsqlmigrator.connect.HoldHarness.tombstone;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dsqlmigrator.connect.HoldHarness.FakeDsql;
import dev.dsqlmigrator.connect.HoldHarness.HoldTask;
import dev.dsqlmigrator.connect.HoldHarness.RecordingReporter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class DsqlSinkHoldBudgetAndLagTest {

  private static final String TABLE = "app.customers";
  private static final long MINUTE = TimeUnit.MINUTES.toNanos(1);

  /** A put during an outage: Connect redelivers a batch whose put threw, so a throw is fine. */
  private static void putThroughOutage(HoldTask task, Collection<SinkRecord> records) {
    try {
      task.put(records);
    } catch (RetriableException redelivered) {
      // Connect hands the same batch back after its next poll
    }
  }

  private static SQLException unavailable() {
    return new SQLException("server unavailable", "XX000");
  }

  // --- The budget does not run during an outage ------------------------------------------------

  @Test
  void aSixMinuteOutageDoesNotDeadLetterAHeldChangeWhoseFreeingDeleteArrivesAfterIt()
      throws Exception {
    // FAILS BEFORE G25: the first try after recovery found the hold 6 minutes old and
    // dead-lettered the insert of 100005; the delete of 5 then applied and the row was gone.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(List.of(create(P1, 10, 100005, "a", "b5")));
    assertEquals(Map.of(P1, 10L), task.heldOffsets());

    // Aurora DSQL answers XX000 to everything for 6 minutes. Every 10 s an idle poll tries the
    // hold and another partition's batch is redelivered; neither fails anything for good.
    dsql.failure = (sql, binds) -> unavailable();
    for (long waited = 0; waited < 6 * MINUTE; waited += 10 * SECOND) {
      task.advance(10 * SECOND);
      putThroughOutage(task, List.of());
      putThroughOutage(task, List.of(create(P2, 1, 400, "z", "q")));
    }
    assertEquals(Map.of(P1, 10L), task.heldOffsets());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));

    // Aurora DSQL answers again; the delete of 5 is still behind the redelivered batch.
    dsql.failure = (sql, binds) -> null;
    task.advance(10 * SECOND);
    task.put(List.of(create(P2, 1, 400, "z", "q"))); // the redelivery lands; the hold's try: 23505
    assertEquals(Map.of(P1, 10L), task.heldOffsets());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));

    task.advance(70 * SECOND); // the other task's redelivery comes up to a commit interval later
    task.put(List.of(delete(P0, 20, 5)));
    task.advance(SECOND);
    task.put(List.of());
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(List.of("400:z:q", "100005:a:b5"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
  }

  @Test
  void aHoldNearItsBudgetWhenAnOutageStartsStillGetsTheWholeWaitAfterIt() throws Exception {
    // The hold has waited 4m50s when a 1-minute connection outage starts: the budget restarts
    // after the outage instead of leaving it 10 s.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(List.of(create(P1, 10, 100005, "a", "b5")));
    for (long waited = 0; waited < 290 * SECOND; waited += 10 * SECOND) {
      task.advance(10 * SECOND);
      task.put(List.of());
    }
    dsql.failure = (sql, binds) -> new SQLException("This connection has been closed.", "08003");
    for (int i = 0; i < 6; i++) {
      task.advance(10 * SECOND);
      putThroughOutage(task, List.of());
    }
    dsql.failure = (sql, binds) -> null;
    for (int i = 0; i < 12; i++) { // two more minutes of a genuine wait after it
      task.advance(10 * SECOND);
      task.put(List.of());
    }
    assertEquals(Map.of(P1, 10L), task.heldOffsets());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
    task.put(List.of(delete(P0, 20, 5)));
    task.advance(10 * SECOND);
    task.put(List.of());
    assertEquals(List.of("100005:a:b5"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
  }

  @Test
  void aGenuineConflictIsStillDeadLetteredFiveMinutesAfterTheLastOutage() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(7, "a", "x"); // another row really holds the email
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(List.of(create(P1, 10, 100005, "a", "y")));
    dsql.failure = (sql, binds) -> unavailable();
    task.advance(10 * SECOND);
    task.put(List.of()); // a probe Aurora DSQL could not answer: the hold stays, no throw
    assertEquals(500L, task.ctx.lastTimeout);
    dsql.failure = (sql, binds) -> null;
    task.advance(DsqlSinkTask.UNIQUE_HOLD_BUDGET_NANOS - SECOND);
    task.put(List.of());
    assertTrue(dlq.records.isEmpty()); // within 5 min of the outage
    task.advance(2 * SECOND);
    task.put(List.of());
    assertEquals(List.of("P1@10"), dlq.names());
    assertEquals("23505", ((SQLException) dlq.causes.get(0)).getSQLState());
  }

  // --- ...nor during a busy outage, where only the failing writes see it ----------------------

  /** A Connect loop on {@code task}'s clock: an empty poll waits out its timeout, else 60 s. */
  private static ConnectSinkLoop connect(HoldTask task) {
    ConnectSinkLoop connect = new ConnectSinkLoop();
    connect.clock = ms -> task.advance(TimeUnit.MILLISECONDS.toNanos(ms));
    return connect;
  }

  /**
   * P1@10, the insert of 100005 (email a), is held behind row 5. Then Aurora DSQL fails
   * {@code batch} (another partition's records) with {@code failure} for six minutes: Connect
   * pauses every partition and hands that batch back after each poll, so no put is idle and the
   * hold is never tried while it lasts -- only the failing writes can tell the hold's budget that
   * Aurora DSQL did not answer. Then row 5's delete frees a. Returns the target's rows.
   */
  private static List<String> aBusySixMinuteOutage(
      Function<FakeDsql, BiFunction<String, List<Object>, SQLException>> failure,
      SinkRecord... batch)
      throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    ConnectSinkLoop connect = connect(task);
    connect.declare(P0, 20);
    connect.declare(P1, 10);
    connect.declare(P2, 0);
    connect.append(create(P1, 10, 100005, "a", "b5"));
    connect.start(task, task.ctx);
    assertTrue(connect.poll());
    assertEquals(Map.of(P1, 10L), task.heldOffsets());

    dsql.failure = failure.apply(dsql);
    connect.append(batch);
    long tries = dsql.count("upsert:100005");
    long start = task.now;
    while (task.now - start < 6 * MINUTE) {
      assertFalse(connect.poll()); // the same batch, handed back after each poll
      connect.commit(false);
    }
    assertEquals(tries, dsql.count("upsert:100005")); // the hold was never tried meanwhile

    dsql.failure = (sql, binds) -> null; // Aurora DSQL answers again
    assertTrue(connect.poll()); // the batch lands, then the hold's try: 23505, row 5 holds a
    assertEquals(Map.of(P1, 10L), task.heldOffsets());
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());

    connect.append(delete(P0, 20, 5), tombstone(P0, 21, 5));
    connect.drain(200);
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    return dsql.rows();
  }

  @Test
  void aBusySixMinuteOutageDoesNotDeadLetterAHeldChangeEither() throws Exception {
    // FAILS IF A FAILED CHUNK STOPS COUNTING AS AN OUTAGE (transientFailure no longer stamping
    // lastTransientNanos): the first try after it found the hold 7 minutes old and dead-lettered
    // the insert of 100005, and the delete of 5 then left the row gone -- the confirmed[11] loss.
    // The idle-probe outage above stays green either way: its probes stamp the time themselves.
    List<String> rows =
        aBusySixMinuteOutage(
            dsql -> (sql, binds) -> unavailable(), create(P2, 0, 400, "z", "q"));
    assertEquals(List.of("400:z:q", "100005:a:b5"), rows);
  }

  @Test
  void theSameWhenTheFailingWriteIsARowReplayedInItsOwnTransaction() throws Exception {
    // Row 401 is rejected for good while the outage lasts (a CHECK, 23514), so its chunk is
    // replayed one row per transaction, where row 400 meets XX000: the record-by-record path.
    List<String> rows =
        aBusySixMinuteOutage(
            dsql ->
                (sql, binds) -> {
                  if (Integer.valueOf(401).equals(binds.get(0))) {
                    return new SQLException("new row violates check constraint", "23514");
                  }
                  boolean ownTransaction = "update".equals(dsql.calls.get(dsql.calls.size() - 1));
                  return ownTransaction ? unavailable() : null;
                },
            create(P2, 0, 400, "z", "q"),
            create(P2, 1, 401, "w", "q"));
    assertEquals(List.of("400:z:q", "401:w:q", "100005:a:b5"), rows);
  }

  // --- A held change's try that Aurora DSQL does not answer backs off and is logged -------------

  /** Records each WARN of a held change's failed try: when, the wait it names, and its subject. */
  static final class UnansweredLogTask extends HoldTask {
    final List<Long> loggedAt = new ArrayList<>();
    final List<Long> loggedWaitMs = new ArrayList<>();
    final List<String> loggedSubjects = new ArrayList<>();

    @Override
    boolean logUnansweredTry(
        TopicPartition tp, UniqueHold hold, SQLException e, long retryInNanos, long now) {
      boolean logged = super.logUnansweredTry(tp, hold, e, retryInNanos, now);
      if (logged) {
        loggedAt.add(now);
        loggedWaitMs.add(TimeUnit.NANOSECONDS.toMillis(retryInNanos));
        loggedSubjects.add(
            e.getSQLState() + " " + hold.head.event().table() + " P" + tp.partition() + "@"
                + hold.offset);
      }
      return logged;
    }
  }

  /** An idle poll: it waits out the timeout the last put set, then puts nothing (no throw). */
  private static long idlePoll(HoldTask task) {
    task.advance(TimeUnit.MILLISECONDS.toNanos(task.ctx.lastTimeout));
    task.put(List.of());
    return task.ctx.lastTimeout;
  }

  private static SQLException refused() {
    return new SQLException("Connection to example refused.", "08001");
  }

  @Test
  void aHeldChangesTriesBackOffAndAreLoggedWhileAuroraDsqlDoesNotAnswer() throws Exception {
    // The stream is idle (the source frozen for cut over), one change is held, and the sink loses
    // its path to Aurora DSQL (08001). FAILS BEFORE THE FIX: every try came 500 ms after the
    // last -- 600 reconnects in 5 minutes -- and none was logged: Connect logs nothing, as the
    // put no longer throws, so only the table's growing lag showed it.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(7, "a", "x"); // a genuine conflict: nothing frees a in this test
    RecordingReporter dlq = new RecordingReporter();
    UnansweredLogTask task = task(new UnansweredLogTask(), dsql, dlq, Map.of());
    task.put(List.of(create(P1, 10, 100005, "a", "y")));
    assertEquals(500L, task.ctx.lastTimeout); // the conflict's own first wait

    dsql.failure = (sql, binds) -> refused();
    long tries = dsql.count("upsert:100005");
    long start = task.now;
    List<Long> waits = new ArrayList<>();
    while (task.now - start < 5 * MINUTE) {
      waits.add(idlePoll(task));
    }
    assertEquals(List.of(500L, 1_000L, 2_000L, 4_000L, 8_000L, 10_000L), waits.subList(0, 6));
    assertTrue(
        waits.subList(6, waits.size()).stream().allMatch(w -> w == 10_000L), waits::toString);
    long outageTries = dsql.count("upsert:100005") - tries;
    assertTrue(outageTries <= 40, outageTries + " tries in 5 minutes");

    // Logged at the first failed try, then about once a minute while they keep failing.
    assertEquals(start + TimeUnit.MILLISECONDS.toNanos(500), task.loggedAt.get(0));
    assertEquals(500L, task.loggedWaitMs.get(0));
    for (int i = 1; i < task.loggedAt.size(); i++) {
      long gap = task.loggedAt.get(i) - task.loggedAt.get(i - 1);
      assertTrue(gap >= MINUTE && gap < MINUTE + 10 * SECOND, "gap " + gap / SECOND + " s");
      assertEquals(10_000L, task.loggedWaitMs.get(i));
    }
    assertTrue(task.loggedAt.size() >= 5 && task.loggedAt.size() <= 6, task.loggedAt::toString);
    assertTrue(task.loggedSubjects.stream().allMatch("08001 app.customers P1@10"::equals));
    assertEquals(Map.of(P1, 10L), task.heldOffsets()); // still held, not dead-lettered
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));

    // A try Aurora DSQL answers (still 23505) starts the next outage over: 500 ms, logged at once.
    dsql.failure = (sql, binds) -> null;
    idlePoll(task);
    assertEquals(Map.of(P1, 10L), task.heldOffsets());
    int logged = task.loggedAt.size();
    dsql.failure = (sql, binds) -> refused();
    assertEquals(500L, idlePoll(task));
    assertEquals(logged + 1, task.loggedAt.size());
    assertEquals(500L, task.loggedWaitMs.get(logged));
  }

  @Test
  void theFailedTryLineStartsLikeConnectsRetriableExceptionAndCarriesNoValue() {
    String line = DsqlSinkTask.UNIQUE_HOLD_TRY_FAILED;
    // SQLSTATE, table, partition, offset and the next wait only (Property 7).
    assertEquals(5, line.split("\\{}", -1).length - 1, line);
    // The text the stall notice and the manual tell the operator to look for: a held change's
    // failed try no longer reaches Connect's own RetriableException line.
    String prefix = "DSQL apply failed (transient sqlstate=";
    assertTrue(line.startsWith(prefix + "{}); "), line);
    String connects =
        DsqlSinkTask.transientRetryException(new SQLException("refused", "08001")).getMessage();
    assertTrue(connects.startsWith(prefix + "08001); "), connects);
    // Never read as a dead letter.
    assertFalse(line.contains("Quarantined record to DLQ"), line);
    assertFalse(line.contains("Dropping unapplicable record"), line);
    assertEquals(TimeUnit.SECONDS.toNanos(60), DsqlSinkTask.UNIQUE_HOLD_TRY_FAILED_LOG_NANOS);
  }

  // --- A held change shows as replication lag --------------------------------------------------

  private static Map<String, String> metricsOn() {
    return Map.of(DsqlSinkConnectorConfig.METRICS_STACK, "test-stack");
  }

  @SuppressWarnings("unchecked")
  private static long takeLag(DsqlSinkTask task) throws Exception {
    Map<String, AtomicLong> lag = (Map<String, AtomicLong>) HoldHarness.get(task, "lagByTable");
    AtomicLong v = lag.get(TABLE);
    return v == null ? 0L : v.getAndSet(0L); // what one emission reads and clears
  }

  @SuppressWarnings("unchecked")
  private static long inserts(DsqlSinkTask task) throws Exception {
    LongAdder v = ((Map<String, LongAdder>) HoldHarness.get(task, "insertsByTable")).get(TABLE);
    return v == null ? 0L : v.sum();
  }

  private static SinkRecord createAt(long sourceTsMs, long offset, int id, String email) {
    return change(P1, offset, id, "c", null, row(id, email, "b"), sourceTsMs);
  }

  @Test
  void aHeldChangeIsReportedAsLagUntilItAppliesWithNoOpCounted() throws Exception {
    // FAILS BEFORE G25: nothing recorded lag for a held change, so its table read as caught up.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b");
    HoldTask task = task(dsql, new RecordingReporter(), metricsOn());
    long committedAt = System.currentTimeMillis() - 30_000L; // the source committed 30 s ago
    task.put(List.of(createAt(committedAt, 10, 100005, "a")));
    assertEquals(Map.of(P1, 10L), task.heldOffsets());
    assertTrue(takeLag(task) >= 30_000L); // the hold started
    assertEquals(0L, inserts(task)); // nothing applied, nothing counted

    task.advance(SECOND);
    task.put(List.of()); // a failed try
    assertTrue(takeLag(task) >= 30_000L);
    task.flush(Map.of()); // every offset commit, while it waits
    assertTrue(takeLag(task) >= 30_000L);
    task.flush(Map.of());
    assertTrue(takeLag(task) >= 30_000L);
    assertEquals(0L, inserts(task));

    task.put(List.of(delete(P0, 20, 5)));
    task.advance(10 * SECOND);
    task.put(List.of()); // it applies: counted once, with its lag
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(1L, inserts(task));
    assertTrue(takeLag(task) >= 30_000L);
    task.flush(Map.of());
    assertEquals(0L, takeLag(task)); // no hold any more: the next window reads caught up
  }

  @Test
  void aDeadLetteredHoldStopsReportingLag() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(7, "a", "x");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq, metricsOn());
    task.put(List.of(createAt(System.currentTimeMillis() - 5_000L, 10, 100005, "a")));
    task.flush(Map.of());
    assertTrue(takeLag(task) >= 5_000L);
    task.advance(DsqlSinkTask.UNIQUE_HOLD_BUDGET_NANOS + SECOND);
    task.put(List.of());
    assertEquals(1, dlq.records.size());
    takeLag(task);
    task.flush(Map.of());
    assertEquals(0L, takeLag(task));
    assertEquals(0L, inserts(task));
  }

  @Test
  void withMetricsOffAHoldRecordsNothing() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b");
    HoldTask task = task(dsql, new RecordingReporter());
    task.put(List.of(createAt(System.currentTimeMillis() - 5_000L, 10, 100005, "a")));
    task.flush(Map.of());
    assertEquals(0L, takeLag(task));
  }

  // --- Every logged hold is closed by a line --------------------------------------------------

  @Test
  void theNewHoldLinesCarryNoValueAndNoParsedPhrase() {
    for (String line : List.of(DsqlSinkTask.UNIQUE_HOLD_REPLACED, DsqlSinkTask.UNIQUE_HOLD_DROPPED)) {
      // Placeholders: table, partition, offset and index name only (Property 7).
      assertTrue(line.split("\\{}", -1).length - 1 <= 4, line);
      assertFalse(line.contains("sqlstate="), line);
      assertFalse(line.contains("Quarantined record to DLQ"), line);
    }
    assertTrue(DsqlSinkTask.UNIQUE_HOLD_REPLACED.contains("replaces the held change"));
    assertTrue(DsqlSinkTask.UNIQUE_HOLD_DROPPED.contains("next owner reads it again"));
  }

  @Test
  void aRevokedHoldIsDroppedAndItsCommitStaysAtTheHeldChange() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    HoldTask task = task(dsql, new RecordingReporter());
    task.put(List.of(create(P1, 10, 100005, "a", "b5")));
    assertEquals(10L, task.preCommit(HoldHarness.offsets(P1, 11L)).get(P1).offset());
    task.close(List.of(P1)); // logs the hold it drops
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(15L, task.preCommit(HoldHarness.offsets(P1, 15L)).get(P1).offset());
    assertNull(task.preCommit(HoldHarness.offsets(P0, 3L)).get(P1));
  }
}
