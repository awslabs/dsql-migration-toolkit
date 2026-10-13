// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * G25: a transaction Aurora DSQL asked the sink to retry (40001 or XX000) and that then succeeded
 * left no trace in any log or metric, so a sink slowed by retries could not be told from an
 * undersized one. OccRetry now hands the retried error to the step between attempts, the task
 * counts it by SQLSTATE, and each offset commit (flush) logs one INFO line when any were retried
 * -- counts and SQLSTATE codes only, and none of the phrases the tool reads from the log.
 */
package dev.dsqlmigrator.connect;

import static dev.dsqlmigrator.connect.HoldHarness.P0;
import static dev.dsqlmigrator.connect.HoldHarness.P1;
import static dev.dsqlmigrator.connect.HoldHarness.SECOND;
import static dev.dsqlmigrator.connect.HoldHarness.create;
import static dev.dsqlmigrator.connect.HoldHarness.delete;
import static dev.dsqlmigrator.connect.HoldHarness.task;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dsqlmigrator.connect.HoldHarness.FakeDsql;
import dev.dsqlmigrator.connect.HoldHarness.HoldTask;
import dev.dsqlmigrator.connect.HoldHarness.RecordingReporter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DsqlSinkRetrySummaryTest {

  private static final OccRetry.Sleeper NO_SLEEP = millis -> {};

  private static SQLException occ() {
    return new SQLException("serialization failure", OccRetry.OCC_SQLSTATE);
  }

  private static SQLException unavailable() {
    return new SQLException("server unavailable", OccRetry.SERVER_UNAVAILABLE_SQLSTATE);
  }

  // --- OccRetry hands the step between attempts the retried error -----------------------------

  @Test
  void theStepBetweenAttemptsReceivesEachRetriedErrorAndNeverTheLast() throws SQLException {
    List<String> seen = new ArrayList<>();
    AtomicInteger calls = new AtomicInteger();
    SQLException last =
        assertThrows(
            SQLException.class,
            () ->
                OccRetry.withRetry(
                    () -> {
                      throw calls.incrementAndGet() % 2 == 1 ? occ() : unavailable();
                    },
                    retried -> seen.add(retried.getSQLState()),
                    4,
                    50L,
                    NO_SLEEP,
                    bound -> 0L));
    assertEquals(4, calls.get());
    assertEquals(List.of("40001", "XX000", "40001"), seen); // between attempts only
    assertEquals("XX000", last.getSQLState());

    List<String> none = new ArrayList<>();
    assertEquals(
        "ok",
        OccRetry.withRetry(() -> "ok", retried -> none.add("x"), 10, 50L, NO_SLEEP, b -> 0L));
    assertTrue(none.isEmpty());
    SQLException other = new SQLException("check", "23514");
    assertThrows(
        SQLException.class,
        () -> OccRetry.withRetry(() -> { throw other; }, retried -> none.add("x"), 10, 50L));
    assertTrue(none.isEmpty()); // not retried, so no step between attempts
  }

  // --- The task counts retries and logs them once per commit -----------------------------------

  @Test
  void retriesAreCountedBySqlstateAndSummarisedOncePerCommit() throws Exception {
    // FAILS BEFORE G25: nothing counted a retried transaction.
    FakeDsql dsql = new FakeDsql();
    AtomicInteger attempts = new AtomicInteger();
    dsql.failure =
        (sql, binds) ->
            switch (attempts.incrementAndGet()) {
              case 1, 2 -> occ();
              case 3 -> unavailable();
              default -> null;
            };
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(List.of(create(P0, 1, 1, "a", "x")));
    assertEquals(List.of("1:a:x"), dsql.rows());
    assertEquals(2L, task.retriesSinceSummary("40001"));
    assertEquals(1L, task.retriesSinceSummary("XX000"));

    assertTrue(task.logRetrySummary()); // one INFO line
    assertEquals(0L, task.retriesSinceSummary("40001"));
    assertEquals(0L, task.retriesSinceSummary("XX000"));
    assertFalse(task.logRetrySummary()); // nothing retried since: no line

    dsql.failure = (sql, binds) -> attempts.incrementAndGet() == 5 ? occ() : null;
    task.put(List.of(create(P0, 2, 2, "b", "x")));
    assertEquals(1L, task.retriesSinceSummary("40001"));
    task.flush(Map.of()); // the offset commit logs and resets them
    assertEquals(0L, task.retriesSinceSummary("40001"));
    assertTrue(dlq.records.isEmpty());
  }

  @Test
  void aHeldChangesRetriedProbeIsCountedToo() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    HoldTask task = task(dsql, new RecordingReporter());
    task.put(List.of(create(P1, 10, 100005, "a", "b5")));
    task.put(List.of(delete(P0, 20, 5)));
    task.logRetrySummary();
    AtomicInteger probes = new AtomicInteger();
    dsql.failure = (sql, binds) -> probes.incrementAndGet() == 1 ? unavailable() : null;
    task.advance(SECOND);
    task.put(List.of());
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(1L, task.retriesSinceSummary("XX000"));
  }

  @Test
  void theSummaryLineCarriesCountsAndCodesOnly() {
    String line = DsqlSinkTask.RETRY_SUMMARY;
    assertEquals(3, line.split("\\{}", -1).length - 1, line); // total, 40001, XX000
    assertTrue(line.contains("SQLSTATE 40001") && line.contains("XX000"), line);
    // Not a line the tool parses: the stall notice greps the RetriableException text, the DLQ
    // reader the quarantine line, and the drift classifier the "sqlstate=" token.
    assertFalse(line.contains("DSQL apply failed (transient sqlstate="), line);
    assertFalse(line.contains("Quarantined record to DLQ"), line);
    assertFalse(line.toLowerCase(java.util.Locale.ROOT).contains("sqlstate="), line);
  }
}
