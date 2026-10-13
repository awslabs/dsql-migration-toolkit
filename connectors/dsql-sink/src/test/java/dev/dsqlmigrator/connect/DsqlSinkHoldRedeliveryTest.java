// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * G25: a unique-index hold released in a put that then threw a RetriableException.
 *
 * Kafka Connect 3.7 keeps the batch of a put that throws a RetriableException and hands the SAME
 * batch back after the next poll -- whose rewind() runs first, but the batch was fetched before
 * it -- and after that put returns it sets each partition's current offset to the end of the
 * batch (currentOffsets.putAll(origOffsets)). Before G25 a hold was released (rewound past its
 * head, its commit cap recorded, its partition resumed) inside the put, and that put could still
 * throw: a later hold's probe meeting a dropped connection, or a chunk after the look-ahead's
 * "replaced" decision failing transiently. The redelivery then cleared the cap and, the hold
 * gone, wrote the released partition's later records ahead of its never-written tail, and the
 * next commit passed that tail: after a stop, a rebalance or a crash the row was lost, with no
 * dead letter. Since G25 a release is applied only when the put returns normally, and a probe's
 * transient failure no longer fails the put. These tests drive the real task through a model of
 * WorkerSinkTask 3.7 (ConnectSinkLoop) and crash it at the worst point.
 */
package dev.dsqlmigrator.connect;

import static dev.dsqlmigrator.connect.HoldHarness.HUGE;
import static dev.dsqlmigrator.connect.HoldHarness.P0;
import static dev.dsqlmigrator.connect.HoldHarness.P1;
import static dev.dsqlmigrator.connect.HoldHarness.P2;
import static dev.dsqlmigrator.connect.HoldHarness.SECOND;
import static dev.dsqlmigrator.connect.HoldHarness.TOPIC;
import static dev.dsqlmigrator.connect.HoldHarness.create;
import static dev.dsqlmigrator.connect.HoldHarness.delete;
import static dev.dsqlmigrator.connect.HoldHarness.offsets;
import static dev.dsqlmigrator.connect.HoldHarness.task;
import static dev.dsqlmigrator.connect.HoldHarness.tombstone;
import static dev.dsqlmigrator.connect.HoldHarness.update;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dsqlmigrator.connect.HoldHarness.FakeDsql;
import dev.dsqlmigrator.connect.HoldHarness.HoldTask;
import dev.dsqlmigrator.connect.HoldHarness.RecordingReporter;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class DsqlSinkHoldRedeliveryTest {

  private static SQLException dropped() {
    return new SQLException("An I/O error occurred while sending to the backend.", "08006");
  }

  private static ConnectSinkLoop connect(HoldTask task) {
    ConnectSinkLoop connect = new ConnectSinkLoop();
    connect.clock = ms -> task.advance(TimeUnit.MILLISECONDS.toNanos(ms));
    return connect;
  }

  /** A fresh task on the same double: a crash or a worker recycle, then a restart. */
  private static HoldTask restart(ConnectSinkLoop connect, FakeDsql dsql, RecordingReporter dlq)
      throws Exception {
    connect.crash();
    HoldTask next = task(dsql, dlq);
    connect.clock = ms -> next.advance(TimeUnit.MILLISECONDS.toNanos(ms));
    connect.start(next, next.ctx);
    return next;
  }

  private static int index(List<String> writes, String write) {
    int i = writes.indexOf(write);
    assertTrue(i >= 0, write + " never committed: " + writes);
    return i;
  }

  // --- Entry point 1: a release in resolveDueHolds, then another hold's probe fails --------------

  @Test
  void aReleaseThenAnotherHoldsTransientProbeNeverLetsTheCommitPassTheTail() throws Exception {
    // A primary-key change 5 -> 100005 on a table with UNIQUE(email): its insert (P1@10) arrives
    // before the old row's delete (P0@20); P1@11 is another row. P2@30 is a genuine conflict.
    // FAILS BEFORE G25: put 2 released P1 (the probe applied 100005) and then P2's probe met a
    // dropped connection and threw; Connect redelivered put 2's batch, which wrote P1@12/13
    // ahead of P1@11 and committed P1=14 -- after a crash row 201 was gone, with no dead letter.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    dsql.seed(7, "q", "b7");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    ConnectSinkLoop connect = connect(task);
    connect.declare(P0, 20);
    connect.declare(P1, 10);
    connect.declare(P2, 30);
    connect.append(
        create(P1, 10, 100005, "a", "b5"), create(P1, 11, 201, "e201", "x"),
        create(P2, 30, 300, "q", "z"));
    connect.start(task, task.ctx);
    assertTrue(connect.poll());
    assertEquals(Map.of(P1, 10L, P2, 30L), task.heldOffsets());

    connect.append(
        delete(P0, 20, 5), tombstone(P0, 21, 5),
        create(P1, 12, 202, "e202", "x"), create(P1, 13, 203, "e203", "x"));
    AtomicInteger p2Probes = new AtomicInteger();
    dsql.failure =
        (sql, binds) ->
            sql.startsWith("INSERT") && Integer.valueOf(300).equals(binds.get(0))
                    && p2Probes.getAndIncrement() == 0 // P2's probe in put 2
                ? dropped()
                : null;
    task.advance(SECOND);
    assertTrue(connect.poll(), "a probe's transient failure must not fail the put");
    assertTrue(dsql.has(100005), dsql.rows().toString());
    assertEquals(Map.of(P2, 30L), task.heldOffsets()); // P2's hold survives the dropped probe
    assertEquals(500L, task.ctx.lastTimeout); // and is tried again soon
    assertFalse(dsql.has(201)); // the released partition's tail is fetched again, in order
    assertEquals(0, dsql.count("upsert:202"));

    // Connect set P1's current offset to 14 (the end of the batch); the commit stays at 11.
    assertEquals(14L, connect.currentOffsets.get(P1));
    Map<TopicPartition, Long> committed = connect.commit(false);
    assertEquals(11L, committed.get(P1));
    assertEquals(30L, committed.get(P2));

    // The worst moment for a crash: the next owner starts at 11 and loses nothing.
    task = restart(connect, dsql, dlq);
    connect.drain(200); // P2's genuine conflict is dead-lettered after its 5 minutes
    assertEquals(
        List.of("7:q:b7", "201:e201:x", "202:e202:x", "203:e203:x", "100005:a:b5"),
        dsql.rows());
    assertEquals(List.of("P2@30"), dlq.names()); // only the genuine conflict, once
    assertTrue(
        index(dsql.committedWrites, "upsert:201") < index(dsql.committedWrites, "upsert:202"));
  }

  // --- Entry point 2: the look-ahead finds the replacement, then a chunk fails -------------------

  @Test
  void aReplacedHoldIsReleasedOnlyWhenTheRedeliveredPutReturns() throws Exception {
    // P1@10 is the held insert of 100005; P1@11 (row 77) is its tail. Put 2 carries P1@12 (row
    // 88) and P1@13, a later full image of 100005 that replaces the head, and the old row's
    // delete (P0@2), whose first attempt meets a dropped connection. FAILS BEFORE G25: the
    // release was issued before the chunk failed, so the redelivered batch wrote row 88 ahead
    // of row 77, Connect committed P1=14, and after a crash row 77 was never applied.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    ConnectSinkLoop connect = connect(task);
    connect.declare(P0, 2);
    connect.declare(P1, 10);
    connect.append(create(P1, 10, 100005, "a", "b5"), create(P1, 11, 77, "q", "x"));
    connect.start(task, task.ctx);
    assertTrue(connect.poll());
    assertEquals(Map.of(P1, 10L), task.heldOffsets());

    connect.append(
        delete(P0, 2, 5), create(P0, 3, 301, "s", "x"),
        create(P1, 12, 88, "r", "x"), update(P1, 13, 100005, "a", "b6"));
    AtomicInteger deletes = new AtomicInteger();
    dsql.failure =
        (sql, binds) -> sql.startsWith("DELETE") && deletes.getAndIncrement() == 0 ? dropped() : null;
    assertFalse(connect.poll()); // the chunk failed transiently: Connect redelivers the batch
    assertEquals(Map.of(P1, 10L), task.heldOffsets()); // the replaced hold is not released yet
    assertTrue(task.ctx.rewinds.isEmpty(), task.ctx.calls.toString());
    assertEquals(10L, connect.commit(false).get(P1));

    assertTrue(connect.poll()); // the redelivery of the SAME batch
    assertEquals(0, dsql.count("upsert:88")); // never ahead of row 77
    assertTrue(task.heldOffsets().isEmpty()); // released now that a put returned
    assertEquals(11L, task.ctx.rewinds.get(P1));
    assertEquals(14L, connect.currentOffsets.get(P1));
    assertEquals(11L, connect.commit(false).get(P1));

    task = restart(connect, dsql, dlq);
    connect.drain(200);
    assertEquals(List.of("77:q:x", "88:r:x", "301:s:x", "100005:a:b6"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
    assertTrue(index(dsql.committedWrites, "upsert:77") < index(dsql.committedWrites, "upsert:88"));
  }

  @Test
  void withoutACrashTheRedeliveredPartitionIsWrittenInOrder() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    ConnectSinkLoop connect = connect(task);
    connect.declare(P0, 2);
    connect.declare(P1, 10);
    connect.append(create(P1, 10, 100005, "a", "b5"), create(P1, 11, 77, "q", "x"));
    connect.start(task, task.ctx);
    connect.poll();
    connect.append(delete(P0, 2, 5), create(P1, 12, 88, "r", "x"), update(P1, 13, 100005, "a", "b6"));
    AtomicInteger deletes = new AtomicInteger();
    dsql.failure =
        (sql, binds) -> sql.startsWith("DELETE") && deletes.getAndIncrement() == 0 ? dropped() : null;
    connect.drain(200);
    assertEquals(List.of("77:q:x", "88:r:x", "100005:a:b6"), dsql.rows());
    List<String> writes = dsql.committedWrites;
    assertTrue(index(writes, "upsert:77") < index(writes, "upsert:88"), writes.toString());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
  }

  // --- A record refused ahead of the hold is dead-lettered once ---------------------------------

  @Test
  void aRecordRefusedAheadOfAReleasedHoldIsDeadLetteredOnceThroughARedelivery() throws Exception {
    // P1@11 (an oversized row) is refused before any write in the put that holds P1@10.
    // FAILS BEFORE G25: the redelivery cleared the release's cap, the commit reached 13 and
    // forgot the refusal, and the re-fetch from 11 dead-lettered it a second time.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    ConnectSinkLoop connect = connect(task);
    connect.declare(P0, 2);
    connect.declare(P1, 10);
    connect.append(create(P1, 10, 100005, "a", "b5"), create(P1, 11, 300, "e", HUGE));
    connect.start(task, task.ctx);
    assertTrue(connect.poll());
    assertEquals(List.of("P1@11"), dlq.names());

    connect.append(delete(P0, 2, 5), update(P1, 12, 100005, "a", "b6"));
    AtomicInteger deletes = new AtomicInteger();
    dsql.failure =
        (sql, binds) -> sql.startsWith("DELETE") && deletes.getAndIncrement() == 0 ? dropped() : null;
    assertFalse(connect.poll());
    connect.commit(false);
    assertTrue(connect.poll()); // the redelivery
    connect.commit(false);
    connect.drain(200);
    assertEquals(List.of("P1@11"), dlq.names()); // exactly once
    assertEquals(List.of("100005:a:b6"), dsql.rows());
  }

  // --- A change skipped as replaced stays skipped through a redelivery -------------------------

  /** Row 50's insert (P0@0) meets a dropped connection {@code times} times, then applies. */
  private static void dropRow50(FakeDsql dsql, int times) {
    AtomicInteger drops = new AtomicInteger();
    dsql.failure =
        (sql, binds) ->
            sql.startsWith("INSERT")
                    && Integer.valueOf(50).equals(binds.get(0))
                    && drops.getAndIncrement() < times
                ? dropped()
                : null;
  }

  @Test
  void anInsertSkippedAsReplacedIsNotDeadLetteredByTheRedeliveriesOfItsPut() throws Exception {
    // A primary-key change 5 -> 100005 (email a): the insert of 100005 (P1@10) and the app's
    // next update of it (P1@11, the same column set) arrive before the old row's delete (P0).
    // The insert is skipped (its update replaces it) and the update is held; row 50 later in
    // the same put meets a dropped connection twice, so Connect hands the batch back twice.
    // FAILS BEFORE G25: a redelivery skips the held tail unparsed, so the insert's replacement
    // was not found and the insert was dead-lettered with 23505 -- once per redelivery, though
    // nothing was lost and its row ends right.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    ConnectSinkLoop connect = connect(task);
    connect.declare(P1, 10);
    connect.declare(P0, 0);
    connect.append(
        create(P1, 10, 100005, "a", "b5"), update(P1, 11, 100005, "a", "b6"),
        create(P0, 0, 50, "q", "q0"));
    dropRow50(dsql, 2);
    connect.start(task, task.ctx);
    assertFalse(connect.poll());
    assertEquals(Map.of(P1, 11L), task.heldOffsets()); // the update; the insert was skipped
    assertFalse(connect.poll()); // the first redelivery meets the second drop
    assertTrue(connect.poll()); // the second redelivery
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    assertEquals(Map.of(P1, 11L), task.heldOffsets());

    connect.append(delete(P0, 1, 5), tombstone(P0, 2, 5));
    connect.drain(200);
    assertEquals(List.of("50:q:q0", "100005:a:b6"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    assertEquals(12L, connect.commit(false).get(P1));
  }

  @Test
  void anUpdateSkippedAsReplacedIsNotDeadLetteredByARedeliveryWhenItsReplacementIsTheHold()
      throws Exception {
    // X(1)=a, Y(9)=b. X a->b (P1@10) and X keeping b (P1@11) while Y b->y lags on P0: X@10 is
    // skipped (X@11 replaces it) and X@11 is held; row 50 meets a dropped connection once.
    // FAILS BEFORE G25: the redelivery dead-lettered X@10 (23505), its replacement being the
    // held change itself.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(1, "a", "x0");
    dsql.seed(9, "b", "y0");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    ConnectSinkLoop connect = connect(task);
    connect.declare(P1, 10);
    connect.declare(P0, 0);
    connect.append(
        update(P1, 10, 1, "b", "x1"), update(P1, 11, 1, "b", "x2"), create(P0, 0, 50, "q", "q0"));
    dropRow50(dsql, 1);
    connect.start(task, task.ctx);
    assertFalse(connect.poll());
    assertEquals(Map.of(P1, 11L), task.heldOffsets());
    assertTrue(connect.poll()); // the redelivery
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());

    connect.append(update(P0, 1, 9, "y", "y0"));
    connect.drain(200);
    assertEquals(List.of("1:b:x2", "9:y:y0", "50:q:q0"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
  }

  // --- A skip is not remembered: a redelivery runs the skipped change again ------------------

  /**
   * One put: X a->b (P1@10) while Y(9) holds b, another row's delete (P1@11), X b->c (P1@12, the
   * same column set, so it replaces X@10) and row 50's insert (P0@5), whose first try meets a
   * dropped connection, so Connect hands the batch back. When {@code freed}, Y gives b up (its
   * change applied by another sink task) before that redelivery. Checks what holds either way --
   * X@10 skipped the first time and run again by the redelivery, nothing held or dead-lettered,
   * the commit past the whole batch, X as on the source -- and returns the writes the
   * redelivery committed, in order.
   */
  private static List<String> aSkippedChangeThroughARedelivery(boolean freed) throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(1, "a", "x0");
    dsql.seed(9, "b", "y0");
    dsql.seed(77, "z", "z0");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    ConnectSinkLoop connect = connect(task);
    connect.declare(P1, 10);
    connect.declare(P0, 5);
    connect.append(
        update(P1, 10, 1, "b", "x1"), delete(P1, 11, 77), update(P1, 12, 1, "c", "x2"),
        create(P0, 5, 50, "o", "p"));
    AtomicInteger drops = new AtomicInteger();
    AtomicInteger x10Runs = new AtomicInteger(); // X@10's upsert: row 1 taking b
    dsql.failure =
        (sql, binds) -> {
          if (sql.startsWith("INSERT")
              && Integer.valueOf(1).equals(binds.get(0))
              && "b".equals(binds.get(1))) {
            x10Runs.incrementAndGet();
          }
          return sql.startsWith("INSERT")
                  && Integer.valueOf(50).equals(binds.get(0))
                  && drops.getAndIncrement() == 0
              ? dropped()
              : null;
        };
    connect.start(task, task.ctx);
    assertFalse(connect.poll()); // X@10 skipped, X@12 applied, then row 50's dropped connection
    assertEquals(List.of("delete:77", "upsert:1"), dsql.committedWrites);
    assertTrue(task.heldOffsets().isEmpty()); // the skip holds nothing
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    if (freed) {
      dsql.seed(9, "y", "y1");
    }
    dsql.committedWrites.clear();
    x10Runs.set(0);
    assertTrue(connect.poll()); // Connect's redelivery of the SAME batch
    assertTrue(x10Runs.get() > 0, "the redelivery did not run X@10 again");
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    assertTrue(task.heldOffsets().isEmpty());
    assertTrue(dsql.rows().contains("1:c:x2"), dsql.rows()::toString); // X as on the source
    Map<TopicPartition, Long> committed = connect.commit(false);
    assertEquals(13L, committed.get(P1)); // nothing holds the commit back
    assertEquals(6L, committed.get(P0));
    return dsql.committedWrites;
  }

  @Test
  void aRedeliveryRunsASkippedChangeAgainAndWritesItOnceItsConflictHasCleared() throws Exception {
    // The skip is no promise that the change is never written: Y has freed b, so the
    // redelivery's X@10 commits -- X holds b for a moment -- and X@12 then applies over it.
    assertEquals(
        List.of("upsert:1", "delete:77", "upsert:1", "upsert:50"),
        aSkippedChangeThroughARedelivery(true));
  }

  @Test
  void aRedeliveryRunsASkippedChangeAgainAndSkipsItAgainWhileItStillConflicts()
      throws Exception {
    // Y still holds b: the redelivery's X@10 is rejected and skipped again, and only X@12 writes
    // row 1.
    assertEquals(
        List.of("delete:77", "upsert:1", "upsert:50"), aSkippedChangeThroughARedelivery(false));
  }

  // --- The redelivery guard (defence in depth) -------------------------------------------------

  @Test
  void aRedeliveryKeepsAReleasedCapSkipsThePartitionPastItAndRewindsAgain() throws Exception {
    // No put releases a hold and then throws any more, so a redelivered batch never meets a cap
    // through the task's own paths; should one ever, the redelivery keeps the cap, writes none
    // of that partition's records at or past it, and issues the rewind again.
    FakeDsql dsql = new FakeDsql();
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    List<SinkRecord> batch =
        List.of(create(P0, 5, 50, "o", "p"), create(P1, 11, 201, "e", "x"),
            create(P1, 12, 202, "f", "x"));
    dsql.failure = (sql, binds) -> dropped();
    assertThrows(RetriableException.class, () -> task.put(batch));
    dsql.failure = (sql, binds) -> null;
    @SuppressWarnings("unchecked")
    Map<TopicPartition, Long> caps = (Map<TopicPartition, Long>) HoldHarness.get(task, "releasedCaps");
    caps.put(P1, 11L); // a release the put that threw had issued

    task.ctx.calls.clear();
    task.put(batch); // Connect's redelivery of the same batch
    assertEquals(List.of("50:o:p"), dsql.rows());
    assertEquals(List.of("offset:" + TOPIC + "-1:11"), task.ctx.calls);
    assertEquals(
        11L, task.preCommit(offsets(P0, 6L, P1, 13L)).get(P1).offset());

    task.put(List.of(create(P1, 11, 201, "e", "x"), create(P1, 12, 202, "f", "x")));
    assertEquals(List.of("50:o:p", "201:e:x", "202:f:x"), dsql.rows());
    Map<TopicPartition, OffsetAndMetadata> current = offsets(P0, 6L, P1, 13L);
    assertSame(current, task.preCommit(current)); // the next put cleared it
  }
}
