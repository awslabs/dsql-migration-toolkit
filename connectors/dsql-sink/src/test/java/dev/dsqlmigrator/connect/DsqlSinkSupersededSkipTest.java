// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * The skip of a conflicting change that a later change of its row replaces (sink plugin v53,
 * G23, kept as is by G25).
 *
 * A change Aurora DSQL rejects on a unique index (23505) is skipped -- neither held nor
 * dead-lettered -- when a LATER change of the same row on its partition replaces it entirely
 * (a plain delete, or a full image with the same column set): in the same put, or in the
 * partition's next poll (the one-poll look-ahead of its hold, which then ends REPLACED). A
 * re-keyed (Composite key) table never skips.
 *
 * The known limit, pinned by the first tests: the skipped row keeps its current values on the
 * target until that later change applies, so a change of ANOTHER row between the two on the
 * partition that waits for a value the skipped change gave up is held with the later change in
 * its tail, and is dead-lettered with its real reason once its 5 minutes run out. Never lost
 * silently: v52 dead-lettered both changes. G25 tried three ways around it and reverted each,
 * as each brought a worse regression (several are pinned below). The other tests pin that a
 * skip which strands nothing waits for nothing and dead-letters nothing, and that a hold after
 * a skip is the plain hold.
 *
 * All through ConnectSinkLoop (WorkerSinkTask 3.7) on the HoldHarness double.
 */
package dev.dsqlmigrator.connect;

import static dev.dsqlmigrator.connect.HoldHarness.P0;
import static dev.dsqlmigrator.connect.HoldHarness.P1;
import static dev.dsqlmigrator.connect.HoldHarness.SECOND;
import static dev.dsqlmigrator.connect.HoldHarness.create;
import static dev.dsqlmigrator.connect.HoldHarness.delete;
import static dev.dsqlmigrator.connect.HoldHarness.offsets;
import static dev.dsqlmigrator.connect.HoldHarness.task;
import static dev.dsqlmigrator.connect.HoldHarness.tombstone;
import static dev.dsqlmigrator.connect.HoldHarness.update;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dsqlmigrator.connect.HoldHarness.FakeDsql;
import dev.dsqlmigrator.connect.HoldHarness.HoldTask;
import dev.dsqlmigrator.connect.HoldHarness.RecordingReporter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class DsqlSinkSupersededSkipTest {

  private static final long BUDGET = DsqlSinkTask.UNIQUE_HOLD_BUDGET_NANOS;

  /** A Connect loop on {@code task}'s clock; an idle poll waits 1 s (or the task's timeout). */
  private static ConnectSinkLoop connect(HoldTask task) {
    ConnectSinkLoop connect = new ConnectSinkLoop();
    connect.clock = ms -> task.advance(TimeUnit.MILLISECONDS.toNanos(ms));
    connect.defaultWaitMs = 1_000L;
    return connect;
  }

  /** {@link #connect} over P1 from offset 10, then P0 from 0: P1 is read first in each poll. */
  private static ConnectSinkLoop connectP1P0(HoldTask task) {
    ConnectSinkLoop connect = connect(task);
    connect.declare(P1, 10);
    connect.declare(P0, 0);
    return connect;
  }

  /** The target X(1)=a, W(5)=w, Y(9)=b. */
  private static FakeDsql xwy() {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(1, "a", "x0");
    dsql.seed(5, "w", "w0");
    dsql.seed(9, "b", "y0");
    return dsql;
  }

  /** The rewinds of {@code label}'s partition the loop ran, in order. */
  private static List<Long> rewinds(ConnectSinkLoop connect, String label) {
    return connect.trace.stream()
        .filter(t -> t.startsWith("rewind " + label + " -> "))
        .map(t -> Long.parseLong(t.substring(t.lastIndexOf(' ') + 1)))
        .toList();
  }

  // --- The known limit: a skip can strand another row's change, which is dead-lettered --------

  /** The target of the limit: X(1)=a, Z(2)=z, Y(9)=b. */
  private static FakeDsql xzy() {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(1, "a", "b1");
    dsql.seed(2, "z", "b2");
    dsql.seed(9, "b", "b9");
    return dsql;
  }

  /**
   * Poll and commit until the first dead letter, P1's commit at or below {@code held} meanwhile
   * (the held change and its tail are read again after a restart until then).
   */
  private static void untilTheFirstDeadLetter(
      ConnectSinkLoop connect, RecordingReporter dlq, long held) {
    for (int i = 0; i < 2_000 && dlq.records.isEmpty(); i++) {
      connect.poll();
      if (dlq.records.isEmpty()) {
        long committed = connect.commit(false).get(P1);
        assertTrue(committed <= held, "the commit passed the held change: " + committed);
      }
    }
    assertFalse(dlq.records.isEmpty(), "never dead-lettered");
  }

  @Test
  void aChangeWaitingForTheValueASkippedChangeGaveUpIsDeadLetteredAfterItsBudgetNeverLost()
      throws Exception {
    // users(id PK, email UNIQUE). Source order: Y b->y (P0@0), X a->b (P1@10), Z z->a (P1@11),
    // X changes again (P1@12, the same column set); P1 is read one poll ahead of P0. X@10
    // conflicts (Y still holds b) and X@12 replaces it: skipped, no hold, no dead letter. Z@11
    // needs the a X@10 gave up, but X keeps a on the target until X@12 applies, and X@12 sits
    // in Z@11's held tail: Z@11 waits out its 5 minutes and is dead-lettered with 23505 -- in
    // the DLQ, where the cut-over gate sees it; a reload of the table repairs it.
    FakeDsql dsql = xzy();
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    long start = task.now;
    ConnectSinkLoop connect = connectP1P0(task);
    connect.append(
        update(P1, 10, 1, "b", "b1"), update(P1, 11, 2, "a", "b2"), update(P1, 12, 1, "b", "b1x"));
    connect.start(task, task.ctx);
    connect.poll();
    assertEquals(Map.of(P1, 11L), task.heldOffsets()); // Z's change; X@10 was skipped
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    connect.append(update(P0, 0, 9, "y", "b9"));
    untilTheFirstDeadLetter(connect, dlq, 11L);
    assertEquals(List.of("P1@11"), dlq.names());
    assertEquals("23505", ((SQLException) dlq.causes.get(0)).getSQLState());
    assertTrue(task.now - start >= BUDGET, "dead-lettered before its 5 minutes");
    connect.drain(100);
    // X ends as on the source; Z keeps its old row: its change is the dead letter.
    assertEquals(List.of("1:b:b1x", "2:z:b2", "9:y:b9"), dsql.rows());
    assertEquals(List.of("P1@11"), dlq.names());
    assertEquals(13L, connect.commit(false).get(P1));
  }

  @Test
  void theSameWhenTheReplacementArrivesInThePartitionsNextPoll() throws Exception {
    // As above, but X@12 comes in P1's next poll: X@10 is held and read one poll further, where
    // X@12 replaces it, so that hold ends and P1 is read again from 11. Z@11 then waits for the
    // a X still holds, with X@12 in its tail, and is dead-lettered after its 5 minutes.
    FakeDsql dsql = xzy();
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    long start = task.now;
    ConnectSinkLoop connect = connectP1P0(task);
    connect.append(update(P1, 10, 1, "b", "b1"), update(P1, 11, 2, "a", "b2"));
    connect.start(task, task.ctx);
    connect.poll();
    assertEquals(Map.of(P1, 10L), task.heldOffsets()); // X@10, read one poll further
    connect.append(update(P1, 12, 1, "b", "b1x"));
    connect.poll(); // the look-ahead: X@12 replaces X@10
    assertTrue(task.heldOffsets().isEmpty());
    connect.poll(); // Z@11 fetched again: it waits for X's a
    assertEquals(Map.of(P1, 11L), task.heldOffsets());
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    connect.append(update(P0, 0, 9, "y", "b9"));
    untilTheFirstDeadLetter(connect, dlq, 11L);
    assertEquals(List.of("P1@11"), dlq.names());
    assertEquals("23505", ((SQLException) dlq.causes.get(0)).getSQLState());
    assertTrue(task.now - start >= BUDGET, "dead-lettered before its 5 minutes");
    connect.drain(100);
    assertEquals(List.of("1:b:b1x", "2:z:b2", "9:y:b9"), dsql.rows());
    assertEquals(List.of("P1@11"), dlq.names());
  }

  // --- A skip that strands nothing waits for nothing and dead-letters nothing ------------------

  @Test
  void aReplayedOlderImageWithAnotherRowsChangeBeforeItsReplacementIsSkippedWithNoHold()
      throws Exception {
    // A replay after a restart: the target already holds row 1 = b and row 2 = a. Row 1's older
    // image (a) conflicts, another row's change follows it, then row 1's newer image (b). The
    // change in between applies, so nothing waits: no hold, no dead letter, no capped commit.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(1, "b", "x");
    dsql.seed(2, "a", "x");
    dsql.seed(4, "q", "x");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(List.of(update(P1, 0, 1, "a", "x"), delete(P1, 1, 4), update(P1, 2, 1, "b", "x")));
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(List.of("1:b:x", "2:a:x"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
    Map<TopicPartition, OffsetAndMetadata> current = offsets(P1, 3L);
    assertSame(current, task.preCommit(current));
  }

  @Test
  void twoSkippedImagesOfOneRowAreNeverAppliedAgainAheadOfAHeldChange() throws Exception {
    // users: A(1)=a, B(2)=v, W(3)=w, Q(5)=q. Source order: B v->x (P1@0), A a->v (P0@0), W w->y
    // (P1@1), A v->w (P0@1), insert N(4)=v (P0@2), A keeps w (P0@3), delete A (P0@4 and its
    // tombstone), Q q->w (P1@2); P0 is read one poll ahead of P1. A@0 and A@1 are skipped (each
    // replaced by A's next change) and N@2 waits for B to free v. A redesign that remembered the
    // skips merged A's two into one and, once B freed v, applied A@0 ahead of the hold: A took v
    // back and N@2 was dead-lettered after its 5 minutes. Here N takes v as soon as B frees it.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(1, "a", "a0");
    dsql.seed(2, "v", "b0");
    dsql.seed(3, "w", "w0");
    dsql.seed(5, "q", "q0");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    long start = task.now;
    ConnectSinkLoop connect = connect(task);
    connect.declare(P0, 0);
    connect.declare(P1, 0);
    connect.append(
        update(P0, 0, 1, "v", "a0"), update(P0, 1, 1, "w", "a0"), create(P0, 2, 4, "v", "n0"),
        update(P0, 3, 1, "w", "a1"), delete(P0, 4, 1), tombstone(P0, 5, 1));
    connect.start(task, task.ctx);
    connect.poll();
    assertEquals(Map.of(P0, 2L), task.heldOffsets()); // N's insert; A@0 and A@1 were skipped
    connect.append(
        update(P1, 0, 2, "x", "b0"), update(P1, 1, 3, "y", "w0"), update(P1, 2, 5, "w", "q0"));
    connect.drain(2_000);
    assertEquals(List.of("2:x:b0", "3:y:w0", "4:v:n0", "5:w:q0"), dsql.rows()); // the source
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    assertTrue(task.now - start < 30 * SECOND, "waited " + (task.now - start) / SECOND + " s");
  }

  @Test
  void aPrimaryKeyChangeAndItsReversalSkipTheMootInsertWithNoHold() throws Exception {
    // The insert of 100005 conflicts with row 5's email and its delete follows in the same put:
    // the insert is moot, so it is skipped and nothing waits.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(List.of(create(P1, 0, 100005, "a", "b"), delete(P1, 1, 100005)));
    assertTrue(task.heldOffsets().isEmpty());
    Map<TopicPartition, OffsetAndMetadata> current = offsets(P1, 2L);
    assertSame(current, task.preCommit(current));
    assertEquals(List.of("5:a:b"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
  }

  /**
   * The re-read of a window on P1 onto a target that already holds its later state: X(1) kept
   * email b through {@code images} updates (bio changes), each followed by two updates of other
   * rows, then X b->c; W(5) took b (P0@0). The target holds X=c, W=b and every other row's image.
   * Returns the next free offset of P1.
   */
  private static long staleReplay(FakeDsql dsql, ConnectSinkLoop connect, int images) {
    dsql.seed(1, "c", "final");
    dsql.seed(5, "b", "w1");
    long off = 0;
    int other = 1000;
    for (int i = 0; i < images; i++) {
      connect.append(update(P1, off++, 1, "b", "x" + i)); // conflicts with W's b
      for (int g = 0; g < 2; g++) {
        int r = other++;
        dsql.seed(r, "e" + r, "r");
        connect.append(update(P1, off++, r, "e" + r, "r"));
      }
    }
    connect.append(update(P1, off++, 1, "c", "final"));
    connect.append(update(P0, 0, 5, "b", "w1"));
    return off;
  }

  @Test
  void aReplayOfOneRowsStaleImagesOverSeveralPollsDrainsWithoutWaiting() throws Exception {
    // Four of X's images, read four records per poll: each is skipped when the next one is in
    // its put, or held and replaced one poll further. A redesign that held X's first skipped
    // image instead paid the full 5 minutes per image (hours for a long window).
    FakeDsql dsql = new FakeDsql();
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq, Map.of(DsqlSinkConnectorConfig.BATCH_SIZE, "3000"));
    long start = task.now;
    ConnectSinkLoop connect = connect(task);
    connect.declare(P1, 0);
    connect.declare(P0, 0);
    connect.maxPerPartition = 4;
    long end = staleReplay(dsql, connect, 4);
    List<String> target = dsql.rows(); // already the source's later state
    connect.start(task, task.ctx);
    connect.drain(1_000);
    assertEquals(target, dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    assertTrue(task.now - start < 30 * SECOND, "waited " + (task.now - start) / SECOND + " s");
    assertEquals(end, connect.commit(false).get(P1));
  }

  @Test
  void aLiveChangeOnAnotherPartitionThatNeedsAValueFreedBehindAReplayIsNotDeadLettered()
      throws Exception {
    // The same replay, then two live changes: I(20) i->i2 on P1, and J(21) j->i on P0, which
    // needs the i that I gives up. With the replay's images held for minutes (the redesign
    // above), J's own 5 minutes ran out first and J's change was dead-lettered.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(20, "i", "i0");
    dsql.seed(21, "j", "j0");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq, Map.of(DsqlSinkConnectorConfig.BATCH_SIZE, "3000"));
    long start = task.now;
    ConnectSinkLoop connect = connect(task);
    connect.declare(P1, 0);
    connect.declare(P0, 0);
    connect.maxPerPartition = 4;
    long off = staleReplay(dsql, connect, 4);
    connect.append(update(P1, off, 20, "i2", "i1"), update(P0, 1, 21, "i", "j1"));
    connect.start(task, task.ctx);
    connect.drain(1_000);
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    assertTrue(dsql.rows().contains("21:i:j1"), dsql.rows()::toString);
    assertTrue(dsql.rows().contains("20:i2:i1"), dsql.rows()::toString);
    assertTrue(task.now - start < 30 * SECOND, "waited " + (task.now - start) / SECOND + " s");
  }

  /**
   * One P1 poll: X@10 a->b (S holds b for good; X@12 replaces it), Q@11 q->t (T holds t until
   * {@code tFreed}; Q@14 replaces it), X@12 b->c (applies), Z@13 z->r (R frees r on P0 in the
   * same poll) and Q@14 t->u; P0 then carries T t->t2 (when {@code tFreed}), R r->r2 and V v->z,
   * which needs the z Z gives up.
   */
  private static void twoSkipsThenAQuickConflict(boolean tFreed) throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(1, "a", "x0"); // X
    dsql.seed(3, "q", "q0"); // Q
    dsql.seed(4, "r", "r0"); // R
    dsql.seed(5, "z", "z0"); // Z
    dsql.seed(6, "v", "v0"); // V
    dsql.seed(7, "b", "s0"); // S
    dsql.seed(8, "t", "t0"); // T
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    long start = task.now;
    ConnectSinkLoop connect = connect(task);
    connect.declare(P1, 10);
    connect.declare(P0, 0);
    connect.append(
        update(P1, 10, 1, "b", "x1"),
        update(P1, 11, 3, "t", "q1"),
        update(P1, 12, 1, "c", "x2"),
        update(P1, 13, 5, "r", "z1"),
        update(P1, 14, 3, "u", "q2"));
    long p0 = 0;
    if (tFreed) {
      connect.append(update(P0, p0++, 8, "t2", "t1"));
    }
    connect.append(update(P0, p0++, 4, "r2", "r1"), update(P0, p0, 6, "z", "v1"));
    connect.start(task, task.ctx);
    connect.drain(10_000);
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    assertTrue(dsql.rows().contains("6:z:v1"), dsql.rows()::toString); // V's change applied
    assertTrue(dsql.rows().contains("5:r:z1"), dsql.rows()::toString);
    assertTrue(task.now - start < 30 * SECOND, "waited " + (task.now - start) / SECOND + " s");
  }

  @Test
  void twoRowsSkippedInOnePutDelayNeitherAQuickConflictAfterThem() throws Exception {
    // Z@13's own hold ends as soon as R frees r, and V's change on P0 then applies. A redesign
    // that kept one skip per partition (X@10, replaced by 14) held X@10 -- already replaced,
    // never able to apply -- for 5 minutes and dead-lettered V's change.
    twoSkipsThenAQuickConflict(true);
  }

  @Test
  void theSameWhenTheOtherSkippedRowsValueIsTakenForGood() throws Exception {
    // Q@11's t stays T's. A redesign that undid the skip held Q@11 for Z@13's conflict instead,
    // for 5 minutes, and dead-lettered V's change.
    twoSkipsThenAQuickConflict(false);
  }

  @Test
  void aDeadLetteredReplacementIsNotReadAgainAfterACrashWhileALaterChangeIsHeld()
      throws Exception {
    // X@10 (a->b, S holds b) is skipped: X@12 replaces it. X@12 is rejected for good (a CHECK
    // constraint, 23514) and dead-lettered. Z@13 then waits for R's change on P0, and the task
    // crashes. The commit is at the hold, never at the skipped change: a redesign that kept it
    // at the skip while any hold existed made the next owner dead-letter X@12 a second time.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(1, "a", "x0");
    dsql.seed(4, "r", "r0");
    dsql.seed(5, "z", "z0");
    dsql.seed(7, "b", "s0");
    dsql.failure =
        (sql, binds) ->
            sql.startsWith("INSERT")
                    && Integer.valueOf(1).equals(binds.get(0))
                    && "c".equals(binds.get(1))
                ? new SQLException("new row violates check constraint", "23514")
                : null;
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    ConnectSinkLoop connect = connect(task);
    connect.declare(P1, 10);
    connect.declare(P0, 0);
    connect.append(
        update(P1, 10, 1, "b", "x1"),
        create(P1, 11, 60, "e60", "i"),
        update(P1, 12, 1, "c", "x2"),
        update(P1, 13, 5, "r", "z1"),
        create(P1, 14, 61, "e61", "i"));
    connect.start(task, task.ctx);
    connect.poll();
    connect.commit(false);
    task.advance(10 * SECOND);
    connect.poll();
    assertEquals(Map.of(P1, 13L), task.heldOffsets());
    assertEquals(List.of("P1@12"), dlq.names());
    assertEquals(13L, connect.commit(false).get(P1)); // at the hold, not at the skip

    connect.crash();
    HoldTask next = task(dsql, dlq);
    connect.clock = ms -> next.advance(TimeUnit.MILLISECONDS.toNanos(ms));
    connect.start(next, next.ctx);
    connect.append(update(P0, 0, 4, "r2", "r1"));
    connect.drain(10_000);
    assertEquals(List.of("P1@12"), dlq.names()); // once
    assertTrue(dsql.rows().contains("5:r:z1"), dsql.rows()::toString);
    assertTrue(dsql.rows().contains("61:e61:i"), dsql.rows()::toString);
  }

  @Test
  void aHotRowWhoseValueTheTargetHoldsForGoodDoesNotPinItsPartitionsCommit() throws Exception {
    // S(7) holds b for good (a row the stream never frees). X(1) takes b on the source and is
    // updated every 20 s (full images keep b), each update followed by an insert of a new row:
    // every image of X is replaced by the next one, skipped or held and replaced one poll
    // further, so the commit keeps up with the stream. A redesign that kept the commit at each
    // skipped image until it was moot left the commit minutes behind.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(1, "a", "x0");
    dsql.seed(7, "b", "stray");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq, Map.of(DsqlSinkConnectorConfig.BATCH_SIZE, "3000"));
    long start = task.now;
    ConnectSinkLoop connect = connect(task);
    connect.declare(P1, 0);
    connect.start(task, task.ctx);
    long next = 0;
    long produceAt = start;
    long atSixMinutes = -1;
    List<Integer> inserted = new ArrayList<>();
    while (task.now - start < TimeUnit.MINUTES.toNanos(12)) {
      while (produceAt <= task.now) {
        connect.append(update(P1, next++, 1, "b", "x" + next));
        int id = 1000 + inserted.size();
        connect.append(create(P1, next++, id, "i" + id, "i"));
        inserted.add(id);
        produceAt += 20 * SECOND;
      }
      if (atSixMinutes < 0 && task.now - start >= TimeUnit.MINUTES.toNanos(6)) {
        atSixMinutes = next;
      }
      connect.poll();
      if (connect.lastFetched > 0) {
        task.advance(TimeUnit.MILLISECONDS.toNanos(50)); // a put with records takes 50 ms
      }
      connect.commit(false);
    }
    long committed = connect.committedOffsets().get(P1);
    assertTrue(committed >= atSixMinutes, "committed " + committed + ", 6-minute mark " + atSixMinutes);
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    for (int id : inserted.subList(0, inserted.size() - 1)) {
      assertTrue(dsql.has(id), "insert of " + id + " not applied");
    }
  }

  // --- A hold after a skip is the plain hold ---------------------------------------------------

  @Test
  void anOwnRowsConflictAfterItsSkippedImageIsEndedByTheNextPollsReplacement() throws Exception {
    // Source order: Y b->y (P0@0); X a->b (P1@10); X keeps b, new bio (P1@11); X b->c (P1@12);
    // W w->b (P0@1). P1 runs one poll ahead of P0. X@11 replaces X@10 but conflicts as well, so
    // X@10 is skipped and X@11 is held -- the conflicting change itself -- and read one poll
    // further, where X@12 replaces it. A redesign that held the skipped X@10 instead (paused at
    // once) dead-lettered it after 5 minutes.
    FakeDsql dsql = xwy();
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    long start = task.now;
    ConnectSinkLoop connect = connectP1P0(task);
    connect.append(update(P1, 10, 1, "b", "x1"), update(P1, 11, 1, "b", "x2"));
    connect.start(task, task.ctx);
    connect.poll();
    assertEquals(Map.of(P1, 11L), task.heldOffsets()); // the conflicting change itself
    assertFalse(task.ctx.paused.contains(P1)); // read one poll further first
    connect.append(
        update(P1, 12, 1, "c", "x2"), update(P0, 0, 9, "y", "y0"), update(P0, 1, 5, "b", "w0"));
    connect.drain(2_000);
    assertEquals(List.of("1:c:x2", "5:b:w0", "9:y:y0"), dsql.rows()); // the source
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    assertTrue(task.now - start < BUDGET / 2, "waited out the budget");
    assertEquals(13L, connect.commit(false).get(P1));
  }

  @Test
  void aChangeOnAnotherPartitionThatNeedsTheHeldRowsOldValueIsNotStranded() throws Exception {
    // As above, plus V v->a on P0 after W: V needs the a that X gives up (on the target, X@12
    // does). V is held until X@12 applies. With the skipped X@10 held and paused at once (the
    // redesign above), X stayed frozen at a and V's change was dead-lettered.
    FakeDsql dsql = xwy();
    dsql.seed(7, "v", "v0");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    long start = task.now;
    ConnectSinkLoop connect = connectP1P0(task);
    connect.append(update(P1, 10, 1, "b", "x1"), update(P1, 11, 1, "b", "x2"));
    connect.start(task, task.ctx);
    connect.poll();
    connect.append(
        update(P1, 12, 1, "c", "x2"), update(P0, 0, 9, "y", "y0"), update(P0, 1, 5, "b", "w0"),
        update(P0, 2, 7, "a", "v0"));
    connect.drain(2_000);
    assertEquals(List.of("1:c:x2", "5:b:w0", "7:a:v0", "9:y:y0"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    assertTrue(task.now - start < BUDGET / 2, "waited out the budget");
  }

  @Test
  void aRowThatKeepsChangingIsHeldAtItsLatestImageAndNotReadAgainEveryPoll() throws Exception {
    // X(1)=a, Y(9)=b. X a->b (P1@10) and two more changes of X that keep b (P1@11, P1@12) while
    // Y b->y (P0@0) lags. X@10 is skipped and X@11 held, read one poll further, where X@12
    // replaces it; P1 is read again from X@12, which is held in turn and not read again while it
    // waits. Once Y frees b, X@12 applies and P1 is rewound past it, once.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(1, "a", "x0");
    dsql.seed(9, "b", "y0");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    ConnectSinkLoop connect = connectP1P0(task);
    connect.append(update(P1, 10, 1, "b", "x1"), update(P1, 11, 1, "b", "x2"));
    connect.start(task, task.ctx);
    connect.poll();
    assertEquals(Map.of(P1, 11L), task.heldOffsets());
    assertFalse(task.ctx.paused.contains(P1)); // read one poll further
    connect.append(update(P1, 12, 1, "b", "x3"));
    connect.poll(); // X@12 replaces X@11: P1 read again from X@12
    assertTrue(task.heldOffsets().isEmpty());
    for (int i = 0; i < 20; i++) {
      connect.poll(); // Y still lags: X@12 alone, held at the row's latest image
    }
    assertEquals(Map.of(P1, 12L), task.heldOffsets());
    assertEquals(List.of(12L), rewinds(connect, "P1")); // not read again while it waits
    connect.append(update(P0, 0, 9, "y", "y0"));
    connect.drain(100);
    assertEquals(List.of("1:b:x3", "9:y:y0"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> "dead-lettered " + dlq.names());
    assertEquals(13L, connect.commit(false).get(P1));
    assertEquals(List.of(12L, 13L), rewinds(connect, "P1")); // past X@12 once it applied
  }

  @Test
  void whenTheReplacementComesOnlyAfterTheBudgetOnlyTheHeldChangeIsDeadLettered()
      throws Exception {
    // X a->b (P1@10) and X keeping b (P1@11), but X@12 is produced only after the 5 minutes, and
    // W takes b for good: X@11 (held) can never apply. It is dead-lettered with its real reason
    // ("a value the target already holds"); the skipped X@10 never is, and X@12 then applies.
    FakeDsql dsql = xwy();
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    long start = task.now;
    ConnectSinkLoop connect = connectP1P0(task);
    connect.append(update(P1, 10, 1, "b", "x1"), update(P1, 11, 1, "b", "x2"));
    connect.start(task, task.ctx);
    connect.poll();
    connect.append(update(P0, 0, 9, "y", "y0"), update(P0, 1, 5, "b", "w0"));
    for (int i = 0; i < 1_000 && !task.heldOffsets().isEmpty(); i++) {
      connect.poll(); // idle polls, each bounded by the hold's next try (10 s at most)
    }
    assertTrue(task.heldOffsets().isEmpty());
    assertTrue(task.now - start >= BUDGET);
    assertEquals(List.of("P1@11"), dlq.names());
    assertEquals("23505", ((SQLException) dlq.causes.get(0)).getSQLState());
    connect.append(update(P1, 12, 1, "c", "x2"));
    connect.drain(100);
    assertEquals(List.of("1:c:x2", "5:b:w0", "9:y:y0"), dsql.rows());
    assertEquals(List.of("P1@11"), dlq.names());
    assertEquals(13L, connect.commit(false).get(P1));
  }

  @Test
  void anotherRowsChangeWaitingForAValueTakenForGoodIsTheOnlyDeadLetter() throws Exception {
    // X(1)=a, Z(2)=z, W(5)=w, Y(9)=b. Source order: Y b->y (P0@0); X a->b (P1@10); Z z->a
    // (P1@11); X keeps b (P1@12); X b->c (P1@13); W w->b (P0@1). X@10 is skipped, Z@11 waits
    // for the a X still holds, and W then takes b for good, so X@10 could never have applied
    // either: Z@11 is dead-lettered with its real reason after its 5 minutes -- the only dead
    // letter.
    FakeDsql dsql = xwy();
    dsql.seed(2, "z", "z0");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    long start = task.now;
    ConnectSinkLoop connect = connectP1P0(task);
    connect.append(
        update(P1, 10, 1, "b", "x1"), update(P1, 11, 2, "a", "z1"), update(P1, 12, 1, "b", "x2"));
    connect.start(task, task.ctx);
    connect.poll();
    assertEquals(Map.of(P1, 11L), task.heldOffsets()); // Z's change itself
    connect.append(
        update(P1, 13, 1, "c", "x2"), update(P0, 0, 9, "y", "y0"), update(P0, 1, 5, "b", "w0"));
    connect.poll();
    assertTrue(task.ctx.paused.contains(P1)); // nothing of Z's row one poll further: paused
    connect.drain(3_000);
    assertEquals(List.of("P1@11"), dlq.names());
    assertEquals("23505", ((SQLException) dlq.causes.get(0)).getSQLState());
    // Z keeps its old row: its change is the dead letter. Every other row is the source's.
    assertEquals(List.of("1:c:x2", "2:z:z0", "5:b:w0", "9:y:y0"), dsql.rows());
    assertTrue(task.now - start < 2 * BUDGET, "a hold was made again and again");
    assertEquals(14L, connect.commit(false).get(P1));
  }
}
