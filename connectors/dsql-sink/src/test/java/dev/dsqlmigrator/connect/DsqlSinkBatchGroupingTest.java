// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Unit tests for DsqlSinkTask's contiguous same-SQL run grouping (the JDBC
 * executeBatch throughput path). The grouping MUST preserve apply order — only
 * consecutive events that render to identical SQL are coalesced — so a run
 * breaks whenever the table, column set, or upsert/delete kind changes. This is
 * the correctness guard for collapsing per-row round-trips into batched sends
 * against latency-bound DSQL.
 */
package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class DsqlSinkBatchGroupingTest {

  private static DsqlSinkTask.Applicable upsert(String table, List<String> cols) {
    List<Object> vals = new ArrayList<>();
    for (int i = 0; i < cols.size(); i++) {
      vals.add(i);
    }
    return new DsqlSinkTask.Applicable(
        null, ChangeEvent.upsert(table, cols, vals, List.of(cols.get(0)), List.of(0), 0L));
  }

  private static DsqlSinkTask.Applicable delete(String table) {
    return new DsqlSinkTask.Applicable(
        null, ChangeEvent.delete(table, List.of("id"), List.of(1), 0L));
  }

  /** Partition a chunk into run lengths using runEnd, mirroring applyChunkBatched. */
  private static List<Integer> runLengths(List<DsqlSinkTask.Applicable> chunk) {
    List<Integer> runs = new ArrayList<>();
    int i = 0;
    while (i < chunk.size()) {
      int end = DsqlSinkTask.runEnd(chunk, i);
      runs.add(end - i);
      i = end;
    }
    return runs;
  }

  @Test
  void allSameUpsertIsOneRun() {
    List<DsqlSinkTask.Applicable> chunk =
        List.of(
            upsert("orders", List.of("id", "amt")),
            upsert("orders", List.of("id", "amt")),
            upsert("orders", List.of("id", "amt")));
    assertEquals(List.of(3), runLengths(chunk));
  }

  @Test
  void upsertThenDeleteSamePkBreaksTheRun() {
    // The ordering-critical case: an upsert followed by a delete on the same
    // table must NOT be coalesced into one batch — order must be honored.
    List<DsqlSinkTask.Applicable> chunk =
        List.of(upsert("orders", List.of("id", "amt")), delete("orders"));
    assertEquals(List.of(1, 1), runLengths(chunk));
  }

  @Test
  void differentTableBreaksTheRun() {
    List<DsqlSinkTask.Applicable> chunk =
        List.of(
            upsert("orders", List.of("id", "amt")),
            upsert("orders", List.of("id", "amt")),
            upsert("payments", List.of("id", "amt")));
    assertEquals(List.of(2, 1), runLengths(chunk));
  }

  @Test
  void differentColumnSetBreaksTheRun() {
    // A partial-column update renders different SQL, so it cannot share a batch.
    List<DsqlSinkTask.Applicable> chunk =
        List.of(
            upsert("orders", List.of("id", "amt")),
            upsert("orders", List.of("id", "amt", "status")));
    assertEquals(List.of(1, 1), runLengths(chunk));
  }

  @Test
  void interleavedRunsRegroupContiguously() {
    // A B B A  -> runs of 1,2,1 (identical A's are NOT merged across the B run,
    // preserving order).
    List<DsqlSinkTask.Applicable> chunk =
        List.of(
            upsert("orders", List.of("id", "amt")),
            upsert("payments", List.of("id", "amt")),
            upsert("payments", List.of("id", "amt")),
            upsert("orders", List.of("id", "amt")));
    assertEquals(List.of(1, 2, 1), runLengths(chunk));
  }

  @Test
  void consecutiveDeletesSameTableCoalesce() {
    List<DsqlSinkTask.Applicable> chunk = List.of(delete("orders"), delete("orders"));
    assertEquals(List.of(2), runLengths(chunk));
  }

  // --- dedupeRunByPk: required for safe reWriteBatchedInserts -----------------

  private static DsqlSinkTask.Applicable upsertPk(int pk, int amt) {
    return new DsqlSinkTask.Applicable(
        null,
        ChangeEvent.upsert(
            "orders", List.of("id", "amt"), List.of(pk, amt), List.of("id"), List.of(pk), 0L));
  }

  private static List<Object> pkOf(DsqlSinkTask.Applicable a) {
    return a.event().pkValues();
  }

  @Test
  void dedupeKeepsLastImagePerPkAndPreservesOrder() {
    // pk 1 appears twice (amt 10 then 30), pk 2 once. Expect [pk1=30, pk2=20] --
    // last write for pk1 wins, order of surviving rows preserved.
    List<DsqlSinkTask.Applicable> chunk =
        List.of(upsertPk(1, 10), upsertPk(2, 20), upsertPk(1, 30));
    List<DsqlSinkTask.Applicable> out = DsqlSinkTask.dedupeRunByPk(chunk, 0, chunk.size());
    assertEquals(2, out.size());
    assertEquals(List.of(1), pkOf(out.get(0)));
    assertEquals(30, out.get(0).event().values().get(1)); // last amt for pk 1
    assertEquals(List.of(2), pkOf(out.get(1)));
  }

  @Test
  void dedupeNoDuplicatesIsIdentity() {
    // The perf-test workload: distinct auto-increment PKs -> dedup is a no-op.
    List<DsqlSinkTask.Applicable> chunk =
        List.of(upsertPk(1, 10), upsertPk(2, 20), upsertPk(3, 30));
    List<DsqlSinkTask.Applicable> out = DsqlSinkTask.dedupeRunByPk(chunk, 0, chunk.size());
    assertEquals(3, out.size());
    assertEquals(List.of(1), pkOf(out.get(0)));
    assertEquals(List.of(2), pkOf(out.get(1)));
    assertEquals(List.of(3), pkOf(out.get(2)));
  }

  @Test
  void dedupeHonorsStartEndWindow() {
    // Only [1,3) is deduped: index 0 (pk 1) is outside the window and untouched.
    List<DsqlSinkTask.Applicable> chunk =
        List.of(upsertPk(1, 10), upsertPk(2, 20), upsertPk(2, 25));
    List<DsqlSinkTask.Applicable> out = DsqlSinkTask.dedupeRunByPk(chunk, 1, 3);
    assertEquals(1, out.size());
    assertEquals(List.of(2), pkOf(out.get(0)));
    assertEquals(25, out.get(0).event().values().get(1));
  }
}
