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
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  // --- byte-budget chunking (estimateModifiedBytes) ---------------------------

  /** An upsert of one wide row: pk + a body string of {@code bodyBytes} ASCII chars. */
  private static DsqlSinkTask.Applicable wideUpsert(int pk, int bodyBytes) {
    String body = "x".repeat(bodyBytes);
    return new DsqlSinkTask.Applicable(
        null,
        ChangeEvent.upsert(
            "docs", List.of("id", "body"), List.of(pk, body), List.of("id"), List.of(pk), 0L));
  }

  @Test
  void estimateModifiedBytesCountsUpsertAfterImageAndDeletePkOnly() {
    // Upsert modifies the after-image (pk scalar ~16 + a 100-byte string).
    DsqlSinkTask.Applicable up = wideUpsert(1, 100);
    long upBytes = DsqlSinkTask.estimateModifiedBytes(up.event());
    assertTrue(upBytes >= 100 && upBytes < 200, "upsert bytes ~= scalar + string body");
    // Delete modifies only the PK, so it is tiny regardless of the row's width.
    DsqlSinkTask.Applicable del = delete("docs");
    assertTrue(DsqlSinkTask.estimateModifiedBytes(del.event()) < 64, "delete sizes PK only");
  }

  @Test
  void estimateModifiedBytesCountsAMovesAfterImageAndOldKey() {
    // A move (G15) upserts its after-image AND deletes its old row in one transaction: it
    // counts both, never only its keys like a delete.
    ChangeEvent afterImage =
        ChangeEvent.upsert(
            "docs",
            List.of("lead", "id", "body"),
            List.of(9, 1, "x".repeat(100)),
            List.of("lead", "id"),
            List.of(9, 1),
            0L);
    ChangeEvent moved =
        afterImage.withSourceKey(List.of("id"), List.of(1)).withMovedFrom(List.of(5, 1));
    assertEquals(
        DsqlSinkTask.estimateModifiedBytes(afterImage)
            + DsqlSinkTask.estimateModifiedBytes(moved.moveDelete()),
        DsqlSinkTask.estimateModifiedBytes(moved));
    // An UPDATE whose old key was unknown, replaced by its source key: the same, by that key.
    ChangeEvent replaced =
        afterImage
            .withSourceKey(List.of("id"), List.of(1))
            .withOldKeyUnknown()
            .withMoveDeleteBySourceKey();
    assertEquals(
        DsqlSinkTask.estimateModifiedBytes(afterImage)
            + DsqlSinkTask.estimateModifiedBytes(replaced.moveDelete()),
        DsqlSinkTask.estimateModifiedBytes(replaced));
    // Still an unknown old key (the index not yet valid): a plain upsert, its image only.
    assertEquals(
        DsqlSinkTask.estimateModifiedBytes(afterImage),
        DsqlSinkTask.estimateModifiedBytes(
            afterImage.withSourceKey(List.of("id"), List.of(1)).withOldKeyUnknown()));
  }

  @Test
  void wideRowsSplitIntoMultipleChunksBelowByteBudget() {
    // Six rows of ~1 MiB each with a 3 MiB budget and a high row cap: byte-bounding must
    // split them into several chunks (never one 6 MiB transaction that DSQL would reject).
    int oneMiB = 1024 * 1024;
    long budget = 3L * 1024 * 1024;
    List<DsqlSinkTask.Applicable> batch =
        List.of(
            wideUpsert(1, oneMiB), wideUpsert(2, oneMiB), wideUpsert(3, oneMiB),
            wideUpsert(4, oneMiB), wideUpsert(5, oneMiB), wideUpsert(6, oneMiB));
    List<List<DsqlSinkTask.Applicable>> chunks =
        Batches.partition(batch, 3000, budget, a -> DsqlSinkTask.estimateModifiedBytes(a.event()));
    assertTrue(chunks.size() > 1, "wide rows must split under the byte budget");
    for (List<DsqlSinkTask.Applicable> chunk : chunks) {
      long bytes = 0;
      for (DsqlSinkTask.Applicable a : chunk) {
        bytes += DsqlSinkTask.estimateModifiedBytes(a.event());
      }
      assertTrue(bytes <= budget, "chunk exceeds byte budget: " + bytes);
    }
  }

  /** An insert of one JSON document of {@code docBytes} ASCII bytes, as the sink binds it. */
  private static DsqlSinkTask.Applicable jsonUpsert(int pk, int docBytes) {
    String doc = "{\"k\": \"" + "x".repeat(Math.max(0, docBytes - 9)) + "\"}";
    Object payload =
        DebeziumTypeConverter.convert(DebeziumTypeConverter.JSON_TYPE, doc); // PGobject(json)
    return new DsqlSinkTask.Applicable(
        null,
        ChangeEvent.upsert(
            "events", List.of("id", "payload"), List.of(pk, payload), List.of("id"),
            List.of(pk), 0L));
  }

  @Test
  void estimateModifiedBytesCountsAJsonValueByItsText() throws Exception {
    // G126: a PGobject -- MySQL JSON, PostgreSQL json/jsonb, an array rendered as jsonb, an
    // interval -- is sized by its text like a string. FAILS WITHOUT G126: a 100 KiB document
    // counted as a 16-byte scalar (32 bytes for the row).
    long jsonBytes = DsqlSinkTask.estimateModifiedBytes(jsonUpsert(1, 100 * 1024).event());
    assertTrue(jsonBytes >= 100 * 1024 && jsonBytes < 100 * 1024 + 64, "json: " + jsonBytes);
    // Multi-byte text counts its UTF-8 bytes, any CharSequence like a String, and a PGobject
    // with no value like a NULL.
    org.postgresql.util.PGobject jsonb = new org.postgresql.util.PGobject();
    jsonb.setType("jsonb");
    jsonb.setValue("[\"" + "한".repeat(1000) + "\"]"); // 3 bytes a character
    org.postgresql.util.PGobject empty = new org.postgresql.util.PGobject();
    empty.setType("interval");
    StringBuilder text = new StringBuilder("y".repeat(5000));
    ChangeEvent mixed =
        ChangeEvent.upsert(
            "events", List.of("id", "tags", "gap", "note"), List.of(1, jsonb, empty, text),
            List.of("id"), List.of(1), 0L);
    long mixedBytes = DsqlSinkTask.estimateModifiedBytes(mixed);
    assertTrue(mixedBytes >= 3004 + 5000 && mixedBytes < 3004 + 5000 + 64, "mixed: " + mixedBytes);
  }

  @Test
  void largeJsonDocumentsSplitUnderTheByteBudget() {
    // The repro's fetch: 158 inserts of ~100 KiB JSON documents, 15.4 MiB in all. Every chunk
    // stays within the 8 MiB budget, and so does every statement pgjdbc rewrites a run into
    // (one statement never spans two chunks), under Aurora DSQL's 10 MiB message limit. FAILS
    // WITHOUT G126: one chunk of all 158.
    List<DsqlSinkTask.Applicable> batch = new ArrayList<>();
    for (int i = 0; i < 158; i++) {
      batch.add(jsonUpsert(i, 100 * 1024));
    }
    List<List<DsqlSinkTask.Applicable>> chunks =
        Batches.partition(
            batch, 3000, DsqlSinkTask.MAX_BATCH_BYTES,
            a -> DsqlSinkTask.estimateModifiedBytes(a.event()));
    assertTrue(chunks.size() >= 2, "chunks: " + chunks.size());
    int rows = 0;
    for (List<DsqlSinkTask.Applicable> chunk : chunks) {
      long documentBytes = 0;
      for (DsqlSinkTask.Applicable a : chunk) {
        documentBytes +=
            ((org.postgresql.util.PGobject) a.event().values().get(1)).getValue().length();
      }
      assertTrue(documentBytes <= DsqlSinkTask.MAX_BATCH_BYTES, "chunk: " + documentBytes);
      rows += chunk.size();
    }
    assertEquals(158, rows);
  }

  @Test
  void tinyRowsStillChunkByRowCount() {
    // Narrow rows never approach the byte budget, so a small row cap still governs: 5 rows,
    // cap 2 -> chunks of 2,2,1 (byte-bounding is inert here).
    List<DsqlSinkTask.Applicable> batch =
        List.of(upsertPk(1, 10), upsertPk(2, 20), upsertPk(3, 30), upsertPk(4, 40), upsertPk(5, 50));
    List<List<DsqlSinkTask.Applicable>> chunks =
        Batches.partition(
            batch, 2, DsqlSinkTask.MAX_BATCH_BYTES, a -> DsqlSinkTask.estimateModifiedBytes(a.event()));
    assertEquals(3, chunks.size());
    assertEquals(2, chunks.get(0).size());
    assertEquals(2, chunks.get(1).size());
    assertEquals(1, chunks.get(2).size());
  }
}
