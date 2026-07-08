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
        null, ChangeEvent.upsert(table, cols, vals, List.of(cols.get(0)), List.of(0)));
  }

  private static DsqlSinkTask.Applicable delete(String table) {
    return new DsqlSinkTask.Applicable(null, ChangeEvent.delete(table, List.of("id"), List.of(1)));
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
}
