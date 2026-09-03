// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class BatchesTest {

  @Test
  void partitionsEvenly() {
    List<List<Integer>> chunks = Batches.partition(List.of(1, 2, 3, 4), 2);
    assertEquals(2, chunks.size());
    assertEquals(List.of(1, 2), chunks.get(0));
    assertEquals(List.of(3, 4), chunks.get(1));
  }

  @Test
  void partitionsWithRemainder() {
    List<List<Integer>> chunks = Batches.partition(List.of(1, 2, 3, 4, 5), 2);
    assertEquals(3, chunks.size());
    assertEquals(List.of(5), chunks.get(2));
  }

  @Test
  void emptyInputYieldsNoChunks() {
    assertTrue(Batches.partition(List.<Integer>of(), 3).isEmpty());
  }

  @Test
  void rejectsNonPositiveSize() {
    assertThrows(IllegalArgumentException.class, () -> Batches.partition(List.of(1), 0));
  }

  // --- byte-budget overload ---------------------------------------------------

  @Test
  void byteBudgetSplitsWideRowsBelowBudgetBeforeRowCap() {
    // Row cap is 1000, but each "row" is estimated at 400 bytes and the budget is 1000
    // bytes: only 2 rows fit per chunk (2*400=800 <= 1000, a 3rd would be 1200 > 1000), so
    // 5 wide rows split into chunks of 2,2,1 -- long before the row cap is reached.
    List<Integer> rows = List.of(1, 2, 3, 4, 5);
    List<List<Integer>> chunks = Batches.partition(rows, 1000, 1000L, r -> 400L);
    assertEquals(3, chunks.size());
    assertEquals(2, chunks.get(0).size());
    assertEquals(2, chunks.get(1).size());
    assertEquals(1, chunks.get(2).size());
    // Every chunk's estimated bytes stays at/below the budget.
    for (List<Integer> chunk : chunks) {
      assertTrue(chunk.size() * 400L <= 1000L, "chunk exceeds byte budget");
    }
  }

  @Test
  void byteBudgetStillChunksTinyRowsByRowCount() {
    // Tiny rows (1 byte each) never approach the byte budget, so chunking falls back to the
    // row-count cap: 5 rows with size 2 -> 2,2,1 (identical to the row-count-only overload).
    List<Integer> rows = List.of(1, 2, 3, 4, 5);
    List<List<Integer>> chunks = Batches.partition(rows, 2, 1_000_000L, r -> 1L);
    assertEquals(3, chunks.size());
    assertEquals(List.of(1, 2), chunks.get(0));
    assertEquals(List.of(3, 4), chunks.get(1));
    assertEquals(List.of(5), chunks.get(2));
  }

  @Test
  void byteBudgetKeepsASingleOversizedRowAsItsOwnChunk() {
    // An item larger than the whole budget cannot be split, so it forms its own chunk
    // (bounded downstream by DSQL's 2 MiB row limit + the 1 MiB per-value guard).
    List<Integer> rows = List.of(1, 2, 3);
    List<List<Integer>> chunks = Batches.partition(rows, 1000, 100L, r -> 5000L);
    assertEquals(3, chunks.size());
    for (List<Integer> chunk : chunks) {
      assertEquals(1, chunk.size());
    }
  }

  @Test
  void byteBudgetRejectsNonPositiveBudget() {
    assertThrows(
        IllegalArgumentException.class, () -> Batches.partition(List.of(1), 10, 0L, r -> 1L));
  }
}
