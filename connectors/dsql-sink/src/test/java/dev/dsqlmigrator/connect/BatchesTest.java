// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.ToLongFunction;
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

  // --- row-weight overload (G15: a move on a re-keyed table modifies two rows) -----------

  @Test
  void rowWeightBoundsTheModifiedRowsNotTheItemCount() {
    // Weights 1,1,2,1 with size 3: [1,1] then [2,1] -- a 2 that would push a chunk to 4 starts
    // the next chunk, and a chunk that reaches the size is flushed.
    List<Integer> weights = List.of(1, 1, 2, 1);
    List<List<Integer>> chunks = Batches.partition(weights, 3, 1_000L, w -> 1L, w -> w);
    assertEquals(List.of(List.of(1, 1), List.of(2, 1)), chunks);
    // 2,999 plain rows and one move with size 3,000: never one chunk of 3,001 modified rows.
    List<Integer> rows = new ArrayList<>();
    for (int i = 0; i < 2999; i++) {
      rows.add(1);
    }
    rows.add(2);
    List<List<Integer>> split = Batches.partition(rows, 3000, Long.MAX_VALUE, w -> 1L, w -> w);
    assertEquals(2, split.size());
    assertEquals(2999, split.get(0).size());
    assertEquals(List.of(2), split.get(1));
    // A lone item heavier than the size still forms its own chunk.
    assertEquals(
        List.of(List.of(2), List.of(1)),
        Batches.partition(List.of(2, 1), 1, 1_000L, w -> 1L, w -> w));
  }

  @Test
  void weightOneChunksExactlyAsBeforeG15() {
    // Every CDC batch now goes through the weighted loop (a non-move event weighs 1), so with
    // every weight 1 it must chunk exactly as the pre-G15 loop did. The oracle is a verbatim
    // copy of that loop -- not the 4-arg overload, which now delegates to the weighted one.
    Random random = new Random(15L);
    for (int round = 0; round < 200; round++) {
      List<Long> items = new ArrayList<>();
      int n = random.nextInt(60);
      for (int i = 0; i < n; i++) {
        // Multiples of 50 with a +/-1 budget jitter: a chunk often lands exactly ON, or
        // exactly one past, the byte budget -- the edge an off-by-one would move.
        items.add(50L * (1 + random.nextInt(10)));
      }
      int size = 1 + random.nextInt(10);
      long budget = 50L * (1 + random.nextInt(40)) + random.nextInt(3) - 1;
      List<List<Long>> expected = preG15Partition(items, size, budget, b -> b);
      assertEquals(expected, Batches.partition(items, size, budget, b -> b), "round " + round);
      assertEquals(
          expected, Batches.partition(items, size, budget, b -> b, b -> 1), "round " + round);
    }
  }

  /** The byte-budget loop exactly as it was before G15: the oracle for the weight-1 path. */
  private static <T> List<List<T>> preG15Partition(
      List<T> items, int size, long maxBytes, ToLongFunction<T> sizer) {
    List<List<T>> chunks = new ArrayList<>();
    List<T> chunk = new ArrayList<>();
    long chunkBytes = 0;
    for (T item : items) {
      long itemBytes = sizer.applyAsLong(item);
      if (!chunk.isEmpty() && chunkBytes + itemBytes > maxBytes) {
        chunks.add(chunk);
        chunk = new ArrayList<>();
        chunkBytes = 0;
      }
      chunk.add(item);
      chunkBytes += itemBytes;
      if (chunk.size() >= size) {
        chunks.add(chunk);
        chunk = new ArrayList<>();
        chunkBytes = 0;
      }
    }
    if (!chunk.isEmpty()) {
      chunks.add(chunk);
    }
    return chunks;
  }
}
