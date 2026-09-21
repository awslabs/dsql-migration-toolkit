// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Custom Aurora DSQL Kafka Connect sink connector.
 */
package dev.dsqlmigrator.connect;

import java.util.List;

/**
 * A parsed Debezium change event ready to apply to DSQL.
 *
 * <p>An upsert carries the full set of {@code columns}/{@code values} (the
 * after-image) plus the {@code pkColumns}/{@code pkValues} used for the
 * {@code ON CONFLICT} target. A delete carries only the PK. Built by
 * {@link DebeziumEvents#parse}.
 */
final class ChangeEvent {

  private final String table;
  private final boolean delete;
  /**
   * True only for a Debezium TOMBSTONE (a keyed record with a null value). It applies
   * exactly like an {@code op=d} delete -- the same idempotent DELETE by key -- but it is
   * not a SECOND delete: Debezium emits both an {@code op=d} envelope AND a tombstone for
   * one source DELETE, so counting both made DeletesApplied report 2 for one deleted row
   * while Inserts/Updates stayed 1:1. The flag exists so the METRIC can skip it without
   * touching the apply path (dropping the tombstone from the apply would break the
   * key-only case and the log-compaction contract).
   */
  private final boolean tombstone;
  // Effect on the target's row count for the per-table net-rows monitor metric:
  // +1 for an insert (Debezium op c/r), -1 for a delete (op d), 0 for an update
  // (op u — an upsert of an existing row does not change the count). This is an
  // apply-order net over a committed chunk, so an insert+update of the same PK in
  // one chunk still nets +1; it is a lightweight monitor signal (approximate under
  // replay), NOT the exact reconciliation (that is Validation).
  private final int netRowDelta;
  private final List<String> columns;
  private final List<Object> values;
  private final List<String> pkColumns;
  private final List<Object> pkValues;
  // Debezium source commit time (epoch millis, from the event's source.ts_ms).
  // The sink computes end-to-end replication lag = apply-wall-clock - sourceTsMs at
  // apply time and emits it as the ReplicationLagMs monitor metric. 0 when the event
  // carried no source.ts_ms (e.g. a tombstone) — in which case lag is not recorded.
  private final long sourceTsMs;

  private ChangeEvent(
      String table,
      boolean delete,
      boolean tombstone,
      int netRowDelta,
      List<String> columns,
      List<Object> values,
      List<String> pkColumns,
      List<Object> pkValues,
      long sourceTsMs) {
    this.table = table;
    this.delete = delete;
    this.tombstone = tombstone;
    this.netRowDelta = netRowDelta;
    this.columns = columns;
    this.values = values;
    this.pkColumns = pkColumns;
    this.pkValues = pkValues;
    this.sourceTsMs = sourceTsMs;
  }

  /** An UPDATE (op u): applied as an idempotent upsert; net row delta 0. */
  static ChangeEvent upsert(
      String table,
      List<String> columns,
      List<Object> values,
      List<String> pkColumns,
      List<Object> pkValues,
      long sourceTsMs) {
    return new ChangeEvent(
        table, false, false, 0, List.copyOf(columns), values,
        List.copyOf(pkColumns), pkValues, sourceTsMs);
  }

  /** An INSERT (op c / snapshot r): same upsert apply, but net row delta +1. */
  static ChangeEvent insert(
      String table,
      List<String> columns,
      List<Object> values,
      List<String> pkColumns,
      List<Object> pkValues,
      long sourceTsMs) {
    return new ChangeEvent(
        table, false, false, 1, List.copyOf(columns), values,
        List.copyOf(pkColumns), pkValues, sourceTsMs);
  }

  /** A DELETE from an {@code op=d} envelope: applied by key, net row delta -1. */
  static ChangeEvent delete(
      String table, List<String> pkColumns, List<Object> pkValues, long sourceTsMs) {
    return new ChangeEvent(
        table, true, false, -1, List.of(), List.of(), List.copyOf(pkColumns), pkValues,
        sourceTsMs);
  }

  /**
   * A TOMBSTONE delete (keyed record, null value): applied identically to {@link #delete},
   * but flagged so the applied-ops metric does not count it as a second delete.
   */
  static ChangeEvent tombstone(
      String table, List<String> pkColumns, List<Object> pkValues, long sourceTsMs) {
    return new ChangeEvent(
        table, true, true, -1, List.of(), List.of(), List.copyOf(pkColumns), pkValues,
        sourceTsMs);
  }

  String table() {
    return table;
  }

  boolean isDelete() {
    return delete;
  }

  /**
   * True for a Debezium tombstone. Applied like any delete; excluded from the applied-ops
   * metric so one source DELETE is counted once (Debezium sends op=d AND a tombstone).
   */
  boolean isTombstone() {
    return tombstone;
  }

  /** An INSERT (Debezium op c / snapshot r): applied as an upsert, net row delta +1. */
  boolean isInsert() {
    return !delete && netRowDelta == 1;
  }

  /** An UPDATE (Debezium op u): applied as an idempotent upsert, net row delta 0. */
  boolean isUpdate() {
    return !delete && netRowDelta == 0;
  }

  /**
   * +1 insert / 0 update / -1 delete. Encodes the op kind (see {@link #isInsert()} /
   * {@link #isUpdate()} / {@link #isDelete()}); summed it also gives a table's net
   * row-count change over a committed chunk.
   */
  int netRowDelta() {
    return netRowDelta;
  }

  /** Source commit time (epoch millis) from source.ts_ms; 0 if unknown. */
  long sourceTsMs() {
    return sourceTsMs;
  }

  List<String> columns() {
    return columns;
  }

  List<Object> values() {
    return values;
  }

  List<String> pkColumns() {
    return pkColumns;
  }

  List<Object> pkValues() {
    return pkValues;
  }
}
