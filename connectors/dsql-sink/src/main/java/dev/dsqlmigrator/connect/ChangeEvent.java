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

  private ChangeEvent(
      String table,
      boolean delete,
      int netRowDelta,
      List<String> columns,
      List<Object> values,
      List<String> pkColumns,
      List<Object> pkValues) {
    this.table = table;
    this.delete = delete;
    this.netRowDelta = netRowDelta;
    this.columns = columns;
    this.values = values;
    this.pkColumns = pkColumns;
    this.pkValues = pkValues;
  }

  /** An UPDATE (op u): applied as an idempotent upsert; net row delta 0. */
  static ChangeEvent upsert(
      String table,
      List<String> columns,
      List<Object> values,
      List<String> pkColumns,
      List<Object> pkValues) {
    return new ChangeEvent(table, false, 0, List.copyOf(columns), values, List.copyOf(pkColumns), pkValues);
  }

  /** An INSERT (op c / snapshot r): same upsert apply, but net row delta +1. */
  static ChangeEvent insert(
      String table,
      List<String> columns,
      List<Object> values,
      List<String> pkColumns,
      List<Object> pkValues) {
    return new ChangeEvent(table, false, 1, List.copyOf(columns), values, List.copyOf(pkColumns), pkValues);
  }

  static ChangeEvent delete(String table, List<String> pkColumns, List<Object> pkValues) {
    return new ChangeEvent(table, true, -1, List.of(), List.of(), List.copyOf(pkColumns), pkValues);
  }

  String table() {
    return table;
  }

  boolean isDelete() {
    return delete;
  }

  /** +1 insert / 0 update / -1 delete — for the per-table net-rows monitor metric. */
  int netRowDelta() {
    return netRowDelta;
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
