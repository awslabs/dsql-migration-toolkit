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
  private final List<String> columns;
  private final List<Object> values;
  private final List<String> pkColumns;
  private final List<Object> pkValues;

  private ChangeEvent(
      String table,
      boolean delete,
      List<String> columns,
      List<Object> values,
      List<String> pkColumns,
      List<Object> pkValues) {
    this.table = table;
    this.delete = delete;
    this.columns = columns;
    this.values = values;
    this.pkColumns = pkColumns;
    this.pkValues = pkValues;
  }

  static ChangeEvent upsert(
      String table,
      List<String> columns,
      List<Object> values,
      List<String> pkColumns,
      List<Object> pkValues) {
    return new ChangeEvent(table, false, List.copyOf(columns), values, List.copyOf(pkColumns), pkValues);
  }

  static ChangeEvent delete(String table, List<String> pkColumns, List<Object> pkValues) {
    return new ChangeEvent(table, true, List.of(), List.of(), List.copyOf(pkColumns), pkValues);
  }

  String table() {
    return table;
  }

  boolean isDelete() {
    return delete;
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
