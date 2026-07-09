/*
 * Custom Aurora DSQL Kafka Connect sink connector.
 * Pure SQL builders for the DSQL dialect (PostgreSQL wire protocol).
 * Offline unit-tested; validate exact column/value binding against a live
 * DSQL cluster before a production deploy.
 */
package dev.dsqlmigrator.connect;

import java.util.List;

/**
 * Builds DSQL-compatible DML for PK-keyed idempotent apply.
 *
 * <p>Debezium {@code c/r/u} map to an {@code INSERT ... ON CONFLICT (pk) DO
 * UPDATE} upsert; {@code d}/tombstones map to a {@code DELETE}. DSQL supports
 * {@code ON CONFLICT} (verified in design research) and does not use sequences
 * or triggers, so the generated SQL stays within DSQL constraints. Identifiers
 * are always double-quoted (embedded quotes doubled) to avoid keyword/casing
 * surprises.
 */
final class DsqlDialect {

  private DsqlDialect() {}

  /** Double-quote a single identifier, escaping embedded double quotes. */
  static String quoteIdent(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  /** Quote a possibly schema-qualified name ({@code schema.table}) part-wise. */
  static String quoteQualified(String name) {
    String[] parts = name.split("\\.");
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < parts.length; i++) {
      if (i > 0) {
        sb.append('.');
      }
      sb.append(quoteIdent(parts[i]));
    }
    return sb.toString();
  }

  /**
   * Build {@code INSERT INTO t (cols) VALUES (?..) ON CONFLICT (pk) DO UPDATE
   * SET nonPk = EXCLUDED.nonPk}. When every column is part of the PK there is
   * nothing to update, so {@code DO NOTHING} is emitted (still idempotent).
   * Value placeholders are ordered to match {@code columns}.
   */
  static String upsertSql(String table, List<String> columns, List<String> pkColumns) {
    if (columns.isEmpty()) {
      throw new IllegalArgumentException("upsert requires at least one column");
    }
    if (pkColumns.isEmpty()) {
      throw new IllegalArgumentException("upsert requires at least one pk column");
    }
    List<String> quotedCols = columns.stream().map(DsqlDialect::quoteIdent).toList();
    List<String> placeholders = columns.stream().map(c -> "?").toList();
    List<String> quotedPk = pkColumns.stream().map(DsqlDialect::quoteIdent).toList();
    List<String> nonPk = columns.stream().filter(c -> !pkColumns.contains(c)).toList();

    String head =
        "INSERT INTO "
            + quoteQualified(table)
            + " ("
            + String.join(", ", quotedCols)
            + ") VALUES ("
            + String.join(", ", placeholders)
            + ") ON CONFLICT ("
            + String.join(", ", quotedPk)
            + ") ";
    if (nonPk.isEmpty()) {
      return head + "DO NOTHING";
    }
    List<String> assignments =
        nonPk.stream().map(c -> quoteIdent(c) + " = EXCLUDED." + quoteIdent(c)).toList();
    return head + "DO UPDATE SET " + String.join(", ", assignments);
  }

  /** Build {@code DELETE FROM t WHERE pk1 = ? AND pk2 = ?}. */
  static String deleteSql(String table, List<String> pkColumns) {
    if (pkColumns.isEmpty()) {
      throw new IllegalArgumentException("delete requires at least one pk column");
    }
    List<String> conditions = pkColumns.stream().map(c -> quoteIdent(c) + " = ?").toList();
    return "DELETE FROM " + quoteQualified(table) + " WHERE " + String.join(" AND ", conditions);
  }
}
