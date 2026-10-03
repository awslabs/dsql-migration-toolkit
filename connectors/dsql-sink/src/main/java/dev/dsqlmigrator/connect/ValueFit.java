// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Custom Aurora DSQL Kafka Connect sink connector.
 * The pre-write value-fit guard: pure, offline unit-tested (ValueFitTest). The JDBC catalog
 * read that feeds it lives in DsqlSinkTask and is validated live.
 */
package dev.dsqlmigrator.connect;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Refuse, never silently change, a number its Aurora DSQL column cannot hold exactly.
 *
 * <p>Aurora DSQL ACCEPTS three kinds of value and stores something else, with no error:
 *
 * <ul>
 *   <li>a number other than 0 or 1 bound to a {@code boolean} column: the sink used to coerce
 *       any non-zero number to {@code true} (and {@code 0.5} to {@code false}). A MySQL
 *       {@code TINYINT(1)} holds -128..127 with no DDL at all, and a {@code TINYINT(1)} widened
 *       to {@code INT} (or a PostgreSQL {@code boolean} changed to an integer type) does the same;
 *   <li>a number with non-zero digits beyond the scale of a {@code numeric(p,s)} column: the
 *       assignment cast rounds it (for example after {@code DECIMAL(10,2)} became
 *       {@code DECIMAL(12,4)} on the source);
 *   <li>a fractional number for a {@code smallint}/{@code integer}/{@code bigint} column: the
 *       assignment cast rounds it to a whole number.
 * </ul>
 *
 * <p>Such a row is dead-lettered BEFORE any write ({@code DsqlSinkTask.put}), with a reason
 * naming the column, its type and the rule, never the value. A value that fits is bound
 * exactly as before -- the same Java object through the same bind path -- so the steady state
 * is byte-identical. Precision/range overflow is not checked here: Aurora DSQL already rejects
 * it loudly (SQLSTATE 22003).
 *
 * <p><b>Default numeric.</b> A {@code numeric} declared without precision is stored by Aurora
 * DSQL at its documented default {@code numeric(18,6)}; Schema Conversion warns about it and
 * Validation compares such a column at scale 6. So a value from an UNCONSTRAINED source
 * {@code numeric} (Debezium {@code VariableScaleDecimal}) is exempt from the scale rule when
 * the target is that default -- whether the catalog reports it as NULL/NULL or as 18/6. The
 * Full Load applies the same rule (core/exporter.py {@code value_fit_rule}); the shared verdict
 * table is tests/fixtures/value_fit_contract.tsv.
 *
 * <p>Not checked (documented residuals, the same on both write paths): a {@code double}/
 * {@code float} value for a {@code numeric} column (PostgreSQL first renders it to 15
 * significant digits, so exact detection is not reliable), and any value for a
 * {@code real}/{@code double precision} column.
 */
final class ValueFit {

  private ValueFit() {}

  /**
   * The reason's leading phrase. The control plane classifies on it:
   * {@code cdc_dlq.parse_dlq_log_message} assigns the synthetic {@code VALUE_DOES_NOT_FIT} code
   * when no SQLSTATE was logged. Keep it byte-identical if the reason is ever reworded.
   */
  static final String REASON_PREFIX = "Value does not fit target column '";

  /**
   * One bounded catalog statement per table: only that table's rows, names bound as parameters.
   * {@code DsqlDialect} always quotes identifiers, so the exact-case names in the event match
   * {@code information_schema}.
   */
  static final String CATALOG_SQL =
      "SELECT column_name, data_type, numeric_precision, numeric_scale "
          + "FROM information_schema.columns WHERE table_schema = ? AND table_name = ?";

  /** How long a successfully read table's column types are trusted. */
  static final long TTL_NANOS = TimeUnit.MINUTES.toNanos(10);

  /** How soon an unreadable / invisible table's column types are read again. */
  static final long UNKNOWN_RETRY_NANOS = TimeUnit.SECONDS.toNanos(60);

  /**
   * A refusal from an entry older than this re-reads the table first, so a column recreated
   * wider out of band does not keep refusing values that now fit.
   */
  static final long RECHECK_BEFORE_REFUSE_NANOS = TimeUnit.SECONDS.toNanos(30);

  /** The guarded target kinds. Every other type is not guarded (absent from the map). */
  enum Kind {
    BOOLEAN,
    INTEGRAL,
    NUMERIC
  }

  /**
   * One guarded target column.
   *
   * @param scale the decimal places the column keeps (NUMERIC only; 6 for the default numeric)
   * @param defaultNumeric a bare {@code numeric}, or an explicit {@code numeric(18,6)} -- the
   *     one target the unconstrained-source exemption applies to
   * @param sqlName the type as the reason names it (value-free)
   */
  record TargetType(Kind kind, int scale, boolean defaultNumeric, String sqlName) {

    /**
     * Map one {@code information_schema.columns} row to a guarded type, or {@code null} when
     * the type is not guarded. {@code precision}/{@code scale} are null for a numeric declared
     * without them.
     */
    static TargetType fromCatalog(String dataType, Integer precision, Integer scale) {
      if (dataType == null) {
        return null;
      }
      switch (dataType.trim().toLowerCase(java.util.Locale.ROOT)) {
        case "boolean":
          return new TargetType(Kind.BOOLEAN, 0, false, "boolean");
        case "smallint":
          return new TargetType(Kind.INTEGRAL, 0, false, "smallint");
        case "integer":
          return new TargetType(Kind.INTEGRAL, 0, false, "integer");
        case "bigint":
          return new TargetType(Kind.INTEGRAL, 0, false, "bigint");
        case "numeric":
          if (precision == null || scale == null) {
            return new TargetType(
                Kind.NUMERIC, 6, true, "numeric (Aurora DSQL default numeric(18,6))");
          }
          return new TargetType(
              Kind.NUMERIC,
              scale,
              precision == 18 && scale == 6,
              "numeric(" + precision + "," + scale + ")");
        default:
          return null;
      }
    }
  }

  /** Why a value would change, each with its value-free phrase. */
  enum Rule {
    NOT_ZERO_OR_ONE("a number other than 0 or 1 would be stored as true or false"),
    FRACTION("a number with a fractional part would be rounded to a whole number"),
    SCALE("a number with more than %d decimal places would be rounded");

    private final String phrase;

    Rule(String phrase) {
      this.phrase = phrase;
    }

    String phrase(TargetType target) {
      return this == SCALE ? String.format(java.util.Locale.ROOT, phrase, target.scale()) : phrase;
    }
  }

  /** True when {@code n} is exactly 0 or 1. An unknown {@link Number} subclass is not. */
  static boolean isZeroOrOne(Number n) {
    if (n instanceof Byte || n instanceof Short || n instanceof Integer || n instanceof Long) {
      long l = n.longValue();
      return l == 0L || l == 1L;
    }
    if (n instanceof BigDecimal bd) {
      return bd.signum() == 0 || bd.compareTo(BigDecimal.ONE) == 0;
    }
    if (n instanceof BigInteger bi) {
      return bi.signum() == 0 || bi.equals(BigInteger.ONE);
    }
    if (n instanceof Double || n instanceof Float) {
      double d = n.doubleValue();
      return d == 0.0 || d == 1.0;
    }
    return false;
  }

  private static boolean isKnownNumber(Object v) {
    return v instanceof Byte
        || v instanceof Short
        || v instanceof Integer
        || v instanceof Long
        || v instanceof BigDecimal
        || v instanceof BigInteger
        || v instanceof Double
        || v instanceof Float;
  }

  /**
   * The rule {@code v} breaks for {@code target}, or {@code null} when it is stored exactly.
   * Never throws. {@code unconstrainedSource} is consulted only on the scale slow path.
   */
  static Rule rule(Object v, TargetType target, BooleanSupplier unconstrainedSource) {
    return ruleFor(v, target, unconstrainedSource, (s, ignored) -> s.getAsBoolean(), null);
  }

  // The shared core. The context + BiPredicate shape keeps the hot path allocation-free: the
  // caller passes a non-capturing function, so no per-value lambda is built.
  private static <C> Rule ruleFor(
      Object v, TargetType target, C ctx, BiPredicate<C, String> unconstrained, String column) {
    if (!(v instanceof Number) || target == null) {
      return null;
    }
    switch (target.kind()) {
      case BOOLEAN:
        // An unknown Number subclass is left to the bind backstop (bound as-is, so Aurora
        // DSQL rejects it instead of storing a guessed boolean).
        return isKnownNumber(v) && !isZeroOrOne((Number) v) ? Rule.NOT_ZERO_OR_ONE : null;
      case INTEGRAL:
        if (v instanceof BigDecimal bd) {
          return bd.scale() > 0 && bd.setScale(0, RoundingMode.DOWN).compareTo(bd) != 0
              ? Rule.FRACTION
              : null;
        }
        if (v instanceof Double || v instanceof Float) {
          double d = ((Number) v).doubleValue();
          return Double.isFinite(d) && d != Math.rint(d) ? Rule.FRACTION : null;
        }
        return null;
      case NUMERIC:
        // Fast path: a scale the column keeps needs no arithmetic and no allocation.
        if (!(v instanceof BigDecimal bd) || bd.scale() <= target.scale()) {
          return null;
        }
        if (bd.setScale(target.scale(), RoundingMode.DOWN).compareTo(bd) == 0) {
          return null; // only trailing zeros beyond the scale: stored exactly
        }
        if (target.defaultNumeric() && unconstrained.test(ctx, column)) {
          return null; // DSQL's documented numeric(18,6) default, warned in Schema Conversion
        }
        return Rule.SCALE;
      default:
        return null;
    }
  }

  /** The value-free dead-letter reason. Never contains the value, a {@code " | "} or a SQLSTATE. */
  static String reason(String column, TargetType target, Rule rule) {
    return REASON_PREFIX
        + column
        + "' of type "
        + target.sqlName()
        + ": "
        + rule.phrase(target)
        + ". Not applied and never retried: the Aurora DSQL column needs a type that holds the"
        + " source values.";
  }

  /**
   * The reason for the FIRST column of {@code event} whose value the target would change, or
   * {@code null}. A delete is never refused: the key of a refused insert never reached the
   * target, so a delete matching 0 rows is correct (and a boolean key outside 0/1 is still
   * rejected by Aurora DSQL through the bind backstop). A column absent from {@code cols} is
   * not checked (fail open).
   */
  static String refusal(
      ChangeEvent event, Map<String, TargetType> cols, Predicate<String> unconstrainedSource) {
    return refusal(event, cols, unconstrainedSource, Predicate::test);
  }

  /** {@link #refusal(ChangeEvent, Map, Predicate)} with an allocation-free source check. */
  static <C> String refusal(
      ChangeEvent event,
      Map<String, TargetType> cols,
      C ctx,
      BiPredicate<C, String> unconstrainedSource) {
    if (event.isDelete() || cols.isEmpty()) {
      return null;
    }
    List<String> columns = event.columns();
    List<Object> values = event.values();
    int n = Math.min(columns.size(), values.size());
    for (int i = 0; i < n; i++) {
      Object v = values.get(i);
      if (!(v instanceof Number)) {
        continue; // before the map lookup: most cells are not numbers
      }
      String column = columns.get(i);
      TargetType target = cols.get(column);
      if (target == null) {
        continue;
      }
      Rule r = ruleFor(v, target, ctx, unconstrainedSource, column);
      if (r != null) {
        return reason(column, target, r);
      }
    }
    return null;
  }

  /**
   * {@code {schema, table}} for a {@code schema.table} target, or {@code null} for any other
   * shape (fail open: the table is then not guarded).
   */
  static String[] catalogKey(String table) {
    if (table == null) {
      return null;
    }
    String[] parts = table.split("\\.", -1);
    if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
      return null;
    }
    return parts;
  }

  /** The SQLSTATEs that mean a table's cached column types may be stale. */
  static boolean invalidatesTargetTypes(String sqlState) {
    return "42703".equals(sqlState) || "42P01".equals(sqlState) || "3F000".equals(sqlState);
  }

  /**
   * Per-task cache: table -> guarded column types. Bounded by captured tables x guarded
   * columns. Not thread-safe: {@code put()} is single-threaded per task.
   */
  static final class TargetTypeCache {

    private record Entry(Map<String, TargetType> cols, long loadedAt, long lifeNanos) {}

    private final Function<String, Map<String, TargetType>> loader;
    private final Map<String, Entry> entries = new HashMap<>();

    /**
     * @param loader returns the guarded columns of a table (possibly empty), or {@code null}
     *     when they are unknown (unreadable, or the table is not visible); it may throw a
     *     {@code RetriableException}, which caches nothing
     */
    TargetTypeCache(Function<String, Map<String, TargetType>> loader) {
      this.loader = loader;
    }

    Map<String, TargetType> get(String table, long nowNanos) {
      Entry entry = entries.get(table);
      if (entry != null && nowNanos - entry.loadedAt() < entry.lifeNanos()) {
        return entry.cols();
      }
      Map<String, TargetType> loaded = loader.apply(table);
      Entry fresh =
          loaded == null
              ? new Entry(Map.of(), nowNanos, UNKNOWN_RETRY_NANOS) // never final: retried
              : new Entry(Map.copyOf(loaded), nowNanos, TTL_NANOS);
      entries.put(table, fresh);
      return fresh.cols();
    }

    /** Age of the table's entry, or {@link Long#MAX_VALUE} when there is none. */
    long ageNanos(String table, long nowNanos) {
      Entry entry = entries.get(table);
      return entry == null ? Long.MAX_VALUE : nowNanos - entry.loadedAt();
    }

    void invalidate(String table) {
      entries.remove(table);
    }
  }
}
