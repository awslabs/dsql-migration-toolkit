// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Unit tests for the pre-write value-fit guard (ValueFit).
 *
 * Regression guard for a silent wrong value on the CDC path: Aurora DSQL ACCEPTS a number
 * other than 0/1 bound to a boolean column (the sink coerced it to true), a number with more
 * decimal places than a numeric(p,s) keeps, and a fraction for an integer column (both rounded
 * by the assignment cast) -- with no error, no dead letter, no log line. Only a CHECKSUM saw
 * it. The guard must refuse exactly those, name the column and type but never the value, and
 * leave every value that fits untouched. The verdict table is shared with the Full Load
 * (tests/fixtures/value_fit_contract.tsv, copied to src/test/resources).
 */
package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.Test;

class ValueFitTest {

  private static final ValueFit.TargetType BOOL = ValueFit.TargetType.fromCatalog("boolean", null, null);
  private static final ValueFit.TargetType INT =
      ValueFit.TargetType.fromCatalog("integer", 32, 0);
  private static final ValueFit.TargetType NUM_10_2 =
      ValueFit.TargetType.fromCatalog("numeric", 10, 2);
  private static final BooleanSupplier CONSTRAINED = () -> false;
  private static final BooleanSupplier UNCONSTRAINED = () -> true;

  private static ValueFit.Rule rule(Object v, ValueFit.TargetType t) {
    return ValueFit.rule(v, t, CONSTRAINED);
  }

  @Test
  void booleanTargetRefusesANumberOtherThanZeroOrOne() {
    for (Object bad :
        List.of(
            (short) 2, (short) 127, -1, 5L, new BigDecimal("0.5"), 0.5d, (byte) 3, 2.0f)) {
      assertEquals(ValueFit.Rule.NOT_ZERO_OR_ONE, rule(bad, BOOL), "value class " + bad.getClass());
    }
    for (Object fits :
        List.of(
            (short) 0,
            (short) 1,
            1,
            0L,
            new BigDecimal("1.0"),
            new BigDecimal("0.00"),
            1.0d,
            java.math.BigInteger.ONE)) {
      assertNull(rule(fits, BOOL), "value class " + fits.getClass());
    }
    // Not a number: never guarded (a Boolean binds as itself; a string never reaches a
    // boolean silently -- it fails 42804).
    assertNull(rule(Boolean.TRUE, BOOL));
    assertNull(rule("2", BOOL));
    assertNull(rule(null, BOOL));
  }

  @Test
  void numericScaleRefusesDigitsTheColumnWouldRound() {
    for (String bad : List.of("1.2345", "-0.001", "0.005")) {
      assertEquals(ValueFit.Rule.SCALE, rule(new BigDecimal(bad), NUM_10_2), bad);
    }
    // Trailing zeros beyond the scale are stored exactly, and so is anything at or under it.
    for (String fits : List.of("1.2300", "1.23", "1", "12345678.99")) {
      assertNull(rule(new BigDecimal(fits), NUM_10_2), fits);
    }
    assertNull(rule(5L, NUM_10_2));
    // Residual, documented: a double into numeric is not checked (PostgreSQL first renders it
    // to 15 significant digits, so exact detection is unreliable).
    assertNull(rule(1.2345d, NUM_10_2));
  }

  @Test
  void integerTargetRefusesAFraction() {
    assertEquals(ValueFit.Rule.FRACTION, rule(new BigDecimal("5.5"), INT));
    assertEquals(ValueFit.Rule.FRACTION, rule(0.5d, INT));
    assertEquals(ValueFit.Rule.FRACTION, rule(2.5f, INT));
    assertNull(rule(new BigDecimal("5.000"), INT));
    assertNull(rule(5L, INT));
    assertNull(rule(3.0d, INT));
    assertNull(rule(Double.NaN, INT)); // not finite: left to Aurora DSQL
    assertNull(rule(new BigDecimal("1E+3"), INT)); // negative scale is whole
  }

  @Test
  void defaultNumericAndUnconstrainedSource() {
    ValueFit.TargetType bare = ValueFit.TargetType.fromCatalog("numeric", null, null);
    ValueFit.TargetType explicitDefault = ValueFit.TargetType.fromCatalog("numeric", 18, 6);
    BigDecimal sevenPlaces = new BigDecimal("1.1234567");
    for (ValueFit.TargetType t : List.of(bare, explicitDefault)) {
      // A constrained source widened past 6 places is refused...
      assertEquals(ValueFit.Rule.SCALE, ValueFit.rule(sevenPlaces, t, CONSTRAINED));
      // ...but an UNCONSTRAINED source numeric into DSQL's documented default is not.
      assertNull(ValueFit.rule(sevenPlaces, t, UNCONSTRAINED));
    }
    // The exemption is ONLY for the default numeric target and never for an integer target.
    assertEquals(ValueFit.Rule.SCALE, ValueFit.rule(new BigDecimal("1.234"), NUM_10_2, UNCONSTRAINED));
    assertEquals(ValueFit.Rule.FRACTION, ValueFit.rule(new BigDecimal("5.5"), INT, UNCONSTRAINED));

    // The source check is consulted only on the slow path, never for a value that fits.
    AtomicInteger calls = new AtomicInteger();
    BooleanSupplier counting =
        () -> {
          calls.incrementAndGet();
          return false;
        };
    assertNull(ValueFit.rule(new BigDecimal("1.23"), NUM_10_2, counting));
    assertNull(ValueFit.rule(new BigDecimal("1.123456"), bare, counting));
    assertNull(ValueFit.rule(new BigDecimal("1.1234560"), bare, counting));
    assertEquals(0, calls.get());
  }

  @Test
  void catalogRowsMapToTargetTypes() {
    assertEquals(new ValueFit.TargetType(ValueFit.Kind.BOOLEAN, 0, false, "boolean"), BOOL);
    assertEquals("smallint", ValueFit.TargetType.fromCatalog("smallint", 16, 0).sqlName());
    assertEquals(ValueFit.Kind.INTEGRAL, ValueFit.TargetType.fromCatalog("smallint", 16, 0).kind());
    assertEquals(ValueFit.Kind.INTEGRAL, INT.kind());
    assertEquals("integer", INT.sqlName());
    assertEquals("bigint", ValueFit.TargetType.fromCatalog("bigint", 64, 0).sqlName());

    assertEquals(ValueFit.Kind.NUMERIC, NUM_10_2.kind());
    assertEquals(2, NUM_10_2.scale());
    assertFalse(NUM_10_2.defaultNumeric());
    assertEquals("numeric(10,2)", NUM_10_2.sqlName());

    ValueFit.TargetType bare = ValueFit.TargetType.fromCatalog("numeric", null, null);
    assertTrue(bare.defaultNumeric());
    assertEquals(6, bare.scale());
    assertEquals("numeric (Aurora DSQL default numeric(18,6))", bare.sqlName());
    assertTrue(ValueFit.TargetType.fromCatalog("numeric", 18, 6).defaultNumeric());

    // Unguarded types are absent from the map.
    assertNull(ValueFit.TargetType.fromCatalog("text", null, null));
    assertNull(ValueFit.TargetType.fromCatalog("double precision", 53, null));
    assertNull(ValueFit.TargetType.fromCatalog("real", 24, null));
    assertNull(ValueFit.TargetType.fromCatalog(null, null, null));
  }

  @Test
  void reasonIsValueFreeAndParserSafe() {
    ChangeEvent flag =
        ChangeEvent.upsert(
            "shop.orders",
            List.of("id", "flag"),
            List.of(7L, (short) 97),
            List.of("id"),
            List.of(7L),
            0L);
    ChangeEvent amount =
        ChangeEvent.upsert(
            "shop.orders",
            List.of("id", "amount"),
            List.of(7L, new BigDecimal("8765.4321")),
            List.of("id"),
            List.of(7L),
            0L);
    String flagReason = ValueFit.refusal(flag, Map.of("flag", BOOL), c -> false);
    String amountReason = ValueFit.refusal(amount, Map.of("amount", NUM_10_2), c -> false);
    assertNotNull(flagReason);
    assertNotNull(amountReason);
    for (String reason : List.of(flagReason, amountReason)) {
      assertTrue(reason.startsWith(ValueFit.REASON_PREFIX), reason);
      // No value, and nothing the Python parser keys other classes on.
      for (String forbidden :
          List.of("97", "8765", "4321", " | ", "exceeds DSQL's", "Cannot apply a source TRUNCATE",
              "sqlstate", "Value for column")) {
        assertFalse(reason.contains(forbidden), "'" + forbidden + "' in: " + reason);
      }
    }
    assertTrue(
        flagReason.contains("'flag' of type boolean: a number other than 0 or 1"), flagReason);
    assertTrue(amountReason.contains("numeric(10,2)"), amountReason);
    assertTrue(amountReason.contains("more than 2 decimal places"), amountReason);
  }

  @Test
  void aMysqlTimeDurationIsRefusedByNameForATimeColumnOnly() throws Exception {
    // Bound as an interval (the sink's conversion of a TIME outside a day): a time column
    // used to reject it with a bind error that read as "the column type changed".
    org.postgresql.util.PGobject duration = new org.postgresql.util.PGobject();
    duration.setType("interval");
    duration.setValue("838:59:59");
    ChangeEvent shift =
        ChangeEvent.upsert(
            "shop.shifts", List.of("id", "worked"), List.of(1L, duration), List.of("id"),
            List.of(1L), 0L);
    ValueFit.TargetType time = ValueFit.TargetType.fromCatalog("time without time zone", null, null);
    String reason = ValueFit.refusal(shift, Map.of("worked", time), c -> false);
    assertNotNull(reason);
    assertTrue(reason.startsWith(ValueFit.REASON_PREFIX), reason);
    assertTrue(reason.contains("'worked' of type time: a MySQL TIME outside"), reason);
    assertFalse(reason.contains("838"), reason); // value-free
    // A column remapped to interval is not guarded: the value lands.
    assertNull(ValueFit.TargetType.fromCatalog("interval", null, null));
    assertNull(ValueFit.refusal(shift, Map.of(), c -> false));
  }

  @Test
  void refusalSkipsDeletesUnknownColumnsAndNamesTheFirstUnfitColumn() {
    ChangeEvent delete =
        ChangeEvent.delete("shop.orders", List.of("id"), List.of(new BigDecimal("5.5")), 0L);
    assertNull(ValueFit.refusal(delete, Map.of("id", INT), c -> false));

    ChangeEvent upsert =
        ChangeEvent.upsert(
            "shop.orders",
            List.of("id", "a", "b", "note"),
            List.of(1L, new BigDecimal("1.5"), (short) 2, "text"),
            List.of("id"),
            List.of(1L),
            0L);
    // Columns absent from the map are not checked (fail open).
    assertNull(ValueFit.refusal(upsert, Map.of("note", BOOL), c -> false));
    assertNull(ValueFit.refusal(upsert, Map.of(), c -> false));
    // Two unfit columns: the first is named.
    String reason = ValueFit.refusal(upsert, Map.of("a", INT, "b", BOOL), c -> false);
    assertNotNull(reason);
    assertTrue(reason.startsWith(ValueFit.REASON_PREFIX + "a' of type integer"), reason);
  }

  @Test
  void catalogKey() {
    assertArrayEquals(new String[] {"shop", "orders"}, ValueFit.catalogKey("shop.orders"));
    for (String bad : new String[] {"orders", "a.b.c", "", ".x", "x.", null}) {
      assertNull(ValueFit.catalogKey(bad), String.valueOf(bad));
    }
  }

  @Test
  void invalidatesTargetTypes() {
    assertTrue(ValueFit.invalidatesTargetTypes("42703"));
    assertTrue(ValueFit.invalidatesTargetTypes("42P01"));
    assertTrue(ValueFit.invalidatesTargetTypes("3F000"));
    for (String other : new String[] {"42804", "23505", "22003", null}) {
      assertFalse(ValueFit.invalidatesTargetTypes(other), String.valueOf(other));
    }
  }

  @Test
  void cacheTtlUnknownRetryAndInvalidation() {
    AtomicInteger loads = new AtomicInteger();
    Map<String, ValueFit.TargetType> known = Map.of("flag", BOOL);
    ValueFit.TargetTypeCache cache =
        new ValueFit.TargetTypeCache(
            table -> {
              loads.incrementAndGet();
              return table.equals("shop.gone") ? null : known;
            });
    long t0 = 1_000_000_000L;
    assertEquals(known, cache.get("shop.orders", t0));
    assertEquals(known, cache.get("shop.orders", t0 + TimeUnit.MINUTES.toNanos(9)));
    assertEquals(1, loads.get());
    assertEquals(TimeUnit.MINUTES.toNanos(9), cache.ageNanos("shop.orders", t0 + TimeUnit.MINUTES.toNanos(9)));
    cache.get("shop.orders", t0 + ValueFit.TTL_NANOS);
    assertEquals(2, loads.get(), "a known entry is re-read after the TTL");

    // An unknown table is an EMPTY map, retried after 60 s and not before.
    loads.set(0);
    assertTrue(cache.get("shop.gone", t0).isEmpty());
    assertTrue(cache.get("shop.gone", t0 + TimeUnit.SECONDS.toNanos(59)).isEmpty());
    assertEquals(1, loads.get());
    cache.get("shop.gone", t0 + ValueFit.UNKNOWN_RETRY_NANOS);
    assertEquals(2, loads.get());

    // Invalidation forces a reload; an absent entry is infinitely old.
    loads.set(0);
    cache.invalidate("shop.orders");
    assertEquals(Long.MAX_VALUE, cache.ageNanos("shop.orders", t0));
    cache.get("shop.orders", t0);
    assertEquals(1, loads.get());

    // A loader failure propagates and caches nothing.
    AtomicInteger attempts = new AtomicInteger();
    ValueFit.TargetTypeCache failing =
        new ValueFit.TargetTypeCache(
            table -> {
              attempts.incrementAndGet();
              throw new RetriableException("transient");
            });
    assertThrows(RetriableException.class, () -> failing.get("shop.orders", t0));
    assertThrows(RetriableException.class, () -> failing.get("shop.orders", t0));
    assertEquals(2, attempts.get());
    assertEquals(Long.MAX_VALUE, failing.ageNanos("shop.orders", t0));
  }

  @Test
  void aValueThatFitsIsTheSameObject() {
    // The guard never produces a replacement value: it only says yes or no. Pinned so a later
    // "normalise while checking" change cannot slip a rescaled BigDecimal onto the bind path.
    BigDecimal fits = new BigDecimal("1.2300");
    ChangeEvent event =
        ChangeEvent.upsert(
            "shop.orders", List.of("id", "amount"), List.of(1L, fits), List.of("id"),
            List.of(1L), 0L);
    assertNull(ValueFit.refusal(event, Map.of("amount", NUM_10_2), c -> false));
    assertSame(fits, event.values().get(1));
  }

  /**
   * The verdict table shared with the Full Load (tests/fixtures/value_fit_contract.tsv; this
   * is its byte-identical copy). Columns: data_type, numeric_precision, numeric_scale,
   * ddl_type, value_kind (int|decimal|double), value, unconstrained_source, expected.
   */
  @Test
  void sharedValueFitContract() throws Exception {
    InputStream in = ValueFitTest.class.getResourceAsStream("/value_fit_contract.tsv");
    assertNotNull(in, "value_fit_contract.tsv missing from src/test/resources");
    int rows = 0;
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      String line;
      boolean header = true;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank() || line.startsWith("#")) {
          continue;
        }
        if (header) {
          header = false;
          continue;
        }
        String[] f = line.split("\t", -1);
        assertEquals(8, f.length, line);
        ValueFit.TargetType target =
            ValueFit.TargetType.fromCatalog(f[0], intOrNull(f[1]), intOrNull(f[2]));
        assertNotNull(target, line);
        Object value =
            switch (f[4]) {
              case "int" -> Long.valueOf(f[5]);
              case "decimal" -> new BigDecimal(f[5]);
              case "double" -> Double.valueOf(f[5]);
              default -> throw new AssertionError("unknown value_kind: " + line);
            };
        boolean unconstrained = Boolean.parseBoolean(f[6]);
        ValueFit.Rule rule = ValueFit.rule(value, target, () -> unconstrained);
        String verdict =
            rule == null
                ? "fit"
                : switch (rule) {
                  case NOT_ZERO_OR_ONE -> "not-0-or-1";
                  case FRACTION -> "fraction";
                  case SCALE -> "scale";
                  // Never from a number: only an interval bound for a time column.
                  case DURATION -> "duration";
                };
        assertEquals(f[7], verdict, line);
        rows++;
      }
    }
    assertTrue(rows >= 20, "contract has only " + rows + " rows");
  }

  private static Integer intOrNull(String field) {
    return "-".equals(field) ? null : Integer.valueOf(field);
  }
}
