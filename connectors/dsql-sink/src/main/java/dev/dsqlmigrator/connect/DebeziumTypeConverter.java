// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Custom Aurora DSQL Kafka Connect sink connector.
 * Converts a Debezium-encoded field value to the canonical DSQL-target form
 * defined by the shared DSQL write contract (tests/fixtures/dsql_write_contract.json,
 * mirrored from converter.DSQL_WRITE_CONTRACT_CASES).
 */
package dev.dsqlmigrator.connect;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.postgresql.util.PGobject;

/**
 * Aligns the CDC (sink) write path with the Full Load (Python bulk loader) write
 * path so the same MySQL source row lands identically in Aurora DSQL regardless of
 * which path migrated it.
 *
 * <p>The bulk loader uses the shared MySQL→DSQL type mapping
 * ({@code converter.map_mysql_type}) and writes proper typed values (a UTC
 * timestamp for {@code DATETIME}, a {@code bytea} for {@code BLOB}, …). The sink,
 * by contrast, used to bind whatever Java object Debezium placed in the Kafka
 * Connect {@code Struct} straight through {@code PreparedStatement.setObject} —
 * which is WRONG for temporal and JSON columns: Debezium serializes
 * {@code DATETIME(6)} as a {@code Long} (microseconds since epoch) tagged with the
 * schema name {@code io.debezium.time.MicroTimestamp}, and {@code setObject(Long)}
 * against a DSQL {@code TIMESTAMP} column is rejected with SQLSTATE 42804 (the H5
 * bug, previously worked around by quarantining the rows to a DLQ or hacking the
 * target column to {@code BIGINT}).
 *
 * <p>This converter inspects the field's Connect schema NAME (the Debezium logical
 * type, e.g. {@code io.debezium.time.MicroTimestamp}) and converts the value to a
 * JDBC-ready object that matches the bulk loader's encoding, BEFORE it is bound.
 * Plain primitives (no schema name) and already-correct types (Debezium
 * {@code Decimal} → {@code BigDecimal}) pass through unchanged.
 */
final class DebeziumTypeConverter {

  // Debezium / Kafka Connect logical-type schema names (field.schema().name()).
  static final String MICRO_TIMESTAMP = "io.debezium.time.MicroTimestamp";
  static final String TIMESTAMP_MS = "io.debezium.time.Timestamp";
  static final String ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp";
  static final String DATE = "io.debezium.time.Date";
  static final String JSON_TYPE = "io.debezium.data.Json";
  static final String DECIMAL_TYPE = "org.apache.kafka.connect.data.Decimal";
  // MySQL BIT(n): Debezium encodes it as a little-endian byte[] under this logical
  // type. The schema converter maps BIT to a DSQL integer (smallint/integer/bigint),
  // so the byte[] must be decoded to a long; binding the raw bytes fails with
  // "column is of type smallint but expression is of type bytea".
  static final String BITS_TYPE = "io.debezium.data.Bits";
  // MySQL TIME: Debezium encodes time-of-day as micros (MicroTime) or millis (Time)
  // since midnight. The DSQL column is ``time``; binding the raw Long fails ("column
  // is of type time without time zone but expression is of type bigint"), so convert
  // to a java.sql.Time. Mirrors the Full Load timedelta->time handling.
  static final String MICRO_TIME = "io.debezium.time.MicroTime";
  static final String TIME_MS = "io.debezium.time.Time";
  // MySQL spatial types: Debezium encodes them as a Struct {wkb: byte[], srid: int}
  // under these logical types. DSQL has no geometry type, so the schema converter
  // maps the column to bytea and the value is stored as the raw WKB bytes -- the
  // SAME bytes Full Load writes via ST_AsBinary(col), keeping the two paths in
  // parity. The SRID is dropped on both paths (plain WKB).
  static final String GEOMETRY_TYPE = "io.debezium.data.geometry.Geometry";
  static final String GEOGRAPHY_TYPE = "io.debezium.data.geometry.Geography";
  static final String POINT_TYPE = "io.debezium.data.geometry.Point";

  // --- PostgreSQL-source logical types --------------------------------------
  // The DSQL sink is engine-neutral: it dispatches on the Debezium logical-type NAME,
  // and these names are emitted ONLY by the PostgreSQL source connector (never by the
  // MySQL one), so adding them is inert for a MySQL migration. Each conversion mirrors
  // the Full Load PostgreSQL value path (exporter_postgres.PostgresValueConverter) so a
  // row lands identically in DSQL whether migrated by Full Load or CDC. The source
  // connector is configured (deploy/cdc-stack.yaml PostgresSourceConnector) to emit the
  // exact encodings decoded here: decimal.handling.mode=precise, interval.handling.mode=
  // string, time.precision.mode=adaptive_time_microseconds.

  // PostgreSQL uuid -> Debezium sends the canonical dashed string; bind a java.util.UUID
  // (the DSQL column is uuid). A plain String would bind as varchar and be rejected.
  static final String UUID_TYPE = "io.debezium.data.Uuid";
  // PostgreSQL time with time zone (timetz) -> an ISO-8601 offset-time string that Debezium
  // ALWAYS normalizes to UTC (e.g. "12:15:00.123456Z"); bind a java.time.OffsetTime to keep
  // the sub-seconds. Because the source offset is discarded here (not by us -- by Debezium),
  // a CDC-written timetz stores the UTC offset while Full Load stores the source offset (the
  // same instant, different stored offset); Validation compares timetz offset-insensitively
  // (validation_sql._pg_checksum_expr shifts both to UTC) so the two write paths agree.
  static final String ZONED_TIME = "io.debezium.time.ZonedTime";
  // PostgreSQL interval (with interval.handling.mode=string) -> an ISO-8601 duration
  // string (e.g. "P1Y2M3DT4H5M6S"); wrap in a PGobject(type=interval) so it binds to the
  // interval column (PostgreSQL's interval input accepts ISO-8601).
  static final String INTERVAL_TYPE = "io.debezium.time.Interval";
  // PostgreSQL numeric WITHOUT a declared scale, under decimal.handling.mode=precise ->
  // a Struct {scale INT32, value BYTES}; decode to a BigDecimal. (A numeric WITH a fixed
  // scale arrives as org.apache.kafka.connect.data.Decimal -> a BigDecimal that passes
  // through the default case unchanged.)
  static final String VARIABLE_SCALE_DECIMAL = "io.debezium.data.VariableScaleDecimal";

  private static final long MICROS_PER_MILLI = 1_000L;
  private static final int NANOS_PER_MICRO = 1_000;
  private static final long MICROS_PER_SECOND = 1_000_000L;

  private DebeziumTypeConverter() {}

  /**
   * Convert one field value from its Debezium-encoded form to a JDBC-ready value
   * matching the DSQL write contract.
   *
   * @param schemaName the Connect field schema name ({@code field.schema().name()}),
   *     or {@code null} for a plain primitive
   * @param value the raw value from {@code struct.get(field)}
   * @return a JDBC-ready value ({@link Timestamp}, {@link BigDecimal},
   *     {@link PGobject}, byte[], primitive, …); {@code null} passes through
   */
  static Object convert(String schemaName, Object value) {
    if (value == null || schemaName == null) {
      return value; // null, or a plain primitive (no logical type) — bind as-is
    }
    switch (schemaName) {
      case MICRO_TIMESTAMP:
        return microsToTimestamp(((Number) value).longValue());
      case TIMESTAMP_MS:
        return Timestamp.from(Instant.ofEpochMilli(((Number) value).longValue()));
      case ZONED_TIMESTAMP:
        return Timestamp.from(OffsetDateTime.parse(value.toString()).toInstant());
      case DATE:
        return java.sql.Date.valueOf(LocalDate.ofEpochDay(((Number) value).longValue()));
      case JSON_TYPE:
        return jsonObject(value.toString());
      case BITS_TYPE:
        return bitsToLong(value);
      case MICRO_TIME:
        return microsToTime(((Number) value).longValue());
      case TIME_MS:
        return microsToTime(((Number) value).longValue() * MICROS_PER_MILLI);
      case GEOMETRY_TYPE:
      case GEOGRAPHY_TYPE:
      case POINT_TYPE:
        return geometryWkb(value);
      case UUID_TYPE:
        return uuidObject(value.toString());
      case ZONED_TIME:
        return offsetTime(value.toString());
      case INTERVAL_TYPE:
        return pgObject("interval", value.toString());
      case VARIABLE_SCALE_DECIMAL:
        return variableScaleDecimal(value);
      // DECIMAL_TYPE: Debezium precise mode already delivers a BigDecimal, which
      // setObject binds correctly to numeric — pass through.
      default:
        return value;
    }
  }

  /**
   * Convert microseconds-since-midnight to a {@link java.time.LocalTime}. MySQL TIME
   * can exceed 24h or be negative (range -838:59:59..838:59:59); such a value has no
   * {@code time} representation, so it is left to bind as-is (failing loudly to the
   * DLQ) rather than being silently wrapped. In-range values bind cleanly.
   *
   * <p><b>Returns {@code LocalTime}, NOT {@code java.sql.Time}.</b>
   * {@code java.sql.Time} holds only hour/minute/second -- {@code Time.valueOf(LocalTime)}
   * DISCARDS the sub-second field per the JDK contract -- so a MySQL {@code TIME(1-6)}
   * value silently lost its microseconds on the CDC path while the Full Load path
   * (Python {@code datetime.time(microsecond=…)}) kept them, diverging the two writes
   * and failing Validation. pgjdbc binds a {@link java.time.LocalTime} to a
   * {@code time}/{@code time(n)} column with full microsecond precision, and it is
   * timezone-independent (unlike {@code new java.sql.Time(millis)}, which would
   * interpret the millis in the JVM's local zone).
   */
  private static Object microsToTime(long micros) {
    if (micros < 0 || micros >= 86_400L * MICROS_PER_SECOND) {
      return micros; // out of [0,24h): cannot represent as time — fail loudly
    }
    return java.time.LocalTime.ofNanoOfDay(micros * NANOS_PER_MICRO);
  }

  /**
   * Decode a Debezium {@code io.debezium.data.Bits} value (a little-endian
   * {@code byte[]}) to the unsigned {@code long} the bit pattern holds, matching
   * the DSQL integer column MySQL {@code BIT(n)} maps to. Mirrors the Full Load
   * value converter's BIT bytes-&gt;int handling. A value already delivered as a
   * number (some configs) passes through.
   */
  private static Object bitsToLong(Object value) {
    if (!(value instanceof byte[])) {
      return value;
    }
    byte[] bytes = (byte[]) value;
    // Debezium Bits is LITTLE-endian: byte[0] is the least-significant byte. Accumulate
    // into a BigInteger so a full 64-bit BIT(64) value keeps its UNSIGNED range: a signed
    // long would WRAP (2^64-1 -> -1), and BIT(64) maps to a DSQL numeric(20,0) that must
    // hold the unsigned value (mirrors the Full Load BIT bytes->unsigned-int handling).
    java.math.BigInteger result = java.math.BigInteger.ZERO;
    for (int i = 0; i < bytes.length && i < 8; i++) {
      result = result.or(
          java.math.BigInteger.valueOf(bytes[i] & 0xFF).shiftLeft(8 * i));
    }
    // BIT(<=63) fits a signed long (target bigint/integer/smallint) -> return a Long so
    // pgjdbc binds it to the integer column. Only BIT(64) above Long.MAX_VALUE needs the
    // BigDecimal (target numeric(20,0)); a Long there would be negative.
    if (result.bitLength() <= 63) {
      return result.longValue();
    }
    return new java.math.BigDecimal(result);
  }

  /**
   * Convert microseconds-since-epoch to a UTC {@link Timestamp}, preserving the
   * sub-millisecond microseconds in the nanos field. Negative (pre-epoch) values
   * are handled by flooring toward negative infinity so the fractional part stays
   * in {@code [0, 1_000_000)} micros.
   */
  /**
   * Extract the raw WKB bytes from a Debezium geometry value (a {@link Struct}
   * with a {@code wkb} byte[] and an {@code srid}). DSQL has no geometry type, so
   * the column is {@code bytea} and stores the WKB -- identical to Full Load's
   * {@code ST_AsBinary(col)}. The SRID is dropped (plain WKB), matching Full Load.
   * Never returns null for a present value: an unexpected shape is bound as-is so
   * it fails loudly to the DLQ rather than silently writing NULL.
   */
  private static Object geometryWkb(Object value) {
    if (value instanceof Struct) {
      Object wkb = ((Struct) value).get("wkb");
      if (wkb instanceof byte[]) {
        return wkb;
      }
    }
    return value;
  }

  private static Timestamp microsToTimestamp(long micros) {
    long seconds = Math.floorDiv(micros, MICROS_PER_SECOND);
    long fractionMicros = Math.floorMod(micros, MICROS_PER_SECOND);
    Timestamp ts = new Timestamp(seconds * 1000L);
    ts.setNanos((int) (fractionMicros * NANOS_PER_MICRO));
    return ts;
  }

  /** Wrap a JSON string in a {@code PGobject(type=json)} so it binds to a json column. */
  private static PGobject jsonObject(String json) {
    return pgObject("json", json);
  }

  /**
   * Wrap a value string in a {@code PGobject} of the given PostgreSQL type name so pgjdbc
   * binds it to that column type (rather than as {@code varchar}). Used for {@code json}
   * and {@code interval}, whose canonical text forms the server re-parses.
   */
  private static PGobject pgObject(String type, String value) {
    PGobject pg = new PGobject();
    try {
      pg.setType(type);
      pg.setValue(value);
    } catch (java.sql.SQLException e) {
      throw new DataException("Failed to wrap " + type + " value for DSQL", e);
    }
    return pg;
  }

  /**
   * Bind a PostgreSQL {@code uuid} value: Debezium sends the canonical dashed string, so
   * parse it to a {@link java.util.UUID} (pgjdbc targets the {@code uuid} column). Mirrors
   * the Full Load path (psycopg {@code uuid.UUID}). A malformed value is left as the raw
   * string so it fails loudly to the DLQ rather than crashing the batch (matching
   * {@link #microsToTime}'s fail-loud stance).
   */
  private static Object uuidObject(String text) {
    try {
      return java.util.UUID.fromString(text);
    } catch (IllegalArgumentException e) {
      return text;
    }
  }

  /**
   * Bind a PostgreSQL {@code time with time zone} (timetz) value: Debezium
   * {@code ZonedTime} is an ISO-8601 offset-time string ALWAYS normalized to UTC (e.g.
   * {@code 12:15:00.123456Z}). Parse to a {@link java.time.OffsetTime} so pgjdbc targets the
   * {@code timetz} column with full microsecond precision -- {@code java.sql.Time} would drop
   * the sub-seconds and the offset. Debezium already discarded the source offset (sent UTC),
   * so a CDC-written timetz differs from the offset-preserving Full Load write in stored
   * offset only (same instant); Validation reconciles them offset-insensitively. An
   * unparseable value is left as the raw string to fail loudly to the DLQ.
   */
  private static Object offsetTime(String text) {
    try {
      return java.time.OffsetTime.parse(text);
    } catch (java.time.format.DateTimeParseException e) {
      return text;
    }
  }

  /**
   * Decode a Debezium {@code io.debezium.data.VariableScaleDecimal} (a {@link Struct}
   * {@code {scale INT32, value BYTES}}) to a {@link BigDecimal}. This is how an
   * UNCONSTRAINED PostgreSQL {@code numeric} is encoded under
   * {@code decimal.handling.mode=precise}. The {@code value} bytes are the
   * two's-complement big-endian unscaled integer, so {@code new BigInteger(bytes)} with
   * the scale reconstructs it exactly (mirrors the Full Load {@code Decimal}). An
   * unexpected shape is bound as-is so it fails loudly to the DLQ rather than writing a
   * wrong value.
   */
  private static Object variableScaleDecimal(Object value) {
    if (value instanceof Struct) {
      Struct struct = (Struct) value;
      Object scale = struct.get("scale");
      Object unscaled = struct.get("value");
      if (scale instanceof Number && unscaled instanceof byte[]) {
        return new BigDecimal(
            new java.math.BigInteger((byte[]) unscaled), ((Number) scale).intValue());
      }
    }
    return value;
  }
}
