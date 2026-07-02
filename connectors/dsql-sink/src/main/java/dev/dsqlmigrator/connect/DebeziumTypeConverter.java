/*
 * Task 23.2 — custom Aurora DSQL Kafka Connect sink connector.
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
      // DECIMAL_TYPE: Debezium precise mode already delivers a BigDecimal, which
      // setObject binds correctly to numeric — pass through.
      default:
        return value;
    }
  }

  /**
   * Convert microseconds-since-midnight to a {@link java.sql.Time}. MySQL TIME can
   * exceed 24h or be negative (range -838:59:59..838:59:59); such a value has no
   * {@code time} representation, so it is left to bind as-is (failing loudly to the
   * DLQ) rather than being silently wrapped. In-range values bind cleanly.
   */
  private static Object microsToTime(long micros) {
    if (micros < 0 || micros >= 86_400L * MICROS_PER_SECOND) {
      return micros; // out of [0,24h): cannot represent as time — fail loudly
    }
    // Build from the wall-clock nano-of-day so the value is timezone-independent
    // (new java.sql.Time(millis) would interpret millis in the JVM's local zone).
    return java.sql.Time.valueOf(
        java.time.LocalTime.ofNanoOfDay(micros * NANOS_PER_MICRO));
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
    long result = 0L;
    // Debezium Bits is LITTLE-endian: byte[0] is the least-significant byte.
    for (int i = 0; i < bytes.length && i < 8; i++) {
      result |= ((long) (bytes[i] & 0xFF)) << (8 * i);
    }
    return result;
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
    PGobject pg = new PGobject();
    try {
      pg.setType("json");
      pg.setValue(json);
    } catch (java.sql.SQLException e) {
      throw new DataException("Failed to wrap JSON value for DSQL", e);
    }
    return pg;
  }
}
