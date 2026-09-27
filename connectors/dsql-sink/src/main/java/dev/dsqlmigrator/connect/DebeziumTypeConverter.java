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
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.kafka.connect.data.Schema;
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
  /** Half-ulp bound divisor for {@link #appendPgFloat}; exact (dividing by 2 always terminates). */
  private static final BigDecimal TWO = BigDecimal.valueOf(2);

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
   * Render a Debezium {@code io.debezium.data.Bits} value as its PostgreSQL bit-string
   * text ("11011011"), for a PostgreSQL SOURCE only. PostgreSQL {@code bit(n)} /
   * {@code bit varying} are bit STRINGS, not integers: the Full Load path (psycopg) writes
   * the exact bit-string to the remodeled text column, so the CDC path must match it --
   * binding the MySQL-BIT integer instead ({@link #bitsToLong}) silently DIVERGED the two
   * writes (Full Load "11011011" vs CDC "219"). Gated on the PG source in
   * {@link DebeziumEvents}; a MySQL BIT keeps the integer mapping unchanged (byte-identical).
   *
   * <p>The bytes are little-endian; they are read into a positive {@link java.math.BigInteger}
   * and formatted MSB-first, left-padded with leading zeros to the Debezium schema's declared
   * bit {@code length} (so {@code bit(8)} value 15 -&gt; "00001111", matching psycopg). A fixed
   * {@code bit(n)} reconstructs exactly; an unbounded {@code bit varying} value whose actual
   * bit-length is shorter than the declared length may carry extra leading zeros (Debezium's
   * byte encoding does not preserve the per-value length) -- a documented best-effort residual.
   */
  static Object pgBitString(Object value, Schema schema) {
    if (!(value instanceof byte[])) {
      return value; // not the expected bytes form -> bind as-is (fails loudly if wrong)
    }
    byte[] le = (byte[]) value;
    byte[] be = new byte[le.length];
    for (int i = 0; i < le.length; i++) {
      be[i] = le[le.length - 1 - i]; // little-endian -> big-endian for a positive BigInteger
    }
    String bits = new java.math.BigInteger(1, be).toString(2);
    int length = bitLengthParam(schema, bits.length());
    if (length < 0 || length > MAX_STORABLE_BIT_LENGTH) {
      // The declared length is not a width this can pad TO, so refuse rather than allocate it.
      // LIVE FAILURE this guard exists for (Aurora PostgreSQL 17.7, 2026-09-27): an unbounded
      // `bit varying` column has atttypmod -1, so the Debezium schema carries a `length` that
      // is not a real column width. Padding to it asked for a multi-gigabyte String and the
      // JVM raised OutOfMemoryError("Requested array size exceeds VM limit") inside
      // String.repeat -- and an OutOfMemoryError is an *Error*, not an Exception, so
      // `errors.tolerance=all` and the dead-letter queue CANNOT catch it. The task died with
      // "will not recover until manually restarted", every partition it owned stopped, and
      // replication halted with the connector still reporting RUNNING at the MSK API (the tool
      // does surface it: CloudWatch ErroredTaskCount folds RUNNING -> FAILED). Bounded at
      // Aurora DSQL's documented `character varying` limit of 65535 bytes, because a bit string
      // longer than that cannot be stored in the remodeled target column under any
      // circumstances -- so this rejects nothing the target would have accepted, and it turns
      // an unrecoverable, un-skippable Error into an ordinary dead-letter.
      throw new DataException(
          "Cannot render a PostgreSQL bit/bit varying value whose Debezium schema declares a "
              + "length of "
              + length
              + " bits: that is not a storable column width (Aurora DSQL's character varying "
              + "limit is 65535 bytes), and it is what an UNBOUNDED `bit varying` column "
              + "reports. Give the column an explicit width (bit varying(n) with n <= "
              + MAX_STORABLE_BIT_LENGTH
              + "), exclude it from capture (column.exclude.list), or migrate the table with "
              + "Full Load only.");
    }
    if (bits.length() < length) {
      bits = "0".repeat(length - bits.length()) + bits;
    } else if (bits.length() > length) {
      bits = bits.substring(bits.length() - length); // keep the low `length` bits
    }
    return bits;
  }

  /**
   * The longest bit string the remodeled target column can hold, so a declared length beyond it
   * is refused instead of allocated. Aurora DSQL's documented {@code character varying} limit is
   * 65535 bytes and a bit string renders one ASCII character per bit, so a longer value cannot be
   * stored regardless of this sink. See {@link #pgBitString} for the live OOM this bounds.
   */
  private static final int MAX_STORABLE_BIT_LENGTH = 65535;

  /** The declared bit {@code length} parameter of a Debezium Bits schema, or {@code fallback}. */
  private static int bitLengthParam(Schema schema, int fallback) {
    if (schema != null && schema.parameters() != null) {
      String len = schema.parameters().get("length");
      if (len != null) {
        try {
          return Integer.parseInt(len.trim());
        } catch (NumberFormatException ignored) {
          // malformed parameter -> fall through to the fallback
        }
      }
    }
    return fallback;
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
   * Bind a PostgreSQL ARRAY column as {@code jsonb}, matching what the Full Load stored.
   *
   * <p><b>The defect this fixes.</b> Schema Conversion substitutes a source {@code <t>[]} to
   * {@code jsonb} because Aurora DSQL has no array type. Debezium delivers the value as a
   * {@code java.util.List} under a {@code SchemaBuilder.array(...)} schema with NO logical
   * name, so {@link #convert} returned it untouched and {@code PreparedStatement.setObject}
   * handed pgjdbc a type it cannot map -- SQLSTATE <b>07006</b>, classified permanent, so the
   * record was dead-lettered and the offset committed past it. Because an after-image always
   * carries every column, an UPDATE that touched only an unrelated column died with it: on a
   * live Aurora PostgreSQL 17.7 run five {@code products.price} updates were lost while the
   * table still read {@code src=15 tgt=15 missing=0 extra=0}, because an update that never
   * lands changes no row count.
   *
   * <p><b>Why this rendering.</b> The Full Load converts on the SOURCE, in SQL
   * ({@code CAST(to_jsonb(col) AS text)}), so the bytes already in DSQL are PostgreSQL's own
   * {@code to_jsonb} output. This must produce the SAME text or the two write paths would
   * disagree and Validation's checksum would mismatch every CDC-written row: a JSON array,
   * elements in order, {@code null} for a NULL element, strings quoted and escaped, numbers
   * and booleans bare.
   *
   * <p><b>Throws rather than guesses.</b> An element type whose {@code to_jsonb} text this
   * cannot reproduce byte-for-byte (bytea, a nested array, a composite) raises, so the record
   * dead-letters with a NAMED reason instead of writing a value that silently differs from the
   * loaded one. A visible gap is recoverable; a wrong value that passes the checksum is not.
   *
   * <p><b>The element's logical type decides the rendering, not its Java class.</b> This method
   * runs on the RAW Debezium values (it is reached BEFORE {@link #convert}), so a temporal
   * element is still a {@code Long}/{@code Integer} and a json element is still a {@code String}.
   * Dispatching on the Java class alone therefore wrote a bare JSON <i>number</i> where
   * {@code to_jsonb} writes a quoted ISO <i>string</i> (measured on PostgreSQL 17.11:
   * {@code timestamp[]} → {@code [1767323045678000]} instead of
   * {@code ["2026-01-02T03:04:05.678"]}; likewise {@code date[]} → {@code [20455]},
   * {@code time[]} → {@code [11045678000]}) and quoted a {@code json[]} element as a string
   * instead of embedding it. Those writes were SILENT — valid jsonb, accepted by DSQL, wrong
   * bytes — so {@code elementSchema.name()} is now the primary dispatch key.
   *
   * @param elementSchema the ARRAY schema's {@code valueSchema()} (the element schema, carrying
   *     the Debezium logical-type name); {@code null} or unnamed for a plain primitive element
   */
  static PGobject pgArrayAsJsonb(Object value, Schema elementSchema) {
    String elementType = elementSchema == null ? null : elementSchema.name();
    StringBuilder out = new StringBuilder("[");
    boolean first = true;
    for (Object element : (Iterable<?>) value) {
      if (!first) {
        out.append(", ");
      }
      first = false;
      appendJsonbElement(out, element, elementType);
    }
    return pgObject("jsonb", out.append("]").toString());
  }

  /**
   * Render ONE array element exactly as PostgreSQL's {@code to_jsonb} would, given the element's
   * Debezium logical-type name ({@code null} for a plain primitive).
   */
  private static void appendJsonbElement(StringBuilder out, Object element, String elementType) {
    if (element == null) {
      out.append("null");
      return;
    }
    if (elementType != null) {
      switch (elementType) {
        case MICRO_TIMESTAMP: // timestamp(4..6)[] -> micros since epoch
          appendPgTimestamp(out, ((Number) element).longValue());
          return;
        case TIMESTAMP_MS: // timestamp(0..3)[] -> MILLIS since epoch
          appendPgTimestamp(out, ((Number) element).longValue() * MICROS_PER_MILLI);
          return;
        case DATE: // date[] -> epoch days
          appendPgDate(out, ((Number) element).longValue());
          return;
        case MICRO_TIME: // time[] -> micros since midnight
          appendPgTime(out, ((Number) element).longValue());
          return;
        case TIME_MS: // time[] under time.precision.mode=adaptive -> millis since midnight
          appendPgTime(out, ((Number) element).longValue() * MICROS_PER_MILLI);
          return;
        case ZONED_TIMESTAMP: // timestamptz[] -> ISO-8601 text, UTC-normalized, offset as "Z"
          appendPgTimestamptz(out, element.toString());
          return;
        case JSON_TYPE:
          // json[] / jsonb[]: to_jsonb EMBEDS the element's JSON as a JSON VALUE
          // ([{"a": 2, "b": 1}]), it does not quote it as a string. The target column is jsonb,
          // so the server re-parses and canonicalizes this text -- which is why embedding a
          // json[] element's RAW (unnormalized) text is also exact: measured on PG 17.11,
          // to_jsonb(ARRAY['{"b":1, "a":2}'::json]) and '[{"b":1, "a":2}]'::jsonb agree, and
          // numeric scale inside the document survives both ([{"a": 1.50}]).
          out.append(element.toString());
          return;
        case ZONED_TIME:
          throw unrenderableArrayElement(
              "a PostgreSQL timetz[] element",
              "to_jsonb keeps the SOURCE offset (measured: [\"03:04:05.678+09\"], and it is NOT "
                  + "affected by the session TimeZone) but Debezium's ZonedTime has already "
                  + "normalized the value to UTC and DISCARDED that offset, so the loaded text "
                  + "cannot be reconstructed from the event");
        case VARIABLE_SCALE_DECIMAL:
          throw unrenderableArrayElement(
              "an unconstrained PostgreSQL numeric[] element (Debezium VariableScaleDecimal)",
              "Debezium calls stripTrailingZeros() before the sink sees the value, so the "
                  + "DISPLAY SCALE to_jsonb prints is already gone (to_jsonb writes 1.50, the "
                  + "event carries 1.5); declare the column numeric(p,s) so the scale survives");
        default:
          break; // Decimal / Uuid / Enum / Ltree -> the value-shape branches below
      }
    }
    if (element instanceof Boolean) {
      out.append(((Boolean) element) ? "true" : "false");
      return;
    }
    if (element instanceof Double || element instanceof Float) {
      appendPgFloat(out, ((Number) element).doubleValue(), element instanceof Float);
      return;
    }
    if (element instanceof BigDecimal) {
      // numeric(p,s)[]: to_jsonb PRESERVES the declared scale and never uses scientific
      // notation. BigDecimal.toString() would print 1E+3; toPlainString is both.
      out.append(((BigDecimal) element).toPlainString());
      return;
    }
    if (element instanceof Number && elementType == null) {
      // int2[]/int4[]/int8[]: an unnamed integral primitive prints the same digits as to_jsonb.
      out.append(element.toString());
      return;
    }
    if (element instanceof CharSequence) {
      // Every remaining PostgreSQL type Debezium delivers as text carries PostgreSQL's own
      // canonical output, which to_jsonb quotes verbatim (measured: uuid, inet, cidr, macaddr,
      // macaddr8, char(n) with its blank padding, and enum labels). The three text-shaped
      // logical types whose spelling Debezium CHANGES -- ZonedTime, ZonedTimestamp and Json --
      // are all dispatched by name above, so they never reach here.
      appendJsonString(out, element.toString());
      return;
    }
    if (element instanceof byte[] || element instanceof java.nio.ByteBuffer) {
      throw unrenderableArrayElement(
          "a PostgreSQL bytea[] element",
          "to_jsonb spells the bytes per the SOURCE's bytea_output setting (measured: "
              + "[\"\\\\x0001ff\"] under hex, [\"\\\\000\\\\001\\\\377\"] under escape) and the "
              + "change event does not carry that setting");
    }
    // Deliberately NOT a best-effort toString(): see pgArrayAsJsonb. A nested List would render
    // as Java's "[a, b]", a Struct as an object identity -- both silently unequal to what the
    // Full Load wrote, which is worse than a dead-letter naming the type. A NAMED logical type
    // that reaches here is one this sink has not been taught (e.g. io.debezium.time.NanoTime):
    // rendering its raw Long as a bare number is exactly the defect this dispatch table fixes.
    throw unrenderableArrayElement(
        "a PostgreSQL array element of Java type "
            + element.getClass().getName()
            + (elementType == null ? "" : " (Debezium logical type " + elementType + ")"),
        "this sink renders array columns as jsonb to match the Full Load's to_jsonb text, and "
            + "that text cannot be reproduced from the change event");
  }

  /** A dead-letter that NAMES the element type and why its to_jsonb text is unreachable. */
  private static DataException unrenderableArrayElement(String what, String why) {
    return new DataException(
        "Cannot bind "
            + what
            + " to its jsonb column: "
            + why
            + ". Exclude the column from capture (column.exclude.list), or migrate the table "
            + "with Full Load only.");
  }

  /**
   * PostgreSQL's {@code to_jsonb} spelling of a {@code timestamp}:
   * {@code "2026-01-02T03:04:05.678"} — a QUOTED ISO-8601 string with a {@code T} separator,
   * independent of {@code DateStyle}. Measured on PostgreSQL 17.11.
   */
  private static void appendPgTimestamp(StringBuilder out, long micros) {
    LocalDateTime ts =
        LocalDateTime.ofEpochSecond(
            Math.floorDiv(micros, MICROS_PER_SECOND),
            (int) (Math.floorMod(micros, MICROS_PER_SECOND) * NANOS_PER_MICRO),
            ZoneOffset.UTC);
    out.append('"');
    appendPgDateParts(out, ts.toLocalDate());
    out.append('T');
    appendPgTimeParts(out, ts.toLocalTime());
    out.append('"');
  }

  /** PostgreSQL's {@code to_jsonb} spelling of a {@code date}: {@code "2026-01-02"}. */
  private static void appendPgDate(StringBuilder out, long epochDay) {
    out.append('"');
    appendPgDateParts(out, LocalDate.ofEpochDay(epochDay));
    out.append('"');
  }

  /**
   * The {@code yyyy-MM-dd} part, rejecting anything {@code to_jsonb} spells differently.
   *
   * <p>PostgreSQL writes a BC date as {@code "0044-03-15 BC"} and the special values as
   * {@code "infinity"} / {@code "-infinity"}, and Debezium encodes BOTH as ordinary epoch
   * days / epoch micros — {@code infinity} arrives as the year 294247 and {@code -infinity}
   * as the year -290308 (measured from {@code PostgresValueConverter.POSITIVE_INFINITY_*}).
   * Nothing in the event distinguishes those from a genuine far-future date, so the whole
   * range outside {@code 0001..9999 CE} dead-letters rather than being approximated.
   */
  private static void appendPgDateParts(StringBuilder out, LocalDate date) {
    int year = date.getYear();
    if (year < 1 || year > 9999) {
      throw unrenderableArrayElement(
          "a PostgreSQL date/timestamp array element outside the years 0001..9999 CE (" + date
              + ")",
          "to_jsonb spells a BC value \"0044-03-15 BC\" and the special values \"infinity\" / "
              + "\"-infinity\", and Debezium encodes all of them as ordinary epoch days/micros "
              + "(infinity arrives as the year 294247), so the two cannot be told apart");
    }
    out.append(String.format("%04d-%02d-%02d", year, date.getMonthValue(), date.getDayOfMonth()));
  }

  /** PostgreSQL's {@code to_jsonb} spelling of a {@code time}: {@code "03:04:05.678"}. */
  private static void appendPgTime(StringBuilder out, long micros) {
    if (micros < 0 || micros >= 86_400L * MICROS_PER_SECOND) {
      throw unrenderableArrayElement(
          "a PostgreSQL time[] element outside [00:00:00, 24:00:00) (" + micros + " micros)",
          "PostgreSQL's boundary value time '24:00:00' is spelled \"24:00:00\" by to_jsonb and "
              + "has no in-range micros-since-midnight form this can render unambiguously");
    }
    out.append('"');
    appendPgTimeParts(out, LocalTime.ofNanoOfDay(micros * NANOS_PER_MICRO));
    out.append('"');
  }

  private static void appendPgTimeParts(StringBuilder out, LocalTime time) {
    out.append(
        String.format("%02d:%02d:%02d", time.getHour(), time.getMinute(), time.getSecond()));
    appendTrimmedFraction(out, time.getNano());
  }

  /**
   * PostgreSQL omits the fractional second entirely when it is zero and TRIMS trailing zeros
   * (measured: {@code "03:04:05"} not {@code "03:04:05.000"}, and {@code .100} prints as
   * {@code .1}). {@code LocalTime.toString()} / {@code DateTimeFormatter.ISO_LOCAL_TIME} pad to
   * 3, 6 or 9 digits instead, so neither is a drop-in match — hence this explicit trim.
   */
  private static void appendTrimmedFraction(StringBuilder out, int nanoOfSecond) {
    if (nanoOfSecond == 0) {
      return;
    }
    String digits = String.format("%09d", nanoOfSecond);
    int end = digits.length();
    while (digits.charAt(end - 1) == '0') {
      end--;
    }
    out.append('.').append(digits, 0, end);
  }

  /**
   * PostgreSQL's {@code to_jsonb} spelling of a {@code timestamptz} in the UTC session the Full
   * Load pins ({@code source_dialect/postgres.py} connects with {@code -c timezone=UTC}):
   * {@code "2026-01-02T03:04:05.678+00:00"} — the offset carries a COLON. Debezium's
   * {@code ZonedTimestamp} prints the same instant with a bare {@code "Z"}, so the value is
   * re-spelled from the parsed instant, never recomputed.
   */
  private static void appendPgTimestamptz(StringBuilder out, String isoOffsetDateTime) {
    OffsetDateTime at;
    try {
      at = OffsetDateTime.parse(isoOffsetDateTime).withOffsetSameInstant(ZoneOffset.UTC);
    } catch (java.time.format.DateTimeParseException parseFailed) {
      throw unrenderableArrayElement(
          "a PostgreSQL timestamptz[] element Debezium spelled \"" + isoOffsetDateTime + "\"",
          "it is not an ISO-8601 offset date-time, so the to_jsonb text cannot be derived");
    }
    out.append('"');
    appendPgDateParts(out, at.toLocalDate());
    out.append('T');
    appendPgTimeParts(out, at.toLocalTime());
    out.append("+00:00").append('"');
  }

  /**
   * PostgreSQL's {@code to_jsonb} spelling of a {@code float8} / {@code float4}.
   *
   * <p>{@code to_jsonb} of a float is {@code numeric_in(float8out(v))}: the SHORTEST decimal
   * that reads back as the same float, printed PLAIN (never scientific), and the non-finite
   * values as JSON STRINGS. {@code Double.toString} is not that text — it keeps the {@code .0}
   * ({@code "1.0"} vs {@code 1}) and the exponent ({@code "1.0E30"}), and reformatting it
   * through {@code BigDecimal} still diverges because Java accepts a decimal sitting exactly on
   * the rounding-interval BOUNDARY (legal when the mantissa is even) while PostgreSQL does not:
   * measured against live PostgreSQL 17.11 over 45,922 values, the reformat-{@code toString}
   * approach missed 40/25,515 doubles and 43/20,407 floats (e.g. {@code 171694364235957200} vs
   * PostgreSQL's {@code 171694364235957180}), while the strict search below matched 45,922/45,922.
   *
   * <p>So: the shortest decimal STRICTLY inside {@code (v-½ulp, v+½ulp)}. Bounds are computed at
   * the value's own width — a {@code float4} must use {@code Math.nextUp/nextDown} on the
   * {@code float}, not on the widened {@code double}, or it yields the double's digits.
   *
   * <p><b>Cost.</b> Measured 4.3 µs per float element against 25 ns for an integral one (the
   * {@code BigDecimal} search dominates). That is fine for CDC's incremental volumes and is left
   * deliberately simple; if a wide {@code float8[]} ever shows up as a throughput problem, the
   * loop can start at the significant-digit count of {@code Double.toString} (a proven lower
   * bound, since every strictly-inside decimal also round-trips) instead of at 1.
   */
  private static void appendPgFloat(StringBuilder out, double value, boolean singlePrecision) {
    if (Double.isNaN(value)) {
      out.append("\"NaN\""); // measured: to_jsonb(ARRAY['NaN'::float8]) -> ["NaN"]
      return;
    }
    if (Double.isInfinite(value)) {
      out.append(value > 0 ? "\"Infinity\"" : "\"-Infinity\"");
      return;
    }
    if (value == 0.0) {
      out.append('0'); // both +0 and -0: measured to_jsonb(ARRAY[0::float8,'-0'::float8]) -> [0, 0]
      return;
    }
    double magnitude = Math.abs(value);
    BigDecimal exact = new BigDecimal(magnitude);
    BigDecimal below =
        new BigDecimal(
            singlePrecision ? (double) Math.nextDown((float) magnitude) : Math.nextDown(magnitude));
    double aboveValue =
        singlePrecision ? (double) Math.nextUp((float) magnitude) : Math.nextUp(magnitude);
    BigDecimal low = exact.add(below).divide(TWO);
    BigDecimal high =
        Double.isInfinite(aboveValue) // the finite maximum: mirror the lower half-ulp
            ? exact.add(exact.subtract(low))
            : exact.add(new BigDecimal(aboveValue)).divide(TWO);
    BigDecimal shortest = exact;
    int maxDigits = singlePrecision ? 9 : 17;
    for (int digits = 1; digits <= maxDigits; digits++) {
      BigDecimal candidate = exact.round(new MathContext(digits, RoundingMode.HALF_EVEN));
      if (candidate.compareTo(low) > 0 && candidate.compareTo(high) < 0) {
        shortest = candidate;
        break;
      }
    }
    if (value < 0) {
      out.append('-');
    }
    out.append(shortest.stripTrailingZeros().toPlainString());
  }

  /** Escape a string the way PostgreSQL's JSON output does. */
  private static void appendJsonString(StringBuilder out, String text) {
    out.append('"');
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      switch (c) {
        case '"':
          out.append("\\\"");
          break;
        case '\\':
          out.append("\\\\");
          break;
        case '\b':
          out.append("\\b");
          break;
        case '\f':
          out.append("\\f");
          break;
        case '\n':
          out.append("\\n");
          break;
        case '\r':
          out.append("\\r");
          break;
        case '\t':
          out.append("\\t");
          break;
        default:
          if (c < 0x20) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
      }
    }
    out.append('"');
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
