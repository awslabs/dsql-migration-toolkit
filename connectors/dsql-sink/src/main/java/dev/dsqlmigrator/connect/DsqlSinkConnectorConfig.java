// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Custom Aurora DSQL Kafka Connect sink connector.
 */
package dev.dsqlmigrator.connect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * Configuration for the custom DSQL sink connector.
 *
 * <p>The Python control plane ({@code CdcPipelineOrchestrator.build_sink_config})
 * produces these values, and the {@code deploy/cdc-stack/cdc-stack.yaml}
 * connector configuration sets the same keys. Primary keys are taken from the
 * Debezium record key ({@code pk.mode=record_key}), so there is no per-table
 * {@code pk.fields} setting -- EXCEPT for a re-keyed table listed in
 * {@link #TARGET_KEY_COLUMNS}: its change records are keyed on the SOURCE primary key, and
 * the sink writes it by the Aurora DSQL composite primary key listed there (see
 * {@link DebeziumEvents}). The DLQ is handled by the Kafka Connect framework
 * ({@code errors.deadletterqueue.topic.name}) and is not read here.
 */
public class DsqlSinkConnectorConfig extends AbstractConfig {

  public static final String CLUSTER_ENDPOINT = "dsql.cluster.endpoint";
  public static final String REGION = "dsql.region";
  public static final String DATABASE = "dsql.database";
  public static final String USERNAME = "dsql.username";
  public static final String DELETE_ENABLED = "delete.enabled";
  public static final String BATCH_SIZE = "batch.size";
  public static final String MAX_RETRIES = "occ.max.retries";
  public static final String RETRY_BACKOFF_MS = "occ.retry.backoff.ms";
  // Per-table net-rows monitor metric (CloudWatch). Best-effort and off unless a
  // stack name is supplied by the cdc-stack connector config; a metric-emit failure
  // never affects replication.
  public static final String METRICS_ENABLED = "metrics.enabled";
  public static final String METRICS_STACK = "metrics.stack";
  // Re-keyed tables (Schema Conversion's Composite key) whose change records are keyed on the
  // SOURCE primary key, with the Aurora DSQL composite primary key the sink writes each one by.
  // The cdc-stack SinkKeyColumns parameter, which Start CDC fills only for a stack whose sink
  // plugin reads it; absent or empty = no table (every record keyed as before).
  public static final String TARGET_KEY_COLUMNS = "target.key.columns";

  // DSQL hard limit: a transaction may modify at most 3,000 rows.
  public static final int DSQL_MAX_ROWS_PER_TXN = 3000;

  private static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(CLUSTER_ENDPOINT, Type.STRING, Importance.HIGH, "DSQL cluster endpoint host.")
          .define(REGION, Type.STRING, Importance.HIGH, "AWS region of the DSQL cluster.")
          .define(DATABASE, Type.STRING, "postgres", Importance.MEDIUM, "DSQL database name.")
          .define(USERNAME, Type.STRING, "admin", Importance.MEDIUM, "DSQL database role.")
          .define(
              DELETE_ENABLED,
              Type.BOOLEAN,
              true,
              Importance.MEDIUM,
              "Apply Debezium delete events (and tombstones) as DSQL DELETEs.")
          .define(
              BATCH_SIZE,
              Type.INT,
              1000,
              ConfigDef.Range.between(1, DSQL_MAX_ROWS_PER_TXN),
              Importance.MEDIUM,
              "Rows per transaction (must be <= 3000, the DSQL per-transaction row limit).")
          .define(
              MAX_RETRIES,
              Type.INT,
              10,
              Importance.MEDIUM,
              "Max attempts per sink transaction (a chunk, or one row) on an OCC conflict "
                  + "(40001) or Aurora DSQL's server unavailable (XX000).")
          .define(
              RETRY_BACKOFF_MS,
              Type.LONG,
              50L,
              Importance.LOW,
              "Base backoff for the OCC / server-unavailable retry (ms).")
          .define(
              METRICS_ENABLED,
              Type.BOOLEAN,
              true,
              Importance.LOW,
              "Emit the per-table applied-ops CloudWatch metrics (InsertsApplied / "
                  + "UpdatesApplied / DeletesApplied) + ReplicationLagMs (best-effort).")
          .define(
              METRICS_STACK,
              Type.STRING,
              "",
              Importance.LOW,
              "Value of the metric's Stack dimension (the cdc-stack name); blank disables emission.")
          .define(
              TARGET_KEY_COLUMNS,
              Type.STRING,
              "",
              Importance.LOW,
              "Re-keyed tables whose change records are keyed on the SOURCE primary key, and the "
                  + "Aurora DSQL composite primary key the sink writes each one by, in Debezium "
                  + "message.key.columns syntax: \"db\\.table:leading,pk;...\". Empty = none.");

  public DsqlSinkConnectorConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
  }

  public static ConfigDef configDef() {
    return CONFIG_DEF;
  }

  public String clusterEndpoint() {
    return getString(CLUSTER_ENDPOINT);
  }

  public String region() {
    return getString(REGION);
  }

  public String database() {
    return getString(DATABASE);
  }

  public String username() {
    return getString(USERNAME);
  }

  public boolean deleteEnabled() {
    return getBoolean(DELETE_ENABLED);
  }

  public int batchSize() {
    return getInt(BATCH_SIZE);
  }

  public int maxRetries() {
    return getInt(MAX_RETRIES);
  }

  public long retryBackoffMs() {
    return getLong(RETRY_BACKOFF_MS);
  }

  public boolean metricsEnabled() {
    return getBoolean(METRICS_ENABLED);
  }

  public String metricsStack() {
    return getString(METRICS_STACK);
  }

  /**
   * {@link #TARGET_KEY_COLUMNS} as {@code {schema.table: [composite primary-key columns]}}, in
   * the order the value lists them. Empty (never null) when the key is absent or empty.
   */
  public Map<String, List<String>> targetKeyColumns() {
    return parseTargetKeyColumns(getString(TARGET_KEY_COLUMNS));
  }

  /**
   * Parse {@code "db\.t:c1,c2;db\.u:c3,c4"} -- the inverse of the control plane's
   * {@code core/cdc.format_message_key_columns}: entries split on {@code ';'}, the FIRST
   * {@code ':'} separates the table from its columns, {@code '\.'} in the table is a literal
   * dot, and columns split on {@code ','} with empty ones dropped. A malformed entry (no
   * {@code ':'}, no table, or no column) is ignored rather than guessed at; a table listed twice
   * keeps its last entry. Pure; package-private for tests.
   */
  static Map<String, List<String>> parseTargetKeyColumns(String value) {
    if (value == null || value.isEmpty()) {
      return Map.of();
    }
    Map<String, List<String>> out = new LinkedHashMap<>();
    for (String item : value.split(";")) {
      int sep = item.indexOf(':');
      if (sep <= 0) {
        continue;
      }
      String table = item.substring(0, sep).replace("\\.", ".");
      List<String> columns = new ArrayList<>();
      for (String column : item.substring(sep + 1).split(",")) {
        if (!column.isEmpty()) {
          columns.add(column);
        }
      }
      if (table.isEmpty() || columns.isEmpty()) {
        continue;
      }
      out.put(table, List.copyOf(columns));
    }
    return Collections.unmodifiableMap(out);
  }
}
