/*
 * Task 23.2 — custom Aurora DSQL Kafka Connect sink connector.
 */
package dev.dsqlmigrator.connect;

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
 * {@code pk.fields} setting. The DLQ is handled by the Kafka Connect framework
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
              "Max statement-level OCC (40001) retries.")
          .define(
              RETRY_BACKOFF_MS,
              Type.LONG,
              50L,
              Importance.LOW,
              "Base backoff for OCC retry (ms).");

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
}
