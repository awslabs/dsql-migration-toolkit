// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Custom Aurora DSQL Kafka Connect sink connector. See ../../../../README.md.
 */
package dev.dsqlmigrator.connect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * Kafka Connect {@link SinkConnector} entrypoint for the custom DSQL sink.
 *
 * <p>Registered as an MSK Connect custom plugin. It only wires config to tasks;
 * the apply logic lives in {@link DsqlSinkTask}. Effective parallelism is bounded
 * by the topic partition count (the control plane sets {@code tasks.max}).
 */
public class DsqlSinkConnector extends SinkConnector {

  private Map<String, String> props;

  @Override
  public String version() {
    return "0.1.0-SNAPSHOT";
  }

  @Override
  public void start(Map<String, String> props) {
    this.props = new HashMap<>(props);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return DsqlSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> configs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; i++) {
      configs.add(new HashMap<>(props));
    }
    return configs;
  }

  @Override
  public void stop() {
    // No connector-level resources; tasks own their JDBC connections.
  }

  @Override
  public ConfigDef config() {
    return DsqlSinkConnectorConfig.configDef();
  }
}
