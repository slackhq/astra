package com.slack.astra.util;

import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.apache.logging.log4j.util.Strings;

public class MetricsFactory {
  private static PrometheusMeterRegistry _instance = null;

  public static void init(AstraConfigs.AstraConfig config) {
    if (_instance == null) {
      _instance = initPrometheusMeterRegistry(config);
    }
  }

  private static PrometheusMeterRegistry initPrometheusMeterRegistry(
      AstraConfigs.AstraConfig config) {
    PrometheusMeterRegistry prometheusMeterRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    prometheusMeterRegistry
        .config()
        .commonTags(
            "astra_cluster_name",
            config.getClusterConfig().getClusterName(),
            "astra_env",
            config.getClusterConfig().getEnv(),
            "astra_component",
            getComponentTag(config));
    return prometheusMeterRegistry;
  }

  private static String getComponentTag(AstraConfigs.AstraConfig config) {
    String component;
    if (config.getNodeRolesList().size() == 1) {
      component = config.getNodeRolesList().get(0).toString();
    } else {
      component = Strings.join(config.getNodeRolesList(), '-');
    }
    return Strings.toRootLowerCase(component);
  }

  public static PrometheusMeterRegistry getRegistry() {
    if (_instance == null) {
      throw new IllegalStateException("MetricsFactory not initialized");
    }
    return _instance;
  }
}
