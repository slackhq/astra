package com.slack.astra.util;

import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.apache.logging.log4j.util.Strings;

public class AstraMeterRegistry {
    private static PrometheusMeterRegistry prometheusMeterRegistry;

    public static PrometheusMeterRegistry initPrometheusMeterRegistry(AstraConfigs.AstraConfig config) {
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

    public static PrometheusMeterRegistry getPrometheusMeterRegistry() {
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
}
