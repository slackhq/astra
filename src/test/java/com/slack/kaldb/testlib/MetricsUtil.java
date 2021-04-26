package com.slack.kaldb.testlib;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class MetricsUtil {
  public static double getCount(String counterName, SimpleMeterRegistry metricsRegistry) {
    return metricsRegistry.get(counterName).counter().count();
  }

  public static double getValue(String guageName, SimpleMeterRegistry metricsRegistry) {
    return metricsRegistry.get(guageName).gauge().value();
  }
}
