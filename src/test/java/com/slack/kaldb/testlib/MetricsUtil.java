package com.slack.kaldb.testlib;

import io.micrometer.core.instrument.MeterRegistry;

public class MetricsUtil {
  public static double getCount(String counterName, MeterRegistry metricsRegistry) {
    return metricsRegistry.get(counterName).counter().count();
  }

  public static double getValue(String guageName, MeterRegistry metricsRegistry) {
    return metricsRegistry.get(guageName).gauge().value();
  }
}
