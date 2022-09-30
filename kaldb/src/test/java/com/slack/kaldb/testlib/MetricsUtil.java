package com.slack.kaldb.testlib;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.MeterNotFoundException;

public class MetricsUtil {

  public static double getCount(String counterName, MeterRegistry metricsRegistry) {
    try {
      return metricsRegistry.get(counterName).counter().count();
    } catch (MeterNotFoundException e) {
      // most likely we'll be calling getCount from a await() and waiting for the counter to match
      // expected value
      // Now the thing is, when the meter has not been initialized we want await() to actually wait
      // till the code initializes the meter
      // if we don't add this catch block we'll throw MeterNotFoundException and await() code needs
      // special handling to ensure the excpeiton is dealt with
      // so instead of every called doing something like the example snippet below we just return 0
      //      await()
      //              .until(
      //                      () -> {
      //                        try {
      //                          double count = getCount(METRIC_NAME, meterRegistry);
      //                          LOG.debug("METRIC_NAME current_count={} total_count={}", count,
      // meterRegistry);
      //                          return count == <count_expected>;
      //                        } catch (MeterNotFoundException e) {
      //                          return false;
      //                        }
      //                      });
      return 0;
    }
  }

  public static double getValue(String guageName, MeterRegistry metricsRegistry) {
    return metricsRegistry.get(guageName).gauge().value();
  }

  public static double getTimerCount(String timerName, MeterRegistry meterRegistry) {
    return meterRegistry.get(timerName).timer().count();
  }
}
