package com.slack.kaldb.testlib;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// The get*() functions do some exception handling.
// most likely we'll be calling getCount from an await() and waiting for the counter to match
// expected value. When the meter has not been initialized we want await() to actually wait
// till the code initializes the meter.
// If we don't add a catch block we'll throw MeterNotFoundException and await() code needs
// special handling to ensure the exception is dealt with
// so instead of every called doing something like this below we just return 0
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
public class MetricsUtil {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsUtil.class);

  public static double getCount(String counterName, MeterRegistry metricsRegistry) {
    try {
      return metricsRegistry.get(counterName).counter().count();
    } catch (MeterNotFoundException e) {
      LOG.warn("Metric not found", e);
      return 0;
    } catch (Exception e) {
      LOG.warn("Error while getting metric", e);
      throw e;
    }
  }

  public static double getValue(String guageName, MeterRegistry metricsRegistry) {
    try {
      return metricsRegistry.get(guageName).gauge().value();
    } catch (MeterNotFoundException e) {
      LOG.warn("Metric not found", e);
      return 0;
    } catch (Exception e) {
      LOG.warn("Error while getting metric", e);
      throw e;
    }
  }

  public static double getTimerCount(String timerName, MeterRegistry metricsRegistry) {
    try {
      return metricsRegistry.get(timerName).timer().count();
    } catch (MeterNotFoundException e) {
      LOG.warn("Metric not found", e);
      return 0;
    } catch (Exception e) {
      LOG.warn("Error while getting metric", e);
      throw e;
    }
  }
}
