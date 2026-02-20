package com.slack.astra.logstore.opensearch;

import static org.opensearch.common.util.PageCacheRecycler.LIMIT_HEAP_SETTING;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Astra singleton wrapper for an OpenSearch BigArrays implementation. Only one BigArrays should be
 * initialized per node (see Node.createBigArrays())
 */
public class AstraBigArrays {
  private static final Logger LOG = LoggerFactory.getLogger(AstraBigArrays.class);

  private static final String ENABLE_CIRCUIT_BREAKER = "astra.enableCircuitBreaker";
  private static final String CIRCUIT_BREAKER_HEAP_PERCENTAGE =
      "astra.circuitBreaker.heapPercentage";
  private static final int DEFAULT_HEAP_PERCENTAGE = 70;

  private static BigArrays bigArray = null;
  private static CircuitBreakerService circuitBreakerService = null;

  private AstraBigArrays() {}

  public static synchronized BigArrays getInstance() {
    if (bigArray == null) {
      circuitBreakerService = buildCircuitBreakerService();
      PageCacheRecycler pageCacheRecycler =
          new PageCacheRecycler(Settings.builder().put(LIMIT_HEAP_SETTING.getKey(), "10%").build());
      bigArray = new BigArrays(pageCacheRecycler, circuitBreakerService, CircuitBreaker.REQUEST);
    }
    return bigArray;
  }

  public static synchronized CircuitBreakerService getCircuitBreakerService() {
    if (circuitBreakerService == null) {
      getInstance();
    }
    return circuitBreakerService;
  }

  /** Reset the singleton state. Only intended for use in tests. */
  static synchronized void reset() {
    bigArray = null;
    circuitBreakerService = null;
  }

  private static CircuitBreakerService buildCircuitBreakerService() {
    if (Boolean.getBoolean(ENABLE_CIRCUIT_BREAKER)) {
      int heapPercentage =
          Integer.parseInt(
              System.getProperty(
                  CIRCUIT_BREAKER_HEAP_PERCENTAGE, String.valueOf(DEFAULT_HEAP_PERCENTAGE)));
      long maxHeap = Runtime.getRuntime().maxMemory();
      long limitBytes = (long) (maxHeap * (heapPercentage / 100.0));
      LOG.info(
          "Circuit breaker enabled: heapPercentage={}, maxHeap={} MB, limit={} MB",
          heapPercentage,
          maxHeap / (1024 * 1024),
          limitBytes / (1024 * 1024));
      return new AstraCircuitBreakerService(limitBytes);
    } else {
      LOG.info("Circuit breaker disabled, using NoneCircuitBreakerService");
      return new NoneCircuitBreakerService();
    }
  }
}
