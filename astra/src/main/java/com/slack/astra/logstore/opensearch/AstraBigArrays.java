package com.slack.astra.logstore.opensearch;

import static org.opensearch.common.util.PageCacheRecycler.LIMIT_HEAP_SETTING;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;

/**
 * Astra singleton wrapper for an OpenSearch BigArrays implementation. Only one BigArrays should be
 * initialized per node (see Node.createBigArrays())
 */
public class AstraBigArrays {
  private static BigArrays bigArray = null;

  private AstraBigArrays() {}

  public static BigArrays getInstance() {
    if (bigArray == null) {
      PageCacheRecycler pageCacheRecycler =
          new PageCacheRecycler(Settings.builder().put(LIMIT_HEAP_SETTING.getKey(), "10%").build());
      bigArray =
          new BigArrays(pageCacheRecycler, new NoneCircuitBreakerService(), "NoneCircuitBreaker");
    }
    return bigArray;
  }
}
