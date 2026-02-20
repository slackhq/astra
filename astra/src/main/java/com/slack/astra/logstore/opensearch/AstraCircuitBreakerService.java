package com.slack.astra.logstore.opensearch;

import java.util.concurrent.atomic.AtomicLong;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.indices.breaker.AllCircuitBreakerStats;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.CircuitBreakerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Circuit breaker service that tracks memory allocations and throws {@link
 * CircuitBreakingException} when the configured limit is exceeded. This prevents aggregation
 * queries from consuming unbounded heap and OOMing the process.
 */
public class AstraCircuitBreakerService extends CircuitBreakerService {
  private static final Logger LOG = LoggerFactory.getLogger(AstraCircuitBreakerService.class);

  private final AstraCircuitBreaker breaker;

  public AstraCircuitBreakerService(long limitBytes) {
    this.breaker = new AstraCircuitBreaker(limitBytes, CircuitBreaker.REQUEST);
    LOG.info(
        "Initialized AstraCircuitBreakerService with limit {} bytes ({} MB)",
        limitBytes,
        limitBytes / (1024 * 1024));
  }

  @Override
  public CircuitBreaker getBreaker(String name) {
    return breaker;
  }

  @Override
  public AllCircuitBreakerStats stats() {
    return new AllCircuitBreakerStats(new CircuitBreakerStats[] {stats(CircuitBreaker.REQUEST)});
  }

  @Override
  public CircuitBreakerStats stats(String name) {
    return new CircuitBreakerStats(
        breaker.getName(),
        breaker.getLimit(),
        breaker.getUsed(),
        breaker.getOverhead(),
        breaker.getTrippedCount());
  }

  /**
   * Simple memory-tracking circuit breaker. Uses an AtomicLong to track the total bytes allocated
   * through BigArrays. When an allocation would push total usage over the limit, throws
   * CircuitBreakingException instead of allowing the allocation (which would eventually OOM).
   */
  static class AstraCircuitBreaker implements CircuitBreaker {
    private final AtomicLong used = new AtomicLong(0);
    private volatile long limit;
    private volatile double overhead;
    private final AtomicLong trippedCount = new AtomicLong(0);
    private final String name;

    AstraCircuitBreaker(long limit, String name) {
      this.limit = limit;
      this.overhead = 1.0;
      this.name = name;
    }

    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
      trippedCount.incrementAndGet();
      long currentUsed = used.get();
      throw new CircuitBreakingException(
          "["
              + name
              + "] Data too large, data for ["
              + fieldName
              + "] would be ["
              + (currentUsed + bytesNeeded)
              + "/"
              + bytesToString(currentUsed + bytesNeeded)
              + "], which is larger than the limit of ["
              + limit
              + "/"
              + bytesToString(limit)
              + "]",
          bytesNeeded,
          limit,
          Durability.TRANSIENT);
    }

    @Override
    public double addEstimateBytesAndMaybeBreak(long bytes, String label)
        throws CircuitBreakingException {
      if (bytes < 0) {
        // Releasing memory, always allow
        return addWithoutBreaking(bytes);
      }

      long newUsed = used.addAndGet(bytes);
      if (newUsed > limit) {
        // Roll back the allocation before breaking
        used.addAndGet(-bytes);
        circuitBreak(label, bytes);
      }
      return newUsed;
    }

    @Override
    public long addWithoutBreaking(long bytes) {
      return used.addAndGet(bytes);
    }

    @Override
    public long getUsed() {
      return used.get();
    }

    @Override
    public long getLimit() {
      return limit;
    }

    @Override
    public double getOverhead() {
      return overhead;
    }

    @Override
    public long getTrippedCount() {
      return trippedCount.get();
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Durability getDurability() {
      return Durability.TRANSIENT;
    }

    @Override
    public void setLimitAndOverhead(long limit, double overhead) {
      this.limit = limit;
      this.overhead = overhead;
    }

    private static String bytesToString(long bytes) {
      if (bytes >= 1024L * 1024L * 1024L) {
        return String.format("%.1fgb", bytes / (1024.0 * 1024.0 * 1024.0));
      } else if (bytes >= 1024L * 1024L) {
        return String.format("%.1fmb", bytes / (1024.0 * 1024.0));
      } else if (bytes >= 1024L) {
        return String.format("%.1fkb", bytes / 1024.0);
      }
      return bytes + "b";
    }
  }
}
