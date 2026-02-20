package com.slack.astra.logstore.opensearch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.indices.breaker.CircuitBreakerStats;

public class AstraCircuitBreakerServiceTest {

  private AstraCircuitBreakerService service;

  @BeforeEach
  void setUp() {
    service = new AstraCircuitBreakerService(1024);
  }

  @AfterEach
  void tearDown() {
    service = null;
  }

  @Test
  void testAllocationUnderLimit() {
    CircuitBreaker breaker = service.getBreaker(CircuitBreaker.REQUEST);
    breaker.addEstimateBytesAndMaybeBreak(512, "testAllocation");
    assertThat(breaker.getUsed()).isEqualTo(512);
  }

  @Test
  void testAllocationExceedingLimitThrowsException() {
    CircuitBreaker breaker = service.getBreaker(CircuitBreaker.REQUEST);
    breaker.addEstimateBytesAndMaybeBreak(512, "first");

    assertThatThrownBy(() -> breaker.addEstimateBytesAndMaybeBreak(1024, "second"))
        .isInstanceOf(CircuitBreakingException.class)
        .hasMessageContaining("Data too large");

    // Usage should not have increased after the failed allocation
    assertThat(breaker.getUsed()).isEqualTo(512);
  }

  @Test
  void testTrippedCountIncrements() {
    CircuitBreaker breaker = service.getBreaker(CircuitBreaker.REQUEST);
    assertThat(breaker.getTrippedCount()).isEqualTo(0);

    breaker.addEstimateBytesAndMaybeBreak(1000, "fill");

    try {
      breaker.addEstimateBytesAndMaybeBreak(100, "overflow");
    } catch (CircuitBreakingException e) {
      // expected
    }

    assertThat(breaker.getTrippedCount()).isEqualTo(1);
  }

  @Test
  void testReleasingMemoryFreesCapacity() {
    CircuitBreaker breaker = service.getBreaker(CircuitBreaker.REQUEST);
    breaker.addEstimateBytesAndMaybeBreak(800, "allocate");
    assertThat(breaker.getUsed()).isEqualTo(800);

    breaker.addWithoutBreaking(-500);
    assertThat(breaker.getUsed()).isEqualTo(300);

    // Should now be able to allocate more since we freed capacity
    breaker.addEstimateBytesAndMaybeBreak(700, "reallocate");
    assertThat(breaker.getUsed()).isEqualTo(1000);
  }

  @Test
  void testExactLimitAllocationSucceeds() {
    CircuitBreaker breaker = service.getBreaker(CircuitBreaker.REQUEST);
    breaker.addEstimateBytesAndMaybeBreak(1024, "exactLimit");
    assertThat(breaker.getUsed()).isEqualTo(1024);
  }

  @Test
  void testOneByteOverLimitFails() {
    CircuitBreaker breaker = service.getBreaker(CircuitBreaker.REQUEST);
    assertThatThrownBy(() -> breaker.addEstimateBytesAndMaybeBreak(1025, "overByOne"))
        .isInstanceOf(CircuitBreakingException.class);
  }

  @Test
  void testStatsReporting() {
    CircuitBreaker breaker = service.getBreaker(CircuitBreaker.REQUEST);
    breaker.addEstimateBytesAndMaybeBreak(256, "stats");

    CircuitBreakerStats stats = service.stats(CircuitBreaker.REQUEST);
    assertThat(stats.getName()).isEqualTo(CircuitBreaker.REQUEST);
    assertThat(stats.getLimit()).isEqualTo(1024);
    assertThat(stats.getEstimated()).isEqualTo(256);
    assertThat(stats.getTrippedCount()).isEqualTo(0);
  }

  @Test
  void testGetBreakerReturnsConsistentInstance() {
    CircuitBreaker breaker1 = service.getBreaker(CircuitBreaker.REQUEST);
    CircuitBreaker breaker2 = service.getBreaker(CircuitBreaker.FIELDDATA);
    // All names return the same breaker since we use a single shared breaker
    assertThat(breaker1).isSameAs(breaker2);
  }

  @Test
  void testBreakerProperties() {
    CircuitBreaker breaker = service.getBreaker(CircuitBreaker.REQUEST);
    assertThat(breaker.getName()).isEqualTo(CircuitBreaker.REQUEST);
    assertThat(breaker.getLimit()).isEqualTo(1024);
    assertThat(breaker.getDurability()).isEqualTo(CircuitBreaker.Durability.TRANSIENT);
    assertThat(breaker.getOverhead()).isEqualTo(1.0);
  }
}
