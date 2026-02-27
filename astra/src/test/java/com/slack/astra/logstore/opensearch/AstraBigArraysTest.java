package com.slack.astra.logstore.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;

public class AstraBigArraysTest {

  @BeforeEach
  void setUp() {
    AstraBigArrays.reset();
  }

  @AfterEach
  void tearDown() {
    System.clearProperty("astra.enableCircuitBreaker");
    System.clearProperty("astra.circuitBreaker.heapPercentage");
    AstraBigArrays.reset();
  }

  @Test
  void testDefaultUsesNoneCircuitBreaker() {
    CircuitBreakerService service = AstraBigArrays.getCircuitBreakerService();
    assertThat(service).isInstanceOf(NoneCircuitBreakerService.class);
  }

  @Test
  void testEnabledFlagUsesAstraCircuitBreaker() {
    System.setProperty("astra.enableCircuitBreaker", "true");
    CircuitBreakerService service = AstraBigArrays.getCircuitBreakerService();
    assertThat(service).isInstanceOf(AstraCircuitBreakerService.class);
  }

  @Test
  void testGetInstanceReturnsBigArrays() {
    BigArrays bigArrays = AstraBigArrays.getInstance();
    assertThat(bigArrays).isNotNull();
  }

  @Test
  void testSingletonBehavior() {
    BigArrays first = AstraBigArrays.getInstance();
    BigArrays second = AstraBigArrays.getInstance();
    assertThat(first).isSameAs(second);
  }

  @Test
  void testCircuitBreakerServiceConsistentWithBigArrays() {
    System.setProperty("astra.enableCircuitBreaker", "true");
    BigArrays bigArrays = AstraBigArrays.getInstance();
    CircuitBreakerService service = AstraBigArrays.getCircuitBreakerService();
    assertThat(bigArrays).isNotNull();
    assertThat(service).isInstanceOf(AstraCircuitBreakerService.class);
  }

  @Test
  void testCustomHeapPercentage() {
    System.setProperty("astra.enableCircuitBreaker", "true");
    System.setProperty("astra.circuitBreaker.heapPercentage", "50");
    CircuitBreakerService service = AstraBigArrays.getCircuitBreakerService();
    assertThat(service).isInstanceOf(AstraCircuitBreakerService.class);

    long maxHeap = Runtime.getRuntime().maxMemory();
    long expectedLimit = (long) (maxHeap * 0.5);
    long actualLimit = service.stats("request").getLimit();
    assertThat(actualLimit).isEqualTo(expectedLimit);
  }
}
