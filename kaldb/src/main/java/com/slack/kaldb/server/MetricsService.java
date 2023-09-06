package com.slack.kaldb.server;

import com.google.common.base.Stopwatch;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Get;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsService {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsService.class);

  private final PrometheusMeterRegistry prometheusMeterRegistry;

  public MetricsService(PrometheusMeterRegistry prometheusMeterRegistry) {
    this.prometheusMeterRegistry = prometheusMeterRegistry;
  }

  @Get
  @Blocking
  public HttpResponse getMetrics() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      return HttpResponse.of(prometheusMeterRegistry.scrape());
    } finally {
      LOG.info("Meter scrape took - {}ms", stopwatch.stop().elapsed().toMillis());
    }
  }
}
