package com.slack.kaldb.testlib;

import com.google.common.util.concurrent.AbstractIdleService;
import com.linecorp.armeria.server.Server;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;

public class TestingArmeriaServer extends AbstractIdleService {

  private final Server server;
  private final SimpleMeterRegistry meterRegistry;

  public TestingArmeriaServer(Server server, SimpleMeterRegistry meterRegistry) {
    this.server = server;
    this.meterRegistry = meterRegistry;
  }

  @Override
  protected void startUp() throws Exception {
    server.start().get(10, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    server.closeAsync().get(15, TimeUnit.SECONDS);
    meterRegistry.close();
  }
}
