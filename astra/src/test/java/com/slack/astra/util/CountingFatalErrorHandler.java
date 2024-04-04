package com.slack.astra.util;

import java.util.concurrent.atomic.AtomicInteger;

public class CountingFatalErrorHandler implements FatalErrorHandler {
  private final AtomicInteger count = new AtomicInteger(0);

  @Override
  public void handleFatal(Throwable t) {
    count.incrementAndGet();
  }

  public int getCount() {
    return count.get();
  }
}
