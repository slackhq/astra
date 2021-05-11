package com.slack.kaldb.util;

public class CountingFatalErrorHandler implements FatalErrorHandler {
  private int count = 0;

  @Override
  public void handleFatal(Throwable t) {
    count = count + 1;
  }

  public int getCount() {
    return count;
  }
}
