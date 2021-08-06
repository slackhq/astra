package com.slack.kaldb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuntimeHalterImpl implements FatalErrorHandler {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeHalterImpl.class);

  @Override
  public void handleFatal(Throwable t) {
    t.printStackTrace();
    LOG.error("Runtime halter is called probably on an unrecoverable error. Stopping the VM.", t);
    // TODO: Make the exit more cleaner, once the system is more stable.
    System.exit(1);
  }
}
