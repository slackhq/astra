package com.slack.kaldb.logstore;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.apache.lucene.util.InfoStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An {@link InfoStream} implementation which passes messages on to Kaldb's logging */
public class LoggingInfoStream extends InfoStream {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void message(String component, String message) {
    if (log.isInfoEnabled()) {
      log.info("[{}][{}]: {}", component, Thread.currentThread().getName(), message);
    }
  }

  @Override
  public boolean isEnabled(String component) {
    // ignore testpoints so this can be used with tests without flooding logs with verbose messages
    return !"TP".equals(component) && log.isInfoEnabled();
  }

  @Override
  public void close() throws IOException {}
}
