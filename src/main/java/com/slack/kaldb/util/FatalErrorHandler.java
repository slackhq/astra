package com.slack.kaldb.util;

/**
 * FatalErrorHandler is an interface for handling fatal errors. This is needed since Log4j2 no
 * longer has a log.fatal level.
 *
 * <p>We implement this class as an interface so we can switch out the implementation in unit tests.
 */
public interface FatalErrorHandler {
  void handleFatal(Throwable t);
}
