package com.slack.kaldb.logstore;

/** A {@link Runnable}-like interface which allows throwing checked exceptions. */
@FunctionalInterface
public interface CheckedRunnable<E extends Exception> {
  void run() throws E;
}
