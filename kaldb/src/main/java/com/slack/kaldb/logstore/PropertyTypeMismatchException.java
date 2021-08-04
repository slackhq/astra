package com.slack.kaldb.logstore;

public class PropertyTypeMismatchException extends RuntimeException {
  public PropertyTypeMismatchException(String msg) {
    super(msg);
  }

  public PropertyTypeMismatchException(String msg, Throwable t) {
    super(msg, t);
  }

  public PropertyTypeMismatchException(Throwable t) {
    super(t);
  }
}
