package com.slack.kaldb.logstore;

public class FieldDefMismatchException extends RuntimeException {
  public FieldDefMismatchException(String msg) {
    super(msg);
  }

  public FieldDefMismatchException(String msg, Throwable t) {
    super(msg, t);
  }

  public FieldDefMismatchException(Throwable t) {
    super(t);
  }
}
