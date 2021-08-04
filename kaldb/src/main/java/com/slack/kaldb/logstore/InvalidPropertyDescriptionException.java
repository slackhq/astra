package com.slack.kaldb.logstore;

public class InvalidPropertyDescriptionException extends RuntimeException {
  public InvalidPropertyDescriptionException(String msg) {
    super(msg);
  }

  public InvalidPropertyDescriptionException(String msg, Throwable t) {
    super(msg, t);
  }

  public InvalidPropertyDescriptionException(Throwable t) {
    super(t);
  }
}
