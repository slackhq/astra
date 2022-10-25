package com.slack.kaldb.logstore;

public class InvalidFieldDefException extends RuntimeException {
  public InvalidFieldDefException(String msg) {
    super(msg);
  }

  public InvalidFieldDefException(String msg, Throwable t) {
    super(msg, t);
  }

  public InvalidFieldDefException(Throwable t) {
    super(t);
  }
}
