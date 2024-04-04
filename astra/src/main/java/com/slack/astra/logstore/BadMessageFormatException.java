package com.slack.astra.logstore;

public class BadMessageFormatException extends RuntimeException {
  public BadMessageFormatException(String msg) {
    super(msg);
  }

  public BadMessageFormatException(String msg, Throwable t) {
    super(msg, t);
  }

  public BadMessageFormatException(Throwable t) {
    super(t);
  }
}
