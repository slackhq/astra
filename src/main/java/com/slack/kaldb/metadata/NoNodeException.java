package com.slack.kaldb.metadata;

public class NoNodeException extends RuntimeException {
  public NoNodeException(String msg) {
    super(msg);
  }

  public NoNodeException(String msg, Throwable t) {
    super(msg, t);
  }
}
