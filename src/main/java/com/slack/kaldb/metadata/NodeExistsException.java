package com.slack.kaldb.metadata;

public class NodeExistsException extends RuntimeException {
  public NodeExistsException(String msg) {
    super(msg);
  }

  public NodeExistsException(String msg, Throwable t) {
    super(msg, t);
  }
}
