package com.slack.kaldb.logstore;

public class UnSupportedPropertyTypeException extends RuntimeException {

  public UnSupportedPropertyTypeException(String msg) {
    super(msg);
  }

  public UnSupportedPropertyTypeException(String msg, Throwable t) {
    super(msg, t);
  }

  public UnSupportedPropertyTypeException(Throwable t) {
    super(t);
  }
}
