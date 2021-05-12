package com.slack.kaldb.metadata;

public class InternalMetadataStoreException extends RuntimeException {
  public InternalMetadataStoreException(String msg) {
    super(msg);
  }

  public InternalMetadataStoreException(String msg, Throwable t) {
    super(msg, t);
  }
}
