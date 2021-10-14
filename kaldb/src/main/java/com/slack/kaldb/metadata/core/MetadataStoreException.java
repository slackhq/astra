package com.slack.kaldb.metadata.core;

public class MetadataStoreException extends RuntimeException {
  public MetadataStoreException(String msg) {
    super(msg);
  }

  public MetadataStoreException(String msg, Throwable t) {
    super(msg, t);
  }
}
