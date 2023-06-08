package com.slack.kaldb.metadata.core;

public interface KaldbMetadataStoreChangeListener<T> {
  void onMetadataStoreChanged(T model);
}
