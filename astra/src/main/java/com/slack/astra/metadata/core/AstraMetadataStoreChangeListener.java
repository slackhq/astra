package com.slack.astra.metadata.core;

public interface AstraMetadataStoreChangeListener<T> {
  void onMetadataStoreChanged(T model);
}
