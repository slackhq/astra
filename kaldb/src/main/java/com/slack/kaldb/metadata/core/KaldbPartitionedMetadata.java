package com.slack.kaldb.metadata.core;

public abstract class KaldbPartitionedMetadata extends KaldbMetadata {
  public KaldbPartitionedMetadata(String name) {
    super(name);
  }

  public abstract String getPartition();
}
