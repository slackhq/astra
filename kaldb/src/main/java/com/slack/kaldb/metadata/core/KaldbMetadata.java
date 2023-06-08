package com.slack.kaldb.metadata.core;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.curator.x.async.modeled.NodeName;

public abstract class KaldbMetadata implements NodeName {
  public final String name;

  public KaldbMetadata(String name) {
    checkArgument(name != null && !name.isEmpty(), "name can't be null or empty.");
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KaldbMetadata that = (KaldbMetadata) o;
    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String nodeName() {
    return name;
  }
}
