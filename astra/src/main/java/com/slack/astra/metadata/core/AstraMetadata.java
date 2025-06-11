package com.slack.astra.metadata.core;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.curator.x.async.modeled.NodeName;

public abstract class AstraMetadata implements NodeName {
  public final String name;

  public AstraMetadata(String name) {
    checkArgument(name != null && !name.isEmpty(), "name can't be null or empty.");
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AstraMetadata)) return false;
    AstraMetadata that = (AstraMetadata) o;
    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Deprecated
  @Override
  // deprecated, part of the original ZK NodeName implementation
  // Will be removed along with implement NodeName once ZK is removed
  public String nodeName() {
    return name;
  }
}
