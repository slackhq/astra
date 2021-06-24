package com.slack.kaldb.metadata.core;

import static com.google.common.base.Preconditions.checkState;

public abstract class KaldbMetadata {
  public final String name;

  public KaldbMetadata(String name) {
    checkState(name != null && !name.isEmpty(), "name can't be null or empty.");
    this.name = name;
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
}
