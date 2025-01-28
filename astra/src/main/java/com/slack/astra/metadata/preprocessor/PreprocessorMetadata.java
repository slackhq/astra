package com.slack.astra.metadata.preprocessor;

import com.slack.astra.metadata.core.AstraMetadata;
import java.util.UUID;

/** A container for all the metadata needed by preprocessors. */
public class PreprocessorMetadata extends AstraMetadata {

  public PreprocessorMetadata() {
    super(UUID.randomUUID().toString());
  }

  public PreprocessorMetadata(String name) {
    super(name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PreprocessorMetadata that)) return false;
    return !super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return "PreprocessorMetadata{" + '\'' + "name='" + name + '\'' + '}';
  }
}
