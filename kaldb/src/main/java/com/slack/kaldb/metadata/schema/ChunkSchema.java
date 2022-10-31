package com.slack.kaldb.metadata.schema;

import com.google.common.base.Objects;
import com.slack.kaldb.metadata.core.KaldbMetadata;
import java.util.Map;

/** Schema for a chunk. */
public class ChunkSchema extends KaldbMetadata {
  public final Map<String, LuceneFieldDef> fieldDefMap;
  public final Map<String, String> metadata;

  public ChunkSchema(
      String name, Map<String, LuceneFieldDef> fieldDefMap, Map<String, String> metadata) {
    super(name);
    // TODO: Ensure map keys with field name.
    this.fieldDefMap = fieldDefMap;
    this.metadata = metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ChunkSchema that = (ChunkSchema) o;
    return Objects.equal(fieldDefMap, that.fieldDefMap) && Objects.equal(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), fieldDefMap, metadata);
  }

  @Override
  public String toString() {
    return "ChunkSchema{"
        + "name='"
        + name
        + '\''
        + ", fieldDefMap="
        + fieldDefMap
        + ", metadata="
        + metadata
        + '}';
  }
}
