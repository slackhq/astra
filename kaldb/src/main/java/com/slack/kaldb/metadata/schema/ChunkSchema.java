package com.slack.kaldb.metadata.schema;

import com.google.common.base.Objects;
import com.slack.kaldb.metadata.core.KaldbMetadata;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This schema class enforces schema for a chunk. The schema is only written in indexer and on the
 * cache node the schema is read only.
 *
 * <p>The fieldMap is a map to the field name and LuceneFieldDef that stores the field definitions.
 * Since this field is written and read my multiple threads it is hardcoded as a ConcurrentHashMap.
 * New fields are added to the fieldMap when it's written and read during query.
 */
public class ChunkSchema extends KaldbMetadata {
  public static ChunkSchemaSerializer serDe = new ChunkSchemaSerializer();

  public static void serializeToFile(ChunkSchema chunkSchema, File file) throws IOException {
    Files.writeString(file.toPath(), serDe.toJsonStr(chunkSchema));
  }

  public static ChunkSchema deserializeFromFile(File file) throws IOException {
    return serDe.fromJsonStr(Files.readString(file.toPath()));
  }

  public final ConcurrentHashMap<String, LuceneFieldDef> fieldDefMap;
  public final ConcurrentHashMap<String, String> metadata;

  public ChunkSchema(
      String name,
      ConcurrentHashMap<String, LuceneFieldDef> fieldDefMap,
      ConcurrentHashMap<String, String> metadata) {
    super(name);
    for (String key : fieldDefMap.keySet()) {
      String fieldName = fieldDefMap.get(key).name;
      if (!key.equals(fieldName)) {
        throw new IllegalArgumentException(
            "The name of the key in the map should match the field " + fieldName);
      }
    }
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
