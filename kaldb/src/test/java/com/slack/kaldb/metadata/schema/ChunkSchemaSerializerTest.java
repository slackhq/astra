package com.slack.kaldb.metadata.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ChunkSchemaSerializerTest {
  private final ChunkSchemaSerializer serDe = new ChunkSchemaSerializer();

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testChunkSchemaSerializer() throws IOException {
    final String intFieldName = "IntfieldDef";
    final String intType = "integer";
    final String field1 = intFieldName + "1";
    final LuceneFieldDef fieldDef1 = new LuceneFieldDef(field1, intType, true, true, true);
    final String field2 = intFieldName + "2";
    final LuceneFieldDef fieldDef2 = new LuceneFieldDef(field2, intType, true, true, true);

    final String schemaName = "schemaName";
    final ConcurrentHashMap<String, LuceneFieldDef> fieldDefMap = new ConcurrentHashMap<>();
    fieldDefMap.put(field1, fieldDef1);
    fieldDefMap.put(field2, fieldDef2);
    final ConcurrentHashMap<String, String> metadataMap = new ConcurrentHashMap<>();
    metadataMap.put("m1", "k1");
    metadataMap.put("m2", "v2");
    final ChunkSchema chunkSchema = new ChunkSchema(schemaName, fieldDefMap, metadataMap);

    final String serializedSchemaDef = serDe.toJsonStr(chunkSchema);
    assertThat(serializedSchemaDef).isNotEmpty();

    final ChunkSchema deserializedSchema = serDe.fromJsonStr(serializedSchemaDef);
    assertThat(deserializedSchema).isEqualTo(chunkSchema);
    assertThat(deserializedSchema.name).isEqualTo(schemaName);
    assertThat(deserializedSchema.fieldDefMap).isEqualTo(fieldDefMap);
    assertThat(deserializedSchema.metadata).isEqualTo(metadataMap);
    assertThat(deserializedSchema.fieldDefMap.keySet()).containsExactly(field1, field2);

    // Serialize and deserialize to a file.
    final File tempFile = tempFolder.newFile("tempSchema.json");
    assertThat(Files.size(tempFile.toPath())).isZero();
    ChunkSchema.serializeToFile(chunkSchema, tempFile);
    assertThat(Files.size(tempFile.toPath())).isNotZero();
    final ChunkSchema schemaFromFile = ChunkSchema.deserializeFromFile(tempFile);
    assertThat(schemaFromFile).isEqualTo(chunkSchema);
  }

  @Test
  public void testChunkSchemaEmptySchemaMetadata() throws InvalidProtocolBufferException {
    String schemaName = "schemaName";
    ChunkSchema chunkSchema =
        new ChunkSchema(schemaName, new ConcurrentHashMap<>(), new ConcurrentHashMap<>());

    String serializedSchemaDef = serDe.toJsonStr(chunkSchema);
    assertThat(serializedSchemaDef).isNotEmpty();

    ChunkSchema deserializedSchema = serDe.fromJsonStr(serializedSchemaDef);
    assertThat(deserializedSchema).isEqualTo(chunkSchema);
    assertThat(deserializedSchema.name).isEqualTo(schemaName);
    assertThat(deserializedSchema.fieldDefMap).isEqualTo(Collections.emptyMap());
    assertThat(deserializedSchema.metadata).isEqualTo(Collections.emptyMap());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testChunkSchemaException() {
    String intFieldName = "IntfieldDef";
    String intType = "integer";
    String field1 = intFieldName + "1";
    LuceneFieldDef fieldDef1 = new LuceneFieldDef(field1, intType, true, true, true);
    String field2 = intFieldName + "2";
    LuceneFieldDef fieldDef2 = new LuceneFieldDef(field2, intType, true, true, true);

    ConcurrentHashMap<String, LuceneFieldDef> fieldDefMap = new ConcurrentHashMap<>();
    fieldDefMap.put(field1, fieldDef1);
    fieldDefMap.put(field2 + "error", fieldDef2);
    String schemaName = "schemaName";
    new ChunkSchema(schemaName, fieldDefMap, new ConcurrentHashMap<>());
  }
}
