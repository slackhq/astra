package com.slack.kaldb.metadata.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;

public class ChunkSchemaSerializerTest {
  private final ChunkSchemaSerializer serDe = new ChunkSchemaSerializer();

  @Test
  public void testChunkSchemaSerializer() throws InvalidProtocolBufferException {
    String intFieldName = "IntfieldDef";
    String intType = "integer";
    String field1 = intFieldName + "1";
    LuceneFieldDef fieldDef1 = new LuceneFieldDef(field1, intType, true, true, false, true);
    String field2 = intFieldName + "2";
    LuceneFieldDef fieldDef2 = new LuceneFieldDef(field2, intType, true, true, false, true);

    String schemaName = "schemaName";
    Map<String, LuceneFieldDef> fieldDefMap = Map.of(field1, fieldDef1, field2, fieldDef2);
    Map<String, String> metadataMap = Map.of("m1", "k1", "m2", "k2");
    ChunkSchema chunkSchema = new ChunkSchema(schemaName, fieldDefMap, metadataMap);

    String serializedSchemaDef = serDe.toJsonStr(chunkSchema);
    assertThat(serializedSchemaDef).isNotEmpty();

    ChunkSchema deserializedSchema = serDe.fromJsonStr(serializedSchemaDef);
    assertThat(deserializedSchema).isEqualTo(chunkSchema);
    assertThat(deserializedSchema.name).isEqualTo(schemaName);
    assertThat(deserializedSchema.fieldDefMap).isEqualTo(fieldDefMap);
    assertThat(deserializedSchema.metadata).isEqualTo(metadataMap);
    assertThat(deserializedSchema.fieldDefMap.keySet()).containsExactly(field1, field2);
  }

  @Test
  public void testChunkSchemaEmptySchemaMetadata() throws InvalidProtocolBufferException {
    String schemaName = "schemaName";
    ChunkSchema chunkSchema =
        new ChunkSchema(schemaName, Collections.emptyMap(), Collections.emptyMap());

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
    LuceneFieldDef fieldDef1 = new LuceneFieldDef(field1, intType, true, true, false, true);
    String field2 = intFieldName + "2";
    LuceneFieldDef fieldDef2 = new LuceneFieldDef(field2, intType, true, true, false, true);

    Map<String, LuceneFieldDef> fieldDefMap =
        Map.of(field1, fieldDef1, field2 + "error", fieldDef2);
    String schemaName = "schemaName";
    new ChunkSchema(schemaName, fieldDefMap, Collections.emptyMap());
  }
}
