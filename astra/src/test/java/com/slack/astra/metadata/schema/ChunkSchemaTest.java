package com.slack.astra.metadata.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

public class ChunkSchemaTest {
  @Test
  public void testChunkSchema() {
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

    assertThat(chunkSchema.name).isEqualTo(schemaName);
    assertThat(chunkSchema.fieldDefMap).isEqualTo(fieldDefMap);
    assertThat(chunkSchema.metadata).isEqualTo(metadataMap);
  }

  @Test
  public void testChunkSchemaEqualsHashCode() {
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
    final ChunkSchema chunkSchema1 = new ChunkSchema(schemaName, fieldDefMap, metadataMap);
    assertThat(chunkSchema1.name).isEqualTo(schemaName);
    assertThat(chunkSchema1.fieldDefMap).isEqualTo(fieldDefMap);
    assertThat(chunkSchema1.metadata).isEqualTo(metadataMap);

    final ChunkSchema chunkSchema2 =
        new ChunkSchema(schemaName, fieldDefMap, new ConcurrentHashMap<>());
    final ChunkSchema chunkSchema3 =
        new ChunkSchema(schemaName, new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
    final ChunkSchema chunkSchema4 =
        new ChunkSchema(schemaName + "1", new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
    final ChunkSchema chunkSchema5 =
        new ChunkSchema(schemaName + "1", fieldDefMap, new ConcurrentHashMap<>());
    final ConcurrentHashMap<String, LuceneFieldDef> fieldDefMap2 = new ConcurrentHashMap<>();
    fieldDefMap2.put(field1, fieldDef1);
    fieldDefMap2.put(field2, fieldDef2);
    final ConcurrentHashMap<String, String> metadataMap2 = new ConcurrentHashMap<>();
    metadataMap2.put("m1", "k1");
    metadataMap2.put("m2", "v2");
    final ChunkSchema chunkSchema6 = new ChunkSchema(schemaName, fieldDefMap2, metadataMap2);

    assertThat(chunkSchema1).isNotEqualTo(chunkSchema2);
    assertThat(chunkSchema1).isNotEqualTo(chunkSchema3);
    assertThat(chunkSchema1).isNotEqualTo(chunkSchema4);
    assertThat(chunkSchema1).isNotEqualTo(chunkSchema5);
    assertThat(chunkSchema1).isEqualTo(chunkSchema6);

    assertThat(chunkSchema1.hashCode()).isEqualTo(chunkSchema1.hashCode());
    assertThat(chunkSchema3.hashCode()).isEqualTo(chunkSchema3.hashCode());
    assertThat(chunkSchema1.hashCode()).isNotEqualTo(chunkSchema2.hashCode());
    assertThat(chunkSchema1.hashCode()).isNotEqualTo(chunkSchema3.hashCode());
    assertThat(chunkSchema1.hashCode()).isNotEqualTo(chunkSchema4.hashCode());
    assertThat(chunkSchema1.hashCode()).isNotEqualTo(chunkSchema5.hashCode());
    assertThat(chunkSchema1.hashCode()).isEqualTo(chunkSchema6.hashCode());
  }

  @Test
  public void testInvalidArgumentShouldThrow() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> new ChunkSchema("", new ConcurrentHashMap<>(), new ConcurrentHashMap<>()));
  }
}
