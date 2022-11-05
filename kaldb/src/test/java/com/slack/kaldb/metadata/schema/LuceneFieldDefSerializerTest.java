package com.slack.kaldb.metadata.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;

public class LuceneFieldDefSerializerTest {
  private final LuceneFieldDefSerializer serDe = new LuceneFieldDefSerializer();

  @Test
  public void testLuceneFieldDefSerializer() throws InvalidProtocolBufferException {
    String intFieldName = "IntfieldDef";
    String intType = "integer";
    LuceneFieldDef fieldDef = new LuceneFieldDef(intFieldName, intType, true, true, false, true);

    String serializedFieldDef = serDe.toJsonStr(fieldDef);
    assertThat(serializedFieldDef).isNotEmpty();

    LuceneFieldDef deserializedFieldDef = serDe.fromJsonStr(serializedFieldDef);
    assertThat(deserializedFieldDef).isEqualTo(fieldDef);

    assertThat(deserializedFieldDef.name).isEqualTo(intFieldName);
    assertThat(deserializedFieldDef.fieldType).isEqualTo(FieldType.INTEGER);
    assertThat(deserializedFieldDef.isStored).isTrue();
    assertThat(deserializedFieldDef.isIndexed).isTrue();
    assertThat(deserializedFieldDef.isAnalyzed).isFalse();
    assertThat(deserializedFieldDef.storeNumericDocValue).isTrue();
  }

  @Test
  public void testInvalidSerializations() {
    Throwable serializeNull = catchThrowable(() -> serDe.toJsonStr(null));
    assertThat(serializeNull).isInstanceOf(IllegalArgumentException.class);

    Throwable deserializeNull = catchThrowable(() -> serDe.fromJsonStr(null));
    assertThat(deserializeNull).isInstanceOf(InvalidProtocolBufferException.class);

    Throwable deserializeEmpty = catchThrowable(() -> serDe.fromJsonStr(""));
    assertThat(deserializeEmpty).isInstanceOf(InvalidProtocolBufferException.class);

    Throwable deserializeCorrupt = catchThrowable(() -> serDe.fromJsonStr("test"));
    assertThat(deserializeCorrupt).isInstanceOf(InvalidProtocolBufferException.class);
  }
}
