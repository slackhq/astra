package com.slack.astra.metadata.redaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataSerializer;
import java.time.Instant;
import org.junit.jupiter.api.Test;

public class FieldRedactionMetadataSerializerTest {
  private final FieldRedactionMetadataSerializer serializer =
      new FieldRedactionMetadataSerializer();

  @Test
  public void testCacheNodeAssignmentSerializer() throws InvalidProtocolBufferException {
    final String name = "testRedaction";
    final String fieldName = "testField";
    final long start = Instant.now().toEpochMilli();
    final long end = Instant.now().plusSeconds(10).toEpochMilli();

    FieldRedactionMetadata fieldRedactionMetadata =
        new FieldRedactionMetadata(name, fieldName, start, end);

    String serializedFieldRedactionMetadata = serializer.toJsonStr(fieldRedactionMetadata);
    assertThat(serializedFieldRedactionMetadata).isNotEmpty();

    FieldRedactionMetadata deserializedFieldRedactionMetadata =
        serializer.fromJsonStr(serializedFieldRedactionMetadata);
    assertThat(deserializedFieldRedactionMetadata).isEqualTo(fieldRedactionMetadata);

    assertThat(deserializedFieldRedactionMetadata.name).isEqualTo(name);
    assertThat(deserializedFieldRedactionMetadata.fieldName).isEqualTo(fieldName);
    assertThat(deserializedFieldRedactionMetadata.startTimeEpochMs).isEqualTo(start);
    assertThat(deserializedFieldRedactionMetadata.endTimeEpochMs).isEqualTo(end);
  }

  @Test
  public void testInvalidSerializations() {
    Throwable serializeNull = catchThrowable(() -> serializer.toJsonStr(null));
    assertThat(serializeNull).isInstanceOf(IllegalArgumentException.class);

    Throwable deserializeNull = catchThrowable(() -> serializer.fromJsonStr(null));
    assertThat(deserializeNull).isInstanceOf(InvalidProtocolBufferException.class);

    Throwable deserializeEmpty = catchThrowable(() -> serializer.fromJsonStr(""));
    assertThat(deserializeEmpty).isInstanceOf(InvalidProtocolBufferException.class);

    Throwable deserializeCorrupt = catchThrowable(() -> serializer.fromJsonStr("test"));
    assertThat(deserializeCorrupt).isInstanceOf(InvalidProtocolBufferException.class);
  }
}
