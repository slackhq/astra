package com.slack.kaldb.metadata.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Instant;
import org.junit.Test;

public class RecoveryTaskMetadataSerializerTest {
  private final RecoveryTaskMetadataSerializer serDe = new RecoveryTaskMetadataSerializer();

  @Test
  public void testRecoverySlotMetadataSerializer() throws InvalidProtocolBufferException {
    String name = "name";
    String partitionId = "partitionId";
    long startOffset = 0;
    long endOffset = 1;
    long createdTimeEpochMs = Instant.now().toEpochMilli();

    RecoveryTaskMetadata recoveryTaskMetadata =
        new RecoveryTaskMetadata(name, partitionId, startOffset, endOffset, createdTimeEpochMs);

    String serializedRecoveryTaskMetadata = serDe.toJsonStr(recoveryTaskMetadata);
    assertThat(serializedRecoveryTaskMetadata).isNotEmpty();

    RecoveryTaskMetadata deserializedRecoveryTaskMetadata =
        serDe.fromJsonStr(serializedRecoveryTaskMetadata);
    assertThat(deserializedRecoveryTaskMetadata).isEqualTo(recoveryTaskMetadata);

    assertThat(deserializedRecoveryTaskMetadata.name).isEqualTo(name);
    assertThat(deserializedRecoveryTaskMetadata.partitionId).isEqualTo(partitionId);
    assertThat(deserializedRecoveryTaskMetadata.startOffset).isEqualTo(startOffset);
    assertThat(deserializedRecoveryTaskMetadata.endOffset).isEqualTo(endOffset);
    assertThat(deserializedRecoveryTaskMetadata.createdTimeEpochMs).isEqualTo(createdTimeEpochMs);
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
