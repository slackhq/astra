package com.slack.kaldb.metadata.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Instant;
import org.junit.Test;

public class RecoveryNodeMetadataSerializerTest {
  private final RecoveryNodeMetadataSerializer serDe = new RecoveryNodeMetadataSerializer();

  @Test
  public void testRecoveryNodeMetadataSerializer() throws InvalidProtocolBufferException {
    String name = "name";
    Metadata.RecoveryNodeMetadata.RecoveryNodeState recoveryNodeState =
        Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED;
    String recoveryTaskName = "taskName";
    long updatedTimeUtc = Instant.now().toEpochMilli();

    RecoveryNodeMetadata recoveryNodeMetadata =
        new RecoveryNodeMetadata(name, recoveryNodeState, recoveryTaskName, updatedTimeUtc);

    String serializedCacheSlotMetadata = serDe.toJsonStr(recoveryNodeMetadata);
    assertThat(serializedCacheSlotMetadata).isNotEmpty();

    RecoveryNodeMetadata deserializedRecoveryNodeMetadata =
        serDe.fromJsonStr(serializedCacheSlotMetadata);
    assertThat(deserializedRecoveryNodeMetadata).isEqualTo(recoveryNodeMetadata);

    assertThat(deserializedRecoveryNodeMetadata.name).isEqualTo(name);
    assertThat(deserializedRecoveryNodeMetadata.recoveryNodeState).isEqualTo(recoveryNodeState);
    assertThat(deserializedRecoveryNodeMetadata.recoveryTaskName).isEqualTo(recoveryTaskName);
    assertThat(deserializedRecoveryNodeMetadata.updatedTimeUtc).isEqualTo(updatedTimeUtc);
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
