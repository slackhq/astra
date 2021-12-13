package com.slack.kaldb.metadata.replica;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.*;

import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Instant;
import org.junit.Test;

public class ReplicaMetadataSerializerTest {
  private final ReplicaMetadataSerializer serDe = new ReplicaMetadataSerializer();

  @Test
  public void testCacheSlotMetadataSerializer() throws InvalidProtocolBufferException {
    String name = "name";
    String snapshotId = "snapshotId";
    long createdTimeUtc = Instant.now().toEpochMilli();

    ReplicaMetadata replicaMetadata = new ReplicaMetadata(name, snapshotId, createdTimeUtc);

    String serializedReplicaMetadata = serDe.toJsonStr(replicaMetadata);
    assertThat(serializedReplicaMetadata).isNotEmpty();

    ReplicaMetadata deserializedReplicaMetadata = serDe.fromJsonStr(serializedReplicaMetadata);
    assertThat(deserializedReplicaMetadata).isEqualTo(replicaMetadata);

    assertThat(deserializedReplicaMetadata.name).isEqualTo(name);
    assertThat(deserializedReplicaMetadata.snapshotId).isEqualTo(snapshotId);
    assertThat(deserializedReplicaMetadata.createdTimeUtc).isEqualTo(createdTimeUtc);
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
