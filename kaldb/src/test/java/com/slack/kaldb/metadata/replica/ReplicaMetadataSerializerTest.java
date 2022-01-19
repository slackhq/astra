package com.slack.kaldb.metadata.replica;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Instant;
import org.junit.Test;

public class ReplicaMetadataSerializerTest {
  private final ReplicaMetadataSerializer serDe = new ReplicaMetadataSerializer();

  @Test
  public void testReplicaMetadataSerializer() throws InvalidProtocolBufferException {
    String name = "name";
    String snapshotId = "snapshotId";
    long createdTimeUtc = Instant.now().toEpochMilli();
    long expireAfterUtc = Instant.now().plusSeconds(60).toEpochMilli();

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(name, snapshotId, createdTimeUtc, expireAfterUtc);

    String serializedReplicaMetadata = serDe.toJsonStr(replicaMetadata);
    assertThat(serializedReplicaMetadata).isNotEmpty();

    ReplicaMetadata deserializedReplicaMetadata = serDe.fromJsonStr(serializedReplicaMetadata);
    assertThat(deserializedReplicaMetadata).isEqualTo(replicaMetadata);

    assertThat(deserializedReplicaMetadata.name).isEqualTo(name);
    assertThat(deserializedReplicaMetadata.snapshotId).isEqualTo(snapshotId);
    assertThat(deserializedReplicaMetadata.createdTimeEpochMs).isEqualTo(createdTimeUtc);
    assertThat(deserializedReplicaMetadata.expireAfterEpochMs).isEqualTo(expireAfterUtc);
  }

  @Test
  public void shouldHandleEmptyExpiration() throws InvalidProtocolBufferException {
    // ensure even though adding expiration field we can still deserialize existing replicas
    // this can likely be removed after this code has shipped to production
    String emptyExpiration =
        "{\n"
            + "  \"name\": \"name\",\n"
            + "  \"snapshotId\": \"snapshotId\",\n"
            + "  \"createdTimeEpochMs\": \"1639677020380\"\n"
            + "}";
    ReplicaMetadata deserializedReplicaMetadata = serDe.fromJsonStr(emptyExpiration);

    assertThat(deserializedReplicaMetadata.name).isEqualTo("name");
    assertThat(deserializedReplicaMetadata.snapshotId).isEqualTo("snapshotId");
    assertThat(deserializedReplicaMetadata.createdTimeEpochMs).isEqualTo(1639677020380L);
    assertThat(deserializedReplicaMetadata.expireAfterEpochMs).isEqualTo(0L);
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
