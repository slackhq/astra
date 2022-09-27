package com.slack.kaldb.metadata.replica;

import static com.slack.kaldb.proto.metadata.Metadata.IndexType.LUCENE_REGULAR;
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
    long createdTimeEpochMs = Instant.now().toEpochMilli();
    long expireAfterEpochMs = Instant.now().plusSeconds(60).toEpochMilli();

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            name, snapshotId, createdTimeEpochMs, expireAfterEpochMs, true, LUCENE_REGULAR);

    String serializedReplicaMetadata = serDe.toJsonStr(replicaMetadata);
    assertThat(serializedReplicaMetadata).isNotEmpty();

    ReplicaMetadata deserializedReplicaMetadata = serDe.fromJsonStr(serializedReplicaMetadata);
    assertThat(deserializedReplicaMetadata).isEqualTo(replicaMetadata);

    assertThat(deserializedReplicaMetadata.name).isEqualTo(name);
    assertThat(deserializedReplicaMetadata.snapshotId).isEqualTo(snapshotId);
    assertThat(deserializedReplicaMetadata.createdTimeEpochMs).isEqualTo(createdTimeEpochMs);
    assertThat(deserializedReplicaMetadata.expireAfterEpochMs).isEqualTo(expireAfterEpochMs);
    assertThat(deserializedReplicaMetadata.isRestored).isTrue();
    assertThat(deserializedReplicaMetadata.indexType).isEqualTo(LUCENE_REGULAR);
  }

  @Test
  public void shouldHandleEmptyExpirationAndRestore() throws InvalidProtocolBufferException {
    // ensure even though adding expiration field we can still deserialize existing replicas
    // this can likely be removed after this code has shipped to production
    String emptyExpirationAndRestore =
        "{\n"
            + "  \"name\": \"name\",\n"
            + "  \"snapshotId\": \"snapshotId\",\n"
            + "  \"createdTimeEpochMs\": \"1639677020380\"\n"
            + "}";
    ReplicaMetadata deserializedReplicaMetadata = serDe.fromJsonStr(emptyExpirationAndRestore);

    assertThat(deserializedReplicaMetadata.name).isEqualTo("name");
    assertThat(deserializedReplicaMetadata.snapshotId).isEqualTo("snapshotId");
    assertThat(deserializedReplicaMetadata.createdTimeEpochMs).isEqualTo(1639677020380L);
    assertThat(deserializedReplicaMetadata.expireAfterEpochMs).isEqualTo(0L);
    assertThat(deserializedReplicaMetadata.isRestored).isFalse();
    assertThat(deserializedReplicaMetadata.indexType).isEqualTo(LUCENE_REGULAR);
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
