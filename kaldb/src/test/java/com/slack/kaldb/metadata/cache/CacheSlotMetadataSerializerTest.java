package com.slack.kaldb.metadata.cache;

import static com.slack.kaldb.proto.metadata.Metadata.IndexType.LOGS_LUCENE9;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

public class CacheSlotMetadataSerializerTest {
  private final CacheSlotMetadataSerializer serDe = new CacheSlotMetadataSerializer();

  @Test
  public void testCacheSlotMetadataSerializer() throws InvalidProtocolBufferException {
    String name = "name";
    String hostname = "hostname";
    String replicaSet = "rep1";
    Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState =
        Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED;
    String replicaId = "123";
    long updatedTimeEpochMs = Instant.now().toEpochMilli();
    List<Metadata.IndexType> supportedIndexTypes = List.of(LOGS_LUCENE9, LOGS_LUCENE9);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            name,
            cacheSlotState,
            replicaId,
            updatedTimeEpochMs,
            supportedIndexTypes,
            hostname,
            replicaSet);

    String serializedCacheSlotMetadata = serDe.toJsonStr(cacheSlotMetadata);
    assertThat(serializedCacheSlotMetadata).isNotEmpty();

    CacheSlotMetadata deserializedCacheSlotMetadata =
        serDe.fromJsonStr(serializedCacheSlotMetadata);
    assertThat(deserializedCacheSlotMetadata).isEqualTo(cacheSlotMetadata);

    assertThat(deserializedCacheSlotMetadata.name).isEqualTo(name);
    assertThat(deserializedCacheSlotMetadata.hostname).isEqualTo(hostname);
    assertThat(deserializedCacheSlotMetadata.replicaSet).isEqualTo(replicaSet);
    assertThat(deserializedCacheSlotMetadata.cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(deserializedCacheSlotMetadata.replicaId).isEqualTo(replicaId);
    assertThat(deserializedCacheSlotMetadata.updatedTimeEpochMs).isEqualTo(updatedTimeEpochMs);
    assertThat(deserializedCacheSlotMetadata.supportedIndexTypes)
        .containsExactlyInAnyOrderElementsOf(supportedIndexTypes);
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
