package com.slack.kaldb.metadata.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Instant;
import org.junit.Test;

public class CacheSlotMetadataSerializerTest {
  private final CacheSlotMetadataSerializer serDe = new CacheSlotMetadataSerializer();

  @Test
  public void testCacheSlotMetadataSerializer() throws InvalidProtocolBufferException {
    String name = "name";
    Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState =
        Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED;
    String replicaId = "123";
    long updatedTimeUtc = Instant.now().toEpochMilli();

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(name, cacheSlotState, replicaId, updatedTimeUtc);

    String serializedCacheSlotMetadata = serDe.toJsonStr(cacheSlotMetadata);
    assertThat(serializedCacheSlotMetadata).isNotEmpty();

    CacheSlotMetadata deserializedCacheSlotMetadata =
        serDe.fromJsonStr(serializedCacheSlotMetadata);
    assertThat(deserializedCacheSlotMetadata).isEqualTo(cacheSlotMetadata);

    assertThat(deserializedCacheSlotMetadata.name).isEqualTo(name);
    assertThat(deserializedCacheSlotMetadata.cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(deserializedCacheSlotMetadata.replicaId).isEqualTo(replicaId);
    assertThat(deserializedCacheSlotMetadata.updatedTimeEpochMs).isEqualTo(updatedTimeUtc);
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
