package com.slack.astra.metadata.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

public class CacheNodeMetadataSerializerTest {
  private final CacheNodeMetadataSerializer serDe = new CacheNodeMetadataSerializer();

  @Test
  public void testCacheNodeMetadataSerializer() throws InvalidProtocolBufferException {
    String id = "abcd";
    String hostname = "host";
    String replicaSet = "rep1";
    long nodeCapacityBytes = 4096;

    CacheNodeMetadata cacheNodeMetadata =
        new CacheNodeMetadata(id, hostname, nodeCapacityBytes, replicaSet);

    String serializedCacheNodeMetadata = serDe.toJsonStr(cacheNodeMetadata);
    assertThat(serializedCacheNodeMetadata).isNotEmpty();

    CacheNodeMetadata deserializedCacheNodeMetadata =
        serDe.fromJsonStr(serializedCacheNodeMetadata);
    assertThat(deserializedCacheNodeMetadata).isEqualTo(cacheNodeMetadata);

    assertThat(deserializedCacheNodeMetadata.id).isEqualTo(id);
    assertThat(deserializedCacheNodeMetadata.hostname).isEqualTo(hostname);
    assertThat(deserializedCacheNodeMetadata.nodeCapacityBytes).isEqualTo(nodeCapacityBytes);
    assertThat(deserializedCacheNodeMetadata.replicaSet).isEqualTo(replicaSet);
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
