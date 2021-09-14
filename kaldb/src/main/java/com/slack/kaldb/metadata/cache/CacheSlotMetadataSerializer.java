package com.slack.kaldb.metadata.cache;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.proto.metadata.Metadata;

public class CacheSlotMetadataSerializer implements MetadataSerializer<CacheSlotMetadata> {
  private static CacheSlotMetadata fromCacheSlotMetadataProto(
      Metadata.CacheSlotMetadata cacheSlotMetadataProto) {
    return new CacheSlotMetadata(
        cacheSlotMetadataProto.getName(),
        Metadata.CacheSlotState.valueOf(cacheSlotMetadataProto.getCacheSlotState().name()),
        cacheSlotMetadataProto.getReplicaId(),
        cacheSlotMetadataProto.getUpdatedTimeUtc());
  }

  private static Metadata.CacheSlotMetadata toCacheSlotMetadataProto(CacheSlotMetadata metadata) {
    return Metadata.CacheSlotMetadata.newBuilder()
        .setName(metadata.name)
        .setReplicaId(metadata.replicaId)
        .setCacheSlotState(metadata.cacheSlotState)
        .setUpdatedTimeUtc(metadata.updatedTimeUtc)
        .build();
  }

  @Override
  public String toJsonStr(CacheSlotMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toCacheSlotMetadataProto(metadata));
  }

  @Override
  public CacheSlotMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.CacheSlotMetadata.Builder cacheNodeMetadataBuilder =
        Metadata.CacheSlotMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, cacheNodeMetadataBuilder);
    return fromCacheSlotMetadataProto(cacheNodeMetadataBuilder.build());
  }
}
