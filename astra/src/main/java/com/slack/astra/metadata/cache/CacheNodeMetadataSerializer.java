package com.slack.astra.metadata.cache;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.metadata.core.MetadataSerializer;
import com.slack.astra.proto.metadata.Metadata;

public class CacheNodeMetadataSerializer implements MetadataSerializer<CacheNodeMetadata> {
  private static CacheNodeMetadata fromCacheNodeMetadataProto(
      Metadata.CacheNodeMetadata cacheNodeMetadataProto) {
    return new CacheNodeMetadata(
        cacheNodeMetadataProto.getId(),
        cacheNodeMetadataProto.getHostname(),
        cacheNodeMetadataProto.getNodeCapacityBytes(),
        cacheNodeMetadataProto.getReplicaSet());
  }

  private static Metadata.CacheNodeMetadata toCacheNodeMetadataProto(CacheNodeMetadata metadata) {
    return Metadata.CacheNodeMetadata.newBuilder()
        .setName(metadata.name)
        .setId(metadata.id)
        .setHostname(metadata.hostname)
        .setReplicaSet(metadata.replicaSet)
        .setNodeCapacityBytes(metadata.nodeCapacityBytes)
        .build();
  }

  @Override
  public String toJsonStr(CacheNodeMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toCacheNodeMetadataProto(metadata));
  }

  @Override
  public CacheNodeMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.CacheNodeMetadata.Builder cacheNodeMetadataBuilder =
        Metadata.CacheNodeMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, cacheNodeMetadataBuilder);
    return fromCacheNodeMetadataProto(cacheNodeMetadataBuilder.build());
  }
}
