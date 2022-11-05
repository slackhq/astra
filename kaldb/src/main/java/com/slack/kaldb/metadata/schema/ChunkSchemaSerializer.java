package com.slack.kaldb.metadata.schema;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.proto.metadata.Metadata;
import java.util.HashMap;
import java.util.Map;

public class ChunkSchemaSerializer implements MetadataSerializer<ChunkSchema> {
  private static Metadata.ChunkSchema toChunkSchemaProto(ChunkSchema chunkSchema) {
    final Map<String, Metadata.LuceneFieldDef> fieldDefProtoMap =
        new HashMap<>(chunkSchema.fieldDefMap.size());
    for (String key : chunkSchema.fieldDefMap.keySet()) {
      fieldDefProtoMap.put(
          key, LuceneFieldDefSerializer.toLuceneFieldDefProto(chunkSchema.fieldDefMap.get(key)));
    }
    return Metadata.ChunkSchema.newBuilder()
        .setName(chunkSchema.name)
        .putAllFieldDefMap(fieldDefProtoMap)
        .putAllMetadata(chunkSchema.metadata)
        .build();
  }

  public static ChunkSchema fromChunkSchemaProto(Metadata.ChunkSchema chunkSchemaProto) {
    final Map<String, LuceneFieldDef> fieldDefMap =
        new HashMap<>(chunkSchemaProto.getFieldDefMapCount());
    for (String key : chunkSchemaProto.getFieldDefMapMap().keySet()) {
      fieldDefMap.put(
          key,
          LuceneFieldDefSerializer.fromLuceneFieldDefProto(
              chunkSchemaProto.getFieldDefMapMap().get(key)));
    }
    return new ChunkSchema(
        chunkSchemaProto.getName(), fieldDefMap, chunkSchemaProto.getMetadataMap());
  }

  @Override
  public String toJsonStr(ChunkSchema chunkSchema) throws InvalidProtocolBufferException {
    if (chunkSchema == null)
      throw new IllegalArgumentException("luceneFieldDef object can't be null");

    return printer.print(toChunkSchemaProto(chunkSchema));
  }

  @Override
  public ChunkSchema fromJsonStr(String chunkSchemaStr) throws InvalidProtocolBufferException {
    Metadata.ChunkSchema.Builder chunkSchemaBuilder = Metadata.ChunkSchema.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(chunkSchemaStr, chunkSchemaBuilder);
    return fromChunkSchemaProto(chunkSchemaBuilder.build());
  }
}
