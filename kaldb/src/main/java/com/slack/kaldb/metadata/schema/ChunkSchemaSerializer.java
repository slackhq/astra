package com.slack.kaldb.metadata.schema;

import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.proto.metadata.Metadata;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ChunkSchemaSerializer implements MetadataSerializer<ChunkSchema> {
  private static Metadata.ChunkSchema toChunkSchemaProto(ChunkSchema chunkSchema) {
    return Metadata.ChunkSchema.newBuilder()
        .setName(chunkSchema.name)
        .addAllFieldDefs(
            chunkSchema
                .fieldDefMap
                .values()
                .stream()
                .map(f -> LuceneFieldDefSerializer.toLuceneFieldDefProto(f))
                .collect(Collectors.toList()))
        .putAllMetadata(chunkSchema.metadata)
        .build();
  }

  public static ChunkSchema fromChunkSchemaProto(Metadata.ChunkSchema chunkSchemaProto) {
    Map<String, LuceneFieldDef> fieldDefMap = new HashMap<>(chunkSchemaProto.getFieldDefsCount());
    for(field : chunkSchemaProto.getFieldDefsList()) {
      if(fieldDefMap.containsKey(field.g))
    }
    return new ChunkSchema(
        chunkSchemaProto.getName(),
        Maps.uniqueIndex(chunkSchemaProto.getFieldDefsList(), LuceneFieldDef::getName),
        chunkSchemaProto.getMetadataMap());
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
