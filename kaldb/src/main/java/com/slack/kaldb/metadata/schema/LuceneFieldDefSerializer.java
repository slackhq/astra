package com.slack.kaldb.metadata.schema;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.proto.metadata.Metadata;

public class LuceneFieldDefSerializer implements MetadataSerializer<LuceneFieldDef> {

  public static Metadata.LuceneFieldDef toLuceneFieldDefProto(LuceneFieldDef fieldDef) {
    return Metadata.LuceneFieldDef.newBuilder()
        .setName(fieldDef.name)
        .setType(fieldDef.fieldType.toString())
        .setIsStored(fieldDef.isStored)
        .setIsIndexed(fieldDef.isIndexed)
        .setIsAnalyzed(fieldDef.isAnalyzed)
        .setStoreDocValue(fieldDef.storeDocValue)
        .build();
  }

  public static LuceneFieldDef fromLuceneFieldDefProto(
      Metadata.LuceneFieldDef luceneFieldDefProto) {
    return new LuceneFieldDef(
        luceneFieldDefProto.getName(),
        luceneFieldDefProto.getType(),
        luceneFieldDefProto.getIsStored(),
        luceneFieldDefProto.isInitialized(),
        luceneFieldDefProto.getIsAnalyzed(),
        luceneFieldDefProto.getStoreDocValue());
  }

  @Override
  public String toJsonStr(LuceneFieldDef luceneFieldDef) throws InvalidProtocolBufferException {
    if (luceneFieldDef == null)
      throw new IllegalArgumentException("luceneFieldDef object can't be null");

    return printer.print(toLuceneFieldDefProto(luceneFieldDef));
  }

  @Override
  public LuceneFieldDef fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.LuceneFieldDef.Builder luceneFieldDefBuilder = Metadata.LuceneFieldDef.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, luceneFieldDefBuilder);
    return fromLuceneFieldDefProto(luceneFieldDefBuilder.build());
  }
}
