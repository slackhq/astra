package com.slack.kaldb.metadata.schema;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.proto.metadata.Metadata;

public class LuceneFieldDefMetadataSerializer
    implements MetadataSerializer<LuceneFieldDefMetadata> {

  private static Metadata.LuceneFieldDefMetadata toLuceneFieldDefProto(
      LuceneFieldDefMetadata metadata) {
    return Metadata.LuceneFieldDefMetadata.newBuilder()
        .setName(metadata.name)
        .setType(metadata.fieldType.toString())
        .setIsStored(metadata.isStored)
        .setIsIndexed(metadata.isIndexed)
        .setIsAnalyzed(metadata.isAnalyzed)
        .setStoreNumericDocValue(metadata.storeNumericDocValue)
        .build();
  }

  private static LuceneFieldDefMetadata fromLuceneFieldDefMetadataProto(
      Metadata.LuceneFieldDefMetadata luceneFieldDefMetadataProto) {
    return new LuceneFieldDefMetadata(
        luceneFieldDefMetadataProto.getName(),
        luceneFieldDefMetadataProto.getType(),
        luceneFieldDefMetadataProto.getIsStored(),
        luceneFieldDefMetadataProto.isInitialized(),
        luceneFieldDefMetadataProto.getIsAnalyzed(),
        luceneFieldDefMetadataProto.getStoreNumericDocValue());
  }

  @Override
  public String toJsonStr(LuceneFieldDefMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toLuceneFieldDefProto(metadata));
  }

  @Override
  public LuceneFieldDefMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.LuceneFieldDefMetadata.Builder luceneFieldDefMetadataBuilder =
        Metadata.LuceneFieldDefMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, luceneFieldDefMetadataBuilder);
    return fromLuceneFieldDefMetadataProto(luceneFieldDefMetadataBuilder.build());
  }
}
