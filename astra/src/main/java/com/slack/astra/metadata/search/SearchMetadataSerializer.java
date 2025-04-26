package com.slack.astra.metadata.search;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.metadata.core.MetadataSerializer;
import com.slack.astra.proto.metadata.Metadata;

public class SearchMetadataSerializer implements MetadataSerializer<SearchMetadata> {
  private static SearchMetadata fromSearchMetadataProto(
      Metadata.SearchMetadata searchMetadataProto) {
    return new SearchMetadata(
        searchMetadataProto.getName(),
        searchMetadataProto.getSnapshotName(),
        searchMetadataProto.getUrl(),
        searchMetadataProto.getSearchable());
  }

  private static Metadata.SearchMetadata toSearchMetadataProto(SearchMetadata metadata) {
    return Metadata.SearchMetadata.newBuilder()
        .setName(metadata.name)
        .setSnapshotName(metadata.snapshotName)
        .setUrl(metadata.url)
        .setSearchable(metadata.getSearchable())
        .build();
  }

  @Override
  public String toJsonStr(SearchMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toSearchMetadataProto(metadata));
  }

  @Override
  public SearchMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.SearchMetadata.Builder searchMetadataBuilder = Metadata.SearchMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, searchMetadataBuilder);
    return fromSearchMetadataProto(searchMetadataBuilder.build());
  }
}
