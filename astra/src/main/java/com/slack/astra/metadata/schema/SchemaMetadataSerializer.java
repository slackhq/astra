package com.slack.astra.metadata.schema;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.metadata.core.MetadataSerializer;
import com.slack.astra.proto.metadata.Metadata;

public class SchemaMetadataSerializer implements MetadataSerializer<SchemaMetadata> {

  private static SchemaMetadata fromSchemaMetadataProto(
      Metadata.SchemaMetadata schemaMetadataProto) {
    return new SchemaMetadata(
        schemaMetadataProto.getName(),
        schemaMetadataProto.getSchema(),
        schemaMetadataProto.getSchemaMode());
  }

  public static Metadata.SchemaMetadata toSchemaMetadataProto(SchemaMetadata metadata) {
    return Metadata.SchemaMetadata.newBuilder()
        .setName(metadata.name)
        .setSchema(metadata.schema)
        .setSchemaMode(metadata.schemaMode)
        .build();
  }

  @Override
  public String toJsonStr(SchemaMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toSchemaMetadataProto(metadata));
  }

  @Override
  public SchemaMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.SchemaMetadata.Builder schemaMetadataBuilder = Metadata.SchemaMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, schemaMetadataBuilder);
    return fromSchemaMetadataProto(schemaMetadataBuilder.build());
  }
}
