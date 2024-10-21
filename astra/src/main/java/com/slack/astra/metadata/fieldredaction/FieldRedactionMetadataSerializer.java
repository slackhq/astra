package com.slack.astra.metadata.fieldredaction;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.metadata.core.MetadataSerializer;
import com.slack.astra.proto.metadata.Metadata;

public class FieldRedactionMetadataSerializer
    implements MetadataSerializer<FieldRedactionMetadata> {

  private static FieldRedactionMetadata fromRedactedFieldMetadataProto(
      Metadata.RedactedFieldMetadata redactedFieldMetadataProto) {
    return new FieldRedactionMetadata(
        redactedFieldMetadataProto.getName(),
        redactedFieldMetadataProto.getFieldName(),
        redactedFieldMetadataProto.getStartTimeEpochMs(),
        redactedFieldMetadataProto.getEndTimeEpochMs());
  }

  public static Metadata.RedactedFieldMetadata toRedactedFieldMetadataProto(
      FieldRedactionMetadata metadata) {
    return Metadata.RedactedFieldMetadata.newBuilder()
        .setName(metadata.name)
        .setFieldName(metadata.fieldName)
        .setStartTimeEpochMs(metadata.startTimeEpochMs)
        .setEndTimeEpochMs(metadata.endTimeEpochMs)
        .build();
  }

  @Override
  public String toJsonStr(FieldRedactionMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toRedactedFieldMetadataProto(metadata));
  }

  @Override
  public FieldRedactionMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.RedactedFieldMetadata.Builder redactedFieldMetadataBuilder =
        Metadata.RedactedFieldMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, redactedFieldMetadataBuilder);
    return fromRedactedFieldMetadataProto(redactedFieldMetadataBuilder.build());
  }
}
