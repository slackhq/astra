package com.slack.astra.metadata.redactedfield;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.metadata.core.MetadataSerializer;
import com.slack.astra.proto.metadata.Metadata;

public class RedactedFieldMetadataSerializer implements MetadataSerializer<RedactedFieldMetadata> {

    private static RedactedFieldMetadata fromRedactedFieldMetadataProto(
            Metadata.RedactedFieldMetadata redactedFieldMetadata) {
        return new RedactedFieldMetadata(
                redactedFieldMetadata.getName(),
                redactedFieldMetadata.getStartTimeEpochMs(),
                redactedFieldMetadata.getEndTimeEpochMs());
    }

    private static Metadata.RedactedFieldMetadata toRedactedFieldMetadataProto(
            RedactedFieldMetadata metadata) {
        return Metadata.RedactedFieldMetadata.newBuilder()
                .setName(metadata.fieldName)
                .setStartTimeEpochMs(metadata.startTimeEpochMs)
                .setEndTimeEpochMs(metadata.endTimeEpochMs)
                .build();
    }

    @Override
    public String toJsonStr(RedactedFieldMetadata metadata) throws InvalidProtocolBufferException {
        if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

        return printer.print(toRedactedFieldMetadataProto(metadata));
    }

    @Override
    public RedactedFieldMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
        Metadata.RedactedFieldMetadata.Builder redactedFieldMetadataBuilder = Metadata.RedactedFieldMetadata.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(data, redactedFieldMetadataBuilder);
        return fromRedactedFieldMetadataProto(redactedFieldMetadataBuilder.build());
    }
}
