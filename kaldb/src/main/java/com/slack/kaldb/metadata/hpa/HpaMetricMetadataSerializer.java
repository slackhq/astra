package com.slack.kaldb.metadata.hpa;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.proto.metadata.Metadata;

public class HpaMetricMetadataSerializer implements MetadataSerializer<HpaMetricMetadata> {
  private static HpaMetricMetadata fromAutoscalerMetadataProto(
      Metadata.HpaMetricMetadata autoscalerMetadata) {
    return new HpaMetricMetadata(
        autoscalerMetadata.getName(),
        autoscalerMetadata.getNodeRole(),
        autoscalerMetadata.getValue());
  }

  private static Metadata.HpaMetricMetadata toAutoscalerMetadataProto(
      HpaMetricMetadata hpaMetricMetadata) {

    return Metadata.HpaMetricMetadata.newBuilder()
        .setName(hpaMetricMetadata.name)
        .setNodeRole(hpaMetricMetadata.nodeRole)
        .setValue(hpaMetricMetadata.value)
        .build();
  }

  @Override
  public String toJsonStr(HpaMetricMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toAutoscalerMetadataProto(metadata));
  }

  @Override
  public HpaMetricMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.HpaMetricMetadata.Builder autoscalerMetadataBuilder =
        Metadata.HpaMetricMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, autoscalerMetadataBuilder);
    return fromAutoscalerMetadataProto(autoscalerMetadataBuilder.build());
  }
}
