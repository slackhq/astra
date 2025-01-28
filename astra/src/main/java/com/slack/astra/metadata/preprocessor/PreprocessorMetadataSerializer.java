package com.slack.astra.metadata.preprocessor;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.metadata.core.MetadataSerializer;
import com.slack.astra.proto.metadata.Metadata;

public class PreprocessorMetadataSerializer implements MetadataSerializer<PreprocessorMetadata> {
  private static PreprocessorMetadata fromPreprocessorMetadataProto(
      Metadata.PreprocessorMetadata preprocessorMetadataProto) {
    return new PreprocessorMetadata(preprocessorMetadataProto.getName());
  }

  private static Metadata.PreprocessorMetadata toPreprocessorMetadataProto(
      PreprocessorMetadata metadata) {
    return Metadata.PreprocessorMetadata.newBuilder().setName(metadata.name).build();
  }

  @Override
  public String toJsonStr(PreprocessorMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toPreprocessorMetadataProto(metadata));
  }

  @Override
  public PreprocessorMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.PreprocessorMetadata.Builder preProcessorBuilder =
        Metadata.PreprocessorMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, preProcessorBuilder);
    return fromPreprocessorMetadataProto(preProcessorBuilder.build());
  }
}
