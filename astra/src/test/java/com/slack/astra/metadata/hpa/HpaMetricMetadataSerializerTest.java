package com.slack.astra.metadata.hpa;

import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.proto.metadata.Metadata;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class HpaMetricMetadataSerializerTest {

  private final HpaMetricMetadataSerializer serDe = new HpaMetricMetadataSerializer();

  @Test
  void testHpaMetricMetadataSerializer() throws InvalidProtocolBufferException {
    String name = "name";
    Metadata.HpaMetricMetadata.NodeRole nodeRole = Metadata.HpaMetricMetadata.NodeRole.CACHE;
    Double value = 1.0;
    HpaMetricMetadata hpaMetricMetadata = new HpaMetricMetadata(name, nodeRole, value);

    String serializedHpaMetricMetadata = serDe.toJsonStr(hpaMetricMetadata);
    assertThat(serializedHpaMetricMetadata).isNotEmpty();

    HpaMetricMetadata deserializedHpaMetric = serDe.fromJsonStr(serializedHpaMetricMetadata);
    assertThat(deserializedHpaMetric).isEqualTo(hpaMetricMetadata);

    assertThat(deserializedHpaMetric.getName()).isEqualTo(name);
    assertThat(deserializedHpaMetric.getNodeRole()).isEqualTo(nodeRole);
    assertThat(deserializedHpaMetric.getValue()).isEqualTo(value);
  }

  @Test
  public void testInvalidSerializations() {
    Throwable serializeNull = catchThrowable(() -> serDe.toJsonStr(null));
    Assertions.assertThat(serializeNull).isInstanceOf(IllegalArgumentException.class);

    Throwable deserializeNull = catchThrowable(() -> serDe.fromJsonStr(null));
    Assertions.assertThat(deserializeNull).isInstanceOf(InvalidProtocolBufferException.class);

    Throwable deserializeEmpty = catchThrowable(() -> serDe.fromJsonStr(""));
    Assertions.assertThat(deserializeEmpty).isInstanceOf(InvalidProtocolBufferException.class);

    Throwable deserializeCorrupt = catchThrowable(() -> serDe.fromJsonStr("test"));
    Assertions.assertThat(deserializeCorrupt).isInstanceOf(InvalidProtocolBufferException.class);
  }
}
