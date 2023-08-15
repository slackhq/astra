package com.slack.kaldb.metadata.hpa;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.slack.kaldb.proto.metadata.Metadata;
import org.junit.jupiter.api.Test;

class HpaMetricMetadataTest {

  @Test
  void testHpaMetadata() {
    String name = "name";
    Metadata.HpaMetricMetadata.NodeRole nodeRole = Metadata.HpaMetricMetadata.NodeRole.CACHE;
    Double value = 1.0;
    HpaMetricMetadata hpaMetricMetadata = new HpaMetricMetadata(name, nodeRole, value);

    assertThat(hpaMetricMetadata.getName()).isEqualTo(name);
    assertThat(hpaMetricMetadata.getNodeRole()).isEqualTo(nodeRole);
    assertThat(hpaMetricMetadata.getValue()).isEqualTo(value);
  }

  @Test
  void testEqualsHashcode() {
    String name = "name";
    Metadata.HpaMetricMetadata.NodeRole nodeRole = Metadata.HpaMetricMetadata.NodeRole.CACHE;
    Double value = 1.0;
    HpaMetricMetadata hpaMetricMetadata1 = new HpaMetricMetadata(name, nodeRole, value);
    HpaMetricMetadata hpaMetricMetadata2 = new HpaMetricMetadata(name, nodeRole, value);

    HpaMetricMetadata hpaMetricMetadataDiff1 = new HpaMetricMetadata("name2", nodeRole, value);
    HpaMetricMetadata hpaMetricMetadataDiff2 =
        new HpaMetricMetadata(name, Metadata.HpaMetricMetadata.NodeRole.INDEX, value);
    HpaMetricMetadata hpaMetricMetadataDiff3 = new HpaMetricMetadata(name, nodeRole, 1.1);

    assertThat(hpaMetricMetadata1).isEqualTo(hpaMetricMetadata2);
    assertThat(hpaMetricMetadata1.hashCode()).isEqualTo(hpaMetricMetadata2.hashCode());

    assertThat(hpaMetricMetadata1).isNotEqualTo(hpaMetricMetadataDiff1);
    assertThat(hpaMetricMetadata1.hashCode()).isNotEqualTo(hpaMetricMetadataDiff1.hashCode());
    assertThat(hpaMetricMetadata1).isNotEqualTo(hpaMetricMetadataDiff2);
    assertThat(hpaMetricMetadata1.hashCode()).isNotEqualTo(hpaMetricMetadataDiff2.hashCode());
    assertThat(hpaMetricMetadata1).isNotEqualTo(hpaMetricMetadataDiff3);
    assertThat(hpaMetricMetadata1.hashCode()).isNotEqualTo(hpaMetricMetadataDiff3.hashCode());
  }
}
