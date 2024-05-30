package com.slack.astra.metadata.snapshot;

import static com.slack.astra.proto.metadata.Metadata.IndexType.LOGS_LUCENE9;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

public class SnapshotMetadataSerializerTest {
  private final SnapshotMetadataSerializer serDe = new SnapshotMetadataSerializer();

  @Test
  public void testSnapshotMetadataSerializer() throws InvalidProtocolBufferException {
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            name, path, startTime, endTime, maxOffset, partitionId, LOGS_LUCENE9, 0);

    String serializedSnapshot = serDe.toJsonStr(snapshotMetadata);
    assertThat(serializedSnapshot).isNotEmpty();

    SnapshotMetadata deserializedSnapshotMetadata = serDe.fromJsonStr(serializedSnapshot);
    assertThat(deserializedSnapshotMetadata).isEqualTo(snapshotMetadata);

    assertThat(deserializedSnapshotMetadata.name).isEqualTo(name);
    assertThat(deserializedSnapshotMetadata.snapshotPath).isEqualTo(path);
    assertThat(deserializedSnapshotMetadata.snapshotId).isEqualTo(name);
    assertThat(deserializedSnapshotMetadata.startTimeEpochMs).isEqualTo(startTime);
    assertThat(deserializedSnapshotMetadata.endTimeEpochMs).isEqualTo(endTime);
    assertThat(deserializedSnapshotMetadata.maxOffset).isEqualTo(maxOffset);
    assertThat(deserializedSnapshotMetadata.partitionId).isEqualTo(partitionId);
    assertThat(deserializedSnapshotMetadata.indexType).isEqualTo(LOGS_LUCENE9);
  }

  @Test
  public void serializeNullObject() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> serDe.toJsonStr(null));
  }

  @Test
  public void deserializeNullObject() {
    assertThatExceptionOfType(InvalidProtocolBufferException.class)
        .isThrownBy(() -> serDe.fromJsonStr(null));
  }

  @Test
  public void deserializeEmptyObject() {
    assertThatExceptionOfType(InvalidProtocolBufferException.class)
        .isThrownBy(() -> serDe.fromJsonStr(""));
  }

  @Test
  public void deserializeTestString() {
    assertThatExceptionOfType(InvalidProtocolBufferException.class)
        .isThrownBy(() -> serDe.fromJsonStr("test"));
  }
}
