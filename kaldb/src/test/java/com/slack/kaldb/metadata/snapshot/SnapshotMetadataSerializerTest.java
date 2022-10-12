package com.slack.kaldb.metadata.snapshot;

import static com.slack.kaldb.proto.metadata.Metadata.IndexType.LOGS_LUCENE9;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;

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
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId, LOGS_LUCENE9);

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

  @Test(expected = IllegalArgumentException.class)
  public void serializeNullObject() throws InvalidProtocolBufferException {
    serDe.toJsonStr(null);
  }

  @Test(expected = InvalidProtocolBufferException.class)
  public void deserializeNullObject() throws InvalidProtocolBufferException {
    serDe.fromJsonStr(null);
  }

  @Test(expected = InvalidProtocolBufferException.class)
  public void deserializeEmptyObject() throws InvalidProtocolBufferException {
    serDe.fromJsonStr("");
  }

  @Test(expected = InvalidProtocolBufferException.class)
  public void deserializeTestString() throws InvalidProtocolBufferException {
    serDe.fromJsonStr("test");
  }
}
