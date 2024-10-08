package com.slack.astra.metadata.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.proto.metadata.Metadata;
import org.junit.jupiter.api.Test;

public class SnapshotMetadataSerializerTest {
  private final SnapshotMetadataSerializer serDe = new SnapshotMetadataSerializer();

  @Test
  public void testSnapshotMetadataSerializer() throws InvalidProtocolBufferException {
    final String name = "testSnapshotId";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";
    final long sizeInBytes = 0;

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(name, startTime, endTime, maxOffset, partitionId, sizeInBytes);

    String serializedSnapshot = serDe.toJsonStr(snapshotMetadata);
    assertThat(serializedSnapshot).isNotEmpty();

    SnapshotMetadata deserializedSnapshotMetadata = serDe.fromJsonStr(serializedSnapshot);
    assertThat(deserializedSnapshotMetadata).isEqualTo(snapshotMetadata);

    assertThat(deserializedSnapshotMetadata.name).isEqualTo(name);
    assertThat(deserializedSnapshotMetadata.snapshotId).isEqualTo(name);
    assertThat(deserializedSnapshotMetadata.startTimeEpochMs).isEqualTo(startTime);
    assertThat(deserializedSnapshotMetadata.endTimeEpochMs).isEqualTo(endTime);
    assertThat(deserializedSnapshotMetadata.maxOffset).isEqualTo(maxOffset);
    assertThat(deserializedSnapshotMetadata.partitionId).isEqualTo(partitionId);
    assertThat(deserializedSnapshotMetadata.sizeInBytesOnDisk).isEqualTo(sizeInBytes);
  }

  @Test
  public void testDeserializingWithoutSizeField() throws InvalidProtocolBufferException {
    final String name = "testSnapshotId";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    Metadata.SnapshotMetadata protoSnapshotMetadata =
        Metadata.SnapshotMetadata.newBuilder()
            .setName(name)
            .setSnapshotId(name)
            .setStartTimeEpochMs(startTime)
            .setEndTimeEpochMs(endTime)
            .setMaxOffset(maxOffset)
            .setPartitionId(partitionId)
            // leaving out the `size` field
            .build();

    SnapshotMetadata deserializedSnapshotMetadata =
        serDe.fromJsonStr(serDe.printer.print(protoSnapshotMetadata));

    // Assert size is 0
    assertThat(deserializedSnapshotMetadata.sizeInBytesOnDisk).isEqualTo(0);

    // Assert everything else is deserialized correctly
    assertThat(deserializedSnapshotMetadata.name).isEqualTo(name);
    assertThat(deserializedSnapshotMetadata.snapshotId).isEqualTo(name);
    assertThat(deserializedSnapshotMetadata.startTimeEpochMs).isEqualTo(startTime);
    assertThat(deserializedSnapshotMetadata.endTimeEpochMs).isEqualTo(endTime);
    assertThat(deserializedSnapshotMetadata.maxOffset).isEqualTo(maxOffset);
    assertThat(deserializedSnapshotMetadata.partitionId).isEqualTo(partitionId);
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
