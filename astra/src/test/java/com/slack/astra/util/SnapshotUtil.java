package com.slack.astra.util;

import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.proto.metadata.Metadata;

public class SnapshotUtil {
  public static SnapshotMetadata makeSnapshot(String name) {
    return new SnapshotMetadata(
        name + "snapshotId", "/testPath_" + name, 1, 100, 1, "1", Metadata.IndexType.LOGS_LUCENE9);
  }
}
