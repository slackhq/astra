package com.slack.kaldb.util;

import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.proto.metadata.Metadata;

public class SnapshotUtil {
  public static SnapshotMetadata makeSnapshot(String name) {
    return new SnapshotMetadata(
        name + "snapshotId",
        "/testPath_" + name,
        1,
        100,
        1,
        "1",
        Metadata.IndexType.LUCENE_REGULAR);
  }
}
