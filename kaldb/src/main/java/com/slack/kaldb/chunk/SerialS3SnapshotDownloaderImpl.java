package com.slack.kaldb.chunk;

import static com.slack.kaldb.logstore.BlobFsUtils.copyFromS3;

import com.slack.kaldb.blobfs.BlobFs;
import java.nio.file.Path;

public class SerialS3SnapshotDownloaderImpl implements SnapshotDownloader {
  private final String s3Bucket;
  private final String snapshotId;
  private BlobFs blobFs;
  private Path dataDirectory;

  public SerialS3SnapshotDownloaderImpl(
      String s3Bucket, String snapshotId, BlobFs blobFs, Path localDataDirectory) {
    this.s3Bucket = s3Bucket;
    this.snapshotId = snapshotId;
    this.blobFs = blobFs;
    this.dataDirectory = localDataDirectory;
  }

  @Override
  public boolean download() throws Exception {
    return copyFromS3(s3Bucket, snapshotId, blobFs, dataDirectory).length == 0;
  }
}
