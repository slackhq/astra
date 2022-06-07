package com.slack.kaldb.chunk;

import com.slack.kaldb.blobfs.BlobFs;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ChunkDownloaderFactory creates ChunkDownloaders based on the config. This also simplifies code by
 * keeping chunk downloader related config in one place.
 */
public class ChunkDownloaderFactory {
  private final String s3Bucket;
  private final BlobFs blobFs;
  private final ExecutorService executor;

  public ChunkDownloaderFactory(String s3Bucket, BlobFs blobFs, int maxParallelDownloads) {
    this.s3Bucket = s3Bucket;
    this.blobFs = blobFs;
    this.executor = Executors.newFixedThreadPool(maxParallelDownloads);
  }

  public ChunkDownloader makeChunkDownloader(String snapshotId, Path localDataDirectory) {
    ChunkDownloader serialDownloader =
        new SerialS3ChunkDownloaderImpl(s3Bucket, snapshotId, blobFs, localDataDirectory);
    return new RateLimitedChunkDownloaderImpl(serialDownloader, executor);
  }
}
