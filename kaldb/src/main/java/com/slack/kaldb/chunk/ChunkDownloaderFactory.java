package com.slack.kaldb.chunk;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.slack.kaldb.blobfs.BlobFs;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;

/**
 * ChunkDownloaderFactory creates ChunkDownloaders based on the config. This also simplifies code by
 * keeping chunk downloader related config in one place.
 */
public class ChunkDownloaderFactory {
  private final String s3Bucket;
  private final BlobFs blobFs;
  private final ExecutorService executor;

  public ChunkDownloaderFactory(
      String s3Bucket, @Nonnull BlobFs blobFs, int maxParallelCacheSlotDownloads) {
    checkNotNull(blobFs, "blobFs can't be null");
    checkState(
        maxParallelCacheSlotDownloads > 0,
        "maxParallelCacheSlotDownloads should be a non-zero value.");
    this.s3Bucket = s3Bucket;
    this.blobFs = blobFs;
    this.executor = Executors.newFixedThreadPool(maxParallelCacheSlotDownloads);
  }

  public ChunkDownloader makeChunkDownloader(String snapshotId, Path localDataDirectory) {
    ChunkDownloader serialDownloader =
        new SerialS3ChunkDownloaderImpl(s3Bucket, snapshotId, blobFs, localDataDirectory);
    return new RateLimitedChunkDownloaderImpl(serialDownloader, executor);
  }
}
