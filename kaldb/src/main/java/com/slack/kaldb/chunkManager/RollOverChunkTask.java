package com.slack.kaldb.chunkManager;

import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.ReadWriteChunkImpl;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollOverChunkTask<T> implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(RollOverChunkTask.class);

  public static final String ROLLOVERS_COMPLETED = "rollovers_completed";
  public static final String ROLLOVERS_FAILED = "rollovers_failed";
  public static final String ROLLOVERS_INITIATED = "rollovers_initiated";

  private final Counter rolloversInitiatedCounter;
  private final Counter rolloversCompletedCounter;
  private final Counter rolloversFailedCounter;

  private final ReadWriteChunkImpl<T> chunk;
  private final String s3Bucket;
  private final String s3BucketPrefix;
  private final S3BlobFs s3BlobFs;

  public RollOverChunkTask(
      ReadWriteChunkImpl<T> chunk,
      MeterRegistry meterRegistry,
      S3BlobFs s3BlobFs,
      String s3Bucket,
      String s3BucketPrefix) {
    this.chunk = chunk;
    this.s3BlobFs = s3BlobFs;
    this.s3Bucket = s3Bucket;
    this.s3BucketPrefix = s3BucketPrefix;
    rolloversInitiatedCounter = meterRegistry.counter(ROLLOVERS_INITIATED);
    rolloversCompletedCounter = meterRegistry.counter(ROLLOVERS_COMPLETED);
    rolloversFailedCounter = meterRegistry.counter(ROLLOVERS_FAILED);
  }

  @Override
  public Boolean call() {
    LOG.info("Start chunk roll over {}", chunk.info());
    rolloversInitiatedCounter.increment();
    // Take the given chunk.
    chunk.preSnapshot();

    // Upload it to S3.
    if (chunk.snapshotToS3(s3Bucket, s3BucketPrefix, s3BlobFs)) {
      // Post snapshot management.
      chunk.postSnapshot();
      rolloversCompletedCounter.increment();
      chunk.info().setChunkSnapshotTimeEpochMs(Instant.now().toEpochMilli());
      LOG.info("Finished chunk roll over {}", chunk.info());
      return true;
    } else {
      rolloversFailedCounter.increment();
      LOG.info("Failed chunk roll over {}", chunk.info());
      return false;
    }
  }
}
