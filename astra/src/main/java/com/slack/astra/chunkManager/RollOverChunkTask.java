package com.slack.astra.chunkManager;

import com.slack.astra.blobfs.BlobFs;
import com.slack.astra.chunk.ReadWriteChunk;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class performs the roll over of a chunk. During a rollover, we run the preSnapshot, snapshot
 * and postOperations operations in that order on the chunk.
 *
 * <p>In case of failures, an error is logged and a failure counter is incremented.
 */
public class RollOverChunkTask<T> implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(RollOverChunkTask.class);

  public static final String ROLLOVERS_COMPLETED = "rollovers_completed";
  public static final String ROLLOVERS_FAILED = "rollovers_failed";
  public static final String ROLLOVERS_INITIATED = "rollovers_initiated";
  public static final String ROLLOVER_TIMER = "rollover_timer";

  private final Counter rolloversInitiatedCounter;
  private final Counter rolloversCompletedCounter;
  private final Counter rolloversFailedCounter;
  private final Timer rollOverTimer;

  private final ReadWriteChunk<T> chunk;
  private final String s3Bucket;
  private final String s3BucketPrefix;
  private final BlobFs blobFs;

  public RollOverChunkTask(
      ReadWriteChunk<T> chunk,
      MeterRegistry meterRegistry,
      BlobFs blobFs,
      String s3Bucket,
      String s3BucketPrefix) {
    this.chunk = chunk;
    this.blobFs = blobFs;
    this.s3Bucket = s3Bucket;
    this.s3BucketPrefix = s3BucketPrefix;
    rolloversInitiatedCounter = meterRegistry.counter(ROLLOVERS_INITIATED);
    rolloversCompletedCounter = meterRegistry.counter(ROLLOVERS_COMPLETED);
    rolloversFailedCounter = meterRegistry.counter(ROLLOVERS_FAILED);
    rollOverTimer = meterRegistry.timer(ROLLOVER_TIMER);
  }

  @Override
  public Boolean call() throws Exception {
    return rollOverTimer.recordCallable(this::doRollover);
  }

  private Boolean doRollover() {
    try {
      LOG.debug("Start chunk roll over {}", chunk.info());
      rolloversInitiatedCounter.increment();
      // Run pre-snapshot and upload chunk to blob store.
      chunk.preSnapshot();
      if (!chunk.snapshotToS3(s3Bucket, s3BucketPrefix, blobFs)) {
        LOG.warn("Failed to snapshot the chunk to S3");
        rolloversFailedCounter.increment();
        return false;
      }
      // Post snapshot management.
      chunk.postSnapshot();
      rolloversCompletedCounter.increment();
      chunk.info().setChunkSnapshotTimeEpochMs(Instant.now().toEpochMilli());
      LOG.debug("Finished chunk roll over {}", chunk.info());
      return true;
    } catch (RuntimeException e) {
      rolloversFailedCounter.increment();
      LOG.error("Failed chunk roll over {}", chunk.info(), e);
    }
    return false;
  }
}
