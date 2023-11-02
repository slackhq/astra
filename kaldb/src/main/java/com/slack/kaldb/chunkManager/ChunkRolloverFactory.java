package com.slack.kaldb.chunkManager;

import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.chunk.ReadWriteChunk;
import com.slack.kaldb.chunkrollover.ChunkRollOverStrategy;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * A chunk rollover factory creates a rollover chunk task.
 *
 * <p>TODO: Consider using partial functions in future.
 */
public class ChunkRolloverFactory {
  private final ChunkRollOverStrategy chunkRollOverStrategy;
  private final BlobFs blobFs;
  private final String s3Bucket;
  private final MeterRegistry meterRegistry;

  public ChunkRolloverFactory(
      ChunkRollOverStrategy chunkRollOverStrategy,
      BlobFs blobFs,
      String s3Bucket,
      MeterRegistry registry) {
    this.chunkRollOverStrategy = chunkRollOverStrategy;
    this.blobFs = blobFs;
    this.s3Bucket = s3Bucket;
    this.meterRegistry = registry;
  }

  public RollOverChunkTask getRollOverChunkTask(ReadWriteChunk chunk, String chunkId) {
    return new RollOverChunkTask<>(chunk, meterRegistry, blobFs, s3Bucket, chunkId);
  }

  public ChunkRollOverStrategy getChunkRollOverStrategy() {
    return chunkRollOverStrategy;
  }
}
