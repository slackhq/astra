package com.slack.astra.chunkManager;

import com.slack.astra.blobfs.ChunkStore;
import com.slack.astra.chunk.ReadWriteChunk;
import com.slack.astra.chunkrollover.ChunkRollOverStrategy;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * A chunk rollover factory creates a rollover chunk task.
 *
 * <p>TODO: Consider using partial functions in future.
 */
public class ChunkRolloverFactory {
  private final ChunkRollOverStrategy chunkRollOverStrategy;
  private final ChunkStore chunkStore;
  private final MeterRegistry meterRegistry;

  public ChunkRolloverFactory(
      ChunkRollOverStrategy chunkRollOverStrategy, ChunkStore chunkStore, MeterRegistry registry) {
    this.chunkRollOverStrategy = chunkRollOverStrategy;
    this.chunkStore = chunkStore;
    this.meterRegistry = registry;
  }

  public RollOverChunkTask getRollOverChunkTask(ReadWriteChunk chunk) {
    return new RollOverChunkTask<>(chunk, meterRegistry, chunkStore);
  }

  public ChunkRollOverStrategy getChunkRollOverStrategy() {
    return chunkRollOverStrategy;
  }
}
