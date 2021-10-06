package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

import com.slack.kaldb.config.KaldbConfig;

/**
 * This class implements a rolls over a chunk when the chunk reaches a specific size or when a chunk
 * hits a max message limit.
 *
 * <p>TODO: Also consider rolling over a chunk based on the time range for messages in the chunk.
 */
public class ChunkRollOverStrategyImpl implements ChunkRollOverStrategy {

  public static ChunkRollOverStrategyImpl fromConfig() {
    return new ChunkRollOverStrategyImpl(
        KaldbConfig.get().getIndexerConfig().getMaxBytesPerChunk(),
        KaldbConfig.get().getIndexerConfig().getMaxMessagesPerChunk());
  }

  private final long maxBytesPerChunk;
  private final long maxMessagesPerChunk;

  public ChunkRollOverStrategyImpl(long maxBytesPerChunk, long maxMessagesPerChunk) {
    ensureTrue(maxBytesPerChunk > 0, "Max bytes per chunk should be a positive number.");
    ensureTrue(maxMessagesPerChunk > 0, "Max messages per chunk should be a positive number.");
    this.maxBytesPerChunk = maxBytesPerChunk;
    this.maxMessagesPerChunk = maxMessagesPerChunk;
  }

  @Override
  public boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed) {
    return (currentBytesIndexed >= maxBytesPerChunk)
        || (currentMessagesIndexed >= maxMessagesPerChunk);
  }

  public long getMaxBytesPerChunk() {
    return maxBytesPerChunk;
  }

  public long getMaxMessagesPerChunk() {
    return maxMessagesPerChunk;
  }
}
