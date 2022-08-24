package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a rolls over a chunk when the chunk reaches a specific size or when a chunk
 * hits a max message limit.
 *
 * <p>TODO: Also consider rolling over a chunk based on the time range for messages in the chunk.
 */
public class SizeOrMessageBasedRolloverStrategy implements ChunkRollOverStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(DiskBasedRolloverStrategy.class);

  private final MeterRegistry registry;

  public static SizeOrMessageBasedRolloverStrategy fromConfig(
      MeterRegistry registry, KaldbConfigs.IndexerConfig indexerConfig) {
    return new SizeOrMessageBasedRolloverStrategy(
        registry, indexerConfig.getMaxBytesPerChunk(), indexerConfig.getMaxMessagesPerChunk());
  }

  private final long maxBytesPerChunk;
  private final long maxMessagesPerChunk;

  public SizeOrMessageBasedRolloverStrategy(
      MeterRegistry registry, long maxBytesPerChunk, long maxMessagesPerChunk) {
    ensureTrue(maxBytesPerChunk > 0, "Max bytes per chunk should be a positive number.");
    ensureTrue(maxMessagesPerChunk > 0, "Max messages per chunk should be a positive number.");
    this.maxBytesPerChunk = maxBytesPerChunk;
    this.maxMessagesPerChunk = maxMessagesPerChunk;
    this.registry = registry;
  }

  @Override
  public boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed) {
    boolean shouldRollover =
        (currentBytesIndexed >= maxBytesPerChunk)
            || (currentMessagesIndexed >= maxMessagesPerChunk);
    LOG.info(
        "After {} messages and {} ingested bytes rolling over chunk",
        currentMessagesIndexed,
        currentBytesIndexed);
    return shouldRollover;
  }

  @Override
  public void setActiveChunkDirectory(File activeChunkDirectory) {}

  @Override
  public void close() {}

  public long getMaxBytesPerChunk() {
    return maxBytesPerChunk;
  }

  public long getMaxMessagesPerChunk() {
    return maxMessagesPerChunk;
  }
}
