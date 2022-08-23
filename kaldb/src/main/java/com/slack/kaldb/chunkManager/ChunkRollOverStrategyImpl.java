package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.proto.config.KaldbConfigs;
import java.io.File;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;

/**
 * This class implements a rolls over a chunk when the chunk reaches a specific size or when a chunk
 * hits a max message limit.
 *
 * <p>TODO: Also consider rolling over a chunk based on the time range for messages in the chunk.
 */
public class ChunkRollOverStrategyImpl implements ChunkRollOverStrategy {

  public static ChunkRollOverStrategyImpl fromConfig(KaldbConfigs.IndexerConfig indexerConfig) {
    return new ChunkRollOverStrategyImpl(
        indexerConfig.getMaxBytesPerChunk(), indexerConfig.getMaxMessagesPerChunk());
  }

  private final long maxBytesPerChunk;
  private final long maxMessagesPerChunk;

  private long approximateDirectoryBytes = 0;
  private static final Duration directorySizeExecutorPeriod = Duration.of(10, ChronoUnit.SECONDS);
  private final ScheduledExecutorService directorySizeExecutorService =
      Executors.newScheduledThreadPool(
          0, new ThreadFactoryBuilder().setNameFormat("directory-size-%d").build());

  private volatile File activeChunkDirectory;

  public ChunkRollOverStrategyImpl(long maxBytesPerChunk, long maxMessagesPerChunk) {
    ensureTrue(maxBytesPerChunk > 0, "Max bytes per chunk should be a positive number.");
    ensureTrue(maxMessagesPerChunk > 0, "Max messages per chunk should be a positive number.");
    this.maxBytesPerChunk = maxBytesPerChunk;
    this.maxMessagesPerChunk = maxMessagesPerChunk;

    directorySizeExecutorService.scheduleAtFixedRate(
        () -> approximateDirectoryBytes = FileUtils.sizeOf(getActiveChunkDirectory()),
        directorySizeExecutorPeriod.getSeconds(),
        directorySizeExecutorPeriod.getSeconds(),
        TimeUnit.SECONDS);
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

  @Override
  public void setActiveChunkDirectory(File activeChunkDirectory) {
    this.activeChunkDirectory = activeChunkDirectory;
    approximateDirectoryBytes = 0;
  }

  public File getActiveChunkDirectory() {
    return activeChunkDirectory;
  }

  @Override
  public long getApproximateDirectoryBytes() {
    return approximateDirectoryBytes;
  }

  @Override
  public void close() {
    // Stop directory size calculations
    directorySizeExecutorService.shutdown();
  }
}
