package com.slack.kaldb.chunkrollover;

import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a disk based rollover strategy that calculates disk size every 10 seconds The disk size
 * being calculated in the lucene index at that point in time. In addition, if we hit the max
 * messages limit we also rollover
 */
public class DiskOrMessageCountBasedRolloverStrategy implements ChunkRollOverStrategy {

  private static final Logger LOG =
      LoggerFactory.getLogger(DiskOrMessageCountBasedRolloverStrategy.class);

  private final MeterRegistry registry;

  public static final String LIVE_BYTES_DIR = "live_bytes_dir";
  private final AtomicLong liveBytesDirGauge;

  private final long maxBytesPerChunk;
  private final long maxMessagesPerChunk;

  private long approximateDirectoryBytes = 0;

  @VisibleForTesting public static int DIRECTORY_SIZE_EXECUTOR_PERIOD_MS = 1000 * 10;

  private final ScheduledExecutorService directorySizeExecutorService =
      Executors.newScheduledThreadPool(
          1, new ThreadFactoryBuilder().setNameFormat("directory-size-%d").build());

  private volatile File activeChunkDirectory;

  public static DiskOrMessageCountBasedRolloverStrategy fromConfig(
      MeterRegistry meterRegistry, KaldbConfigs.IndexerConfig indexerConfig) {
    return new DiskOrMessageCountBasedRolloverStrategy(
        meterRegistry, indexerConfig.getMaxBytesPerChunk(), indexerConfig.getMaxMessagesPerChunk());
  }

  public DiskOrMessageCountBasedRolloverStrategy(
      MeterRegistry registry, long maxBytesPerChunk, long maxMessagesPerChunk) {
    ensureTrue(maxBytesPerChunk > 0, "Max bytes per chunk should be a positive number.");
    ensureTrue(maxMessagesPerChunk > 0, "Max messages per chunk should be a positive number.");
    this.maxBytesPerChunk = maxBytesPerChunk;
    this.maxMessagesPerChunk = maxMessagesPerChunk;
    this.registry = registry;
    this.liveBytesDirGauge = this.registry.gauge(LIVE_BYTES_DIR, new AtomicLong(0));

    directorySizeExecutorService.scheduleAtFixedRate(
        this::calculateDirectorySize,
        DIRECTORY_SIZE_EXECUTOR_PERIOD_MS,
        DIRECTORY_SIZE_EXECUTOR_PERIOD_MS,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed) {
    liveBytesDirGauge.set(approximateDirectoryBytes);
    boolean shouldRollover =
        (approximateDirectoryBytes >= maxBytesPerChunk)
            || (currentMessagesIndexed >= maxMessagesPerChunk);
    if (shouldRollover) {
      LOG.info(
          "After {} messages and {} ingested bytes rolling over chunk of {} bytes",
          currentMessagesIndexed,
          currentBytesIndexed,
          approximateDirectoryBytes);
    }
    return shouldRollover;
  }

  public long getMaxBytesPerChunk() {
    return maxBytesPerChunk;
  }

  @Override
  public void setActiveChunkDirectory(File activeChunkDirectory) {
    this.activeChunkDirectory = activeChunkDirectory;
    approximateDirectoryBytes = 0;
  }

  public void calculateDirectorySize() {
    if (activeChunkDirectory != null && activeChunkDirectory.exists()) {
      approximateDirectoryBytes = FileUtils.sizeOf(activeChunkDirectory);
    }
  }

  @Override
  public void close() {
    // Stop directory size calculations
    directorySizeExecutorService.shutdown();
  }
}
