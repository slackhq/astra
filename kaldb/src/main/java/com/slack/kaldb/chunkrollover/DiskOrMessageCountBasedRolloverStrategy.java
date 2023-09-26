package com.slack.kaldb.chunkrollover;

import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.FSDirectory;
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

  @VisibleForTesting public static int DIRECTORY_SIZE_EXECUTOR_PERIOD_MS = 1000 * 10;

  private final ScheduledExecutorService directorySizeExecutorService =
      Executors.newScheduledThreadPool(
          1, new ThreadFactoryBuilder().setNameFormat("directory-size-%d").build());

  private final AtomicReference<FSDirectory> activeChunkDirectory = new AtomicReference<>();
  private final AtomicLong approximateDirectoryBytes = new AtomicLong(0);

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
        () -> {
          long dirSize = calculateDirectorySize(activeChunkDirectory);
          // in case the method fails to calculate we return -1 so don't update the old value
          if (dirSize > 0) {
            approximateDirectoryBytes.set(dirSize);
          }
        },
        DIRECTORY_SIZE_EXECUTOR_PERIOD_MS,
        DIRECTORY_SIZE_EXECUTOR_PERIOD_MS,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed) {
    liveBytesDirGauge.set(approximateDirectoryBytes.get());
    boolean shouldRollover =
        (approximateDirectoryBytes.get() >= maxBytesPerChunk)
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
  public void setActiveChunkDirectory(FSDirectory directory) {
    this.activeChunkDirectory.set(directory);
    approximateDirectoryBytes.set(0);
  }

  public static long calculateDirectorySize(AtomicReference<FSDirectory> activeChunkDirectoryRef) {
    FSDirectory activeChunkDirectory = activeChunkDirectoryRef.get();
    return calculateDirectorySize(activeChunkDirectory);
  }

  public static long calculateDirectorySize(FSDirectory activeChunkDirectory) {
    try {
      if (activeChunkDirectory != null && activeChunkDirectory.listAll().length > 0) {
        return SegmentInfos.readLatestCommit(activeChunkDirectory).asList().stream()
            .mapToLong(
                segmentCommitInfo -> {
                  try {
                    return segmentCommitInfo.sizeInBytes();
                  } catch (IOException e) {
                    return 0;
                  }
                })
            .sum();
      }
    } catch (IndexNotFoundException ignored) {
      // no committed index found (may be brand new)
    } catch (Exception e) {
      LOG.error("Error calculating the directory size", e);
      return -1;
    }
    return 0;
  }

  @Override
  public void close() {
    // Stop directory size calculations
    directorySizeExecutorService.shutdown();
  }
}
