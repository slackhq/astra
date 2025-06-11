package com.slack.astra.chunkrollover;

import static com.slack.astra.util.ArgValidationUtils.ensureTrue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private final AtomicBoolean maxTimePerChunksMinsReached = new AtomicBoolean(false);
  private Instant rolloverStartTime;

  public static DiskOrMessageCountBasedRolloverStrategy fromConfig(
      MeterRegistry meterRegistry, AstraConfigs.IndexerConfig indexerConfig) {
    return new DiskOrMessageCountBasedRolloverStrategy(
        meterRegistry,
        indexerConfig.getMaxBytesPerChunk(),
        indexerConfig.getMaxMessagesPerChunk(),
        indexerConfig.getMaxTimePerChunkSeconds() > 0
            ? indexerConfig.getMaxTimePerChunkSeconds()
            : Long.MAX_VALUE);
  }

  public DiskOrMessageCountBasedRolloverStrategy(
      MeterRegistry registry, long maxBytesPerChunk, long maxMessagesPerChunk) {
    // Default max time per chunk is 24 hours
    this(registry, maxBytesPerChunk, maxMessagesPerChunk, 24 * 60 * 60);
  }

  public DiskOrMessageCountBasedRolloverStrategy(
      MeterRegistry registry,
      long maxBytesPerChunk,
      long maxMessagesPerChunk,
      long maxTimePerChunksSeconds) {
    ensureTrue(maxBytesPerChunk > 0, "Max bytes per chunk should be a positive number.");
    ensureTrue(maxMessagesPerChunk > 0, "Max messages per chunk should be a positive number.");
    this.maxBytesPerChunk = maxBytesPerChunk;
    this.maxMessagesPerChunk = maxMessagesPerChunk;
    this.registry = registry;
    this.rolloverStartTime = Instant.now();
    this.liveBytesDirGauge = this.registry.gauge(LIVE_BYTES_DIR, new AtomicLong(0));

    var unused = directorySizeExecutorService.scheduleAtFixedRate(
        () -> {
          try {
            long dirSize = calculateDirectorySize(activeChunkDirectory);
            // in case the method fails to calculate we return -1 so don't update the old value
            if (dirSize > 0) {
              approximateDirectoryBytes.set(dirSize);
            }
            if (!maxTimePerChunksMinsReached.get()
                && Instant.now()
                    .isAfter(rolloverStartTime.plus(maxTimePerChunksSeconds, ChronoUnit.SECONDS))) {
              LOG.info(
                  "Max time per chunk reached. chunkStartTime: {} currentTime: {}",
                  rolloverStartTime,
                  Instant.now());
              maxTimePerChunksMinsReached.set(true);
            }
          } catch (Exception e) {
            LOG.error("Error calculating directory size", e);
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
            || (currentMessagesIndexed >= maxMessagesPerChunk)
            || maxTimePerChunksMinsReached.get();
    if (shouldRollover) {
      LOG.debug(
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
    maxTimePerChunksMinsReached.set(false);
    rolloverStartTime = Instant.now();
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
