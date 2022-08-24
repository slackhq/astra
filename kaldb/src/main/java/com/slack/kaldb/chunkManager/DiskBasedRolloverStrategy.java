package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** TODO */
public class DiskBasedRolloverStrategy implements ChunkRollOverStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(DiskBasedRolloverStrategy.class);

  private final MeterRegistry registry;

  public static final String LIVE_BYTES_DIR = "live_bytes_dir";
  private final AtomicLong liveBytesDirGauge;

  private final long maxBytesPerChunk;

  private long approximateDirectoryBytes = 0;
  private static final Duration directorySizeExecutorPeriod = Duration.of(10, ChronoUnit.SECONDS);
  private final ScheduledExecutorService directorySizeExecutorService =
      Executors.newScheduledThreadPool(
          0, new ThreadFactoryBuilder().setNameFormat("directory-size-%d").build());

  private volatile File activeChunkDirectory;

  public static DiskBasedRolloverStrategy fromConfig(
      MeterRegistry meterRegistry, KaldbConfigs.IndexerConfig indexerConfig) {
    return new DiskBasedRolloverStrategy(meterRegistry, indexerConfig.getMaxBytesPerChunk());
  }

  public DiskBasedRolloverStrategy(MeterRegistry registry, long maxBytesPerChunk) {
    ensureTrue(maxBytesPerChunk > 0, "Max bytes per chunk should be a positive number.");
    this.maxBytesPerChunk = maxBytesPerChunk;
    this.registry = registry;
    this.liveBytesDirGauge = this.registry.gauge(LIVE_BYTES_DIR, new AtomicLong(0));

    directorySizeExecutorService.scheduleAtFixedRate(
        () -> approximateDirectoryBytes = FileUtils.sizeOf(getActiveChunkDirectory()),
        directorySizeExecutorPeriod.getSeconds(),
        directorySizeExecutorPeriod.getSeconds(),
        TimeUnit.SECONDS);
  }

  @Override
  public boolean shouldRollOver(long currentBytesIndexed, long currentMessagesIndexed) {
    liveBytesDirGauge.set(approximateDirectoryBytes);
    boolean shouldRollover = approximateDirectoryBytes >= maxBytesPerChunk;
    LOG.info(
        "After {} messages and {} ingested bytes rolling over chunk of {} bytes.",
        currentMessagesIndexed,
        currentBytesIndexed,
        approximateDirectoryBytes);
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

  public File getActiveChunkDirectory() {
    return activeChunkDirectory;
  }

  @Override
  public void close() {
    // Stop directory size calculations
    directorySizeExecutorService.shutdown();
  }
}
