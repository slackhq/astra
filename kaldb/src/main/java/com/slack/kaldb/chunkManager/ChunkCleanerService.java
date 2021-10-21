package com.slack.kaldb.chunkManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.slack.kaldb.chunk.Chunk;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The chunk cleaner service removes snapshot chunks after a configurable duration from the chunk
 * manager.
 */
public class ChunkCleanerService<T> extends AbstractScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkCleanerService.class);

  private final Duration staleDelayDuration;
  private final IndexingChunkManager chunkManager;

  public ChunkCleanerService(IndexingChunkManager chunkManager, Duration staleDelayDuration) {
    this.chunkManager = chunkManager;
    this.staleDelayDuration = staleDelayDuration;
  }

  @VisibleForTesting
  public void runAt(Instant instant) {
    LOG.info("Starting ChunkCleanerTask.");
    deleteStaleData(instant);
    LOG.info("Finished ChunkCleanerTask.");
  }

  @VisibleForTesting
  public int deleteStaleData(Instant startInstant) {
    final long staleCutOffMs =
        startInstant.minusSeconds(staleDelayDuration.toSeconds()).toEpochMilli();
    return deleteStaleChunksPastCutOff(staleCutOffMs);
  }

  @VisibleForTesting
  public int deleteStaleChunksPastCutOff(long staleDataCutOffMs) {
    if (staleDataCutOffMs <= 0) {
      throw new IllegalArgumentException("staleDataCutoffMs can't be negative");
    }

    List<Chunk<T>> staleChunks = new ArrayList<>();
    for (Chunk<T> chunk : (Iterable<Chunk<T>>) chunkManager.chunkList) {
      if (chunk.info().getChunkSnapshotTimeEpochMs() <= staleDataCutOffMs) {
        staleChunks.add(chunk);
      }
    }

    LOG.info(
        "Number of stale chunks at staleDataCutOffMs {} is {}",
        staleDataCutOffMs,
        staleChunks.size());
    chunkManager.removeStaleChunks(staleChunks);
    return staleChunks.size();
  }

  @Override
  protected void runOneIteration() {
    try {
      runAt(Instant.now());
    } catch (Exception e) {
      LOG.error("ChunkCleanerTask failed with error", e);
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(staleDelayDuration, staleDelayDuration);
  }
}
