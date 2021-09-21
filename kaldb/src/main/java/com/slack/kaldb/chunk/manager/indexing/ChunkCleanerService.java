package com.slack.kaldb.chunk.manager.indexing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.slack.kaldb.chunk.Chunk;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    final long staleCutOffSecs =
        startInstant.minusSeconds(staleDelayDuration.toSeconds()).getEpochSecond();
    return deleteStaleChunksPastCutOff(staleCutOffSecs);
  }

  @VisibleForTesting
  public int deleteStaleChunksPastCutOff(long staleDataCutOffSecs) {
    if (staleDataCutOffSecs <= 0) {
      throw new IllegalArgumentException("staleDataCutoffSecs can't be negative");
    }

    List<Map.Entry<String, Chunk<T>>> staleChunks = new ArrayList<>();

    Set<Map.Entry<String, Chunk<T>>> mapEntries = chunkManager.getChunkMap().entrySet();
    for (Map.Entry<String, Chunk<T>> chunkEntry : mapEntries) {
      Chunk<T> chunk = chunkEntry.getValue();
      if (chunk.info().getChunkSnapshotTimeEpochSecs() <= staleDataCutOffSecs) {
        staleChunks.add(chunkEntry);
      }
    }
    LOG.info(
        "Number of stale chunks at staleDataCutOffSecs {} is {}",
        staleDataCutOffSecs,
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
