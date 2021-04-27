package com.slack.kaldb.chunk;

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The chunk cleaner task removes snapshotted chunks after a configurable duration from the chunk
 * manager.
 */
public class ChunkCleanerTask<T> implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkCleanerTask.class);

  private final Duration staleDelayDuration;
  private final ChunkManager chunkManager;

  public ChunkCleanerTask(ChunkManager chunkManager, Duration staleDelayDuration) {
    this.chunkManager = chunkManager;
    this.staleDelayDuration = staleDelayDuration;
  }

  @Override
  public void run() {
    try {
      runAt(Instant.now());
    } catch (Exception e) {
      LOG.error("ChunkCleanerTask failed with error", e);
    }
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
}
