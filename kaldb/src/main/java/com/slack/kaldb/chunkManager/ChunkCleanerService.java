package com.slack.kaldb.chunkManager;

import com.slack.kaldb.chunk.Chunk;
import com.slack.kaldb.chunk.ChunkInfo;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The chunk cleaner service removes snapshot chunks after a configurable duration from the chunk
 * manager and when the number of chunks goes over a certain amount.
 */
public class ChunkCleanerService<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkCleanerService.class);

  private final ChunkManager<T> chunkManager;
  private final int limit;

  private final Duration staleDelayDuration;

  public ChunkCleanerService(ChunkManager<T> chunkManager, int limit, Duration staleDelayDuration) {
    this.chunkManager = chunkManager;
    this.limit = limit;
    this.staleDelayDuration = staleDelayDuration;
  }

  public int deleteStaleData(Instant startInstant) {
    int total = 0;
    final long staleCutOffMs =
        startInstant.minusSeconds(staleDelayDuration.toSeconds()).toEpochMilli();
    total += deleteStaleChunksPastCutOff(staleCutOffMs);
    total += deleteChunksOverLimit(this.limit);
    return total;
  }

  public int deleteStaleData() {
    int total = 0;
    Instant startInstant = Instant.now();
    final long staleCutOffMs =
        startInstant.minusSeconds(staleDelayDuration.toSeconds()).toEpochMilli();
    total += deleteStaleChunksPastCutOff(staleCutOffMs);
    total += deleteChunksOverLimit(this.limit);
    return total;
  }

  private int deleteChunksOverLimit(int limit) {
    if (limit < 0) {
      throw new IllegalArgumentException("limit can't be negative");
    }

    final List<Chunk<T>> unsortedChunks = chunkManager.getChunkList();

    if (unsortedChunks.size() <= limit) {
      LOG.info("Unsorted chunks less than or equal to limit. Doing nothing.");
      return 0;
    }

    // Sorts the list in ascending order (i.e. oldest to newest) and only gets chunks that we've
    // taken a snapshot of
    final List<Chunk<T>> sortedChunks =
        unsortedChunks.stream()
            .sorted(Comparator.comparingLong(chunk -> chunk.info().getChunkCreationTimeEpochMs()))
            .filter(chunk -> chunk.info().getChunkSnapshotTimeEpochMs() > 0)
            .toList();

    final int totalChunksToDelete = sortedChunks.size() - limit;

    final List<Chunk<T>> chunksToDelete = sortedChunks.subList(0, totalChunksToDelete);

    LOG.info("Number of chunks past limit of {} is {}", limit, chunksToDelete.size());
    chunkManager.removeStaleChunks(chunksToDelete);
    return chunksToDelete.size();
  }

  private int deleteStaleChunksPastCutOff(long staleDataCutOffMs) {
    if (staleDataCutOffMs <= 0) {
      throw new IllegalArgumentException("staleDataCutoffMs can't be negative");
    }

    List<Chunk<T>> staleChunks = new ArrayList<>();
    for (Chunk<T> chunk : (Iterable<Chunk<T>>) chunkManager.getChunkList()) {
      if (chunkIsStale(chunk.info(), staleDataCutOffMs)) {
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

  private boolean chunkIsStale(ChunkInfo chunkInfo, long staleDataCutoffMs) {
    return chunkInfo.getChunkSnapshotTimeEpochMs() > 0
        && chunkInfo.getChunkSnapshotTimeEpochMs() <= staleDataCutoffMs;
  }
}
