package com.slack.kaldb.chunk;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.logstore.search.LogIndexSearcher;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.server.MetadataStoreService;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ReadOnlyChunkImpl provides a concrete implementation for a shard to which we can support reads
 * but not writes.
 *
 * <p>This would be a wrapper around the log searcher interface without creating a heavier logStore
 * object. This implementation is also safer since we don't open the files for writing.
 *
 * <p>This class will be read only by default.
 *
 * <p>It is unclear now if the snapshot functions should be in this class or not. So, for now, we
 * leave them as no-ops.
 *
 * <p>TODO: In future, make chunkInfo read only for more safety.
 *
 * <p>TODO: Is chunk responsible for maintaining it's own metadata?
 */
public class ReadOnlyChunkImpl<T> implements Chunk<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyChunkImpl.class);

  private Optional<ChunkInfo> chunkInfo = Optional.empty();
  private Optional<LogIndexSearcher<T>> logSearcher = Optional.empty();

  private final String chunkId;
  private final ExecutorService executorService;

  public ReadOnlyChunkImpl(
      String chunkId,
      MetadataStoreService metadataStoreService,
      KaldbConfigs.CacheConfig cacheConfig)
      throws Exception {
    this.chunkId = chunkId;
    this.executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat(String.format("readonly-chunk-%s-%%d", chunkId))
                .build());
    LOG.info("Created a new read only chunk {}", chunkId);
  }

  @Override
  public ChunkInfo info() {
    return chunkInfo.orElse(null);
  }

  @Override
  public boolean containsDataInTimeRange(long startTs, long endTs) {
    return chunkInfo.map(info -> info.containsDataInTimeRange(startTs, endTs)).orElse(false);
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closed chunk {}", chunkId);

    // todo - do final cleanup of directory before exiting
  }

  @Override
  public String id() {
    return chunkId;
  }

  @Override
  public SearchResult<T> query(SearchQuery query) {
    if (logSearcher.isPresent()) {
      return logSearcher
          .get()
          .search(
              query.indexName,
              query.queryStr,
              query.startTimeEpochMs,
              query.endTimeEpochMs,
              query.howMany,
              query.bucketCount);
    } else {
      return SearchResult.empty();
    }
  }
}
