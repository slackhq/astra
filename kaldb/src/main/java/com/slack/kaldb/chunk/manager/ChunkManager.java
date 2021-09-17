package com.slack.kaldb.chunk.manager;

import brave.ScopedSpan;
import brave.Tracing;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linecorp.armeria.common.RequestContext;
import com.slack.kaldb.chunk.Chunk;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.SearchResultAggregator;
import com.slack.kaldb.logstore.search.SearchResultAggregatorImpl;
import com.spotify.futures.CompletableFutures;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ChunkManager<T> extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ChunkManager.class);

  protected final Map<String, Chunk<T>> chunkMap = new ConcurrentHashMap<>();

  // TODO: We want to move this to the config eventually
  // Less than KaldbDistributedQueryService#READ_TIMEOUT_MS
  public static final int QUERY_TIMEOUT_SECONDS = 10;
  public static final int LOCAL_QUERY_THREAD_POOL_SIZE = 4;

  private static final ExecutorService queryExecutorService = queryThreadPool();

  /*
     One day we will have to think about rate limiting/backpressure and we will revisit this so it could potentially reject threads if the pool is full
  */
  private static ExecutorService queryThreadPool() {
    return Executors.newFixedThreadPool(
        LOCAL_QUERY_THREAD_POOL_SIZE,
        new ThreadFactoryBuilder().setNameFormat("chunk-manager-query-%d").build());
  }

  /*
   * Query the chunks in the time range, aggregate the results per aggregation policy and return the results.
   * We aggregate locally and and then the query aggregator will aggregate again. This is OKAY for the current use-case we support
   * 1. topK results sorted by timestamp
   * 2. histogram over a fixed time range
   * We will not aggregate locally for future use-cases that have complex group by etc
   */
  public CompletableFuture<SearchResult<T>> query(SearchQuery query) {

    SearchResult<T> errorResult =
        new SearchResult<>(new ArrayList<>(), 0, 0, new ArrayList<>(), 0, 0, 1, 0);

    List<CompletableFuture<SearchResult<T>>> queries =
        chunkMap
            .values()
            .stream()
            .filter(
                chunk ->
                    chunk.containsDataInTimeRange(
                        query.startTimeEpochMs / 1000, query.endTimeEpochMs / 1000))
            .map(
                (chunk) ->
                    CompletableFuture.supplyAsync(
                            () -> {
                              ScopedSpan span =
                                  Tracing.currentTracer()
                                      .startScopedSpan("ReadWriteChunkImpl.query");
                              span.tag("chunkId", chunk.info().chunkId);
                              span.tag(
                                  "chunkCreationTimeSecsSinceEpoch",
                                  String.valueOf(chunk.info().getChunkCreationTimeEpochSecs()));
                              span.tag(
                                  "chunkLastUpdatedTimeSecsEpochSecs",
                                  String.valueOf(
                                      chunk.info().getChunkLastUpdatedTimeSecsEpochSecs()));
                              span.tag(
                                  "dataStartTimeEpochSecs",
                                  String.valueOf(chunk.info().getDataStartTimeEpochSecs()));
                              span.tag(
                                  "dataEndTimeEpochSecs",
                                  String.valueOf(chunk.info().getDataEndTimeEpochSecs()));
                              span.tag(
                                  "chunkSnapshotTimeEpochSecs",
                                  String.valueOf(chunk.info().getChunkSnapshotTimeEpochSecs()));
                              span.tag("numDocs", String.valueOf(chunk.info().getNumDocs()));
                              span.tag("chunkSize", String.valueOf(chunk.info().getChunkSize()));
                              span.tag("readOnly", String.valueOf(chunk.isReadOnly()));

                              try {
                                return chunk.query(query);
                              } finally {
                                span.finish();
                              }
                            },
                            RequestContext.makeContextPropagating(queryExecutorService))
                        // TODO: this will not cancel lucene query. Use ExitableDirectoryReader in
                        // the future and pass this timeout
                        .orTimeout(QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS))
            .map(
                chunkFuture ->
                    chunkFuture.exceptionally(
                        err -> {
                          LOG.warn("Chunk Query Exception " + err);
                          // We catch IllegalArgumentException ( and any other exception that
                          // represents a parse failure ) and instead of returning an empty result
                          // we throw back an error to the user
                          if (err.getCause() instanceof IllegalArgumentException) {
                            throw (IllegalArgumentException) err.getCause();
                          }
                          return errorResult;
                        }))
            .collect(Collectors.<CompletableFuture<SearchResult<T>>>toList());

    // TODO: if all fails return error instead of empty and add test

    // Using the spotify library ( this method is much easier to operate then using
    // CompletableFuture.allOf and converting the CompletableFuture<Void> to
    // CompletableFuture<List<SearchResult>>
    CompletableFuture<List<SearchResult<T>>> searchResults = CompletableFutures.allAsList(queries);

    // Increment the node count right at the end so that we increment it only once
    //noinspection unchecked
    return ((SearchResultAggregator<T>) new SearchResultAggregatorImpl<>(query))
        .aggregate(searchResults)
        .thenApply(this::incrementNodeCount);
  }

  private SearchResult<T> incrementNodeCount(SearchResult<T> searchResult) {
    return new SearchResult<>(
        searchResult.hits,
        searchResult.tookMicros,
        searchResult.totalCount,
        searchResult.buckets,
        searchResult.failedNodes,
        searchResult.totalNodes + 1,
        searchResult.totalSnapshots,
        searchResult.snapshotsWithReplicas);
  }

  @VisibleForTesting
  public Map<String, Chunk<T>> getChunkMap() {
    return chunkMap;
  }
}
