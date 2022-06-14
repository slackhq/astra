package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.server.KaldbConfig.LOCAL_QUERY_TIMEOUT_DURATION;

import brave.Tracing;
import brave.propagation.CurrentTraceContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.chunk.Chunk;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.SearchResultAggregator;
import com.slack.kaldb.logstore.search.SearchResultAggregatorImpl;
import com.spotify.futures.CompletableFutures;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A chunk manager provides a unified api to write and query all the chunks in the application.
 *
 * <p>Internally the chunk manager maintains a map of chunks, and includes a way to populate and
 * safely query this collection in parallel with a dedicated executor.
 */
public abstract class ChunkManagerBase<T> extends AbstractIdleService implements ChunkManager<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ChunkManagerBase.class);

  // we use a CopyOnWriteArrayList as we expect to have very few edits to this list compared
  // to the amount of reads, and it must be a threadsafe implementation
  protected final List<Chunk<T>> chunkList = new CopyOnWriteArrayList<>();

  private static final ExecutorService queryExecutorService = queryThreadPool();

  /*
   * We want to provision the chunk query capacity such that we can almost saturate the CPU. In the event we allow
   * these to saturate the CPU it can result in the container being killed due to failed healthchecks.
   *
   * Revisit the thread pool settings if this becomes a perf issue. Also, we may need
   * different thread pools for indexer and cache nodes in the future.
   */
  private static ExecutorService queryThreadPool() {
    int threadPoolSize = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
    return Executors.newFixedThreadPool(
        threadPoolSize, new ThreadFactoryBuilder().setNameFormat("chunk-manager-query-%d").build());
  }

  /*
   * Query the chunks in the time range, aggregate the results per aggregation policy and return the results.
   * We aggregate locally and and then the query aggregator will aggregate again. This is OKAY for the current use-case we support
   * 1. topK results sorted by timestamp
   * 2. histogram over a fixed time range
   * We will not aggregate locally for future use-cases that have complex group by etc
   */
  public SearchResult<T> query(SearchQuery query) {
    SearchResult<T> errorResult =
        new SearchResult<>(new ArrayList<>(), 0, 0, new ArrayList<>(), 0, 0, 1, 0);

    CurrentTraceContext currentTraceContext = Tracing.current().currentTraceContext();
    List<CompletableFuture<SearchResult<T>>> queries =
        chunkList
            .stream()
            .filter(
                chunk ->
                    chunk.containsDataInTimeRange(query.startTimeEpochMs, query.endTimeEpochMs))
            .map(
                (chunk) ->
                    CompletableFuture.supplyAsync(
                            () -> chunk.query(query),
                            currentTraceContext.executorService(queryExecutorService))
                        // TODO: this will not cancel lucene query. Use ExitableDirectoryReader
                        //  in the future and pass this timeout
                        .orTimeout(LOCAL_QUERY_TIMEOUT_DURATION.toMillis(), TimeUnit.MILLISECONDS))
            .map(
                chunkFuture ->
                    chunkFuture.exceptionally(
                        err -> {
                          LOG.warn("Chunk Query Exception: ", err);
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
    CompletableFuture<List<SearchResult<T>>> searchResultFuture =
        CompletableFutures.allAsList(queries);
    try {
      List<SearchResult<T>> searchResults =
          searchResultFuture.get(LOCAL_QUERY_TIMEOUT_DURATION.toMillis(), TimeUnit.MILLISECONDS);
      //noinspection unchecked
      SearchResult<T> aggregatedResults =
          ((SearchResultAggregator<T>) new SearchResultAggregatorImpl<>(query))
              .aggregate(searchResults);
      return incrementNodeCount(aggregatedResults);
    } catch (Exception e) {
      LOG.error("Error searching across chunks ", e);
      throw new RuntimeException(e);
    } finally {
      // always request future cancellation. This won't interrupt I/O or downstream futures,
      // but is good practice. Since this is backed by a CompletableFuture
      // mayInterruptIfRunning has no effect
      searchResultFuture.cancel(true);
    }
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
  public List<Chunk<T>> getChunkList() {
    return chunkList;
  }
}
