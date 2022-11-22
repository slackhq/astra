package com.slack.kaldb.chunkManager;

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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    return Executors.newFixedThreadPool(
        Runtime.getRuntime().availableProcessors(),
        new ThreadFactoryBuilder()
            .setUncaughtExceptionHandler(
                (t, e) -> LOG.error("Exception on thread {}: {}", t.getName(), e))
            .setNameFormat("chunk-manager-query-%d")
            .build());
  }

  /*
   * Query the chunks in the time range, aggregate the results per aggregation policy and return the results.
   * We aggregate locally and then the query aggregator will aggregate again. This is OKAY for the current use-case we support
   * 1. topK results sorted by timestamp
   * 2. histogram over a fixed time range
   * We will not aggregate locally for future use-cases that have complex group by etc
   */
  public SearchResult<T> query(SearchQuery query, Duration queryTimeout) {
    SearchResult<T> errorResult =
        new SearchResult<>(new ArrayList<>(), 0, 0, new ArrayList<>(), 0, 0, 1, 0);

    CurrentTraceContext currentTraceContext = Tracing.current().currentTraceContext();

    List<Chunk<T>> chunksMatchingQuery;
    if (query.chunkIds.isEmpty()) {
      chunksMatchingQuery =
          chunkList
              .stream()
              .filter(c -> c.containsDataInTimeRange(query.startTimeEpochMs, query.endTimeEpochMs))
              .collect(Collectors.toList());
    } else {
      chunksMatchingQuery =
          chunkList
              .stream()
              .filter(c -> query.chunkIds.contains(c.id()))
              .collect(Collectors.toList());
    }

    List<CompletableFuture<SearchResult<T>>> queries =
        chunksMatchingQuery
            .stream()
            .map(
                (chunk) ->
                    CompletableFuture.supplyAsync(
                            () -> chunk.query(query),
                            currentTraceContext.executorService(queryExecutorService))
                        // TODO: this will not cancel lucene query. Use ExitableDirectoryReader
                        //  in the future and pass this timeout
                        .orTimeout(queryTimeout.toMillis(), TimeUnit.MILLISECONDS))
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
          searchResultFuture.get(queryTimeout.toMillis(), TimeUnit.MILLISECONDS);
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
