package com.slack.kaldb.chunkManager;

import brave.Tracing;
import brave.propagation.CurrentTraceContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.chunk.Chunk;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.SearchResultAggregator;
import com.slack.kaldb.logstore.search.SearchResultAggregatorImpl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
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

  private static final ListeningExecutorService queryExecutorService = queryThreadPool();

  private static final ScheduledExecutorService queryCancellationService =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder()
              .setNameFormat("chunk-manager-query-cancellation-%d")
              .setUncaughtExceptionHandler(
                  (t, e) -> LOG.error("Exception on thread {}: {}", t.getName(), e))
              .build());

  /*
   * We want to provision the chunk query capacity such that we can almost saturate the CPU. In the event we allow
   * these to saturate the CPU it can result in the container being killed due to failed healthchecks.
   *
   * Revisit the thread pool settings if this becomes a perf issue. Also, we may need
   * different thread pools for indexer and cache nodes in the future.
   */
  private static ListeningExecutorService queryThreadPool() {
    // todo - consider making the thread count a config option; this would allow for more
    //  fine-grained tuning, but we might not need to expose this to the user if we can set sensible
    //  defaults
    return MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(
            Math.max(1, Runtime.getRuntime().availableProcessors() - 2),
            new ThreadFactoryBuilder()
                .setNameFormat("chunk-manager-query-%d")
                .setUncaughtExceptionHandler(
                    (t, e) -> LOG.error("Exception on thread {}: {}", t.getName(), e))
                .build()));
  }

  /*
   * Query the chunks in the time range, aggregate the results per aggBuilder policy and return the results.
   * We aggregate locally and then the query aggregator will aggregate again. This is OKAY for the current use-case we support
   * 1. topK results sorted by timestamp
   * 2. histogram over a fixed time range
   * We will not aggregate locally for future use-cases that have complex group by etc
   */
  public SearchResult<T> query(SearchQuery query, Duration queryTimeout) {
    SearchResult<T> errorResult = new SearchResult<>(new ArrayList<>(), 0, 0, 0, 0, 1, 0, null);

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

    // Shuffle the chunks to query. The chunkList is ordered, meaning if you had multiple concurrent
    // queries that need to search the same N chunks, they would all attempt to search the same
    // chunk at the same time, and then proceed to search the next chunk at the same time.
    // Randomizing the list of chunks helps reduce contention when attempting to concurrently search
    // a single IndexSearcher.
    Collections.shuffle(chunksMatchingQuery);

    List<ListenableFuture<SearchResult<T>>> queries =
        chunksMatchingQuery
            .stream()
            .map(
                (chunk) ->
                    queryExecutorService.submit(
                        currentTraceContext.wrap(
                            () -> {
                              try {
                                if (Thread.interrupted()) {
                                  LOG.warn(
                                      "Chunk query thread timed out without starting work, returning error result.");
                                  return errorResult;
                                }
                                return chunk.query(query);
                              } catch (Exception err) {
                                // Only log the exception message as warn, and not the entire trace
                                // as this can cause performance issues if significant amounts of
                                // invalid queries are received
                                LOG.warn("Chunk Query Exception: {}", err.getMessage());
                                LOG.debug("Chunk Query Exception", err);
                                // We catch IllegalArgumentException ( and any other exception that
                                // represents a parse failure ) and instead of returning an empty
                                // result we throw back an error to the user
                                if (err instanceof IllegalArgumentException) {
                                  throw err;
                                }
                                return errorResult;
                              }
                            })))
            .peek(
                (future) ->
                    queryCancellationService.schedule(
                        () -> future.cancel(true), queryTimeout.toMillis(), TimeUnit.MILLISECONDS))
            .collect(Collectors.toList());

    Future<List<SearchResult<T>>> searchResultFuture = Futures.successfulAsList(queries);
    try {
      List<SearchResult<T>> searchResults =
          searchResultFuture.get(queryTimeout.toMillis(), TimeUnit.MILLISECONDS);

      // check if all results are null, and if so return an error to the user
      if (searchResults.size() > 0 && searchResults.stream().allMatch(Objects::isNull)) {
        try {
          Futures.allAsList(queries).get(0, TimeUnit.SECONDS);
        } catch (Exception e) {
          throw new IllegalArgumentException(e);
        }
        // not expected to happen - we should be guaranteed that the list has at least one failed
        // future, which should throw when we try to get on allAsList
        throw new IllegalArgumentException(
            "Chunk query error - all results returned null values with no exceptions thrown");
      }

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
        searchResult.failedNodes,
        searchResult.totalNodes + 1,
        searchResult.totalSnapshots,
        searchResult.snapshotsWithReplicas,
        searchResult.internalAggregation);
  }

  @VisibleForTesting
  public List<Chunk<T>> getChunkList() {
    return chunkList;
  }
}
