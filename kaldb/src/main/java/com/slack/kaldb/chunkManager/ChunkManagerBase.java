package com.slack.kaldb.chunkManager;

import brave.ScopedSpan;
import brave.Tracing;
import brave.propagation.CurrentTraceContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.chunk.Chunk;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.SearchResultAggregator;
import com.slack.kaldb.logstore.search.SearchResultAggregatorImpl;
import com.slack.kaldb.metadata.schema.FieldType;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeoutException;
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
  private final Semaphore concurrentQueries;

  public ChunkManagerBase() {
    // todo - move this to a config value if we end up needing this param
    int semaphoreCount =
        Integer.parseInt(
            System.getProperty(
                "kaldb.concurrent.query",
                String.valueOf(Runtime.getRuntime().availableProcessors() - 1)));
    LOG.info("Using kaldb.concurrent.query - {}", semaphoreCount);
    concurrentQueries = new Semaphore(semaphoreCount, true);
  }

  /*
   * Query the chunks in the time range, aggregate the results per aggregation policy and return the results.
   * We aggregate locally and then the query aggregator will aggregate again. This is OKAY for the current use-case we support
   * 1. topK results sorted by timestamp
   * 2. histogram over a fixed time range
   * We will not aggregate locally for future use-cases that have complex group by etc
   */
  public SearchResult<T> query(SearchQuery query, Duration queryTimeout) {
    SearchResult<T> errorResult = new SearchResult<>(new ArrayList<>(), 0, 0, 0, 1, 0, null);

    CurrentTraceContext currentTraceContext = Tracing.current().currentTraceContext();

    List<Chunk<T>> chunksMatchingQuery;
    if (query.chunkIds.isEmpty()) {
      chunksMatchingQuery =
          chunkList.stream()
              .filter(c -> c.containsDataInTimeRange(query.startTimeEpochMs, query.endTimeEpochMs))
              .collect(Collectors.toList());
    } else {
      chunksMatchingQuery =
          chunkList.stream()
              .filter(c -> query.chunkIds.contains(c.id()))
              .collect(Collectors.toList());
    }

    // Shuffle the chunks to query. The chunkList is ordered, meaning if you had multiple concurrent
    // queries that need to search the same N chunks, they would all attempt to search the same
    // chunk at the same time, and then proceed to search the next chunk at the same time.
    // Randomizing the list of chunks helps reduce contention when attempting to concurrently search
    // a single IndexSearcher.
    Collections.shuffle(chunksMatchingQuery);

    try {
      try (var scope = new StructuredTaskScope<SearchResult<T>>()) {
        List<StructuredTaskScope.Subtask<SearchResult<T>>> chunkSubtasks =
            chunksMatchingQuery.stream()
                .map(
                    (chunk) ->
                        scope.fork(
                            currentTraceContext.wrap(
                                () -> {
                                  ScopedSpan span =
                                      Tracing.currentTracer()
                                          .startScopedSpan("ChunkManagerBase.chunkQuery");
                                  span.tag("chunkId", chunk.id());
                                  span.tag("chunkSnapshotPath", chunk.info().getSnapshotPath());
                                  concurrentQueries.acquire();
                                  try {
                                    return chunk.query(query);
                                  } finally {
                                    concurrentQueries.release();
                                    span.finish();
                                  }
                                })))
                .toList();
        try {
          scope.joinUntil(Instant.now().plusSeconds(queryTimeout.toSeconds()));
        } catch (TimeoutException timeoutException) {
          scope.shutdown();
          scope.join();
        }

        List<SearchResult<T>> searchResults =
            chunkSubtasks.stream()
                .map(
                    searchResultSubtask -> {
                      try {
                        if (searchResultSubtask
                            .state()
                            .equals(StructuredTaskScope.Subtask.State.SUCCESS)) {
                          return searchResultSubtask.get();
                        } else if (searchResultSubtask
                            .state()
                            .equals(StructuredTaskScope.Subtask.State.FAILED)) {
                          Throwable throwable = searchResultSubtask.exception();
                          if (throwable instanceof IllegalArgumentException) {
                            // We catch IllegalArgumentException ( and any other exception that
                            // represents a parse failure ) and instead of returning an empty
                            // result we throw back an error to the user
                            throw new IllegalArgumentException(throwable);
                          }
                          LOG.warn("Chunk Query Exception: {}", throwable.getMessage());
                        }
                        // else UNAVAILABLE (ie, timedout)
                        return errorResult;
                      } catch (Exception err) {
                        if (err instanceof IllegalArgumentException) {
                          throw err;
                        }

                        // Only log the exception message as warn, and not the entire trace
                        // as this can cause performance issues if significant amounts of
                        // invalid queries are received
                        LOG.warn("Chunk Query Exception: {}", err.getMessage());
                        return errorResult;
                      }
                    })
                .toList();

        // check if all results are null, and if so return an error to the user
        if (!searchResults.isEmpty() && searchResults.stream().allMatch(Objects::isNull)) {
          throw new IllegalArgumentException(
              "Chunk query error - all results returned null values");
        }

        //noinspection unchecked
        SearchResult<T> aggregatedResults =
            ((SearchResultAggregator<T>) new SearchResultAggregatorImpl<>(query))
                .aggregate(searchResults, false);
        return incrementNodeCount(aggregatedResults);
      }
    } catch (Exception e) {
      LOG.error("Error searching across chunks ", e);
      throw new RuntimeException(e);
    }
  }

  private SearchResult<T> incrementNodeCount(SearchResult<T> searchResult) {
    return new SearchResult<>(
        searchResult.hits,
        searchResult.tookMicros,
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

  @Override
  public Map<String, FieldType> getSchema() {
    Map<String, FieldType> schema = new HashMap<>();
    chunkList.forEach(chunk -> schema.putAll(chunk.getSchema()));
    return Collections.unmodifiableMap(schema);
  }
}
