package com.slack.kaldb.chunkManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linecorp.armeria.common.RequestContext;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.blobfs.s3.S3BlobFsConfig;
import com.slack.kaldb.chunk.Chunk;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.SearchResultAggregator;
import com.slack.kaldb.logstore.search.SearchResultAggregatorImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.spotify.futures.CompletableFutures;
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
public abstract class ChunkManager<T> extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ChunkManager.class);

  // we use a CopyOnWriteArrayList as we expect to have very few edits to this list compared
  // to the amount of reads, and it must be a threadsafe implementation
  protected final List<Chunk<T>> chunkList = new CopyOnWriteArrayList<>();

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
        chunkList
            .stream()
            .filter(
                chunk ->
                    chunk.containsDataInTimeRange(query.startTimeEpochMs, query.endTimeEpochMs))
            .map(
                (chunk) ->
                    CompletableFuture.supplyAsync(
                            () -> chunk.query(query),
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
  public List<Chunk<T>> getChunkList() {
    return chunkList;
  }

  protected static S3BlobFs getS3BlobFsClient(KaldbConfigs.KaldbConfig kaldbCfg) {
    KaldbConfigs.S3Config s3Config = kaldbCfg.getS3Config();
    S3BlobFsConfig s3BlobFsConfig =
        new S3BlobFsConfig(
            s3Config.getS3AccessKey(),
            s3Config.getS3SecretKey(),
            s3Config.getS3Region(),
            s3Config.getS3EndPoint());
    S3BlobFs s3BlobFs = new S3BlobFs();
    s3BlobFs.init(s3BlobFsConfig);
    return s3BlobFs;
  }
}
