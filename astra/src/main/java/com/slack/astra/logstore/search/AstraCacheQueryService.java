package com.slack.astra.logstore.search;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.chunk.SearchContext;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.metadata.schema.ChunkSchema;
import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.server.AstraQueryServiceBase;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AstraCacheQueryService extends AstraQueryServiceBase {
  private static final Logger LOG = LoggerFactory.getLogger(AstraCacheQueryService.class);
  private static final int queryConcurrency =
      Integer.parseInt(
          System.getProperty(
              "astra.concurrent.query",
              String.valueOf(Runtime.getRuntime().availableProcessors() - 1)));

  private static final int cacheSize = Integer.parseInt(
      System.getProperty(
          "astra.concurrent.cache", String.valueOf(50)));

  private final ExecutorService executorService =
      Executors.newFixedThreadPool(
          queryConcurrency,
          new ThreadFactoryBuilder().setNameFormat("cache-query-service-%d").build());

  private final BlobStore blobStore;
  private final Duration queryTimeout;
  private SearchMetadata searchMetadata;
  private SearchMetadataStore searchMetadataStore;

  private final LoadingCache<String, LogIndexSearcher<LogMessage>> searcherCache =
      CacheBuilder.newBuilder()
          .maximumSize(cacheSize)
          // todo - what to do when these chunkIds expire?
          // .expireAfterAccess(1, TimeUnit.HOURS)
          .removalListener(
              (RemovalListener<String, LogIndexSearcher<LogMessage>>)
                  notification -> {
                    if (searchMetadataStore != null && searchMetadata != null) {
                      searchMetadata.getSnapshotNames().remove(notification.getKey());
                      searchMetadataStore.updateAsync(searchMetadata);
                    }
                  })
          .build(
              new CacheLoader<>() {
                @Override
                public LogIndexSearcher<LogMessage> load(String key) {
                  searchMetadata.getSnapshotNames().add(key);
                  searchMetadataStore.updateAsync(searchMetadata);
                  return getLogIndexSearcher(key);
                }
              });

  public AstraCacheQueryService(
      BlobStore blobStore,
      SearchMetadataStore searchMetadataStore,
      AstraConfigs.ServerConfig serverConfig,
      Duration queryTimeout) {
    this.blobStore = blobStore;
    this.searchMetadataStore = searchMetadataStore;
    this.queryTimeout = queryTimeout;

    SearchContext context = SearchContext.fromConfig(serverConfig);
    searchMetadata =
        new SearchMetadata(
            "streaming_" + context.hostname,
            context.toUrl(),
            Metadata.SearchMetadata.SearchNodeType.CACHE,
            new ArrayList<>());
    searchMetadataStore.createSync(searchMetadata);
  }

  @Override
  public AstraSearch.SearchResult doSearch(AstraSearch.SearchRequest request) {

    // todo - timeout?

    SearchQuery query = SearchResultUtils.fromSearchRequest(request);

    // at this point we already have a list of chunks that (may) be cached - just do the query
    // already

    List<CompletableFuture<SearchResult<LogMessage>>> queryFutures = new ArrayList<>();
    query.chunkIds.forEach(
        chunkId -> {
          queryFutures.add(
              CompletableFuture.supplyAsync(
                  () -> {
                    try {
                      // todo - searchStartTime/searchEndtime instead of query start/end time?
                      return searcherCache
                          .get(chunkId)
                          .search(
                              query.dataset,
                              query.queryStr,
                              query.startTimeEpochMs,
                              query.endTimeEpochMs,
                              query.howMany,
                              query.aggBuilder,
                              query.queryBuilder,
                              query.sourceFieldFilter);
                    } catch (ExecutionException e) {
                      throw new RuntimeException(e);
                    }
                  },
                  executorService));
        });

    try {
      CompletableFuture.allOf(queryFutures.toArray(CompletableFuture[]::new))
          .get(queryTimeout.get(ChronoUnit.SECONDS), TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      LOG.warn(
          "Query timeout - {} cancelled, {} total",
          queryFutures.stream().map(CompletableFuture::isCancelled).toList().size(),
          queryFutures.size());
    }

    List<SearchResult<LogMessage>> searchResults = new ArrayList<>();
    for (CompletableFuture<SearchResult<LogMessage>> queryFuture : queryFutures) {
      searchResults.add(queryFuture.getNow(new SearchResult<>()));
    }

    SearchResult<LogMessage> aggregatedResults =
        ((SearchResultAggregator<LogMessage>) new SearchResultAggregatorImpl<>(query))
            .aggregate(searchResults, false);
    return SearchResultUtils.toSearchResultProto(aggregatedResults);
  }

  @Override
  public AstraSearch.SchemaResult getSchema(AstraSearch.SchemaRequest request) {
    throw new NotImplementedException();
    // return null;
  }

  private LogIndexSearcher<LogMessage> getLogIndexSearcher(String chunkId) {
    try {
      ChunkSchema chunkSchema = ChunkSchema.deserializeBytes(blobStore.getSchema(chunkId));
      return new LogIndexSearcherImpl(
          LogIndexSearcherImpl.searcherManagerFromChunkId(chunkId, blobStore),
          chunkSchema.fieldDefMap);
    } catch (Exception e) {
      LOG.error("ERROR initializing logindexsearcher", e);
      throw new RuntimeException(e);
    }
  }
}
