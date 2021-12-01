package com.slack.kaldb.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import brave.grpc.GrpcTracing;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.linecorp.armeria.client.Clients;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import com.spotify.futures.CompletableFutures;
import com.spotify.futures.ListenableFuturesExtra;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbDistributedQueryService extends KaldbQueryServiceBase {

  private static final Logger LOG = LoggerFactory.getLogger(KaldbDistributedQueryService.class);

  public static List<String> servers = new ArrayList<>();

  private final LoadingCache<String, KaldbServiceGrpc.KaldbServiceFutureStub> stubLoadingCache =
      CacheBuilder.newBuilder().build(CacheLoader.from(this::getKaldbServiceGrpcClient));

  private KaldbServiceGrpc.KaldbServiceFutureStub getKaldbServiceGrpcClient(String server) {
    return Clients.newClient(server, KaldbServiceGrpc.KaldbServiceFutureStub.class)
        .withCompression("gzip")
        .withInterceptors(GrpcTracing.newBuilder(Tracing.current()).build().newClientInterceptor());
  }

  // public so that we can override in tests
  // TODO: In the future expose this as a config in the proto
  // TODO: ChunkManager#QUERY_TIMEOUT_SECONDS and this could be unified?
  public static int READ_TIMEOUT_MS = 15000;

  public static final KaldbSearch.SearchResult emptyResult =
      KaldbSearch.SearchResult.newBuilder().setFailedNodes(1).setTotalNodes(1).build();

  // TODO Integrate with ZK to update list of servers

  private CompletableFuture<List<KaldbSearch.SearchResult>> distributedSearch(
      KaldbSearch.SearchRequest request) {
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("KaldbDistributedQueryService.distributedSearch");

    List<CompletableFuture<KaldbSearch.SearchResult>> queryServers =
        new ArrayList<>(servers.size());

    for (String server : servers) {
      // With the deadline we are keeping a high limit on how much time can each CompletableFuture
      // take
      // Alternately use completeOnTimeout and the value can then we configured per request and not
      // as part of the config
      queryServers.add(
          ListenableFuturesExtra.toCompletableFuture(
                  stubLoadingCache
                      .getUnchecked(server)
                      .withDeadlineAfter(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                      .search(request))
              .orTimeout(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }

    try {
      return CompletableFutures.successfulAsList(
          queryServers,
          ex -> {
            LOG.error("Node failed to respond ", ex);
            return emptyResult;
          });
    } finally {
      span.finish();
    }
  }

  public CompletableFuture<KaldbSearch.SearchResult> doSearch(KaldbSearch.SearchRequest request) {
    CompletableFuture<List<SearchResult<LogMessage>>> searchResults =
        distributedSearch(request)
            .thenApply(
                results ->
                    results
                        .stream()
                        .map(SearchResultUtils::fromSearchResultProtoOrEmpty)
                        .collect(Collectors.toList()));

    CompletableFuture<SearchResult<LogMessage>> aggregatedResult =
        ((SearchResultAggregator<LogMessage>)
                new SearchResultAggregatorImpl<>(SearchResultUtils.fromSearchRequest(request)))
            .aggregate(searchResults);

    return SearchResultUtils.toSearchResultProto(aggregatedResult);
  }
}
