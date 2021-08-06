package com.slack.kaldb.logstore.search;

import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.internal.shaded.futures.CompletableFutures;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import com.spotify.futures.ListenableFuturesExtra;
import io.grpc.stub.StreamObserver;
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

  // public so that we can override in tests
  // TODO: In the future expose this as a config in the proto
  // TODO: ChunkManager#QUERY_TIMEOUT_SECONDS and this could be unified?
  public static int READ_TIMEOUT_MS = 15000;

  public static final KaldbSearch.SearchResult error =
      KaldbSearch.SearchResult.newBuilder().setFailedNodes(1).setTotalNodes(1).build();

  // TODO Cache the stub
  // TODO Integrate with ZK to update list of servers

  private CompletableFuture<List<KaldbSearch.SearchResult>> distributedSearch(
      KaldbSearch.SearchRequest request) {

    List<CompletableFuture<KaldbSearch.SearchResult>> futures = new ArrayList<>(servers.size());

    for (String server : servers) {
      // With the deadline we are keeping a high limit on how much time can each CompletableFuture
      // take
      // Alternately use completeOnTimeout and the value can then we configured per request and not
      // as part of the config
      KaldbServiceGrpc.KaldbServiceFutureStub stub =
          Clients.newClient(server, KaldbServiceGrpc.KaldbServiceFutureStub.class)
              .withExecutor(queryServiceExecutor)
              .withDeadlineAfter(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      futures.add(ListenableFuturesExtra.toCompletableFuture(stub.search(request)));
    }

    return futures
        .stream()
        .map(
            result ->
                result.exceptionally(
                    ex -> {
                      LOG.error("Result exception", ex);
                      return error;
                    }))
        .collect(CompletableFutures.joinList());
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

  @Override
  public void search(
      KaldbSearch.SearchRequest request,
      StreamObserver<KaldbSearch.SearchResult> responseObserver) {
    LOG.warn("Beginning distributed query");
    super.search(request, new StreamObserver<>() {
      @Override
      public void onNext(KaldbSearch.SearchResult value) {
        responseObserver.onNext(value);
      }

      @Override
      public void onError(Throwable t) {
        responseObserver.onError(t);
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
        LOG.warn("Distributed search complete, responseObserver notified");
      }
    });
  }
}
