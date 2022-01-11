package com.slack.kaldb.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import brave.grpc.GrpcTracing;
import com.linecorp.armeria.client.Clients;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import com.spotify.futures.CompletableFutures;
import com.spotify.futures.ListenableFuturesExtra;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service that performs a distributed search We take all the search metadata stores and query
 * the unique nodes Each node performs a local search across all it's chunks and sends back an
 * aggregated response The distributed query service then aggregates results across all the nodes So
 * there is two layers of aggregation going on currently which is okay for now since the only types
 * of queries we support are - searches and date range histograms In the future we want to query
 * each chunk from the distributed query service and perform the aggregation here
 */
public class KaldbDistributedQueryService extends KaldbQueryServiceBase {

  private static final Logger LOG = LoggerFactory.getLogger(KaldbDistributedQueryService.class);

  private final SearchMetadataStore searchMetadataStore;

  // Number of times the listener is fired
  public static final String SEARCH_METADATA_TOTAL_CHANGE_COUNTER =
      "search_metadata_total_change_counter";
  private final MeterRegistry meterRegistry;
  private final Counter searchMetadataTotalChangeCounter;

  private Map<String, KaldbServiceGrpc.KaldbServiceFutureStub> stubs = new ConcurrentHashMap<>();

  // public so that we can override in tests
  // TODO: In the future expose this as a config in the proto
  // TODO: ChunkManager#QUERY_TIMEOUT_SECONDS and this could be unified?
  public static int READ_TIMEOUT_MS = 15000;

  // For now we will use SearchMetadataStore to populate servers
  // But this is wasteful since we add snapshots more often than we add/remove nodes ( hopefully )
  // So this should be replaced cache/index metadata store when that info is present in ZK
  // Whichever store we fetch the information in the future, we should also store the
  // protocol that the node can be contacted by there since we hardcode it today
  public KaldbDistributedQueryService(
      SearchMetadataStore searchMetadataStore, MeterRegistry meterRegistry) {
    this.searchMetadataStore = searchMetadataStore;
    this.meterRegistry = meterRegistry;
    searchMetadataTotalChangeCounter =
        this.meterRegistry.counter(SEARCH_METADATA_TOTAL_CHANGE_COUNTER);
    this.searchMetadataStore.addListener(this::updateStubs);

    // first time call this function manually so that we initialize stubs
    updateStubs();
  }

  private void updateStubs() {
    try {
      searchMetadataTotalChangeCounter.increment();
      Set<String> latestSearchServers = new HashSet<>();
      searchMetadataStore
          .list()
          .get()
          .forEach(searchMetadata -> latestSearchServers.add(searchMetadata.url));

      int currentSearchMetadataCount = stubs.size();
      AtomicInteger addedStubs = new AtomicInteger();
      AtomicInteger removedStubs = new AtomicInteger();

      // add new servers
      latestSearchServers.forEach(
          server -> {
            if (!stubs.containsKey(server)) {
              LOG.debug("SearchMetadata listener event. Adding server={}", server);
              stubs.put(server, getKaldbServiceGrpcClient(server));
              addedStubs.getAndIncrement();
            }
          });

      // invalidate old servers that no longer exist
      stubs
          .keySet()
          .forEach(
              server -> {
                LOG.debug("SearchMetadata listener event. Removing server={}", server);
                if (!latestSearchServers.contains(server)) {
                  stubs.remove(server);
                  removedStubs.getAndIncrement();
                }
              });

      LOG.info(
          "SearchMetadata listener event. previous_total_stub_count={} current_total_stub_count={} added_stubs={} removed_stubs={}",
          currentSearchMetadataCount,
          stubs.size(),
          addedStubs.get(),
          removedStubs.get());
    } catch (Exception e) {
      LOG.error("Cannot update SearchMetadata cache on the query service", e);
      throw new RuntimeException("Cannot update SearchMetadata cache on the query service ", e);
    }
  }

  private KaldbServiceGrpc.KaldbServiceFutureStub getKaldbServiceGrpcClient(String server) {
    return Clients.newClient(server, KaldbServiceGrpc.KaldbServiceFutureStub.class)
        .withCompression("gzip");
  }

  private CompletableFuture<List<SearchResult<LogMessage>>> distributedSearch(
      KaldbSearch.SearchRequest request) {
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("KaldbDistributedQueryService.distributedSearch");

    try {
      List<CompletableFuture<SearchResult<LogMessage>>> queryServers =
          new ArrayList<>(stubs.size());

      for (KaldbServiceGrpc.KaldbServiceFutureStub stub : stubs.values()) {
        // With the deadline we are keeping a high limit on how much time each CompletableFuture
        // take. Alternately use completeOnTimeout and the value can then we configured
        // per request and not as part of the config
        queryServers.add(
            ListenableFuturesExtra.toCompletableFuture(
                    stub.withDeadlineAfter(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        .withInterceptors(
                            GrpcTracing.newBuilder(Tracing.current())
                                .build()
                                .newClientInterceptor())
                        .search(request))
                .thenApply(SearchResultUtils::fromSearchResultProtoOrEmpty)
                .orTimeout(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      }

      try {
        return CompletableFutures.successfulAsList(
            queryServers,
            ex -> {
              LOG.error("Node failed to respond ", ex);
              return SearchResult.empty();
            });
      } finally {
        span.finish();
      }
    } catch (Exception e) {
      LOG.error("SearchMetadata failed with ", e);
      List<CompletableFuture<SearchResult<LogMessage>>> emptyResultList =
          Collections.singletonList(CompletableFuture.supplyAsync(SearchResult::empty));
      return CompletableFutures.allAsList(emptyResultList);
    }
  }

  public CompletableFuture<KaldbSearch.SearchResult> doSearch(KaldbSearch.SearchRequest request) {
    CompletableFuture<List<SearchResult<LogMessage>>> searchResults = distributedSearch(request);

    CompletableFuture<SearchResult<LogMessage>> aggregatedResult =
        ((SearchResultAggregator<LogMessage>)
                new SearchResultAggregatorImpl<>(SearchResultUtils.fromSearchRequest(request)))
            .aggregate(searchResults);

    return SearchResultUtils.toSearchResultProto(aggregatedResult);
  }
}
