package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.chunk.ChunkInfo.containsDataInTimeRange;
import static com.slack.kaldb.server.KaldbConfig.DISTRIBUTED_QUERY_TIMEOUT_DURATION;

import brave.ScopedSpan;
import brave.Tracing;
import brave.grpc.GrpcTracing;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.linecorp.armeria.client.grpc.GrpcClients;
import com.linecorp.armeria.common.RequestContext;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
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
  private final SnapshotMetadataStore snapshotMetadataStore;

  // Number of times the listener is fired
  public static final String SEARCH_METADATA_TOTAL_CHANGE_COUNTER =
      "search_metadata_total_change_counter";
  private final Counter searchMetadataTotalChangeCounter;

  private final Map<String, KaldbServiceGrpc.KaldbServiceFutureStub> stubs =
      new ConcurrentHashMap<>();

  @VisibleForTesting
  public static long READ_TIMEOUT_MS = DISTRIBUTED_QUERY_TIMEOUT_DURATION.toMillis();

  private static final long GRPC_TIMEOUT_BUFFER_MS = 100;

  private static final Random random = new Random();

  // For now we will use SearchMetadataStore to populate servers
  // But this is wasteful since we add snapshots more often than we add/remove nodes ( hopefully )
  // So this should be replaced cache/index metadata store when that info is present in ZK
  // Whichever store we fetch the information in the future, we should also store the
  // protocol that the node can be contacted by there since we hardcode it today
  public KaldbDistributedQueryService(
      SearchMetadataStore searchMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      MeterRegistry meterRegistry) {
    this.searchMetadataStore = searchMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    searchMetadataTotalChangeCounter = meterRegistry.counter(SEARCH_METADATA_TOTAL_CHANGE_COUNTER);
    this.searchMetadataStore.addListener(this::updateStubs);

    // first time call this function manually so that we initialize stubs
    updateStubs();
  }

  private void updateStubs() {
    try {
      searchMetadataTotalChangeCounter.increment();
      Set<String> latestSearchServers = new HashSet<>();
      searchMetadataStore
          .getCached()
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
    return GrpcClients.builder(server)
        .build(KaldbServiceGrpc.KaldbServiceFutureStub.class)
        // This enables compression for requests
        // Independent of this setting, servers choose whether to compress responses
        .withCompression("gzip");
  }

  @VisibleForTesting
  public static Collection<String> getSnapshotsToSearch(
      SnapshotMetadataStore snapshotMetadataStore,
      SearchMetadataStore searchMetadataStore,
      long startTimeMs,
      long endTimeMs) {
    // step 1 - find all snapshots that match time window
    Set<String> snapshotsToSearch =
        snapshotMetadataStore
            .getCached()
            .stream()
            .filter(
                snapshotMetadata ->
                    containsDataInTimeRange(
                        snapshotMetadata.startTimeEpochMs,
                        snapshotMetadata.endTimeEpochMs,
                        startTimeMs,
                        endTimeMs))
            .map(snapshotMetadata -> snapshotMetadata.name)
            .collect(Collectors.toSet());

    // TODO: in the future, we'd have another field in search metadata to indicate if it's hosted by
    // an indexer or cache node and
    // it won't be a shuffle, but can be something more nuanced so that we prefer cache over indexer

    // step 2 - iterate every snapshot and map it to the search metadata while de-duplicating search
    // metadata with the same snapshot name
    return searchMetadataStore
        .getCached()
        .stream()
        .filter(searchMetadata -> snapshotsToSearch.contains(searchMetadata.snapshotName))
        // ensure we randomize the list so that when there are multiple
        // search metadata nodes with the same snapshot we don't always pick the same one
        .collect(
            Collectors.toMap(
                SearchMetadata::getSnapshotName,
                SearchMetadata::getUrl,
                (firstUrl, secondUrl) -> random.nextBoolean() ? firstUrl : secondUrl))
        .values();
  }

  private Set<KaldbServiceGrpc.KaldbServiceFutureStub> getSnapshotUrlsToSearch(
      long startTimeMs, long endTimeMs) {

    return getSnapshotsToSearch(snapshotMetadataStore, searchMetadataStore, startTimeMs, endTimeMs)
        .stream()
        .map(this::getStub)
        .collect(Collectors.toSet());
  }

  private KaldbServiceGrpc.KaldbServiceFutureStub getStub(String url) {
    if (stubs.get(url) != null) {
      return stubs.get(url);
    } else {
      LOG.warn(
          "snapshot {} is not cached. ZK listener on searchMetadataStore should have cached the stub",
          url);
      return null;
    }
  }

  private List<SearchResult<LogMessage>> distributedSearch(KaldbSearch.SearchRequest request) {
    LOG.info("Starting distributed search for request: {}", request);
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("KaldbDistributedQueryService.distributedSearch");

    List<ListenableFuture<SearchResult<LogMessage>>> queryServers = new ArrayList<>(stubs.size());
    span.tag("queryServerCount", String.valueOf(stubs.size()));

    for (KaldbServiceGrpc.KaldbServiceFutureStub stub :
        getSnapshotUrlsToSearch(request.getStartTimeEpochMs(), request.getEndTimeEpochMs())) {

      // make sure all underlying futures finish executing (successful/cancelled/failed/other)
      // and cannot be pending when the successfulAsList.get(SAME_TIMEOUT_MS) runs
      ListenableFuture<KaldbSearch.SearchResult> searchRequest =
          stub.withDeadlineAfter(READ_TIMEOUT_MS - GRPC_TIMEOUT_BUFFER_MS, TimeUnit.MILLISECONDS)
              .withInterceptors(
                  GrpcTracing.newBuilder(Tracing.current()).build().newClientInterceptor())
              .search(request);
      Function<KaldbSearch.SearchResult, SearchResult<LogMessage>> searchRequestTransform =
          SearchResultUtils::fromSearchResultProtoOrEmpty;
      queryServers.add(
          Futures.transform(
              searchRequest,
              searchRequestTransform::apply,
              RequestContext.current().makeContextAware(MoreExecutors.directExecutor())));
    }

    Future<List<SearchResult<LogMessage>>> searchFuture = Futures.successfulAsList(queryServers);
    try {
      List<SearchResult<LogMessage>> searchResults =
          searchFuture.get(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      LOG.debug("searchResults.size={} searchResults={}", searchResults.size(), searchResults);

      return searchResults
          .stream()
          .map(searchResult -> searchResult == null ? SearchResult.empty() : searchResult)
          .collect(Collectors.toList());
    } catch (Exception e) {
      LOG.error("Search failed with ", e);
      span.error(e);
      return List.of(SearchResult.empty());
    } finally {
      // always request future cancellation, so that any exceptions or incomplete futures don't
      // continue to consume CPU on work that will not be used
      searchFuture.cancel(false);
      LOG.info("Finished distributed search for request: {}", request);
      span.finish();
    }
  }

  public KaldbSearch.SearchResult doSearch(KaldbSearch.SearchRequest request) {
    try {
      List<SearchResult<LogMessage>> searchResults = distributedSearch(request);
      SearchResult<LogMessage> aggregatedResult =
          ((SearchResultAggregator<LogMessage>)
                  new SearchResultAggregatorImpl<>(SearchResultUtils.fromSearchRequest(request)))
              .aggregate(searchResults);

      LOG.debug("aggregatedResult={}", aggregatedResult);
      return SearchResultUtils.toSearchResultProto(aggregatedResult);
    } catch (Exception e) {
      LOG.error("Distributed search failed", e);
      throw new RuntimeException(e);
    }
  }
}
