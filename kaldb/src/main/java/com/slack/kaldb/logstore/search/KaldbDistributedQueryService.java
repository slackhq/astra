package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.chunk.ChunkInfo.containsDataInTimeRange;

import brave.ScopedSpan;
import brave.Tracing;
import brave.grpc.GrpcTracing;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.linecorp.armeria.client.grpc.GrpcClients;
import com.slack.kaldb.chunk.ChunkInfo;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
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
  private final DatasetMetadataStore datasetMetadataStore;

  // Number of times the listener is fired
  public static final String SEARCH_METADATA_TOTAL_CHANGE_COUNTER =
      "search_metadata_total_change_counter";
  private final Counter searchMetadataTotalChangeCounter;

  private final Map<String, KaldbServiceGrpc.KaldbServiceFutureStub> stubs =
      new ConcurrentHashMap<>();

  public static final String DISTRIBUTED_QUERY_APDEX_SATISFIED =
      "distributed_query_apdex_satisfied";
  public static final String DISTRIBUTED_QUERY_APDEX_TOLERATING =
      "distributed_query_apdex_tolerating";
  public static final String DISTRIBUTED_QUERY_APDEX_FRUSTRATED =
      "distributed_query_apdex_frustrated";

  public static final String DISTRIBUTED_QUERY_TOTAL_SNAPSHOTS =
      "distributed_query_total_snapshots";
  public static final String DISTRIBUTED_QUERY_FAILED_SNAPSHOTS =
      "distributed_query_failed_snapshots";
  public static final String DISTRIBUTED_QUERY_SUCCESSFUL_SNAPSHOTS =
      "distributed_query_successful_snapshots";

  private final Counter distributedQueryApdexSatisfied;
  private final Counter distributedQueryApdexTolerating;
  private final Counter distributedQueryApdexFrustrated;

  private final Counter distributedQueryTotalSnapshots;

  private final Counter distributedQueryFailedSnapshots;
  private final Counter distributedQuerySuccessfulSnapshots;

  // Timeouts are structured such that we always attempt to return a successful response, as we
  // include metadata that should always be present. The Armeria timeout is used at the top request,
  // distributed query is used as a deadline for all nodes to return, and the local query timeout
  // is used for controlling lucene future timeouts.
  private final Duration requestTimeout;
  private final Duration defaultQueryTimeout;

  // For now we will use SearchMetadataStore to populate servers
  // But this is wasteful since we add snapshots more often than we add/remove nodes ( hopefully )
  // So this should be replaced cache/index metadata store when that info is present in ZK
  // Whichever store we fetch the information in the future, we should also store the
  // protocol that the node can be contacted by there since we hardcode it today
  public KaldbDistributedQueryService(
      SearchMetadataStore searchMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      DatasetMetadataStore datasetMetadataStore,
      MeterRegistry meterRegistry,
      Duration requestTimeout,
      Duration defaultQueryTimeout) {
    this.searchMetadataStore = searchMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.datasetMetadataStore = datasetMetadataStore;
    this.requestTimeout = requestTimeout;
    this.defaultQueryTimeout = defaultQueryTimeout;
    searchMetadataTotalChangeCounter = meterRegistry.counter(SEARCH_METADATA_TOTAL_CHANGE_COUNTER);
    this.searchMetadataStore.addListener(this::updateStubs);

    this.distributedQueryApdexSatisfied = meterRegistry.counter(DISTRIBUTED_QUERY_APDEX_SATISFIED);
    this.distributedQueryApdexTolerating =
        meterRegistry.counter(DISTRIBUTED_QUERY_APDEX_TOLERATING);
    this.distributedQueryApdexFrustrated =
        meterRegistry.counter(DISTRIBUTED_QUERY_APDEX_FRUSTRATED);

    this.distributedQueryTotalSnapshots = meterRegistry.counter(DISTRIBUTED_QUERY_TOTAL_SNAPSHOTS);
    this.distributedQueryFailedSnapshots =
        meterRegistry.counter(DISTRIBUTED_QUERY_FAILED_SNAPSHOTS);
    this.distributedQuerySuccessfulSnapshots =
        meterRegistry.counter(DISTRIBUTED_QUERY_SUCCESSFUL_SNAPSHOTS);

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
  protected static Map<String, List<String>> getNodesAndSnapshotsToQuery(
      Map<String, List<SearchMetadata>> searchMetadataNodesBySnapshotName) {
    ScopedSpan getQueryNodesSpan =
        Tracing.currentTracer()
            .startScopedSpan("KaldbDistributedQueryService.getNodesAndSnapshotsToQuery");
    Map<String, List<String>> nodeUrlToSnapshotNames = new HashMap<>();
    for (List<SearchMetadata> searchMetadataList : searchMetadataNodesBySnapshotName.values()) {
      SearchMetadata searchMetadata =
          KaldbDistributedQueryService.pickSearchNodeToQuery(searchMetadataList);

      if (nodeUrlToSnapshotNames.containsKey(searchMetadata.url)) {
        nodeUrlToSnapshotNames.get(searchMetadata.url).add(getRawSnapshotName(searchMetadata));
      } else {
        List<String> snapshotNames = new ArrayList<>();
        snapshotNames.add(getRawSnapshotName(searchMetadata));
        nodeUrlToSnapshotNames.put(searchMetadata.url, snapshotNames);
      }
    }
    getQueryNodesSpan.finish();
    return nodeUrlToSnapshotNames;
  }

  @VisibleForTesting
  protected static Map<String, List<SearchMetadata>> getMatchingSearchMetadata(
      SearchMetadataStore searchMetadataStore, Map<String, SnapshotMetadata> snapshotsToSearch) {
    // iterate every search metadata whose snapshot needs to be searched.
    // if there are multiple search metadata nodes then pick the most on based on
    // pickSearchNodeToQuery
    ScopedSpan getMatchingSearchMetadataSpan =
        Tracing.currentTracer()
            .startScopedSpan("KaldbDistributedQueryService.getMatchingSearchMetadata");

    Map<String, List<SearchMetadata>> searchMetadataGroupedByName = new HashMap<>();
    for (SearchMetadata searchMetadata : searchMetadataStore.getCached()) {
      if (!snapshotsToSearch.containsKey(searchMetadata.snapshotName)) {
        continue;
      }

      String rawSnapshotName = KaldbDistributedQueryService.getRawSnapshotName(searchMetadata);
      if (searchMetadataGroupedByName.containsKey(rawSnapshotName)) {
        searchMetadataGroupedByName.get(rawSnapshotName).add(searchMetadata);
      } else {
        List<SearchMetadata> searchMetadataList = new ArrayList<>();
        searchMetadataList.add(searchMetadata);
        searchMetadataGroupedByName.put(rawSnapshotName, searchMetadataList);
      }
    }
    getMatchingSearchMetadataSpan.finish();
    return searchMetadataGroupedByName;
  }

  @VisibleForTesting
  protected static Map<String, SnapshotMetadata> getMatchingSnapshots(
      SnapshotMetadataStore snapshotMetadataStore,
      DatasetMetadataStore datasetMetadataStore,
      long queryStartTimeEpochMs,
      long queryEndTimeEpochMs,
      String dataset) {
    ScopedSpan findPartitionsToQuerySpan =
        Tracing.currentTracer()
            .startScopedSpan("KaldbDistributedQueryService.findPartitionsToQuery");

    List<DatasetPartitionMetadata> partitions =
        DatasetPartitionMetadata.findPartitionsToQuery(
            datasetMetadataStore, queryStartTimeEpochMs, queryEndTimeEpochMs, dataset);
    findPartitionsToQuerySpan.finish();

    // find all snapshots that match time window and partition
    ScopedSpan snapshotsToSearchSpan =
        Tracing.currentTracer().startScopedSpan("KaldbDistributedQueryService.snapshotsToSearch");
    Map<String, SnapshotMetadata> snapshotsToSearch = new HashMap<>();
    for (SnapshotMetadata snapshotMetadata : snapshotMetadataStore.getCached()) {
      if (containsDataInTimeRange(
              snapshotMetadata.startTimeEpochMs,
              snapshotMetadata.endTimeEpochMs,
              queryStartTimeEpochMs,
              queryEndTimeEpochMs)
          && isSnapshotInPartition(snapshotMetadata, partitions)) {
        snapshotsToSearch.put(snapshotMetadata.name, snapshotMetadata);
      }
    }
    snapshotsToSearchSpan.finish();
    return snapshotsToSearch;
  }

  public static boolean isSnapshotInPartition(
      SnapshotMetadata snapshotMetadata, List<DatasetPartitionMetadata> partitions) {
    for (DatasetPartitionMetadata partition : partitions) {
      if (partition.partitions.contains(snapshotMetadata.partitionId)
          && containsDataInTimeRange(
              partition.startTimeEpochMs,
              partition.endTimeEpochMs,
              snapshotMetadata.startTimeEpochMs,
              snapshotMetadata.endTimeEpochMs)) {
        return true;
      }
    }
    return false;
  }

  private static String getRawSnapshotName(SearchMetadata searchMetadata) {
    return searchMetadata.snapshotName.startsWith("LIVE")
        ? searchMetadata.snapshotName.substring(5) // LIVE_
        : searchMetadata.snapshotName;
  }

  /*
   If there is only one node hosting the snapshot use that
   If the same snapshot exists on indexer and cache node prefer cache
   If there are multiple cache nodes, pick a cache node at random
  */
  private static SearchMetadata pickSearchNodeToQuery(
      List<SearchMetadata> queryableSearchMetadataNodes) {
    if (queryableSearchMetadataNodes.size() == 1) {
      return queryableSearchMetadataNodes.get(0);
    } else {
      List<SearchMetadata> cacheNodeHostedSearchMetadata = new ArrayList<>();
      for (SearchMetadata searchMetadata : queryableSearchMetadataNodes) {
        if (!searchMetadata.snapshotName.startsWith("LIVE")) {
          cacheNodeHostedSearchMetadata.add(searchMetadata);
        }
      }
      if (cacheNodeHostedSearchMetadata.size() == 1) {
        return cacheNodeHostedSearchMetadata.get(0);
      } else {
        return cacheNodeHostedSearchMetadata.get(
            ThreadLocalRandom.current().nextInt(cacheNodeHostedSearchMetadata.size()));
      }
    }
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

  private List<SearchResult<LogMessage>> distributedSearch(
      final KaldbSearch.SearchRequest distribSearchReq,
      Map<String, List<String>> nodesAndSnapshotsToQuery) {
    LOG.info("Starting distributed search for request: {}", distribSearchReq);
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("KaldbDistributedQueryService.distributedSearch");
    span.tag("queryServerCount", String.valueOf(nodesAndSnapshotsToQuery.size()));

    List<ListenableFuture<SearchResult<LogMessage>>> queryServers = new ArrayList<>(stubs.size());
    for (Map.Entry<String, List<String>> searchNode : nodesAndSnapshotsToQuery.entrySet()) {
      KaldbServiceGrpc.KaldbServiceFutureStub stub = getStub(searchNode.getKey());
      if (stub == null) {
        // TODO: insert a failed result in the results object that we return from this method
        // mimicing
        continue;
      }

      KaldbSearch.SearchRequest localSearchReq =
          distribSearchReq.toBuilder().addAllChunkIds(searchNode.getValue()).build();

      // make sure all underlying futures finish executing (successful/cancelled/failed/other)
      // and cannot be pending when the successfulAsList.get(SAME_TIMEOUT_MS) runs
      ListenableFuture<KaldbSearch.SearchResult> searchRequest =
          stub.withDeadlineAfter(defaultQueryTimeout.toMillis(), TimeUnit.MILLISECONDS)
              .withInterceptors(
                  GrpcTracing.newBuilder(Tracing.current()).build().newClientInterceptor())
              .search(localSearchReq);
      Function<KaldbSearch.SearchResult, SearchResult<LogMessage>> searchRequestTransform =
          SearchResultUtils::fromSearchResultProtoOrEmpty;

      queryServers.add(
          Futures.transform(
              searchRequest, searchRequestTransform::apply, MoreExecutors.directExecutor()));
    }

    Future<List<SearchResult<LogMessage>>> searchFuture = Futures.successfulAsList(queryServers);

    try {
      List<SearchResult<LogMessage>> searchResults =
          searchFuture.get(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
      LOG.debug("searchResults.size={} searchResults={}", searchResults.size(), searchResults);

      List<SearchResult<LogMessage>> response = new ArrayList(searchResults.size());
      for (SearchResult<LogMessage> searchResult : searchResults) {
        response.add(searchResult == null ? SearchResult.empty() : searchResult);
      }
      return response;
    } catch (TimeoutException e) {
      // We provide a deadline to the stub of "defaultQueryTimeout" - if this is sufficiently lower
      // than the request timeout, we would expect searchFuture.get(requestTimeout) to never throw
      // an exception. This however doesn't necessarily hold true if the query node is CPU
      // saturated, and there is not enough cpu time to fail the pending stub queries that have
      // exceeded their deadline - causing the searchFuture get to fail with a timeout.
      LOG.error(
          "Search failed with timeout exception. This is potentially due to CPU saturation of the query node.",
          e);
      span.error(e);
      return List.of(SearchResult.empty());
    } catch (Exception e) {
      LOG.error("Search failed with ", e);
      span.error(e);
      return List.of(SearchResult.empty());
    } finally {
      // always request future cancellation, so that any exceptions or incomplete futures don't
      // continue to consume CPU on work that will not be used
      searchFuture.cancel(false);
      LOG.info("Finished distributed search for request: {}", distribSearchReq);
      span.finish();
    }
  }

  public KaldbSearch.SearchResult doSearch(final KaldbSearch.SearchRequest request) {
    try {
      Map<String, SnapshotMetadata> snapshotsMatchingQuery =
          getMatchingSnapshots(
              snapshotMetadataStore,
              datasetMetadataStore,
              request.getStartTimeEpochMs(),
              request.getEndTimeEpochMs(),
              request.getDataset());

      // for each matching snapshot, we find the search metadata nodes that we can potentially query
      Map<String, List<SearchMetadata>> searchMetadataNodesMatchingQuery =
          getMatchingSearchMetadata(searchMetadataStore, snapshotsMatchingQuery);

      // from the list of search metadata nodes per snapshot, pick one. Additionally, map it to the
      // underlying URL to query
      Map<String, List<String>> nodesAndSnapshotsToQuery =
          getNodesAndSnapshotsToQuery(searchMetadataNodesMatchingQuery);

      List<SearchResult<LogMessage>> searchResults =
          distributedSearch(request, nodesAndSnapshotsToQuery);

      SearchResult<LogMessage> aggregatedResult =
          ((SearchResultAggregator<LogMessage>)
                  new SearchResultAggregatorImpl<>(SearchResultUtils.fromSearchRequest(request)))
              .aggregate(searchResults, getTotalSnapshotCount(snapshotMetadataStore));

      // We report a query with more than 0% of requested nodes, but less than 2% as a tolerable
      // response. Anything over 2% is considered an unacceptable.
      if (aggregatedResult.totalSnapshots == 0 || aggregatedResult.failedSnapshots == 0) {
        distributedQueryApdexSatisfied.increment();
      } else if (((double) aggregatedResult.failedSnapshots
              / (aggregatedResult.failedSnapshots + aggregatedResult.totalSnapshots))
          <= 0.02) {
        distributedQueryApdexTolerating.increment();
      } else {
        distributedQueryApdexFrustrated.increment();
        LOG.warn(
            "User query for '{}' was marked as frustrated due to percentage of snapshot failures ({} failed of {} requested, {} total - took {} micros)",
            request.getQueryString(),
            aggregatedResult.failedSnapshots,
            aggregatedResult.failedSnapshots + aggregatedResult.successfulSnapshots,
            aggregatedResult.totalSnapshots,
            aggregatedResult.tookMicros);
      }

      distributedQueryFailedSnapshots.increment(aggregatedResult.failedSnapshots);
      distributedQuerySuccessfulSnapshots.increment(aggregatedResult.successfulSnapshots);

      LOG.debug("aggregatedResult={}", aggregatedResult);
      return SearchResultUtils.toSearchResultProto(aggregatedResult);
    } catch (Exception e) {
      LOG.error("Distributed search failed", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the total number of unique snapshots in the system. Because we technically have duplicate
   * snapshots for a time (indexer rolled over + published, and old LIVE) we need to discount the
   * duplicates.
   */
  private int getTotalSnapshotCount(SnapshotMetadataStore snapshotMetadataStore) {
    return (int)
        snapshotMetadataStore
            .getCached()
            .stream()
            .filter(
                snapshot -> {
                  // remove one of the double-counted LIVE snapshots that will eventually be deleted
                  return !(SnapshotMetadata.isLive(snapshot)
                      && snapshot.endTimeEpochMs != ChunkInfo.MAX_FUTURE_TIME);
                })
            .count();
  }
}
