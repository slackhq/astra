package com.slack.astra.logstore.search;

import static com.slack.astra.chunk.ChunkInfo.containsDataInTimeRange;

import brave.ScopedSpan;
import brave.Tracing;
import brave.grpc.GrpcTracing;
import brave.propagation.CurrentTraceContext;
import com.google.common.annotations.VisibleForTesting;
import com.linecorp.armeria.client.grpc.GrpcClients;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.proto.service.AstraServiceGrpc;
import com.slack.astra.server.AstraQueryServiceBase;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
public class AstraDistributedQueryService extends AstraQueryServiceBase implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(AstraDistributedQueryService.class);
  private final MeterRegistry meterRegistry;

  public static final String ASTRA_ENABLE_QUERY_GATING_FLAG = "astra.enableQueryGating";

  private final SearchMetadataStore searchMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final DatasetMetadataStore datasetMetadataStore;

  // There can be 100s of nodes to query the schema. Tecnically asking 1 node is enough.
  // But to be in the safe we query upto 5 nodes
  private static final Integer LIMIT_SCHEMA_NODES_TO_QUERY = 5;

  // Number of times the listener is fired
  public static final String SEARCH_METADATA_TOTAL_CHANGE_COUNTER =
      "search_metadata_total_change_counter";
  private final Counter searchMetadataTotalChangeCounter;

  protected final Map<String, AstraServiceGrpc.AstraServiceFutureStub> stubs =
      new ConcurrentHashMap<>();

  public static final String DISTRIBUTED_QUERY_APDEX_SATISFIED =
      "distributed_query_apdex_satisfied";
  public static final String DISTRIBUTED_QUERY_APDEX_TOLERATING =
      "distributed_query_apdex_tolerating";
  public static final String DISTRIBUTED_QUERY_APDEX_FRUSTRATED =
      "distributed_query_apdex_frustrated";

  public static final String DISTRIBUTED_QUERY_TOTAL_SNAPSHOTS =
      "distributed_query_total_snapshots";
  public static final String DISTRIBUTED_QUERY_SNAPSHOTS_WITH_REPLICAS =
      "distributed_query_snapshots_with_replicas";

  public static final String ASTRA_QUERIES_SUCCESSFUL_COUNT = "astra_queries_successful_count";
  public static final String ASTRA_QUERIES_INCOMPLETE_COUNT = "astra_queries_incomplete_count";
  public static final String ASTRA_QUERIES_FAILED_COUNT = "astra_queries_failed_count";

  public static final String DISTRIBUTED_QUERY_DURATION_SECONDS =
      "distributed_query_duration_seconds";
  public static final String SNAPSHOT_BATCH_SUCCESS_RATIO = "snapshot_batch_success_ratio";

  private static final String TIME_WINDOW_TAG = "requested_time_window";

  private final Counter distributedQueryApdexSatisfied;
  private final Counter distributedQueryApdexTolerating;
  private final Counter distributedQueryApdexFrustrated;
  private final Counter distributedQueryTotalSnapshots;
  private final Counter distributedQuerySnapshotsWithReplicas;
  private final Counter successfulQueryCount;
  private final Counter incompleteQueryCount;
  private final Counter failedQueryCount;
  // Timeouts are structured such that we always attempt to return a successful response, as we
  // include metadata that should always be present. The Armeria timeout is used at the top request,
  // distributed query is used as a deadline for all nodes to return, and the local query timeout
  // is used for controlling lucene future timeouts.
  private final Duration defaultQueryTimeout;
  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor();
  private ScheduledFuture<?> pendingStubUpdate;
  private final AstraMetadataStoreChangeListener<SearchMetadata> searchMetadataListener =
      (searchMetadata) -> triggerStubUpdate();

  private final int SCHEMA_TIMEOUT_MS =
      Integer.parseInt(System.getProperty("astra.query.schemaTimeoutMs", "500"));

  // For now we will use SearchMetadataStore to populate servers
  // But this is wasteful since we add snapshots more often than we add/remove nodes ( hopefully )
  // So this should be replaced cache/index metadata store when that info is present in ZK
  // Whichever store we fetch the information in the future, we should also store the
  // protocol that the node can be contacted by there since we hardcode it today
  public AstraDistributedQueryService(
      SearchMetadataStore searchMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      DatasetMetadataStore datasetMetadataStore,
      MeterRegistry meterRegistry,
      Duration requestTimeout,
      Duration defaultQueryTimeout) {
    this.searchMetadataStore = searchMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.datasetMetadataStore = datasetMetadataStore;
    this.defaultQueryTimeout = defaultQueryTimeout;
    searchMetadataTotalChangeCounter = meterRegistry.counter(SEARCH_METADATA_TOTAL_CHANGE_COUNTER);
    this.distributedQueryApdexSatisfied = meterRegistry.counter(DISTRIBUTED_QUERY_APDEX_SATISFIED);
    this.distributedQueryApdexTolerating =
        meterRegistry.counter(DISTRIBUTED_QUERY_APDEX_TOLERATING);
    this.distributedQueryApdexFrustrated =
        meterRegistry.counter(DISTRIBUTED_QUERY_APDEX_FRUSTRATED);
    this.distributedQueryTotalSnapshots = meterRegistry.counter(DISTRIBUTED_QUERY_TOTAL_SNAPSHOTS);
    this.distributedQuerySnapshotsWithReplicas =
        meterRegistry.counter(DISTRIBUTED_QUERY_SNAPSHOTS_WITH_REPLICAS);
    this.successfulQueryCount = meterRegistry.counter(ASTRA_QUERIES_SUCCESSFUL_COUNT);
    this.incompleteQueryCount = meterRegistry.counter(ASTRA_QUERIES_INCOMPLETE_COUNT);
    this.failedQueryCount = meterRegistry.counter(ASTRA_QUERIES_FAILED_COUNT);
    this.meterRegistry = meterRegistry;

    // start listening for new events
    this.searchMetadataStore.addListener(searchMetadataListener);

    // trigger an update, if it hasn't already happened
    triggerStubUpdate();
  }

  private void triggerStubUpdate() {
    if (pendingStubUpdate == null || pendingStubUpdate.getDelay(TimeUnit.SECONDS) <= 0) {
      // Add a small aggregation window to prevent churn of zk updates causing too many internal
      // updates
      pendingStubUpdate = executorService.schedule(this::doStubUpdate, 1500, TimeUnit.MILLISECONDS);
    } else {
      LOG.debug(
          "Update stubs already queued for execution, will run in {} ms",
          pendingStubUpdate.getDelay(TimeUnit.MILLISECONDS));
    }
  }

  private void doStubUpdate() {
    try {
      searchMetadataTotalChangeCounter.increment();
      Set<String> latestSearchServers = new HashSet<>();
      searchMetadataStore
          .listSync()
          .forEach(
              searchMetadata -> {
                if (!Boolean.getBoolean(ASTRA_ENABLE_QUERY_GATING_FLAG)
                    || searchMetadata.isSearchable()) {
                  latestSearchServers.add(searchMetadata.url);
                }
              });

      int currentSearchMetadataCount = stubs.size();
      AtomicInteger addedStubs = new AtomicInteger();
      AtomicInteger removedStubs = new AtomicInteger();

      // add new servers
      latestSearchServers.forEach(
          server -> {
            if (!stubs.containsKey(server)) {
              LOG.debug("SearchMetadata listener event. Adding server={}", server);
              stubs.put(server, getAstraServiceGrpcClient(server));
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

      LOG.debug(
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

  private AstraServiceGrpc.AstraServiceFutureStub getAstraServiceGrpcClient(String server) {
    return GrpcClients.builder(server)
        .build(AstraServiceGrpc.AstraServiceFutureStub.class)
        // This enables compression for requests
        // Independent of this setting, servers choose whether to compress responses
        .withCompression("gzip");
  }

  @VisibleForTesting
  protected static Map<String, List<String>> getNodesAndSnapshotsToQuery(
      Map<String, List<SearchMetadata>> searchMetadataNodesBySnapshotName) {
    ScopedSpan getQueryNodesSpan =
        Tracing.currentTracer()
            .startScopedSpan("AstraDistributedQueryService.getNodesAndSnapshotsToQuery");
    Map<String, List<String>> nodeUrlToSnapshotNames = new HashMap<>();
    for (List<SearchMetadata> searchMetadataList : searchMetadataNodesBySnapshotName.values()) {
      SearchMetadata searchMetadata =
          AstraDistributedQueryService.pickSearchNodeToQuery(searchMetadataList);

      if (nodeUrlToSnapshotNames.containsKey(searchMetadata.url)) {
        nodeUrlToSnapshotNames.get(searchMetadata.url).add(getRawSnapshotName(searchMetadata));
      } else {
        List<String> snapshotNames = new ArrayList<>();
        snapshotNames.add(getRawSnapshotName(searchMetadata));
        nodeUrlToSnapshotNames.put(searchMetadata.url, snapshotNames);
      }
    }
    getQueryNodesSpan.tag("nodes_to_query", String.valueOf(nodeUrlToSnapshotNames.size()));
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
            .startScopedSpan("AstraDistributedQueryService.getMatchingSearchMetadata");

    Map<String, List<SearchMetadata>> searchMetadataGroupedByName = new HashMap<>();
    for (SearchMetadata searchMetadata : searchMetadataStore.listSync()) {
      if (!snapshotsToSearch.containsKey(searchMetadata.snapshotName)) {
        continue;
      }

      if (Boolean.getBoolean(ASTRA_ENABLE_QUERY_GATING_FLAG) && !searchMetadata.isSearchable()) {
        LOG.info(
            "Skipping searching search metadata={} because it's not searchable!",
            searchMetadata.name);
        continue;
      }

      String rawSnapshotName = AstraDistributedQueryService.getRawSnapshotName(searchMetadata);
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
            .startScopedSpan("AstraDistributedQueryService.findPartitionsToQuery");

    List<DatasetPartitionMetadata> partitions =
        DatasetPartitionMetadata.findPartitionsToQuery(
            datasetMetadataStore, queryStartTimeEpochMs, queryEndTimeEpochMs, dataset);
    findPartitionsToQuerySpan.finish();

    // find all snapshots that match time window and partition
    ScopedSpan snapshotsToSearchSpan =
        Tracing.currentTracer().startScopedSpan("AstraDistributedQueryService.snapshotsToSearch");
    Map<String, SnapshotMetadata> snapshotsToSearch = new HashMap<>();
    for (SnapshotMetadata snapshotMetadata : snapshotMetadataStore.listSync()) {
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

  private AstraServiceGrpc.AstraServiceFutureStub getStub(String url) {
    if (stubs.get(url) != null) {
      return stubs.get(url);
    } else {
      LOG.warn(
          "snapshot {} is not cached. ZK listener on searchMetadataStore should have cached the stub. Will attempt to get uncached, which will be slow.",
          url);
      return getAstraServiceGrpcClient(url);
    }
  }

  private String getTimeWindowTag(long requestedDataHours) {
    if (requestedDataHours < 1) {
      return "<1hr";
    } else if (requestedDataHours <= 24) {
      return "1-24hrs";
    } else if (requestedDataHours <= 72) {
      return "24-72hrs";
    } else {
      return "72hrs+";
    }
  }

  /**
   * Records the success ratio of a batch snapshot operation.
   *
   * @param dataWindowHours The duration of the data window requested for the batch.
   * @param snapshotsRequestedInBatch The total number of snapshots requested in this batch.
   * @param snapshotsFulfilledInBatch The number of snapshots successfully fulfilled in this batch.
   */
  public void recordBatchSnapshotOperation(
      long dataWindowHours, long snapshotsRequestedInBatch, long snapshotsFulfilledInBatch) {

    if (snapshotsRequestedInBatch <= 0) {
      // Avoid division by zero or meaningless ratios for empty requests
      return;
    }

    double batchSuccessRatio = (double) snapshotsFulfilledInBatch / snapshotsRequestedInBatch;
    String timeWindowTag = getTimeWindowTag(dataWindowHours);

    DistributionSummary.builder(SNAPSHOT_BATCH_SUCCESS_RATIO)
        .description("Distribution of success ratios for batch snapshot operations (0.0 to 1.0)")
        .tags(TIME_WINDOW_TAG, timeWindowTag)
        .register(meterRegistry)
        .record(batchSuccessRatio);
  }

  public void recordQueryDuration(long requestedDataHours, long requestDuration) {
    String timeWindowTag = getTimeWindowTag(requestedDataHours);

    Timer.builder(DISTRIBUTED_QUERY_DURATION_SECONDS)
        .description("Histogram of query duration.")
        .serviceLevelObjectives(
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            Duration.ofSeconds(10),
            Duration.ofSeconds(30),
            Duration.ofSeconds(60))
        .tag(TIME_WINDOW_TAG, timeWindowTag)
        .register(meterRegistry)
        .record(requestDuration, TimeUnit.MILLISECONDS);
  }

  private List<SearchResult<LogMessage>> distributedSearch(
      final AstraSearch.SearchRequest distribSearchReq) {
    LOG.debug("Starting distributed search for request: {}", distribSearchReq);
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("AstraDistributedQueryService.distributedSearch");
    long requestedDataHours =
        Duration.ofMillis(
                distribSearchReq.getEndTimeEpochMs() - distribSearchReq.getStartTimeEpochMs())
            .toHours();
    long startTime = Instant.now().toEpochMilli();

    Map<String, SnapshotMetadata> snapshotsMatchingQuery =
        getMatchingSnapshots(
            snapshotMetadataStore,
            datasetMetadataStore,
            distribSearchReq.getStartTimeEpochMs(),
            distribSearchReq.getEndTimeEpochMs(),
            distribSearchReq.getDataset());

    // for each matching snapshot, we find the search metadata nodes that we can potentially query
    Map<String, List<SearchMetadata>> searchMetadataNodesMatchingQuery =
        getMatchingSearchMetadata(searchMetadataStore, snapshotsMatchingQuery);

    // from the list of search metadata nodes per snapshot, pick one. Additionally map it to the
    // underlying URL to query
    Map<String, List<String>> nodesAndSnapshotsToQuery =
        getNodesAndSnapshotsToQuery(searchMetadataNodesMatchingQuery);

    span.tag("queryServerCount", String.valueOf(nodesAndSnapshotsToQuery.size()));

    CurrentTraceContext currentTraceContext = Tracing.current().currentTraceContext();
    AtomicLong totalRequests = new AtomicLong();
    try {
      try (var scope = new StructuredTaskScope<SearchResult<LogMessage>>()) {
        List<StructuredTaskScope.Subtask<SearchResult<LogMessage>>> searchSubtasks =
            nodesAndSnapshotsToQuery.entrySet().stream()
                .map(
                    (searchNode) ->
                        scope.fork(
                            currentTraceContext.wrap(
                                () -> {
                                  AstraServiceGrpc.AstraServiceFutureStub stub =
                                      getStub(searchNode.getKey());

                                  if (stub == null) {
                                    // TODO: insert a failed result in the results object that we
                                    // return from this method
                                    return null;
                                  }

                                  AstraSearch.SearchRequest localSearchReq =
                                      distribSearchReq.toBuilder()
                                          .addAllChunkIds(searchNode.getValue())
                                          .build();
                                  SearchResult<LogMessage> temp =
                                      SearchResultUtils.fromSearchResultProtoOrEmpty(
                                          stub.withDeadlineAfter(
                                                  defaultQueryTimeout.toMillis(),
                                                  TimeUnit.MILLISECONDS)
                                              .withInterceptors(
                                                  GrpcTracing.newBuilder(Tracing.current())
                                                      .build()
                                                      .newClientInterceptor())
                                              .search(localSearchReq)
                                              .get());
                                  totalRequests.addAndGet(temp.totalSnapshots);
                                  return temp;
                                })))
                .toList();

        try {
          scope.joinUntil(Instant.now().plusSeconds(defaultQueryTimeout.toSeconds()));
        } catch (TimeoutException timeoutException) {
          scope.shutdown();
          scope.join();
        }

        List<SearchResult<LogMessage>> response = new ArrayList(searchSubtasks.size());
        for (StructuredTaskScope.Subtask<SearchResult<LogMessage>> searchResult : searchSubtasks) {
          try {
            if (searchResult.state().equals(StructuredTaskScope.Subtask.State.SUCCESS)) {
              response.add(searchResult.get() == null ? SearchResult.error() : searchResult.get());
            } else {
              response.add(SearchResult.error());
              LOG.warn("Error fetching part of search result {}", searchResult);
            }
          } catch (Exception e) {
            LOG.error("Error fetching search result", e);
            response.add(SearchResult.error());
          }
        }
        return response;
      }
    } catch (Exception e) {
      LOG.error("Search failed with ", e);
      span.error(e);
      failedQueryCount.increment();
      return List.of(SearchResult.empty());
    } finally {
      recordQueryDuration(requestedDataHours, Instant.now().toEpochMilli() - startTime);
      recordBatchSnapshotOperation(
          requestedDataHours, snapshotsMatchingQuery.size(), totalRequests.get());
      if (snapshotsMatchingQuery.size() == totalRequests.get()) {
        successfulQueryCount.increment();
      } else {
        incompleteQueryCount.increment();
      }
      span.finish();
    }
  }

  @Override
  public AstraSearch.SearchResult doSearch(final AstraSearch.SearchRequest request) {
    try {
      List<SearchResult<LogMessage>> searchResults = distributedSearch(request);
      SearchResult<LogMessage> aggregatedResult =
          ((SearchResultAggregator<LogMessage>)
                  new SearchResultAggregatorImpl<>(SearchResultUtils.fromSearchRequest(request)))
              .aggregate(searchResults, true);

      // We report a query with more than 0% of requested nodes, but less than 2% as a tolerable
      // response. Anything over 2% is considered an unacceptable.
      if (aggregatedResult.totalNodes == 0 || aggregatedResult.failedNodes == 0) {
        distributedQueryApdexSatisfied.increment();
      } else if (((double) aggregatedResult.failedNodes / (double) aggregatedResult.totalNodes)
          <= 0.02) {
        distributedQueryApdexTolerating.increment();
      } else {
        distributedQueryApdexFrustrated.increment();
      }

      distributedQueryTotalSnapshots.increment(aggregatedResult.totalSnapshots);
      distributedQuerySnapshotsWithReplicas.increment(aggregatedResult.snapshotsWithReplicas);
      if (aggregatedResult.totalSnapshots != aggregatedResult.snapshotsWithReplicas) {}

      LOG.debug("aggregatedResult={}", aggregatedResult);
      return SearchResultUtils.toSearchResultProto(aggregatedResult);
    } catch (Exception e) {
      LOG.error("Distributed search failed", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public AstraSearch.SchemaResult getSchema(AstraSearch.SchemaRequest distribSchemaReq) {
    // todo - this shares a significant amount of code with the distributed search request
    //  and would benefit from refactoring the current "distributedSearch" abstraction to support
    //  different types of requests

    LOG.debug("Starting distributed search for schema request: {}", distribSchemaReq);
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("AstraDistributedQueryService.distributedSchema");

    Map<String, SnapshotMetadata> snapshotsMatchingQuery =
        getMatchingSnapshots(
            snapshotMetadataStore,
            datasetMetadataStore,
            distribSchemaReq.getStartTimeEpochMs(),
            distribSchemaReq.getEndTimeEpochMs(),
            distribSchemaReq.getDataset());

    // for each matching snapshot, we find the search metadata nodes that we can potentially query
    Map<String, List<SearchMetadata>> searchMetadataNodesMatchingQuery =
        getMatchingSearchMetadata(searchMetadataStore, snapshotsMatchingQuery);

    // from the list of search metadata nodes per snapshot, pick one. Additionally map it to the
    // underlying URL to query
    Map<String, List<String>> nodesAndSnapshotsToQuery =
        getNodesAndSnapshotsToQuery(searchMetadataNodesMatchingQuery);

    CurrentTraceContext currentTraceContext = Tracing.current().currentTraceContext();
    try {
      try (var scope = new StructuredTaskScope<AstraSearch.SchemaResult>()) {
        List<StructuredTaskScope.Subtask<AstraSearch.SchemaResult>> searchSubtasks =
            nodesAndSnapshotsToQuery.entrySet().stream()
                .limit(LIMIT_SCHEMA_NODES_TO_QUERY)
                .map(
                    (searchNode) ->
                        scope.fork(
                            currentTraceContext.wrap(
                                () -> {
                                  AstraServiceGrpc.AstraServiceFutureStub stub =
                                      getStub(searchNode.getKey());

                                  if (stub == null) {
                                    return null;
                                  }

                                  AstraSearch.SchemaRequest localSearchReq =
                                      distribSchemaReq.toBuilder()
                                          .addAllChunkIds(searchNode.getValue())
                                          .build();

                                  return stub.withDeadlineAfter(
                                          SCHEMA_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                                      .withInterceptors(
                                          GrpcTracing.newBuilder(Tracing.current())
                                              .build()
                                              .newClientInterceptor())
                                      .schema(localSearchReq)
                                      .get();
                                })))
                .toList();

        try {
          scope.joinUntil(Instant.now().plusMillis(SCHEMA_TIMEOUT_MS));
        } catch (TimeoutException timeoutException) {
          scope.shutdown();
          scope.join();
        }

        AstraSearch.SchemaResult.Builder schemaBuilder = AstraSearch.SchemaResult.newBuilder();
        for (StructuredTaskScope.Subtask<AstraSearch.SchemaResult> schemaResult : searchSubtasks) {
          try {
            if (schemaResult.state().equals(StructuredTaskScope.Subtask.State.SUCCESS)) {
              if (schemaResult.get() != null) {
                schemaBuilder.putAllFieldDefinition(schemaResult.get().getFieldDefinitionMap());
              } else {
                LOG.error("Schema result was unexpectedly null {}", schemaResult);
              }
            } else {
              LOG.error("Schema query result state was not success {}", schemaResult);
            }
          } catch (Exception e) {
            LOG.error("Error fetching search result", e);
          }
        }
        return schemaBuilder.build();
      }
    } catch (Exception e) {
      LOG.error("Schema failed with ", e);
      span.error(e);
      return AstraSearch.SchemaResult.newBuilder().build();
    } finally {
      span.finish();
    }
  }

  @Override
  public void close() {
    this.searchMetadataStore.removeListener(searchMetadataListener);
  }
}
