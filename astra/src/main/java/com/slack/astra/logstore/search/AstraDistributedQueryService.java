package com.slack.astra.logstore.search;

import static com.slack.astra.chunk.ChunkInfo.containsDataInTimeRange;
import static com.slack.astra.logstore.search.AstraQueryFlags.ASTRA_NEXT_GEN_DISTRIBUTED_QUERIES;

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
import com.slack.astra.proto.config.AstraConfigs;
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
import java.util.Iterator;
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
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
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
  private static final String QUERY_PATH_TAG = "query_path";

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

  private final AstraConfigs.QueryServiceConfig queryServiceConfig;

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
      Duration defaultQueryTimeout,
      AstraConfigs.QueryServiceConfig queryServiceConfig) {
    this.queryServiceConfig = queryServiceConfig;
    this.searchMetadataStore = searchMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.datasetMetadataStore = datasetMetadataStore;
    this.defaultQueryTimeout = defaultQueryTimeout;
    searchMetadataTotalChangeCounter = meterRegistry.counter(SEARCH_METADATA_TOTAL_CHANGE_COUNTER);
    this.meterRegistry = meterRegistry;
    initializeMetrics();

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
  private void recordBatchSnapshotOperation(
      long dataWindowHours,
      long snapshotsRequestedInBatch,
      long snapshotsFulfilledInBatch,
      String queryPath) {

    if (snapshotsRequestedInBatch <= 0) {
      // Avoid division by zero or meaningless ratios for empty requests
      return;
    }

    double batchSuccessRatio = (double) snapshotsFulfilledInBatch / snapshotsRequestedInBatch;
    String timeWindowTag = getTimeWindowTag(dataWindowHours);

    DistributionSummary.builder(SNAPSHOT_BATCH_SUCCESS_RATIO)
        .description("Distribution of success ratios for batch snapshot operations (0.0 to 1.0)")
        .tags(TIME_WINDOW_TAG, timeWindowTag, QUERY_PATH_TAG, queryPath)
        .register(meterRegistry)
        .record(batchSuccessRatio);
  }

  // The first time a metric is collected, the value is thrown away. Initializing these to zero
  // allows the first
  // record to be meaningful.
  private void initializeMetrics() {
    for (String tag : List.of("old", "new")) {
      meterRegistry.counter(SEARCH_METADATA_TOTAL_CHANGE_COUNTER, QUERY_PATH_TAG, tag).increment(0);
      meterRegistry.counter(DISTRIBUTED_QUERY_APDEX_SATISFIED, QUERY_PATH_TAG, tag).increment(0);
      meterRegistry.counter(DISTRIBUTED_QUERY_APDEX_TOLERATING, QUERY_PATH_TAG, tag).increment(0);
      meterRegistry.counter(DISTRIBUTED_QUERY_APDEX_FRUSTRATED, QUERY_PATH_TAG, tag).increment(0);
      meterRegistry.counter(DISTRIBUTED_QUERY_TOTAL_SNAPSHOTS, QUERY_PATH_TAG, tag).increment(0);
      meterRegistry
          .counter(DISTRIBUTED_QUERY_SNAPSHOTS_WITH_REPLICAS, QUERY_PATH_TAG, tag)
          .increment(0);
      meterRegistry.counter(ASTRA_QUERIES_SUCCESSFUL_COUNT, QUERY_PATH_TAG, tag).increment(0);
      meterRegistry.counter(ASTRA_QUERIES_INCOMPLETE_COUNT, QUERY_PATH_TAG, tag).increment(0);
      meterRegistry.counter(ASTRA_QUERIES_FAILED_COUNT, QUERY_PATH_TAG, tag).increment(0);
    }
  }

  private void recordQueryDuration(
      long requestedDataHours, long requestDuration, String queryPath) {
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
        .tag(QUERY_PATH_TAG, queryPath)
        .register(meterRegistry)
        .record(requestDuration, TimeUnit.MILLISECONDS);
  }

  private void recordApdexMetrics(SearchResult<LogMessage> result, String queryPath) {
    if (result.totalNodes == 0 || result.failedNodes == 0) {
      meterRegistry
          .counter(DISTRIBUTED_QUERY_APDEX_SATISFIED, QUERY_PATH_TAG, queryPath)
          .increment();
    } else if (((double) result.failedNodes / (double) result.totalNodes) <= 0.02) {
      meterRegistry
          .counter(DISTRIBUTED_QUERY_APDEX_TOLERATING, QUERY_PATH_TAG, queryPath)
          .increment();
    } else {
      meterRegistry
          .counter(DISTRIBUTED_QUERY_APDEX_FRUSTRATED, QUERY_PATH_TAG, queryPath)
          .increment();
    }
  }

  private void recordTotalSnapshots(SearchResult<LogMessage> result, String queryPath) {
    meterRegistry
        .counter(DISTRIBUTED_QUERY_TOTAL_SNAPSHOTS, QUERY_PATH_TAG, queryPath)
        .increment(result.totalSnapshots);
    meterRegistry
        .counter(DISTRIBUTED_QUERY_SNAPSHOTS_WITH_REPLICAS, QUERY_PATH_TAG, queryPath)
        .increment(result.snapshotsWithReplicas);
  }

  private void recordQuerySuccessMetrics(
      long totalSnapshotsRequested, long totalSnapshotsSuccessful, String queryPath) {
    // Record query completion status
    if (totalSnapshotsRequested == totalSnapshotsSuccessful) {
      meterRegistry.counter(ASTRA_QUERIES_SUCCESSFUL_COUNT, QUERY_PATH_TAG, queryPath).increment();
    } else if (totalSnapshotsSuccessful > 0) {
      meterRegistry.counter(ASTRA_QUERIES_INCOMPLETE_COUNT, QUERY_PATH_TAG, queryPath).increment();
    } else {
      meterRegistry.counter(ASTRA_QUERIES_FAILED_COUNT, QUERY_PATH_TAG, queryPath).increment();
    }
  }

  @Override
  public AstraSearch.SearchResult doSearch(final AstraSearch.SearchRequest request) {
    Pair<Boolean, String> useNewQuery =
        AstraQueryFlags.isQueryFlagEnabled(request.getQuery(), ASTRA_NEXT_GEN_DISTRIBUTED_QUERIES);
    if (useNewQuery.getKey()) {
      AstraSearch.SearchRequest newSearchRequest =
          AstraSearch.SearchRequest.newBuilder(request).setQuery(useNewQuery.getValue()).build();
      try {
        return this.newDoSearch(newSearchRequest);
      } catch (Exception e) {
        LOG.error("Something went wrong:", e);
        throw new RuntimeException(e);
      }
    }
    return this.oldDosSearch(request);
  }

  private AstraSearch.SearchResult newDoSearch(final AstraSearch.SearchRequest request)
      throws InterruptedException {
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("AstraDistributedQueryService.newDoSearch");
    long requestedDataHours =
        Duration.ofMillis(request.getEndTimeEpochMs() - request.getStartTimeEpochMs()).toHours();
    long startTime = Instant.now().toEpochMilli();
    int maxQueueSize = this.queryServiceConfig.getMaxSnapshotsQueriedPerBatch();

    Map<String, SnapshotMetadata> snapshotsMatchingQuery =
        getMatchingSnapshots(
            snapshotMetadataStore,
            datasetMetadataStore,
            request.getStartTimeEpochMs(),
            request.getEndTimeEpochMs(),
            request.getDataset());

    // for each matching snapshot, we find the search metadata nodes that we can potentially query
    Map<String, List<SearchMetadata>> snapshotToSearchMetadata =
        getMatchingSearchMetadata(searchMetadataStore, snapshotsMatchingQuery);

    // The list of snapshots we still need to query. Using a hashmap here because we need to
    // do quite a few removal calls and that O(1) deletion time is going to be useful
    HashMap<String, Boolean> remainingSnapshots = new HashMap<>(snapshotToSearchMetadata.size());
    for (String snapshotName : snapshotToSearchMetadata.keySet()) {
      remainingSnapshots.put(snapshotName, true);
    }

    // Fill the list with what snapshots are currently being processed
    List<String> currentSnapshots = new ArrayList<>();
    Iterator<String> remainingSnapshotsIterator = remainingSnapshots.keySet().iterator();
    while (remainingSnapshotsIterator.hasNext()) {
      String snapshotName = remainingSnapshotsIterator.next();
      if (currentSnapshots.size() <= maxQueueSize) {
        currentSnapshots.add(snapshotName);
        remainingSnapshotsIterator.remove();
      }
    }

    // The list for continuous aggregations
    List<SearchResult<LogMessage>> partialAggregatedResult = new ArrayList<>();

    // Snapshots that are FULLY failed. As in, we have either timed out the overall request, or
    // tried all the nodes and they all have timed out, or the request had an error in it
    // (e.g. a parsing error)
    List<String> failedSnapshots = new ArrayList<>();

    // The list of snapshots that were successful
    List<String> successfulSnapshots = new ArrayList<>();

    try (var scope = new StructuredTaskScope<List<SearchResult<LogMessage>>>()) {
      StructuredTaskScope.Subtask<List<SearchResult<LogMessage>>> searchSubtasks =
          scope.fork(
              () -> {
                while (!currentSnapshots.isEmpty()) {

                  // Build up the nodes to query. We need this to be
                  // Search Metadata URL -> A List Of Snapshot Names
                  // (e.g. "host.local:8888 -> ["foo", "bar", "baz"]")
                  // so that we can batch up everything to be a single request to the given host
                  Map<String, List<String>> searchMetadataURLToSnapshotNames = new HashMap<>();
                  Iterator<String> currentSnapshotIterator = currentSnapshots.iterator();
                  while (currentSnapshotIterator.hasNext()) {
                    String snapshot = currentSnapshotIterator.next();
                    List<SearchMetadata> searchMetadataForSnapshot =
                        snapshotToSearchMetadata.get(snapshot);

                    // If there are no nodes left to try, then it's a failure
                    if (searchMetadataForSnapshot.isEmpty()) {
                      LOG.error("All nodes for snapshot '{}' have failed. Skipping.", snapshot);
                      partialAggregatedResult.add(SearchResult.empty());
                      failedSnapshots.add(snapshot);
                      currentSnapshotIterator.remove();
                      continue;
                    }

                    // Otherwise, get the first one from the list of possibilities and try that
                    SearchMetadata nodeToQuery = searchMetadataForSnapshot.removeFirst();

                    // Maintain the mapping of Search Metadata URL -> A List Of Snapshot Names
                    if (searchMetadataURLToSnapshotNames.containsKey(nodeToQuery.url)) {
                      searchMetadataURLToSnapshotNames.get(nodeToQuery.url).add(snapshot);
                    } else {
                      List<String> snapshotNames = new ArrayList<>();
                      snapshotNames.add(snapshot);
                      searchMetadataURLToSnapshotNames.put(nodeToQuery.url, snapshotNames);
                    }

                    // Update the HashMap of Snapshot -> SearchMetadata Nodes To Query with the
                    // current state of the world
                    snapshotToSearchMetadata.put(snapshot, searchMetadataForSnapshot);
                  }

                  // Do the search and add all the results to the list of partial results
                  // NOTE: This returns a mapping of node URL to the result. We should be able to
                  // use the node URL key to get the snapshots we were supposed to search
                  // ALSO NOTE: There are two levels of failure here. We have failure on the node
                  // level (which is why we have the mapping here at all) and we have failure on
                  // the snapshot level (which is why we return a list of failed snapshots for
                  // every search to every node)
                  Map<String, SearchResult<LogMessage>> snapshotToResult =
                      newDistributedSearch(request, searchMetadataURLToSnapshotNames);

                  for (Map.Entry<String, SearchResult<LogMessage>> entry :
                      snapshotToResult.entrySet()) {
                    String nodeURL = entry.getKey();
                    SearchResult<LogMessage> result = entry.getValue();

                    if (result.equals(SearchResult.soft_error())) {
                      LOG.debug("There was a soft error for the entire request!");
                    } else if (result.equals(SearchResult.error())) {
                      LOG.debug(
                          "There was a hard error for the entire request! We need to retry all snapshots for {}",
                          nodeURL);
                    } else {
                      List<String> allSnapshotsQueriedForNode =
                          searchMetadataURLToSnapshotNames.get(nodeURL);

                      // These are user errors and in MOST cases don't need to be retried. However,
                      // there are a few cases where we will only get a user error on a specific
                      // node and not on the other (schema issues being the most likely cause).
                      // For now, retry it like any other failure, but we should revisit
                      // this as a possible optimization in the future
                      for (String snapshot : result.softFailedChunkIds) {
                        allSnapshotsQueriedForNode.remove(snapshot);
                      }

                      for (String snapshot : result.hardFailedChunkIds) {
                        LOG.debug("Retrying snapshot: '{}'", snapshot);
                        allSnapshotsQueriedForNode.remove(snapshot);
                      }

                      for (String snapshot : allSnapshotsQueriedForNode) {
                        successfulSnapshots.add(snapshot);
                        currentSnapshots.remove(snapshot);
                      }
                    }

                    partialAggregatedResult.add(result);
                  }

                  // Do a partial aggregation on the results from the last batch and the current
                  // ones
                  SearchResult<LogMessage> aggregatedResult =
                      new SearchResultAggregatorImpl<>(SearchResultUtils.fromSearchRequest(request))
                          .aggregate(partialAggregatedResult, false);

                  // Since we just did the partial aggregation on a batch, we need to clear out the
                  // previous results before adding the newest partial aggregation
                  partialAggregatedResult.clear();
                  partialAggregatedResult.add(aggregatedResult);

                  // Refill the queue
                  Iterator<String> otherRemainingSnapshotsIterator =
                      remainingSnapshots.keySet().iterator();
                  while (otherRemainingSnapshotsIterator.hasNext()) {
                    String snapshotName = otherRemainingSnapshotsIterator.next();
                    if (currentSnapshots.size() <= maxQueueSize) {
                      currentSnapshots.add(snapshotName);
                      otherRemainingSnapshotsIterator.remove();
                    }
                  }
                }
                return partialAggregatedResult;
              });
      try {
        scope.joinUntil(Instant.now().plusSeconds(defaultQueryTimeout.toSeconds()));
      } catch (Exception e) {
        scope.shutdown();
        scope.join();
      }

      if (!searchSubtasks.state().equals(StructuredTaskScope.Subtask.State.SUCCESS)) {
        LOG.error(
            "Search was not able to be completed successfully! State was {}",
            searchSubtasks.state());
        throw new IllegalStateException("Search was not able to be completed successfully!");
      }
    }

    SearchResult<LogMessage> finalAggregatedResult =
        new SearchResultAggregatorImpl<>(SearchResultUtils.fromSearchRequest(request))
            .aggregate(partialAggregatedResult, true);

    // Record metrics for new query path
    long endTime = Instant.now().toEpochMilli();
    recordQueryDuration(requestedDataHours, endTime - startTime, "new");

    int totalSnapshotsRequested = snapshotsMatchingQuery.size();
    int totalSnapshotsSuccessful = successfulSnapshots.size();
    recordBatchSnapshotOperation(
        requestedDataHours, totalSnapshotsRequested, totalSnapshotsSuccessful, "new");

    recordApdexMetrics(finalAggregatedResult, "new");
    recordTotalSnapshots(finalAggregatedResult, "new");
    recordQuerySuccessMetrics(totalSnapshotsRequested, totalSnapshotsSuccessful, "new");

    span.finish();
    return SearchResultUtils.toSearchResultProto(finalAggregatedResult);
  }

  private Map<String, SearchResult<LogMessage>> newDistributedSearch(
      final AstraSearch.SearchRequest distribSearchReq,
      Map<String, List<String>> searchMetadataURLToSnapshotNames) {
    ScopedSpan span =
        Tracing.currentTracer()
            .startScopedSpan("AstraDistributedQueryService.newDistributedSearch");

    span.tag("nodeQueryCount", String.valueOf(searchMetadataURLToSnapshotNames.size()));

    CurrentTraceContext currentTraceContext = Tracing.current().currentTraceContext();
    AtomicLong totalRequests = new AtomicLong();

    try {
      try (var scope = new StructuredTaskScope<SearchResult<LogMessage>>()) {
        List<Pair<String, StructuredTaskScope.Subtask<SearchResult<LogMessage>>>> searchSubtasks =
            searchMetadataURLToSnapshotNames.entrySet().stream()
                .map(
                    (searchNode) ->
                        Pair.of(
                            searchNode.getKey(),
                            scope.fork(
                                currentTraceContext.wrap(
                                    () -> {
                                      AstraServiceGrpc.AstraServiceFutureStub stub =
                                          getStub(searchNode.getKey());

                                      if (stub == null) {
                                        return null;
                                      }

                                      AstraSearch.SearchRequest localSearchReq =
                                          distribSearchReq.toBuilder()
                                              .addAllChunkIds(searchNode.getValue())
                                              .build();

                                      SearchResult<LogMessage> temp =
                                          SearchResultUtils.fromSearchResultProtoOrEmpty(
                                              stub.withDeadlineAfter(
                                                      queryServiceConfig
                                                          .getPerBatchQueryTimeoutMs(),
                                                      TimeUnit.MILLISECONDS)
                                                  .withInterceptors(
                                                      GrpcTracing.newBuilder(Tracing.current())
                                                          .build()
                                                          .newClientInterceptor())
                                                  .search(localSearchReq)
                                                  .get());
                                      totalRequests.addAndGet(temp.totalSnapshots);
                                      return temp;
                                    }))))
                .toList();

        try {
          scope.joinUntil(
              Instant.now()
                  .plusSeconds(
                      Duration.ofMillis(queryServiceConfig.getPerBatchQueryTimeoutMs())
                          .toSeconds()));
        } catch (TimeoutException timeoutException) {
          scope.shutdown();
          scope.join();
        }

        Map<String, SearchResult<LogMessage>> nodeURLToResponse =
            new HashMap<>(searchSubtasks.size());

        for (Pair<String, StructuredTaskScope.Subtask<SearchResult<LogMessage>>>
            nodeToSearchResult : searchSubtasks) {
          String nodeURL = nodeToSearchResult.getKey();
          StructuredTaskScope.Subtask<SearchResult<LogMessage>> searchResult =
              nodeToSearchResult.getValue();

          SearchResult<LogMessage> hardError = SearchResult.error().clone();
          hardError.hardFailedChunkIds.addAll(searchMetadataURLToSnapshotNames.get(nodeURL));

          try {
            if (searchResult.state().equals(StructuredTaskScope.Subtask.State.SUCCESS)) {
              nodeURLToResponse.put(
                  nodeURL, searchResult.get() == null ? hardError : searchResult.get());
            } else {
              nodeURLToResponse.put(nodeURL, hardError);
              LOG.warn("Error fetching part of search result {}", searchResult);
            }
          } catch (Exception e) {
            LOG.error("Error fetching search result", e);
            nodeURLToResponse.put(nodeURL, hardError);
          }
        }
        return nodeURLToResponse;
      }
    } catch (Exception e) {
      LOG.error("Search failed with ", e);
      span.error(e);
      meterRegistry.counter(ASTRA_QUERIES_FAILED_COUNT, QUERY_PATH_TAG, "new").increment();
      return searchMetadataURLToSnapshotNames.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, _ -> SearchResult.error()));
    }
  }

  private AstraSearch.SearchResult oldDosSearch(final AstraSearch.SearchRequest request) {
    try {
      List<SearchResult<LogMessage>> searchResults = oldDistributedSearch(request);
      SearchResult<LogMessage> aggregatedResult =
          ((SearchResultAggregator<LogMessage>)
                  new SearchResultAggregatorImpl<>(SearchResultUtils.fromSearchRequest(request)))
              .aggregate(searchResults, true);

      // We report a query with more than 0% of requested nodes, but less than 2% as a tolerable
      // response. Anything over 2% is considered an unacceptable.
      recordApdexMetrics(aggregatedResult, "old");
      recordTotalSnapshots(aggregatedResult, "old");
      LOG.debug("aggregatedResult={}", aggregatedResult);
      return SearchResultUtils.toSearchResultProto(aggregatedResult);
    } catch (Exception e) {
      LOG.error("Distributed search failed", e);
      throw new RuntimeException(e);
    }
  }

  private List<SearchResult<LogMessage>> oldDistributedSearch(
      final AstraSearch.SearchRequest distribSearchReq) {
    LOG.debug("Starting old distributed search for request: {}", distribSearchReq);
    ScopedSpan span =
        Tracing.currentTracer()
            .startScopedSpan("AstraDistributedQueryService.oldDistributedSearch");
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

                                  LOG.info(
                                      "OLD REQUEST SENDING: {} TO {}",
                                      localSearchReq,
                                      searchNode.getKey());
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
      meterRegistry.counter(ASTRA_QUERIES_FAILED_COUNT, QUERY_PATH_TAG, "old").increment();
      return List.of(SearchResult.empty());
    } finally {
      recordQueryDuration(requestedDataHours, Instant.now().toEpochMilli() - startTime, "old");
      recordBatchSnapshotOperation(
          requestedDataHours, snapshotsMatchingQuery.size(), totalRequests.get(), "old");
      recordQuerySuccessMetrics(snapshotsMatchingQuery.size(), totalRequests.get(), "old");
      span.finish();
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
