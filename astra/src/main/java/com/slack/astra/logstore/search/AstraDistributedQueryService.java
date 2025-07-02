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
  public static final String ASTRA_NEXT_GEN_DISTRIBUTED_QUERIES =
      "astra.enableNextGenDistributedQueries";

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

  @Override
  public AstraSearch.SearchResult doSearch(final AstraSearch.SearchRequest request) {
    // TODO FOR KYLE: Should this use a query param? It'd require a larger refactor, but it could
    // allow
    // for more fine grained control
    boolean shouldUseNewDistributedSearch = Boolean.getBoolean(ASTRA_NEXT_GEN_DISTRIBUTED_QUERIES);
    if (shouldUseNewDistributedSearch) {
      return this.newDoSearch(request);
    }
    return this.oldDosSearch(request);
  }

  private AstraSearch.SearchResult newDoSearch(final AstraSearch.SearchRequest request) {
    /*
     1. Get the number of snapshots requested (M)
     2. Get list of nodes the snapshots is on (N)
     3. Sort the list into replicas (R)
     4. Query X of those nodes (where X is configurable and <= N) from random replicas -- TODO FOR KYLE: Should we only query so many snapshots at once? Or only so many nodes at once?
     5. Sort through the responses and separate those that succeeded from those that failed
       5a. For those that failed, retry them on a different relica
       5b. For those that succeeded, run an preaggregation on the results to reduce size in memory -- TODO FOR KYLE: Is this safe to do? I imagine it should be, but I am a bit concerned
       5c. Keep doing this loop until done or timed out
     6. Run the final aggregation
     7. Return the result

     TASKS:
     * newDistributedSearch needs to be less abstract and just run the search method on the nodes/snapshots it's given
       * Perhaps we just need a "runRequest" method? Then we can shove the schema code in there as well?
     * newDoSearch needs to do the heavy lifting and contain the main loop (search, aggregate, repeat on remaining)
     * Add monitoring for all of this. Particular interests are on:
       * Success rate (i.e. all success vs all failures)
       * Partial rate
       * Speed
    */

    LOG.debug("Starting new distributed search for request: {}", request);
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("AstraDistributedQueryService.newDoSearch");

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

    // The list of snapshots we still need to query
    List<String> remainingSnapshots = new ArrayList<>(snapshotToSearchMetadata.keySet());

    // TODO FOR KYLE: MAKE THIS CONFIGURABLE ... or maybe it should just be the maxRequestSize
    // that's configurable?
    int maxQueueSize = 20;

    // Fill the list with what snapshots are currently being processed
    List<String> currentSnapshots = new ArrayList<>();
    for (String snapshotName : remainingSnapshots) {
      if (currentSnapshots.size() <= maxQueueSize) {
        currentSnapshots.add(snapshotName);

        // TODO FOR KYLE: IS THIS EFFICIENT? FEELS NOT EFFICIENT
        remainingSnapshots.remove(snapshotName);
      }
    }

    // The list for continuous aggregations
    List<SearchResult<LogMessage>> partialAggregatedResult = new ArrayList<>();

    // Snapshots that are FULLY failed. As in, we have either timed out the overall request, or
    // tried all of the nodes
    // and they all have timed out, or the request had an error in it (e.g. a parsing error)
    List<String> failedSnapshots = new ArrayList<>();

    // The list of snapshots that #MadeIt
    List<String> successfulSnapshots = new ArrayList<>();

    // Iterate through the list of currently active snapshots
    // TODO FOR KYLE: This should also stop when we hit some kind of timeout, yeah? Like there's one
    // timeout for all the requests to the nodes,
    //  then there's one timeout for the query as a whole, so that we can still try to aggregate and
    // return a partial
    while (!currentSnapshots.isEmpty()) {

      // Build up the nodes to query
      // We need this to be Search Metadata URL -> A List Of Snapshot Names (e.g. "host.local:8888
      // -> ["foo", "bar", "baz"]")
      // so tha twe can batch up everything to be a single request to the given host
      Map<String, List<String>> searchMetadataURLToSnapshotNames = new HashMap<>();

      for (String snapshot : currentSnapshots) {
        List<SearchMetadata> searchMetadataForSnapshot = snapshotToSearchMetadata.get(snapshot);

        // If there are no nodes left to try, then it's a failure
        if (searchMetadataForSnapshot.isEmpty()) {
          // TODO FOR KYLE: Do something here?
          System.out.println("FAILURE");
          failedSnapshots.add(snapshot);
          currentSnapshots.remove(snapshot);
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

        // Update the HashMap of Snapshot -> SearchMetadata Nodes To Query with the current state of
        // the world
        snapshotToSearchMetadata.put(snapshot, searchMetadataForSnapshot);
      }

      // Do the search and add all the results to the list of partial results
      // NOTE: This returns a mapping of node URL to the result. We should be able to use the node
      // URL key to get the snapshots we
      // were supposed to search
      // ALSO NOTE: There are two levels of failure here. We have failure on the node level (which
      // is why we have the mapping here at all)
      // and we have failure on the snapshot level (which is why we return a list of failed
      // snapshots for every search to every node)
      Map<String, SearchResult<LogMessage>> snapshotToResult =
          newDistributedSearch(request, searchMetadataURLToSnapshotNames);

      for (Map.Entry<String, SearchResult<LogMessage>> entry : snapshotToResult.entrySet()) {
        String nodeURL = entry.getKey();
        SearchResult<LogMessage> result = entry.getValue();

        // This is a user error and probably shouldn't be retried (e.g. an invalid query/aggregation
        // will fail on EVERY node)
        if (result.equals(SearchResult.soft_error())) {
          failedSnapshots.addAll(searchMetadataURLToSnapshotNames.get(nodeURL));
          currentSnapshots.removeAll(searchMetadataURLToSnapshotNames.get(nodeURL));
          // TODO FOR KYLE: IN FACT IF WE HAVE A PARSING/SCHMEA ERROR, SHOULD WE BAIL ENTIRELY AND
          // NOT DO ANY MORE BATCHES? BASICALLY A FAIL FAST APPROACH?
        }

        // Otherwise, if this is an Astra error, retry it on a different node
        else if (result.equals(SearchResult.error())) {
          // Do nothing so that this is retried
          System.out.println("We need to retry all snapshots for " + nodeURL);
        }

        // Other-otherwise, this was successful. Iterate through all the snapshots queried and mark
        // them as successful
        // or unsuccessful depending on the result
        else {
          List<String> allSnapshotsQueriedForNode = searchMetadataURLToSnapshotNames.get(nodeURL);

          // These are user errors and shouldn't be retried. We need to remove them from the list of
          // current snapshots and add them to the list of failed snapshots
          // TODO FOR KYLE: SHOULD WE BAIL EARLY HERE?
          for (String snapshot : result.softFailedChunkIds) {
            failedSnapshots.add(snapshot);
            currentSnapshots.remove(snapshot);
            allSnapshotsQueriedForNode.remove(snapshot);
          }

          // These are Astra errors and SHOULD be retried
          for (String snapshot : result.hardFailedChunkIds) {
            // Do nothing so that this is retried
            System.out.println("We need to retry " + snapshot);
            allSnapshotsQueriedForNode.remove(snapshot);
          }

          // Everything that we queried that didn't come up as a hard or soft failure, must have
          // succeeded. Mark it as successful, don't retry it, and add the result to the list
          for (String snapshot : allSnapshotsQueriedForNode) {
            successfulSnapshots.add(snapshot);
            currentSnapshots.remove(snapshot);
          }
        }

        // Always add the result, even if it's a failure. We still need it for aggregating and
        // counting crap
        partialAggregatedResult.add(result);
      }

      // Do a partial aggregation on the results from the last batch and the current ones
      SearchResult<LogMessage> aggregatedResult =
          ((SearchResultAggregator<LogMessage>)
                  new SearchResultAggregatorImpl<>(SearchResultUtils.fromSearchRequest(request)))
              .aggregate(partialAggregatedResult, false);

      // TODO FOR KYLE: IS THIS EFFICIENT?
      partialAggregatedResult.clear();
      partialAggregatedResult.add(aggregatedResult);

      // Refill the queue
      for (String snapshotName : remainingSnapshots) {
        if (currentSnapshots.size() <= maxQueueSize) {
          currentSnapshots.add(snapshotName);

          // TODO FOR KYLE: IS THIS EFFICIENT? FEELS NOT EFFICIENT
          remainingSnapshots.remove(snapshotName);
        }
      }
    }

    SearchResult<LogMessage> finalAggregatedResult =
        ((SearchResultAggregator<LogMessage>)
                new SearchResultAggregatorImpl<>(SearchResultUtils.fromSearchRequest(request)))
            .aggregate(partialAggregatedResult, true);

    // TODO FOR KYLE: ADD OTHER METRICS AND STUFF HERE
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
                                        // TODO: insert a failed result in the results object that
                                        // we
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
                                    }))))
                .toList();

        try {
          scope.joinUntil(Instant.now().plusSeconds(defaultQueryTimeout.toSeconds()));
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

          SearchResult<LogMessage> hardError = SearchResult.error();
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
      failedQueryCount.increment();
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
