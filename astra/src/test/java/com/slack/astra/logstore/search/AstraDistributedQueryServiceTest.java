package com.slack.astra.logstore.search;

import static com.slack.astra.chunk.ChunkInfo.toSnapshotMetadata;
import static com.slack.astra.chunk.ReadWriteChunk.LIVE_SNAPSHOT_PREFIX;
import static com.slack.astra.chunk.ReadWriteChunk.toSearchMetadata;
import static com.slack.astra.logstore.search.AstraDistributedQueryService.getMatchingSearchMetadata;
import static com.slack.astra.logstore.search.AstraDistributedQueryService.getMatchingSnapshots;
import static com.slack.astra.logstore.search.AstraDistributedQueryService.getNodesAndSnapshotsToQuery;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import com.slack.astra.chunk.ChunkInfo;
import com.slack.astra.chunk.SearchContext;
import com.slack.astra.logstore.LogWireMessage;
import com.slack.astra.logstore.opensearch.OpenSearchInternalAggregation;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.proto.service.AstraServiceGrpc;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import com.slack.astra.util.JsonUtil;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AstraDistributedQueryServiceTest {

  private SimpleMeterRegistry metricsRegistry;

  private AsyncCuratorFramework curatorFramework;
  private SearchMetadataStore searchMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private DatasetMetadataStore datasetMetadataStore;

  private TestingServer testZKServer;
  private static EtcdCluster etcdCluster;
  private Client etcdClient;
  private SearchContext indexer1SearchContext;
  private SearchContext indexer2SearchContext;
  private SearchContext cache1SearchContext;
  private SearchContext cache2SearchContext;
  private SearchContext cache3SearchContext;
  private SearchContext cache4SearchContext;

  private AstraConfigs.QueryServiceConfig queryServiceConfig;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();

    queryServiceConfig =
        AstraConfigs.QueryServiceConfig.newBuilder()
            .setMaxSnapshotsQueriedPerBatch(20)
            .setPerBatchQueryTimeoutMs(5000)
            .setDefaultQueryTimeoutMs(10_000)
            .build();

    metricsRegistry = new SimpleMeterRegistry();
    testZKServer = new TestingServer();

    etcdCluster = TestEtcdClusterFactory.start();

    etcdClient =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream().map(Object::toString).toArray(String[]::new))
            .namespace(
                ByteSequence.from("distributedQuery", java.nio.charset.StandardCharsets.UTF_8))
            .build();

    // Metadata store
    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testZKServer.getConnectString())
            .setZkPathPrefix("distributedQuery")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .setZkCacheInitTimeoutMs(1000)
            .build();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .putStoreModes("DatasetMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("SnapshotMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("ReplicaMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("HpaMetricMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("SearchMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheSlotMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheNodeAssignmentStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes(
                "FieldRedactionMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("PreprocessorMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("RecoveryNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("RecoveryTaskMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .setZookeeperConfig(zkConfig)
            .setEtcdConfig(
                AstraConfigs.EtcdConfig.newBuilder()
                    .addAllEndpoints(
                        etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
                    .setConnectionTimeoutMs(5000)
                    .setKeepaliveTimeoutMs(3000)
                    .setOperationsMaxRetries(3)
                    .setOperationsTimeoutMs(3000)
                    .setRetryDelayMs(100)
                    .setNamespace("distributedQuery")
                    .setEnabled(true)
                    .setEphemeralNodeTtlMs(3000)
                    .setEphemeralNodeMaxRetries(3)
                    .build())
            .build();

    curatorFramework = spy(CuratorBuilder.build(metricsRegistry, zkConfig));

    snapshotMetadataStore =
        spy(
            new SnapshotMetadataStore(
                curatorFramework, etcdClient, metadataStoreConfig, metricsRegistry));
    searchMetadataStore =
        spy(
            new SearchMetadataStore(
                curatorFramework, etcdClient, metadataStoreConfig, metricsRegistry, true));
    datasetMetadataStore =
        new DatasetMetadataStore(
            curatorFramework, etcdClient, metadataStoreConfig, metricsRegistry, true);

    indexer1SearchContext = new SearchContext("indexer_host1", 10000);
    indexer2SearchContext = new SearchContext("indexer_host2", 10001);
    cache1SearchContext = new SearchContext("cache_host1", 20000);
    cache2SearchContext = new SearchContext("cache_host2", 20001);
    cache3SearchContext = new SearchContext("cache_host3", 20002);
    cache4SearchContext = new SearchContext("cache_host4", 20003);
  }

  @AfterEach
  public void tearDown() throws Exception {
    snapshotMetadataStore.close();
    searchMetadataStore.close();
    datasetMetadataStore.close();
    curatorFramework.unwrap().close();
    metricsRegistry.close();
    testZKServer.close();
    if (etcdClient != null) {
      etcdClient.close();
    }
  }

  private SearchMetadata registerTestSearchMetadata(
      SearchMetadataStore searchMetadataStore,
      SearchContext cacheSearchContext,
      String snapshotName) {
    SearchMetadata metadata =
        new SearchMetadata(
            SearchMetadata.generateSearchContextSnapshotId(
                snapshotName, cacheSearchContext.hostname),
            snapshotName,
            cacheSearchContext.toUrl(),
            true);
    searchMetadataStore.createSync(metadata);
    return metadata;
  }

  private SearchMetadata registerUnsearchableTestSearchMetadata(
      SearchMetadataStore searchMetadataStore,
      SearchContext cacheSearchContext,
      String snapshotName) {
    SearchMetadata metadata =
        new SearchMetadata(
            SearchMetadata.generateSearchContextSnapshotId(
                snapshotName, cacheSearchContext.hostname),
            snapshotName,
            cacheSearchContext.toUrl(),
            false);
    searchMetadataStore.createSync(metadata);
    return metadata;
  }

  @Test
  public void testGetSearchNodesToQueryWithUnsearchableNodeAndGatingEnabled() {
    // Set feature flag to enable query gating for this test
    System.setProperty(AstraDistributedQueryService.ASTRA_ENABLE_QUERY_GATING_FLAG, "true");

    try {
      String indexName = "testIndex";
      DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 500, List.of("1"));
      DatasetMetadata datasetMetadata =
          new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
      datasetMetadataStore.createSync(datasetMetadata);
      await()
          .until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

      Instant chunk1CreationTime = Instant.ofEpochMilli(100);
      Instant chunk1EndTime = Instant.ofEpochMilli(200);
      String snapshot1Name =
          createIndexerZKMetadata(chunk1CreationTime, chunk1EndTime, "1", indexer1SearchContext);

      assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size())
          .isEqualTo(2);
      assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(1);

      AtomicReference<Map<String, List<String>>> searchNodes = new AtomicReference<>();
      await()
          .until(
              () -> {
                searchNodes.set(
                    getSearchNodesToQuery(
                        snapshotMetadataStore,
                        searchMetadataStore,
                        datasetMetadataStore,
                        chunk1CreationTime.toEpochMilli(),
                        chunk1EndTime.toEpochMilli(),
                        indexName));
                return searchNodes.get().size() == 1;
              });

      assertThat(searchNodes.get().size()).isEqualTo(1);
      assertThat(searchNodes.get().keySet().iterator().next())
          .isEqualTo(indexer1SearchContext.toString());
      List<String> chunks = searchNodes.get().values().iterator().next();
      assertThat(chunks.size()).isEqualTo(1);
      Iterator<String> chunkIter = chunks.iterator();
      assertThat(chunkIter.next()).isEqualTo(snapshot1Name);

      // create cache node entry for search metadata also serving the snapshot
      registerUnsearchableTestSearchMetadata(
          searchMetadataStore, cache1SearchContext, snapshot1Name);
      await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 2);

      searchNodes.set(
          getSearchNodesToQuery(
              snapshotMetadataStore,
              searchMetadataStore,
              datasetMetadataStore,
              chunk1CreationTime.toEpochMilli(),
              chunk1EndTime.toEpochMilli(),
              indexName));

      // Confirm that the data we get back is only for the indexer since the cache node is
      // unsearchable
      assertThat(chunks.size()).isEqualTo(1);
      assertThat(searchNodes.get().keySet().iterator().next())
          .isEqualTo(indexer1SearchContext.toString());
      chunks = searchNodes.get().values().iterator().next();
      assertThat(chunks.size()).isEqualTo(1);
      chunkIter = chunks.iterator();
      assertThat(chunkIter.next()).isEqualTo(snapshot1Name);
    } finally {
      System.clearProperty(AstraDistributedQueryService.ASTRA_ENABLE_QUERY_GATING_FLAG);
    }
  }

  @Test
  public void testOneIndexer() {
    String indexName = "testIndex";
    Instant chunk1CreationTime = Instant.ofEpochMilli(100);
    Instant chunk1EndTime = Instant.ofEpochMilli(200);
    String chunk1Name =
        createIndexerZKMetadata(chunk1CreationTime, chunk1EndTime, "1", indexer1SearchContext);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 2);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 1);

    // we don't have any dataset metadata entry, so we shouldn't be able to find any snapshot
    Map<String, List<String>> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);

    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 300, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    // now we can find the snapshot
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);

    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());
    List<String> chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    assertThat(chunks.iterator().next()).isEqualTo(chunk1Name);

    // we can't find snapshot since the time window doesn't match snapshot
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1EndTime.toEpochMilli() + 1,
            chunk1EndTime.toEpochMilli() + 100,
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);

    // add another chunk on the same indexer and ensure we still find the node
    Instant chunk2CreationTime = Instant.ofEpochMilli(201);
    Instant chunk2EndTime = Instant.ofEpochMilli(300);
    String chunk2Name =
        createIndexerZKMetadata(chunk2CreationTime, chunk2EndTime, "1", indexer1SearchContext);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 4);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 2);
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 0, 300, indexName);
    assertThat(searchNodes.size()).isEqualTo(1);

    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());

    // if we request a time window that matches both chunks the searchable chunks will be 2
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 0, 250, indexName);
    assertThat(searchNodes.size()).isEqualTo(1);

    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());
    chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(2);
    Iterator<String> chunkIter = chunks.iterator();
    String result1 = chunkIter.next();
    String result2 = chunkIter.next();
    assertThat(result1.equals(chunk1Name) || result1.equals(chunk2Name)).isTrue();
    assertThat(result2.equals(chunk1Name) || result2.equals(chunk2Name)).isTrue();

    // request a time window that matches only 1 chunk
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 250, 300, indexName);
    assertThat(searchNodes.size()).isEqualTo(1);

    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());
    chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(chunk2Name);

    // re-add dataset metadata with a different time window that doesn't match any snapshot
    datasetMetadataStore.deleteAsync(datasetMetadata.name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 0);
    partition = new DatasetPartitionMetadata(1, 99, List.of("1"));
    datasetMetadata = new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    // we can't find snapshot since the time window doesn't match any dataset metadata
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);
  }

  @Test
  // snaphost1[100-200] -> hosted on indexer1 , cache1 , cache2
  // snapshot2[51-150] -> cache2
  // snapshot3[151-250] - cache1
  public void testOneIndexerTwoCacheNode() {
    String indexName = "testIndex";
    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 500, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    Instant chunk1CreationTime = Instant.ofEpochMilli(100);
    Instant chunk1EndTime = Instant.ofEpochMilli(200);
    String snapshot1Name =
        createIndexerZKMetadata(chunk1CreationTime, chunk1EndTime, "1", indexer1SearchContext);

    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isEqualTo(2);
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(1);

    AtomicReference<Map<String, List<String>>> searchNodes = new AtomicReference<>();
    await()
        .until(
            () -> {
              searchNodes.set(
                  getSearchNodesToQuery(
                      snapshotMetadataStore,
                      searchMetadataStore,
                      datasetMetadataStore,
                      chunk1CreationTime.toEpochMilli(),
                      chunk1EndTime.toEpochMilli(),
                      indexName));
              return searchNodes.get().size() == 1;
            });

    assertThat(searchNodes.get().size()).isEqualTo(1);
    assertThat(searchNodes.get().keySet().iterator().next())
        .isEqualTo(indexer1SearchContext.toString());
    List<String> chunks = searchNodes.get().values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    Iterator<String> chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(snapshot1Name);

    // create cache node entry for search metadata also serving the snapshot
    SearchMetadata cacheNodeSearchMetada =
        registerTestSearchMetadata(searchMetadataStore, cache1SearchContext, snapshot1Name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 2);

    searchNodes.set(
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName));
    assertThat(searchNodes.get().size()).isEqualTo(1);
    assertThat(searchNodes.get().keySet().iterator().next())
        .isEqualTo(cache1SearchContext.toString());
    chunks = searchNodes.get().values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(cacheNodeSearchMetada.snapshotName);

    // create cache node entry for search metadata also serving the snapshot
    registerTestSearchMetadata(searchMetadataStore, cache2SearchContext, snapshot1Name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 3);

    searchNodes.set(
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName));
    assertThat(searchNodes.get().size()).isEqualTo(1);
    String searchNodeUrl = searchNodes.get().keySet().iterator().next();
    assertThat(
            searchNodeUrl.equals(cache1SearchContext.toString())
                || searchNodeUrl.equals(cache2SearchContext.toString())
                || searchNodeUrl.equals(cache3SearchContext.toString()))
        .isTrue();
    chunks = searchNodes.get().values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(cacheNodeSearchMetada.snapshotName);

    Instant snapshot2CreationTime = Instant.ofEpochMilli(51);
    Instant snapshot2EndTime = Instant.ofEpochMilli(150);
    SnapshotMetadata snapshot2Metadata =
        createSnapshot(snapshot2CreationTime, snapshot2EndTime, false, "1");
    await()
        .until(
            () ->
                AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()
                    == 3); // snapshot1(live + non_live) + snapshot2

    registerTestSearchMetadata(searchMetadataStore, cache2SearchContext, snapshot2Metadata.name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 4);

    Instant snapshot3CreationTime = Instant.ofEpochMilli(151);
    Instant snapshot3EndTime = Instant.ofEpochMilli(250);
    SnapshotMetadata snapshot3Metadata =
        createSnapshot(snapshot3CreationTime, snapshot3EndTime, false, "1");
    await()
        .until(
            () ->
                AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()
                    == 4); // snapshot1(live + non_live) + snapshot2 + snapshot3

    registerTestSearchMetadata(searchMetadataStore, cache1SearchContext, snapshot3Metadata.name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 5);

    searchNodes.set(
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName));
    assertThat(searchNodes.get().size()).isEqualTo(2);
    // snapshot1 ( could be picked from either cache1 or cache2) + snapshot2 + snapshot3
    int totalChunksToBeSearched = 3;
    int count = 0;
    for (Map.Entry<String, List<String>> node : searchNodes.get().entrySet()) {
      // must not be the index node
      assertThat(searchNodeUrl.equals(indexer1SearchContext.toString())).isFalse();
      count += node.getValue().size();
    }
    assertThat(count).isEqualTo(totalChunksToBeSearched);

    searchNodes.set(
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            snapshot2CreationTime.toEpochMilli(),
            snapshot2EndTime.toEpochMilli(),
            indexName));
    assertThat(searchNodes.get().size() == 1 || searchNodes.get().size() == 2).isTrue();
    if (searchNodes.get().size() == 1) {
      chunks = searchNodes.get().values().iterator().next();
      assertThat(chunks.size()).isEqualTo(2);
    } else {
      for (Map.Entry<String, List<String>> searchNode : searchNodes.get().entrySet()) {
        if (searchNode.getKey().equals(cache1SearchContext.toUrl())) {
          assertThat(searchNode.getValue().size()).isEqualTo(1);
          assertThat(searchNode.getValue().iterator().next()).isEqualTo(snapshot1Name);
        } else if (searchNode.getKey().equals(cache2SearchContext.toUrl())) {
          assertThat(searchNode.getValue().size()).isEqualTo(1);
          assertThat(searchNode.getValue().iterator().next()).isEqualTo(snapshot2Metadata.name);
        } else {
          fail(
              "SearchNodes should only query cache1 and cache2 but is trying to search "
                  + searchNode.getKey());
        }
      }
    }
  }

  @Test
  public void testOneIndexerOneCache() throws Exception {
    String indexName = "testIndex";
    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(199, 500, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    Instant chunk1CreationTime = Instant.ofEpochMilli(100);
    Instant chunk1EndTime = Instant.ofEpochMilli(200);
    String snapshot1Name =
        createIndexerZKMetadata(chunk1CreationTime, chunk1EndTime, "1", indexer1SearchContext);

    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isEqualTo(2);
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(1);

    await().until(() -> snapshotMetadataStore.listSync().size(), (size) -> size == 2);
    await().until(() -> searchMetadataStore.listSync().size(), (size) -> size == 1);

    Map<String, List<String>> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);

    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());
    List<String> chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    Iterator<String> chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(snapshot1Name);

    Instant chunk2CreationTime = Instant.ofEpochMilli(201);
    Instant chunk2EndTime = Instant.ofEpochMilli(300);
    String snapshot2Name =
        createIndexerZKMetadata(chunk2CreationTime, chunk2EndTime, "1", indexer1SearchContext);

    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isEqualTo(4);
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(2);

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);

    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());
    chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(snapshot1Name);

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk2CreationTime.toEpochMilli(),
            chunk2EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);

    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());
    chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(snapshot2Name);

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk2EndTime.toEpochMilli() + 1,
            chunk2EndTime.toEpochMilli() + 100,
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);

    // create cache node entry for search metadata also serving the snapshot
    SearchMetadata cacheNodeSearchMetada =
        registerTestSearchMetadata(searchMetadataStore, cache1SearchContext, snapshot1Name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 3);

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(cache1SearchContext.toString());
    chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(cacheNodeSearchMetada.snapshotName);

    // re-add dataset metadata with a different time window that doesn't match any snapshot
    datasetMetadataStore.deleteAsync(datasetMetadata.name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 0);
    partition = new DatasetPartitionMetadata(1, 99, List.of("1"));
    datasetMetadata = new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    // we can't find snapshot since the time window doesn't match any dataset metadata
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);
  }

  @Test
  // snaphost1[100-200] -> hosted on cache1 , cache2
  // snapshot2[51-150] -> cache2
  // snapshot3[151-250] - cache1
  public void testTwoCacheNodes() throws Exception {
    String indexName = "testIndex";
    // create snapshot
    Instant chunkCreationTime = Instant.ofEpochMilli(100);
    Instant chunkEndTime = Instant.ofEpochMilli(200);
    SnapshotMetadata snapshotMetadata = createSnapshot(chunkCreationTime, chunkEndTime, false, "1");
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 1);

    // create first search metadata hosted by cache1
    SearchMetadata cache1NodeSearchMetada =
        registerTestSearchMetadata(searchMetadataStore, cache1SearchContext, snapshotMetadata.name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 1);

    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 500, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    // assert search will always find cache1
    Map<String, List<String>> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            1,
            chunkCreationTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(cache1SearchContext.toString());
    List<String> chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    Iterator<String> chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(cache1NodeSearchMetada.snapshotName);

    // create second search metadata hosted by cache2
    SearchMetadata cache2NodeSearchMetada =
        registerTestSearchMetadata(searchMetadataStore, cache2SearchContext, snapshotMetadata.name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 2);

    // assert search will always find cache1 or cache2
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            0,
            chunkCreationTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    String searchNodeUrl = searchNodes.keySet().iterator().next();
    assertThat(
            searchNodeUrl.equals(cache1SearchContext.toString())
                || searchNodeUrl.equals(cache2SearchContext.toString()))
        .isTrue();
    chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    chunkIter = chunks.iterator();
    String chunkName = chunkIter.next();
    assertThat(
            chunkName.equals(cache1NodeSearchMetada.snapshotName)
                || chunkName.equals(cache2NodeSearchMetada.snapshotName))
        .isTrue();

    // now add snapshot2 to cache2
    Instant snapshot2CreationTime = Instant.ofEpochMilli(51);
    Instant snapshot2EndTime = Instant.ofEpochMilli(150);
    SnapshotMetadata snapshot2Metadata =
        createSnapshot(snapshot2CreationTime, snapshot2EndTime, false, "1");
    await()
        .until(
            () ->
                AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()
                    == 2); // snapshot1 + snapshot2

    registerTestSearchMetadata(searchMetadataStore, cache2SearchContext, snapshot2Metadata.name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 3);

    // now add snapshot3 to cache1
    Instant snapshot3CreationTime = Instant.ofEpochMilli(151);
    Instant snapshot3EndTime = Instant.ofEpochMilli(250);
    SnapshotMetadata snapshot3Metadata =
        createSnapshot(snapshot3CreationTime, snapshot3EndTime, false, "1");
    await()
        .until(
            () ->
                AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()
                    == 3); // snapshot1 + snapshot2 + snapshot3

    registerTestSearchMetadata(searchMetadataStore, cache1SearchContext, snapshot3Metadata.name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 4);

    // assert search will always find cache1 AND cache2
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(2);
    // snapshot1 ( could be picked from either cache1 or cache2) + snapshot2 + snapshot3
    int totalChunksToBeSearched = 3;
    int count = 0;
    for (Map.Entry<String, List<String>> node : searchNodes.entrySet()) {
      count += node.getValue().size();
    }
    assertThat(count).isEqualTo(totalChunksToBeSearched);

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            snapshot2CreationTime.toEpochMilli(),
            snapshot2EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size() == 1 || searchNodes.size() == 2).isTrue();
    if (searchNodes.size() == 1) {
      chunks = searchNodes.values().iterator().next();
      assertThat(chunks.size()).isEqualTo(2);
    } else {
      for (Map.Entry<String, List<String>> searchNode : searchNodes.entrySet()) {
        if (searchNode.getKey().equals(cache1SearchContext.toUrl())) {
          assertThat(searchNode.getValue().size()).isEqualTo(1);
          assertThat(searchNode.getValue().iterator().next()).isEqualTo(snapshotMetadata.name);
        } else if (searchNode.getKey().equals(cache2SearchContext.toUrl())) {
          assertThat(searchNode.getValue().size()).isEqualTo(1);
          assertThat(searchNode.getValue().iterator().next()).isEqualTo(snapshot2Metadata.name);
        } else {
          fail(
              "SearchNodes should only query cache1 and cache2 but is trying to search "
                  + searchNode.getKey());
        }
      }
    }
  }

  @Test
  public void testMultipleDatasetsMultipleTimeRange() throws Exception {

    // dataset1 snapshots/search-metadata/partitions
    SnapshotMetadata snapshotMetadata =
        createSnapshot(Instant.ofEpochMilli(100), Instant.ofEpochMilli(200), false, "1");
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 1);
    registerTestSearchMetadata(searchMetadataStore, cache1SearchContext, snapshotMetadata.name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 1);

    snapshotMetadata =
        createSnapshot(Instant.ofEpochMilli(201), Instant.ofEpochMilli(300), false, "2");
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 2);
    registerTestSearchMetadata(searchMetadataStore, cache2SearchContext, snapshotMetadata.name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 2);

    final String name = "testDataset";
    final String owner = "DatasetOwner";
    final long throughputBytes = 1000;
    final DatasetPartitionMetadata partition11 =
        new DatasetPartitionMetadata(100, 200, List.of("1"));
    final DatasetPartitionMetadata partition12 =
        new DatasetPartitionMetadata(201, 300, List.of("2"));

    DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughputBytes, List.of(partition11, partition12), name);

    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    // dataset2 snapshots/search-metadata/partitions
    snapshotMetadata =
        createSnapshot(Instant.ofEpochMilli(100), Instant.ofEpochMilli(200), false, "2");
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 3);
    registerTestSearchMetadata(searchMetadataStore, cache3SearchContext, snapshotMetadata.name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 3);

    snapshotMetadata =
        createSnapshot(Instant.ofEpochMilli(201), Instant.ofEpochMilli(300), false, "1");
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 4);
    registerTestSearchMetadata(searchMetadataStore, cache4SearchContext, snapshotMetadata.name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 4);

    final String name1 = "testDataset1";
    final String owner1 = "DatasetOwner1";
    final long throughputBytes1 = 1;
    final DatasetPartitionMetadata partition21 =
        new DatasetPartitionMetadata(100, 200, List.of("2"));
    final DatasetPartitionMetadata partition22 =
        new DatasetPartitionMetadata(201, 300, List.of("1"));

    DatasetMetadata datasetMetadata1 =
        new DatasetMetadata(
            name1, owner1, throughputBytes1, List.of(partition21, partition22), name1);
    datasetMetadataStore.createSync(datasetMetadata1);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 2);

    // find search nodes that will be queries for the first dataset
    Map<String, List<String>> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 100, 199, name);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(cache1SearchContext.toString());

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 100, 299, name);
    assertThat(searchNodes.size()).isEqualTo(2);
    Iterator<String> iter = searchNodes.keySet().iterator();
    String node1 = iter.next();
    String node2 = iter.next();
    assertThat(
            node1.equals(cache1SearchContext.toString())
                || node1.equals(cache2SearchContext.toString()))
        .isTrue();
    assertThat(
            node2.equals(cache1SearchContext.toString())
                || node2.equals(cache2SearchContext.toString()))
        .isTrue();

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 100, 299, name1);
    assertThat(searchNodes.size()).isEqualTo(2);
    iter = searchNodes.keySet().iterator();
    node1 = iter.next();
    node2 = iter.next();
    assertThat(
            node1.equals(cache3SearchContext.toString())
                || node1.equals(cache4SearchContext.toString()))
        .isTrue();
    assertThat(
            node2.equals(cache3SearchContext.toString())
                || node2.equals(cache4SearchContext.toString()))
        .isTrue();
  }

  @Test
  public void testTwoIndexerWithDifferentPartitions() {
    String indexName1 = "testIndex1";
    String indexName2 = "testIndex2";
    // search for partition "1" only
    Instant chunkCreationTime = Instant.ofEpochMilli(100);
    Instant chunkEndTime = Instant.ofEpochMilli(200);
    createIndexerZKMetadata(chunkCreationTime, chunkEndTime, "1", indexer1SearchContext);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 2);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 1);

    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 200, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName1, "testOwner1", 1, List.of(partition), indexName1);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    Map<String, List<String>> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            indexName1);
    assertThat(searchNodes.size()).isEqualTo(1);

    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());

    // search for partition "2" only
    createIndexerZKMetadata(chunkCreationTime, chunkEndTime, "2", indexer2SearchContext);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 4);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 2);

    partition = new DatasetPartitionMetadata(1, 101, List.of("2"));
    datasetMetadata =
        new DatasetMetadata(indexName2, "testOwner2", 1, List.of(partition), indexName2);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 2);

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            indexName2);
    assertThat(searchNodes.size()).isEqualTo(1);

    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer2SearchContext.toString());

    // search for wrong indexName and see if you get 0 nodes
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            "new_dataset_that_does_not_have_a_partition");
    assertThat(searchNodes.size()).isEqualTo(0);
  }

  @Test
  public void testNewDistributedQueryWithSingleSnapshotSuccess() throws JsonProcessingException {
    String indexName = "testIndex";
    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 300, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    Instant chunk1CreationTime = Instant.ofEpochMilli(100);
    Instant chunk1EndTime = Instant.ofEpochMilli(200);
    String snapshot1Name =
        createIndexerZKMetadata(chunk1CreationTime, chunk1EndTime, "1", indexer1SearchContext);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 2);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 1);

    AstraDistributedQueryService distributedQueryService =
        new AstraDistributedQueryService(
            searchMetadataStore,
            snapshotMetadataStore,
            datasetMetadataStore,
            metricsRegistry,
            Duration.ofSeconds(2),
            Duration.ofSeconds(2),
            queryServiceConfig);

    // Mock successful response
    AstraServiceGrpc.AstraServiceFutureStub mockStub =
        mock(AstraServiceGrpc.AstraServiceFutureStub.class);
    distributedQueryService.stubs.put(indexer1SearchContext.toUrl(), mockStub);
    when(mockStub.withDeadlineAfter(anyLong(), any(TimeUnit.class))).thenReturn(mockStub);
    when(mockStub.withInterceptors(any())).thenReturn(mockStub);
    LogWireMessage logWireMessage =
        new LogWireMessage(
            "testIndex", "testType", "testId", Instant.now(), Map.of("testSource", "testValue"));
    when(mockStub.search(any(AstraSearch.SearchRequest.class)))
        .thenReturn(
            Futures.immediateFuture(
                AstraSearch.SearchResult.newBuilder()
                    .addHits(JsonUtil.writeAsString(logWireMessage))
                    .setTotalSnapshots(1)
                    .setTotalNodes(1)
                    .setInternalAggregations(
                        ByteString.copyFrom(OpenSearchInternalAggregation.toByteArray(null)))
                    .build()));

    AstraSearch.SearchRequest request =
        AstraSearch.SearchRequest.newBuilder()
            .setDataset(indexName)
            .setStartTimeEpochMs(chunk1CreationTime.toEpochMilli())
            .setEndTimeEpochMs(chunk1EndTime.toEpochMilli())
            .setQuery(
                """
                      {"bool":{"filter":[{"range":{"@timestamp":{"gte":0,"lte":400,"format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"* !astra.enableNextGenDistributedQueries"}}]}}
                      """)
            .setHowMany(100)
            .build();

    AstraSearch.SearchResult result = distributedQueryService.doSearch(request);
    assertThat(result.getHitsCount()).isEqualTo(1);
    distributedQueryService.close();
  }

  @Test
  public void testNewDistributedQueryRetryOnHardFailure() throws JsonProcessingException {
    String indexName = "testIndex";
    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 300, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    Instant chunk1CreationTime = Instant.ofEpochMilli(100);
    Instant chunk1EndTime = Instant.ofEpochMilli(200);
    SnapshotMetadata snapshotMetadata =
        createSnapshot(chunk1CreationTime, chunk1EndTime, false, "1");
    String snapshot1Name = snapshotMetadata.name;

    await().until(() -> AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 1);

    // Mock this one out so that we can guarantee the return order and verify the failover
    // behavior
    SearchMetadataStore mockSearchMetadataStore = mock(SearchMetadataStore.class);
    when(mockSearchMetadataStore.listSync())
        .thenReturn(
            List.of(
                new SearchMetadata(
                    "testSearchMetadata", snapshot1Name, cache1SearchContext.toUrl()),
                new SearchMetadata(
                    "otherTestSearchMetadata", snapshot1Name, cache2SearchContext.toUrl())));

    AstraDistributedQueryService distributedQueryService =
        new AstraDistributedQueryService(
            mockSearchMetadataStore,
            snapshotMetadataStore,
            datasetMetadataStore,
            metricsRegistry,
            Duration.ofSeconds(2),
            Duration.ofSeconds(2),
            queryServiceConfig);

    // Mock first call failure, second call success
    AstraServiceGrpc.AstraServiceFutureStub mockFailedCacheStub =
        mock(AstraServiceGrpc.AstraServiceFutureStub.class);
    AstraServiceGrpc.AstraServiceFutureStub mockSuccessfulCacheStub =
        mock(AstraServiceGrpc.AstraServiceFutureStub.class);

    distributedQueryService.stubs.put(cache1SearchContext.toUrl(), mockFailedCacheStub);
    distributedQueryService.stubs.put(cache2SearchContext.toUrl(), mockSuccessfulCacheStub);

    when(mockSuccessfulCacheStub.withDeadlineAfter(anyLong(), any(TimeUnit.class)))
        .thenReturn(mockSuccessfulCacheStub);
    when(mockSuccessfulCacheStub.withInterceptors(any())).thenReturn(mockSuccessfulCacheStub);
    when(mockFailedCacheStub.withDeadlineAfter(anyLong(), any(TimeUnit.class)))
        .thenReturn(mockFailedCacheStub);
    when(mockFailedCacheStub.withInterceptors(any())).thenReturn(mockFailedCacheStub);

    // First call returns timeout/error, second call succeeds
    when(mockFailedCacheStub.search(any(AstraSearch.SearchRequest.class)))
        .thenReturn(
            Futures.immediateFuture(
                AstraSearch.SearchResult.newBuilder()
                    .setTotalNodes(1)
                    .setTotalSnapshots(0)
                    .setFailedNodes(1)
                    .setInternalAggregations(
                        ByteString.copyFrom(OpenSearchInternalAggregation.toByteArray(null)))
                    .build()));

    LogWireMessage successLogWireMessage =
        new LogWireMessage(
            "testIndex", "testType", "testId", Instant.now(), Map.of("testSource", "testValue"));
    when(mockSuccessfulCacheStub.search(any(AstraSearch.SearchRequest.class)))
        .thenReturn(
            Futures.immediateFuture(
                AstraSearch.SearchResult.newBuilder()
                    .addHits(JsonUtil.writeAsString(successLogWireMessage))
                    .setTotalSnapshots(1)
                    .setTotalNodes(1)
                    .setInternalAggregations(
                        ByteString.copyFrom(OpenSearchInternalAggregation.toByteArray(null)))
                    .build()));

    AstraSearch.SearchRequest request =
        AstraSearch.SearchRequest.newBuilder()
            .setDataset(indexName)
            .setStartTimeEpochMs(chunk1CreationTime.toEpochMilli())
            .setEndTimeEpochMs(chunk1EndTime.toEpochMilli())
            .setQuery(
                """
                      {"bool":{"filter":[{"range":{"@timestamp":{"gte":0,"lte":500,"format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"* !astra.enableNextGenDistributedQueries"}}]}}
                      """)
            .setHowMany(100)
            .build();

    AstraSearch.SearchResult result = distributedQueryService.doSearch(request);
    assertThat(result.getHitsCount()).isEqualTo(1);
    distributedQueryService.close();
  }

  @Test
  public void testNewDistributedQueryDoesNotRetryOnSoftFailure() {
    String indexName = "testIndex";
    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 300, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    Instant chunk1CreationTime = Instant.ofEpochMilli(100);
    Instant chunk1EndTime = Instant.ofEpochMilli(200);
    String snapshot1Name =
        createIndexerZKMetadata(chunk1CreationTime, chunk1EndTime, "1", indexer1SearchContext);

    // Create cache replica for the same snapshot
    registerTestSearchMetadata(searchMetadataStore, cache1SearchContext, snapshot1Name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 2);

    AstraDistributedQueryService distributedQueryService =
        new AstraDistributedQueryService(
            searchMetadataStore,
            snapshotMetadataStore,
            datasetMetadataStore,
            metricsRegistry,
            Duration.ofSeconds(2),
            Duration.ofSeconds(2),
            queryServiceConfig);

    AstraServiceGrpc.AstraServiceFutureStub mockStub =
        mock(AstraServiceGrpc.AstraServiceFutureStub.class);
    distributedQueryService.stubs.put(indexer1SearchContext.toUrl(), mockStub);
    when(mockStub.withDeadlineAfter(anyLong(), any(TimeUnit.class))).thenReturn(mockStub);
    when(mockStub.withInterceptors(any())).thenReturn(mockStub);

    // Return soft failure (parsing error) - should not be retried
    when(mockStub.search(any(AstraSearch.SearchRequest.class)))
        .thenReturn(
            Futures.immediateFuture(
                AstraSearch.SearchResult.newBuilder()
                    .addAllSoftFailedChunkIds(List.of(snapshot1Name))
                    .build()));

    AstraSearch.SearchRequest request =
        AstraSearch.SearchRequest.newBuilder()
            .setDataset(indexName)
            .setStartTimeEpochMs(chunk1CreationTime.toEpochMilli())
            .setEndTimeEpochMs(chunk1EndTime.toEpochMilli())
            .setQuery(
                """
                      {"bool":{"filter":[{"range":{"@timestamp":{"gte":0,"lte":500,"format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"* !astra.enableNextGenDistributedQueries"}}]}}
                      """)
            .setHowMany(100)
            .build();

    AstraSearch.SearchResult result = distributedQueryService.doSearch(request);
    assertThat(result.getHitsCount()).isEqualTo(0);
    // Verify cache stub was never called (no retry)
    distributedQueryService.close();
  }

  @Test
  public void testNewDistributedQueryHandlesEmptySnapshotList() {
    String indexName = "testIndex";
    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 50, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    AstraDistributedQueryService distributedQueryService =
        new AstraDistributedQueryService(
            searchMetadataStore,
            snapshotMetadataStore,
            datasetMetadataStore,
            metricsRegistry,
            Duration.ofSeconds(2),
            Duration.ofSeconds(2),
            queryServiceConfig);

    // Query time range that matches no snapshots
    AstraSearch.SearchRequest request =
        AstraSearch.SearchRequest.newBuilder()
            .setDataset(indexName)
            .setStartTimeEpochMs(100)
            .setEndTimeEpochMs(200)
            .setQuery(
                """
                      {"bool":{"filter":[{"range":{"@timestamp":{"gte":0,"lte":500,"format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"* !astra.enableNextGenDistributedQueries"}}]}}
                      """)
            .setHowMany(100)
            .build();

    AstraSearch.SearchResult result = distributedQueryService.doSearch(request);
    assertThat(result.getHitsCount()).isEqualTo(0);
    distributedQueryService.close();
  }

  @Test
  public void testNewDistributedQueryMultipleReplicasRetryLogic() throws JsonProcessingException {
    String indexName = "testIndex";
    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 300, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    Instant chunk1CreationTime = Instant.ofEpochMilli(100);
    Instant chunk1EndTime = Instant.ofEpochMilli(200);
    SnapshotMetadata snapshotMetadata =
        createSnapshot(chunk1CreationTime, chunk1EndTime, false, "1");
    String snapshot1Name = snapshotMetadata.name;

    await().until(() -> AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 1);

    // Mock this one out so that we can guarantee the return order and verify the failover
    // behavior
    SearchMetadataStore mockSearchMetadataStore = mock(SearchMetadataStore.class);
    when(mockSearchMetadataStore.listSync())
        .thenReturn(
            List.of(
                new SearchMetadata(
                    "testSearchMetadata1", snapshot1Name, indexer1SearchContext.toUrl()),
                new SearchMetadata(
                    "testSearchMetadata2", snapshot1Name, cache1SearchContext.toUrl()),
                new SearchMetadata(
                    "testSearchMetadata3", snapshot1Name, cache2SearchContext.toUrl()),
                new SearchMetadata(
                    "testSearchMetadata4", snapshot1Name, cache3SearchContext.toUrl())));

    AstraDistributedQueryService distributedQueryService =
        new AstraDistributedQueryService(
            mockSearchMetadataStore,
            snapshotMetadataStore,
            datasetMetadataStore,
            metricsRegistry,
            Duration.ofSeconds(2),
            Duration.ofSeconds(2),
            queryServiceConfig);

    // Mock stubs - first three fail with hard failure, fourth succeeds
    AstraServiceGrpc.AstraServiceFutureStub mockIndexerStub =
        mock(AstraServiceGrpc.AstraServiceFutureStub.class);
    AstraServiceGrpc.AstraServiceFutureStub mockCache1Stub =
        mock(AstraServiceGrpc.AstraServiceFutureStub.class);
    AstraServiceGrpc.AstraServiceFutureStub mockCache2Stub =
        mock(AstraServiceGrpc.AstraServiceFutureStub.class);
    AstraServiceGrpc.AstraServiceFutureStub mockCache3Stub =
        mock(AstraServiceGrpc.AstraServiceFutureStub.class);

    distributedQueryService.stubs.put(indexer1SearchContext.toUrl(), mockIndexerStub);
    distributedQueryService.stubs.put(cache1SearchContext.toUrl(), mockCache1Stub);
    distributedQueryService.stubs.put(cache2SearchContext.toUrl(), mockCache2Stub);
    distributedQueryService.stubs.put(cache3SearchContext.toUrl(), mockCache3Stub);

    // Configure all stubs
    when(mockIndexerStub.withDeadlineAfter(anyLong(), any(TimeUnit.class)))
        .thenReturn(mockIndexerStub);
    when(mockIndexerStub.withInterceptors(any())).thenReturn(mockIndexerStub);
    when(mockCache1Stub.withDeadlineAfter(anyLong(), any(TimeUnit.class)))
        .thenReturn(mockCache1Stub);
    when(mockCache1Stub.withInterceptors(any())).thenReturn(mockCache1Stub);
    when(mockCache2Stub.withDeadlineAfter(anyLong(), any(TimeUnit.class)))
        .thenReturn(mockCache2Stub);
    when(mockCache2Stub.withInterceptors(any())).thenReturn(mockCache2Stub);
    when(mockCache3Stub.withDeadlineAfter(anyLong(), any(TimeUnit.class)))
        .thenReturn(mockCache3Stub);
    when(mockCache3Stub.withInterceptors(any())).thenReturn(mockCache3Stub);

    AstraSearch.SearchResult failedResult =
        AstraSearch.SearchResult.newBuilder()
            .setTotalNodes(1)
            .setTotalSnapshots(0)
            .setFailedNodes(1)
            .setInternalAggregations(
                ByteString.copyFrom(OpenSearchInternalAggregation.toByteArray(null)))
            .build();

    // Mock responses: first three return hard failures, fourth succeeds
    when(mockIndexerStub.search(any(AstraSearch.SearchRequest.class)))
        .thenReturn(Futures.immediateFuture(failedResult));
    when(mockCache1Stub.search(any(AstraSearch.SearchRequest.class)))
        .thenReturn(Futures.immediateFuture(failedResult));
    when(mockCache2Stub.search(any(AstraSearch.SearchRequest.class)))
        .thenReturn(Futures.immediateFuture(failedResult));

    LogWireMessage successLogWireMessage =
        new LogWireMessage(
            "testIndex", "testType", "testId", Instant.now(), Map.of("testSource", "testValue"));
    when(mockCache3Stub.search(any(AstraSearch.SearchRequest.class)))
        .thenReturn(
            Futures.immediateFuture(
                AstraSearch.SearchResult.newBuilder()
                    .addHits(JsonUtil.writeAsString(successLogWireMessage))
                    .setTotalSnapshots(1)
                    .setTotalNodes(1)
                    .setInternalAggregations(
                        ByteString.copyFrom(OpenSearchInternalAggregation.toByteArray(null)))
                    .build()));

    AstraSearch.SearchRequest request =
        AstraSearch.SearchRequest.newBuilder()
            .setDataset(indexName)
            .setStartTimeEpochMs(chunk1CreationTime.toEpochMilli())
            .setEndTimeEpochMs(chunk1EndTime.toEpochMilli())
            .setQuery(
                """
                      {"bool":{"filter":[{"range":{"@timestamp":{"gte":0,"lte":500,"format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"* !astra.enableNextGenDistributedQueries"}}]}}
                      """)
            .setHowMany(100)
            .build();

    AstraSearch.SearchResult result = distributedQueryService.doSearch(request);
    assertThat(result.getHitsCount()).isEqualTo(1);
    distributedQueryService.close();
  }

  @Test
  public void testNewDistributedQueryAllReplicasFailForSnapshot() {
    String indexName = "testIndex";
    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 300, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    Instant chunk1CreationTime = Instant.ofEpochMilli(100);
    Instant chunk1EndTime = Instant.ofEpochMilli(200);
    String snapshot1Name =
        createIndexerZKMetadata(chunk1CreationTime, chunk1EndTime, "1", indexer1SearchContext);

    // Create cache replica for the same snapshot
    registerTestSearchMetadata(searchMetadataStore, cache1SearchContext, snapshot1Name);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 2);

    AstraDistributedQueryService distributedQueryService =
        new AstraDistributedQueryService(
            searchMetadataStore,
            snapshotMetadataStore,
            datasetMetadataStore,
            metricsRegistry,
            Duration.ofSeconds(2),
            Duration.ofSeconds(2),
            queryServiceConfig);

    // Mock stubs - both fail
    AstraServiceGrpc.AstraServiceFutureStub mockIndexerStub =
        mock(AstraServiceGrpc.AstraServiceFutureStub.class);
    AstraServiceGrpc.AstraServiceFutureStub mockCacheStub =
        mock(AstraServiceGrpc.AstraServiceFutureStub.class);

    distributedQueryService.stubs.put(indexer1SearchContext.toUrl(), mockIndexerStub);
    distributedQueryService.stubs.put(cache1SearchContext.toUrl(), mockCacheStub);

    when(mockIndexerStub.withDeadlineAfter(anyLong(), any(TimeUnit.class)))
        .thenReturn(mockIndexerStub);
    when(mockIndexerStub.withInterceptors(any())).thenReturn(mockIndexerStub);
    when(mockCacheStub.withDeadlineAfter(anyLong(), any(TimeUnit.class))).thenReturn(mockCacheStub);
    when(mockCacheStub.withInterceptors(any())).thenReturn(mockCacheStub);

    // Both calls fail
    when(mockIndexerStub.search(any(AstraSearch.SearchRequest.class)))
        .thenReturn(Futures.immediateFailedFuture(new RuntimeException("Indexer timeout")));
    when(mockCacheStub.search(any(AstraSearch.SearchRequest.class)))
        .thenReturn(Futures.immediateFailedFuture(new RuntimeException("Cache timeout")));

    AstraSearch.SearchRequest request =
        AstraSearch.SearchRequest.newBuilder()
            .setDataset(indexName)
            .setStartTimeEpochMs(chunk1CreationTime.toEpochMilli())
            .setEndTimeEpochMs(chunk1EndTime.toEpochMilli())
            .setQuery(
                """
                      {"bool":{"filter":[{"range":{"@timestamp":{"gte":0,"lte":500,"format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"* !astra.enableNextGenDistributedQueries"}}]}}
                      """)
            .setHowMany(100)
            .build();

    AstraSearch.SearchResult result = distributedQueryService.doSearch(request);
    // Should still return a result even if snapshot failed completely
    assertThat(result.getHitsCount()).isEqualTo(0);
    distributedQueryService.close();
  }

  @Test
  public void testSchema() {
    Instant endTime = Instant.now();
    Instant startTime = endTime.minus(1, ChronoUnit.HOURS);

    // mock the zookeeper responses, as we don't want to test ZK behavior here
    SearchMetadataStore searchMetadataStoreMock = mock(SearchMetadataStore.class);
    when(searchMetadataStoreMock.listSync())
        .thenReturn(List.of(new SearchMetadata("foo", "snapshot1", "http://127.0.0.1")));
    SnapshotMetadataStore snapshotMetadataStoreMock = mock(SnapshotMetadataStore.class);
    when(snapshotMetadataStoreMock.listSync())
        .thenReturn(
            List.of(
                new SnapshotMetadata(
                    "snapshot1",
                    endTime.minus(30, ChronoUnit.MINUTES).toEpochMilli(),
                    endTime.toEpochMilli(),
                    10,
                    "1",
                    0)));
    DatasetMetadataStore datasetMetadataStoreMock = mock(DatasetMetadataStore.class);
    when(datasetMetadataStoreMock.listSync())
        .thenReturn(
            List.of(
                new DatasetMetadata(
                    "foo",
                    "foo",
                    10,
                    List.of(
                        new DatasetPartitionMetadata(
                            endTime.minus(1, ChronoUnit.DAYS).toEpochMilli(),
                            Long.MAX_VALUE,
                            List.of("1"))),
                    "")));

    AstraDistributedQueryService distributedQueryService =
        new AstraDistributedQueryService(
            searchMetadataStoreMock,
            snapshotMetadataStoreMock,
            datasetMetadataStoreMock,
            new SimpleMeterRegistry(),
            Duration.of(2, ChronoUnit.SECONDS),
            Duration.of(2, ChronoUnit.SECONDS),
            queryServiceConfig);

    // Make a mock grpc stub and store it in the stubs cache map
    AstraServiceGrpc.AstraServiceFutureStub futureStub =
        mock(AstraServiceGrpc.AstraServiceFutureStub.class);
    distributedQueryService.stubs.put("http://127.0.0.1", futureStub);
    when(futureStub.withDeadlineAfter(anyLong(), any(TimeUnit.class))).thenReturn(futureStub);
    when(futureStub.withInterceptors(any())).thenReturn(futureStub);
    when(futureStub.schema(any(AstraSearch.SchemaRequest.class)))
        .thenReturn(
            Futures.immediateFuture(
                AstraSearch.SchemaResult.newBuilder()
                    .putFieldDefinition(
                        "foo",
                        AstraSearch.SchemaDefinition.newBuilder()
                            .setType(Schema.SchemaFieldType.TEXT)
                            .build())
                    .build()));

    // wildcard schema query should return a result
    AstraSearch.SchemaRequest schemaRequestWildcard =
        AstraSearch.SchemaRequest.newBuilder()
            .setStartTimeEpochMs(startTime.toEpochMilli())
            .setEndTimeEpochMs(endTime.toEpochMilli())
            .setDataset("*")
            .build();
    AstraSearch.SchemaResult schemaResultWildcard =
        distributedQueryService.getSchema(schemaRequestWildcard);
    assertThat(schemaResultWildcard.getFieldDefinitionMap())
        .isEqualTo(
            Map.of(
                "foo",
                AstraSearch.SchemaDefinition.newBuilder()
                    .setType(Schema.SchemaFieldType.TEXT)
                    .build()));

    // Exact dataset query should return a result
    AstraSearch.SchemaRequest schemaRequestExact =
        AstraSearch.SchemaRequest.newBuilder()
            .setStartTimeEpochMs(startTime.toEpochMilli())
            .setEndTimeEpochMs(endTime.toEpochMilli())
            .setDataset("foo")
            .build();
    AstraSearch.SchemaResult schemaResultExact =
        distributedQueryService.getSchema(schemaRequestExact);
    assertThat(schemaResultExact.getFieldDefinitionMap())
        .isEqualTo(
            Map.of(
                "foo",
                AstraSearch.SchemaDefinition.newBuilder()
                    .setType(Schema.SchemaFieldType.TEXT)
                    .build()));

    // query window that returns no results should have no schema
    AstraSearch.SchemaRequest schemaRequestMissing =
        AstraSearch.SchemaRequest.newBuilder()
            .setStartTimeEpochMs(0)
            .setEndTimeEpochMs(10)
            .setDataset("*")
            .build();
    AstraSearch.SchemaResult schemaResultMissing =
        distributedQueryService.getSchema(schemaRequestMissing);
    assertThat(schemaResultMissing.getFieldDefinitionMap().size()).isEqualTo(0);

    // query window that queries a different dataset should have no schema
    AstraSearch.SchemaRequest schemaRequestWrongDataset =
        AstraSearch.SchemaRequest.newBuilder()
            .setStartTimeEpochMs(startTime.toEpochMilli())
            .setEndTimeEpochMs(endTime.toEpochMilli())
            .setDataset("bar")
            .build();
    AstraSearch.SchemaResult schemaResultWrongDataset =
        distributedQueryService.getSchema(schemaRequestWrongDataset);
    assertThat(schemaResultWrongDataset.getFieldDefinitionMap().size()).isEqualTo(0);
    distributedQueryService.close();
  }

  private String createIndexerZKMetadata(
      Instant chunkCreationTime,
      Instant chunkEndTime,
      String partition,
      SearchContext searchContext) {
    SnapshotMetadata liveSnapshotMetadata =
        createSnapshot(chunkCreationTime, chunkEndTime, true, partition);
    SearchMetadata liveSearchMetadata = toSearchMetadata(liveSnapshotMetadata.name, searchContext);

    searchMetadataStore.createSync(liveSearchMetadata);

    return liveSnapshotMetadata.name.substring(
        5); // remove LIVE_ prefix for a search metadata hosted by a cache node
  }

  private SnapshotMetadata createSnapshot(
      Instant chunkCreationTime, Instant chunkEndTime, boolean isLive, String partition) {
    String chunkName = "logStore_" + chunkCreationTime.getEpochSecond() + "_" + UUID.randomUUID();
    ChunkInfo chunkInfo =
        new ChunkInfo(
            chunkName,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            1234,
            partition,
            0);
    SnapshotMetadata snapshotMetadata =
        toSnapshotMetadata(chunkInfo, isLive ? LIVE_SNAPSHOT_PREFIX : "");

    snapshotMetadataStore.createSync(snapshotMetadata);

    if (isLive) {
      // create non-live as well to simulate postClose of IndexingChunkImpl
      SnapshotMetadata nonLiveSnapshotMetadata = toSnapshotMetadata(chunkInfo, "");
      snapshotMetadataStore.createSync(nonLiveSnapshotMetadata);
    }

    return snapshotMetadata;
  }

  private Map<String, List<String>> getSearchNodesToQuery(
      SnapshotMetadataStore snapshotMetadataStore,
      SearchMetadataStore searchMetadataStore,
      DatasetMetadataStore datasetMetadataStore,
      long queryStartTimeEpochMs,
      long queryEndTimeEpochMs,
      String dataset) {
    Map<String, SnapshotMetadata> snapshotsToSearch =
        getMatchingSnapshots(
            snapshotMetadataStore,
            datasetMetadataStore,
            queryStartTimeEpochMs,
            queryEndTimeEpochMs,
            dataset);

    Map<String, List<SearchMetadata>> searchMetadataToQuery =
        getMatchingSearchMetadata(searchMetadataStore, snapshotsToSearch);

    return getNodesAndSnapshotsToQuery(searchMetadataToQuery);
  }
}
