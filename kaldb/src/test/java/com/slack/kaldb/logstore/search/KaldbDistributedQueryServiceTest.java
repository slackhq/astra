package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.chunk.ChunkInfo.toSnapshotMetadata;
import static com.slack.kaldb.chunk.ReadWriteChunk.LIVE_SNAPSHOT_PREFIX;
import static com.slack.kaldb.chunk.ReadWriteChunk.toSearchMetadata;
import static com.slack.kaldb.logstore.search.KaldbDistributedQueryService.findPartitionsToQuery;
import static com.slack.kaldb.logstore.search.KaldbDistributedQueryService.getSearchNodesToQuery;
import static com.slack.kaldb.metadata.snapshot.SnapshotMetadata.LIVE_SNAPSHOT_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.kaldb.chunk.ChunkInfo;
import com.slack.kaldb.chunk.ReadOnlyChunkImpl;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.service.DatasetMetadata;
import com.slack.kaldb.metadata.service.DatasetMetadataStore;
import com.slack.kaldb.metadata.service.DatasetPartitionMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.*;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KaldbDistributedQueryServiceTest {

  private SimpleMeterRegistry metricsRegistry;

  private MetadataStore zkMetadataStore;
  private SearchMetadataStore searchMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private DatasetMetadataStore datasetMetadataStore;

  private TestingServer testZKServer;
  private SearchContext indexer1SearchContext;
  private SearchContext indexer2SearchContext;
  private SearchContext cache1SearchContext;
  private SearchContext cache2SearchContext;
  private SearchContext cache3SearchContext;
  private SearchContext cache4SearchContext;

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();

    metricsRegistry = new SimpleMeterRegistry();
    testZKServer = new TestingServer();

    // Metadata store
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testZKServer.getConnectString())
            .setZkPathPrefix("indexerTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    zkMetadataStore = spy(ZookeeperMetadataStoreImpl.fromConfig(metricsRegistry, zkConfig));

    snapshotMetadataStore = spy(new SnapshotMetadataStore(zkMetadataStore, true));
    searchMetadataStore = spy(new SearchMetadataStore(zkMetadataStore, true));
    datasetMetadataStore = new DatasetMetadataStore(zkMetadataStore, true);

    indexer1SearchContext = new SearchContext("indexer_host1", 10000);
    indexer2SearchContext = new SearchContext("indexer_host2", 10001);
    cache1SearchContext = new SearchContext("cache_host1", 20000);
    cache2SearchContext = new SearchContext("cache_host2", 20001);
    cache3SearchContext = new SearchContext("cache_host3", 20002);
    cache4SearchContext = new SearchContext("cache_host4", 20003);
  }

  @After
  public void tearDown() throws Exception {
    snapshotMetadataStore.close();
    searchMetadataStore.close();
    datasetMetadataStore.close();
    zkMetadataStore.close();
    metricsRegistry.close();
    testZKServer.close();
  }

  @Test
  public void testOneServiceOnePartition() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final DatasetPartitionMetadata partition = new DatasetPartitionMetadata(100, 200, List.of("1"));

    DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughputBytes, List.of(partition));

    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    List<DatasetPartitionMetadata> partitionMetadata =
        findPartitionsToQuery(datasetMetadataStore, 101, 199, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);

    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 1, 150, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);

    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 1, 250, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);

    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 200, 250, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);

    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 201, 250, name);
    assertThat(partitionMetadata.size()).isEqualTo(0);
  }

  @Test
  public void testOneServiceMultipleWindows() {
    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final DatasetPartitionMetadata partition1 =
        new DatasetPartitionMetadata(100, 200, List.of("1"));

    final DatasetPartitionMetadata partition2 =
        new DatasetPartitionMetadata(201, 300, List.of("2", "3"));

    DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughputBytes, List.of(partition1, partition2));

    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    List<DatasetPartitionMetadata> partitionMetadata =
        findPartitionsToQuery(datasetMetadataStore, 101, 199, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).startTimeEpochMs).isEqualTo(100);
    assertThat(partitionMetadata.get(0).endTimeEpochMs).isEqualTo(200);

    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 201, 300, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(2);
    assertThat(partitionMetadata.get(0).startTimeEpochMs).isEqualTo(201);
    assertThat(partitionMetadata.get(0).endTimeEpochMs).isEqualTo(300);

    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 100, 202, name);
    assertThat(partitionMetadata.size()).isEqualTo(2);

    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).startTimeEpochMs).isEqualTo(100);
    assertThat(partitionMetadata.get(0).endTimeEpochMs).isEqualTo(200);

    assertThat(partitionMetadata.get(1).partitions.size()).isEqualTo(2);
    assertThat(partitionMetadata.get(1).startTimeEpochMs).isEqualTo(201);
    assertThat(partitionMetadata.get(1).endTimeEpochMs).isEqualTo(300);
  }

  @Test
  public void testMultipleServicesOneTimeRange() {

    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final DatasetPartitionMetadata partition = new DatasetPartitionMetadata(100, 200, List.of("1"));

    DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughputBytes, List.of(partition));

    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    final String name1 = "testService1";
    final String owner1 = "serviceOwner1";
    final long throughputBytes1 = 1;
    final DatasetPartitionMetadata partition1 =
        new DatasetPartitionMetadata(100, 200, List.of("2"));

    DatasetMetadata datasetMetadata1 =
        new DatasetMetadata(name1, owner1, throughputBytes1, List.of(partition1));

    datasetMetadataStore.createSync(datasetMetadata1);
    await().until(() -> datasetMetadataStore.listSync().size() == 2);

    List<DatasetPartitionMetadata> partitionMetadata =
        findPartitionsToQuery(datasetMetadataStore, 101, 199, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.get(0)).isEqualTo("1");
  }

  @Test
  public void testOneIndexer() {
    String indexName = "testIndex";
    Instant chunkCreationTime = Instant.ofEpochMilli(100);
    Instant chunkEndTime = Instant.ofEpochMilli(200);
    createIndexerZKMetadata(chunkCreationTime, chunkEndTime, "1", indexer1SearchContext);
    await().until(() -> snapshotMetadataStore.listSync().size() == 2);
    await().until(() -> searchMetadataStore.listSync().size() == 1);

    // we don't have any service metadata entry, so we shouldn't be able to find any snapshot
    Collection<String> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);

    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 300, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition));
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    // now we can find the snapshot
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.iterator().next()).isEqualTo(indexer1SearchContext.toString());

    // we can't find snapshot since the time window doesn't match snapshot
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkEndTime.toEpochMilli() + 1,
            chunkEndTime.toEpochMilli() + 100,
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);

    // add another chunk on the same indexer and ensure we still find the node
    createIndexerZKMetadata(
        Instant.ofEpochMilli(201), Instant.ofEpochMilli(300), "1", indexer1SearchContext);
    await().until(() -> snapshotMetadataStore.listSync().size() == 4);
    await().until(() -> searchMetadataStore.listSync().size() == 2);
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 0, 300, indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.iterator().next()).isEqualTo(indexer1SearchContext.toString());
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 0, 150, indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.iterator().next()).isEqualTo(indexer1SearchContext.toString());

    // re-add service metadata with a different time window that doesn't match any snapshot
    datasetMetadataStore.delete(datasetMetadata.name);
    await().until(() -> datasetMetadataStore.listSync().size() == 0);
    partition = new DatasetPartitionMetadata(1, 99, List.of("1"));
    datasetMetadata = new DatasetMetadata(indexName, "testOwner", 1, List.of(partition));
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    // we can't find snapshot since the time window doesn't match any service metadata
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);
  }

  @Test
  public void testOneIndexerOneCache() throws Exception {
    String indexName = "testIndex";
    Instant chunkCreationTime = Instant.ofEpochMilli(100);
    Instant chunkEndTime = Instant.ofEpochMilli(200);
    String snapshotName =
        createIndexerZKMetadata(chunkCreationTime, chunkEndTime, "1", indexer1SearchContext);
    assertThat(snapshotMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(1);

    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(199, 500, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition));
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    Collection<String> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.iterator().next()).isEqualTo(indexer1SearchContext.toString());

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkEndTime.toEpochMilli() + 1,
            chunkEndTime.toEpochMilli() + 100,
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);

    // create cache node entry for search metadata also serving the snapshot
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache1SearchContext, snapshotName);
    await().until(() -> searchMetadataStore.listSync().size() == 2);

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.iterator().next()).isEqualTo(cache1SearchContext.toString());

    // re-add service metadata with a different time window that doesn't match any snapshot
    datasetMetadataStore.delete(datasetMetadata.name);
    await().until(() -> datasetMetadataStore.listSync().size() == 0);
    partition = new DatasetPartitionMetadata(1, 99, List.of("1"));
    datasetMetadata = new DatasetMetadata(indexName, "testOwner", 1, List.of(partition));
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    // we can't find snapshot since the time window doesn't match any service metadata
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);
  }

  @Test
  public void testTwoCacheNodes() throws Exception {
    String indexName = "testIndex";
    // create snapshot
    Instant chunkCreationTime = Instant.ofEpochMilli(100);
    Instant chunkEndTime = Instant.ofEpochMilli(200);
    SnapshotMetadata snapshotMetadata = createSnapshot(chunkCreationTime, chunkEndTime, false, "1");
    await().until(() -> snapshotMetadataStore.listSync().size() == 1);

    // create first search metadata hosted by cache1
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache1SearchContext, snapshotMetadata.name);
    await().until(() -> searchMetadataStore.listSync().size() == 1);

    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 101, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition));
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    // assert search will always find cache1
    Collection<String> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            1,
            chunkCreationTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.iterator().next()).isEqualTo(cache1SearchContext.toString());

    // create second search metadata hosted by cache2
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache2SearchContext, snapshotMetadata.name);
    await().until(() -> searchMetadataStore.listSync().size() == 2);

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
    String searchNodeUrl = searchNodes.iterator().next();
    assertThat(
            searchNodeUrl.equals(cache1SearchContext.toString())
                || searchNodeUrl.equals((cache2SearchContext.toString())))
        .isTrue();
  }

  @Test
  public void testMultipleServicesMultipleTimeRange() throws Exception {

    // service1 snapshots/search-metadata/partitions
    SnapshotMetadata snapshotMetadata =
        createSnapshot(Instant.ofEpochMilli(100), Instant.ofEpochMilli(200), false, "1");
    await().until(() -> snapshotMetadataStore.listSync().size() == 1);
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache1SearchContext, snapshotMetadata.name);
    await().until(() -> searchMetadataStore.listSync().size() == 1);

    snapshotMetadata =
        createSnapshot(Instant.ofEpochMilli(201), Instant.ofEpochMilli(300), false, "2");
    await().until(() -> snapshotMetadataStore.listSync().size() == 2);
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache2SearchContext, snapshotMetadata.name);
    await().until(() -> searchMetadataStore.listSync().size() == 2);

    final String name = "testService";
    final String owner = "serviceOwner";
    final long throughputBytes = 1000;
    final DatasetPartitionMetadata partition11 =
        new DatasetPartitionMetadata(100, 200, List.of("1"));
    final DatasetPartitionMetadata partition12 =
        new DatasetPartitionMetadata(201, 300, List.of("2"));

    DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughputBytes, List.of(partition11, partition12));

    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    // service2 snapshots/search-metadata/partitions
    snapshotMetadata =
        createSnapshot(Instant.ofEpochMilli(100), Instant.ofEpochMilli(200), false, "2");
    await().until(() -> snapshotMetadataStore.listSync().size() == 3);
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache3SearchContext, snapshotMetadata.name);
    await().until(() -> searchMetadataStore.listSync().size() == 3);

    snapshotMetadata =
        createSnapshot(Instant.ofEpochMilli(201), Instant.ofEpochMilli(300), false, "1");
    await().until(() -> snapshotMetadataStore.listSync().size() == 4);
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache4SearchContext, snapshotMetadata.name);
    await().until(() -> searchMetadataStore.listSync().size() == 4);

    final String name1 = "testService1";
    final String owner1 = "serviceOwner1";
    final long throughputBytes1 = 1;
    final DatasetPartitionMetadata partition21 =
        new DatasetPartitionMetadata(100, 200, List.of("2"));
    final DatasetPartitionMetadata partition22 =
        new DatasetPartitionMetadata(201, 300, List.of("1"));

    DatasetMetadata datasetMetadata1 =
        new DatasetMetadata(name1, owner1, throughputBytes1, List.of(partition21, partition22));
    datasetMetadataStore.createSync(datasetMetadata1);
    await().until(() -> datasetMetadataStore.listSync().size() == 2);

    // find search nodes that will be queries for the first service
    Collection<String> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 100, 199, name);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.iterator().next()).isEqualTo(cache1SearchContext.toString());

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 100, 299, name);
    assertThat(searchNodes.size()).isEqualTo(2);
    Iterator<String> iter = searchNodes.iterator();
    String node1 = iter.next();
    String node2 = iter.next();
    assertThat(
            node1.equals(cache1SearchContext.toString())
                || node1.equals((cache2SearchContext.toString())))
        .isTrue();
    assertThat(
            node2.equals(cache1SearchContext.toString())
                || node2.equals((cache2SearchContext.toString())))
        .isTrue();

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 100, 299, name1);
    assertThat(searchNodes.size()).isEqualTo(2);
    iter = searchNodes.iterator();
    node1 = iter.next();
    node2 = iter.next();
    assertThat(
            node1.equals(cache3SearchContext.toString())
                || node1.equals((cache4SearchContext.toString())))
        .isTrue();
    assertThat(
            node2.equals(cache3SearchContext.toString())
                || node2.equals((cache4SearchContext.toString())))
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
    await().until(() -> snapshotMetadataStore.listSync().size() == 2);
    await().until(() -> searchMetadataStore.listSync().size() == 1);

    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 200, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName1, "testOwner1", 1, List.of(partition));
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    Collection<String> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            indexName1);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.iterator().next()).isEqualTo(indexer1SearchContext.toString());

    // search for partition "2" only
    createIndexerZKMetadata(chunkCreationTime, chunkEndTime, "2", indexer2SearchContext);
    await().until(() -> snapshotMetadataStore.listSync().size() == 4);
    await().until(() -> searchMetadataStore.listSync().size() == 2);

    partition = new DatasetPartitionMetadata(1, 101, List.of("2"));
    datasetMetadata = new DatasetMetadata(indexName2, "testOwner2", 1, List.of(partition));
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 2);

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            indexName2);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.iterator().next()).isEqualTo(indexer2SearchContext.toString());

    // search for wrong indexName and see if you get 0 nodes
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            "new_service_that_does_not_have_a_partition");
    assertThat(searchNodes.size()).isEqualTo(0);
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
            isLive ? LIVE_SNAPSHOT_PATH : "cacheSnapshotPath");
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
}
