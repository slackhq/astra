package com.slack.astra.metadata.dataset;

import static com.slack.astra.metadata.dataset.DatasetPartitionMetadata.findPartitionsToQuery;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DatasetPartitionMetadataTest {

  private SimpleMeterRegistry metricsRegistry;
  private AsyncCuratorFramework curatorFramework;
  private DatasetMetadataStore datasetMetadataStore;
  private TestingServer testZKServer;
  private static EtcdCluster etcdCluster;
  private Client etcdClient;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();

    metricsRegistry = new SimpleMeterRegistry();
    testZKServer = new TestingServer();

    etcdCluster = TestEtcdClusterFactory.start();

    // Create etcd client
    etcdClient =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream().map(Object::toString).toArray(String[]::new))
            .namespace(
                ByteSequence.from(
                    "datasetPartitionMetadataTest", java.nio.charset.StandardCharsets.UTF_8))
            .build();

    // Metadata store
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
            .setEtcdConfig(
                AstraConfigs.EtcdConfig.newBuilder()
                    .addAllEndpoints(
                        etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
                    .setConnectionTimeoutMs(5000)
                    .setKeepaliveTimeoutMs(3000)
                    .setOperationsMaxRetries(3)
                    .setOperationsTimeoutMs(3000)
                    .setRetryDelayMs(100)
                    .setNamespace("datasetPartitionMetadataTest")
                    .setEnabled(true)
                    .setEphemeralNodeTtlMs(3000)
                    .setEphemeralNodeMaxRetries(3)
                    .build())
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testZKServer.getConnectString())
                    .setZkPathPrefix("datasetPartitionMetadataTest")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();

    this.curatorFramework =
        CuratorBuilder.build(metricsRegistry, metadataStoreConfig.getZookeeperConfig());
    this.datasetMetadataStore =
        new DatasetMetadataStore(
            curatorFramework, etcdClient, metadataStoreConfig, metricsRegistry, true);
  }

  @AfterEach
  public void tearDown() throws Exception {
    datasetMetadataStore.close();
    curatorFramework.unwrap().close();
    if (etcdClient != null) etcdClient.close();
    if (etcdCluster != null) testZKServer.close();
    metricsRegistry.close();
  }

  @Test
  public void testDatasetPartitionMetadata() {
    final Instant start = Instant.now();
    final Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
    final String name = "partitionName";
    final List<String> list = List.of(name);

    final DatasetPartitionMetadata datasetPartitionMetadata =
        new DatasetPartitionMetadata(start.toEpochMilli(), end.toEpochMilli(), list);

    assertThat(datasetPartitionMetadata.startTimeEpochMs).isEqualTo(start.toEpochMilli());
    assertThat(datasetPartitionMetadata.endTimeEpochMs).isEqualTo(end.toEpochMilli());
    assertThat(datasetPartitionMetadata.getPartitions()).isEqualTo(list);
  }

  @Test
  public void testEqualsAndHashCode() {
    final Instant start = Instant.now();
    final Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
    final String name = "partitionName";
    final List<String> list = List.of(name);

    final DatasetPartitionMetadata datasetPartitionMetadata1 =
        new DatasetPartitionMetadata(start.toEpochMilli(), end.toEpochMilli(), list);
    final DatasetPartitionMetadata datasetPartitionMetadata2 =
        new DatasetPartitionMetadata(start.toEpochMilli() + 2, end.toEpochMilli(), list);
    final DatasetPartitionMetadata datasetPartitionMetadata3 =
        new DatasetPartitionMetadata(start.toEpochMilli(), end.toEpochMilli() + 3, list);
    final DatasetPartitionMetadata datasetPartitionMetadata4 =
        new DatasetPartitionMetadata(
            start.toEpochMilli(), end.toEpochMilli(), Collections.emptyList());

    assertThat(datasetPartitionMetadata1).isNotEqualTo(datasetPartitionMetadata2);
    assertThat(datasetPartitionMetadata1).isNotEqualTo(datasetPartitionMetadata3);
    assertThat(datasetPartitionMetadata1).isNotEqualTo(datasetPartitionMetadata4);

    Set<DatasetPartitionMetadata> set = new HashSet<>();
    set.add(datasetPartitionMetadata1);
    set.add(datasetPartitionMetadata2);
    set.add(datasetPartitionMetadata3);
    set.add(datasetPartitionMetadata4);
    assertThat(set.size()).isEqualTo(4);
    assertThat(set)
        .containsOnly(
            datasetPartitionMetadata1,
            datasetPartitionMetadata2,
            datasetPartitionMetadata3,
            datasetPartitionMetadata4);
  }

  @Test
  public void testValidDatasetPartitionMetadata() {
    final Instant start = Instant.now();
    final Instant end = Instant.now().plus(1, ChronoUnit.DAYS);
    final String name = "partitionName";
    final List<String> list = List.of(name);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new DatasetPartitionMetadata(0, end.toEpochMilli(), list));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new DatasetPartitionMetadata(start.toEpochMilli(), 0, list));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> new DatasetPartitionMetadata(start.toEpochMilli(), end.toEpochMilli(), null));
  }

  @Test
  public void testMultipleDatasetMatches() {
    final long throughputBytes = 1000;

    {
      DatasetPartitionMetadata partition = new DatasetPartitionMetadata(100, 200, List.of("1"));

      DatasetMetadata datasetMetadata =
          new DatasetMetadata(
              "testDataset1", "datasetOwner1", throughputBytes, List.of(partition), "testDataset1");

      datasetMetadataStore.createSync(datasetMetadata);
      await().until(() -> datasetMetadataStore.listSync().size() == 1);
    }

    {
      DatasetPartitionMetadata partition = new DatasetPartitionMetadata(201, 300, List.of("2"));
      DatasetMetadata datasetMetadata =
          new DatasetMetadata(
              "testDataset2", "datasetOwner2", throughputBytes, List.of(partition), "testDataset2");
      datasetMetadataStore.createSync(datasetMetadata);
      await().until(() -> datasetMetadataStore.listSync().size() == 2);
    }

    // Start and end time within query window
    List<DatasetPartitionMetadata> partitionMetadata =
        findPartitionsToQuery(datasetMetadataStore, 101, 199, "testDataset1");
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.get(0)).isEqualTo("1");

    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 201, 299, "testDataset2");
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.get(0)).isEqualTo("2");

    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 0, 299, "_all");
    assertThat(partitionMetadata.size()).isEqualTo(2);

    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 0, 299, "*");
    assertThat(partitionMetadata.size()).isEqualTo(2);

    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 101, 199, "*");
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.get(0)).isEqualTo("1");

    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 201, 299, "*");
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.get(0)).isEqualTo("2");
  }

  @Test
  public void testOneDatasetOnePartition() {
    final String name = "testDataset";
    final String owner = "datasetOwner";
    final long throughputBytes = 1000;
    final DatasetPartitionMetadata partition = new DatasetPartitionMetadata(100, 200, List.of("1"));

    DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughputBytes, List.of(partition), name);

    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    // Start and end time within query window
    List<DatasetPartitionMetadata> partitionMetadata =
        findPartitionsToQuery(datasetMetadataStore, 101, 199, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);

    // End time partially overlapping query window
    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 1, 150, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);

    // End time overlapping entire query window
    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 1, 250, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);

    // Start time at edge of query window
    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 200, 250, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);

    // Start and end time outside of query window
    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 201, 250, name);
    assertThat(partitionMetadata.size()).isEqualTo(0);
  }

  @Test
  public void testOneDatasetMultipleWindows() {
    final String name = "testDataset";
    final String owner = "datasetOwner";
    final long throughputBytes = 1000;
    final DatasetPartitionMetadata partition1 =
        new DatasetPartitionMetadata(100, 200, List.of("1"));

    final DatasetPartitionMetadata partition2 =
        new DatasetPartitionMetadata(201, 300, List.of("2", "3"));

    DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughputBytes, List.of(partition1, partition2), name);

    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    // Fetch first partition between time 101 and 199
    List<DatasetPartitionMetadata> partitionMetadata =
        findPartitionsToQuery(datasetMetadataStore, 101, 199, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).startTimeEpochMs).isEqualTo(100);
    assertThat(partitionMetadata.get(0).endTimeEpochMs).isEqualTo(200);

    // Fetch second partition between time 201 and 300
    partitionMetadata = findPartitionsToQuery(datasetMetadataStore, 201, 300, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(2);
    assertThat(partitionMetadata.get(0).startTimeEpochMs).isEqualTo(201);
    assertThat(partitionMetadata.get(0).endTimeEpochMs).isEqualTo(300);

    // Fetch both partitions
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
  public void testMultipleDatasetsOneTimeRange() {
    final String name = "testDataset";
    final String owner = "datasetOwner";
    final long throughputBytes = 1000;
    final DatasetPartitionMetadata partition = new DatasetPartitionMetadata(100, 200, List.of("1"));

    DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughputBytes, List.of(partition), name);

    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    final String name1 = "testDataset1";
    final String owner1 = "datasetOwner1";
    final long throughputBytes1 = 1;
    final DatasetPartitionMetadata partition1 =
        new DatasetPartitionMetadata(100, 200, List.of("2"));

    DatasetMetadata datasetMetadata1 =
        new DatasetMetadata(name1, owner1, throughputBytes1, List.of(partition1), name1);

    datasetMetadataStore.createSync(datasetMetadata1);
    await().until(() -> datasetMetadataStore.listSync().size() == 2);

    List<DatasetPartitionMetadata> partitionMetadata =
        findPartitionsToQuery(datasetMetadataStore, 101, 199, name);
    assertThat(partitionMetadata.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.size()).isEqualTo(1);
    assertThat(partitionMetadata.get(0).partitions.get(0)).isEqualTo("1");
  }
}
