package com.slack.astra.clusterManager;

import static com.slack.astra.clusterManager.CacheNodeAssignmentService.CACHE_HPA_METRIC_NAME;
import static com.slack.astra.clusterManager.CacheNodeAssignmentService.assign;
import static com.slack.astra.proto.metadata.Metadata.IndexType.LOGS_LUCENE9;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.astra.metadata.cache.CacheNodeAssignment;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheNodeMetadata;
import com.slack.astra.metadata.cache.CacheNodeMetadataStore;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.hpa.HpaMetricMetadataStore;
import com.slack.astra.metadata.replica.ReplicaMetadata;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CacheNodeAssignmentServiceTest {

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private AsyncCuratorFramework curatorFramework;
  private CacheNodeAssignmentStore cacheNodeAssignmentStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private AstraConfigs.ManagerConfig managerConfig;
  private CacheNodeMetadataStore cacheNodeMetadataStore;
  private ReplicaMetadataStore replicaMetadataStore;
  private HpaMetricMetadataStore hpaMetricMetadataStore;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    com.slack.astra.proto.config.AstraConfigs.ZookeeperConfig zkConfig =
        com.slack.astra.proto.config.AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("CacheNodeAssignmentServiceTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    AstraConfigs.ManagerConfig.CacheNodeAssignmentServiceConfig cacheNodeAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.CacheNodeAssignmentServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1"))
            .setSchedulePeriodMins(1)
            .setReplicaLifespanMins(60)
            .build();

    managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setCacheNodeAssignmentServiceConfig(cacheNodeAssignmentServiceConfig)
            .build();

    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    cacheNodeAssignmentStore = spy(new CacheNodeAssignmentStore(curatorFramework));
    cacheNodeMetadataStore = spy(new CacheNodeMetadataStore(curatorFramework));
    snapshotMetadataStore = spy(new SnapshotMetadataStore(curatorFramework));
    replicaMetadataStore = spy(new ReplicaMetadataStore(curatorFramework));
    hpaMetricMetadataStore = spy(new HpaMetricMetadataStore(curatorFramework, false));
  }

  @AfterEach
  public void tearDown() throws IOException {
    meterRegistry.close();
    testingServer.close();
    cacheNodeAssignmentStore.close();
    curatorFramework.unwrap().close();
  }

  @Test
  public void testBasicLifecycle() throws Exception {
    String name = "foo";
    for (int i = 0; i < 3; i++) {
      CacheNodeMetadata cacheNodeMetadata = new CacheNodeMetadata(name + i, "foo.com", 5, "rep1");
      cacheNodeMetadataStore.createSync(cacheNodeMetadata);

      SnapshotMetadata snapshotMetadata =
          new SnapshotMetadata(
              "snapshot" + i, "snapshot" + i, 1L, 2L, 10L, "abcd", LOGS_LUCENE9, 5);
      snapshotMetadataStore.createSync(snapshotMetadata);

      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata("replica" + i, "snapshot" + i, "rep1", 1L, 2L, false, LOGS_LUCENE9);
      replicaMetadataStore.createSync(replicaMetadata);
    }
    CacheNodeAssignmentService cacheNodeAssignmentService =
        new CacheNodeAssignmentService(
            meterRegistry,
            managerConfig,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore,
            cacheNodeAssignmentStore,
            hpaMetricMetadataStore);

    cacheNodeAssignmentService.startAsync();
    cacheNodeAssignmentService.awaitRunning(Duration.ofSeconds(15));

    await()
        .timeout(10, TimeUnit.SECONDS)
        .until(() -> cacheNodeAssignmentStore.listSync().size() == 3);
    assertThat(
            cacheNodeAssignmentStore.listSync().stream()
                .map(assignment -> assignment.snapshotId)
                .toList())
        .containsOnly("snapshot0", "snapshot1", "snapshot2");
    assertThat(hpaMetricMetadataStore.getSync(String.format(CACHE_HPA_METRIC_NAME, "rep1")).value)
        .isEqualTo(1.0);
  }

  @Test
  public void testExpiredReplicasMarkedForEvictionLifecycle() throws TimeoutException {
    String name = "foo";
    String snapshotKey = "snapshot_%s";
    String replicaKey = "replica_%s";
    String replicaSet = "rep1";
    for (int i = 0; i < 3; i++) {
      // create assignments in the past
      CacheNodeAssignment newAssignment =
          new CacheNodeAssignment(
              UUID.randomUUID().toString(),
              name,
              String.format(snapshotKey, i),
              replicaSet,
              Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE);
      cacheNodeAssignmentStore.createSync(newAssignment);

      SnapshotMetadata snapshotMetadata =
          new SnapshotMetadata(
              String.format(snapshotKey, i),
              String.format(snapshotKey, i),
              1L,
              2L,
              10L,
              "abcd",
              LOGS_LUCENE9,
              5);
      snapshotMetadataStore.createSync(snapshotMetadata);

      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              String.format(replicaKey, i),
              String.format(snapshotKey, i),
              replicaSet,
              1L,
              Instant.now().minusSeconds(120).toEpochMilli(),
              false,
              LOGS_LUCENE9);
      replicaMetadataStore.createSync(replicaMetadata);
    }

    assertThat(cacheNodeAssignmentStore.listSync().size()).isEqualTo(3);

    CacheNodeAssignmentService cacheNodeAssignmentService =
        new CacheNodeAssignmentService(
            meterRegistry,
            managerConfig,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore,
            cacheNodeAssignmentStore,
            hpaMetricMetadataStore);
    cacheNodeAssignmentService.startAsync();
    cacheNodeAssignmentService.awaitRunning(Duration.ofSeconds(15));

    await()
        .timeout(20, TimeUnit.SECONDS)
        .until(() -> cacheNodeAssignmentStore.listSync().isEmpty());
  }

  @Test
  public void testEvictExpiredReplicasOnly() {
    String name = "foo";
    String snapshotKey = "snapshot_%s";
    String replicaKey = "replica_%s";
    String replicaSet = "rep1";
    for (int i = 0; i < 6; i++) {
      // create assignments in the past, half LIVE half EVICT
      CacheNodeAssignment newAssignment =
          new CacheNodeAssignment(
              UUID.randomUUID().toString(),
              name,
              String.format(snapshotKey, i),
              replicaSet,
              i % 2 == 0
                  ? Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE
                  : Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICT);
      cacheNodeAssignmentStore.createSync(newAssignment);

      SnapshotMetadata snapshotMetadata =
          new SnapshotMetadata(
              String.format(snapshotKey, i),
              String.format(snapshotKey, i),
              1L,
              2L,
              10L,
              "abcd",
              LOGS_LUCENE9,
              5);
      snapshotMetadataStore.createSync(snapshotMetadata);

      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              String.format(replicaKey, i),
              String.format(snapshotKey, i),
              replicaSet,
              1L,
              Instant.now().minusSeconds(120).toEpochMilli(),
              false,
              LOGS_LUCENE9);
      replicaMetadataStore.createSync(replicaMetadata);
    }

    assertThat(
            filterAssignmentsByState(Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICT))
        .isEqualTo(3);
    assertThat(filterAssignmentsByState(Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE))
        .isEqualTo(3);

    CacheNodeAssignmentService cacheNodeAssignmentService =
        new CacheNodeAssignmentService(
            meterRegistry,
            managerConfig,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore,
            cacheNodeAssignmentStore,
            hpaMetricMetadataStore);
    cacheNodeAssignmentService.markAssignmentsForEviction(
        cacheNodeAssignmentStore.listSync(),
        replicaMetadataStore.listSync().stream()
            .collect(Collectors.toMap(replica -> replica.snapshotId, Function.identity())),
        Instant.now());

    await()
        .timeout(20, TimeUnit.SECONDS)
        .until(
            () ->
                filterAssignmentsByState(
                        Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICT)
                    == 6);
  }

  /* Test Case 1: Simple Case with Exact Fit
  - Items: [5, 5, 5, 5]
  - Initial Bins: [10, 10]
  - Expected Output: Initial bins: [10: 5, 5], [10: 5, 5]
    - Explanation: Each bin can exactly fit two items of weight 5.
  */
  @Test
  public void testAssignmentSimpleFit() {
    List<CacheNodeMetadata> cacheNodes = makeCacheNodesWithCapacities(List.of(10, 10));
    List<SnapshotMetadata> snapshots = makeSnapshotsWithSizes(List.of(5, 5, 5, 5));

    Map<String, CacheNodeBin> result =
        assign(cacheNodeAssignmentStore, snapshotMetadataStore, List.of(), snapshots, cacheNodes);

    assertThat(result.size()).isEqualTo(2);
    assertThat(result.get("node0").getSnapshotIds()).contains("snapshot0", "snapshot1");
    assertThat(result.get("node1").getSnapshotIds()).contains("snapshot2", "snapshot3");
  }

  /* Test Case 2: Simple Case with Excess Capacity
  - Items: [3, 7, 2, 5]
  - Initial Bins: [8, 6]
  - Expected Output: Initial bins: [8: 3, 5], [6: 2], New bins: [7]
    - Explanation: The first bin is filled with items of weight 3 and 5, the second bin with 2, and a new bin with 7.
  */
  @Test
  public void testAssignmentExcessCapacity() {
    List<CacheNodeMetadata> cacheNodes = makeCacheNodesWithCapacities(List.of(8, 6));
    List<SnapshotMetadata> snapshots = makeSnapshotsWithSizes(List.of(3, 7, 2, 5));

    Map<String, CacheNodeBin> result =
        assign(cacheNodeAssignmentStore, snapshotMetadataStore, List.of(), snapshots, cacheNodes);

    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get("node0").getSnapshotIds()).contains("snapshot0", "snapshot2");
    assertThat(result.get("node1").getSnapshotIds()).contains("snapshot3");
    assertThat(result.get("NEW_0").getSnapshotIds()).contains("snapshot1");
  }

  /* Test Case 3: Multiple Small Items
  - Items: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
  - Initial Bins: [3, 3, 2]
  - Expected Output: Initial bins: [3: 1, 1, 1], [3: 1, 1, 1], [2: 1, 1], New bins: [3: 1, 1, 1]
    - Explanation: The first three bins can hold six items, and a new bin is needed for the remaining four items.
  */
  @Test
  public void testAssignmentUniformSizes() {
    List<CacheNodeMetadata> cacheNodes = makeCacheNodesWithCapacities(List.of(3, 2, 5));
    List<SnapshotMetadata> snapshots =
        makeSnapshotsWithSizes(List.of(1, 1, 1, 1, 1, 1, 1, 1, 1, 1));

    Map<String, CacheNodeBin> result =
        assign(cacheNodeAssignmentStore, snapshotMetadataStore, List.of(), snapshots, cacheNodes);

    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get("node0").getSnapshotIds())
        .contains("snapshot5", "snapshot6", "snapshot7");
    assertThat(result.get("node1").getSnapshotIds()).contains("snapshot8", "snapshot9");
    assertThat(result.get("node2").getSnapshotIds())
        .contains("snapshot3", "snapshot2", "snapshot1", "snapshot0", "snapshot4");
  }

  /* Test Case 5: All Items Fit into One Bin
  - Items: [2, 2, 2, 2, 2]
  - Initial Bins: [10]
  - Expected Output: Initial bins: [10: 2, 2, 2, 2, 2]
    - Explanation: All items fit perfectly into the initial bin.
  */
  @Test
  public void testAssignmentPerfectFit() {
    List<CacheNodeMetadata> cacheNodes = makeCacheNodesWithCapacities(List.of(10));
    List<SnapshotMetadata> snapshots = makeSnapshotsWithSizes(List.of(2, 2, 2, 2, 2));

    Map<String, CacheNodeBin> result =
        assign(cacheNodeAssignmentStore, snapshotMetadataStore, List.of(), snapshots, cacheNodes);

    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get("node0").getSnapshotIds())
        .contains("snapshot0", "snapshot1", "snapshot2", "snapshot3", "snapshot4");
  }

  /* Test Case 7: No Items
  - Items: []
  - Initial Bins: [10]
  - Expected Output: Initial bins: []
    - Explanation: No items mean no bins are needed, even if they exist.
  */
  @Test
  public void testAssignmentNoItems() {
    List<CacheNodeMetadata> cacheNodes = makeCacheNodesWithCapacities(List.of(10));
    List<SnapshotMetadata> snapshots = makeSnapshotsWithSizes(List.of());

    Map<String, CacheNodeBin> result =
        assign(cacheNodeAssignmentStore, snapshotMetadataStore, List.of(), snapshots, cacheNodes);

    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get("node0").getSnapshotIds()).isEmpty();
  }

  /* Test Case 8: Single Item Larger than Bin Capacity
  - Items: [12]
  - Initial Bins: [10]
  - Expected Output: Initial bins: [], New bins: [12]
    - Explanation: The item cannot fit into the initial bin, so a new bin is created.
  */
  @Test
  public void testAssignmentItemLargerThanBin() {
    List<CacheNodeMetadata> cacheNodes = makeCacheNodesWithCapacities(List.of(10));
    List<SnapshotMetadata> snapshots = makeSnapshotsWithSizes(List.of(12));

    Map<String, CacheNodeBin> result =
        assign(cacheNodeAssignmentStore, snapshotMetadataStore, List.of(), snapshots, cacheNodes);

    assertThat(result.size()).isEqualTo(2);
    assertThat(result.get("node0").getSnapshotIds()).isEmpty();
  }

  /* Test Case 10: Complex Combination
  - Items: [4, 8, 1, 4, 7, 3, 6, 2, 5]
  - Initial Bins: [10, 5, 8]
  - Expected Output: Initial bins: [10: 4, 6], [5: 5], [8: 4, 3], New bins: [8: 8], [7: 7], [2: 2], [1: 1]
    - Explanation: The items are packed into the first initial bins that have enough space, and new bins are created as needed.
  */
  @Test
  public void testAssignmentComplicatedCombination() {
    List<CacheNodeMetadata> cacheNodes = makeCacheNodesWithCapacities(List.of(10, 5, 8));
    List<SnapshotMetadata> snapshots = makeSnapshotsWithSizes(List.of(4, 8, 1, 4, 7, 3, 6, 2, 5));

    Map<String, CacheNodeBin> result =
        assign(cacheNodeAssignmentStore, snapshotMetadataStore, List.of(), snapshots, cacheNodes);

    assertThat(result.size()).isEqualTo(6);
    assertThat(result.get("node0").getSnapshotIds()).contains("snapshot1", "snapshot7");
    assertThat(result.get("node1").getSnapshotIds()).contains("snapshot3");
    assertThat(result.get("node2").getSnapshotIds())
        .contains("snapshot0", "snapshot2", "snapshot5");
  }

  private List<SnapshotMetadata> makeSnapshotsWithSizes(List<Integer> sizes) {
    List<SnapshotMetadata> snapshots = new ArrayList<>();
    for (int i = 0; i < sizes.size(); i++) {
      Integer size = sizes.get(i);
      snapshots.add(
          new SnapshotMetadata("snapshot" + i, "/" + i, 1, 2 * 1000, 3, "a", LOGS_LUCENE9, size));
    }
    return snapshots;
  }

  // given N capacities, create N cache nodes with respective capacities
  private List<CacheNodeMetadata> makeCacheNodesWithCapacities(List<Integer> capacities) {
    List<CacheNodeMetadata> cacheNodes = new ArrayList<>();
    for (int i = 0; i < capacities.size(); i++) {
      Integer size = capacities.get(i);
      cacheNodes.add(new CacheNodeMetadata("node" + i, "node" + i + ".com", size, "rep"));
    }

    return cacheNodes;
  }

  private long filterAssignmentsByState(
      Metadata.CacheNodeAssignment.CacheNodeAssignmentState state) {
    return cacheNodeAssignmentStore.listSync().stream()
        .filter(assignment -> assignment.state == state)
        .count();
  }
}
