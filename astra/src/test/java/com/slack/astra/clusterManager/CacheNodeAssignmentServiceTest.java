package com.slack.astra.clusterManager;

import static com.slack.astra.clusterManager.CacheNodeAssignmentService.assign;
import static com.slack.astra.clusterManager.CacheNodeAssignmentService.sortSnapshotsByReplicaCreationTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.astra.metadata.cache.CacheNodeAssignment;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheNodeMetadata;
import com.slack.astra.metadata.cache.CacheNodeMetadataStore;
import com.slack.astra.metadata.core.CuratorBuilder;
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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
            .setZkCacheInitTimeoutMs(1000)
            .build();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(zkConfig)
            .build();

    AstraConfigs.ManagerConfig.CacheNodeAssignmentServiceConfig cacheNodeAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.CacheNodeAssignmentServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1"))
            .setSchedulePeriodMins(1)
            .setMaxConcurrentPerNode(2)
            .build();

    managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setCacheNodeAssignmentServiceConfig(cacheNodeAssignmentServiceConfig)
            .build();

    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    cacheNodeAssignmentStore =
        spy(new CacheNodeAssignmentStore(curatorFramework, metadataStoreConfig, meterRegistry));
    cacheNodeMetadataStore =
        spy(new CacheNodeMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
    snapshotMetadataStore =
        spy(new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
    replicaMetadataStore =
        spy(new ReplicaMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
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
          new SnapshotMetadata("snapshot" + i, 1L, 2L, 10L, "abcd", 5);
      snapshotMetadataStore.createSync(snapshotMetadata);

      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              "replica" + i,
              "snapshot" + i,
              "rep1",
              1L,
              Instant.now().plus(15, ChronoUnit.MINUTES).toEpochMilli(),
              false);
      replicaMetadataStore.createSync(replicaMetadata);
    }
    CacheNodeAssignmentService cacheNodeAssignmentService =
        new CacheNodeAssignmentService(
            meterRegistry,
            managerConfig,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore,
            cacheNodeAssignmentStore);

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
  }

  @Test
  public void testExpiredReplicasMarkedForEvictionLifecycle() throws TimeoutException {
    String name = "foo";
    String snapshotKey = "snapshot_%s";
    String replicaKey = "replica_%s";
    String replicaSet = "rep1";
    String replicaId = "replica1";
    for (int i = 0; i < 3; i++) {
      // create assignments in the past
      CacheNodeAssignment newAssignment =
          new CacheNodeAssignment(
              UUID.randomUUID().toString(),
              name,
              String.format(snapshotKey, i),
              replicaId,
              replicaSet,
              0,
              Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE);
      cacheNodeAssignmentStore.createSync(newAssignment);

      SnapshotMetadata snapshotMetadata =
          new SnapshotMetadata(String.format(snapshotKey, i), 1L, 2L, 10L, "abcd", 5);
      snapshotMetadataStore.createSync(snapshotMetadata);

      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              String.format(replicaKey, i),
              String.format(snapshotKey, i),
              replicaSet,
              1L,
              Instant.now().minusSeconds(120).toEpochMilli(),
              false);
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
            cacheNodeAssignmentStore);
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
    String replicaId = "replica1";
    for (int i = 0; i < 6; i++) {
      // create assignments in the past, half LIVE half EVICT
      CacheNodeAssignment newAssignment =
          new CacheNodeAssignment(
              UUID.randomUUID().toString(),
              name,
              String.format(snapshotKey, i),
              replicaId,
              replicaSet,
              0,
              i % 2 == 0
                  ? Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE
                  : Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICT);
      cacheNodeAssignmentStore.createSync(newAssignment);

      SnapshotMetadata snapshotMetadata =
          new SnapshotMetadata(String.format(snapshotKey, i), 1L, 2L, 10L, "abcd", 5);
      snapshotMetadataStore.createSync(snapshotMetadata);

      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              String.format(replicaKey, i),
              String.format(snapshotKey, i),
              replicaSet,
              1L,
              Instant.now().minusSeconds(120).toEpochMilli(),
              false);
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
            cacheNodeAssignmentStore);
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

  @Test
  public void testShouldntCreateDuplicateAssignments() throws TimeoutException {
    String name = "foo";
    String snapshotId = "snapshot1";
    String replicaSet = "rep1";
    String replicaId = "replica1";

    // Create a CacheNodeAssignment for rep1 with ID: snapshot1
    CacheNodeAssignment existingAssignment =
        new CacheNodeAssignment(
            UUID.randomUUID().toString(),
            name,
            snapshotId,
            replicaId,
            replicaSet,
            0,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE);
    cacheNodeAssignmentStore.createSync(existingAssignment);

    // Create snapshot1 and store it in the store
    SnapshotMetadata snapshotMetadata = new SnapshotMetadata(snapshotId, 1L, 2L, 10L, "abcd", 5);
    snapshotMetadataStore.createSync(snapshotMetadata);

    // Create a replica for snapshot1
    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            "replica1", snapshotId, replicaSet, 1L, Instant.now().toEpochMilli(), false);
    replicaMetadataStore.createSync(replicaMetadata);

    CacheNodeAssignmentService cacheNodeAssignmentService =
        new CacheNodeAssignmentService(
            meterRegistry,
            managerConfig,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore,
            cacheNodeAssignmentStore);

    // Run the service once
    cacheNodeAssignmentService.startAsync();
    cacheNodeAssignmentService.awaitRunning(Duration.ofSeconds(15));

    // Assert assignment created
    await()
        .timeout(10, TimeUnit.SECONDS)
        .until(() -> cacheNodeAssignmentStore.listSync().size() == 1);

    assertThat(cacheNodeAssignmentStore.listSync().size()).isEqualTo(1);
    assertThat(
            cacheNodeAssignmentStore.listSync().stream()
                .map(assignment -> assignment.snapshotId)
                .toList())
        .containsOnly(snapshotId);

    // Run the service once more
    cacheNodeAssignmentService.runOneIteration();

    // Assert no duplicate assignment created
    await()
        .timeout(10, TimeUnit.SECONDS)
        .until(() -> cacheNodeAssignmentStore.listSync().size() == 1);

    assertThat(cacheNodeAssignmentStore.listSync().size()).isEqualTo(1);
    assertThat(
            cacheNodeAssignmentStore.listSync().stream()
                .map(assignment -> assignment.snapshotId)
                .toList())
        .containsOnly(snapshotId);
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
    assertThat(snapshotsToSnapshotIds(result.get("node0").getSnapshots()))
        .contains("snapshot0", "snapshot1");
    assertThat(snapshotsToSnapshotIds(result.get("node1").getSnapshots()))
        .contains("snapshot2", "snapshot3");
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
    assertThat(snapshotsToSnapshotIds(result.get("node0").getSnapshots())).contains("snapshot1");
    assertThat(snapshotsToSnapshotIds(result.get("node1").getSnapshots()))
        .contains("snapshot0", "snapshot2");
    assertThat(snapshotsToSnapshotIds(result.get("NEW_0").getSnapshots())).contains("snapshot3");
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
    assertThat(snapshotsToSnapshotIds(result.get("node0").getSnapshots()))
        .contains("snapshot2", "snapshot3", "snapshot4");
    assertThat(snapshotsToSnapshotIds(result.get("node1").getSnapshots()))
        .contains("snapshot0", "snapshot1");
    assertThat(snapshotsToSnapshotIds(result.get("node2").getSnapshots()))
        .contains("snapshot5", "snapshot6", "snapshot7", "snapshot8", "snapshot9");
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
    assertThat(snapshotsToSnapshotIds(result.get("node0").getSnapshots()))
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
    assertThat(snapshotsToSnapshotIds(result.get("node0").getSnapshots())).isEmpty();
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
    assertThat(result.get("node0").getSnapshots()).isEmpty();
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
    assertThat(snapshotsToSnapshotIds(result.get("node0").getSnapshots()))
        .contains("snapshot3", "snapshot5", "snapshot7");
    assertThat(snapshotsToSnapshotIds(result.get("node1").getSnapshots()))
        .contains("snapshot0", "snapshot2");
    assertThat(snapshotsToSnapshotIds(result.get("node2").getSnapshots())).contains("snapshot1");
  }

  @Test
  public void testComplyWithMaxConcurrentAssignments() {
    String name = "foo";
    for (int i = 0; i < 4; i++) {
      CacheNodeMetadata cacheNodeMetadata = new CacheNodeMetadata(name + i, "foo.com", 6, "rep1");
      cacheNodeMetadataStore.createSync(cacheNodeMetadata);

      SnapshotMetadata snapshotMetadata =
          new SnapshotMetadata("snapshot" + i, 1L, 2L, 10L, "abcd", 1);
      snapshotMetadataStore.createSync(snapshotMetadata);

      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              "replica" + i,
              "snapshot" + i,
              "rep1",
              1L,
              Instant.now().plus(15, ChronoUnit.MINUTES).toEpochMilli(),
              false);
      replicaMetadataStore.createSync(replicaMetadata);
    }
    CacheNodeAssignmentService cacheNodeAssignmentService =
        new CacheNodeAssignmentService(
            meterRegistry,
            managerConfig,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore,
            cacheNodeAssignmentStore);

    cacheNodeAssignmentService.runOneIteration();

    await()
        .timeout(10, TimeUnit.SECONDS)
        .until(() -> cacheNodeAssignmentStore.listSync().size() == 2);

    markAllAssignmentsLive(cacheNodeAssignmentStore, cacheNodeAssignmentStore.listSync());
    cacheNodeAssignmentService.runOneIteration();

    await()
        .timeout(10, TimeUnit.SECONDS)
        .until(() -> cacheNodeAssignmentStore.listSync().size() == 4);
    assertThat(
            cacheNodeAssignmentStore.listSync().stream()
                .map(assignment -> assignment.snapshotId)
                .toList())
        .containsOnly("snapshot0", "snapshot1", "snapshot2", "snapshot3");
  }

  @Test
  public void testAssignMostRecentSnapshotsFirst() {
    String SNAPSHOT_ID_KEY = "snapshot_%s";
    String CACHE_NODE_ID_KEY = "snapshot_%s";
    for (int i = 0; i < 2; i++) {
      CacheNodeMetadata cacheNodeMetadata =
          new CacheNodeMetadata(String.format(CACHE_NODE_ID_KEY, i), "foo.com", 20, "rep1");
      cacheNodeMetadataStore.createSync(cacheNodeMetadata);
    }

    Instant now = Instant.now();
    for (int i = 0; i < 6; i++) {
      SnapshotMetadata snapshotMetadata =
          new SnapshotMetadata(String.format(SNAPSHOT_ID_KEY, i), 1L, 2L, 5L, "abcd", 10);
      snapshotMetadataStore.createSync(snapshotMetadata);

      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              "replica" + i,
              String.format(SNAPSHOT_ID_KEY, i),
              "rep1",
              now.plus(15 * i, ChronoUnit.MINUTES).toEpochMilli(),
              now.plus(15, ChronoUnit.MINUTES).toEpochMilli(),
              false);
      replicaMetadataStore.createSync(replicaMetadata);
    }

    CacheNodeAssignmentService cacheNodeAssignmentService =
        new CacheNodeAssignmentService(
            meterRegistry,
            managerConfig,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore,
            cacheNodeAssignmentStore);

    cacheNodeAssignmentService.runOneIteration();

    await().until(() -> cacheNodeAssignmentStore.listSync().size() == 4);
    Map<String, List<CacheNodeAssignment>> assignmentsByCacheNode =
        cacheNodeAssignmentStore.listSync().stream()
            .collect(Collectors.groupingBy(CacheNodeAssignment::getPartition));

    // assert that assignments contain only 4 most recent snapshots
    assertThat(
            assignmentsByCacheNode.get(String.format(CACHE_NODE_ID_KEY, 0)).stream()
                .map(assignment -> assignment.snapshotId)
                .toList())
        .containsExactlyInAnyOrder(
            String.format(SNAPSHOT_ID_KEY, 0), String.format(SNAPSHOT_ID_KEY, 1));

    assertThat(
            assignmentsByCacheNode.get(String.format(CACHE_NODE_ID_KEY, 1)).stream()
                .map(assignment -> assignment.snapshotId)
                .toList())
        .containsExactlyInAnyOrder(
            String.format(SNAPSHOT_ID_KEY, 2), String.format(SNAPSHOT_ID_KEY, 3));
  }

  @Test
  public void testSortSnapshotsByReplicaCreationTime() {
    List<SnapshotMetadata> snapshots = new ArrayList<>();
    Map<String, ReplicaMetadata> replicas = new HashMap<>();
    String SNAPSHOT_ID_KEY = "snapshot_%s";

    for (int i = 0; i < 4; i++) {
      String snapshotId = String.format(SNAPSHOT_ID_KEY, i);
      SnapshotMetadata snapshotMetadata = new SnapshotMetadata(snapshotId, 1L, 2L, 10L, "abcd", 1);
      snapshots.add(snapshotMetadata);

      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              "replica" + i,
              snapshotId,
              "rep1",
              10 + i,
              Instant.now().plus(15, ChronoUnit.MINUTES).toEpochMilli(),
              false);
      replicas.put(snapshotId, replicaMetadata);
    }
    Collections.shuffle(snapshots);

    sortSnapshotsByReplicaCreationTime(snapshots, replicas);

    assertThat(snapshots.stream().map(snapshot -> snapshot.snapshotId).toList())
        .containsExactly(
            String.format(SNAPSHOT_ID_KEY, 0),
            String.format(SNAPSHOT_ID_KEY, 1),
            String.format(SNAPSHOT_ID_KEY, 2),
            String.format(SNAPSHOT_ID_KEY, 3));
  }

  @Test
  public void testPacksMostEmptyBinsFirst() {
    String SNAPSHOT_ID_KEY = "snapshot_%s";
    String CACHE_NODE_ID_KEY = "snapshot_%s";

    CacheNodeMetadata cacheNodeMetadata =
        new CacheNodeMetadata(String.format(CACHE_NODE_ID_KEY, 0), "foo.com", 200, "rep1");
    cacheNodeMetadataStore.createSync(cacheNodeMetadata);

    CacheNodeMetadata cacheNodeMetadata2 =
        new CacheNodeMetadata(String.format(CACHE_NODE_ID_KEY, 1), "foo.com", 20, "rep1");
    cacheNodeMetadataStore.createSync(cacheNodeMetadata2);

    Instant now = Instant.now();
    for (int i = 0; i < 3; i++) {
      SnapshotMetadata snapshotMetadata =
          new SnapshotMetadata(String.format(SNAPSHOT_ID_KEY, i), 1L, 2L, 5L, "abcd", 10);
      snapshotMetadataStore.createSync(snapshotMetadata);

      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              "replica" + i,
              String.format(SNAPSHOT_ID_KEY, i),
              "rep1",
              now.plus(15 * i, ChronoUnit.MINUTES).toEpochMilli(),
              now.plus(15, ChronoUnit.MINUTES).toEpochMilli(),
              false);
      replicaMetadataStore.createSync(replicaMetadata);
    }

    CacheNodeAssignmentService cacheNodeAssignmentService =
        new CacheNodeAssignmentService(
            meterRegistry,
            managerConfig,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            snapshotMetadataStore,
            cacheNodeAssignmentStore);

    cacheNodeAssignmentService.runOneIteration();

    await().until(() -> cacheNodeAssignmentStore.listSync().size() == 3);
    Map<String, List<CacheNodeAssignment>> assignmentsByCacheNode =
        cacheNodeAssignmentStore.listSync().stream()
            .collect(Collectors.groupingBy(CacheNodeAssignment::getPartition));

    // assert that bigger bin has 1 assignment & smaller bin has 2 assignments
    assertThat(
            assignmentsByCacheNode.get(String.format(CACHE_NODE_ID_KEY, 0)).stream()
                .map(assignment -> assignment.snapshotId)
                .toList())
        .containsExactlyInAnyOrder(String.format(SNAPSHOT_ID_KEY, 2));

    assertThat(
            assignmentsByCacheNode.get(String.format(CACHE_NODE_ID_KEY, 1)).stream()
                .map(assignment -> assignment.snapshotId)
                .toList())
        .containsExactlyInAnyOrder(
            String.format(SNAPSHOT_ID_KEY, 0), String.format(SNAPSHOT_ID_KEY, 1));
  }

  private static void markAllAssignmentsLive(
      CacheNodeAssignmentStore cacheNodeAssignmentStore,
      List<CacheNodeAssignment> cacheNodeAssignments) {
    for (CacheNodeAssignment cacheNodeAssignment : cacheNodeAssignments) {
      cacheNodeAssignmentStore.updateAssignmentState(
          cacheNodeAssignment, Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE);
    }
  }

  private static List<SnapshotMetadata> makeSnapshotsWithSizes(List<Integer> sizes) {
    List<SnapshotMetadata> snapshots = new ArrayList<>();
    for (int i = 0; i < sizes.size(); i++) {
      Integer size = sizes.get(i);
      snapshots.add(new SnapshotMetadata("snapshot" + i, 1, 2 * 1000, 3, "a", size));
    }
    return snapshots;
  }

  // given N capacities, create N cache nodes with respective capacities
  private static List<CacheNodeMetadata> makeCacheNodesWithCapacities(List<Integer> capacities) {
    List<CacheNodeMetadata> cacheNodes = new ArrayList<>();
    for (int i = 0; i < capacities.size(); i++) {
      Integer size = capacities.get(i);
      cacheNodes.add(new CacheNodeMetadata("node" + i, "node" + i + ".com", size, "rep"));
    }

    return cacheNodes;
  }

  private static List<String> snapshotsToSnapshotIds(List<SnapshotMetadata> snapshots) {
    return snapshots.stream().map(snapshot -> snapshot.snapshotId).toList();
  }

  private long filterAssignmentsByState(
      Metadata.CacheNodeAssignment.CacheNodeAssignmentState state) {
    return cacheNodeAssignmentStore.listSync().stream()
        .filter(assignment -> assignment.state == state)
        .count();
  }
}
