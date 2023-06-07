package com.slack.kaldb.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataSerializer;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.server.EphemeralType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KaldbPartitioningMetadataStoreTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(KaldbPartitioningMetadataStoreTest.class);

  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private AsyncCuratorFramework curatorFramework;

  private static final String ZNODE_CONTAINER_CHECK_INTERVAL_MS = "znode.container.checkIntervalMs";
  private final Integer checkInterval = Integer.getInteger(ZNODE_CONTAINER_CHECK_INTERVAL_MS);

  private static class ExampleMetadata extends KaldbPartitionedMetadata {

    private String extraField = "";

    public ExampleMetadata(String name) {
      super(name);
    }

    @JsonCreator
    public ExampleMetadata(
        @JsonProperty("name") String name, @JsonProperty("extraField") String extraField) {
      super(name);
      this.extraField = extraField;
    }

    public String getExtraField() {
      return extraField;
    }

    public void setExtraField(String extraField) {
      this.extraField = extraField;
    }

    @Override
    @JsonIgnore
    public String getPartition() {
      return String.valueOf(Math.abs(name.hashCode() % 10));
    }
  }

  private static class ExampleMetadataSerializer implements MetadataSerializer<ExampleMetadata> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String toJsonStr(ExampleMetadata metadata) {
      try {
        return objectMapper.writeValueAsString(metadata);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public ExampleMetadata fromJsonStr(String data) {
      try {
        return objectMapper.readValue(data, ExampleMetadata.class);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    System.setProperty(ZNODE_CONTAINER_CHECK_INTERVAL_MS, "100");

    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zookeeperConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            // .setZkConnectString("127.0.0.1:2181")
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("Test")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(500)
            .build();
    this.curatorFramework = CuratorBuilder.build(meterRegistry, zookeeperConfig);
  }

  @AfterEach
  public void tearDown() throws IOException {
    curatorFramework.unwrap().close();
    testingServer.close();
    meterRegistry.close();

    System.setProperty(ZNODE_CONTAINER_CHECK_INTERVAL_MS, String.valueOf(checkInterval));
  }

  @Test
  @Disabled("For local benchmarking purposes only")
  void legacy() {
    KaldbMetadataStore<SnapshotMetadata> unpartitionedMetdataStore =
        new KaldbMetadataStore<>(
            curatorFramework,
            CreateMode.PERSISTENT,
            true,
            new SnapshotMetadataSerializer().toModelSerializer(),
            "/unpartitioned_snapshot");

    int size = 50_000;

    for (int i = 1; i <= (size / 1000); i++) {
      for (int j = 0; j < 1000; j++) {
        Instant randomizedStart =
            Instant.now().minus(Duration.of(Math.round(Math.random() * 24), ChronoUnit.HOURS));

        unpartitionedMetdataStore.createAsync(
            new SnapshotMetadata(
                "id" + i + "_" + j,
                "foo",
                randomizedStart.toEpochMilli(),
                randomizedStart.toEpochMilli(),
                10,
                "foo",
                Metadata.IndexType.LOGS_LUCENE9));
      }

      int finalI = i;
      await().until(() -> unpartitionedMetdataStore.listSyncUncached().size() == (finalI * 1000));

      LOG.info("current batch iterator {}", i);
    }

    await()
        .atMost(Duration.ofSeconds(30))
        .until(
            () -> {
              int cacheSize = unpartitionedMetdataStore.listSyncUncached().size();
              LOG.info("current cache size {}", cacheSize);
              return cacheSize == size;
            });
  }

  @Test
  void partitioned() throws IOException {
    try (KaldbPartitioningMetadataStore<ExampleMetadata> partitionedMetdataStore =
        new KaldbPartitioningMetadataStore<>(
            curatorFramework,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_snapshot")) {

      int size = 50_000;

      for (int i = 1; i <= (size / 1000); i++) {
        for (int j = 0; j < 1000; j++) {
          partitionedMetdataStore.createAsync(new ExampleMetadata("id" + i + "_" + j));
        }

        int finalI = i;
        await().until(() -> partitionedMetdataStore.listSync().size() == (finalI * 1000));

        LOG.info("current batch iterator {}", i);
      }

      await()
          .atMost(Duration.ofSeconds(30))
          .until(
              () -> {
                int cacheSize = partitionedMetdataStore.listSync().size();
                LOG.info("current cache size {}", cacheSize);
                return cacheSize == size;
              });
    }
  }

  @Test
  void create() throws IOException {
    try (KaldbPartitioningMetadataStore<ExampleMetadata> partitionedMetdataStore =
        new KaldbPartitioningMetadataStore<>(
            curatorFramework,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_snapshot_2")) {

      ExampleMetadata exampleMetadata = new ExampleMetadata("id");
      partitionedMetdataStore.createSync(exampleMetadata);

      await().until(() -> partitionedMetdataStore.listSync().size() == 1);
      await()
          .until(
              () -> {
                List<ExampleMetadata> snapshotMetadataList = partitionedMetdataStore.listSync();
                return snapshotMetadataList.contains(exampleMetadata)
                    && snapshotMetadataList.size() == 1;
              });
    }
  }

  @Test
  void update() throws IOException {
    try (KaldbPartitioningMetadataStore<ExampleMetadata> partitionedMetadataStore =
        new KaldbPartitioningMetadataStore<>(
            curatorFramework,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_snapshot_update")) {

      ExampleMetadata exampleMetadata = new ExampleMetadata("id");
      partitionedMetadataStore.createAsync(exampleMetadata);

      AtomicReference<List<ExampleMetadata>> exampleMetadataList = new AtomicReference<>();
      await()
          .until(
              () -> {
                exampleMetadataList.set(partitionedMetadataStore.listSync());
                return exampleMetadataList.get().size() == 1;
              });
      assertThat(exampleMetadataList.get()).containsExactly(exampleMetadata);

      exampleMetadata.setExtraField("foo");
      partitionedMetadataStore.updateSync(exampleMetadata);

      await()
          .until(
              () -> Objects.equals(partitionedMetadataStore.listSync().get(0).extraField, "foo"));
    }
  }

  @Test
  void listeners() throws ExecutionException, InterruptedException, IOException {
    try (KaldbPartitioningMetadataStore<ExampleMetadata> partitionedMetadataStore =
        new KaldbPartitioningMetadataStore<>(
            curatorFramework,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_snapshot_listeners")) {

      AtomicInteger counter = new AtomicInteger(0);
      partitionedMetadataStore.addListener((metadata) -> counter.incrementAndGet());

      int size = 100;
      Queue<ExampleMetadata> addedMetadata = new ArrayBlockingQueue<>(size);
      for (int i = 1; i <= size; i++) {
        Instant randomizedStart =
            Instant.now().minus(Duration.of(Math.round(Math.random() * 10), ChronoUnit.HOURS));

        ExampleMetadata exampleMetadata = new ExampleMetadata("id" + i);
        partitionedMetadataStore.createAsync(exampleMetadata);
        addedMetadata.add(exampleMetadata);
      }

      await()
          .until(
              () -> {
                int currentSize = partitionedMetadataStore.listSync().size();
                LOG.info("Current size - {}", currentSize);
                return currentSize == size;
              });
      assertThat(counter.get()).isGreaterThanOrEqualTo(size);

      List<String> partitions =
          curatorFramework
              .getChildren()
              .forPath("/partitioned_snapshot_listeners")
              .toCompletableFuture()
              .get();

      partitions.forEach(
          partition -> {
            curatorFramework
                .checkExists()
                .forPath("/partitioned_snapshot_listeners/" + partition)
                .thenAccept(
                    stat -> {
                      EphemeralType ephemeralType = EphemeralType.get(stat.getEphemeralOwner());
                      // todo - this is not clear why this is reported as a VOID type when
                      // inspecting
                      //  the nodes created. The persisted type is correct, but upon fetching later
                      //  it appears unset. This behavior is consistent directly using ZK or via
                      //  Curator, so we cannot do asserts on the node type here.
                      LOG.info("Curator checked ephemeralType - {}", ephemeralType.toString());
                    });
          });

      LOG.info("Deleting nodes");
      for (int i = 0; i < 50; i++) {
        ExampleMetadata toRemove = addedMetadata.remove();
        partitionedMetadataStore.deleteAsync(toRemove);
      }

      await().until(() -> partitionedMetadataStore.listSync().size() == 50);
      assertThat(counter.get())
          // todo - why can this be larger than 150? Extra bucket event again?
          .isGreaterThanOrEqualTo(150);

      for (int i = 0; i < 50; i++) {
        ExampleMetadata toRemove = addedMetadata.remove();
        partitionedMetadataStore.deleteAsync(toRemove);
      }

      await().until(() -> partitionedMetadataStore.listSync().size() == 0);
      // todo - explain why 211 (200 + 11 bucket events)?
      // todo - should we be including bucket events?
      await().until(counter::get, (value) -> value >= 200);

      await()
          .until(
              () -> {
                if (curatorFramework
                        .checkExists()
                        .forPath("/partitioned_snapshot_listeners")
                        .toCompletableFuture()
                        .get()
                    == null) {
                  LOG.info("Parent node no longer exists");
                  return true;
                }

                int childrenSize =
                    curatorFramework
                        .getChildren()
                        .forPath("/partitioned_snapshot_listeners")
                        .toCompletableFuture()
                        .get()
                        .size();
                LOG.info("Children size - {}", childrenSize);
                return childrenSize == 0;
              });
    }
  }
}
