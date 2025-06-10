package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
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

class ZookeeperPartitioningMetadataStoreTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ZookeeperPartitioningMetadataStoreTest.class);

  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private AsyncCuratorFramework curatorFramework;

  private AstraConfigs.ZookeeperConfig zkConfig;

  private static final String ZNODE_CONTAINER_CHECK_INTERVAL_MS = "znode.container.checkIntervalMs";
  private final Integer checkInterval = Integer.getInteger(ZNODE_CONTAINER_CHECK_INTERVAL_MS);

  private static class ExampleMetadata extends AstraPartitionedMetadata {

    private String extraField = null;

    public ExampleMetadata(String name) {
      super(name);
    }

    @JsonCreator
    public ExampleMetadata(
        @JsonProperty("name") String name, @JsonProperty("extraField") String extraField) {
      super(name);
      this.extraField = extraField;
    }

    public void setExtraField(String extraField) {
      this.extraField = extraField;
    }

    public String getExtraField() {
      return extraField;
    }

    @Override
    @JsonIgnore
    public String getPartition() {
      // Use up to 10 partitions
      return String.valueOf(Math.abs(name.hashCode() % 10));
    }

    @Override
    public String toString() {
      return "ExampleMetadata{"
          + "extraField='"
          + extraField
          + '\''
          + ", name='"
          + name
          + '\''
          + '}';
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

  static class FixedPartitionExampleMetadata extends AstraPartitionedMetadata {

    private final String partitionId;
    private String extraField = null;

    public FixedPartitionExampleMetadata(String name, String partitionId) {
      super(name);
      this.partitionId = partitionId;
    }

    @JsonCreator
    public FixedPartitionExampleMetadata(
        @JsonProperty("name") String name,
        @JsonProperty("extraField") String extraField,
        @JsonProperty("partitionId") String partitionId) {
      super(name);
      this.extraField = extraField;
      this.partitionId = partitionId;
    }

    public void setExtraField(String extraField) {
      this.extraField = extraField;
    }

    public String getExtraField() {
      return extraField;
    }

    @Override
    @JsonIgnore
    public String getPartition() {
      // use a fixed partition
      return partitionId;
    }

    @Override
    public String toString() {
      return "FixedPartitionExampleMetadata{"
          + "extraField='"
          + extraField
          + '\''
          + ", name='"
          + name
          + '\''
          + '}';
    }
  }

  static class FixedPartitionMetadataSerializer
      implements MetadataSerializer<FixedPartitionExampleMetadata> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String toJsonStr(FixedPartitionExampleMetadata metadata) {
      try {
        return objectMapper.writeValueAsString(metadata);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public FixedPartitionExampleMetadata fromJsonStr(String data) {
      try {
        return objectMapper.readValue(data, FixedPartitionExampleMetadata.class);
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

    zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            // .setZkConnectString("127.0.0.1:2181")
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("Test")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(500)
            .setZkCacheInitTimeoutMs(1000)
            .build();
    this.curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
  }

  @AfterEach
  public void tearDown() throws IOException {
    System.setProperty(ZNODE_CONTAINER_CHECK_INTERVAL_MS, String.valueOf(checkInterval));
    curatorFramework.unwrap().close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  void testLargeNumberOfZNodes() throws IOException {
    try (ZookeeperPartitioningMetadataStore<ExampleMetadata> partitionedMetadataStore =
        new ZookeeperPartitioningMetadataStore<>(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_znodes")) {

      int size = 50_000;

      for (int i = 1; i <= (size / 1000); i++) {
        for (int j = 0; j < 1000; j++) {
          partitionedMetadataStore.createAsync(new ExampleMetadata("id" + i + "_" + j));
        }

        int finalI = i;
        await().until(() -> partitionedMetadataStore.listSync().size() == (finalI * 1000));

        LOG.info("current batch iterator {}", i);
      }

      await()
          .atMost(Duration.ofSeconds(30))
          .until(
              () -> {
                int cacheSize = partitionedMetadataStore.listSync().size();
                LOG.info("current cache size {}", cacheSize);
                return cacheSize == size;
              });
    }
  }

  @Test
  void testCreate() throws IOException {
    try (ZookeeperPartitioningMetadataStore<ExampleMetadata> partitionedMetadataStore =
        new ZookeeperPartitioningMetadataStore<>(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_create")) {

      ExampleMetadata exampleMetadata = new ExampleMetadata("id");
      partitionedMetadataStore.createSync(exampleMetadata);

      await().until(() -> partitionedMetadataStore.listSync().size() == 1);
      await()
          .until(
              () -> {
                List<ExampleMetadata> snapshotMetadataList = partitionedMetadataStore.listSync();
                return snapshotMetadataList.contains(exampleMetadata)
                    && snapshotMetadataList.size() == 1;
              });
    }
  }

  @Test
  void testUpdate() throws IOException {
    try (ZookeeperPartitioningMetadataStore<ExampleMetadata> partitionedMetadataStore =
        new ZookeeperPartitioningMetadataStore<>(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_update")) {

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
              () ->
                  Objects.equals(
                      partitionedMetadataStore.listSync().get(0).getExtraField(), "foo"));
    }
  }

  @Test
  void testDelete() throws IOException {
    try (ZookeeperPartitioningMetadataStore<ExampleMetadata> partitionedMetadataStore =
        new ZookeeperPartitioningMetadataStore<>(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_delete")) {

      ExampleMetadata exampleMetadata = new ExampleMetadata("id");
      partitionedMetadataStore.createSync(exampleMetadata);

      await().until(() -> partitionedMetadataStore.listSync().size() == 1);
      await()
          .until(
              () -> {
                List<ExampleMetadata> snapshotMetadataList = partitionedMetadataStore.listSync();
                return snapshotMetadataList.contains(exampleMetadata)
                    && snapshotMetadataList.size() == 1;
              });

      partitionedMetadataStore.deleteSync(exampleMetadata);
      await().until(() -> partitionedMetadataStore.listSync().isEmpty());
    }
  }

  @Test
  void testFind() throws IOException {
    try (ZookeeperPartitioningMetadataStore<ExampleMetadata> partitionedMetadataStore =
        new ZookeeperPartitioningMetadataStore<>(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_find")) {

      String nodeName = "findme";
      ExampleMetadata exampleMetadataToFindLater = new ExampleMetadata(nodeName);
      partitionedMetadataStore.createSync(exampleMetadataToFindLater);
      for (int i = 0; i < 19; i++) {
        partitionedMetadataStore.createSync(new ExampleMetadata("node" + i));
      }
      await().until(() -> partitionedMetadataStore.listSync().size() == 20);

      ExampleMetadata exampleMetadataFound = partitionedMetadataStore.findSync(nodeName);
      assertThat(exampleMetadataToFindLater).isEqualTo(exampleMetadataFound);
    }
  }

  @Test
  void testFindMissingNode() throws IOException {
    try (ZookeeperPartitioningMetadataStore<ExampleMetadata> partitionedMetadataStore =
        new ZookeeperPartitioningMetadataStore<>(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_find_missing")) {

      assertThatExceptionOfType(InternalMetadataStoreException.class)
          .isThrownBy(() -> partitionedMetadataStore.findSync("missing"));
    }
  }

  @Test
  void testDuplicateCreate() throws IOException {
    try (ZookeeperPartitioningMetadataStore<ExampleMetadata> partitionedMetadataStore =
        new ZookeeperPartitioningMetadataStore<>(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_snapshot_duplicate_create")) {
      ExampleMetadata exampleMetadata = new ExampleMetadata("name", "field");
      partitionedMetadataStore.createSync(exampleMetadata);
      await().until(() -> partitionedMetadataStore.listSync().size() == 1);

      assertThatExceptionOfType(InternalMetadataStoreException.class)
          .isThrownBy(() -> partitionedMetadataStore.createSync(exampleMetadata));
    }
  }

  @Test
  void testEphemeralNodeBehavior() throws IOException {
    class PersistentMetadataStore extends ZookeeperPartitioningMetadataStore<ExampleMetadata> {
      public PersistentMetadataStore() {
        super(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_persistent");
      }
    }

    class EphemeralMetadataStore extends ZookeeperPartitioningMetadataStore<ExampleMetadata> {
      public EphemeralMetadataStore() {
        super(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.EPHEMERAL,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_ephemeral");
      }
    }

    ExampleMetadata metadata1 = new ExampleMetadata("foo", "va1");
    try (ZookeeperPartitioningMetadataStore<ExampleMetadata> persistentStore =
        new PersistentMetadataStore()) {
      // create metadata
      persistentStore.createSync(metadata1);

      // do a non-cached list to ensure node has been persisted
      assertThat(AstraMetadataTestUtils.listSyncUncached(persistentStore))
          .containsExactly(metadata1);
    }

    ExampleMetadata metadata2 = new ExampleMetadata("foo", "val1");
    try (ZookeeperPartitioningMetadataStore<ExampleMetadata> ephemeralStore =
        new EphemeralMetadataStore()) {
      // create metadata
      ephemeralStore.createSync(metadata2);

      // do a non-cached list to ensure node has been persisted
      assertThat(AstraMetadataTestUtils.listSyncUncached(ephemeralStore))
          .containsExactly(metadata2);
    }

    // close curator, and then instantiate a new copy
    // This is because we cannot restart the closed curator.
    curatorFramework.unwrap().close();
    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);

    try (ZookeeperPartitioningMetadataStore<ExampleMetadata> persistentStore =
        new PersistentMetadataStore()) {
      assertThat(persistentStore.getSync(metadata1.getPartition(), "foo")).isEqualTo(metadata1);
    }

    try (ZookeeperPartitioningMetadataStore<ExampleMetadata> ephemeralStore =
        new EphemeralMetadataStore()) {
      assertThatExceptionOfType(InternalMetadataStoreException.class)
          .isThrownBy(() -> ephemeralStore.getSync(metadata2.getPartition(), "foo"));
    }
  }

  @Test
  @Disabled("ZK reconnect support currently disabled")
  void testListenersWithZkReconnect() throws Exception {
    class TestMetadataStore extends ZookeeperPartitioningMetadataStore<ExampleMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_zk_reconnect");
      }
    }

    try (ZookeeperPartitioningMetadataStore<ExampleMetadata> store = new TestMetadataStore()) {
      AtomicInteger counter = new AtomicInteger(0);
      AstraMetadataStoreChangeListener<ExampleMetadata> listener =
          (testMetadata) -> counter.incrementAndGet();
      store.addListener(listener);

      await().until(() -> counter.get() == 0);

      // create metadata
      ExampleMetadata metadata1 = new ExampleMetadata("foo", "val1");
      store.createSync(metadata1);

      await().until(() -> counter.get() == 1);

      testingServer.restart();

      assertThat(store.getSync(metadata1.getPartition(), metadata1.name)).isEqualTo(metadata1);
      assertThat(counter.get()).isEqualTo(1);

      metadata1.setExtraField("val2");
      store.updateSync(metadata1);

      await().until(() -> counter.get() == 2);
    }
  }

  @Test
  void testListenersOnAddRemoveNodes()
      throws ExecutionException, InterruptedException, IOException {
    try (ZookeeperPartitioningMetadataStore<ExampleMetadata> partitionedMetadataStore =
        new ZookeeperPartitioningMetadataStore<>(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_snapshot_listeners")) {

      AtomicInteger counter = new AtomicInteger(0);
      partitionedMetadataStore.addListener(
          (metadata) -> {
            counter.incrementAndGet();
          });

      int size = 100;
      Queue<ExampleMetadata> addedMetadata = new ArrayBlockingQueue<>(size);
      for (int i = 1; i <= size; i++) {
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

      // this can be higher than 100, due to container znodes contributing events during clean
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
                      // This is not clear why this is reported as a VOID type when inspecting the
                      // nodes created. The persisted type is correct, but upon fetching later it
                      // appears unset. This behavior is consistent directly using ZK or via
                      // Curator, so we cannot do asserts on the node type here.
                      LOG.info("Curator checked ephemeralType - {}", ephemeralType.toString());
                    });
          });

      LOG.info("Deleting nodes");
      for (int i = 0; i < 50; i++) {
        ExampleMetadata toRemove = addedMetadata.remove();
        partitionedMetadataStore.deleteAsync(toRemove);
      }

      await().until(() -> partitionedMetadataStore.listSync().size() == 50);
      // this can be higher than 150, due to container znodes contributing events during clean
      assertThat(counter.get()).isGreaterThanOrEqualTo(150);

      for (int i = 0; i < 50; i++) {
        ExampleMetadata toRemove = addedMetadata.remove();
        partitionedMetadataStore.deleteAsync(toRemove);
      }

      await().until(() -> partitionedMetadataStore.listSync().size() == 0);
      // this can be higher than 200, due to container znodes contributing events during clean
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

  @Test
  void testAddRemoveListener() throws Exception {
    class TestMetadataStore extends ZookeeperPartitioningMetadataStore<ExampleMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.PERSISTENT,
            new ExampleMetadataSerializer().toModelSerializer(),
            "/partitioned_add_remove_listeners");
      }
    }

    try (ZookeeperPartitioningMetadataStore<ExampleMetadata> store = new TestMetadataStore()) {
      AtomicInteger counter = new AtomicInteger(0);
      AstraMetadataStoreChangeListener<ExampleMetadata> listener =
          (testMetadata) -> counter.incrementAndGet();
      store.addListener(listener);

      await().until(() -> counter.get() == 0);

      // create metadata
      ExampleMetadata metadata1 = new ExampleMetadata("foo", "val1");
      store.createSync(metadata1);

      await().until(() -> counter.get() == 1);

      assertThat(store.getSync(metadata1.getPartition(), metadata1.name)).isEqualTo(metadata1);
      assertThat(counter.get()).isEqualTo(1);

      metadata1.setExtraField("val2");
      store.updateSync(metadata1);

      await().until(() -> counter.get() == 2);

      store.removeListener(listener);
      metadata1.setExtraField("val3");
      store.updateSync(metadata1);

      // Sleep here to verify that we're not still firing and it's just slow / another thread
      Thread.sleep(2000);
      assertThat(counter.get()).isEqualTo(2);
    }
  }

  @Test
  void testListenerNotDuplicatedAddingBeforeDuring() throws Exception {
    class TestMetadataStore
        extends ZookeeperPartitioningMetadataStore<FixedPartitionExampleMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.PERSISTENT,
            new FixedPartitionMetadataSerializer().toModelSerializer(),
            "/partitioned_duplicate_listeners");
      }
    }

    try (ZookeeperPartitioningMetadataStore<FixedPartitionExampleMetadata> store =
        new TestMetadataStore()) {
      AtomicInteger beforeCounter = new AtomicInteger(0);
      AstraMetadataStoreChangeListener<FixedPartitionExampleMetadata> beforeListener =
          (testMetadata) -> {
            LOG.info("testMetadata - {}", testMetadata);
            beforeCounter.incrementAndGet();
          };
      store.addListener(beforeListener);

      AtomicInteger afterCounter = new AtomicInteger(0);
      FixedPartitionExampleMetadata metadata0 =
          new FixedPartitionExampleMetadata("foo0", "val1", "1");
      store.createSync(metadata0);

      FixedPartitionExampleMetadata metadata1 =
          new FixedPartitionExampleMetadata("foo1", "val1", "1");
      store.createSync(metadata1);

      // create metadata
      for (int i = 2; i < 10; i++) {
        FixedPartitionExampleMetadata otherMetadata =
            new FixedPartitionExampleMetadata("foo" + i, "val1", "1");
        store.createSync(otherMetadata);
      }

      await()
          .until(
              () -> store.listSync().size(),
              (size) -> {
                LOG.info("size - {}", size);
                return size == 10;
              });

      AstraMetadataStoreChangeListener<FixedPartitionExampleMetadata> afterListener =
          (testMetadata) -> {
            LOG.info("testMetadata - {}", testMetadata);
            afterCounter.incrementAndGet();
          };
      store.addListener(afterListener);

      metadata0.setExtraField("val2");
      store.updateSync(metadata0);

      metadata1.setExtraField("val2");
      store.updateSync(metadata1);

      await().until(beforeCounter::get, (value) -> value == 12);
      await().until(afterCounter::get, (value) -> value == 2);
    }
  }

  @Test
  void testPartitionFilters() throws Exception {
    final String partitionStoreFolder = "/test_partition_filters";

    class TestMetadataStore
        extends ZookeeperPartitioningMetadataStore<FixedPartitionExampleMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.PERSISTENT,
            new FixedPartitionMetadataSerializer().toModelSerializer(),
            partitionStoreFolder);
      }
    }

    class FilteredTestMetadataStore
        extends ZookeeperPartitioningMetadataStore<FixedPartitionExampleMetadata> {
      public FilteredTestMetadataStore() {
        super(
            curatorFramework,
            zkConfig,
            meterRegistry,
            CreateMode.PERSISTENT,
            new FixedPartitionMetadataSerializer().toModelSerializer(),
            partitionStoreFolder,
            List.of("1", "2", "4"));
      }
    }

    try (ZookeeperPartitioningMetadataStore<FixedPartitionExampleMetadata> store =
        new TestMetadataStore()) {
      store.createSync(new FixedPartitionExampleMetadata("example1", "1"));
      store.createSync(new FixedPartitionExampleMetadata("example2", "2"));
      store.createSync(new FixedPartitionExampleMetadata("example3", "3"));
      store.createSync(new FixedPartitionExampleMetadata("example4", "3"));
    }

    AtomicInteger counter = new AtomicInteger(0);
    try (ZookeeperPartitioningMetadataStore<FixedPartitionExampleMetadata> store =
        new FilteredTestMetadataStore()) {
      store.addListener(_ -> counter.incrementAndGet());

      store.createSync(new FixedPartitionExampleMetadata("example5", "1"));
      await().until(() -> counter.get() >= 1);

      assertThatThrownBy(
          () -> store.createSync(new FixedPartitionExampleMetadata("example6", "3")));

      // example1, example2, and example 5
      assertThat(store.listSync().size()).isEqualTo(3);

      store.createSync(new FixedPartitionExampleMetadata("example7", "4"));
    }
  }
}
