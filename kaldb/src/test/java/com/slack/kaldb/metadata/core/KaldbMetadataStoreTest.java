package com.slack.kaldb.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KaldbMetadataStoreTest {

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private AsyncCuratorFramework curatorFramework;

  private KaldbConfigs.ZookeeperConfig zookeeperConfig;

  @BeforeEach
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are
    // flaky.
    testingServer = new TestingServer();

    zookeeperConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("Test")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(500)
            .build();
    this.curatorFramework = CuratorBuilder.build(meterRegistry, zookeeperConfig);
  }

  @AfterEach
  public void tearDown() throws Exception {
    curatorFramework.unwrap().close();
    testingServer.close();
    meterRegistry.close();
  }

  private static class TestMetadata extends KaldbMetadata {
    private String value;

    @JsonCreator
    public TestMetadata(@JsonProperty("name") String name, @JsonProperty("value") String value) {
      super(name);
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TestMetadata)) return false;
      if (!super.equals(o)) return false;

      TestMetadata metadata = (TestMetadata) o;

      return value.equals(metadata.value);
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + value.hashCode();
      return result;
    }
  }

  private static final String STORE_FOLDER = "/testMetadata";

  @Test
  public void testCrudOperations() {
    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      // create two metadata
      TestMetadata metadata1 = new TestMetadata("foo", "val1");
      TestMetadata metadata2 = new TestMetadata("bar", "val2");
      store.createSync(metadata1);
      store.createSync(metadata2);

      await()
          .until(
              store::listSync,
              (metadataList) ->
                  metadataList.size() == 2
                      && metadataList.containsAll(List.of(metadata1, metadata2)));

      // check to see if the cache contains the elements as well
      await().until(() -> store.listSync().containsAll(List.of(metadata1, metadata2)));

      // update the value of one of the nodes
      String updatedValue = "updatedVal1";
      metadata1.setValue(updatedValue);
      store.updateSync(metadata1);

      // check that the node was updated using both a sync get, and checking the cache as well
      await().until(() -> Objects.equals(store.getSync(metadata1.name).getValue(), updatedValue));

      await()
          .until(
              () ->
                  store.listSync().stream()
                      .filter(instance -> instance.name.equals("foo"))
                      .findFirst()
                      .get()
                      .getValue()
                      .equals(updatedValue));

      // delete a node by object reference, and ensure that cache reflects the change
      store.deleteSync(metadata2);
      await()
          .until(
              store::listSync,
              (metadataList) -> metadataList.size() == 1 && metadataList.contains(metadata1));

      // delete a node by path reference, and ensure that cache reflects the change
      store.deleteSync(metadata1.name);
      await().until(() -> store.listSync().isEmpty());
    }
  }

  @Test
  public void testDuplicateCreate() {
    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      TestMetadata metadata1 = new TestMetadata("foo", "val1");
      store.createSync(metadata1);
      assertThatExceptionOfType(InternalMetadataStoreException.class)
          .isThrownBy(() -> store.createSync(metadata1));
    }
  }

  @Test
  public void testUncachedStoreAttemptingCacheOperations() {
    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            false,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      // create metadata
      TestMetadata metadata1 = new TestMetadata("foo", "val1");
      store.createSync(metadata1);

      await()
          .until(
              () -> {
                List<TestMetadata> metadata = store.listSync();
                return metadata.contains(metadata1) && metadata.size() == 1;
              });

      // verify exceptions are thrown attempting to use cached methods
      assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(store::listSync);
      assertThatExceptionOfType(UnsupportedOperationException.class)
          .isThrownBy(() -> store.addListener(() -> {}));
      assertThatExceptionOfType(UnsupportedOperationException.class)
          .isThrownBy(() -> store.removeListener(() -> {}));
    }
  }

  @Test
  public void testEphemeralNodeBehavior() {
    class PersistentMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public PersistentMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            false,
            new JacksonModelSerializer<>(TestMetadata.class),
            "/persistent");
      }
    }

    class EphemeralMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public EphemeralMetadataStore() {
        super(
            curatorFramework,
            CreateMode.EPHEMERAL,
            false,
            new JacksonModelSerializer<>(TestMetadata.class),
            "/ephemeral");
      }
    }

    TestMetadata metadata1 = new TestMetadata("foo", "val1");
    try (KaldbMetadataStore<TestMetadata> persistentStore = new PersistentMetadataStore()) {
      // create metadata
      persistentStore.createSync(metadata1);

      // do a list to ensure node has been persisted
      await()
          .until(
              persistentStore::listSync,
              (metadataList) -> metadataList.size() == 1 && metadataList.contains(metadata1));
    }

    TestMetadata metadata2 = new TestMetadata("foo", "val1");
    try (KaldbMetadataStore<TestMetadata> ephemeralStore = new EphemeralMetadataStore()) {
      // create metadata
      ephemeralStore.createSync(metadata2);

      // do a list to ensure node has been persisted
      await()
          .until(
              ephemeralStore::listSync,
              (metadataList) -> metadataList.size() == 1 && metadataList.contains(metadata2));
    }

    // close curator, and then instantiate a new copy
    // This is because we cannot restart the closed curator.
    curatorFramework.unwrap().close();
    curatorFramework = CuratorBuilder.build(meterRegistry, zookeeperConfig);

    try (KaldbMetadataStore<TestMetadata> persistentStore = new PersistentMetadataStore()) {
      assertThat(persistentStore.getSync("foo")).isEqualTo(metadata1);
    }

    try (KaldbMetadataStore<TestMetadata> ephemeralStore = new EphemeralMetadataStore()) {
      assertThatExceptionOfType(InternalMetadataStoreException.class)
          .isThrownBy(() -> ephemeralStore.getSync("foo"));
    }
  }

  @Test
  public void testListenersWithZkReconnect() throws Exception {
    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      AtomicInteger counter = new AtomicInteger(0);
      KaldbMetadataStoreChangeListener listener = counter::incrementAndGet;
      store.addListener(listener);

      await().until(() -> counter.get() == 0);

      // create metadata
      TestMetadata metadata1 = new TestMetadata("foo", "val1");
      store.createSync(metadata1);

      await().until(() -> counter.get() == 1);

      testingServer.restart();

      assertThat(store.getSync(metadata1.name)).isEqualTo(metadata1);
      assertThat(counter.get()).isEqualTo(1);

      metadata1.setValue("val2");
      store.updateSync(metadata1);

      await().until(() -> counter.get() == 2);
    }
  }

  @Test
  public void testAddRemoveListener() throws Exception {
    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      AtomicInteger counter = new AtomicInteger(0);
      KaldbMetadataStoreChangeListener listener = counter::incrementAndGet;
      store.addListener(listener);

      await().until(() -> counter.get() == 0);

      // create metadata
      TestMetadata metadata1 = new TestMetadata("foo", "val1");
      store.createSync(metadata1);

      await().until(() -> counter.get() == 1);

      assertThat(store.getSync(metadata1.name)).isEqualTo(metadata1);
      assertThat(counter.get()).isEqualTo(1);

      metadata1.setValue("val2");
      store.updateSync(metadata1);

      await().until(() -> counter.get() == 2);

      store.removeListener(listener);
      metadata1.setValue("val3");
      store.updateSync(metadata1);

      Thread.sleep(2000);
      assertThat(counter.get()).isEqualTo(2);
    }
  }

  @Test
  public void serializeAndDeserializeOnlyInvokeOnce() {
    AtomicInteger serializeCounter = new AtomicInteger(0);
    AtomicInteger deserializeCounter = new AtomicInteger(0);

    class CountingSerializer implements ModelSerializer<TestMetadata> {
      final JacksonModelSerializer<TestMetadata> serializer =
          new JacksonModelSerializer<>(TestMetadata.class);

      @Override
      public byte[] serialize(TestMetadata model) {
        serializeCounter.incrementAndGet();
        return serializer.serialize(model);
      }

      @Override
      public TestMetadata deserialize(byte[] bytes) {
        deserializeCounter.incrementAndGet();
        return serializer.deserialize(bytes);
      }
    }

    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework, CreateMode.PERSISTENT, true, new CountingSerializer(), STORE_FOLDER);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      TestMetadata metadata = new TestMetadata("name", "value");
      store.createSync(metadata);

      Thread.sleep(500);

      store.getSync("name");
      store.getSync("name");

      assertThat(serializeCounter.get()).isEqualTo(1);
      assertThat(deserializeCounter.get()).isEqualTo(1);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
