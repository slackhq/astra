package com.slack.kaldb.metadata.zookeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.slack.kaldb.util.CountingFatalErrorHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WatchCacheTest {

  private TestingServer testingServer;
  private ZookeeperMetadataStoreImpl metadataStore;
  private MeterRegistry meterRegistry;
  private CountingFatalErrorHandler countingFatalErrorHandler;

  @Before
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are flaky.
    testingServer = new TestingServer();
    countingFatalErrorHandler = new CountingFatalErrorHandler();
    metadataStore =
        new ZookeeperMetadataStoreImpl(
            testingServer.getConnectString(),
            "test",
            1000,
            1000,
            new RetryNTimes(1, 500),
            countingFatalErrorHandler,
            meterRegistry);
  }

  @After
  public void tearDown() throws IOException {
    metadataStore.close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void watchCachePersistentTreeTest() throws ExecutionException, InterruptedException {
    String root = "/root";
    assertThat(metadataStore.create(root, "", true).get()).isNull();
    CuratorCache cache = metadataStore.watchAndCacheNodeAndChildren("/root");

    String path1 = "/root/1/2/3";
    String path2 = "/root/1/2/4";
    String path3 = "/root/1/2/5";
    assertThat(metadataStore.create(path1, "3", true).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.size()).isEqualTo(4));
    assertThat(metadataStore.create(path2, "", true).get()).isNull();
    assertThat(metadataStore.create(path3, "5", true).get()).isNull();
    assertThat(metadataStore.exists(path1).get()).isTrue();
    assertThat(metadataStore.get(path1).get()).isEqualTo("3");
    assertThat(metadataStore.get(path2).get()).isEmpty();
    assertThat(metadataStore.get(path3).get()).isEqualTo("5");

    await()
        .untilAsserted(
            () -> assertThat(new String(cache.get("/root/1").get().getData())).isEmpty());
    assertThat(cache.size()).isEqualTo(6);
    assertThat(new String(cache.get(path1).get().getData())).isEqualTo("3");
    assertThat(new String(cache.get(path2).get().getData())).isEmpty();
    assertThat(new String(cache.get(path3).get().getData())).isEqualTo("5");
    assertThat(new String(cache.get(path3).get().getData())).isEqualTo("5");
    assertThat(metadataStore.getChildren("/root/1/2").get())
        .hasSameElementsAs(List.of("3", "4", "5"));

    // Updating data in a node refreshes the cache.
    final String newData = "newData";
    assertThat(metadataStore.put(path3, newData).get()).isNull();
    await()
        .untilAsserted(
            () -> assertThat(new String(cache.get(path3).get().getData())).isEqualTo(newData));
    assertThat(metadataStore.get(path3).get()).isEqualTo(newData);
    assertThat(metadataStore.getChildren("/root/1/2").get())
        .hasSameElementsAs(List.of("3", "4", "5"));

    assertThat(metadataStore.delete(path3).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get(path3).isPresent()).isFalse());
    assertThat(cache.get(path1).isPresent()).isTrue();
    assertThat(cache.get(path2).isPresent()).isTrue();
    cache.close();
  }

  // TODO: Add a test with a counting listener.
  // TODO: Add a test with server shutdown and timeout.
  // TODO: Add a test with server shutdown and timeout with ephemeral node.

  @Test
  public void testWatchCacheEphemeralTreeTest() throws ExecutionException, InterruptedException {
    String root = "/root";
    assertThat(metadataStore.create(root, "", true).get()).isNull();
    CuratorCache cache = metadataStore.watchAndCacheNodeAndChildren(root);

    String path1 = "/root/ephemeral1";
    String path2 = "/root/ephemeral2";
    String path3 = "/root/ephemeral3";
    assertThat(metadataStore.createEphemeralNode(path1, "1").get()).isNull();
    assertThat(metadataStore.createEphemeralNode(path2, "2").get()).isNull();
    assertThat(metadataStore.createEphemeralNode(path3, "3").get()).isNull();
    assertThat(metadataStore.get(path1).get()).isEqualTo("1");
    assertThat(metadataStore.get(path2).get()).isEqualTo("2");
    assertThat(metadataStore.get(path3).get()).isEqualTo("3");
    assertThat(metadataStore.get(path3).get()).isEqualTo("3");

    await()
        .untilAsserted(
            () -> assertThat(new String(cache.get(path1).get().getData())).isEqualTo("1"));
    assertThat(cache.size()).isEqualTo(4);
    assertThat(new String(cache.get(path2).get().getData())).isEqualTo("2");
    assertThat(new String(cache.get(path3).get().getData())).isEqualTo("3");
    assertThat(metadataStore.getChildren(root).get())
        .hasSameElementsAs(List.of("ephemeral1", "ephemeral2", "ephemeral3"));

    // Update ephemeral node refreshes the cache.
    final String newData = "newData";
    assertThat(metadataStore.put(path3, newData).get()).isNull();
    await()
        .untilAsserted(
            () -> assertThat(new String(cache.get(path3).get().getData())).isEqualTo(newData));
    assertThat(metadataStore.get(path3).get()).isEqualTo(newData);
    assertThat(metadataStore.getChildren(root).get())
        .hasSameElementsAs(List.of("ephemeral1", "ephemeral2", "ephemeral3"));

    await().untilAsserted(() -> assertThat(metadataStore.delete(path3).get()).isNull());
    assertThat(cache.get(path3).isPresent()).isFalse();
    assertThat(metadataStore.exists(path3).get()).isFalse();
    assertThat(new String(cache.get(path1).get().getData())).isEqualTo("1");
    assertThat(new String(cache.get(path2).get().getData())).isEqualTo("2");

    // TODO: test node expiry

    cache.close();
  }

  @Test(expected = NoNodeException.class)
  public void testCacheOnMissingPathThrowsException() {
    metadataStore.watchAndCacheNodeAndChildren("/root123");
  }

  @Test
  public void testWatchingPersistentNode() throws ExecutionException, InterruptedException {
    final String root = "/root";
    assertThat(metadataStore.create(root, "", true).get()).isNull();

    final String node = "/root/node";
    final String data = "data";
    assertThat(metadataStore.create(node, data, true).get()).isNull();

    CuratorCache cache = metadataStore.watchAndCacheNodeAndChildren(node);
    await()
        .untilAsserted(
            () -> assertThat(new String(cache.get(node).get().getData())).isEqualTo(data));
    assertThat(cache.size()).isEqualTo(1);
    assertThat(metadataStore.exists(node).get()).isTrue();
    assertThat(metadataStore.get(node).get()).isEqualTo(data);

    final String newData = "newData";
    assertThat(metadataStore.put(node, newData).get()).isNull();
    await()
        .untilAsserted(
            () -> assertThat(new String(cache.get(node).get().getData())).isEqualTo(newData));
    assertThat(cache.size()).isEqualTo(1);

    assertThat(metadataStore.delete(node).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get(node).isPresent()).isFalse());
    assertThat(cache.size()).isEqualTo(0);

    cache.close();
  }

  @Test
  public void testWatchingEphermeralNode() throws ExecutionException, InterruptedException {
    final String root = "/root";
    assertThat(metadataStore.create(root, "", true).get()).isNull();

    final String node = "/root/ephemeralRoot";
    final String data = "data";
    assertThat(metadataStore.createEphemeralNode(node, data).get()).isNull();

    CuratorCache cache = metadataStore.watchAndCacheNodeAndChildren(node);
    await()
        .untilAsserted(
            () -> assertThat(new String(cache.get(node).get().getData())).isEqualTo(data));
    assertThat(cache.size()).isEqualTo(1);
    assertThat(metadataStore.exists(node).get()).isTrue();
    assertThat(metadataStore.get(node).get()).isEqualTo(data);

    final String newData = "newData";
    assertThat(metadataStore.put(node, newData).get()).isNull();
    await()
        .untilAsserted(
            () -> assertThat(new String(cache.get(node).get().getData())).isEqualTo(newData));
    assertThat(cache.size()).isEqualTo(1);

    assertThat(metadataStore.delete(node).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get(node).isPresent()).isFalse());
    assertThat(cache.size()).isEqualTo(0);

    // TODO: test node expiry
    cache.close();
  }
}
