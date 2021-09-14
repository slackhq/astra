package com.slack.kaldb.metadata.zookeeper;

import static com.slack.kaldb.metadata.zookeeper.CachedMetadataStoreImpl.CACHE_ERROR_COUNTER;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.ZkUtils.closeZookeeperClientConnection;
import static com.slack.kaldb.util.SnapshotUtil.makeSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataSerializer;
import com.slack.kaldb.util.CountingFatalErrorHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedMetadataStoreImplTest {

  private static final Logger LOG = LoggerFactory.getLogger(CachedMetadataStoreImplTest.class);

  private TestingServer testingServer;
  private ZookeeperMetadataStoreImpl metadataStore;
  private MeterRegistry meterRegistry;
  private final SnapshotMetadataSerializer serDe = new SnapshotMetadataSerializer();
  private ZooKeeper zooKeeper;

  static class CountingCachedMetadataListener implements CachedMetadataStoreListener {
    private int cacheChangedCounter = 0;
    private int stateChangedCounter = 0;

    @Override
    public void cacheChanged() {
      cacheChangedCounter += 1;
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
      stateChangedCounter += 1;
    }

    public int getCacheChangedCounter() {
      return cacheChangedCounter;
    }

    public int getStateChangedCounter() {
      return stateChangedCounter;
    }
  }

  @Before
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are flaky.
    testingServer = new TestingServer();
    CountingFatalErrorHandler countingFatalErrorHandler = new CountingFatalErrorHandler();
    metadataStore =
        new ZookeeperMetadataStoreImpl(
            testingServer.getConnectString(),
            "test",
            1000,
            1000,
            new RetryNTimes(1, 500),
            countingFatalErrorHandler,
            meterRegistry);
    zooKeeper = metadataStore.getCurator().getZookeeperClient().getZooKeeper();
  }

  @After
  public void tearDown() throws IOException {
    metadataStore.close();
    testingServer.close();
    meterRegistry.close();
  }

  private CachedMetadataStore<SnapshotMetadata> makeCachedStore(
      String path,
      CachedMetadataStoreListener listener,
      MetadataSerializer<SnapshotMetadata> metadataSerializer)
      throws Exception {

    CachedMetadataStore<SnapshotMetadata> cachedMetadataStore =
        new CachedMetadataStoreImpl<>(
            path,
            metadataSerializer,
            metadataStore.getCurator(),
            metadataStore.getMetadataExecutorService(),
            meterRegistry);
    if (listener != null) {
      cachedMetadataStore.addListener(listener);
    }
    cachedMetadataStore.start();
    return cachedMetadataStore;
  }

  @Test
  public void raceConditionOnMetadataStoreCloseListeners() throws Exception {
    AtomicBoolean metadataStoreClosed = new AtomicBoolean(false);
    AtomicInteger listenerExecutions = new AtomicInteger(0);
    CountDownLatch countDownLatch = new CountDownLatch(3);

    String root = "/race";
    assertThat(metadataStore.create(root, "", true).get()).isNull();
    CachedMetadataStore<SnapshotMetadata> cache =
        makeCachedStore(
            "/race",
            new CachedMetadataStoreListener() {
              @Override
              public void cacheChanged() {
                LOG.info("Cache change started execution");
                try {
                  Thread.sleep(500);
                } catch (InterruptedException ignored) {
                }

                if (metadataStoreClosed.get()) {
                  // we are operating in a listener _after_ curator has closed
                  LOG.info("Metadata store is closed, but we are still executing");
                  listenerExecutions.incrementAndGet();
                }
                LOG.info("Cache change finished execution");
                countDownLatch.countDown();
              }

              @Override
              public void stateChanged(CuratorFramework client, ConnectionState newState) {}
            },
            serDe);
    assertThat(((CachedMetadataStoreImpl<SnapshotMetadata>) cache).isStarted()).isTrue();

    String path1 = "/race/1";
    SnapshotMetadata snapshot1 = makeSnapshot("race1");
    metadataStore.create(path1, serDe.toJsonStr(snapshot1), true).get();
    String path2 = "/race/2";
    SnapshotMetadata snapshot2 = makeSnapshot("race2");
    metadataStore.create(path2, serDe.toJsonStr(snapshot2), true).get();
    String path3 = "/race/3";
    SnapshotMetadata snapshot3 = makeSnapshot("race3");
    metadataStore.create(path3, serDe.toJsonStr(snapshot3), true).get();

    // wait until all 3 records safely made it into the cache
    await().until(() -> cache.getInstances().size() == 3);

    metadataStore.close();
    metadataStoreClosed.set(true);

    // verify that no listener executions have fired yet
    assertThat(listenerExecutions.get()).isEqualTo(0);

    // allow time for any queued executions to complete
    assertThat(countDownLatch.await(3000, TimeUnit.MILLISECONDS)).isTrue();

    // verify that we still did not see any executions
    assertEquals(0, listenerExecutions.get());
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void watchCachePersistentTreeTest() throws Exception {
    String root = "/root";
    assertThat(metadataStore.create(root, "", true).get()).isNull();
    CountingCachedMetadataListener listener = new CountingCachedMetadataListener();
    CachedMetadataStore<SnapshotMetadata> cache = makeCachedStore("/root", listener, serDe);
    assertThat(((CachedMetadataStoreImpl<SnapshotMetadata>) cache).isStarted()).isTrue();

    String path1 = "/root/1";
    SnapshotMetadata snapshot1 = makeSnapshot("test1");
    assertThat(metadataStore.create(path1, serDe.toJsonStr(snapshot1), true).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isSameAs(1));
    assertThat(metadataStore.get(path1).get()).isEqualTo(serDe.toJsonStr(snapshot1));
    assertThat(cache.get("1").get()).isEqualTo(snapshot1);
    assertThat(cache.getInstances().get(0)).isEqualTo(snapshot1);
    assertThat(listener.getCacheChangedCounter()).isEqualTo(1);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    String path2 = "/root/2";
    SnapshotMetadata snapshot2 = makeSnapshot("test2");
    assertThat(metadataStore.create(path2, serDe.toJsonStr(snapshot2), true).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isEqualTo(2));
    assertThat(metadataStore.get(path2).get()).isEqualTo(serDe.toJsonStr(snapshot2));
    assertThat(cache.get("2").get()).isEqualTo(snapshot2);
    assertThat(cache.getInstances()).contains(snapshot1, snapshot2);
    assertThat(listener.getCacheChangedCounter()).isEqualTo(2);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    String path3 = "/root/3";
    SnapshotMetadata snapshot3 = makeSnapshot("test3");
    assertThat(metadataStore.create(path3, serDe.toJsonStr(snapshot3), true).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isEqualTo(3));
    assertThat(metadataStore.get(path3).get()).isEqualTo(serDe.toJsonStr(snapshot3));
    assertThat(cache.get("3").get()).isEqualTo(snapshot3);
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2, snapshot3);
    assertThat(listener.getCacheChangedCounter()).isEqualTo(3);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    // Updating data in a node refreshes the cache.
    SnapshotMetadata snapshot31 = makeSnapshot("snapshot31");
    assertThat(metadataStore.put(path3, serDe.toJsonStr(snapshot31)).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.getInstances().contains(snapshot31)).isTrue());
    assertThat(metadataStore.get(path3).get()).isEqualTo(serDe.toJsonStr(snapshot31));
    assertThat(cache.get("3").get()).isEqualTo(snapshot31);
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2, snapshot31);
    assertThat(listener.getCacheChangedCounter()).isEqualTo(4);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    // Deleting a node refreshes the cache.
    assertThat(metadataStore.delete(path3).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isEqualTo(2));
    assertThat(cache.get(path3).isPresent()).isFalse();
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2);
    assertThat(listener.getCacheChangedCounter()).isEqualTo(5);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    cache.close();
    assertThat(listener.getCacheChangedCounter()).isEqualTo(5);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    assertThat(((CachedMetadataStoreImpl<SnapshotMetadata>) cache).isStarted()).isFalse();

    // Cache is not cleared after closing. The data grows stale.
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2);

    assertThat(metadataStore.create(path3, serDe.toJsonStr(snapshot31), true).get()).isNull();
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2);

    // Restarting the cache throws an exception.
    assertThatIllegalStateException().isThrownBy(cache::start);
    assertThat(((CachedMetadataStoreImpl<SnapshotMetadata>) cache).isStarted()).isFalse();
    // A stale cache can still be accessed.
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testWatchCacheEphemeralTreeTest() throws Exception {
    String root = "/eroot";
    assertThat(metadataStore.create(root, "", true).get()).isNull();
    CountingCachedMetadataListener listener = new CountingCachedMetadataListener();
    CachedMetadataStore<SnapshotMetadata> cache = makeCachedStore(root, listener, serDe);
    assertThat(((CachedMetadataStoreImpl<SnapshotMetadata>) cache).isStarted()).isTrue();

    String path1 = "/eroot/ephemeral1";
    SnapshotMetadata snapshot1 = makeSnapshot("test1");
    assertThat(metadataStore.createEphemeralNode(path1, serDe.toJsonStr(snapshot1)).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isSameAs(1));
    assertThat(metadataStore.get(path1).get()).isEqualTo(serDe.toJsonStr(snapshot1));
    assertThat(cache.get("ephemeral1").get()).isEqualTo(snapshot1);
    assertThat(cache.getInstances().get(0)).isEqualTo(snapshot1);
    assertThat(listener.getCacheChangedCounter()).isEqualTo(1);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    String path2 = "/eroot/ephemeral2";
    SnapshotMetadata snapshot2 = makeSnapshot("test2");
    assertThat(metadataStore.createEphemeralNode(path2, serDe.toJsonStr(snapshot2)).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isEqualTo(2));
    assertThat(metadataStore.get(path2).get()).isEqualTo(serDe.toJsonStr(snapshot2));
    assertThat(cache.get("ephemeral2").get()).isEqualTo(snapshot2);
    assertThat(cache.getInstances()).contains(snapshot1, snapshot2);
    assertThat(listener.getCacheChangedCounter()).isEqualTo(2);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    String path3 = "/eroot/ephemeral3";
    SnapshotMetadata snapshot3 = makeSnapshot("test3");
    assertThat(metadataStore.createEphemeralNode(path3, serDe.toJsonStr(snapshot3)).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isEqualTo(3));
    assertThat(metadataStore.get(path3).get()).isEqualTo(serDe.toJsonStr(snapshot3));
    assertThat(cache.get("ephemeral3").get()).isEqualTo(snapshot3);
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2, snapshot3);
    assertThat(listener.getCacheChangedCounter()).isEqualTo(3);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    // TODO: Add metrics on cache metadata updates.
    // Updating ephemeral data in a node refreshes the cache.
    SnapshotMetadata snapshot31 = makeSnapshot("snapshot31");
    assertThat(metadataStore.put(path3, serDe.toJsonStr(snapshot31)).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.getInstances().contains(snapshot31)).isTrue());
    assertThat(metadataStore.get(path3).get()).isEqualTo(serDe.toJsonStr(snapshot31));
    assertThat(cache.get("ephemeral3").get()).isEqualTo(snapshot31);
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2, snapshot31);
    assertThat(listener.getCacheChangedCounter()).isEqualTo(4);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    // Deleting ephemeral node refreshes the cache.
    assertThat(metadataStore.delete(path3).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isEqualTo(2));
    assertThat(cache.get(path3).isPresent()).isFalse();
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2);
    assertThat(listener.getCacheChangedCounter()).isEqualTo(5);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    cache.close();
    assertThat(((CachedMetadataStoreImpl<SnapshotMetadata>) cache).isStarted()).isFalse();
    assertThat(listener.getCacheChangedCounter()).isEqualTo(5);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    // Cache is not cleared after closing. The data grows stale.
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2);

    assertThat(metadataStore.createEphemeralNode(path3, serDe.toJsonStr(snapshot31)).get())
        .isNull();
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2);

    // Restarting the cache throws an exception.
    assertThatIllegalStateException().isThrownBy(cache::start);
    assertThat(((CachedMetadataStoreImpl<SnapshotMetadata>) cache).isStarted()).isFalse();
    // A stale cache can still be accessed.
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2);
  }

  @Test
  public void testCacheOnNonExistentNode() throws Exception {
    final String root = "/root123";
    makeCachedStore(root, null, serDe);
    // Succeeds since creating a cached store creates the node.
    assertThat(metadataStore.exists(root).get()).isTrue();
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testThrowingListenerOnCache() throws Exception {
    final String root = "/root";
    CachedMetadataStoreListener listener =
        new CachedMetadataStoreListener() {
          @Override
          public void cacheChanged() {
            throw new RuntimeException("fail");
          }

          @Override
          public void stateChanged(CuratorFramework client, ConnectionState newState) {
            throw new RuntimeException("fail");
          }
        };

    CachedMetadataStore<SnapshotMetadata> cache = makeCachedStore(root, listener, serDe);
    assertThat(metadataStore.exists(root).get()).isTrue();

    // notification fired without issues ane error is counted.
    final String node = "/root/node1";
    SnapshotMetadata snapshot1 = makeSnapshot("test1");
    assertThat(metadataStore.create(node, serDe.toJsonStr(snapshot1), true).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get("node1").get()).isEqualTo(snapshot1));
    assertThat(cache.getInstances()).containsOnly(snapshot1);
    assertThat(metadataStore.exists(node).get()).isTrue();
    assertThat(metadataStore.get(node).get()).isEqualTo(serDe.toJsonStr(snapshot1));
    assertThat(getCount(CACHE_ERROR_COUNTER, meterRegistry)).isEqualTo(1);

    // Removing the listener shouldn't fire the notification.
    cache.removeListener(listener);

    final String node2 = "/root/node2";
    SnapshotMetadata snapshot2 = makeSnapshot("test2");
    assertThat(metadataStore.create(node2, serDe.toJsonStr(snapshot2), true).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get("node2").get()).isEqualTo(snapshot2));
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2);
    assertThat(metadataStore.exists(node2).get()).isTrue();
    assertThat(metadataStore.get(node2).get()).isEqualTo(serDe.toJsonStr(snapshot2));
    assertThat(getCount(CACHE_ERROR_COUNTER, meterRegistry)).isEqualTo(1);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testWatchingPersistentNode() throws Exception {
    final String root = "/root";
    assertThat(metadataStore.create(root, "", true).get()).isNull();

    final String node = "/root/node";
    SnapshotMetadata snapshot1 = makeSnapshot("test1");
    assertThat(metadataStore.create(node, serDe.toJsonStr(snapshot1), true).get()).isNull();

    CachedMetadataStore<SnapshotMetadata> cache = makeCachedStore(root, null, serDe);

    await().untilAsserted(() -> assertThat(cache.get("node").get()).isEqualTo(snapshot1));
    assertThat(cache.getInstances().size()).isEqualTo(1);
    assertThat(metadataStore.exists(node).get()).isTrue();
    assertThat(metadataStore.get(node).get()).isEqualTo(serDe.toJsonStr(snapshot1));

    SnapshotMetadata snapshot11 = makeSnapshot("test11");
    assertThat(metadataStore.put(node, serDe.toJsonStr(snapshot11)).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get("node").get()).isEqualTo(snapshot11));
    assertThat(cache.getInstances().size()).isEqualTo(1);

    assertThat(metadataStore.delete(node).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get(node).isPresent()).isFalse());
    assertThat(cache.getInstances()).isEmpty();

    cache.close();
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testCachingPersistentNodeOnCuratorShutdown() throws Exception {
    final String root = "/root";
    assertThat(metadataStore.create(root, "", true).get()).isNull();

    final String node = "/root/node";
    SnapshotMetadata snapshot1 = makeSnapshot("test1");
    assertThat(metadataStore.create(node, serDe.toJsonStr(snapshot1), true).get()).isNull();

    CachedMetadataStore<SnapshotMetadata> cache = makeCachedStore(root, null, serDe);

    await().untilAsserted(() -> assertThat(cache.get("node").get()).isEqualTo(snapshot1));
    assertThat(cache.getInstances().size()).isEqualTo(1);
    assertThat(metadataStore.exists(node).get()).isTrue();
    assertThat(metadataStore.get(node).get()).isEqualTo(serDe.toJsonStr(snapshot1));

    SnapshotMetadata snapshot11 = makeSnapshot("test11");
    assertThat(metadataStore.put(node, serDe.toJsonStr(snapshot11)).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get("node").get()).isEqualTo(snapshot11));
    assertThat(cache.getInstances().size()).isEqualTo(1);

    // Closing the curator connection still keeps the persistent node around in the cache.
    metadataStore.close();
    await().untilAsserted(() -> assertThat(cache.getInstances().isEmpty()).isFalse());
    assertThat(cache.getInstances().get(0)).isEqualTo(snapshot11);

    cache.close();
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testWatchingEphermeralNode() throws Exception {
    final String root = "/root";
    assertThat(metadataStore.create(root, "", true).get()).isNull();

    final String node = "/root/enode";
    SnapshotMetadata snapshot1 = makeSnapshot("test1");
    assertThat(metadataStore.createEphemeralNode(node, serDe.toJsonStr(snapshot1)).get()).isNull();

    CachedMetadataStore<SnapshotMetadata> cache = makeCachedStore(root, null, serDe);
    await().untilAsserted(() -> assertThat(cache.get("enode").get()).isEqualTo(snapshot1));
    assertThat(cache.getInstances().size()).isEqualTo(1);
    assertThat(metadataStore.exists(node).get()).isTrue();
    assertThat(metadataStore.get(node).get()).isEqualTo(serDe.toJsonStr(snapshot1));

    SnapshotMetadata snapshot11 = makeSnapshot("test11");
    assertThat(metadataStore.put(node, serDe.toJsonStr(snapshot11)).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get("enode").get()).isEqualTo(snapshot11));
    assertThat(cache.getInstances().size()).isEqualTo(1);

    assertThat(metadataStore.delete(node).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get("enode").isPresent()).isFalse());
    assertThat(cache.getInstances()).isEmpty();

    cache.close();
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testEphermeralNodeExpiry() throws Exception {
    final String root = "/root";
    assertThat(metadataStore.create(root, "", true).get()).isNull();

    final String node = "/root/enode";
    SnapshotMetadata snapshot1 = makeSnapshot("test1");
    assertThat(metadataStore.createEphemeralNode(node, serDe.toJsonStr(snapshot1)).get()).isNull();

    CountingCachedMetadataListener listener = new CountingCachedMetadataListener();
    CachedMetadataStore<SnapshotMetadata> cache = makeCachedStore(root, listener, serDe);
    await().untilAsserted(() -> assertThat(cache.get("enode").get()).isEqualTo(snapshot1));
    assertThat(cache.getInstances().size()).isEqualTo(1);
    assertThat(metadataStore.exists(node).get()).isTrue();
    assertThat(metadataStore.get(node).get()).isEqualTo(serDe.toJsonStr(snapshot1));
    // No listener invocations on initial cache load.
    assertThat(listener.getCacheChangedCounter()).isEqualTo(0);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    SnapshotMetadata snapshot11 = makeSnapshot("test11");
    assertThat(metadataStore.put(node, serDe.toJsonStr(snapshot11)).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get("enode").get()).isEqualTo(snapshot11));
    assertThat(cache.getInstances().size()).isEqualTo(1);
    assertThat(listener.getCacheChangedCounter()).isEqualTo(1);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    // Closing the curator connection expires the ephemeral node and cache is updated also.
    metadataStore.getCurator().close();
    await().untilAsserted(() -> assertThat(cache.getInstances().isEmpty()).isTrue());
    assertThat(listener.getCacheChangedCounter()).isEqualTo(2);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    cache.close();
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testEphermeralNodeOnZkShutdown() throws Exception {
    final String root = "/root";
    assertThat(metadataStore.create(root, "", true).get()).isNull();

    final String node = "/root/enode";
    SnapshotMetadata snapshot1 = makeSnapshot("test1");
    assertThat(metadataStore.createEphemeralNode(node, serDe.toJsonStr(snapshot1)).get()).isNull();

    CountingCachedMetadataListener listener = new CountingCachedMetadataListener();
    CachedMetadataStore<SnapshotMetadata> cache = makeCachedStore(root, listener, serDe);
    await().untilAsserted(() -> assertThat(cache.get("enode").get()).isEqualTo(snapshot1));
    assertThat(cache.getInstances()).containsOnly(snapshot1);
    assertThat(metadataStore.exists(node).get()).isTrue();
    assertThat(metadataStore.get(node).get()).isEqualTo(serDe.toJsonStr(snapshot1));
    // No listener invocations on initial cache load.
    assertThat(listener.getCacheChangedCounter()).isEqualTo(0);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    SnapshotMetadata snapshot11 = makeSnapshot("test11");
    assertThat(metadataStore.put(node, serDe.toJsonStr(snapshot11)).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get("enode").get()).isEqualTo(snapshot11));
    assertThat(cache.getInstances()).containsOnly(snapshot11);
    assertThat(listener.getCacheChangedCounter()).isEqualTo(1);
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    // Closing the zk server doesn't close the cache since cache would be refreshed once the
    // connection is back up.
    testingServer.stop();
    assertThat(cache.getInstances()).containsOnly(snapshot11);
    assertThat(listener.getCacheChangedCounter()).isEqualTo(1);
    // NOTE: No listener on state change is fired on server stop. A stale cache should be flagged
    // in this case.
    assertThat(listener.getStateChangedCounter()).isEqualTo(0);

    cache.close();
    closeZookeeperClientConnection(zooKeeper);
  }

  // TODO: Add a unit test when server is started or restarted.
  // TODO: Add a test with server shutdown and timeout on persistent node.
  // TODO: Add a test with server shutdown and timeout with ephemeral node.

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testCacheCanHoldPersistentOrEphemeralNodes() throws Exception {
    final String root = "/root";
    assertThat(metadataStore.create(root, "", true).get()).isNull();

    final String ephemeralNode = "/root/enode";
    SnapshotMetadata snapshot1 = makeSnapshot("test1");
    assertThat(metadataStore.createEphemeralNode(ephemeralNode, serDe.toJsonStr(snapshot1)).get())
        .isNull();

    final String persistentNode = "/root/node";
    SnapshotMetadata snapshot2 = makeSnapshot("test2");
    assertThat(metadataStore.create(persistentNode, serDe.toJsonStr(snapshot2), true).get())
        .isNull();

    CachedMetadataStore<SnapshotMetadata> cache = makeCachedStore(root, null, serDe);
    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isEqualTo(2));
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2);
    assertThat(metadataStore.get(ephemeralNode).get()).isEqualTo(serDe.toJsonStr(snapshot1));
    assertThat(metadataStore.get(persistentNode).get()).isEqualTo(serDe.toJsonStr(snapshot2));
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2);

    SnapshotMetadata snapshot11 = makeSnapshot("test11");
    assertThat(metadataStore.put(ephemeralNode, serDe.toJsonStr(snapshot11)).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get("enode").get()).isEqualTo(snapshot11));
    assertThat(cache.getInstances()).containsOnly(snapshot11, snapshot2);

    SnapshotMetadata snapshot21 = makeSnapshot("test21");
    assertThat(metadataStore.put(persistentNode, serDe.toJsonStr(snapshot21)).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get("node").get()).isEqualTo(snapshot21));
    assertThat(cache.getInstances()).containsOnly(snapshot11, snapshot21);

    // Closing the curator connection expires the ephemeral node and cache is left with
    // persistent node.
    metadataStore.getCurator().close();
    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isEqualTo(1));
    assertThat(cache.getInstances()).containsOnly(snapshot21);

    cache.close();
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testCachedStoreWorksForNestedNodes() throws Exception {
    final String root = "/root";
    assertThat(metadataStore.create(root, "", true).get()).isNull();

    final String node = "/root/node";
    SnapshotMetadata snapshot1 = makeSnapshot("test1");
    assertThat(metadataStore.create(node, serDe.toJsonStr(snapshot1), true).get()).isNull();

    CountingCachedMetadataListener listener = new CountingCachedMetadataListener();
    CachedMetadataStore<SnapshotMetadata> cache = makeCachedStore(root, listener, serDe);

    SnapshotMetadata snapshot3 = makeSnapshot("test3");
    SnapshotMetadata snapshot33 = makeSnapshot("test33");
    SnapshotMetadata snapshot333 = makeSnapshot("test333");
    // Nested nodes are also cached, but root nodes should also have data that is serializable.
    String path333 = "/root/3/33/333";
    assertThat(metadataStore.create("/root/3", serDe.toJsonStr(snapshot3), false).get()).isNull();
    assertThat(metadataStore.create("/root/3/33", serDe.toJsonStr(snapshot33), false).get())
        .isNull();
    assertThat(metadataStore.create(path333, serDe.toJsonStr(snapshot333), false).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isEqualTo(4));
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot3, snapshot33, snapshot333);
    assertThat(cache.get("node").get()).isEqualTo(snapshot1);
    assertThat(cache.get("3").get()).isEqualTo(snapshot3);
    assertThat(cache.get("3/33").get()).isEqualTo(snapshot33);
    assertThat(cache.get("3/33/333").get()).isEqualTo(snapshot333);

    SnapshotMetadata esnapshot = makeSnapshot("ephemeraldata");
    assertThat(metadataStore.createEphemeralNode("/root/3/enode", serDe.toJsonStr(esnapshot)).get())
        .isNull();
    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isEqualTo(5));
    assertThat(cache.getInstances())
        .containsOnly(snapshot1, snapshot3, snapshot33, snapshot333, esnapshot);

    assertThat(metadataStore.get("/root/3/enode").get()).isEqualTo(serDe.toJsonStr(esnapshot));
    assertThat(cache.get("3/enode").get()).isEqualTo(esnapshot);
  }
}
