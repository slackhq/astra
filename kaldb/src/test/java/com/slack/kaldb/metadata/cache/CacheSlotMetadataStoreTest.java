package com.slack.kaldb.metadata.cache;

import static com.slack.kaldb.proto.metadata.Metadata.CacheSlotMetadata.CacheSlotState;
import static com.slack.kaldb.proto.metadata.Metadata.IndexType.LOGS_LUCENE9;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;

import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CacheSlotMetadataStoreTest {
  private static final List<Metadata.IndexType> SUPPORTED_INDEX_TYPES = List.of(LOGS_LUCENE9);

  private TestingServer testingServer;

  private AsyncCuratorFramework curatorFramework;
  private MeterRegistry meterRegistry;
  private CacheSlotMetadataStore store;

  @BeforeEach
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are
    // flaky.
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zookeeperConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("Test")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(500)
            .build();
    this.curatorFramework = CuratorBuilder.build(meterRegistry, zookeeperConfig);
    this.store = new CacheSlotMetadataStore(curatorFramework, true);
  }

  @AfterEach
  public void tearDown() throws IOException {
    curatorFramework.unwrap().close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testGetAndUpdateNonFreeCacheSlotStateSync() throws Exception {
    final String name = "slot1";
    final String hostname = "hostname";
    Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState = CacheSlotState.ASSIGNED;
    final String replicaId = "3456";
    long updatedTimeEpochMs = Instant.now().toEpochMilli();

    final CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            name, cacheSlotState, replicaId, updatedTimeEpochMs, SUPPORTED_INDEX_TYPES, hostname);
    assertThat(cacheSlotMetadata.name).isEqualTo(name);
    assertThat(cacheSlotMetadata.cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(cacheSlotMetadata.replicaId).isEqualTo(replicaId);
    assertThat(cacheSlotMetadata.updatedTimeEpochMs).isEqualTo(updatedTimeEpochMs);
    assertThat(cacheSlotMetadata.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);
    assertThat(cacheSlotMetadata.hostname).isEqualTo(hostname);

    store.createSync(cacheSlotMetadata);
    await().until(() -> store.listSync().size() == 1);

    store.getAndUpdateNonFreeCacheSlotState(name, CacheSlotState.LIVE).get(1, TimeUnit.SECONDS);

    AtomicReference<CacheSlotMetadata> liveNode = new AtomicReference<>();
    await()
        .until(
            () -> {
              liveNode.set(store.getSync(name));
              return liveNode.get().cacheSlotState == CacheSlotState.LIVE;
            });
    assertThat(liveNode.get().name).isEqualTo(name);
    assertThat(liveNode.get().cacheSlotState).isEqualTo(CacheSlotState.LIVE);
    assertThat(liveNode.get().replicaId).isEqualTo(replicaId);
    assertThat(liveNode.get().updatedTimeEpochMs)
        .isGreaterThan(cacheSlotMetadata.updatedTimeEpochMs);
    assertThat(liveNode.get().supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    store.getAndUpdateNonFreeCacheSlotState(name, CacheSlotState.EVICT).get(1, TimeUnit.SECONDS);

    AtomicReference<CacheSlotMetadata> evictNode = new AtomicReference<>();
    await()
        .until(
            () -> {
              evictNode.set(store.getSync(name));
              return evictNode.get().cacheSlotState == CacheSlotState.EVICT;
            });
    assertThat(evictNode.get().name).isEqualTo(name);
    assertThat(evictNode.get().cacheSlotState).isEqualTo(CacheSlotState.EVICT);
    assertThat(evictNode.get().replicaId).isEqualTo(replicaId);
    assertThat(evictNode.get().updatedTimeEpochMs).isGreaterThan(liveNode.get().updatedTimeEpochMs);
    assertThat(evictNode.get().supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    store.getAndUpdateNonFreeCacheSlotState(name, CacheSlotState.FREE).get(1, TimeUnit.SECONDS);

    AtomicReference<CacheSlotMetadata> freeNode = new AtomicReference<>();
    await()
        .until(
            () -> {
              freeNode.set(store.getSync(name));
              return freeNode.get().cacheSlotState == CacheSlotState.FREE;
            });

    assertThat(freeNode.get().name).isEqualTo(name);
    assertThat(freeNode.get().cacheSlotState).isEqualTo(CacheSlotState.FREE);
    assertThat(freeNode.get().replicaId).isEmpty();
    assertThat(freeNode.get().updatedTimeEpochMs).isGreaterThan(evictNode.get().updatedTimeEpochMs);
    assertThat(freeNode.get().supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    // Only non-free states can be set.
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                store
                    .getAndUpdateNonFreeCacheSlotState(name, CacheSlotState.ASSIGNED)
                    .get(1, TimeUnit.SECONDS));
  }

  @Test
  public void testNonFreeCacheSlotState() throws Exception {
    final String hostname = "hostname";
    final String name = "slot1";
    Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState = CacheSlotState.ASSIGNED;
    final String replicaId = "3456";
    long updatedTimeEpochMs = Instant.now().toEpochMilli();

    final CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            name, cacheSlotState, replicaId, updatedTimeEpochMs, SUPPORTED_INDEX_TYPES, hostname);
    assertThat(cacheSlotMetadata.name).isEqualTo(name);
    assertThat(cacheSlotMetadata.cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(cacheSlotMetadata.replicaId).isEqualTo(replicaId);
    assertThat(cacheSlotMetadata.updatedTimeEpochMs).isEqualTo(updatedTimeEpochMs);
    assertThat(cacheSlotMetadata.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);
    assertThat(cacheSlotMetadata.hostname).isEqualTo(hostname);

    store.createSync(cacheSlotMetadata);
    await().until(() -> store.listSync().size() == 1);

    store
        .updateNonFreeCacheSlotState(cacheSlotMetadata, CacheSlotState.LIVE)
        .get(1, TimeUnit.SECONDS);

    AtomicReference<CacheSlotMetadata> liveNode = new AtomicReference<>();
    await()
        .until(
            () -> {
              liveNode.set(store.getSync(name));
              return liveNode.get().cacheSlotState == CacheSlotState.LIVE;
            });
    assertThat(liveNode.get().name).isEqualTo(name);
    assertThat(liveNode.get().cacheSlotState).isEqualTo(CacheSlotState.LIVE);
    assertThat(liveNode.get().replicaId).isEqualTo(replicaId);
    assertThat(liveNode.get().updatedTimeEpochMs)
        .isGreaterThan(cacheSlotMetadata.updatedTimeEpochMs);
    assertThat(liveNode.get().supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    store
        .updateNonFreeCacheSlotState(liveNode.get(), CacheSlotState.EVICT)
        .get(1, TimeUnit.SECONDS);

    AtomicReference<CacheSlotMetadata> evictNode = new AtomicReference<>();
    await()
        .until(
            () -> {
              evictNode.set(store.getSync(name));
              return evictNode.get().cacheSlotState == CacheSlotState.EVICT;
            });
    assertThat(evictNode.get().name).isEqualTo(name);
    assertThat(evictNode.get().cacheSlotState).isEqualTo(CacheSlotState.EVICT);
    assertThat(evictNode.get().replicaId).isEqualTo(replicaId);
    assertThat(evictNode.get().updatedTimeEpochMs).isGreaterThan(liveNode.get().updatedTimeEpochMs);
    assertThat(evictNode.get().supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    store
        .updateNonFreeCacheSlotState(evictNode.get(), CacheSlotState.FREE)
        .get(1, TimeUnit.SECONDS);

    AtomicReference<CacheSlotMetadata> freeNode = new AtomicReference<>();
    await()
        .until(
            () -> {
              freeNode.set(store.getSync(name));
              return freeNode.get().cacheSlotState == CacheSlotState.FREE;
            });
    assertThat(freeNode.get().name).isEqualTo(name);
    assertThat(freeNode.get().cacheSlotState).isEqualTo(CacheSlotState.FREE);
    assertThat(freeNode.get().replicaId).isEmpty();
    assertThat(freeNode.get().updatedTimeEpochMs).isGreaterThan(evictNode.get().updatedTimeEpochMs);
    assertThat(freeNode.get().supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    // Only non-free states can be set.
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                store
                    .updateNonFreeCacheSlotState(freeNode.get(), CacheSlotState.ASSIGNED)
                    .get(1, TimeUnit.SECONDS));
  }

  @Test
  public void testCacheSlotStateWithReplica() throws Exception {
    String hostname = "hostname";
    String name = "slot1";
    Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState =
        Metadata.CacheSlotMetadata.CacheSlotState.FREE;
    String emptyReplicaId = "";
    long updatedTimeEpochMs = Instant.now().toEpochMilli();

    final CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            name,
            cacheSlotState,
            emptyReplicaId,
            updatedTimeEpochMs,
            SUPPORTED_INDEX_TYPES,
            hostname);
    assertThat(cacheSlotMetadata.name).isEqualTo(name);
    assertThat(cacheSlotMetadata.cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(cacheSlotMetadata.replicaId).isEqualTo(emptyReplicaId);
    assertThat(cacheSlotMetadata.updatedTimeEpochMs).isEqualTo(updatedTimeEpochMs);
    assertThat(cacheSlotMetadata.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);
    assertThat(cacheSlotMetadata.hostname).isEqualTo(hostname);

    store.createSync(cacheSlotMetadata);
    await().until(() -> store.listSync().size() == 1);

    store
        .updateCacheSlotStateStateWithReplicaId(cacheSlotMetadata, cacheSlotState, "")
        .get(1, TimeUnit.SECONDS);

    AtomicReference<CacheSlotMetadata> freeNode = new AtomicReference<>();
    await()
        .until(
            () -> {
              freeNode.set(store.getSync(name));
              return freeNode.get().cacheSlotState == cacheSlotState;
            });
    assertThat(freeNode.get().name).isEqualTo(name);
    assertThat(freeNode.get().cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(freeNode.get().replicaId).isEqualTo(emptyReplicaId);
    assertThat(freeNode.get().updatedTimeEpochMs).isGreaterThan(updatedTimeEpochMs);
    assertThat(freeNode.get().supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    final String replicaId = "1234";
    store
        .updateCacheSlotStateStateWithReplicaId(
            cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED, replicaId)
        .get(1, TimeUnit.SECONDS);

    AtomicReference<CacheSlotMetadata> assignedNode = new AtomicReference<>();
    await()
        .until(
            () -> {
              assignedNode.set(store.getSync(name));
              return assignedNode.get().cacheSlotState == CacheSlotState.ASSIGNED;
            });
    assertThat(assignedNode.get().name).isEqualTo(name);
    assertThat(assignedNode.get().cacheSlotState)
        .isEqualTo(Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED);
    assertThat(assignedNode.get().replicaId).isEqualTo(replicaId);
    assertThat(assignedNode.get().updatedTimeEpochMs)
        .isGreaterThan(freeNode.get().updatedTimeEpochMs);
    assertThat(assignedNode.get().supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    store
        .updateCacheSlotStateStateWithReplicaId(
            cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.EVICT, replicaId)
        .get(1, TimeUnit.SECONDS);

    AtomicReference<CacheSlotMetadata> evictedNode = new AtomicReference<>();
    await()
        .until(
            () -> {
              evictedNode.set(store.getSync(name));
              return evictedNode.get().cacheSlotState == CacheSlotState.EVICT;
            });
    assertThat(evictedNode.get().name).isEqualTo(name);
    assertThat(evictedNode.get().cacheSlotState)
        .isEqualTo(Metadata.CacheSlotMetadata.CacheSlotState.EVICT);
    assertThat(evictedNode.get().replicaId).isEqualTo(replicaId);
    assertThat(evictedNode.get().updatedTimeEpochMs)
        .isGreaterThan(freeNode.get().updatedTimeEpochMs);
    assertThat(evictedNode.get().supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);
  }
}
