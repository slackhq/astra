package com.slack.kaldb.metadata.cache;

import static com.slack.kaldb.proto.metadata.Metadata.CacheSlotMetadata.CacheSlotState;
import static com.slack.kaldb.proto.metadata.Metadata.IndexType.LOGS_LUCENE9;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.util.CountingFatalErrorHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CacheSlotMetadataStoreTest {
  private static final List<Metadata.IndexType> SUPPORTED_INDEX_TYPES = List.of(LOGS_LUCENE9);

  private TestingServer testingServer;
  private ZookeeperMetadataStoreImpl zkMetadataStore;
  private MeterRegistry meterRegistry;
  private CacheSlotMetadataStore uncachedStore;

  @Before
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are
    // flaky.
    testingServer = new TestingServer();
    CountingFatalErrorHandler countingFatalErrorHandler = new CountingFatalErrorHandler();
    zkMetadataStore =
        new ZookeeperMetadataStoreImpl(
            testingServer.getConnectString(),
            "test",
            1000,
            1000,
            new RetryNTimes(1, 500),
            countingFatalErrorHandler,
            meterRegistry);

    this.uncachedStore = new CacheSlotMetadataStore(zkMetadataStore, false);
  }

  @After
  public void tearDown() throws IOException {
    zkMetadataStore.close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testNonFreeCacheSlotState() throws Exception {
    final String name = "slot1";
    Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState = CacheSlotState.ASSIGNED;
    final String replicaId = "3456";
    long updatedTimeEpochMs = Instant.now().toEpochMilli();

    final CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            name, cacheSlotState, replicaId, updatedTimeEpochMs, SUPPORTED_INDEX_TYPES);
    assertThat(cacheSlotMetadata.name).isEqualTo(name);
    assertThat(cacheSlotMetadata.cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(cacheSlotMetadata.replicaId).isEqualTo(replicaId);
    assertThat(cacheSlotMetadata.updatedTimeEpochMs).isEqualTo(updatedTimeEpochMs);
    assertThat(cacheSlotMetadata.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    uncachedStore.createSync(cacheSlotMetadata);
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);

    uncachedStore
        .updateNonFreeCacheSlotState(cacheSlotMetadata, CacheSlotState.LIVE)
        .get(1, TimeUnit.SECONDS);
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);
    final CacheSlotMetadata liveNode = uncachedStore.getNodeSync(name);
    assertThat(liveNode.name).isEqualTo(name);
    assertThat(liveNode.cacheSlotState).isEqualTo(CacheSlotState.LIVE);
    assertThat(liveNode.replicaId).isEqualTo(replicaId);
    assertThat(liveNode.updatedTimeEpochMs).isGreaterThan(cacheSlotMetadata.updatedTimeEpochMs);
    assertThat(liveNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    uncachedStore
        .updateNonFreeCacheSlotState(liveNode, CacheSlotState.EVICT)
        .get(1, TimeUnit.SECONDS);
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);
    final CacheSlotMetadata evictNode = uncachedStore.getNodeSync(name);
    assertThat(evictNode.name).isEqualTo(name);
    assertThat(evictNode.cacheSlotState).isEqualTo(CacheSlotState.EVICT);
    assertThat(evictNode.replicaId).isEqualTo(replicaId);
    assertThat(evictNode.updatedTimeEpochMs).isGreaterThan(liveNode.updatedTimeEpochMs);
    assertThat(evictNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    uncachedStore
        .updateNonFreeCacheSlotState(evictNode, CacheSlotState.FREE)
        .get(1, TimeUnit.SECONDS);
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);
    final CacheSlotMetadata freeNode = uncachedStore.getNodeSync(name);
    assertThat(freeNode.name).isEqualTo(name);
    assertThat(freeNode.cacheSlotState).isEqualTo(CacheSlotState.FREE);
    assertThat(freeNode.replicaId).isEmpty();
    assertThat(freeNode.updatedTimeEpochMs).isGreaterThan(evictNode.updatedTimeEpochMs);
    assertThat(freeNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    // Only non-free states can be set.
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                uncachedStore
                    .updateNonFreeCacheSlotState(freeNode, CacheSlotState.ASSIGNED)
                    .get(1, TimeUnit.SECONDS));
  }

  @Test
  public void testCacheSlotStateWithReplica() throws Exception {
    String name = "slot1";
    Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState =
        Metadata.CacheSlotMetadata.CacheSlotState.FREE;
    String emptyReplicaId = "";
    long updatedTimeEpochMs = Instant.now().toEpochMilli();

    final CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            name, cacheSlotState, emptyReplicaId, updatedTimeEpochMs, SUPPORTED_INDEX_TYPES);
    assertThat(cacheSlotMetadata.name).isEqualTo(name);
    assertThat(cacheSlotMetadata.cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(cacheSlotMetadata.replicaId).isEqualTo(emptyReplicaId);
    assertThat(cacheSlotMetadata.updatedTimeEpochMs).isEqualTo(updatedTimeEpochMs);
    assertThat(cacheSlotMetadata.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    uncachedStore.createSync(cacheSlotMetadata);
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);

    uncachedStore
        .updateCacheSlotStateStateWithReplicaId(cacheSlotMetadata, cacheSlotState, "")
        .get(1, TimeUnit.SECONDS);
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);
    final CacheSlotMetadata freeNode = uncachedStore.getNodeSync(name);
    assertThat(freeNode.name).isEqualTo(name);
    assertThat(freeNode.cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(freeNode.replicaId).isEqualTo(emptyReplicaId);
    assertThat(freeNode.updatedTimeEpochMs).isGreaterThan(updatedTimeEpochMs);
    assertThat(freeNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    final String replicaId = "1234";
    uncachedStore
        .updateCacheSlotStateStateWithReplicaId(
            cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED, replicaId)
        .get(1, TimeUnit.SECONDS);
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);
    CacheSlotMetadata assignedNode = uncachedStore.getNodeSync(name);
    assertThat(assignedNode.name).isEqualTo(name);
    assertThat(assignedNode.cacheSlotState)
        .isEqualTo(Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED);
    assertThat(assignedNode.replicaId).isEqualTo(replicaId);
    assertThat(assignedNode.updatedTimeEpochMs).isGreaterThan(freeNode.updatedTimeEpochMs);
    assertThat(assignedNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    uncachedStore
        .updateCacheSlotStateStateWithReplicaId(
            cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.EVICT, replicaId)
        .get(1, TimeUnit.SECONDS);
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);
    CacheSlotMetadata evictedNode = uncachedStore.getNodeSync(name);
    assertThat(evictedNode.name).isEqualTo(name);
    assertThat(evictedNode.cacheSlotState)
        .isEqualTo(Metadata.CacheSlotMetadata.CacheSlotState.EVICT);
    assertThat(evictedNode.replicaId).isEqualTo(replicaId);
    assertThat(evictedNode.updatedTimeEpochMs).isGreaterThan(freeNode.updatedTimeEpochMs);
    assertThat(evictedNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);
  }
}
