package com.slack.kaldb.chunk;

import static org.awaitility.Awaitility.await;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.chunk.manager.caching.CachingChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.server.MetadataStoreService;
import com.slack.kaldb.util.CountingFatalErrorHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class CachingChunkManagerTest {

  private TestingServer testingServer;
  private ZookeeperMetadataStoreImpl metadataStore;
  private MeterRegistry meterRegistry;

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  @Before
  public void startup() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
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
  }

  @After
  public void shutdown() throws IOException, TimeoutException {
    metadataStore.close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldHandleChunkLivecycle() throws Exception {
    KaldbConfigs.CacheConfig cacheConfig =
        KaldbConfigs.CacheConfig.newBuilder()
            .setSlotsPerInstance(3)
            .setServerConfig(
                KaldbConfigs.ServerConfig.newBuilder().setServerAddress("localhost").build())
            .build();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("test")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    MetadataStoreService metadataStoreService = new MetadataStoreService(meterRegistry, zkConfig);
    metadataStoreService.startAsync();
    metadataStoreService.awaitRunning(15, TimeUnit.SECONDS);

    CachingChunkManager<LogMessage> cachingChunkManager =
        new CachingChunkManager<>(meterRegistry, metadataStoreService, cacheConfig);
    cachingChunkManager.startAsync();
    cachingChunkManager.awaitRunning(15, TimeUnit.SECONDS);

    ReadOnlyChunkImpl readOnlyChunk =
        (ReadOnlyChunkImpl) cachingChunkManager.getChunkMap().values().toArray()[0];

    // wait for chunk to register
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.FREE);

    // update chunk to assigned
    readOnlyChunk.setChunkMetadataState(Metadata.CacheSlotState.ASSIGNED);

    // ensure that the chunk was marked LIVE
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.LIVE);

    // mark the chunk for eviction
    readOnlyChunk.setChunkMetadataState(Metadata.CacheSlotState.EVICT);

    // ensure that the evicted chunk was released
    await().until(() -> readOnlyChunk.getChunkMetadataState() == Metadata.CacheSlotState.FREE);

    cachingChunkManager.stopAsync();
    metadataStoreService.stopAsync();

    cachingChunkManager.awaitTerminated(15, TimeUnit.SECONDS);
    metadataStoreService.awaitTerminated(15, TimeUnit.SECONDS);
  }
}
