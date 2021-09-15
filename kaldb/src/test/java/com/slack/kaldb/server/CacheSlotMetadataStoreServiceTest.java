package com.slack.kaldb.server;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.util.CountingFatalErrorHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class CacheSlotMetadataStoreServiceTest {
  private TestingServer testingServer;
  private ZookeeperMetadataStoreImpl metadataStore;
  private MeterRegistry meterRegistry;
  private ChunkManagerUtil<LogMessage> chunkManagerUtil;

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  @Before
  public void startup() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        new ChunkManagerUtil<>(S3_MOCK_RULE, meterRegistry, 10 * 1024 * 1024 * 1024L, 100);

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
  public void shutdown() throws Exception {
    chunkManagerUtil.close();
    metadataStore.close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldStartAndShutdown() throws Exception {
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

    CacheSlotMetadataStoreService service =
        new CacheSlotMetadataStoreService(
            metadataStoreService, chunkManagerUtil.chunkManager, cacheConfig);
    service.startAsync();
    service.awaitRunning(15, TimeUnit.SECONDS);

    service.stopAsync();
    metadataStoreService.stopAsync();

    service.awaitTerminated(15, TimeUnit.SECONDS);
    metadataStoreService.awaitTerminated(15, TimeUnit.SECONDS);
  }
}
