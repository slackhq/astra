package com.slack.astra.metadata.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SearchMetadataStoreTest {
  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private AsyncCuratorFramework curatorFramework;
  private AstraConfigs.MetadataStoreConfig metadataStoreConfig;
  private SearchMetadataStore store;

  @BeforeEach
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("Test")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(500)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();
    this.curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (store != null) store.close();
    curatorFramework.unwrap().close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testSearchMetadataStoreUpdateSearchability() throws Exception {
    store = new SearchMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true);
    SearchMetadata searchMetadata = new SearchMetadata("test", "snapshot", "http", false);
    assertThat(searchMetadata.isSearchable()).isFalse();
    store.createSync(searchMetadata);

    store.updateSearchability(searchMetadata, true);

    // Confirm that this eventually becomes searchable
    await().atMost(5, TimeUnit.SECONDS).until(() -> store.getSync("test").isSearchable());
  }

  @Test
  public void testSearchMetadataStoreIsNotUpdatable() throws Exception {
    store = new SearchMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true);
    SearchMetadata searchMetadata = new SearchMetadata("test", "snapshot", "http");
    Throwable exAsync = catchThrowable(() -> store.updateAsync(searchMetadata));
    assertThat(exAsync).isInstanceOf(UnsupportedOperationException.class);

    Throwable exSync = catchThrowable(() -> store.updateSync(searchMetadata));
    assertThat(exSync).isInstanceOf(UnsupportedOperationException.class);
  }
}
