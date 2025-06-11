package com.slack.astra.metadata.redaction;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FieldRedactionMetadataStoreTest {

  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private AsyncCuratorFramework curatorFramework;
  private FieldRedactionMetadataStore store;

  @BeforeEach
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
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
    store =
        new FieldRedactionMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true);
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (store != null) store.close();
    curatorFramework.unwrap().close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testFieldRedactionMetadataStore() throws IOException {
    final String name = "testRedaction";
    final String fieldName = "testField";
    final long start = Instant.now().toEpochMilli();
    final long end = Instant.now().plusSeconds(10).toEpochMilli();

    FieldRedactionMetadata fieldRedactionMetadata =
        new FieldRedactionMetadata(name, fieldName, start, end);

    assertThat(fieldRedactionMetadata.name).isEqualTo(name);
    assertThat(fieldRedactionMetadata.fieldName).isEqualTo(fieldName);
    assertThat(fieldRedactionMetadata.startTimeEpochMs).isEqualTo(start);
    assertThat(fieldRedactionMetadata.endTimeEpochMs).isEqualTo(end);

    store.createSync(fieldRedactionMetadata);
    assertThat(AstraMetadataTestUtils.listSyncUncached(store).size()).isEqualTo(1);
  }
}
