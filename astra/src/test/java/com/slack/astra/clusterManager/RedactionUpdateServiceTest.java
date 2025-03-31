package com.slack.astra.clusterManager;

import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class RedactionUpdateServiceTest {
  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private AsyncCuratorFramework curatorFramework;
  private FieldRedactionMetadataStore fieldRedactionMetadataStore;

  @BeforeEach
  public void setup() throws Exception {
    testingServer = new TestingServer();
    meterRegistry = new SimpleMeterRegistry();

    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("RecoveryTaskAssignmentServiceTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .setZkCacheInitTimeoutMs(1000)
            .build();

    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    fieldRedactionMetadataStore = new FieldRedactionMetadataStore(curatorFramework, zkConfig, true);
  }

  @AfterEach
  public void shutdown() throws IOException {
    fieldRedactionMetadataStore.close();
    curatorFramework.unwrap().close();

    testingServer.close();
    meterRegistry.close();
  }
}
