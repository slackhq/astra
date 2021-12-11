package com.slack.kaldb.server;

import static com.slack.kaldb.metadata.snapshot.SnapshotMetadata.LIVE_SNAPSHOT_PATH;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.util.CountingFatalErrorHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.List;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RecoveryTaskFactoryTest {
  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private ZookeeperMetadataStoreImpl zkMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private RecoveryTaskMetadataStore recoveryTaskStore;

  @Before
  public void startup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();
    zkMetadataStore =
        new ZookeeperMetadataStoreImpl(
            testingServer.getConnectString(),
            "test",
            1000,
            1000,
            new RetryNTimes(1, 500),
            new CountingFatalErrorHandler(),
            meterRegistry);
    snapshotMetadataStore = new SnapshotMetadataStore(zkMetadataStore, false);
    recoveryTaskStore = new RecoveryTaskMetadataStore(zkMetadataStore, false);
  }

  @After
  public void shutdown() throws IOException {
    recoveryTaskStore.close();
    snapshotMetadataStore.close();
    zkMetadataStore.close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testStaleSnapshotDetection() {
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId);
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "1", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, partitionId);
    SnapshotMetadata livePartition2 =
        new SnapshotMetadata(name + "2", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, "2");
    SnapshotMetadata partition2 =
        new SnapshotMetadata(name + "3", path, startTime, endTime, maxOffset, "2");

    RecoveryTaskFactory recoveryTaskFactory =
        new RecoveryTaskFactory(snapshotMetadataStore, recoveryTaskStore, partitionId);

    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(partition1))).isEmpty();
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(partition2))).isEmpty();
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(livePartition2))).isEmpty();
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(livePartition1)))
        .containsExactly(livePartition1);
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(partition1, livePartition1)))
        .containsExactly(livePartition1);
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(partition2, livePartition1)))
        .containsExactly(livePartition1);
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(livePartition2, livePartition1)))
        .containsExactly(livePartition1);
    assertThat(
            recoveryTaskFactory.getStaleLiveSnapshots(
                List.of(livePartition2, livePartition1, partition1, partition2)))
        .containsExactly(livePartition1);
    assertThat(recoveryTaskFactory.getStaleLiveSnapshots(List.of(partition1, partition2)))
        .isEmpty();
  }
}
