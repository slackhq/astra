package com.slack.kaldb.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataSerializer;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.NoNodeException;
import com.slack.kaldb.metadata.zookeeper.NodeExistsException;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.util.CountingFatalErrorHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbMetadataStoreTest {
  private static final Logger LOG = LoggerFactory.getLogger(DummyPersistentMetadataStore.class);

  static class DummyPersistentMetadataStore
      extends CreatablePersistentMetadataStore<SnapshotMetadata> {
    public DummyPersistentMetadataStore(
        MetadataStore metadataStore,
        String storeFolder,
        MetadataSerializer<SnapshotMetadata> metadataSerializer,
        Logger logger) {
      super(metadataStore, storeFolder, metadataSerializer, logger);
    }
  }

  private TestingServer testingServer;
  private ZookeeperMetadataStoreImpl metadataStore;
  private MeterRegistry meterRegistry;
  private CountingFatalErrorHandler countingFatalErrorHandler;
  private DummyPersistentMetadataStore dummyKaldbMetadataStore;

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
    this.dummyKaldbMetadataStore =
        new DummyPersistentMetadataStore(
            metadataStore, "/snapshots", new SnapshotMetadataSerializer(), LOG);
  }

  @After
  public void tearDown() throws IOException {
    metadataStore.close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testCreateGetDelete() throws ExecutionException, InterruptedException {
    final String name = "testSnapshot";
    final String snapshotPath = "s3://snapshots/path";
    final String snapshotId = "testSnapshotId";
    final long startTimeUtc = 12345;
    final long endTimeUtc = 123456;
    final long maxOffset = 100;
    final String partitionId = "1";

    final SnapshotMetadata testSnapshot =
        new SnapshotMetadata(
            name, snapshotPath, snapshotId, startTimeUtc, endTimeUtc, maxOffset, partitionId);
    assertThat(dummyKaldbMetadataStore.create(testSnapshot).get()).isNull();

    SnapshotMetadata metadata = dummyKaldbMetadataStore.get(name).get();
    assertThat(metadata.name).isEqualTo(name);
    assertThat(metadata.snapshotPath).isEqualTo(snapshotPath);
    assertThat(metadata.snapshotId).isEqualTo(snapshotId);
    assertThat(metadata.startTimeUtc).isEqualTo(startTimeUtc);
    assertThat(metadata.endTimeUtc).isEqualTo(endTimeUtc);
    assertThat(metadata.maxOffset).isEqualTo(maxOffset);
    assertThat(metadata.partitionId).isEqualTo(partitionId);

    assertThat(dummyKaldbMetadataStore.delete(name).get()).isNull();
  }

  @Test
  public void testDuplicateCreateNode() throws ExecutionException, InterruptedException {
    final String name = "testSnapshot";
    final String snapshotPath = "s3://snapshots/path";
    final String snapshotId = "testSnapshotId";
    final long startTimeUtc = 12345;
    final long endTimeUtc = 123456;
    final long maxOffset = 100;
    final String partitionId = "1";

    final SnapshotMetadata testSnapshot =
        new SnapshotMetadata(
            name, snapshotPath, snapshotId, startTimeUtc, endTimeUtc, maxOffset, partitionId);
    assertThat(dummyKaldbMetadataStore.create(testSnapshot).get()).isNull();

    SnapshotMetadata metadata = dummyKaldbMetadataStore.get(name).get();
    assertThat(metadata.name).isEqualTo(name);
    assertThat(metadata.snapshotPath).isEqualTo(snapshotPath);
    assertThat(metadata.snapshotId).isEqualTo(snapshotId);
    assertThat(metadata.startTimeUtc).isEqualTo(startTimeUtc);
    assertThat(metadata.endTimeUtc).isEqualTo(endTimeUtc);
    assertThat(metadata.maxOffset).isEqualTo(maxOffset);
    assertThat(metadata.partitionId).isEqualTo(partitionId);

    Throwable duplicateCreateEx =
        catchThrowable(() -> dummyKaldbMetadataStore.create(testSnapshot).get());
    assertThat(duplicateCreateEx.getCause()).isInstanceOf(NodeExistsException.class);

    assertThat(dummyKaldbMetadataStore.delete(name).get()).isNull();

    Throwable getMissingNodeEx = catchThrowable(() -> dummyKaldbMetadataStore.get(name).get());
    assertThat(getMissingNodeEx.getCause()).isInstanceOf(NoNodeException.class);

    Throwable deleteMissingNodeEx =
        catchThrowable(() -> dummyKaldbMetadataStore.delete(name).get());
    assertThat(deleteMissingNodeEx.getCause()).isInstanceOf(NoNodeException.class);
  }
}
