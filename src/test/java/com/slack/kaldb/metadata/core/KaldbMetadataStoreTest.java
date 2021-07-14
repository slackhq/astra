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
  private static final Logger LOG =
      LoggerFactory.getLogger(DummyPersistentMutableMetadataStore.class);

  static class DummyPersistentMutableMetadataStore
      extends PersistentMutableMetadataStore<SnapshotMetadata> {
    public DummyPersistentMutableMetadataStore(
        boolean shouldCache,
        boolean updatable,
        MetadataStore metadataStore,
        String storeFolder,
        MetadataSerializer<SnapshotMetadata> metadataSerializer,
        Logger logger)
        throws Exception {
      super(shouldCache, updatable, storeFolder, metadataStore, metadataSerializer, logger);
    }

    public DummyPersistentMutableMetadataStore(
        MetadataStore metadataStore,
        String storeFolder,
        MetadataSerializer<SnapshotMetadata> metadataSerializer,
        Logger logger)
        throws Exception {
      super(false, false, storeFolder, metadataStore, metadataSerializer, logger);
    }
  }

  // TODO: Add tests for enabled cache.
  // TODO: Add tests for disabled cache.
  // TODO: Add unit tests for EphemeralPersistentStore.
  // TODO: Add a unit test for updatable store.
  // TODO: Add a unit test for creatable only store.

  private TestingServer testingServer;
  private ZookeeperMetadataStoreImpl metadataStore;
  private MeterRegistry meterRegistry;
  private CountingFatalErrorHandler countingFatalErrorHandler;
  private DummyPersistentMutableMetadataStore creatablePersistentKaldbMetadataStore;

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
    this.creatablePersistentKaldbMetadataStore =
        new DummyPersistentMutableMetadataStore(
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

    // TODO: Enable below check if we assume store always ensures node exists.
    // assertThat(creatablePersistentKaldbMetadataStore.list().get()).isEmpty();
    assertThat(creatablePersistentKaldbMetadataStore.create(testSnapshot).get()).isNull();
    assertThat(creatablePersistentKaldbMetadataStore.list().get().size()).isEqualTo(1);

    SnapshotMetadata metadata = creatablePersistentKaldbMetadataStore.get(name).get();
    assertThat(metadata.name).isEqualTo(name);
    assertThat(metadata.snapshotPath).isEqualTo(snapshotPath);
    assertThat(metadata.snapshotId).isEqualTo(snapshotId);
    assertThat(metadata.startTimeUtc).isEqualTo(startTimeUtc);
    assertThat(metadata.endTimeUtc).isEqualTo(endTimeUtc);
    assertThat(metadata.maxOffset).isEqualTo(maxOffset);
    assertThat(metadata.partitionId).isEqualTo(partitionId);

    assertThat(creatablePersistentKaldbMetadataStore.delete(name).get()).isNull();
    assertThat(creatablePersistentKaldbMetadataStore.list().get()).isEmpty();
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
    assertThat(creatablePersistentKaldbMetadataStore.create(testSnapshot).get()).isNull();

    SnapshotMetadata metadata = creatablePersistentKaldbMetadataStore.get(name).get();
    assertThat(metadata.name).isEqualTo(name);
    assertThat(metadata.snapshotPath).isEqualTo(snapshotPath);
    assertThat(metadata.snapshotId).isEqualTo(snapshotId);
    assertThat(metadata.startTimeUtc).isEqualTo(startTimeUtc);
    assertThat(metadata.endTimeUtc).isEqualTo(endTimeUtc);
    assertThat(metadata.maxOffset).isEqualTo(maxOffset);
    assertThat(metadata.partitionId).isEqualTo(partitionId);

    Throwable duplicateCreateEx =
        catchThrowable(() -> creatablePersistentKaldbMetadataStore.create(testSnapshot).get());
    assertThat(duplicateCreateEx.getCause()).isInstanceOf(NodeExistsException.class);

    assertThat(creatablePersistentKaldbMetadataStore.delete(name).get()).isNull();

    Throwable getMissingNodeEx =
        catchThrowable(() -> creatablePersistentKaldbMetadataStore.get(name).get());
    assertThat(getMissingNodeEx.getCause()).isInstanceOf(NoNodeException.class);

    Throwable deleteMissingNodeEx =
        catchThrowable(() -> creatablePersistentKaldbMetadataStore.delete(name).get());
    assertThat(deleteMissingNodeEx.getCause()).isInstanceOf(NoNodeException.class);
  }
}
