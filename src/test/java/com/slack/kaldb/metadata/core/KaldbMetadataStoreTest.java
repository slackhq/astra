package com.slack.kaldb.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataSerializer;
import com.slack.kaldb.metadata.zookeeper.InternalMetadataStoreException;
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
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Enclosed.class)
public class KaldbMetadataStoreTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(DummyPersistentMutableMetadataStore.class);

  static SnapshotMetadata makeSnapshot(String name) {
    return makeSnapshot(name, 100);
  }

  static SnapshotMetadata makeSnapshot(String name, long maxOffset) {
    final String snapshotPath = "s3://snapshots/path";
    final String snapshotId = name + "Id";
    final long startTimeUtc = 12345;
    final long endTimeUtc = 123456;
    final String partitionId = "1";

    return new SnapshotMetadata(
        name, snapshotPath, snapshotId, startTimeUtc, endTimeUtc, maxOffset, partitionId);
  }

  static class DummyPersistentMutableMetadataStore
      extends PersistentMutableMetadataStore<SnapshotMetadata> {
    public DummyPersistentMutableMetadataStore(
        boolean shouldCache,
        boolean updatable,
        String storeFolder,
        MetadataStore metadataStore,
        MetadataSerializer<SnapshotMetadata> metadataSerializer,
        Logger logger)
        throws Exception {
      super(shouldCache, updatable, storeFolder, metadataStore, metadataSerializer, logger);
    }
  }

  public static class TestCreatableUpdatableCacheablePersistentMetadataStore {
    private TestingServer testingServer;
    private ZookeeperMetadataStoreImpl _metadataStore;
    private MeterRegistry meterRegistry;
    private DummyPersistentMutableMetadataStore store;

    @Before
    public void setUp() throws Exception {
      meterRegistry = new SimpleMeterRegistry();
      // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are
      // flaky.
      testingServer = new TestingServer();
      CountingFatalErrorHandler countingFatalErrorHandler = new CountingFatalErrorHandler();
      _metadataStore =
          new ZookeeperMetadataStoreImpl(
              testingServer.getConnectString(),
              "test",
              1000,
              1000,
              new RetryNTimes(1, 500),
              countingFatalErrorHandler,
              meterRegistry);
      this.store =
          new DummyPersistentMutableMetadataStore(
              true, true, "/snapshots", _metadataStore, new SnapshotMetadataSerializer(), LOG);
    }

    @After
    public void tearDown() throws IOException {
      _metadataStore.close();
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

      final SnapshotMetadata snapshot =
          new SnapshotMetadata(
              name, snapshotPath, snapshotId, startTimeUtc, endTimeUtc, maxOffset, partitionId);

      assertThat(store.list().get()).isEmpty();
      assertThat(store.create(snapshot).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.list().get()).containsOnly(snapshot);

      SnapshotMetadata metadata = store.get(name).get();
      assertThat(metadata.name).isEqualTo(name);
      assertThat(metadata.snapshotPath).isEqualTo(snapshotPath);
      assertThat(metadata.snapshotId).isEqualTo(snapshotId);
      assertThat(metadata.startTimeUtc).isEqualTo(startTimeUtc);
      assertThat(metadata.endTimeUtc).isEqualTo(endTimeUtc);
      assertThat(metadata.maxOffset).isEqualTo(maxOffset);
      assertThat(metadata.partitionId).isEqualTo(partitionId);

      String newSnapshotId = "newSnapshotId";
      final SnapshotMetadata newSnapshot =
          new SnapshotMetadata(
              name,
              snapshotPath,
              newSnapshotId,
              startTimeUtc + 1,
              endTimeUtc + 1,
              maxOffset + 100,
              partitionId);
      assertThat(store.update(newSnapshot).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.list().get()).containsOnly(newSnapshot);
      SnapshotMetadata newMetadata = store.get(name).get();
      assertThat(newMetadata.name).isEqualTo(name);
      assertThat(newMetadata.snapshotPath).isEqualTo(snapshotPath);
      assertThat(newMetadata.snapshotId).isEqualTo(newSnapshotId);
      assertThat(newMetadata.startTimeUtc).isEqualTo(startTimeUtc + 1);
      assertThat(newMetadata.endTimeUtc).isEqualTo(endTimeUtc + 1);
      assertThat(newMetadata.maxOffset).isEqualTo(maxOffset + 100);
      assertThat(newMetadata.partitionId).isEqualTo(partitionId);

      assertThat(store.delete(name).get()).isNull();
      assertThat(store.list().get()).isEmpty();
    }

    @Test
    public void testMultipleCreates() throws ExecutionException, InterruptedException {
      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);

      assertThat(store.list().get()).isEmpty();
      assertThat(store.create(snapshot1).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.create(snapshot2).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);

      SnapshotMetadata newSnapshot1 = makeSnapshot(name1, 300);
      assertThat(store.update(newSnapshot1).get()).isNull();
      assertThat(store.list().get()).containsOnly(newSnapshot1, snapshot2);

      assertThat(store.delete(name2).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.list().get()).containsOnly(newSnapshot1);

      assertThat(store.delete(name1).get()).isNull();
      assertThat(store.list().get().isEmpty()).isTrue();

      Throwable throwable = catchThrowable(() -> store.delete(name1).get());
      assertThat(throwable.getCause()).isInstanceOf(NoNodeException.class);
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
      assertThat(store.create(testSnapshot).get()).isNull();

      SnapshotMetadata metadata = store.get(name).get();
      assertThat(metadata.name).isEqualTo(name);
      assertThat(metadata.snapshotPath).isEqualTo(snapshotPath);
      assertThat(metadata.snapshotId).isEqualTo(snapshotId);
      assertThat(metadata.startTimeUtc).isEqualTo(startTimeUtc);
      assertThat(metadata.endTimeUtc).isEqualTo(endTimeUtc);
      assertThat(metadata.maxOffset).isEqualTo(maxOffset);
      assertThat(metadata.partitionId).isEqualTo(partitionId);

      Throwable duplicateCreateEx = catchThrowable(() -> store.create(testSnapshot).get());
      assertThat(duplicateCreateEx.getCause()).isInstanceOf(NodeExistsException.class);

      assertThat(store.delete(name).get()).isNull();

      Throwable getMissingNodeEx = catchThrowable(() -> store.get(name).get());
      assertThat(getMissingNodeEx.getCause()).isInstanceOf(NoNodeException.class);

      Throwable deleteMissingNodeEx = catchThrowable(() -> store.delete(name).get());
      assertThat(deleteMissingNodeEx.getCause()).isInstanceOf(NoNodeException.class);
    }

    @Test
    public void testStoreOperationsOnStoppedServer()
        throws ExecutionException, InterruptedException, IOException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      assertThat(store.list().get()).containsOnly(snapshot1);
      assertThat(store.list().get().size()).isEqualTo(1);

      // Stop the ZK server
      testingServer.stop();

      Throwable createEx = catchThrowable(() -> store.create(snapshot1).get());
      assertThat(createEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

      Throwable updateEx = catchThrowable(() -> store.update(snapshot1).get());
      assertThat(updateEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

      Throwable deleteEx = catchThrowable(() -> store.delete(name1).get());
      assertThat(deleteEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

      Throwable listEx = catchThrowable(() -> store.list().get());
      assertThat(listEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

      Throwable getEx = catchThrowable(() -> store.get(name1).get());
      assertThat(getEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);
    }

    // TODO: Add tests for disabled cache store and update store.

    // TODO: Check cached operations in all these tests.
    @Test
    public void testCachedStore() {}
  }

  // TODO: Add tests for enabled cache.
  // TODO: Add tests for disabled cache.
  // TODO: Add unit tests for EphemeralPersistentStore.
  // TODO: Add a unit test for updatable store.
  // TODO: Add a unit test for creatable only store.
}
