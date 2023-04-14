package com.slack.kaldb.metadata.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.util.CountingFatalErrorHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RecoveryNodeMetadataStoreTest {

  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private ZookeeperMetadataStoreImpl zkMetadataStore;
  private RecoveryNodeMetadataStore uncachedStore;

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

    this.uncachedStore = new RecoveryNodeMetadataStore(zkMetadataStore, false);
  }

  @After
  public void tearDown() throws IOException {
    zkMetadataStore.close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testUpdateRecoveryNodeState() {
    final String name = "name";
    final Metadata.RecoveryNodeMetadata.RecoveryNodeState recoveryNodeState =
        Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE;
    final String recoveryTaskName = "taskName";
    final long updatedTimeEpochMs = Instant.now().toEpochMilli();

    final List<Metadata.IndexType> supportedIndexTypes =
        List.of(Metadata.IndexType.LOGS_LUCENE9, Metadata.IndexType.LOGS_LUCENE9);
    RecoveryNodeMetadata recoveryNode =
        new RecoveryNodeMetadata(
            name, recoveryNodeState, "", supportedIndexTypes, updatedTimeEpochMs);
    assertThat(recoveryNode.recoveryNodeState).isEqualTo(recoveryNodeState);
    assertThat(recoveryNode.name).isEqualTo(name);

    uncachedStore.createSync(recoveryNode);
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);

    uncachedStore.updateRecoveryNodeState(
        recoveryNode, Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED, recoveryTaskName);
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);
    final RecoveryNodeMetadata assignedNode = uncachedStore.listSync().get(0);
    assertThat(assignedNode.name).isEqualTo(name);
    assertThat(assignedNode.recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED);
    assertThat(assignedNode.recoveryTaskName).isEqualTo(recoveryTaskName);
    assertThat(assignedNode.supportedIndexTypes)
        .containsExactlyInAnyOrderElementsOf(supportedIndexTypes);
    assertThat(assignedNode.updatedTimeEpochMs).isGreaterThan(updatedTimeEpochMs);

    uncachedStore.updateRecoveryNodeState(
        assignedNode, Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING, recoveryTaskName);
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);
    final RecoveryNodeMetadata recoveringNode = uncachedStore.listSync().get(0);
    assertThat(recoveringNode.name).isEqualTo(name);
    assertThat(recoveringNode.recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING);
    assertThat(recoveringNode.recoveryTaskName).isEqualTo(recoveryTaskName);
    assertThat(recoveringNode.supportedIndexTypes)
        .containsExactlyInAnyOrderElementsOf(supportedIndexTypes);
    assertThat(recoveringNode.updatedTimeEpochMs).isGreaterThan(assignedNode.updatedTimeEpochMs);

    uncachedStore.updateRecoveryNodeState(
        recoveringNode, Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE, "");
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);
    final RecoveryNodeMetadata freeNode = uncachedStore.listSync().get(0);
    assertThat(freeNode.name).isEqualTo(name);
    assertThat(freeNode.recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
    assertThat(freeNode.recoveryTaskName).isEmpty();
    assertThat(freeNode.supportedIndexTypes)
        .containsExactlyInAnyOrderElementsOf(supportedIndexTypes);
    assertThat(freeNode.updatedTimeEpochMs).isGreaterThan(recoveringNode.updatedTimeEpochMs);

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                uncachedStore.updateRecoveryNodeState(
                    recoveryNode,
                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
                    recoveryTaskName));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                uncachedStore.updateRecoveryNodeState(
                    recoveryNode, Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED, ""));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                uncachedStore.updateRecoveryNodeState(
                    recoveryNode, Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING, ""));
  }

  @Test
  public void testGetAndUpdateNonFreeRecoveryNodeState() {
    final String name = "name";
    final Metadata.RecoveryNodeMetadata.RecoveryNodeState recoveryNodeState =
        Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED;
    final String recoveryTaskName = "taskName";
    final long updatedTimeEpochMs = Instant.now().toEpochMilli();
    final List<Metadata.IndexType> supportedIndexTypes = List.of(Metadata.IndexType.LOGS_LUCENE9);

    RecoveryNodeMetadata recoveryNode =
        new RecoveryNodeMetadata(
            name, recoveryNodeState, recoveryTaskName, supportedIndexTypes, updatedTimeEpochMs);

    uncachedStore.createSync(recoveryNode);
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);

    uncachedStore.getAndUpdateNonFreeRecoveryNodeStateSync(
        name, Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING);
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);
    final RecoveryNodeMetadata recoveringNode = uncachedStore.listSync().get(0);
    assertThat(recoveringNode.name).isEqualTo(name);
    assertThat(recoveringNode.recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING);
    assertThat(recoveringNode.recoveryTaskName).isEqualTo(recoveryTaskName);
    assertThat(recoveringNode.supportedIndexTypes)
        .containsExactlyInAnyOrderElementsOf(supportedIndexTypes);
    assertThat(recoveringNode.updatedTimeEpochMs).isGreaterThan(updatedTimeEpochMs);

    uncachedStore.getAndUpdateNonFreeRecoveryNodeStateSync(
        name, Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
    assertThat(uncachedStore.listSync().size()).isEqualTo(1);
    final RecoveryNodeMetadata freeNode = uncachedStore.listSync().get(0);
    assertThat(freeNode.name).isEqualTo(name);
    assertThat(freeNode.recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
    assertThat(freeNode.recoveryTaskName).isEmpty();
    assertThat(freeNode.supportedIndexTypes)
        .containsExactlyInAnyOrderElementsOf(supportedIndexTypes);
    assertThat(freeNode.updatedTimeEpochMs).isGreaterThan(recoveringNode.updatedTimeEpochMs);
  }
}
