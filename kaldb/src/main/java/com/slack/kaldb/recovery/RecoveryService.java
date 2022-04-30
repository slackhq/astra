package com.slack.kaldb.recovery;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The recovery service is intended to be executed on a recovery node, and is responsible for
 * fulfilling recovery assignments provided from the cluster manager.
 *
 * <p>When the recovery service starts it advertises its availability by creating a recovery node,
 * and then subscribing to any state changes. Upon receiving an assignment from the cluster manager
 * the recovery service will delegate the recovery task to an executor service. Once the recovery
 * task has been completed, the recovery node will make itself available again for assignment.
 */
public class RecoveryService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryService.class);

  private final SearchContext searchContext;
  private final MetadataStore metadataStore;
  private final MeterRegistry meterRegistry;
  // private final KaldbConfigs.RecoveryConfig recoveryConfig;
  private final BlobFs blobFs;
  private final KaldbConfigs.KaldbConfig kaldbConfig;

  private RecoveryNodeMetadataStore recoveryNodeMetadataStore;
  private RecoveryNodeMetadataStore recoveryNodeListenerMetadataStore;
  private RecoveryTaskMetadataStore recoveryTaskMetadataStore;
  private final ExecutorService executorService;

  private Metadata.RecoveryNodeMetadata.RecoveryNodeState recoveryNodeLastKnownState;

  public static final String RECOVERY_NODE_RECEIVED_ASSIGNMENT =
      "recovery_node_received_assignment";
  public static final String RECOVERY_NODE_COMPLETED_ASSIGNMENT =
      "recovery_node_completed_assignment";
  public static final String RECOVERY_NODE_FAILED_ASSIGNMENT = "recovery_node_failed_assignment";
  protected final Counter recoveryNodeReceivedAssignment;
  protected final Counter recoveryNodeCompletedAssignment;
  protected final Counter recoveryNodeFailedAssignment;

  public RecoveryService(
      KaldbConfigs.KaldbConfig kaldbConfig,
      MetadataStore metadataStore,
      MeterRegistry meterRegistry,
      BlobFs blobFs) {
    this.metadataStore = metadataStore;
    this.searchContext =
        SearchContext.fromConfig(kaldbConfig.getRecoveryConfig().getServerConfig());
    this.meterRegistry = meterRegistry;
    this.blobFs = blobFs;
    this.kaldbConfig = kaldbConfig;

    // we use a single thread executor to allow operations for this recovery node to queue,
    // guaranteeing that they are executed in the order they were received
    this.executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("recovery-service-%d").build());

    Collection<Tag> meterTags = ImmutableList.of(Tag.of("nodeHostname", searchContext.hostname));
    recoveryNodeReceivedAssignment =
        meterRegistry.counter(RECOVERY_NODE_RECEIVED_ASSIGNMENT, meterTags);
    recoveryNodeCompletedAssignment =
        meterRegistry.counter(RECOVERY_NODE_COMPLETED_ASSIGNMENT, meterTags);
    recoveryNodeFailedAssignment =
        meterRegistry.counter(RECOVERY_NODE_FAILED_ASSIGNMENT, meterTags);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting recovery service");

    recoveryNodeMetadataStore = new RecoveryNodeMetadataStore(metadataStore, false);
    recoveryTaskMetadataStore = new RecoveryTaskMetadataStore(metadataStore, false);

    recoveryNodeMetadataStore.createSync(
        new RecoveryNodeMetadata(
            searchContext.hostname,
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
            "",
            Instant.now().toEpochMilli()));
    recoveryNodeLastKnownState = Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE;

    recoveryNodeListenerMetadataStore =
        new RecoveryNodeMetadataStore(metadataStore, searchContext.hostname, true);
    recoveryNodeListenerMetadataStore.addListener(recoveryNodeListener());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing the recovery service");

    // Immediately shutdown recovery tasks. Any incomplete recovery tasks will be picked up by
    // another recovery node so we don't need to wait for processing to complete.
    executorService.shutdownNow();

    recoveryNodeListenerMetadataStore.close();
    recoveryNodeMetadataStore.close();

    LOG.info("Closed the recovery service");
  }

  private KaldbMetadataStoreChangeListener recoveryNodeListener() {
    return () -> {
      RecoveryNodeMetadata recoveryNodeMetadata =
          recoveryNodeMetadataStore.getNodeSync(searchContext.hostname);
      Metadata.RecoveryNodeMetadata.RecoveryNodeState newRecoveryNodeState =
          recoveryNodeMetadata.recoveryNodeState;

      if (newRecoveryNodeState.equals(Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED)) {
        LOG.info("Recovery node - ASSIGNED received");
        recoveryNodeReceivedAssignment.increment();
        if (!recoveryNodeLastKnownState.equals(
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE)) {
          LOG.warn(
              "Unexpected state transition from {} to {}",
              recoveryNodeLastKnownState,
              newRecoveryNodeState);
        }
        executorService.submit(() -> handleRecoveryTaskAssignment(recoveryNodeMetadata));
      }
      recoveryNodeLastKnownState = newRecoveryNodeState;
    };
  }

  /**
   * This method is invoked after the cluster manager has assigned a recovery node a task. As part
   * of handling a task assignment we should update the recovery node state as we make progress (ie,
   * Recovering and then Free once completed).
   */
  private void handleRecoveryTaskAssignment(RecoveryNodeMetadata recoveryNodeMetadata) {
    try {
      setRecoveryNodeMetadataState(Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING);
      RecoveryTaskMetadata recoveryTaskMetadata =
          recoveryTaskMetadataStore.getNodeSync(recoveryNodeMetadata.recoveryTaskName);

      // Process recovery task.
      handleRecoveryTask(recoveryTaskMetadata);

      // delete the completed recovery task
      recoveryTaskMetadataStore.deleteSync(recoveryTaskMetadata.name);

      setRecoveryNodeMetadataState(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
      recoveryNodeCompletedAssignment.increment();
    } catch (Exception e) {
      setRecoveryNodeMetadataState(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
      LOG.error("Failed to complete recovery node task assignment", e);
      recoveryNodeFailedAssignment.increment();
    }
  }

  // index between recoveryTaskMetadata.startOffset and recoveryTaskMetadata.endOffset,
  //   upload to S3, create the snapshot
  private void handleRecoveryTask(RecoveryTaskMetadata recoveryTaskMetadata)
      throws InterruptedException {
    // Create an recovery chunk manager from an indexing chunk manager.
    // recovery chunk manager - indexing chunk manager that stops indexing at an offset - no search
    // and no cleaner.
    // on close upload active chunk to S3.
    // returns bool when done.
    //    IndexingChunkManager<LogMessage> chunkManager =
    //        IndexingChunkManager.fromConfig(
    //            meterRegistry,
    //            metadataStore,
    //            // TODO: Replace with params from IndexerConfig
    //            kaldbConfig.getIndexerConfig(),
    //            blobFs,
    //            kaldbConfig.getS3Config());
    //    // Index the data until offset in parallel
    //     KaldbKafkaConsumer kafkaConsumer = new KaldbKafkaConsumer();
    //    kafkaConsumer.consumeMessagesBetweenOffsetsInParallel(
    //        KaldbKafkaConsumer.KAFKA_POLL_TIMEOUT_MS,
    //        recoveryTaskMetadata.startOffset,
    //        recoveryTaskMetadata.endOffset);
    //    // close and upload active chunk to S3.
    //    chunkManager.rollOverActiveChunk();
    //    // Close the indexing chunk manager and kafka consumer.
    //    kafkaConsumer.close();
    //    chunkManager.stopAsync();
  }

  @VisibleForTesting
  public void setRecoveryNodeMetadataState(
      Metadata.RecoveryNodeMetadata.RecoveryNodeState newRecoveryNodeState) {
    RecoveryNodeMetadata recoveryNodeMetadata =
        recoveryNodeMetadataStore.getNodeSync(searchContext.hostname);
    RecoveryNodeMetadata updatedRecoveryNodeMetadata =
        new RecoveryNodeMetadata(
            recoveryNodeMetadata.name,
            newRecoveryNodeState,
            newRecoveryNodeState.equals(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE)
                ? ""
                : recoveryNodeMetadata.recoveryTaskName,
            Instant.now().toEpochMilli());
    recoveryNodeMetadataStore.updateSync(updatedRecoveryNodeMetadata);
  }
}
