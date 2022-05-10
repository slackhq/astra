package com.slack.kaldb.recovery;

import static com.slack.kaldb.server.KaldbConfig.DATA_TRANSFORMER_MAP;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.TextFormat;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.chunkManager.RecoveryChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.writer.LogMessageTransformer;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import com.slack.kaldb.writer.kafka.KaldbKafkaConsumer;
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
 *
 * <p>Look at handleRecoveryTaskAssignment method understand the implementation and limitations of
 * the current implementation.
 */
public class RecoveryService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryService.class);

  private final SearchContext searchContext;
  private final MetadataStore metadataStore;
  private final MeterRegistry meterRegistry;
  private final BlobFs blobFs;
  private final KaldbConfigs.KaldbConfig kaldbConfig;

  private RecoveryNodeMetadataStore recoveryNodeMetadataStore;
  private RecoveryNodeMetadataStore recoveryNodeListenerMetadataStore;
  private RecoveryTaskMetadataStore recoveryTaskMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private final ExecutorService executorService;

  private Metadata.RecoveryNodeMetadata.RecoveryNodeState recoveryNodeLastKnownState;

  public static final String RECOVERY_NODE_ASSIGNMENT_RECEIVED =
      "recovery_node_assignment_received";
  public static final String RECOVERY_NODE_ASSIGNMENT_SUCCESS = "recovery_node_assignment_success";
  public static final String RECOVERY_NODE_ASSIGNMENT_FAILED = "recovery_node_assignment_failed";
  protected final Counter recoveryNodeAssignmentReceived;
  protected final Counter recoveryNodeAssignmentSuccess;
  protected final Counter recoveryNodeAssignmentFailed;
  private SearchMetadataStore searchMetadataStore;

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
    recoveryNodeAssignmentReceived =
        meterRegistry.counter(RECOVERY_NODE_ASSIGNMENT_RECEIVED, meterTags);
    recoveryNodeAssignmentSuccess =
        meterRegistry.counter(RECOVERY_NODE_ASSIGNMENT_SUCCESS, meterTags);
    recoveryNodeAssignmentFailed =
        meterRegistry.counter(RECOVERY_NODE_ASSIGNMENT_FAILED, meterTags);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting recovery service");

    recoveryNodeMetadataStore = new RecoveryNodeMetadataStore(metadataStore, false);
    recoveryTaskMetadataStore = new RecoveryTaskMetadataStore(metadataStore, false);
    snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
    searchMetadataStore = new SearchMetadataStore(metadataStore, false);

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

    searchMetadataStore.close();
    snapshotMetadataStore.close();
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
        recoveryNodeAssignmentReceived.increment();
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
   * of handling a task assignment we update the recovery node state to recovering once we start the
   * recovery process. If the recovery succeeds, we delete the recovery task and set node to free.
   * If the recovery task fails, we set the node to free so the recovery task can be assigned to
   * another node again.
   *
   * <p>Currently, we expect each recovery task to create one chunk. We don't support multiple
   * chunks per recovery task for a few reasons. It keeps the recovery protocol very simple since we
   * don't have to deal with partial chunk upload failures. By creating only one chunk per recovery
   * task, the runtime and resource utilization of an individual recovery task is bounded and
   * predictable, so it's easy to plan capacity for it. We get implicit parallelism in execution by
   * adding more recovery nodes and there is no need for additional mechanisms for parallelizing
   * execution.
   *
   * <p>TODO: Re-queuing failed re-assignment task will lead to wasted resources if recovery always
   * fails. To break this cycle add a enqueue_count value to recovery task so we can stop recovering
   * it if the task fails a certain number of times.
   */
  private void handleRecoveryTaskAssignment(RecoveryNodeMetadata recoveryNodeMetadata) {
    try {
      setRecoveryNodeMetadataState(Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING);
      RecoveryTaskMetadata recoveryTaskMetadata =
          recoveryTaskMetadataStore.getNodeSync(recoveryNodeMetadata.recoveryTaskName);

      boolean success = handleRecoveryTask(recoveryTaskMetadata);
      if (success) {
        // delete the completed recovery task on success
        recoveryTaskMetadataStore.deleteSync(recoveryTaskMetadata.name);
        setRecoveryNodeMetadataState(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
        recoveryNodeAssignmentSuccess.increment();
      } else {
        setRecoveryNodeMetadataState(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
        recoveryNodeAssignmentFailed.increment();
      }
    } catch (Exception e) {
      setRecoveryNodeMetadataState(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
      LOG.error("Failed to complete recovery node task assignment", e);
      recoveryNodeAssignmentFailed.increment();
    }
  }

  /**
   * This method does the recovery work from a recovery task. A recovery task indicates the start
   * and end offset of a kafka partition to index. To do the recovery work, we create a recovery
   * chunk manager, create a kafka consumer for the recovery partition, indexes the data in
   * parallel, uploads the data to S3 and closes all the components correctly. We return true if the
   * operation succeeded.
   */
  @VisibleForTesting
  boolean handleRecoveryTask(RecoveryTaskMetadata recoveryTaskMetadata) {
    try {
      RecoveryChunkManager<LogMessage> chunkManager =
          RecoveryChunkManager.fromConfig(
              meterRegistry,
              searchMetadataStore,
              snapshotMetadataStore,
              kaldbConfig.getIndexerConfig(),
              blobFs,
              kaldbConfig.getS3Config());
      // Ingest data in parallel
      LogMessageTransformer messageTransformer =
          DATA_TRANSFORMER_MAP.get(kaldbConfig.getIndexerConfig().getDataTransformer());
      LogMessageWriterImpl logMessageWriterImpl =
          new LogMessageWriterImpl(chunkManager, messageTransformer);
      KaldbKafkaConsumer kafkaConsumer =
          KaldbKafkaConsumer.fromConfig(
              makeKafkaConfig(kaldbConfig.getKafkaConfig(), recoveryTaskMetadata.partitionId),
              logMessageWriterImpl,
              meterRegistry);
      kafkaConsumer.prepConsumerForConsumption(recoveryTaskMetadata.startOffset);
      kafkaConsumer.consumeMessagesBetweenOffsetsInParallel(
          KaldbKafkaConsumer.KAFKA_POLL_TIMEOUT_MS,
          recoveryTaskMetadata.startOffset,
          recoveryTaskMetadata.endOffset);
      // Wait for chunks to upload.
      boolean success = chunkManager.waitForRollOvers();
      // Close the recovery chunk manager and kafka consumer.
      kafkaConsumer.close();
      chunkManager.stopAsync();
      chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
      return success;
    } catch (Exception ex) {
      LOG.error("Encountered exception in recovery task: {}", recoveryTaskMetadata, ex);
      return false;
    }
  }

  // Replace the Kafka PartitionId from the kafkaConfig added.
  private KaldbConfigs.KafkaConfig makeKafkaConfig(
      KaldbConfigs.KafkaConfig kafkaConfig, String partitionId) throws TextFormat.ParseException {
    KaldbConfigs.KafkaConfig.Builder builder = KaldbConfigs.KafkaConfig.newBuilder();
    TextFormat.merge(kafkaConfig.toString(), builder);
    builder.setKafkaTopicPartition(partitionId);
    return builder.build();
  }

  private void setRecoveryNodeMetadataState(
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
