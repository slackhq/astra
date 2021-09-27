package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.config.KaldbConfig.REPLICA_STORE_ZK_PATH;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.server.MetadataStoreService;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the lifecycle for the Replica metadata type. At least one Replica is expected to be
 * created once a snapshot has been published by an indexer node.
 *
 * <p>Each Replica is then expected to be assigned to a Cache node, depending on availability, by
 * the cache assignment service in the cluster manager
 */
public class ReplicaCreatorService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaCreatorService.class);

  private final MetadataStoreService metadataStoreService;
  private final MeterRegistry meterRegistry;

  private ReplicaMetadataStore replicaMetadataStore;

  public ReplicaCreatorService(
      MetadataStoreService metadataStoreService, MeterRegistry meterRegistry) {
    this.metadataStoreService = metadataStoreService;
    this.meterRegistry = meterRegistry;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting replica creator service");
    metadataStoreService.awaitRunning(DEFAULT_START_STOP_DURATION);

    replicaMetadataStore =
        new ReplicaMetadataStore(
            metadataStoreService.getMetadataStore(), REPLICA_STORE_ZK_PATH, true);
    replicaMetadataStore.addListener(replicaNodeListener());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing replica create service");

    replicaMetadataStore.close();

    LOG.info("Closed replica create service");
  }

  private KaldbMetadataStoreChangeListener replicaNodeListener() {
    return () -> {
      List<ReplicaMetadata> replicaMetadataList = replicaMetadataStore.getCached();
      LOG.debug(
          "Change on replica metadata, new replica metadata count {}", replicaMetadataList.size());
    };
  }
}
