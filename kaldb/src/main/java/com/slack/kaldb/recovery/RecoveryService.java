package com.slack.kaldb.recovery;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.server.MetadataStoreService;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryService.class);

  private final SearchContext searchContext;
  private final MetadataStoreService metadataStoreService;
  private final MeterRegistry meterRegistry;

  private RecoveryNodeMetadataStore recoveryNodeMetadataStore;
  private RecoveryNodeMetadataStore recoveryNodeListenerMetadataStore;
  private RecoveryTaskMetadataStore recoveryTaskMetadataStore;

  public RecoveryService(
      KaldbConfigs.RecoveryConfig recoveryConfig,
      MetadataStoreService metadataStoreService,
      MeterRegistry meterRegistry) {
    this.metadataStoreService = metadataStoreService;
    this.meterRegistry = meterRegistry;
    this.searchContext = SearchContext.fromConfig(recoveryConfig.getServerConfig());
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting recovery service");
    metadataStoreService.awaitRunning(DEFAULT_START_STOP_DURATION);

    recoveryNodeMetadataStore =
        new RecoveryNodeMetadataStore(metadataStoreService.getMetadataStore(), false);
    recoveryTaskMetadataStore =
        new RecoveryTaskMetadataStore(metadataStoreService.getMetadataStore(), false);

    recoveryNodeMetadataStore.createSync(
        new RecoveryNodeMetadata(
            searchContext.hostname, Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE));

    recoveryNodeListenerMetadataStore =
        new RecoveryNodeMetadataStore(
            metadataStoreService.getMetadataStore(), searchContext.hostname, true);
    recoveryNodeListenerMetadataStore.addListener(recoveryNodeListener());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closed recovery service");
  }

  private KaldbMetadataStoreChangeListener recoveryNodeListener() {
    return () -> {
      // on assignment, do indexing between the assigned offsets
      // once complete (created the snapshot successfully), delete the task and mark ourselves as
      // free
    };
  }
}
