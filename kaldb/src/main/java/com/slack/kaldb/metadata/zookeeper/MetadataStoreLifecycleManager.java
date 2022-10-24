package com.slack.kaldb.metadata.zookeeper;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.metadata.core.CacheableMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each instance of MetadataStoreLifecycleManager holds all the metadata stores required for each
 * node role MetadataStoreLifecycleManager is a managed guava service. The caller needs to add the
 * created instance to a service manager which would then take care of the object lifecycle
 */
public class MetadataStoreLifecycleManager extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataStoreLifecycleManager.class);

  private final KaldbConfigs.NodeRole nodeRole;
  private final List<CacheableMetadataStore<?>> metadataStores;

  public MetadataStoreLifecycleManager(
      KaldbConfigs.NodeRole role, List<CacheableMetadataStore<?>> metadataStores) {
    this.metadataStores = metadataStores;
    this.nodeRole = role;
  }

  @Override
  protected void startUp() throws Exception {}

  @Override
  protected void shutDown() throws Exception {
    LOG.info("shutting down MetadataStoreLifecycleManager for role=" + nodeRole.toString());
    metadataStores.forEach(CacheableMetadataStore::close);
  }
}
