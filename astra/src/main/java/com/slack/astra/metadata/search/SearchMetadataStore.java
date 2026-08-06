package com.slack.astra.metadata.search;

import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.core.AstraPartitioningMetadataStore;
import com.slack.astra.metadata.core.DynamoDbPartitioningMetadataStore;
import com.slack.astra.metadata.core.EtcdCreateMode;
import com.slack.astra.metadata.core.EtcdPartitioningMetadataStore;
import com.slack.astra.metadata.core.ZookeeperPartitioningMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.Client;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

public class SearchMetadataStore extends AstraPartitioningMetadataStore<SearchMetadata> {
  private static final Logger LOG = LoggerFactory.getLogger(SearchMetadataStore.class);

  public static final String SEARCH_PARTITIONED_METADATA_STORE_PATH = "/partitioned_search";
  final SearchMetadataStoreLegacy legacyStore;

  /** ZK/etcd-only constructor (no DynamoDB backend), retained for existing call sites. */
  public SearchMetadataStore(
      AsyncCuratorFramework curatorFramework,
      Client etcdClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache) {
    this(curatorFramework, etcdClient, null, null, metadataStoreConfig, meterRegistry, shouldCache);
  }

  public SearchMetadataStore(
      AsyncCuratorFramework curatorFramework,
      Client etcdClient,
      DynamoDbAsyncClient dynamoClient,
      DynamoDbStreamsAsyncClient dynamoStreamsClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache) {
    super(
        curatorFramework != null
            ? new ZookeeperPartitioningMetadataStore<>(
                curatorFramework,
                metadataStoreConfig.getZookeeperConfig(),
                meterRegistry,
                CreateMode.EPHEMERAL,
                new SearchMetadataSerializer().toModelSerializer(),
                SEARCH_PARTITIONED_METADATA_STORE_PATH)
            : null,
        etcdClient != null
            ? new EtcdPartitioningMetadataStore<>(
                etcdClient,
                metadataStoreConfig.getEtcdConfig(),
                meterRegistry,
                EtcdCreateMode.EPHEMERAL,
                new SearchMetadataSerializer(),
                SEARCH_PARTITIONED_METADATA_STORE_PATH)
            : null,
        dynamoClient != null
            ? new DynamoDbPartitioningMetadataStore<>(
                dynamoClient,
                dynamoStreamsClient,
                metadataStoreConfig.getDynamodbConfig(),
                SEARCH_PARTITIONED_METADATA_STORE_PATH,
                shouldCache,
                EtcdCreateMode.EPHEMERAL,
                new SearchMetadataSerializer(),
                meterRegistry)
            : null,
        metadataStoreConfig.getStoreModesOrDefault(
            "SearchMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES),
        meterRegistry);

    // The legacy /search store is a ZK/etcd migration artifact. A dynamo-only store (no ZK/etcd
    // clients) has nothing to fall back to, so we skip it; the fallbacks below tolerate null.
    this.legacyStore =
        (curatorFramework != null || etcdClient != null)
            ? new SearchMetadataStoreLegacy(
                curatorFramework, etcdClient, metadataStoreConfig, meterRegistry, shouldCache)
            : null;
  }

  @Override
  public CompletionStage<String> updateAsync(SearchMetadata metadataNode) {
    throw new UnsupportedOperationException("Updates are not permitted for search metadata");
  }

  @Override
  public void updateSync(SearchMetadata metadataNode) {
    throw new UnsupportedOperationException("Updates are not permitted for search metadata");
  }

  @Override
  public SearchMetadata getSync(String partition, String path) {
    try {
      return super.getSync(partition, path);
    } catch (Exception e) {
      if (legacyStore == null) {
        throw e;
      }
      LOG.warn("Failed to get search metadata, falling back to legacy store", e);
      return legacyStore.getSync(path);
    }
  }

  @Override
  public void deleteSync(SearchMetadata metadataNode) {
    try {
      super.deleteSync(metadataNode);
    } catch (Exception e) {
      if (legacyStore == null) {
        throw e;
      }
      LOG.warn(
          "Failed to delete search metadata: {}, falling back to legacy store", metadataNode, e);
      this.legacyStore.deleteSync(metadataNode);
    }
  }

  @Override
  public List<SearchMetadata> listSync() {
    List<SearchMetadata> partitionedNodes = null;
    List<SearchMetadata> legacyNodes = null;

    if (legacyStore != null) {
      try {
        legacyNodes = legacyStore.listSync();
      } catch (Exception e) {
        LOG.warn("Failed to get list search metadata, falling back to legacy store", e);
        legacyNodes = Collections.emptyList();
      }
    } else {
      legacyNodes = Collections.emptyList();
    }
    try {
      partitionedNodes = super.listSync();
    } catch (Exception e) {
      partitionedNodes = Collections.emptyList();
    }

    partitionedNodes.addAll(legacyNodes);
    return partitionedNodes;
  }

  /** Removes a listener from both stores. */
  @Override
  public void removeListener(AstraMetadataStoreChangeListener<SearchMetadata> listener) {
    if (legacyStore != null) {
      legacyStore.removeListener(listener);
    }
    super.removeListener(listener);
  }

  @Override
  public void close() {
    if (legacyStore != null) {
      try {
        legacyStore.close();
      } catch (Exception e) {
        LOG.error("Error closing legacy search metadata store", e);
      }
    }

    try {
      super.close();
    } catch (Exception e) {
      LOG.error("Error closing partitioned search metadata store", e);
    }
  }
}
