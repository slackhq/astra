package com.slack.astra.metadata.dataset;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.DynamoDbMetadataStore;
import com.slack.astra.metadata.core.EtcdCreateMode;
import com.slack.astra.metadata.core.EtcdMetadataStore;
import com.slack.astra.metadata.core.ZookeeperMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.Client;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

public class DatasetMetadataStore extends AstraMetadataStore<DatasetMetadata> {
  // TODO: The path should be dataset, but leaving it as /service for backwards compatibility.
  public static final String DATASET_METADATA_STORE_PATH = "/service";

  /** ZK/etcd-only constructor (no DynamoDB backend), retained for existing call sites. */
  public DatasetMetadataStore(
      AsyncCuratorFramework curator,
      Client etcdClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache) {
    this(curator, etcdClient, null, null, metadataStoreConfig, meterRegistry, shouldCache);
  }

  public DatasetMetadataStore(
      AsyncCuratorFramework curator,
      Client etcdClient,
      DynamoDbAsyncClient dynamoClient,
      DynamoDbStreamsAsyncClient dynamoStreamsClient,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache) {
    super(
        curator != null
            ? new ZookeeperMetadataStore<>(
                curator,
                metadataStoreConfig.getZookeeperConfig(),
                CreateMode.PERSISTENT,
                shouldCache,
                new DatasetMetadataSerializer().toModelSerializer(),
                DATASET_METADATA_STORE_PATH,
                meterRegistry)
            : null,
        etcdClient != null
            ? new EtcdMetadataStore<>(
                DATASET_METADATA_STORE_PATH,
                metadataStoreConfig.getEtcdConfig(),
                shouldCache,
                meterRegistry,
                new DatasetMetadataSerializer(),
                EtcdCreateMode.PERSISTENT,
                etcdClient)
            : null,
        dynamoClient != null
            ? new DynamoDbMetadataStore<>(
                dynamoClient,
                dynamoStreamsClient,
                metadataStoreConfig.getDynamodbConfig(),
                DATASET_METADATA_STORE_PATH,
                shouldCache,
                EtcdCreateMode.PERSISTENT,
                new DatasetMetadataSerializer(),
                meterRegistry)
            : null,
        metadataStoreConfig.getStoreModesOrDefault(
            "DatasetMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES),
        meterRegistry);
  }
}
