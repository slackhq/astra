package com.slack.astra.testlib;

import com.slack.astra.proto.config.AstraConfigs;
import java.util.List;

public class AstraConfigUtil {

  public static AstraConfigs.AstraConfig makeAstraConfig(
      String bootstrapServers,
      int indexerPort,
      String kafkaTopic,
      int kafkaPartition,
      String kafkaClientGroup,
      String s3Bucket,
      int queryPort,
      String metadataZkConnectionString,
      String metadataPathPrefix,
      AstraConfigs.NodeRole nodeRole,
      int maxOffsetDelay,
      int recoveryPort,
      int maxMessagesPerChunk,
      List<String> etcdEndpoints) {
    AstraConfigs.KafkaConfig kafkaConfig =
        AstraConfigs.KafkaConfig.newBuilder()
            .setKafkaTopic(kafkaTopic)
            .setKafkaTopicPartition(String.valueOf(kafkaPartition))
            .setKafkaBootStrapServers(bootstrapServers)
            .setKafkaClientGroup(kafkaClientGroup)
            .setEnableKafkaAutoCommit("true")
            .setKafkaAutoCommitInterval("5000")
            .setKafkaSessionTimeout("5000")
            .build();

    AstraConfigs.S3Config s3Config =
        AstraConfigs.S3Config.newBuilder()
            .setS3Bucket(s3Bucket)
            .setS3Region("us-east-1")
            .setS3AccessKey("")
            .setS3SecretKey("")
            .setS3TargetThroughputGbps(10D)
            .build();

    AstraConfigs.IndexerConfig indexerConfig =
        AstraConfigs.IndexerConfig.newBuilder()
            .setServerConfig(
                AstraConfigs.ServerConfig.newBuilder()
                    .setServerPort(indexerPort)
                    .setServerAddress("localhost")
                    .setRequestTimeoutMs(5000)
                    .build())
            .setMaxBytesPerChunk(10L * 1024 * 1024 * 1024)
            .setMaxMessagesPerChunk(maxMessagesPerChunk)
            .setMaxChunksOnDisk(3)
            .setStaleDurationSecs(7200)
            .setMaxOffsetDelayMessages(maxOffsetDelay)
            .setDefaultQueryTimeoutMs(2500)
            .setKafkaConfig(kafkaConfig)
            .build();

    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setEnabled(true)
            .setZkConnectString(metadataZkConnectionString)
            .setZkPathPrefix(metadataPathPrefix)
            .setZkSessionTimeoutMs(15000)
            .setZkConnectionTimeoutMs(15000)
            .setSleepBetweenRetriesMs(1000)
            .setZkCacheInitTimeoutMs(1000)
            .build();
    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .setEnabled(true)
            .addAllEndpoints(
                etcdEndpoints != null && !etcdEndpoints.isEmpty() ? etcdEndpoints : List.of(""))
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace(metadataPathPrefix) // Use same namespace as ZK pathPrefix for consistency
            .setEphemeralNodeTtlMs(3000)
            .setEphemeralNodeMaxRetries(3)
            .build();
    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .putStoreModes("DatasetMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("SnapshotMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("ReplicaMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("HpaMetricMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("SearchMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheSlotMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("CacheNodeAssignmentStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes(
                "FieldRedactionMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("PreprocessorMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("RecoveryNodeMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .putStoreModes("RecoveryTaskMetadataStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .setZookeeperConfig(zkConfig)
            .setEtcdConfig(etcdConfig)
            .build();

    AstraConfigs.QueryServiceConfig queryConfig =
        AstraConfigs.QueryServiceConfig.newBuilder()
            .setServerConfig(
                AstraConfigs.ServerConfig.newBuilder()
                    .setServerPort(queryPort)
                    .setServerAddress("localhost")
                    .setRequestTimeoutMs(5000)
                    .build())
            .setDefaultQueryTimeoutMs(3000)
            .build();

    AstraConfigs.RecoveryConfig recoveryConfig =
        AstraConfigs.RecoveryConfig.newBuilder()
            .setServerConfig(
                AstraConfigs.ServerConfig.newBuilder()
                    .setServerPort(recoveryPort)
                    .setServerAddress("localhost")
                    .setRequestTimeoutMs(5000)
                    .build())
            .setKafkaConfig(kafkaConfig)
            .build();

    AstraConfigs.RedactionUpdateServiceConfig redactionUpdateServiceConfig =
        AstraConfigs.RedactionUpdateServiceConfig.newBuilder()
            .setRedactionUpdatePeriodSecs(1)
            .setRedactionUpdateInitDelaySecs(1)
            .build();

    AstraConfigs.LuceneConfig luceneConfig =
        AstraConfigs.LuceneConfig.newBuilder()
            .setCommitDurationSecs(10)
            .setRefreshDurationSecs(10)
            .setEnableFullTextSearch(true)
            .build();

    return AstraConfigs.AstraConfig.newBuilder()
        .setS3Config(s3Config)
        .setIndexerConfig(indexerConfig)
        .setRecoveryConfig(recoveryConfig)
        .setQueryConfig(queryConfig)
        .setMetadataStoreConfig(metadataStoreConfig)
        .setRedactionUpdateServiceConfig(redactionUpdateServiceConfig)
        .setLuceneConfig(luceneConfig)
        .addNodeRoles(nodeRole)
        .build();
  }

  public static AstraConfigs.KafkaConfig makeKafkaConfig(
      String kafkaTopic, int topicPartition, String kafkaClient, String brokerList) {
    return AstraConfigs.KafkaConfig.newBuilder()
        .setKafkaTopic(kafkaTopic)
        .setKafkaTopicPartition(String.valueOf(topicPartition))
        .setKafkaBootStrapServers(brokerList)
        .setKafkaClientGroup(kafkaClient)
        .setEnableKafkaAutoCommit("true")
        .setKafkaAutoCommitInterval("5000")
        .setKafkaSessionTimeout("30000")
        .build();
  }

  public static int TEST_INDEXER_PORT = 10000;

  public static AstraConfigs.IndexerConfig makeIndexerConfig() {
    return makeIndexerConfig(TEST_INDEXER_PORT, 1000, 100);
  }

  public static AstraConfigs.IndexerConfig makeIndexerConfig(boolean enableFullTextSearch) {
    return makeIndexerConfig(TEST_INDEXER_PORT, 1000, 100, enableFullTextSearch);
  }

  public static AstraConfigs.IndexerConfig makeIndexerConfig(int maxOffsetDelay) {
    return makeIndexerConfig(TEST_INDEXER_PORT, maxOffsetDelay, 100);
  }

  public static AstraConfigs.IndexerConfig makeIndexerConfig(
      int indexerPort, int maxOffsetDelay, int maxMessagesPerChunk) {
    return makeIndexerConfig(indexerPort, maxOffsetDelay, maxMessagesPerChunk, true);
  }

  public static AstraConfigs.QueryServiceConfig makeQueryConfig() {
    return makeQueryConfig(5555, "localhost", 5000, 3000, false);
  }

  public static AstraConfigs.QueryServiceConfig makeQueryConfig(
      int queryPort,
      String serverAddress,
      int requestTimeoutMs,
      int defaultQueryTimeout,
      boolean cacheEnabled) {
    AstraConfigs.QueryServiceConfig queryConfig =
        AstraConfigs.QueryServiceConfig.newBuilder()
            .setServerConfig(
                AstraConfigs.ServerConfig.newBuilder()
                    .setServerPort(queryPort)
                    .setServerAddress(serverAddress)
                    .setRequestTimeoutMs(requestTimeoutMs)
                    .build())
            .setDefaultQueryTimeoutMs(defaultQueryTimeout)
            .setQueryRequestCacheEnabled(cacheEnabled)
            .build();
    return queryConfig;
  }

  public static AstraConfigs.IndexerConfig makeIndexerConfig(
      int indexerPort, int maxOffsetDelay, int maxMessagesPerChunk, boolean enableFullTextSearch) {
    return AstraConfigs.IndexerConfig.newBuilder()
        .setServerConfig(
            AstraConfigs.ServerConfig.newBuilder()
                .setServerPort(indexerPort)
                .setServerAddress("localhost")
                .build())
        .setMaxBytesPerChunk(10L * 1024 * 1024 * 1024)
        .setMaxMessagesPerChunk(maxMessagesPerChunk)
        .setMaxChunksOnDisk(3)
        .setStaleDurationSecs(7200)
        .setMaxOffsetDelayMessages(maxOffsetDelay)
        .build();
  }

  public static AstraConfigs.IndexerConfig makeIndexerConfig(
      int indexerPort,
      int maxOffsetDelay,
      int maxMessagesPerChunk,
      int maxChunksOnDisk,
      long staleDuration) {
    return AstraConfigs.IndexerConfig.newBuilder()
        .setServerConfig(
            AstraConfigs.ServerConfig.newBuilder()
                .setServerPort(indexerPort)
                .setServerAddress("localhost")
                .build())
        .setMaxBytesPerChunk(10L * 1024 * 1024 * 1024)
        .setMaxMessagesPerChunk(maxMessagesPerChunk)
        .setMaxChunksOnDisk(maxChunksOnDisk)
        .setStaleDurationSecs(staleDuration)
        .setMaxOffsetDelayMessages(maxOffsetDelay)
        .build();
  }
}
