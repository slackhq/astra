package com.slack.kaldb.testlib;

import com.slack.kaldb.proto.config.KaldbConfigs;

public class KaldbConfigUtil {

  public static KaldbConfigs.KaldbConfig makeKaldbConfig(
      String bootstrapServers,
      int indexerPort,
      String kafkaTopic,
      int kafkaPartition,
      String kafkaClientGroup,
      String s3Bucket,
      int queryPort,
      String metadataZkConnectionString,
      String metadataZkPathPrefix,
      KaldbConfigs.NodeRole nodeRole,
      int maxOffsetDelay,
      String dataTransformerConfig,
      int recoveryPort,
      int maxMessagesPerChunk) {
    KaldbConfigs.KafkaConfig kafkaConfig =
        KaldbConfigs.KafkaConfig.newBuilder()
            .setKafkaTopic(kafkaTopic)
            .setKafkaTopicPartition(String.valueOf(kafkaPartition))
            .setKafkaBootStrapServers(bootstrapServers)
            .setKafkaClientGroup(kafkaClientGroup)
            .setEnableKafkaAutoCommit("true")
            .setKafkaAutoCommitInterval("5000")
            .setKafkaSessionTimeout("5000")
            .build();

    KaldbConfigs.S3Config s3Config =
        KaldbConfigs.S3Config.newBuilder()
            .setS3Bucket(s3Bucket)
            .setS3Region("us-east-1")
            .setS3AccessKey("")
            .setS3SecretKey("")
            .setS3TargetThroughputGbps(10D)
            .build();

    KaldbConfigs.IndexerConfig indexerConfig =
        KaldbConfigs.IndexerConfig.newBuilder()
            .setServerConfig(
                KaldbConfigs.ServerConfig.newBuilder()
                    .setServerPort(indexerPort)
                    .setServerAddress("localhost")
                    .setRequestTimeoutMs(5000)
                    .build())
            .setMaxBytesPerChunk(10L * 1024 * 1024 * 1024)
            .setMaxMessagesPerChunk(maxMessagesPerChunk)
            .setLuceneConfig(
                KaldbConfigs.LuceneConfig.newBuilder()
                    .setCommitDurationSecs(10)
                    .setRefreshDurationSecs(10)
                    .setEnableFullTextSearch(true)
                    .build())
            .setMaxChunksOnDisk(3)
            .setStaleDurationSecs(7200)
            .setDataTransformer(dataTransformerConfig)
            .setMaxOffsetDelayMessages(maxOffsetDelay)
            .setDefaultQueryTimeoutMs(2500)
            .setKafkaConfig(kafkaConfig)
            .build();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(metadataZkConnectionString)
            .setZkPathPrefix(metadataZkPathPrefix)
            .setZkSessionTimeoutMs(15000)
            .setZkConnectionTimeoutMs(15000)
            .setSleepBetweenRetriesMs(1000)
            .build();
    KaldbConfigs.MetadataStoreConfig metadataStoreConfig =
        KaldbConfigs.MetadataStoreConfig.newBuilder().setZookeeperConfig(zkConfig).build();

    KaldbConfigs.QueryServiceConfig queryConfig =
        KaldbConfigs.QueryServiceConfig.newBuilder()
            .setServerConfig(
                KaldbConfigs.ServerConfig.newBuilder()
                    .setServerPort(queryPort)
                    .setServerAddress("localhost")
                    .setRequestTimeoutMs(5000)
                    .build())
            .setDefaultQueryTimeoutMs(3000)
            .build();

    KaldbConfigs.RecoveryConfig recoveryConfig =
        KaldbConfigs.RecoveryConfig.newBuilder()
            .setServerConfig(
                KaldbConfigs.ServerConfig.newBuilder()
                    .setServerPort(recoveryPort)
                    .setServerAddress("localhost")
                    .setRequestTimeoutMs(5000)
                    .build())
            .setKafkaConfig(kafkaConfig)
            .build();

    return KaldbConfigs.KaldbConfig.newBuilder()
        .setS3Config(s3Config)
        .setIndexerConfig(indexerConfig)
        .setRecoveryConfig(recoveryConfig)
        .setQueryConfig(queryConfig)
        .setMetadataStoreConfig(metadataStoreConfig)
        .addNodeRoles(nodeRole)
        .build();
  }

  public static KaldbConfigs.KafkaConfig makeKafkaConfig(
      String kafkaTopic, int topicPartition, String kafkaClient, String brokerList) {
    return KaldbConfigs.KafkaConfig.newBuilder()
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

  public static KaldbConfigs.IndexerConfig makeIndexerConfig() {
    return makeIndexerConfig(TEST_INDEXER_PORT, 1000, "log_message", 100);
  }

  public static KaldbConfigs.IndexerConfig makeIndexerConfig(
      int maxOffsetDelay, String dataTransformer) {
    return makeIndexerConfig(TEST_INDEXER_PORT, maxOffsetDelay, dataTransformer, 100);
  }

  public static KaldbConfigs.IndexerConfig makeIndexerConfig(
      int indexerPort, int maxOffsetDelay, String dataTransformer, int maxMessagesPerChunk) {
    return KaldbConfigs.IndexerConfig.newBuilder()
        .setServerConfig(
            KaldbConfigs.ServerConfig.newBuilder()
                .setServerPort(indexerPort)
                .setServerAddress("localhost")
                .build())
        .setMaxBytesPerChunk(10L * 1024 * 1024 * 1024)
        .setMaxMessagesPerChunk(maxMessagesPerChunk)
        .setLuceneConfig(
            KaldbConfigs.LuceneConfig.newBuilder()
                .setCommitDurationSecs(10)
                .setRefreshDurationSecs(10)
                .setEnableFullTextSearch(true)
                .build())
        .setMaxChunksOnDisk(3)
        .setStaleDurationSecs(7200)
        .setMaxOffsetDelayMessages(maxOffsetDelay)
        .setDataTransformer(dataTransformer)
        .build();
  }
}
