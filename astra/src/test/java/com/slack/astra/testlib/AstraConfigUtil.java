package com.slack.astra.testlib;

import com.slack.astra.proto.config.AstraConfigs;

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
      String metadataZkPathPrefix,
      AstraConfigs.NodeRole nodeRole,
      int maxOffsetDelay,
      int recoveryPort,
      int maxMessagesPerChunk) {
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
            .setLuceneConfig(
                AstraConfigs.LuceneConfig.newBuilder()
                    .setCommitDurationSecs(10)
                    .setRefreshDurationSecs(10)
                    .setEnableFullTextSearch(true)
                    .build())
            .setMaxChunksOnDisk(3)
            .setStaleDurationSecs(7200)
            .setMaxOffsetDelayMessages(maxOffsetDelay)
            .setDefaultQueryTimeoutMs(2500)
            .setKafkaConfig(kafkaConfig)
            .build();

    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(metadataZkConnectionString)
            .setZkPathPrefix(metadataZkPathPrefix)
            .setZkSessionTimeoutMs(15000)
            .setZkConnectionTimeoutMs(15000)
            .setSleepBetweenRetriesMs(1000)
            .setZkCacheInitTimeoutMs(1000)
            .build();
    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder().setZookeeperConfig(zkConfig).build();

    AstraConfigs.QueryServiceConfig queryConfig =
        AstraConfigs.QueryServiceConfig.newBuilder()
            .setServerConfig(
                AstraConfigs.ServerConfig.newBuilder()
                    .setServerPort(queryPort)
                    .setServerAddress("localhost")
                    .setRequestTimeoutMs(5000)
                    .build())
            .setDefaultQueryTimeoutMs(3000)
            .setZipkinDefaultMaxSpans(20000)
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

    return AstraConfigs.AstraConfig.newBuilder()
        .setS3Config(s3Config)
        .setIndexerConfig(indexerConfig)
        .setRecoveryConfig(recoveryConfig)
        .setQueryConfig(queryConfig)
        .setMetadataStoreConfig(metadataStoreConfig)
        .setRedactionUpdateServiceConfig(redactionUpdateServiceConfig)
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

  public static AstraConfigs.IndexerConfig makeIndexerConfig(int maxOffsetDelay) {
    return makeIndexerConfig(TEST_INDEXER_PORT, maxOffsetDelay, 100);
  }

  public static AstraConfigs.IndexerConfig makeIndexerConfig(
      int indexerPort, int maxOffsetDelay, int maxMessagesPerChunk) {
    return AstraConfigs.IndexerConfig.newBuilder()
        .setServerConfig(
            AstraConfigs.ServerConfig.newBuilder()
                .setServerPort(indexerPort)
                .setServerAddress("localhost")
                .build())
        .setMaxBytesPerChunk(10L * 1024 * 1024 * 1024)
        .setMaxMessagesPerChunk(maxMessagesPerChunk)
        .setLuceneConfig(
            AstraConfigs.LuceneConfig.newBuilder()
                .setCommitDurationSecs(10)
                .setRefreshDurationSecs(10)
                .setEnableFullTextSearch(true)
                .build())
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
        .setLuceneConfig(
            AstraConfigs.LuceneConfig.newBuilder()
                .setCommitDurationSecs(10)
                .setRefreshDurationSecs(10)
                .setEnableFullTextSearch(true)
                .build())
        .setMaxChunksOnDisk(maxChunksOnDisk)
        .setStaleDurationSecs(staleDuration)
        .setMaxOffsetDelayMessages(maxOffsetDelay)
        .build();
  }
}
