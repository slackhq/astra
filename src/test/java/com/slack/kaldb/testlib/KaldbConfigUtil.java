package com.slack.kaldb.testlib;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.proto.config.KaldbConfigs;

public class KaldbConfigUtil {
  // Initialize kaldb config with empty for tests.
  public static void initEmptyConfig() throws InvalidProtocolBufferException {
    KaldbConfig.initFromJsonStr("{}");
  }

  public static KaldbConfigs.KaldbConfig makeKaldbConfig(
      String bootstrapServers,
      int indexerPort,
      String kafkaTopic,
      int kafkaPartition,
      String kafkaClientGroup,
      String s3Bucket,
      int readPort) {
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
        KaldbConfigs.S3Config.newBuilder().setS3Bucket(s3Bucket).setS3Region("us-east-1").build();

    KaldbConfigs.IndexerConfig indexerConfig =
        KaldbConfigs.IndexerConfig.newBuilder()
            .setServerPort(indexerPort)
            .setMaxBytesPerChunk(10L * 1024 * 1024 * 1024)
            .setMaxMessagesPerChunk(100)
            .setCommitDurationSecs(10)
            .setRefreshDurationSecs(10)
            .setStaleDurationSecs(7200)
            .build();

    KaldbConfigs.ReadConfig readConfig =
        KaldbConfigs.ReadConfig.newBuilder().setServerPort(readPort).build();

    return KaldbConfigs.KaldbConfig.newBuilder()
        .setKafkaConfig(kafkaConfig)
        .setS3Config(s3Config)
        .setIndexerConfig(indexerConfig)
        .setReadConfig(readConfig)
        .build();
  }
}
