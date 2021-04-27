package com.slack.kaldb.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.proto.config.KaldbConfigs;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KaldbConfigTest {

  @Before
  public void setUp() {
    KaldbConfig.reset();
  }

  @After
  public void tearDown() {
    KaldbConfig.reset();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInitWithMissingConfigFile() throws IOException {
    KaldbConfig.initFromFile(Path.of("missing_config_file.json"));
  }

  @Test(expected = InvalidProtocolBufferException.class)
  public void testEmptyCfgFile() throws InvalidProtocolBufferException {
    KaldbConfig.initFromJsonStr("");
  }

  @Test
  public void testIntToStrTypeConversionForWrongJsonType() throws InvalidProtocolBufferException {
    final String missingRequiredField =
        "{\n"
            + "  \"kafkaConfig\" : {\n"
            + "    \"kafkaTopicPartition\": \"1\",\n"
            + "    \"kafkaSessionTimeout\": 30000\n"
            + "  }\n"
            + "}\n";

    final KaldbConfigs.KaldbConfig kalDbConfig = KaldbConfig.fromJsonConfig(missingRequiredField);

    final KaldbConfigs.KafkaConfig kafkaCfg = kalDbConfig.getKafkaConfig();
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEqualTo("1");
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEqualTo("30000");
  }

  @Test
  public void testStrToIntTypeConversionForWrongJsonType() throws InvalidProtocolBufferException {
    final String missingRequiredField =
        "{\n"
            + "  \"indexerConfig\" : {\n"
            + "    \"maxMessagesPerChunk\": 1,\n"
            + "    \"maxBytesPerChunk\": \"100\"\n"
            + "  }\n"
            + "}\n";

    final KaldbConfigs.KaldbConfig kalDbConfig = KaldbConfig.fromJsonConfig(missingRequiredField);

    final KaldbConfigs.IndexerConfig indexerCfg = kalDbConfig.getIndexerConfig();
    assertThat(indexerCfg.getMaxMessagesPerChunk()).isEqualTo(1);
    assertThat(indexerCfg.getMaxBytesPerChunk()).isEqualTo(100);
  }

  @Test
  public void testCfgFileWithoutRequiredField() throws IOException {
    final String missingRequiredField =
        "{\n"
            + "  \"kafkaConfig\" : {\n"
            + "    \"kafkaTopicPartition\": \"1\",\n"
            + "    \"kafkaBootStrapServers\": \"kafka.us-east-1.consul:9092\",\n"
            + "    \"kafkaClientGroup\":  \"kaldb-test\",\n"
            + "    \"enableKafkaAutoCommit\":  \"true\",\n"
            + "    \"kafkaAutoCommitInterval\":  \"5000\",\n"
            + "    \"kafkaSessionTimeout\": \"30000\"\n"
            + "  }\n"
            + "}\n";

    final KaldbConfigs.KaldbConfig kalDbConfig = KaldbConfig.fromJsonConfig(missingRequiredField);

    final KaldbConfigs.KafkaConfig kafkaCfg = kalDbConfig.getKafkaConfig();
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEqualTo("1");
    assertThat(kafkaCfg.getKafkaBootStrapServers()).isEqualTo("kafka.us-east-1.consul:9092");
    assertThat(kafkaCfg.getKafkaClientGroup()).isEqualTo("kaldb-test");
    assertThat(kafkaCfg.getEnableKafkaAutoCommit()).isEqualTo("true");
    assertThat(kafkaCfg.getKafkaAutoCommitInterval()).isEqualTo("5000");
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEqualTo("30000");
    assertThat(kafkaCfg.getKafkaTopic()).isEmpty();

    final KaldbConfigs.S3Config s3Config = kalDbConfig.getS3Config();
    assertThat(s3Config.getS3AccessKey()).isEmpty();
    assertThat(s3Config.getS3SecretKey()).isEmpty();
    assertThat(s3Config.getS3Region()).isEmpty();
    assertThat(s3Config.getS3EndPoint()).isEmpty();
  }

  @Test
  public void testParseKaldbConfigFile() throws IOException {
    final File cfgFile =
        new File(getClass().getClassLoader().getResource("test_config.json").getFile());
    KaldbConfig.initFromFile(cfgFile.toPath());
    final KaldbConfigs.KaldbConfig config = KaldbConfig.get();

    assertThat(config).isNotNull();
    final KaldbConfigs.KafkaConfig kafkaCfg = config.getKafkaConfig();
    assertThat(kafkaCfg.getKafkaTopic()).isEqualTo("testTopic");
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEqualTo("1");
    assertThat(kafkaCfg.getKafkaBootStrapServers()).isEqualTo("kafka.us-east-1.consul:9092");
    assertThat(kafkaCfg.getKafkaClientGroup()).isEqualTo("kaldb-test");
    assertThat(kafkaCfg.getEnableKafkaAutoCommit()).isEqualTo("true");
    assertThat(kafkaCfg.getKafkaAutoCommitInterval()).isEqualTo("5000");
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEqualTo("30000");

    final KaldbConfigs.S3Config s3Config = config.getS3Config();
    assertThat(s3Config.getS3AccessKey()).isEqualTo("access");
    assertThat(s3Config.getS3SecretKey()).isEqualTo("secret");
    assertThat(s3Config.getS3Region()).isEqualTo("us-east-1");
    assertThat(s3Config.getS3EndPoint()).isEqualTo("https://s3.us-east-1.amazonaws.com/");
    assertThat(s3Config.getS3Bucket()).isEqualTo("test-s3-bucket");

    final KaldbConfigs.IndexerConfig indexerConfig = config.getIndexerConfig();
    assertThat(indexerConfig.getMaxMessagesPerChunk()).isEqualTo(1000);
    assertThat(indexerConfig.getMaxBytesPerChunk()).isEqualTo(100000);
    assertThat(indexerConfig.getCommitDurationSecs()).isEqualTo(10);
    assertThat(indexerConfig.getRefreshDurationSecs()).isEqualTo(11);
    assertThat(indexerConfig.getStaleDurationSecs()).isEqualTo(7200);
    assertThat(indexerConfig.getDataTransformer()).isEqualTo("api_log");
    assertThat(indexerConfig.getDataDirectory()).isEqualTo("/tmp");
  }

  @Test
  public void testEmptyJsonStringInit() throws InvalidProtocolBufferException {
    KaldbConfigs.KaldbConfig config = KaldbConfig.fromJsonConfig("{}");

    final KaldbConfigs.KafkaConfig kafkaCfg = config.getKafkaConfig();
    assertThat(kafkaCfg.getKafkaTopicPartition()).isEmpty();
    assertThat(kafkaCfg.getKafkaBootStrapServers()).isEmpty();
    assertThat(kafkaCfg.getKafkaClientGroup()).isEmpty();
    assertThat(kafkaCfg.getEnableKafkaAutoCommit()).isEmpty();
    assertThat(kafkaCfg.getKafkaAutoCommitInterval()).isEmpty();
    assertThat(kafkaCfg.getKafkaSessionTimeout()).isEmpty();
    assertThat(kafkaCfg.getKafkaTopic()).isEmpty();

    final KaldbConfigs.S3Config s3Config = config.getS3Config();
    assertThat(s3Config.getS3AccessKey()).isEmpty();
    assertThat(s3Config.getS3SecretKey()).isEmpty();
    assertThat(s3Config.getS3Region()).isEmpty();
    assertThat(s3Config.getS3EndPoint()).isEmpty();

    final KaldbConfigs.IndexerConfig indexerConfig = config.getIndexerConfig();
    assertThat(indexerConfig.getMaxMessagesPerChunk()).isEqualTo(0);
    assertThat(indexerConfig.getMaxBytesPerChunk()).isEqualTo(0);
    assertThat(indexerConfig.getCommitDurationSecs()).isEqualTo(0);
    assertThat(indexerConfig.getRefreshDurationSecs()).isEqualTo(0);
    assertThat(indexerConfig.getStaleDurationSecs()).isEqualTo(0);
    assertThat(indexerConfig.getDataDirectory()).isEqualTo("");
    assertThat(indexerConfig.getDataTransformer()).isEqualTo("");
  }
}
