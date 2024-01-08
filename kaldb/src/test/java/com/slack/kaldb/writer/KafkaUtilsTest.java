package com.slack.kaldb.writer;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.writer.kafka.KaldbKafkaConsumer;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

class KafkaUtilsTest {
  public static final String TEST_KAFKA_CLIENT_GROUP = "test_kaldb_consumer";

  @Test
  public void testOverridingProperties() {
    KaldbConfigs.KafkaConfig kafkaConfig =
        KaldbConfigs.KafkaConfig.newBuilder()
            .setKafkaTopic(TestKafkaServer.TEST_KAFKA_TOPIC)
            .setKafkaTopicPartition("0")
            .setKafkaBootStrapServers("bootstrap_server")
            .setKafkaClientGroup(TEST_KAFKA_CLIENT_GROUP)
            .setEnableKafkaAutoCommit("true")
            .setKafkaAutoCommitInterval("5000")
            .setKafkaSessionTimeout("5000")
            .build();

    Properties properties = KaldbKafkaConsumer.makeKafkaConsumerProps(kafkaConfig);
    assertThat(properties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG))
        .isEqualTo("org.apache.kafka.common.serialization.StringDeserializer");

    kafkaConfig =
        KaldbConfigs.KafkaConfig.newBuilder()
            .setKafkaTopic(TestKafkaServer.TEST_KAFKA_TOPIC)
            .setKafkaTopicPartition("0")
            .setKafkaBootStrapServers("bootstrap_server")
            .setKafkaClientGroup(TEST_KAFKA_CLIENT_GROUP)
            .setEnableKafkaAutoCommit("true")
            .setKafkaAutoCommitInterval("5000")
            .setKafkaSessionTimeout("5000")
            .putAdditionalProps(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "test_serializer")
            .build();

    properties = KaldbKafkaConsumer.makeKafkaConsumerProps(kafkaConfig);
    assertThat(properties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG))
        .isEqualTo("test_serializer");
  }
}
