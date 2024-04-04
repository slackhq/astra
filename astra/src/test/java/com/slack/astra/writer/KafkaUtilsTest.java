package com.slack.astra.writer;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.TestKafkaServer;
import com.slack.astra.writer.kafka.AstraKafkaConsumer;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

class KafkaUtilsTest {
  public static final String TEST_KAFKA_CLIENT_GROUP = "test_astra_consumer";

  @Test
  public void testOverridingProperties() {
    AstraConfigs.KafkaConfig kafkaConfig =
        AstraConfigs.KafkaConfig.newBuilder()
            .setKafkaTopic(TestKafkaServer.TEST_KAFKA_TOPIC)
            .setKafkaTopicPartition("0")
            .setKafkaBootStrapServers("bootstrap_server")
            .setKafkaClientGroup(TEST_KAFKA_CLIENT_GROUP)
            .setEnableKafkaAutoCommit("true")
            .setKafkaAutoCommitInterval("5000")
            .setKafkaSessionTimeout("5000")
            .build();

    Properties properties = AstraKafkaConsumer.makeKafkaConsumerProps(kafkaConfig);
    assertThat(properties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG))
        .isEqualTo("org.apache.kafka.common.serialization.StringDeserializer");

    kafkaConfig =
        AstraConfigs.KafkaConfig.newBuilder()
            .setKafkaTopic(TestKafkaServer.TEST_KAFKA_TOPIC)
            .setKafkaTopicPartition("0")
            .setKafkaBootStrapServers("bootstrap_server")
            .setKafkaClientGroup(TEST_KAFKA_CLIENT_GROUP)
            .setEnableKafkaAutoCommit("true")
            .setKafkaAutoCommitInterval("5000")
            .setKafkaSessionTimeout("5000")
            .putAdditionalProps(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "test_serializer")
            .build();

    properties = AstraKafkaConsumer.makeKafkaConsumerProps(kafkaConfig);
    assertThat(properties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG))
        .isEqualTo("test_serializer");
  }
}
