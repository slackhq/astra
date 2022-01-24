package com.slack.kaldb.writer.kafka;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbKafkaWriterTest {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbKafkaWriterTest.class);

  final String kafkaBootStrapServers = "localhost:29092,localhost:39092";

  public static void createTopic(final String topic, final Properties cloudConfig) {
    final NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.empty());
    try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
      adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
    } catch (final InterruptedException | ExecutionException e) {
      // Ignore if TopicExistsException, which may be valid if topic exists
      if (!(e.getCause() instanceof TopicExistsException)) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testKafkaOffsets() {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers);
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<String, String>(producerProps);

    final String topic = "one_partition";

    // Produce sample data
    final Long numMessages = 10L;
    for (Long i = 0L; i < numMessages; i++) {
      String value = String.valueOf(i);
      System.out.printf("Producing record: %s\t%s%n", value, value);
      producer.send(
          new ProducerRecord<String, String>("one_partition", value, value),
          new Callback() {
            @Override
            public void onCompletion(RecordMetadata m, Exception e) {
              if (e != null) {
                e.printStackTrace();
              } else {
                System.out.printf(
                    "Produced record to topic %s partition [%d] @ offset %d%n",
                    m.topic(), m.partition(), m.offset());
              }
            }
          });
    }

    producer.flush();
    System.out.printf("10 messages were produced to topic %s%n", topic);
    producer.close();

    final AdminClient adminClient = AdminClient.create(producerProps);

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers);
    consumerProps.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-1");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // consumer
    final Consumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps);
    final TopicPartition topicPartition = new TopicPartition(topic, 0);
    final List<TopicPartition> topicPartitionList = Collections.singletonList(topicPartition);
    consumer.assign(topicPartitionList);

    LOG.info("highest offset is: {}", consumer.endOffsets(topicPartitionList).get(topicPartition));
    LOG.info("current consumer offset position: {}", consumer.position(topicPartition));
    final long startOffset = 40L;
    final long endOffset = 80L;
    consumer.seek(topicPartition, startOffset);
    LOG.info("current consumer offset position: {}", consumer.position(topicPartition));

    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {
          if (record.offset() > endOffset) {
            break;
          }
          System.out.printf(
              "Consumed record with key %s and value %s, at offset %d%n",
              record.key(), record.value(), record.offset());
        }
      }
    } finally {
      consumer.close();
    }
  }
}
