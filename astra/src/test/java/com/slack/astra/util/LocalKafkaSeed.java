package com.slack.astra.util;

import static com.slack.astra.testlib.TestKafkaServer.TEST_KAFKA_TOPIC;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.google.protobuf.ByteString;
import com.slack.astra.testlib.TestKafkaServer;
import com.slack.service.murron.Murron;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Instant;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Provides functionality to seed a local instance of Kafka with sample data.
 *
 * <p>These tests are intended for local debugging purposes, and will be executed manually as
 * needed.
 */
public class LocalKafkaSeed {
  private static final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss.SSSzzz");

  /** Initializes local kafka broker with sample messages occurring in the immediate future. */
  @Disabled
  @Test
  public void seedLocalBrokerWithSampleData() throws Exception {
    EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 2181);
    final Instant startTime = Instant.now();
    TestKafkaServer.produceMessagesToKafka(broker, startTime);
  }

  @Disabled
  @Test
  public void seedJsonLogsFromFile() throws IOException {
    EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 2181);
    try (BufferedReader reader = Files.newBufferedReader(Path.of("../example_logs.txt"))) {

      String line = reader.readLine();
      int i = 0;

      try (KafkaProducer<String, byte[]> producer =
          broker.createProducer(new StringSerializer(), new ByteArraySerializer(), null)) {
        while (line != null) {
          ProducerRecord<String, byte[]> kafkaRecord =
              new ProducerRecord<>(
                  TEST_KAFKA_TOPIC, 0, String.valueOf(i), line.getBytes(StandardCharsets.UTF_8));
          producer.send(kafkaRecord);
          line = reader.readLine();
          i++;
        }
      }
    }
  }

  /** Initializes local kafka broker with sample messages replayed from a log file */
  @Disabled
  @Test
  public void seedFromFile() throws IOException {
    EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 2181);
    try (BufferedReader reader = Files.newBufferedReader(Path.of("../example_logs.txt"))) {

      String line = reader.readLine();
      int i = 0;

      try (KafkaProducer<String, byte[]> producer =
          broker.createProducer(new StringSerializer(), new ByteArraySerializer(), null)) {
        while (line != null) {
          ProducerRecord<String, byte[]> kafkaRecord = makeProducerRecord(line, i);
          producer.send(kafkaRecord);
          line = reader.readLine();
          i++;
        }
      }
    }
  }

  /**
   * Reads in a log line and generates a ProducerRecord to insert to Kafka, originally based off of
   * the IndexAPILog Benchmark test
   */
  private ProducerRecord<String, byte[]> makeProducerRecord(String line, int offset) {
    try {
      // get start of messageBody
      int messageDivision = line.indexOf("{");

      // Everything will there is metadata
      String[] splitLine = line.substring(0, messageDivision - 1).split("\\s+");
      String ts = splitLine[0] + splitLine[1] + splitLine[2] + splitLine[3];
      long timestamp = df.parse(ts).toInstant().toEpochMilli();

      String message = line.substring(messageDivision);
      Murron.MurronMessage testMurronMsg =
          Murron.MurronMessage.newBuilder()
              .setMessage(ByteString.copyFrom(message.getBytes(StandardCharsets.UTF_8)))
              .setType(splitLine[5])
              .setHost(splitLine[4])
              .setTimestamp(timestamp * 1000 * 1000)
              .setPid(100)
              .setOffset(offset)
              .build();

      return new ProducerRecord<>(
          TEST_KAFKA_TOPIC, 0, String.valueOf(offset), testMurronMsg.toByteArray());
    } catch (Exception e) {
      System.out.println("skipping - cannot parse input" + e);
      return null;
    }
  }
}
