package com.slack.astra.writer;

import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/*
 * MessageWriter interface is an interface that is used by KafkaWriter to ingest messages into a store.
 */
public interface MessageWriter {
  boolean insertRecord(ConsumerRecord<String, byte[]> record) throws IOException;
}
