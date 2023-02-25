package com.slack.kaldb.writer;

import com.slack.kaldb.logstore.LogMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** An interface a ConsumerRecord message from Kafka into a LogMessage. */
@FunctionalInterface
public interface LogMessageTransformer {
  LogMessage toLogMessage(ConsumerRecord<String, byte[]> record) throws Exception;
}
