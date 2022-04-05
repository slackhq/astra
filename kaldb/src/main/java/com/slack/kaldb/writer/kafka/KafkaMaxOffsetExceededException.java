package com.slack.kaldb.writer.kafka;

/**
 * KafkaMaxOffsetExceededException is thrown when the consumer is trying to index past the max
 * allowed offset for a Kafka topic partition.
 */
public class KafkaMaxOffsetExceededException extends RuntimeException {
  public KafkaMaxOffsetExceededException(String msg) {
    super(msg);
  }
}
