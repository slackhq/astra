package com.slack.kaldb.logstore.columnar.message;

import java.util.Arrays;
import java.util.List;

/**
 * Parsed message is a Kafka message that has been processed by the parser that extracted its
 * partitions.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ParsedMessage extends Message {
  private String[] mPartitions;

  @Override
  public String toString() {
    return "ParsedMessage{"
        + fieldsToString(false)
        + ", mPartitions="
        + Arrays.toString(mPartitions)
        + '}';
  }

  public String toTruncatedString() {
    return "ParsedMessage{"
        + fieldsToString(true)
        + ", mPartitions="
        + Arrays.toString(mPartitions)
        + '}';
  }

  public ParsedMessage(
      String topic,
      int kafkaPartition,
      long offset,
      byte[] kafkaKey,
      byte[] payload,
      String[] mPartitions,
      long timestamp,
      List<MessageHeader> headers) {
    super(topic, kafkaPartition, offset, kafkaKey, payload, timestamp, headers);
    this.mPartitions = mPartitions;
  }

  public String[] getPartitions() {
    return mPartitions;
  }
}
