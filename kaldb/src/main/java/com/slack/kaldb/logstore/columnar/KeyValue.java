package com.slack.kaldb.logstore.columnar;

import com.slack.kaldb.logstore.columnar.message.MessageHeader;
import java.util.List;

/**
 * Generic Object used to read next message from various file reader implementations
 *
 * @author Praveen Murugesan (praveen@uber.com)
 */
public class KeyValue {

  private final long mOffset;
  private final byte[] mKafkaKey;
  private final byte[] mValue;
  private final long mTimestamp;
  private List<MessageHeader> mHeaders;

  // constructor
  public KeyValue(long offset, byte[] value) {
    this.mOffset = offset;
    this.mKafkaKey = new byte[0];
    this.mValue = value;
    this.mTimestamp = -1;
  }

  // constructor
  public KeyValue(long offset, byte[] kafkaKey, byte[] value) {
    this.mOffset = offset;
    this.mKafkaKey = kafkaKey;
    this.mValue = value;
    this.mTimestamp = -1;
  }

  // constructor
  public KeyValue(long offset, byte[] kafkaKey, byte[] value, long timestamp) {
    this.mOffset = offset;
    this.mKafkaKey = kafkaKey;
    this.mValue = value;
    this.mTimestamp = timestamp;
  }

  // constructor
  public KeyValue(
      long offset, byte[] kafkaKey, byte[] value, long timestamp, List<MessageHeader> headers) {
    this.mOffset = offset;
    this.mKafkaKey = kafkaKey;
    this.mValue = value;
    this.mTimestamp = timestamp;
    this.mHeaders = headers;
  }

  public long getOffset() {
    return this.mOffset;
  }

  public byte[] getKafkaKey() {
    return this.mKafkaKey;
  }

  public byte[] getValue() {
    return this.mValue;
  }

  public long getTimestamp() {
    return this.mTimestamp;
  }

  public boolean hasKafkaKey() {
    return this.mKafkaKey != null && this.mKafkaKey.length != 0;
  }

  public boolean hasTimestamp() {
    return this.mTimestamp != -1;
  }

  public List<MessageHeader> getHeaders() {
    return mHeaders;
  }
}
