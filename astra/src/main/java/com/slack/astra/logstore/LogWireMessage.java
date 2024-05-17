package com.slack.astra.logstore;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * LogWireMessage is the raw message we get from Kafka. This message may be invalid or malformed.
 * LogMessage is a refined form of this message.
 */
public class LogWireMessage extends Message {

  private String index;
  private String type;

  public LogWireMessage() {
    super("", Instant.now(), Collections.emptyMap());
  }

  public LogWireMessage(
      String index, String type, String id, Instant timestamp, Map<String, Object> source) {
    super(id, timestamp, source);
    this.index = index;
    this.type = type;
  }

  public String getIndex() {
    return index;
  }

  public String getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof LogWireMessage)) return false;

    LogWireMessage that = (LogWireMessage) o;

    if (!index.equals(that.index)) return false;
    return type.equals(that.type);
  }

  @Override
  public int hashCode() {
    int result = index.hashCode();
    result = 31 * result + type.hashCode();
    return result;
  }
}
