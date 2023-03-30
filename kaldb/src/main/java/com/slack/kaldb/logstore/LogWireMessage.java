package com.slack.kaldb.logstore;

import com.slack.kaldb.util.JsonUtil;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogWireMessage is the raw message we get from Kafka. This message may be invalid or malformed.
 * LogMessage is a refined form of this message.
 */
public class LogWireMessage extends Message {
  private static final Logger LOG = LoggerFactory.getLogger(LogWireMessage.class);

  private String index;
  private String type;

  /**
   * Move all Kafka message serializers to common class
   *
   * @see com.slack.kaldb.preprocessor.KaldbSerdes
   */
  @Deprecated
  static Optional<LogWireMessage> fromJson(String jsonStr) {
    try {
      LogWireMessage wireMessage = JsonUtil.read(jsonStr, LogWireMessage.class);
      return Optional.of(wireMessage);
    } catch (IOException e) {
      LOG.error("Error parsing JSON Object from string " + jsonStr, e);
    }
    return Optional.empty();
  }

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
