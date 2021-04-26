package com.slack.kaldb.logstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
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

  static Optional<LogWireMessage> fromJson(String jsonStr) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      LogWireMessage wireMessage = objectMapper.readValue(jsonStr, LogWireMessage.class);
      return Optional.of(wireMessage);
    } catch (IOException e) {
      LOG.error("Error parsing JSON Object from string " + jsonStr, e);
    }
    return Optional.empty();
  }

  public LogWireMessage() {
    super("", Collections.emptyMap());
  }

  public LogWireMessage(String index, String type, String id, Map<String, Object> source) {
    super(id, source);
    this.index = index;
    this.type = type;
  }

  public String getIndex() {
    return index;
  }

  public String getType() {
    return type;
  }
}
