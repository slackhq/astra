package com.slack.kaldb.logstore;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * Base message for a log index. We will use this class to abstract away the lucene index from the
 * specific log message payload. This will allow us to add more fields to the payload thus making
 * the indexing system more flexible to the evolving data needs.
 */
public abstract class Message {
  private final String id;

  private final Instant timestamp;

  private final Map<String, Object> source;

  public Message(String id, Instant timestamp, Map<String, Object> source) {
    this.id = id;
    this.timestamp = timestamp;
    this.source = Collections.unmodifiableMap(source);
  }

  public String getId() {
    return id;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public Map<String, Object> getSource() {
    return source;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Message)) return false;

    Message message = (Message) o;

    if (!id.equals(message.id)) return false;
    if (!timestamp.equals(message.timestamp)) return false;
    return source.equals(message.source);
  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + timestamp.hashCode();
    result = 31 * result + source.hashCode();
    return result;
  }
}
