package com.slack.kaldb.logstore;

import java.util.Map;

/**
 * Base message for a log index. We will use this class to abstract away the lucene index from the
 * specific log message payload. This will allow us to add more fields to the payload thus making
 * the indexing system more flexible to the evolving data needs.
 */
public abstract class Message {
  public final String id;

  public final Map<String, Object> source;

  public Message(String id, Map<String, Object> source) {
    this.id = id;
    this.source = source;
  }
}
