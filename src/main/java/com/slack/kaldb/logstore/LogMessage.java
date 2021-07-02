package com.slack.kaldb.logstore;

import java.time.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * LogMessage class represents a well formed log message that can indexed by the Lucene indexer.
 * This class also has methods to convert this class into a lucene document.
 *
 * <p>This class handles all times in UTC timezone.
 */
public class LogMessage extends LogWireMessage {

  public static final ZoneOffset DEFAULT_TIME_ZONE = ZoneOffset.UTC;

  static final Pattern INDEX_NAME_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_./:]*$");

  public enum SystemField {
    // The source field contains the input document.
    SOURCE("_source"),
    ID("id"),
    INDEX("index"),
    TIME_SINCE_EPOCH("_timesinceepoch"),
    TYPE("type");

    public final String fieldName;

    SystemField(String fieldName) {
      this.fieldName = fieldName;
    }

    static final Set<String> systemFieldNames = new TreeSet<String>();

    static {
      for (SystemField f : SystemField.values()) {
        systemFieldNames.add(f.fieldName);
      }
    }

    static boolean isSystemField(String name) {
      return SystemField.systemFieldNames.contains(name);
    }
  }

  public enum ReservedField {
    HOSTNAME("hostname"),
    PACKAGE("package"),
    MESSAGE("message"),
    TAG("tag"),
    TIMESTAMP("@timestamp"),
    USERNAME("username"),
    PAYLOAD("payload"),
    NAME("name"),
    SERVICE_NAME("service_name"),
    DURATION_MS("duration_ms"),
    TRACE_ID("trace_id"),
    PARENT_ID("parent_id");

    public final String fieldName;

    ReservedField(String fieldName) {
      this.fieldName = fieldName;
    }

    static final Set<String> reservedFieldNames = new TreeSet<String>();

    static {
      for (ReservedField f : ReservedField.values()) {
        reservedFieldNames.add(f.fieldName);
      }
    }

    static boolean isReservedField(String name) {
      return ReservedField.reservedFieldNames.contains(name);
    }
  }

  public static String computedIndexName(String indexName) {
    return indexName.replace("-", "_");
  }

  public static Optional<LogMessage> fromJSON(String jsonStr) {
    Optional<LogWireMessage> optionalWireMsg = LogWireMessage.fromJson(jsonStr);
    if (optionalWireMsg.isPresent()) {
      return Optional.of(fromWireMessage(optionalWireMsg.get()));
    }
    return Optional.empty();
  }

  public static LogMessage fromWireMessage(LogWireMessage wireMessage) {
    return new LogMessage(
        computedIndexName(wireMessage.getIndex()),
        wireMessage.getType(),
        wireMessage.id,
        wireMessage.source);
  }

  private boolean isValid() {
    return (getIndex() != null
        && getType() != null
        && id != null
        && source != null
        && INDEX_NAME_PATTERN.matcher(getIndex()).matches());
  }

  private BadMessageFormatException raiseException(Throwable t) {
    throw new BadMessageFormatException(
        String.format(
            "Index:%s, Type: %s, Id: %s, Source: %s".format(getIndex(), getType(), id, source)),
        t);
  }

  // TODO: Use timestamp in micros
  public final long timeSinceEpochMilli;

  public LogMessage(String index, String type, String messageId, Map<String, Object> source) {
    super(index, type, messageId, source);
    if (!isValid()) {
      throw new BadMessageFormatException(
          String.format("Index:%s, Type: %s, Id: %s, Source: %s".format(index, type, id, source)));
    }
    this.timeSinceEpochMilli = getMillisecondsSinceEpoch();
  }

  private Long getMillisecondsSinceEpoch() {
    String s = (String) source.get(ReservedField.TIMESTAMP.fieldName);
    if (s != null) {
      return getTime(s);
    }
    throw raiseException(null);
  }

  private Long getTime(String dateStr) {
    return Instant.parse(dateStr).toEpochMilli();
  }

  public void addProperty(String key, Object value) {
    source.put(key, value);
  }

  public LogWireMessage toWireMessage() {
    return new LogWireMessage(getIndex(), getType(), id, source);
  }
}
