package com.slack.kaldb.logstore;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * LogMessage class represents a well formed log message that can indexed by the Lucene indexer.
 * This class also has methods to convert this class into a lucene document.
 *
 * <p>This class handles all times in UTC timezone.
 */
public class LogMessage extends LogWireMessage {

  static final Pattern INDEX_NAME_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_./:]*$");

  // SystemFields are lucene fields created for internal use of KalDB.
  public enum SystemField {
    // The source field contains the input document.
    SOURCE("_source"),
    ID("_id"),
    INDEX("_index"),
    TIME_SINCE_EPOCH("_timesinceepoch"),
    ALL("_all");

    public final String fieldName;

    SystemField(String fieldName) {
      this.fieldName = fieldName;
    }

    static final Set<String> systemFieldNames = new TreeSet<>();

    static {
      for (SystemField f : SystemField.values()) {
        systemFieldNames.add(f.fieldName);
      }
    }

    static boolean isSystemField(String name) {
      return SystemField.systemFieldNames.contains(name);
    }
  }

  // ReservedFields are field with pre-defined definitions created for a consistent experience.
  public enum ReservedField {
    TYPE("type"),
    HOSTNAME("hostname"),
    PACKAGE("package"),
    MESSAGE("message"),
    TAG("tag"),
    USERNAME("username"),
    PAYLOAD("payload"),
    NAME("name"),
    SERVICE_NAME("service_name"),
    DURATION_MS("duration_ms"),
    DURATION("duration"),
    TRACE_ID("trace_id"),
    PARENT_ID("parent_id"),
    KALDB_INVALID_TIMESTAMP("kaldb_invalid_timestamp");

    public final String fieldName;

    ReservedField(String fieldName) {
      this.fieldName = fieldName;
    }

    static final Set<String> reservedFieldNames = new TreeSet<>();

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
    return optionalWireMsg.map(LogMessage::fromWireMessage);
  }

  public static LogMessage fromWireMessage(LogWireMessage wireMessage) {
    return new LogMessage(
        computedIndexName(wireMessage.getIndex()),
        wireMessage.getType(),
        wireMessage.getId(),
        wireMessage.getTimestamp(),
        wireMessage.getSource());
  }

  private boolean isValid() {
    return (getIndex() != null
        && getType() != null
        && getId() != null
        && getSource() != null
        && INDEX_NAME_PATTERN.matcher(getIndex()).matches());
  }

  public LogMessage(
      String index, String type, String messageId, Instant timestamp, Map<String, Object> source) {
    super(index, type, messageId, timestamp, source);
    if (!isValid()) {
      throw new BadMessageFormatException(
          String.format("Index:%s, Type: %s, Id: %s, Source: %s", index, type, getId(), source));
    }
  }

  public LogWireMessage toWireMessage() {
    return new LogWireMessage(getIndex(), getType(), getId(), getTimestamp(), getSource());
  }
}
