package com.slack.kaldb.logstore;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogMessage class represents a well formed log message that can indexed by the Lucene indexer.
 * This class also has methods to convert this class into a lucene document.
 *
 * <p>This class handles all times in UTC timezone.
 */
public class LogMessage extends LogWireMessage {

  private static final Logger LOG = LoggerFactory.getLogger(LogMessage.class);
  static final Pattern INDEX_NAME_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_./:]*$");

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
    TYPE("type"),
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
        "Index:%s, Type: %s, Id: %s, Source: %s".format(getIndex(), getType(), id, source), t);
  }

  // TODO: Use timestamp in micros
  public final long timeSinceEpochMilli;

  public LogMessage(String index, String type, String messageId, Map<String, Object> source) {
    super(index, type, messageId, source);
    if (!isValid()) {
      throw new BadMessageFormatException(
          "Index:%s, Type: %s, Id: %s, Source: %s".format(index, type, id, source));
    }
    this.timeSinceEpochMilli = getMillisecondsSinceEpoch();
  }

  public Long getMillisecondsSinceEpoch() {
    String s = (String) source.get(ReservedField.TIMESTAMP.fieldName);
    if (s != null) {
      return getTime(s);
    }
    throw raiseException(
        new Throwable("Parse failure for message id=" + id + " with timestamp=" + s));
  }

  public Map<String, Object> getSource() {
    return ImmutableMap.copyOf(source);
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LogMessage that = (LogMessage) o;
    if (id == null || that.id == null) {
      LOG.warn("id missing - equals comparison won't be accurate");
    }
    return timeSinceEpochMilli == that.timeSinceEpochMilli
        && Objects.equal(getIndex(), that.getIndex())
        && Objects.equal(getType(), that.getType())
        && Objects.equal(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(timeSinceEpochMilli, getIndex(), getMillisecondsSinceEpoch(), id);
  }
}
