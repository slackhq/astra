package com.slack.astra.writer;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.schema.ReservedFields;
import com.slack.astra.proto.schema.Schema;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A utility class that converts a Span into a LogMessage, Json map to Span */
public class SpanFormatter {

  private static final Logger LOG = LoggerFactory.getLogger(SpanFormatter.class);

  public static final String DEFAULT_LOG_MESSAGE_TYPE = "INFO";
  public static final String DEFAULT_INDEX_NAME = "unknown";

  // TODO: Take duration unit as input.
  // TODO: Take a generic field mapping dictionary as input for fields.
  public static Trace.Span toSpan(
      Map<String, Object> jsonMsgMap,
      String id,
      String name,
      String serviceName,
      long timestamp,
      long duration,
      Optional<String> host,
      Optional<String> traceId) {

    Trace.Span.Builder spanBuilder = Trace.Span.newBuilder();

    spanBuilder.setName(name);

    String parentId =
        (String) jsonMsgMap.getOrDefault(LogMessage.ReservedField.PARENT_ID.fieldName, "");
    spanBuilder.setParentId(ByteString.copyFrom(parentId.getBytes()));

    traceId.ifPresent(s -> spanBuilder.setTraceId(ByteString.copyFromUtf8(s)));
    spanBuilder.setTimestamp(timestamp);
    spanBuilder.setDuration(duration);

    List<Trace.KeyValue> tags = new ArrayList<>(jsonMsgMap.size());
    for (Map.Entry<String, Object> entry : jsonMsgMap.entrySet()) {
      String key = entry.getKey();
      if (MurronLogFormatter.nonTagFields.contains(key)) {
        continue;
      }
      tags.add(convertKVtoProto(key, entry.getValue()));
    }

    // Add missing fields from murron message.
    boolean containsHostName =
        tags.stream()
            .anyMatch(
                keyValue -> keyValue.getKey().equals(LogMessage.ReservedField.HOSTNAME.fieldName));
    if (!containsHostName) {
      host.ifPresent(
          s ->
              tags.add(
                  Trace.KeyValue.newBuilder()
                      .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
                      .setFieldType(Schema.SchemaFieldType.KEYWORD)
                      .setVStr(s)
                      .build()));
    }

    spanBuilder.setId(ByteString.copyFrom(id.getBytes()));

    boolean containsServiceName =
        tags.stream()
            .anyMatch(
                keyValue ->
                    keyValue.getKey().equals(LogMessage.ReservedField.SERVICE_NAME.fieldName));
    if (!containsServiceName) {
      tags.add(
          Trace.KeyValue.newBuilder()
              .setKey(LogMessage.ReservedField.SERVICE_NAME.fieldName)
              .setFieldType(Schema.SchemaFieldType.KEYWORD)
              .setVStr(serviceName)
              .build());
    }
    spanBuilder.addAllTags(tags);
    return spanBuilder.build();
  }

  public static Timestamp parseDate(String dateStr, Schema.SchemaFieldType type) {
    Instant instant;
    try {
      // type will expose parsing params in the future
      // for now we'll just use Instant.parse
      instant = Instant.parse(dateStr);
    } catch (Exception e) {
      // easier to debug rather than to skip or put current value
      LOG.warn("Failed to parse date: {}", dateStr, e);
      instant = Instant.EPOCH;
    }
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  public static Trace.KeyValue makeTraceKV(String key, Object value, Schema.SchemaFieldType type) {
    Trace.KeyValue.Builder tagBuilder = Trace.KeyValue.newBuilder();
    tagBuilder.setKey(key);
    try {
      switch (type) {
        case KEYWORD -> {
          tagBuilder.setFieldType(Schema.SchemaFieldType.KEYWORD);
          tagBuilder.setVStr(value.toString());
        }
        case TEXT -> {
          tagBuilder.setFieldType(Schema.SchemaFieldType.TEXT);
          tagBuilder.setVStr(value.toString());
        }
        case IP -> {
          tagBuilder.setFieldType(Schema.SchemaFieldType.IP);
          tagBuilder.setVStr(value.toString());
        }
        case DATE -> {
          tagBuilder.setFieldType(Schema.SchemaFieldType.DATE);
          tagBuilder.setVDate(parseDate(value.toString(), type));
          // setting both for backward compatibility while deploying preprocessor and indexer
          // I however commented it while testing to make sure all tests use the new field
          tagBuilder.setVInt64(Instant.parse(value.toString()).toEpochMilli());
        }
        case BOOLEAN -> {
          tagBuilder.setFieldType(Schema.SchemaFieldType.BOOLEAN);
          tagBuilder.setVBool(Boolean.parseBoolean(value.toString()));
        }
        case DOUBLE -> {
          tagBuilder.setFieldType(Schema.SchemaFieldType.DOUBLE);
          tagBuilder.setVFloat64(Double.parseDouble(value.toString()));
        }
        case FLOAT -> {
          tagBuilder.setFieldType(Schema.SchemaFieldType.FLOAT);
          tagBuilder.setVFloat32(Float.parseFloat(value.toString()));
        }
        case HALF_FLOAT -> {
          tagBuilder.setFieldType(Schema.SchemaFieldType.HALF_FLOAT);
          tagBuilder.setVFloat32(Float.parseFloat(value.toString()));
        }
        case INTEGER -> {
          tagBuilder.setFieldType(Schema.SchemaFieldType.INTEGER);
          tagBuilder.setVInt32(Integer.parseInt(value.toString()));
        }
        case LONG -> {
          tagBuilder.setFieldType(Schema.SchemaFieldType.LONG);
          tagBuilder.setVInt64(Long.parseLong(value.toString()));
        }
        case SCALED_LONG -> {
          tagBuilder.setFieldType(Schema.SchemaFieldType.SCALED_LONG);
          tagBuilder.setVInt64(Long.parseLong(value.toString()));
        }
        case SHORT -> {
          tagBuilder.setFieldType(Schema.SchemaFieldType.SHORT);
          tagBuilder.setVInt32(Integer.parseInt(value.toString()));
        }
        case BYTE -> {
          tagBuilder.setFieldType(Schema.SchemaFieldType.BYTE);
          tagBuilder.setVInt32(Integer.parseInt(value.toString()));
        }
        case BINARY -> {
          tagBuilder.setFieldType(Schema.SchemaFieldType.BINARY);
          tagBuilder.setVBinary(ByteString.copyFrom(value.toString().getBytes()));
        }
      }
      return tagBuilder.build();
    } catch (Exception e) {
      tagBuilder.setKey(STR."failed_\{key}");
      tagBuilder.setFieldType(Schema.SchemaFieldType.KEYWORD);
      tagBuilder.setVStr(value.toString());
      return tagBuilder.build();
    }
  }

  public static Schema.SchemaField getSchemaFieldDefForValue(Object value) {
    return switch (value) {
      case Boolean b -> ReservedFields.getSchemaFieldForType(Schema.SchemaFieldType.BOOLEAN);
      case Integer i -> ReservedFields.getSchemaFieldForType(Schema.SchemaFieldType.INTEGER);
      case Long l -> ReservedFields.getSchemaFieldForType(Schema.SchemaFieldType.LONG);
      case Float v -> ReservedFields.getSchemaFieldForType(Schema.SchemaFieldType.FLOAT);
      case Double v -> ReservedFields.getSchemaFieldForType(Schema.SchemaFieldType.DOUBLE);
      case null, default -> ReservedFields.getSchemaFieldForType(Schema.SchemaFieldType.TEXT);
    };
  }

  public static List<Trace.KeyValue> convertKVtoProto(
      String key, Object value, Schema.IngestSchema schema) {
    if (value == null || value.toString().isEmpty()) {
      return null;
    }

    Schema.SchemaField schemaFieldDef;
    if (schema.containsFields(key)) {
      schemaFieldDef = schema.getFieldsMap().get(key);
    } else {
      schemaFieldDef = getSchemaFieldDefForValue(value);
    }

    List<Trace.KeyValue> tags = new ArrayList<>();
    tags.add(makeTraceKV(key, value, schemaFieldDef.getType()));
    for (Map.Entry<String, Schema.SchemaField> additionalField :
        schemaFieldDef.getFieldsMap().entrySet()) {
      // skip conditions
      if (additionalField.getValue().getIgnoreAbove() > 0
          && additionalField.getValue().getType() == Schema.SchemaFieldType.KEYWORD
          && value.toString().length() > additionalField.getValue().getIgnoreAbove()) {
        continue;
      }
      Trace.KeyValue additionalKV =
          makeTraceKV(
              STR."\{key}.\{additionalField.getKey()}",
              value,
              additionalField.getValue().getType());
      tags.add(additionalKV);
    }
    // For all TEXT fields that have NOT been explicitly defined in the schema, we will also
    // copy the value to a .keyword field if it is less than N characters and the flag is enabled
    if (!schema.containsFields(key)
        && schemaFieldDef.getType() == Schema.SchemaFieldType.TEXT
        && schema.getEnableKeywordSubfield()) {
      if (schema.getIgnoreAboveSubfield() <= 0
          || value.toString().length() <= schema.getIgnoreAboveSubfield()) {
        tags.add(makeTraceKV(STR."\{key}_keyword", value, Schema.SchemaFieldType.KEYWORD));
      }
    }
    return tags;
  }

  // this is now only used in one place that will go away once we cleanup all the old parsers that
  // are no longer used
  private static Trace.KeyValue convertKVtoProto(String key, Object value) {
    Trace.KeyValue.Builder tagBuilder = Trace.KeyValue.newBuilder();
    tagBuilder.setKey(key);
    if (value instanceof String || value instanceof List || value instanceof Map) {
      tagBuilder.setFieldType(Schema.SchemaFieldType.KEYWORD);
      tagBuilder.setVStr(value.toString());
    } else if (value instanceof Boolean) {
      tagBuilder.setFieldType(Schema.SchemaFieldType.BOOLEAN);
      tagBuilder.setVBool((boolean) value);
    } else if (value instanceof Integer) {
      tagBuilder.setFieldType(Schema.SchemaFieldType.INTEGER);
      tagBuilder.setVInt32((int) value);
    } else if (value instanceof Long) {
      tagBuilder.setFieldType(Schema.SchemaFieldType.LONG);
      tagBuilder.setVInt64((long) value);
    } else if (value instanceof Float) {
      tagBuilder.setFieldType(Schema.SchemaFieldType.FLOAT);
      tagBuilder.setVFloat32((float) value);
    } else if (value instanceof Double) {
      tagBuilder.setFieldType(Schema.SchemaFieldType.DOUBLE);
      tagBuilder.setVFloat64((double) value);
    } else if (value != null) {
      tagBuilder.setFieldType(Schema.SchemaFieldType.BINARY);
      tagBuilder.setVBinary(ByteString.copyFrom(value.toString().getBytes()));
    }
    return tagBuilder.build();
  }

  public static Trace.ListOfSpans fromMurronMessage(Murron.MurronMessage message)
      throws InvalidProtocolBufferException {
    return Trace.ListOfSpans.parseFrom(message.getMessage());
  }

  /**
   * Determines if provided timestamp is a reasonable value, or is too far in the past/future for
   * use. This can happen when using user-provided timestamp (such as on a mobile client).
   */
  // Todo - this should be moved to the edge, in the preprocessor pipeline instead of
  //  using it here as part of the toLogMessage. Also consider making these values config options.
  @SuppressWarnings("RedundantIfStatement")
  public static boolean isValidTimestamp(Instant timestamp) {
    // cannot be in the future by more than 1 hour
    if (timestamp.isAfter(Instant.now().plus(1, ChronoUnit.HOURS))) {
      return false;
    }
    // cannot be in the past by more than 168 hours
    if (timestamp.isBefore(Instant.now().minus(168, ChronoUnit.HOURS))) {
      return false;
    }
    return true;
  }
}
