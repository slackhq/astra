package com.slack.astra.writer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.slack.astra.proto.schema.Schema;
import com.slack.service.murron.trace.Trace;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A utility class that converts a Span into a LogMessage, Json map to Span */
public class SpanFormatter {

  private static final Logger LOG = LoggerFactory.getLogger(SpanFormatter.class);

  public static final String DEFAULT_LOG_MESSAGE_TYPE = "INFO";
  public static final String DEFAULT_INDEX_NAME = "unknown";

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

  public static List<Trace.KeyValue> convertKVtoProto(
      String key, Object value, Schema.IngestSchema schema) {
    if (value == null || value.toString().isEmpty()) {
      return null;
    }

    if (schema.containsFields(key)) {
      List<Trace.KeyValue> tags = new ArrayList<>();
      Schema.SchemaField schemaFieldDef = schema.getFieldsMap().get(key);
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
      return tags;
    } else {
      return List.of(SpanFormatter.convertKVtoProto(key, value));
    }
  }

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
