package com.slack.kaldb.writer;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
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
      Trace.KeyValue.Builder tagBuilder = Trace.KeyValue.newBuilder();
      tagBuilder.setKey(key);
      if (entry.getValue() instanceof String) {
        tagBuilder.setVType(Trace.ValueType.STRING);
        tagBuilder.setVStr(entry.getValue().toString());
      } else if (entry.getValue() instanceof Boolean) {
        tagBuilder.setVType(Trace.ValueType.BOOL);
        tagBuilder.setVBool((boolean) entry.getValue());
      } else if (entry.getValue() instanceof Integer) {
        tagBuilder.setVType(Trace.ValueType.INT64);
        tagBuilder.setVInt64((int) entry.getValue());
      } else if (entry.getValue() instanceof Float) {
        tagBuilder.setVType(Trace.ValueType.FLOAT64);
        tagBuilder.setVFloat64((float) entry.getValue());
      } else if (entry.getValue() instanceof Double) {
        tagBuilder.setVType(Trace.ValueType.FLOAT64);
        tagBuilder.setVFloat64((double) entry.getValue());
      } else if (entry.getValue() != null) {
        tagBuilder.setVType(Trace.ValueType.BINARY);
        tagBuilder.setVBinary(ByteString.copyFrom(entry.getValue().toString().getBytes()));
      }
      tags.add(tagBuilder.build());
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
                      .setVType(Trace.ValueType.STRING)
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
              .setVType(Trace.ValueType.STRING)
              .build());
    }
    spanBuilder.addAllTags(tags);
    return spanBuilder.build();
  }

  public static Trace.ListOfSpans fromMurronMessage(Murron.MurronMessage message)
      throws InvalidProtocolBufferException {
    return Trace.ListOfSpans.parseFrom(message.getMessage());
  }

  public static String encodeBinaryTagValue(ByteString binaryTagValue) {
    return Base64.getEncoder().encodeToString(binaryTagValue.toByteArray());
  }

  // TODO: Make this function more memory efficient?
  public static LogMessage toLogMessage(Trace.Span span) {
    if (span == null) return null;

    Map<String, Object> jsonMap = new HashMap<>();

    String id = span.getId().toStringUtf8();

    // Set these fields even if they are empty so we can always search these fields.
    jsonMap.put(LogMessage.ReservedField.PARENT_ID.fieldName, span.getParentId().toStringUtf8());
    jsonMap.put(LogMessage.ReservedField.TRACE_ID.fieldName, span.getTraceId().toStringUtf8());
    jsonMap.put(LogMessage.ReservedField.NAME.fieldName, span.getName());
    jsonMap.put(LogMessage.ReservedField.DURATION_MS.fieldName, span.getDuration());

    // TODO: Use a microsecond resolution, instead of millisecond resolution.
    if (span.getTimestamp() <= 0) {
      throw new IllegalStateException(
          "span id=" + id + " has incorrect timestamp=" + span.getTimestamp());
    }
    Instant timestamp = Instant.ofEpochMilli(span.getTimestamp() / (1000));
    jsonMap.put(LogMessage.ReservedField.TIMESTAMP.fieldName, timestamp.toString());

    String indexName = "";
    String msgType = DEFAULT_LOG_MESSAGE_TYPE;
    for (Trace.KeyValue tag : span.getTagsList()) {
      String key = tag.getKey();
      int valueType = tag.getVType().getNumber();
      if (valueType == 0) {
        if (key.equals(LogMessage.ReservedField.TYPE.fieldName)) {
          msgType = tag.getVStr();
          continue;
        }
        jsonMap.put(key, tag.getVStr());
        if (key.equals(LogMessage.ReservedField.SERVICE_NAME.fieldName)) {
          indexName = tag.getVStr();
          // Also, add service name to the map so can search by service name also.
          jsonMap.put(LogMessage.ReservedField.SERVICE_NAME.fieldName, indexName);
        }
      } else if (valueType == 1) {
        jsonMap.put(key, tag.getVBool());
      } else if (valueType == 2) {
        jsonMap.put(key, tag.getVInt64());
      } else if (valueType == 3) {
        jsonMap.put(key, tag.getVFloat64());
      } else if (valueType == 4) {
        jsonMap.put(key, encodeBinaryTagValue(tag.getVBinary()));
      } else {
        LOG.warn("Skipping field with unknown value type {} with key {}", valueType, key);
      }
    }

    if (indexName.isEmpty()) {
      indexName = DEFAULT_INDEX_NAME;
    }
    jsonMap.put(LogMessage.ReservedField.SERVICE_NAME.fieldName, indexName);

    //  This logging is in place to debug span parsing exceptions. Once this pipeline
    //  is more stable remove this code.
    // try {
    //      return LogMessage.fromWireMessage(new LogWireMessage(indexName, msgType, id, jsonMap));
    //    } catch (Exception e) {
    //      try {
    //        LOG.info(
    //            "span conversion failed: "
    //                + JsonFormat.printer()
    //                    .includingDefaultValueFields()
    //                    .omittingInsignificantWhitespace()
    //                    .print(span));
    //      } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
    //        invalidProtocolBufferException.printStackTrace();
    //      }
    //      throw e;
    //    }

    // Drop the type field from LogMessage since with spans it doesn't make sense.
    return LogMessage.fromWireMessage(new LogWireMessage(indexName, msgType, id, jsonMap));
  }

  // TODO: For now assuming that the tags in ListOfSpans is empty. Handle this case in future.
  public static List<LogMessage> toLogMessage(Trace.ListOfSpans protoSpans) {
    if (protoSpans == null) return Collections.EMPTY_LIST;

    List<Trace.Span> spans = protoSpans.getSpansList();
    List<LogMessage> messages = new ArrayList<>(spans.size());
    for (Trace.Span span : spans) {
      messages.add(toLogMessage(span));
    }
    return messages;
  }
}
