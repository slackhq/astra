package com.slack.kaldb.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiLogFormatter {
  public static final Logger LOG = LoggerFactory.getLogger(ApiLogFormatter.class);

  public static final String API_LOG_DURATION_FIELD = "microtime_elapsed";
  public static final Set<String> nonTagFields =
      ImmutableSet.of(
          LogMessage.ReservedField.PARENT_ID.fieldName,
          LogMessage.ReservedField.TRACE_ID.fieldName,
          API_LOG_DURATION_FIELD);
  private static final String TYPE_TAG = "type";

  // TODO: Catch exception in a separate wrapper method?
  public static Trace.Span toSpan(Murron.MurronMessage murronMsg) throws JsonProcessingException {
    if (murronMsg == null) return null;

    LOG.trace(
        "{} {} {} {}",
        murronMsg.getTimestamp(),
        murronMsg.getHost(),
        murronMsg.getType(),
        murronMsg.getMessage().toStringUtf8());

    // Create mapper
    ObjectMapper mapper = new ObjectMapper();
    TypeReference<Map<String, Object>> mapTypeRef = new TypeReference<>() {};
    Map<String, Object> jsonMsgMap =
        mapper.readValue(murronMsg.getMessage().toStringUtf8(), mapTypeRef);

    Trace.Span.Builder spanBuilder = Trace.Span.newBuilder();

    // Set the type tag as the span name.
    spanBuilder.setName((String) jsonMsgMap.getOrDefault(TYPE_TAG, ""));

    String parentId =
        (String) jsonMsgMap.getOrDefault(LogMessage.ReservedField.PARENT_ID.fieldName, "");
    spanBuilder.setParentId(ByteString.copyFrom(parentId.getBytes()));

    String traceId =
        (String) jsonMsgMap.getOrDefault(LogMessage.ReservedField.TRACE_ID.fieldName, "");
    spanBuilder.setTraceId(ByteString.copyFrom(traceId.getBytes()));

    spanBuilder.setStartTimestampMicros(murronMsg.getTimestamp() / 1000);
    spanBuilder.setDurationMicros((int) jsonMsgMap.getOrDefault(API_LOG_DURATION_FIELD, 1));

    List<Trace.KeyValue> tags = new ArrayList<>(jsonMsgMap.size());
    for (Map.Entry<String, Object> entry : jsonMsgMap.entrySet()) {
      String key = entry.getKey();
      if (nonTagFields.contains(key)) {
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
    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
            .setVType(Trace.ValueType.STRING)
            .setVStr(murronMsg.getHost())
            .build());

    // Create a message id.
    String msgId = murronMsg.getHost() + ":" + murronMsg.getPid() + ":" + murronMsg.getOffset();
    spanBuilder.setId(ByteString.copyFrom(msgId.getBytes()));

    // Replace hyphens with underscore since lucene doesn't like it.
    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.SERVICE_NAME.fieldName)
            .setVType(Trace.ValueType.STRING)
            .setVStr(murronMsg.getType().replace("-", "_"))
            .build());

    spanBuilder.addAllTags(tags);
    return spanBuilder.build();
  }
}
