package com.slack.kaldb.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.util.JsonUtil;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MurronLogFormatter {
  public static final Logger LOG = LoggerFactory.getLogger(MurronLogFormatter.class);

  public static final String API_LOG_DURATION_FIELD = "microtime_elapsed";
  public static final String ENVOY_DURATION_FIELD = "duration";
  public static final String ENVOY_REQUEST_ID = "request_id";
  public static final String ID = "id";
  static final Set<String> nonTagFields =
      ImmutableSet.of(
          LogMessage.ReservedField.PARENT_ID.fieldName,
          LogMessage.ReservedField.TRACE_ID.fieldName,
          API_LOG_DURATION_FIELD);
  private static final String TYPE_TAG = "type";

  public static Trace.Span fromJsonLog(byte[] data) throws IOException {
    TypeReference<Map<String, Object>> mapTypeRef = new TypeReference<>() {};
    Map<String, Object> jsonMsgMap = JsonUtil.read(data, mapTypeRef);

    String id = (String) jsonMsgMap.getOrDefault(ID, UUID.randomUUID().toString());

    String serviceName =
        (String) jsonMsgMap.getOrDefault(LogMessage.ReservedField.SERVICE_NAME.fieldName, "");
    if (serviceName == null || serviceName.isEmpty()) {
      throw new IOException("Document must contain service_name key");
    }
    String name =
        (String) jsonMsgMap.getOrDefault(LogMessage.ReservedField.NAME.fieldName, serviceName);

    long duration =
        Long.parseLong(
            String.valueOf(
                jsonMsgMap.getOrDefault(LogMessage.ReservedField.DURATION_MS.fieldName, "1")));

    String dateStr =
        (String) jsonMsgMap.getOrDefault(LogMessage.ReservedField.TIMESTAMP.fieldName, "");
    if (dateStr == null || dateStr.isEmpty()) {
      throw new IOException("Document must contain timestamp key");
    }
    long timestamp = Instant.parse(dateStr).toEpochMilli();

    String traceId = (String) jsonMsgMap.get(LogMessage.ReservedField.TRACE_ID.fieldName);
    String host = (String) jsonMsgMap.get(LogMessage.ReservedField.HOSTNAME.fieldName);

    return toSpan(
        jsonMsgMap,
        id,
        name,
        timestamp,
        duration,
        Optional.ofNullable(host),
        Optional.ofNullable(traceId));
  }

  public static Trace.Span fromEnvoyLog(Murron.MurronMessage murronMsg)
      throws JsonProcessingException {
    return toSpan(
        murronMsg, TYPE_TAG, ENVOY_DURATION_FIELD, 1000, ENVOY_REQUEST_ID, ENVOY_REQUEST_ID);
  }

  public static Trace.Span fromApiLog(Murron.MurronMessage murronMsg)
      throws JsonProcessingException {
    return toSpan(
        murronMsg,
        TYPE_TAG,
        API_LOG_DURATION_FIELD,
        1,
        LogMessage.ReservedField.TRACE_ID.fieldName,
        "");
  }

  private static Trace.Span toSpan(
      Murron.MurronMessage murronMsg,
      String typeTag,
      String durationField,
      int durationTimeMuiltiplier,
      String traceIdFieldName,
      String idField)
      throws JsonProcessingException {
    if (murronMsg == null) return null;

    LOG.trace(
        "{} {} {} {}",
        murronMsg.getTimestamp(),
        murronMsg.getHost(),
        murronMsg.getType(),
        murronMsg.getMessage().toStringUtf8());

    TypeReference<Map<String, Object>> mapTypeRef = new TypeReference<>() {};
    Map<String, Object> jsonMsgMap =
        JsonUtil.read(murronMsg.getMessage().toStringUtf8(), mapTypeRef);

    String type = (String) jsonMsgMap.getOrDefault(typeTag, murronMsg.getType());

    String id = "";
    if (!idField.isEmpty()) {
      id = (String) jsonMsgMap.getOrDefault(idField, "");
    }
    if (id.isEmpty()) {
      id = murronMsg.getHost() + ":" + murronMsg.getPid() + ":" + murronMsg.getOffset();
    }
    long timestamp = murronMsg.getTimestamp() / 1000;
    long duration =
        Long.parseLong(String.valueOf(jsonMsgMap.getOrDefault(durationField, "1")))
            * durationTimeMuiltiplier;
    String traceId = (String) jsonMsgMap.get(traceIdFieldName);

    return toSpan(
        jsonMsgMap,
        id,
        type,
        timestamp,
        duration,
        Optional.of(murronMsg.getHost()),
        Optional.ofNullable(traceId));
  }

  // TODO: Take duration unit as input.
  // TODO: Take a generic field mapping dictionary as input for fields.
  private static Trace.Span toSpan(
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
    spanBuilder.setTimestamp(timestamp / 1000);
    spanBuilder.setDuration(duration);

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
    host.ifPresent(
        s ->
            tags.add(
                Trace.KeyValue.newBuilder()
                    .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
                    .setVType(Trace.ValueType.STRING)
                    .setVStr(s)
                    .build()));

    spanBuilder.setId(ByteString.copyFrom(id.getBytes()));

    // Replace hyphens with underscore since lucene doesn't like it.
    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.SERVICE_NAME.fieldName)
            .setVType(Trace.ValueType.STRING)
            .setVStr(name.replace("-", "_"))
            .build());

    spanBuilder.addAllTags(tags);
    return spanBuilder.build();
  }
}
