package com.slack.kaldb.writer;

import static com.slack.kaldb.preprocessor.PreprocessorValueMapper.SERVICE_NAME_KEY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.util.JsonUtil;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.opensearch.ingest.IngestDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MurronLogFormatter {
  public static final Logger LOG = LoggerFactory.getLogger(MurronLogFormatter.class);

  public static final String API_LOG_DURATION_FIELD = "microtime_elapsed";
  public static final String ENVOY_DURATION_FIELD = "duration";
  public static final String ENVOY_REQUEST_ID = "request_id";
  public static final String ID = "id";
  protected static final Set<String> nonTagFields =
      ImmutableSet.of(
          LogMessage.ReservedField.PARENT_ID.fieldName,
          LogMessage.ReservedField.TRACE_ID.fieldName,
          API_LOG_DURATION_FIELD);
  private static final String TYPE_TAG = "type";

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

  public static Trace.Span fromIngestDocument(IngestDocument ingestDocument) {
    ZonedDateTime timestamp =
        (ZonedDateTime)
            ingestDocument
                .getIngestMetadata()
                .getOrDefault("timestamp", ZonedDateTime.now(ZoneOffset.UTC));
    // Trace.Span expects duration in microseconds today
    long epochMicro = timestamp.toInstant().toEpochMilli() / 1000;
    Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
    String id = (String) sourceAndMetadata.get(IngestDocument.Metadata.ID.getFieldName());
    String index = (String) sourceAndMetadata.get(IngestDocument.Metadata.INDEX.getFieldName());

    Trace.Span.Builder spanBuilder = Trace.Span.newBuilder();
    spanBuilder.setId(ByteString.copyFrom(id.getBytes()));
    spanBuilder.setTimestamp(epochMicro);

    // Remove the following internal metadata fields that OpenSearch adds
    sourceAndMetadata.remove(IngestDocument.Metadata.ROUTING.getFieldName());
    sourceAndMetadata.remove(IngestDocument.Metadata.VERSION.getFieldName());
    sourceAndMetadata.remove(IngestDocument.Metadata.VERSION_TYPE.getFieldName());
    // these two fields don't need to be tags as they have been explicitly set already
    sourceAndMetadata.remove(IngestDocument.Metadata.ID.getFieldName());
    sourceAndMetadata.remove(IngestDocument.Metadata.INDEX.getFieldName());

    sourceAndMetadata.forEach(
        (key, value) -> spanBuilder.addTags(SpanFormatter.convertKVtoProto(key, value)));
    spanBuilder.addTags(
        Trace.KeyValue.newBuilder()
            .setKey(SERVICE_NAME_KEY)
            .setVType(Trace.ValueType.STRING)
            .setVStr(index)
            .build());
    return spanBuilder.build();
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

    String name = (String) jsonMsgMap.getOrDefault(typeTag, murronMsg.getType());

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

    return SpanFormatter.toSpan(
        jsonMsgMap,
        id,
        name,
        murronMsg.getType(),
        timestamp,
        duration,
        Optional.of(murronMsg.getHost()),
        Optional.ofNullable(traceId));
  }
}
