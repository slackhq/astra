package com.slack.astra.writer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.util.JsonUtil;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/*
   Utility classes to take a byte array with a Json payload and converts to a Trace.Span.
   If the Json does not have an "id" we generate a random UUID.
   The payload Json is expected to contain a "service_name" and a "@timestamp" field otherwise the document will fail to index
*/
public class JsonLogFormatter {

  public static Trace.Span fromJsonLog(byte[] data) throws IOException {
    TypeReference<Map<String, Object>> mapTypeRef = new TypeReference<>() {};
    Map<String, Object> jsonMsgMap = JsonUtil.read(data, mapTypeRef);

    String id =
        (String) jsonMsgMap.getOrDefault(MurronLogFormatter.ID, UUID.randomUUID().toString());

    String serviceName =
        (String) jsonMsgMap.getOrDefault(LogMessage.ReservedField.SERVICE_NAME.fieldName, "");
    if (serviceName == null || serviceName.isEmpty()) {
      throw new IllegalArgumentException("Document must contain service_name key");
    }
    String name =
        (String) jsonMsgMap.getOrDefault(LogMessage.ReservedField.NAME.fieldName, serviceName);

    long duration =
        Long.parseLong(
            String.valueOf(
                jsonMsgMap.getOrDefault(LogMessage.ReservedField.DURATION_MS.fieldName, "1")));

    String dateStr = (String) jsonMsgMap.getOrDefault("@timestamp", "");
    if (dateStr == null || dateStr.isEmpty()) {
      throw new IllegalArgumentException("Document must contain timestamp key");
    }
    long timestamp = Instant.parse(dateStr).toEpochMilli();

    String traceId = (String) jsonMsgMap.get(LogMessage.ReservedField.TRACE_ID.fieldName);
    String host = (String) jsonMsgMap.get(LogMessage.ReservedField.HOSTNAME.fieldName);

    return SpanFormatter.toSpan(
        jsonMsgMap,
        id,
        name,
        name,
        timestamp,
        duration,
        Optional.ofNullable(host),
        Optional.ofNullable(traceId));
  }
}
