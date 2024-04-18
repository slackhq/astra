package com.slack.astra.zipkinApi;

import static com.slack.astra.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.astra.testlib.MessageUtil.TEST_MESSAGE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogWireMessage;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class ZipkinServiceSpanConversionTest {
  private static LogWireMessage makeWireMessageForSpans(
      String id,
      Instant ts,
      String traceId,
      Optional<String> parentId,
      long duration,
      String serviceName,
      String name) {
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(LogMessage.ReservedField.TRACE_ID.fieldName, traceId);
    fieldMap.put(LogMessage.ReservedField.SERVICE_NAME.fieldName, serviceName);
    fieldMap.put(LogMessage.ReservedField.NAME.fieldName, name);
    parentId.ifPresent(s -> fieldMap.put(LogMessage.ReservedField.PARENT_ID.fieldName, s));
    fieldMap.put(LogMessage.ReservedField.DURATION.fieldName, duration);
    return new LogWireMessage(TEST_DATASET_NAME, TEST_MESSAGE_TYPE, id, ts, fieldMap);
  }

  private static List<LogWireMessage> generateLogWireMessagesForOneTrace(
      Instant time, int count, String traceId) {
    List<LogWireMessage> messages = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      String parentId = null;
      if (i > 1) {
        parentId = String.valueOf(i - 1);
      }
      messages.add(
          makeWireMessageForSpans(
              String.valueOf(i),
              time.plusSeconds(i),
              traceId,
              Optional.ofNullable(parentId),
              i,
              "service1",
              ("Trace" + i)));
    }
    return messages;
  }

  @Test
  public void testLogWireMessageToZipkinSpanConversion() throws JsonProcessingException {
    Instant time = Instant.now();
    List<LogWireMessage> messages = generateLogWireMessagesForOneTrace(time, 2, "1");

    // follows output format from https://zipkin.io/zipkin-api/#/default/get_trace__traceId_
    String output =
        String.format(
            "[{\"duration\":1,\"id\":\"1\",\"name\":\"Trace1\",\"remoteEndpoint\":{\"serviceName\":\"service1\"},\"timestamp\":%d,\"traceId\":\"1\"},{\"duration\":2,\"id\":\"2\",\"name\":\"Trace2\",\"parentId\":\"1\",\"remoteEndpoint\":{\"serviceName\":\"service1\"},\"timestamp\":%d,\"traceId\":\"1\"}]",
            ZipkinService.convertToMicroSeconds(time.plusSeconds(1)),
            ZipkinService.convertToMicroSeconds(time.plusSeconds(2)));
    assertThat(ZipkinService.convertLogWireMessageToZipkinSpan(messages)).isEqualTo(output);

    assertThat(ZipkinService.convertLogWireMessageToZipkinSpan(new ArrayList<>())).isEqualTo("[]");
  }
}
