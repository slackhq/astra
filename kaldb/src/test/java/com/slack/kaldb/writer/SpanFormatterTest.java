package com.slack.kaldb.writer;

import static com.slack.kaldb.testlib.SpanUtil.BINARY_TAG_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.testlib.SpanUtil;
import com.slack.service.murron.trace.Trace;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class SpanFormatterTest {
  @Test
  public void testNonRootSpanToLogMessage() {
    final String traceId = "t1";
    final String id = "i2";
    final String parentId = "p2";
    final Instant timestamp = Instant.now();
    final long durationMicros = 500000L;
    final String serviceName = "test_service";
    final String name = "testSpanName";
    final Trace.Span span =
        SpanUtil.makeSpan(
            traceId,
            id,
            parentId,
            TimeUnit.MICROSECONDS.convert(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS),
            durationMicros,
            name,
            serviceName,
            SpanFormatter.DEFAULT_LOG_MESSAGE_TYPE);

    LogMessage logMsg = SpanFormatter.toLogMessage(span);
    assertThat(logMsg.getTimestamp().toEpochMilli()).isEqualTo(timestamp.toEpochMilli());
    assertThat(logMsg.getId()).isEqualTo(id);
    assertThat(logMsg.getType()).isEqualTo("INFO");
    assertThat(logMsg.getIndex()).isEqualTo(serviceName);

    Map<String, Object> source = logMsg.getSource();
    assertThat(source.get(LogMessage.ReservedField.PARENT_ID.fieldName)).isEqualTo(parentId);
    assertThat(source.get(LogMessage.ReservedField.TRACE_ID.fieldName)).isEqualTo(traceId);
    assertThat(source.get(LogMessage.ReservedField.SERVICE_NAME.fieldName)).isEqualTo(serviceName);
    assertThat(source.get(LogMessage.ReservedField.NAME.fieldName)).isEqualTo(name);
    assertThat(source.get(LogMessage.ReservedField.DURATION_MS.fieldName))
        .isEqualTo(Duration.of(durationMicros, ChronoUnit.MICROS).toMillis());
    assertThat(source.get("http_method")).isEqualTo("POST");
    assertThat(source.get("method")).isEqualTo("callbacks.flannel");
    assertThat(source.get("boolean")).isEqualTo(true);
    assertThat(source.get("int")).isEqualTo(1000L);
    assertThat(source.get("float")).isEqualTo(1001.2);
    String binaryTagValue = (String) source.get("binary");
    assertThat(binaryTagValue)
        .isEqualTo(SpanFormatter.encodeBinaryTagValue(ByteString.copyFromUtf8(BINARY_TAG_VALUE)));
    assertThat(new String(Base64.getDecoder().decode(binaryTagValue), StandardCharsets.UTF_8))
        .isEqualTo(BINARY_TAG_VALUE);
  }

  @Test
  public void testRootSpanToLogMessage() {
    final String traceId = "traceid1";
    final String id = "1";
    final Instant timestamp = Instant.now();
    final String msgType = "test_message_type";
    final long durationMicros = 5000L;
    final String serviceName = "test_service";
    final String name = "testSpanName";
    final Trace.Span span =
        SpanUtil.makeSpan(
            traceId,
            id,
            "",
            TimeUnit.MICROSECONDS.convert(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS),
            durationMicros,
            name,
            serviceName,
            msgType);

    LogMessage logMsg = SpanFormatter.toLogMessage(span);
    assertThat(logMsg.getTimestamp().toEpochMilli()).isEqualTo(timestamp.toEpochMilli());
    assertThat(logMsg.getId()).isEqualTo(id);
    assertThat(logMsg.getType()).isEqualTo(msgType);
    assertThat(logMsg.getIndex()).isEqualTo(serviceName);

    Map<String, Object> source = logMsg.getSource();
    assertThat((String) source.get(LogMessage.ReservedField.PARENT_ID.fieldName)).isEmpty();
    assertThat(source.get(LogMessage.ReservedField.TRACE_ID.fieldName)).isEqualTo(traceId);
    assertThat(source.get(LogMessage.ReservedField.SERVICE_NAME.fieldName)).isEqualTo(serviceName);
    assertThat(source.get(LogMessage.ReservedField.NAME.fieldName)).isEqualTo(name);
    assertThat(source.get(LogMessage.ReservedField.DURATION_MS.fieldName))
        .isEqualTo(Duration.of(durationMicros, ChronoUnit.MICROS).toMillis());
    assertThat(source.get("http_method")).isEqualTo("POST");
    assertThat(source.get("method")).isEqualTo("callbacks.flannel");
    assertThat(source.get("boolean")).isEqualTo(true);
    assertThat(source.get("int")).isEqualTo(1000L);
    assertThat(source.get("float")).isEqualTo(1001.2);
    String binaryTagValue = (String) source.get("binary");
    assertThat(binaryTagValue)
        .isEqualTo(SpanFormatter.encodeBinaryTagValue(ByteString.copyFromUtf8(BINARY_TAG_VALUE)));
    assertThat(new String(Base64.getDecoder().decode(binaryTagValue), StandardCharsets.UTF_8))
        .isEqualTo(BINARY_TAG_VALUE);
  }

  @Test
  public void testListOfSpansConversion() {
    final String traceId = "t1";
    final String id = "i1";
    final String id2 = "i2";
    final Instant timestamp = Instant.now();
    final long durationMicros = 500000L;
    final String serviceName = "test_service";
    final String name = "testSpanName";
    final Trace.Span span1 =
        SpanUtil.makeSpan(
            traceId,
            id,
            "",
            TimeUnit.MICROSECONDS.convert(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS),
            durationMicros,
            name,
            serviceName,
            SpanFormatter.DEFAULT_LOG_MESSAGE_TYPE);
    final Trace.Span span2 =
        SpanUtil.makeSpan(
            traceId,
            id2,
            id,
            TimeUnit.MICROSECONDS.convert(timestamp.toEpochMilli() + 1000, TimeUnit.MILLISECONDS),
            durationMicros,
            name + "2",
            serviceName,
            SpanFormatter.DEFAULT_LOG_MESSAGE_TYPE);

    // Test empty list
    assertThat(
            SpanFormatter.toLogMessage(
                    Trace.ListOfSpans.newBuilder().addAllSpans(Collections.emptyList()).build())
                .size())
        .isZero();

    // List with 1 span.
    assertThat(
            SpanFormatter.toLogMessage(
                    Trace.ListOfSpans.newBuilder().addAllSpans(List.of(span1)).build())
                .size())
        .isEqualTo(1);

    // List with 2 spans
    List<LogMessage> logMessages =
        SpanFormatter.toLogMessage(
            Trace.ListOfSpans.newBuilder().addAllSpans(List.of(span1, span2)).build());
    assertThat(logMessages.size()).isEqualTo(2);

    for (LogMessage logMsg : logMessages) {
      assertThat(logMsg.getTimestamp().toEpochMilli())
          .isIn(timestamp.toEpochMilli(), timestamp.toEpochMilli() + 1000);
      assertThat(logMsg.getId()).isIn(id, id2);
      assertThat(logMsg.getType()).isEqualTo("INFO");
      assertThat(logMsg.getIndex()).isEqualTo(serviceName);
      Map<String, Object> source = logMsg.getSource();
      assertThat(source.get(LogMessage.ReservedField.PARENT_ID.fieldName)).isIn(id, "");
      assertThat(source.get(LogMessage.ReservedField.TRACE_ID.fieldName)).isEqualTo(traceId);
      assertThat(source.get(LogMessage.ReservedField.SERVICE_NAME.fieldName))
          .isEqualTo(serviceName);
      assertThat((String) source.get(LogMessage.ReservedField.NAME.fieldName)).startsWith(name);
      assertThat(source.get(LogMessage.ReservedField.DURATION_MS.fieldName))
          .isEqualTo(Duration.of(durationMicros, ChronoUnit.MICROS).toMillis());
      assertThat(source.get("http_method")).isEqualTo("POST");
      assertThat(source.get("method")).isEqualTo("callbacks.flannel");
      assertThat(source.get("boolean")).isEqualTo(true);
      assertThat(source.get("int")).isEqualTo(1000L);
      assertThat(source.get("float")).isEqualTo(1001.2);
      String binaryTagValue = (String) source.get("binary");
      assertThat(binaryTagValue)
          .isEqualTo(SpanFormatter.encodeBinaryTagValue(ByteString.copyFromUtf8(BINARY_TAG_VALUE)));
      assertThat(new String(Base64.getDecoder().decode(binaryTagValue), StandardCharsets.UTF_8))
          .isEqualTo(BINARY_TAG_VALUE);
    }
  }

  @Test
  public void testEmptyTimestamp() {
    final Trace.Span span =
        SpanUtil.makeSpan("", "", "", 0, 0, "", "", SpanFormatter.DEFAULT_LOG_MESSAGE_TYPE);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> SpanFormatter.toLogMessage(span));
  }

  @Test
  public void testSpanWithoutKeyFieldsToLogMessage() {
    Instant timestamp = Instant.now();
    final Trace.Span span =
        SpanUtil.makeSpan(
            "",
            "",
            "",
            TimeUnit.MICROSECONDS.convert(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS),
            0,
            "",
            "",
            SpanFormatter.DEFAULT_LOG_MESSAGE_TYPE);

    LogMessage logMsg = SpanFormatter.toLogMessage(span);
    // we convert any time by 1000 in SpanFormatter#toLogMessage
    assertThat(logMsg.getTimestamp().toEpochMilli()).isEqualTo(timestamp.toEpochMilli());
    assertThat(logMsg.getId()).isEmpty();
    assertThat(logMsg.getType()).isEqualTo("INFO");
    assertThat(logMsg.getIndex()).isEqualTo(SpanFormatter.DEFAULT_INDEX_NAME);

    Map<String, Object> source = logMsg.getSource();
    assertThat((String) source.get(LogMessage.ReservedField.PARENT_ID.fieldName)).isEmpty();
    assertThat((String) source.get(LogMessage.ReservedField.TRACE_ID.fieldName)).isEmpty();
    assertThat(source.get(LogMessage.ReservedField.SERVICE_NAME.fieldName))
        .isEqualTo(SpanFormatter.DEFAULT_INDEX_NAME);
    assertThat((String) source.get(LogMessage.ReservedField.NAME.fieldName)).isEmpty();
    assertThat((long) source.get(LogMessage.ReservedField.DURATION_MS.fieldName)).isZero();
    assertThat(source.get("http_method")).isEqualTo("POST");
    assertThat(source.get("method")).isEqualTo("callbacks.flannel");
    assertThat(source.get("boolean")).isEqualTo(true);
    assertThat(source.get("int")).isEqualTo(1000L);
    assertThat(source.get("float")).isEqualTo(1001.2);
    String binaryTagValue = (String) source.get("binary");
    assertThat(binaryTagValue)
        .isEqualTo(SpanFormatter.encodeBinaryTagValue(ByteString.copyFromUtf8(BINARY_TAG_VALUE)));
    assertThat(new String(Base64.getDecoder().decode(binaryTagValue), StandardCharsets.UTF_8))
        .isEqualTo(BINARY_TAG_VALUE);
  }

  @Test
  public void testValidateTimestamp() {
    Instant currentTime = Instant.now();

    assertThatThrownBy(
            () -> SpanFormatter.validateTimestamp("1", Instant.ofEpochMilli(0), currentTime))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(
            () ->
                SpanFormatter.validateTimestamp(
                    "1", currentTime.plus(61, ChronoUnit.MINUTES), currentTime))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatNoException()
        .isThrownBy(
            () ->
                SpanFormatter.validateTimestamp(
                    "1", currentTime.plus(60, ChronoUnit.MINUTES), currentTime));
    assertThatNoException()
        .isThrownBy(
            () ->
                SpanFormatter.validateTimestamp(
                    "1", currentTime.plus(59, ChronoUnit.MINUTES), currentTime));

    assertThatNoException()
        .isThrownBy(
            () ->
                SpanFormatter.validateTimestamp(
                    "1", currentTime.minus(721, ChronoUnit.MINUTES), currentTime));
    assertThatNoException()
        .isThrownBy(
            () ->
                SpanFormatter.validateTimestamp(
                    "1", currentTime.minus(720, ChronoUnit.MINUTES), currentTime));
    assertThatNoException()
        .isThrownBy(
            () ->
                SpanFormatter.validateTimestamp(
                    "1", currentTime.minus(719, ChronoUnit.MINUTES), currentTime));
  }
}
