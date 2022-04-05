package com.slack.kaldb.preprocessor;

import static com.slack.kaldb.testlib.SpanUtil.makeSpan;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.Test;

public class PreprocessorValueMapperTest {

  @Test
  public void shouldValueMapApiLog() {
    // Make a test message
    String message =
        "{\"http_method\":\"POST\",\"method\":\"verifyToken\",\"type\":\"api_log\",\"level\":\"info\"}";
    String indexName = "api_log";
    String host = "www-host";
    long timestamp = 1612550512340953000L;
    Murron.MurronMessage testMurronMsg =
        Murron.MurronMessage.newBuilder()
            .setMessage(ByteString.copyFrom(message.getBytes(StandardCharsets.UTF_8)))
            .setType(indexName)
            .setHost(host)
            .setTimestamp(timestamp)
            .build();

    ValueMapper<byte[], Trace.ListOfSpans> valueMapper =
        PreprocessorValueMapper.byteArrayToTraceListOfSpans("api_log");
    byte[] inputBytes =
        KaldbSerdes.MurronMurronMessage().serializer().serialize(indexName, testMurronMsg);

    Trace.ListOfSpans listOfSpans = valueMapper.apply(inputBytes);

    assertThat(listOfSpans.getTagsList().size()).isEqualTo(0);
    assertThat(listOfSpans.getSpansList().size()).isEqualTo(1);

    assertThat(listOfSpans.getSpansList().get(0).getStartTimestampMicros())
        .isEqualTo(timestamp / 1000);
    assertThat(listOfSpans.getSpansList().get(0).getDurationMicros()).isEqualTo(1);

    assertThat(listOfSpans.getSpansList().get(0).getTagsList().size()).isEqualTo(6);
    assertTrue(
        listOfSpans
            .getSpansList()
            .get(0)
            .getTagsList()
            .contains(Trace.KeyValue.newBuilder().setKey("http_method").setVStr("POST").build()));
    assertTrue(
        listOfSpans
            .getSpansList()
            .get(0)
            .getTagsList()
            .contains(Trace.KeyValue.newBuilder().setKey("method").setVStr("verifyToken").build()));
    assertTrue(
        listOfSpans
            .getSpansList()
            .get(0)
            .getTagsList()
            .contains(Trace.KeyValue.newBuilder().setKey("type").setVStr(indexName).build()));
    assertTrue(
        listOfSpans
            .getSpansList()
            .get(0)
            .getTagsList()
            .contains(Trace.KeyValue.newBuilder().setKey("level").setVStr("info").build()));
    assertTrue(
        listOfSpans
            .getSpansList()
            .get(0)
            .getTagsList()
            .contains(Trace.KeyValue.newBuilder().setKey("hostname").setVStr(host).build()));
    assertTrue(
        listOfSpans
            .getSpansList()
            .get(0)
            .getTagsList()
            .contains(
                Trace.KeyValue.newBuilder().setKey("service_name").setVStr(indexName).build()));
  }

  @Test
  public void shouldValueMapSpans() {
    final String traceId = "t1";
    final String id = "i1";
    final String parentId = "p2";
    final long timestampMicros = 1612550512340953L;
    final long durationMicros = 500000L;
    final String serviceName = "test_service";
    final String name = "testSpanName";
    final String msgType = "test_message_type";
    final Trace.Span span =
        makeSpan(
            traceId, id, parentId, timestampMicros, durationMicros, name, serviceName, msgType);

    final String type = "testIndex";
    final String host = "testHost";
    final Murron.MurronMessage murronMessage =
        Murron.MurronMessage.newBuilder()
            .setMessage(Trace.ListOfSpans.newBuilder().addSpans(span).build().toByteString())
            .setType(type)
            .setHost(host)
            .setTimestamp(timestampMicros * 1000 + 1)
            .build();

    ValueMapper<byte[], Trace.ListOfSpans> valueMapper =
        PreprocessorValueMapper.byteArrayToTraceListOfSpans("spans");
    byte[] inputBytes =
        KaldbSerdes.MurronMurronMessage().serializer().serialize(serviceName, murronMessage);

    Trace.ListOfSpans listOfSpans = valueMapper.apply(inputBytes);

    assertThat(listOfSpans.getTagsList().size()).isEqualTo(0);
    assertThat(listOfSpans.getSpansList().size()).isEqualTo(1);

    assertThat(listOfSpans.getSpansList().get(0).getStartTimestampMicros())
        .isEqualTo(timestampMicros);
    assertThat(listOfSpans.getSpansList().get(0).getDurationMicros()).isEqualTo(durationMicros);

    assertThat(listOfSpans.getSpansList().get(0).getTagsList().size()).isEqualTo(8);
    assertTrue(
        listOfSpans
            .getSpansList()
            .get(0)
            .getTagsList()
            .contains(
                Trace.KeyValue.newBuilder()
                    .setKey("service_name")
                    .setVStr("test_service")
                    .build()));
    assertTrue(
        listOfSpans
            .getSpansList()
            .get(0)
            .getTagsList()
            .contains(Trace.KeyValue.newBuilder().setKey("http_method").setVStr("POST").build()));
    assertTrue(
        listOfSpans
            .getSpansList()
            .get(0)
            .getTagsList()
            .contains(
                Trace.KeyValue.newBuilder().setKey("method").setVStr("callbacks.flannel").build()));
  }

  @Test
  public void shouldPreventInvalidTransform() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              ValueMapper<byte[], Trace.ListOfSpans> valueMapper =
                  PreprocessorValueMapper.byteArrayToTraceListOfSpans("invalid");
            });
  }
}
