package com.slack.kaldb.preprocessor;

import static com.slack.kaldb.testlib.SpanUtil.makeSpan;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.google.protobuf.ByteString;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
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

    ValueMapper<byte[], Iterable<Trace.Span>> valueMapper =
        PreprocessorValueMapper.byteArrayToTraceSpans("api_log", null);
    byte[] inputBytes =
        KaldbSerdes.MurronMurronMessage().serializer().serialize(indexName, testMurronMsg);

    Iterable<Trace.Span> spanIterable = valueMapper.apply(inputBytes);
    Iterator<Trace.Span> spanIterator = spanIterable.iterator();
    Trace.Span span = spanIterator.next();

    assertThat(span.getTimestamp()).isEqualTo(timestamp / 1000);
    assertThat(span.getDuration()).isEqualTo(1);

    assertThat(span.getTagsList().size()).isEqualTo(6);
    assertThat(
            span.getTagsList()
                .contains(
                    Trace.KeyValue.newBuilder().setKey("http_method").setVStr("POST").build()))
        .isTrue();
    assertThat(
            span.getTagsList()
                .contains(
                    Trace.KeyValue.newBuilder().setKey("method").setVStr("verifyToken").build()))
        .isTrue();
    assertThat(
            span.getTagsList()
                .contains(Trace.KeyValue.newBuilder().setKey("type").setVStr(indexName).build()))
        .isTrue();
    assertThat(
            span.getTagsList()
                .contains(Trace.KeyValue.newBuilder().setKey("level").setVStr("info").build()))
        .isTrue();
    assertThat(
            span.getTagsList()
                .contains(Trace.KeyValue.newBuilder().setKey("hostname").setVStr(host).build()))
        .isTrue();
    assertThat(
            span.getTagsList()
                .contains(
                    Trace.KeyValue.newBuilder().setKey("service_name").setVStr(indexName).build()))
        .isTrue();
    assertThat(spanIterator.hasNext()).isFalse();
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
    final Trace.Span inputSpan =
        makeSpan(
            traceId, id, parentId, timestampMicros, durationMicros, name, serviceName, msgType);

    final String type = "testIndex";
    final String host = "testHost";
    final Murron.MurronMessage murronMessage =
        Murron.MurronMessage.newBuilder()
            .setMessage(Trace.ListOfSpans.newBuilder().addSpans(inputSpan).build().toByteString())
            .setType(type)
            .setHost(host)
            .setTimestamp(timestampMicros * 1000 + 1)
            .build();

    ValueMapper<byte[], Iterable<Trace.Span>> valueMapper =
        PreprocessorValueMapper.byteArrayToTraceSpans("spans", null);
    byte[] inputBytes =
        KaldbSerdes.MurronMurronMessage().serializer().serialize(serviceName, murronMessage);

    Iterable<Trace.Span> spanIterable = valueMapper.apply(inputBytes);
    Iterator<Trace.Span> spanIterator = spanIterable.iterator();
    Trace.Span mappedSpan = spanIterator.next();

    assertThat(mappedSpan.getTimestamp()).isEqualTo(timestampMicros);
    assertThat(mappedSpan.getDuration()).isEqualTo(durationMicros);

    assertThat(mappedSpan.getTagsList().size()).isEqualTo(8);
    assertThat(
            mappedSpan
                .getTagsList()
                .contains(
                    Trace.KeyValue.newBuilder()
                        .setKey("service_name")
                        .setVStr("test_service")
                        .build()))
        .isTrue();
    assertThat(
            mappedSpan
                .getTagsList()
                .contains(
                    Trace.KeyValue.newBuilder().setKey("http_method").setVStr("POST").build()))
        .isTrue();
    assertThat(
            mappedSpan
                .getTagsList()
                .contains(
                    Trace.KeyValue.newBuilder()
                        .setKey("method")
                        .setVStr("callbacks.flannel")
                        .build()))
        .isTrue();
    assertThat(spanIterator.hasNext()).isFalse();
  }

  @Test
  public void shouldPreventInvalidTransform() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              ValueMapper<byte[], Iterable<Trace.Span>> valueMapper =
                  PreprocessorValueMapper.byteArrayToTraceSpans("invalid", null);
            });
  }

  @Test
  public void shouldExtractServiceName() {
    String service1 = "service1";
    Trace.Span span1 =
        Trace.Span.newBuilder()
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setKey(PreprocessorValueMapper.SERVICE_NAME_KEY)
                    .setVStr(service1)
                    .build())
            .build();

    assertThat(PreprocessorValueMapper.getServiceName(span1)).isEqualTo(service1);

    Trace.Span span2 = Trace.Span.newBuilder().build();
    assertThat(PreprocessorValueMapper.getServiceName(span2)).isNull();
  }
}
