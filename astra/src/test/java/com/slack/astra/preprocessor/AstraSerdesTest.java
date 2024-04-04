package com.slack.astra.preprocessor;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.time.Instant;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;

public class AstraSerdesTest {

  @Test
  public void shouldSerializeAndDeserializeMurronMessage() {
    Serde<Murron.MurronMessage> serdes = AstraSerdes.MurronMurronMessage();

    String topic = "topic";
    String message = "hello message";
    String host = "host";
    String type = "type";
    long timestamp = Instant.now().toEpochMilli();

    Murron.MurronMessage messageToSerialize =
        Murron.MurronMessage.newBuilder()
            .setMessage(ByteString.copyFromUtf8(message))
            .setHost(host)
            .setType(type)
            .setTimestamp(timestamp)
            .build();

    byte[] serializedBytes = serdes.serializer().serialize(topic, messageToSerialize);
    Murron.MurronMessage deserializedMessage =
        serdes.deserializer().deserialize(topic, serializedBytes);

    assertThat(deserializedMessage.getMessage()).isEqualTo(ByteString.copyFromUtf8(message));
    assertThat(deserializedMessage.getHost()).isEqualTo(host);
    assertThat(deserializedMessage.getType()).isEqualTo(type);
    assertThat(deserializedMessage.getTimestamp()).isEqualTo(timestamp);
  }

  @Test
  public void shouldSerializeAndDeserializeMurronMessageNullsCorrectly() {
    Serde<Murron.MurronMessage> serdes = AstraSerdes.MurronMurronMessage();

    byte[] serializedBytes = serdes.serializer().serialize("topic", null);
    assertThat(serializedBytes).isNull();

    Murron.MurronMessage deserializedMessage = serdes.deserializer().deserialize("topic", null);
    assertThat(deserializedMessage).isNull();
  }

  @Test
  public void shouldSerializeAndDeserializeTraceListOfSpans() {
    Serde<Trace.ListOfSpans> serdes = AstraSerdes.TraceListOfSpans();

    String topic = "topic";
    String id = "id";
    String traceId = "traceId";
    String name = "name";
    long timestamp = Instant.now().toEpochMilli() * 1000;
    long duration = 10;
    Trace.Span span =
        Trace.Span.newBuilder()
            .setId(ByteString.copyFromUtf8(id))
            .setTraceId(ByteString.copyFromUtf8(traceId))
            .setName(name)
            .setTimestamp(timestamp)
            .setDuration(duration)
            .build();
    Trace.ListOfSpans listOfSpans = Trace.ListOfSpans.newBuilder().addSpans(span).build();

    byte[] serializedBytes = serdes.serializer().serialize(topic, listOfSpans);
    Trace.ListOfSpans deserializedMessage =
        serdes.deserializer().deserialize(topic, serializedBytes);

    assertThat(deserializedMessage.getSpansList().size()).isEqualTo(1);
    assertThat(deserializedMessage.getTagsList().size()).isEqualTo(0);

    assertThat(deserializedMessage.getSpansList().get(0).getId())
        .isEqualTo(ByteString.copyFromUtf8(id));
    assertThat(deserializedMessage.getSpansList().get(0).getTraceId())
        .isEqualTo(ByteString.copyFromUtf8(traceId));
    assertThat(deserializedMessage.getSpansList().get(0).getName()).isEqualTo(name);
    assertThat(deserializedMessage.getSpansList().get(0).getTimestamp()).isEqualTo(timestamp);
    assertThat(deserializedMessage.getSpansList().get(0).getDuration()).isEqualTo(duration);
    assertThat(deserializedMessage.getSpansList().get(0).getTagsList().size()).isEqualTo(0);
  }

  @Test
  public void shouldSerializeAndDeserializeTraceListOfSpansNullsCorrectly() {
    Serde<Trace.ListOfSpans> serdes = AstraSerdes.TraceListOfSpans();

    byte[] serializedBytes = serdes.serializer().serialize("topic", null);
    assertThat(serializedBytes).isNull();

    Trace.ListOfSpans deserializedMessage = serdes.deserializer().deserialize("topic", null);
    assertThat(deserializedMessage).isNull();
  }

  @Test
  public void shouldSerializeAndDeserializeTraceSpan() {
    Serde<Trace.Span> serdes = AstraSerdes.TraceSpan();

    String topic = "topic";
    String id = "id";
    String traceId = "traceId";
    String name = "name";
    long timestamp = Instant.now().toEpochMilli() * 1000;
    long duration = 10;
    Trace.Span span =
        Trace.Span.newBuilder()
            .setId(ByteString.copyFromUtf8(id))
            .setTraceId(ByteString.copyFromUtf8(traceId))
            .setName(name)
            .setTimestamp(timestamp)
            .setDuration(duration)
            .build();

    byte[] serializedBytes = serdes.serializer().serialize(topic, span);
    Trace.Span deserializedMessage = serdes.deserializer().deserialize(topic, serializedBytes);

    assertThat(deserializedMessage.getId()).isEqualTo(ByteString.copyFromUtf8(id));
    assertThat(deserializedMessage.getTraceId()).isEqualTo(ByteString.copyFromUtf8(traceId));
    assertThat(deserializedMessage.getName()).isEqualTo(name);
    assertThat(deserializedMessage.getTimestamp()).isEqualTo(timestamp);
    assertThat(deserializedMessage.getDuration()).isEqualTo(duration);
    assertThat(deserializedMessage.getTagsList().size()).isEqualTo(0);
  }

  @Test
  public void shouldSerializeAndDeserializeTraceSpanNullsCorrectly() {
    Serde<Trace.Span> serdes = AstraSerdes.TraceSpan();

    byte[] serializedBytes = serdes.serializer().serialize("topic", null);
    assertThat(serializedBytes).isNull();

    Trace.Span deserializedMessage = serdes.deserializer().deserialize("topic", null);
    assertThat(deserializedMessage).isNull();
  }

  @Test
  public void shouldGracefullyHandleWrongMessageTypes() {
    byte[] malformedData = "malformed data".getBytes();

    Trace.Span deserializedTraceSpan =
        AstraSerdes.TraceSpan().deserializer().deserialize("topic", malformedData);
    assertThat(deserializedTraceSpan).isEqualTo(null);

    Trace.ListOfSpans deserializedTraceListOfSpans =
        AstraSerdes.TraceListOfSpans().deserializer().deserialize("topic", malformedData);
    assertThat(deserializedTraceListOfSpans).isEqualTo(null);

    Murron.MurronMessage deserializedMurronMessage =
        AstraSerdes.MurronMurronMessage().deserializer().deserialize("topic", malformedData);
    assertThat(deserializedMurronMessage).isEqualTo(null);
  }
}
