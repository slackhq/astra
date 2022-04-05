package com.slack.kaldb.preprocessor;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.google.protobuf.ByteString;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.time.Instant;
import org.apache.kafka.common.serialization.Serde;
import org.junit.Test;

public class KaldbSerdesTest {

  @Test
  public void shouldSerializeAndDeserializeMurronMessage() {
    Serde<Murron.MurronMessage> serdes = KaldbSerdes.MurronMurronMessage();

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
    assertThat(timestamp).isEqualTo(timestamp);
  }

  @Test
  public void shouldSerializeAndDeserializeMurronMessageNullsCorrectly() {
    Serde<Murron.MurronMessage> serdes = KaldbSerdes.MurronMurronMessage();

    byte[] serializedBytes = serdes.serializer().serialize("topic", null);
    assertThat(serializedBytes).isNull();

    Murron.MurronMessage deserializedMessage = serdes.deserializer().deserialize("topic", null);
    assertThat(deserializedMessage).isNull();
  }

  @Test
  public void shouldSerializeAndDeserializeTraceListOfSpans() {
    Serde<Trace.ListOfSpans> serdes = KaldbSerdes.TraceListOfSpans();

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
            .setStartTimestampMicros(timestamp)
            .setDurationMicros(duration)
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
    assertThat(deserializedMessage.getSpansList().get(0).getStartTimestampMicros())
        .isEqualTo(timestamp);
    assertThat(deserializedMessage.getSpansList().get(0).getDurationMicros()).isEqualTo(duration);
    assertThat(deserializedMessage.getSpansList().get(0).getTagsList().size()).isEqualTo(0);
  }

  @Test
  public void shouldSerializeAndDeserializeTraceListOfSpansNullsCorrectly() {
    Serde<Trace.ListOfSpans> serdes = KaldbSerdes.TraceListOfSpans();

    byte[] serializedBytes = serdes.serializer().serialize("topic", null);
    assertThat(serializedBytes).isNull();

    Trace.ListOfSpans deserializedMessage = serdes.deserializer().deserialize("topic", null);
    assertThat(deserializedMessage).isNull();
  }

  @Test
  public void shouldGracefullyHandleWrongMessageTypes() {
    byte[] malformedData = "malformed data".getBytes();

    Trace.ListOfSpans deserializedTraceListOfSpans =
        KaldbSerdes.TraceListOfSpans().deserializer().deserialize("topic", malformedData);
    assertThat(deserializedTraceListOfSpans).isEqualTo(null);

    Murron.MurronMessage deserializedMurronMessage =
        KaldbSerdes.MurronMurronMessage().deserializer().deserialize("topic", malformedData);
    assertThat(deserializedMurronMessage).isEqualTo(null);
  }
}
