package com.slack.kaldb.testlib;

import static com.slack.kaldb.testlib.MessageUtil.DEFAULT_MESSAGE_PREFIX;
import static com.slack.kaldb.testlib.MessageUtil.TEST_SOURCE_DOUBLE_PROPERTY;
import static com.slack.kaldb.testlib.MessageUtil.TEST_SOURCE_FLOAT_PROPERTY;
import static com.slack.kaldb.testlib.MessageUtil.TEST_SOURCE_INT_PROPERTY;
import static com.slack.kaldb.testlib.MessageUtil.TEST_SOURCE_LONG_PROPERTY;
import static com.slack.kaldb.testlib.MessageUtil.TEST_SOURCE_STRING_PROPERTY;

import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.service.murron.trace.Trace;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SpanUtil {

  public static final String BINARY_TAG_VALUE = "binaryTagValue";

  public static Trace.Span makeSpan(
      String traceId,
      String id,
      String parentId,
      long timestampMicros,
      long durationMicros,
      String name,
      String serviceName,
      String msgType) {
    Trace.Span.Builder spanBuilder =
        makeSpanBuilder(
            traceId, id, parentId, timestampMicros, durationMicros, name, serviceName, msgType);
    return spanBuilder.build();
  }

  private static Trace.Span.Builder makeSpanBuilder(
      String traceId,
      String id,
      String parentId,
      long timestampMicros,
      long durationMicros,
      String name,
      String serviceName,
      String msgType) {
    Trace.Span.Builder spanBuilder = Trace.Span.newBuilder();
    spanBuilder.setTraceId(ByteString.copyFrom(traceId.getBytes()));
    spanBuilder.setId(ByteString.copyFrom(id.getBytes()));
    spanBuilder.setParentId(ByteString.copyFrom(parentId.getBytes()));
    spanBuilder.setTimestamp(timestampMicros);
    spanBuilder.setDuration(durationMicros);
    spanBuilder.setName(name);

    List<Trace.KeyValue> tags = new ArrayList<>();
    // Set service tag
    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.SERVICE_NAME.fieldName)
            .setVTypeValue(Trace.ValueType.STRING.getNumber())
            .setVStr(serviceName)
            .build());

    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey("http_method")
            .setVTypeValue(Trace.ValueType.STRING.getNumber())
            .setVStr("POST")
            .build());

    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey("method")
            .setVTypeValue(Trace.ValueType.STRING.getNumber())
            .setVStr("callbacks.flannel")
            .build());

    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey("boolean")
            .setVTypeValue(Trace.ValueType.BOOL.getNumber())
            .setVBool(true)
            .build());

    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey("int")
            .setVTypeValue(Trace.ValueType.INT64.getNumber())
            .setVInt64(1000)
            .setVFloat64(1001.2)
            .build());

    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey("float")
            .setVTypeValue(Trace.ValueType.FLOAT64.getNumber())
            .setVFloat64(1001.2)
            .setVInt64(1000)
            .build());

    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey("binary")
            .setVTypeValue(Trace.ValueType.BINARY.getNumber())
            .setVBinary(ByteString.copyFromUtf8(BINARY_TAG_VALUE))
            .setVStr("ignored")
            .build());

    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.TYPE.fieldName)
            .setVTypeValue(Trace.ValueType.STRING.getNumber())
            .setVStr(msgType)
            .build());

    spanBuilder.addAllTags(tags);
    return spanBuilder;
  }

  public static List<Trace.Span> makeSpansWithTimeDifference(
      int low, int high, long timeDeltaMills, Instant start) {
    List<Trace.Span> result = new ArrayList<>();
    for (int i = 0; i <= (high - low); i++) {
      String id = DEFAULT_MESSAGE_PREFIX + (low + i);

      Instant timeStamp = start.plusNanos(1000 * 1000 * timeDeltaMills * i);
      String message = String.format("The identifier in this message is %s", id);

      Trace.Span span =
          Trace.Span.newBuilder()
              .setTimestamp(
                  TimeUnit.MICROSECONDS.convert(timeStamp.toEpochMilli(), TimeUnit.MILLISECONDS))
              .setId(ByteString.copyFromUtf8(id))
              .addTags(
                  Trace.KeyValue.newBuilder()
                      .setVStr(message)
                      .setKey("message")
                      .setVType(Trace.ValueType.STRING)
                      .build())
              .addTags(
                  Trace.KeyValue.newBuilder()
                      .setVInt32((low + i))
                      .setKey(TEST_SOURCE_INT_PROPERTY)
                      .setVType(Trace.ValueType.INT32)
                      .build())
              .addTags(
                  Trace.KeyValue.newBuilder()
                      .setVInt64((low + i))
                      .setKey(TEST_SOURCE_LONG_PROPERTY)
                      .setVType(Trace.ValueType.INT64)
                      .build())
              .addTags(
                  Trace.KeyValue.newBuilder()
                      .setVFloat32((low + i))
                      .setKey(TEST_SOURCE_FLOAT_PROPERTY)
                      .setVType(Trace.ValueType.FLOAT32)
                      .build())
              .addTags(
                  Trace.KeyValue.newBuilder()
                      .setVFloat64((low + i))
                      .setKey(TEST_SOURCE_DOUBLE_PROPERTY)
                      .setVType(Trace.ValueType.FLOAT64)
                      .build())
              .addTags(
                  Trace.KeyValue.newBuilder()
                      .setVStr(String.format("String-%s", (low + i)))
                      .setKey(TEST_SOURCE_STRING_PROPERTY)
                      .setVType(Trace.ValueType.STRING)
                      .build())
              .build();

      result.add(span);
    }
    return result;
  }
}
