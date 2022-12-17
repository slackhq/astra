package com.slack.kaldb.testlib;

import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.service.murron.trace.Trace;
import java.util.ArrayList;
import java.util.List;

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

  public static Trace.Span.Builder makeSpanBuilder(
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

    if (!msgType.isEmpty() && msgType != null) {
      tags.add(
          Trace.KeyValue.newBuilder()
              .setKey(LogMessage.ReservedField.TYPE.fieldName)
              .setVTypeValue(Trace.ValueType.STRING.getNumber())
              .setVStr(msgType)
              .build());
    }

    spanBuilder.addAllTags(tags);
    return spanBuilder;
  }
}
