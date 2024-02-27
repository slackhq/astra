package com.slack.kaldb.testlib;

import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.service.murron.trace.Trace;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MessageUtil {
  // TODO: Add Timer

  public static final String DEFAULT_MESSAGE_PREFIX = "Message";
  public static final String TEST_DATASET_NAME = "testDataSet";
  public static final String TEST_MESSAGE_TYPE = "INFO";
  public static final String TEST_SOURCE_INT_PROPERTY = "intproperty";
  public static final String TEST_SOURCE_LONG_PROPERTY = "longproperty";
  public static final String TEST_SOURCE_DOUBLE_PROPERTY = "doubleproperty";
  public static final String TEST_SOURCE_FLOAT_PROPERTY = "floatproperty";
  public static final String TEST_SOURCE_STRING_PROPERTY = "stringproperty";

  // TODO: Convert message to a Span object.
  public static LogWireMessage makeWireMessage(int i) {
    return makeWireMessage(i, Instant.now(), Map.of());
  }

  public static LogWireMessage makeWireMessage(int i, Map<String, Object> properties) {
    return makeWireMessage(i, Instant.now(), properties);
  }

  public static LogWireMessage makeWireMessage(
      int i, Instant timeStamp, Map<String, Object> properties) {
    String id = DEFAULT_MESSAGE_PREFIX + i;
    Map<String, Object> fieldMap = new HashMap<>();
    String message = String.format("The identifier in this message is %s", id);
    fieldMap.put(LogMessage.ReservedField.MESSAGE.fieldName, message);
    fieldMap.put(TEST_SOURCE_INT_PROPERTY, i);
    fieldMap.put(TEST_SOURCE_LONG_PROPERTY, (long) i);
    fieldMap.put(TEST_SOURCE_DOUBLE_PROPERTY, (double) i);
    fieldMap.put(TEST_SOURCE_FLOAT_PROPERTY, (float) i);
    fieldMap.put(TEST_SOURCE_STRING_PROPERTY, String.format("String-%s", i));

    fieldMap.putAll(properties);

    return new LogWireMessage(TEST_DATASET_NAME, TEST_MESSAGE_TYPE, id, timeStamp, fieldMap);
  }

  public static LogMessage makeMessageWithIndexAndTimestamp(
      int i, String msgStr, String indexName, Instant timeStamp, Map<String, Object> properties) {
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(LogMessage.ReservedField.MESSAGE.fieldName, msgStr);
    fieldMap.put(TEST_SOURCE_INT_PROPERTY, i);
    fieldMap.put(TEST_SOURCE_LONG_PROPERTY, (long) i);
    fieldMap.put(TEST_SOURCE_DOUBLE_PROPERTY, (double) i);
    fieldMap.put(TEST_SOURCE_FLOAT_PROPERTY, (float) i);
    fieldMap.put(TEST_SOURCE_STRING_PROPERTY, String.format("String-%s", i));

    fieldMap.putAll(properties);

    LogWireMessage wireMsg =
        new LogWireMessage(indexName, TEST_MESSAGE_TYPE, Integer.toString(i), timeStamp, fieldMap);
    return LogMessage.fromWireMessage(wireMsg);
  }

  public static LogMessage makeMessageWithIndexAndTimestamp(
      int i, String msgStr, String indexName, Instant timeStamp) {
    return makeMessageWithIndexAndTimestamp(i, msgStr, indexName, timeStamp, Map.of());
  }

  public static LogMessage makeMessage(int i) {
    return LogMessage.fromWireMessage(makeWireMessage(i));
  }

  public static LogMessage makeMessage(int i, Map<String, Object> properties) {
    return LogMessage.fromWireMessage(makeWireMessage(i, properties));
  }

  public static LogMessage makeMessage(int i, Instant timestamp) {
    return LogMessage.fromWireMessage(makeWireMessage(i, timestamp, Map.of()));
  }

  public static List<LogMessage> makeMessagesWithTimeDifference(int low, int high) {
    return makeMessagesWithTimeDifference(low, high, 1);
  }

  public static List<LogMessage> makeMessagesWithTimeDifference(
      int low, int high, long timeDeltaMills) {
    return makeMessagesWithTimeDifference(low, high, timeDeltaMills, Instant.now());
  }

  public static List<LogMessage> makeMessagesWithTimeDifference(
      int low, int high, long timeDeltaMills, Instant start) {
    List<LogMessage> result = new ArrayList<>();
    for (int i = 0; i <= (high - low); i++) {
      result.add(
          MessageUtil.makeMessage(low + i, start.plusNanos(1000 * 1000 * timeDeltaMills * i)));
    }
    return result;
  }

  public static List<Trace.Span> makeMessagesWithTimeDifference1(
      int low, int high, long timeDeltaMills, Instant start) {
    List<Trace.Span> result = new ArrayList<>();
    for (int i = 0; i <= (high - low); i++) {
      String id = DEFAULT_MESSAGE_PREFIX + (low + i);

      Instant timeStamp = start.plusNanos(1000 * 1000 * timeDeltaMills * i);
      String message = String.format("The identifier in this message is %s", id);

      // fieldMap.put(TEST_SOURCE_LONG_PROPERTY, (long) i);
      // fieldMap.put(TEST_SOURCE_FLOAT_PROPERTY, (float) i);
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
                      .setVInt64((low + i))
                      .setKey(TEST_SOURCE_INT_PROPERTY)
                      .setVType(Trace.ValueType.INT64)
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

  public static Trace.Span convertLogMessageToSpan(LogMessage logMessage) {
    Trace.Span.Builder spanBuilder = Trace.Span.newBuilder();
    spanBuilder.setId(ByteString.copyFromUtf8(logMessage.getId()));
    spanBuilder.setTimestamp(
        TimeUnit.MICROSECONDS.convert(
            logMessage.getTimestamp().toEpochMilli(), TimeUnit.MILLISECONDS));
    // TODO
    return spanBuilder.build();
  }

  public static Trace.Span withMessageId(int i) {
    String id = DEFAULT_MESSAGE_PREFIX + i;
    Trace.Span.Builder spanBuilder = Trace.Span.newBuilder();
    spanBuilder.setId(ByteString.copyFromUtf8(id));
    return spanBuilder.build();
  }
}
