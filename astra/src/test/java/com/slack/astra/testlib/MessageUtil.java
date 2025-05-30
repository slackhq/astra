package com.slack.astra.testlib;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogWireMessage;
import com.slack.astra.util.JsonUtil;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
  public static final String TEST_SOURCE_BINARY_PROPERTY = "binaryproperty";

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
    fieldMap.put(TEST_SOURCE_BINARY_PROPERTY, String.format("String-%s", i).getBytes());

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
    fieldMap.put(TEST_SOURCE_BINARY_PROPERTY, String.format("String-%s", i).getBytes());

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

  public static LogMessage makeMessage(int i, Instant timestamp, Map<String, Object> properties) {
    return LogMessage.fromWireMessage(makeWireMessage(i, timestamp, properties));
  }

  public static String makeSerializedMessage(int i) {
    try {
      return JsonUtil.writeAsString(makeWireMessage(i));
    } catch (JsonProcessingException j) {
      return null;
    }
  }

  public static String makeSerializedBadMessage(int i) {
    LogWireMessage msg =
        new LogWireMessage(TEST_DATASET_NAME, null, "Message" + i, Instant.now(), null);
    try {
      return JsonUtil.writeAsString(msg);
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  public static List<String> makeSerializedMessages(int low, int high) {
    return IntStream.rangeClosed(low, high)
        .boxed()
        .map(MessageUtil::makeSerializedMessage)
        .collect(Collectors.toList());
  }

  public static List<String> makeSerializedBadMessages(int low, int high) {
    return IntStream.rangeClosed(low, high)
        .boxed()
        .map(MessageUtil::makeSerializedBadMessage)
        .collect(Collectors.toList());
  }

  public static List<LogMessage> makeMessages(int low, int high) {
    return IntStream.rangeClosed(low, high)
        .boxed()
        .map(MessageUtil::makeMessage)
        .collect(Collectors.toList());
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
}
