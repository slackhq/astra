package com.slack.kaldb.testlib;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.util.JsonUtil;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MessageUtil {
  // TODO: Add Timer

  public static final DateTimeFormatter LogDateFormat =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

  public static final String DEFAULT_MESSAGE_PREFIX = "Message";
  public static final String TEST_INDEX_NAME = "testindex";
  public static final String TEST_MESSAGE_TYPE = "INFO";
  public static final String TEST_SOURCE_INT_PROPERTY = "intproperty";
  public static final String TEST_SOURCE_LONG_PROPERTY = "longproperty";
  public static final String TEST_SOURCE_DOUBLE_PROPERTY = "doubleproperty";
  public static final String TEST_SOURCE_FLOAT_PROPERTY = "floatproperty";

  public static String getCurrentLogDate() {
    return LocalDateTime.now().format(LogDateFormat);
  }

  // TODO: Convert message to a Span object.
  public static LogWireMessage makeWireMessage(int i) {
    return makeWireMessage(i, getCurrentLogDate());
  }

  public static String makeLogMessageJSON(int i) throws JsonProcessingException {
    return makeLogMessageJSON(i, getCurrentLogDate());
  }

  public static String makeLogMessageJSON(int i, String ts) throws JsonProcessingException {
    String id = DEFAULT_MESSAGE_PREFIX + i;
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put("type", TEST_MESSAGE_TYPE);
    fieldMap.put("index", TEST_INDEX_NAME);
    fieldMap.put("id", id);

    Map<String, Object> sourceFieldMap = new HashMap<>();
    sourceFieldMap.put(LogMessage.ReservedField.TIMESTAMP.fieldName, ts);
    String message = String.format("The identifier in this message is %s", id);
    sourceFieldMap.put(LogMessage.ReservedField.MESSAGE.fieldName, message);
    sourceFieldMap.put(TEST_SOURCE_INT_PROPERTY, i);
    sourceFieldMap.put(TEST_SOURCE_LONG_PROPERTY, (long) i);
    sourceFieldMap.put(TEST_SOURCE_DOUBLE_PROPERTY, (double) i);
    sourceFieldMap.put(TEST_SOURCE_FLOAT_PROPERTY, (float) i);
    fieldMap.put("source", sourceFieldMap);

    return new ObjectMapper().writeValueAsString(fieldMap);
  }

  public static LogWireMessage makeWireMessage(int i, String ts) {
    String id = DEFAULT_MESSAGE_PREFIX + i;
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(LogMessage.ReservedField.TIMESTAMP.fieldName, ts);
    String message = String.format("The identifier in this message is %s", id);
    fieldMap.put(LogMessage.ReservedField.MESSAGE.fieldName, message);
    fieldMap.put(TEST_SOURCE_INT_PROPERTY, i);
    fieldMap.put(TEST_SOURCE_LONG_PROPERTY, (long) i);
    fieldMap.put(TEST_SOURCE_DOUBLE_PROPERTY, (double) i);
    fieldMap.put(TEST_SOURCE_FLOAT_PROPERTY, (float) i);
    return new LogWireMessage(TEST_INDEX_NAME, TEST_MESSAGE_TYPE, id, fieldMap);
  }

  public static LogMessage makeMessageWithIndexAndTimestamp(
      int i, String msgStr, String indexName, LocalDateTime timeStamp) {
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(
        LogMessage.ReservedField.TIMESTAMP.fieldName, timeStamp.format(MessageUtil.LogDateFormat));
    fieldMap.put(LogMessage.ReservedField.MESSAGE.fieldName, msgStr);

    LogWireMessage wireMsg =
        new LogWireMessage(indexName, TEST_MESSAGE_TYPE, Integer.toString(i), fieldMap);
    return LogMessage.fromWireMessage(wireMsg);
  }

  public static LogMessage makeMessage(int i) {
    return LogMessage.fromWireMessage(makeWireMessage(i));
  }

  public static LogMessage makeMessage(int i, String ts) {
    return LogMessage.fromWireMessage(makeWireMessage(i, ts));
  }

  public static void addFieldToMessage(LogMessage msg, String key, Object value) {
    msg.source.put(key, value);
  }

  public static String makeSerializedMessage(int i) {
    try {
      return JsonUtil.writeAsString(makeWireMessage(i));
    } catch (JsonProcessingException j) {
      return null;
    }
  }

  public static String makeSerializedBadMessage(int i) {
    LogWireMessage msg = new LogWireMessage(TEST_INDEX_NAME, null, "Message" + i, null);
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
    return makeMessagesWithTimeDifference(low, high, timeDeltaMills, LocalDateTime.now());
  }

  public static List<LogMessage> makeMessagesWithTimeDifference(
      int low, int high, long timeDeltaMills, LocalDateTime start) {
    List<LogMessage> result = new ArrayList<>();
    for (int i = 0; i <= (high - low); i++) {
      result.add(
          MessageUtil.makeMessage(
              low + i, start.plusNanos(1000 * 1000 * timeDeltaMills * i).format(LogDateFormat)));
    }
    return result;
  }

  // TODO: Move this to TestKafkaServer class.
  public int getPort() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    return socket.getLocalPort();
  }
}
