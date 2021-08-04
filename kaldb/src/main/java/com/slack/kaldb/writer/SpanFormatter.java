package com.slack.kaldb.writer;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A utility class that converts a Span into a LogMessage. */
public class SpanFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(SpanFormatter.class);

  public static final String DEFAULT_LOG_MESSAGE_TYPE = "INFO";
  public static final String DEFAULT_INDEX_NAME = "unknown";

  public static Trace.ListOfSpans fromMurronMessage(Murron.MurronMessage message)
      throws InvalidProtocolBufferException {
    return Trace.ListOfSpans.parseFrom(message.getMessage());
  }

  public static String encodeBinaryTagValue(ByteString binaryTagValue) {
    return Base64.getEncoder().encodeToString(binaryTagValue.toByteArray());
  }

  // TODO: Make this function more memory efficient?
  public static LogMessage toLogMessage(Trace.Span span) {
    if (span == null) return null;

    Map<String, Object> jsonMap = new HashMap<>();

    String id = span.getId().toStringUtf8();

    // Set these fields even if they are empty so we can always search these fields.
    jsonMap.put(LogMessage.ReservedField.PARENT_ID.fieldName, span.getParentId().toStringUtf8());
    jsonMap.put(LogMessage.ReservedField.TRACE_ID.fieldName, span.getTraceId().toStringUtf8());
    jsonMap.put(LogMessage.ReservedField.NAME.fieldName, span.getName());
    jsonMap.put(LogMessage.ReservedField.DURATION_MS.fieldName, span.getDurationMicros());

    // TODO: Use a microsecond resolution, instead of millisecond resolution.
    Instant timestamp = Instant.ofEpochMilli(span.getStartTimestampMicros() / (1000));
    jsonMap.put(LogMessage.ReservedField.TIMESTAMP.fieldName, timestamp.toString());

    String indexName = "";
    String msgType = DEFAULT_LOG_MESSAGE_TYPE;
    for (Trace.KeyValue tag : span.getTagsList()) {
      String key = tag.getKey();
      int valueType = tag.getVType().getNumber();
      if (valueType == 0) {
        jsonMap.put(key, tag.getVStr());
        if (key.equals(LogMessage.ReservedField.SERVICE_NAME.fieldName)) {
          indexName = tag.getVStr();
          // Also, add service name to the map so can search by service name also.
          jsonMap.put(LogMessage.ReservedField.SERVICE_NAME.fieldName, indexName);
        }
        if (key.equals(LogMessage.SystemField.TYPE.fieldName)) {
          msgType = tag.getVStr();
        }
      } else if (valueType == 1) {
        jsonMap.put(key, tag.getVBool());
      } else if (valueType == 2) {
        jsonMap.put(key, tag.getVInt64());
      } else if (valueType == 3) {
        jsonMap.put(key, tag.getVFloat64());
      } else if (valueType == 4) {
        jsonMap.put(key, encodeBinaryTagValue(tag.getVBinary()));
      } else {
        LOG.warn("Skipping field with unknown value type {} with key {}", valueType, key);
      }
    }

    if (indexName.isEmpty()) {
      indexName = DEFAULT_INDEX_NAME;
    }
    jsonMap.put(LogMessage.ReservedField.SERVICE_NAME.fieldName, indexName);

    //  This logging is in place to debug span parsing exceptions. Once this pipeline
    //  is more stable remove this code.
    // try {
    //      return LogMessage.fromWireMessage(new LogWireMessage(indexName, msgType, id, jsonMap));
    //    } catch (Exception e) {
    //      try {
    //        LOG.info(
    //            "span conversion failed: "
    //                + JsonFormat.printer()
    //                    .includingDefaultValueFields()
    //                    .omittingInsignificantWhitespace()
    //                    .print(span));
    //      } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
    //        invalidProtocolBufferException.printStackTrace();
    //      }
    //      throw e;
    //    }

    // Drop the type field from LogMessage since with spans it doesn't make sense.
    return LogMessage.fromWireMessage(new LogWireMessage(indexName, msgType, id, jsonMap));
  }

  // TODO: For now assuming that the tags in ListOfSpans is empty. Handle this case in future.
  public static List<LogMessage> toLogMessage(Trace.ListOfSpans protoSpans) {
    if (protoSpans == null) return Collections.EMPTY_LIST;

    List<Trace.Span> spans = protoSpans.getSpansList();
    List<LogMessage> messages = new ArrayList<>(spans.size());
    for (Trace.Span span : spans) {
      messages.add(toLogMessage(span));
    }
    return messages;
  }
}
