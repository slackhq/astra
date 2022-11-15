package com.slack.kaldb.preprocessor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.slack.kaldb.writer.MurronLogFormatter;
import com.slack.kaldb.writer.SpanFormatter;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaStream value mappers for transforming upstream byte array messages into Iterable<Trace.Span>
 */
public class PreprocessorValueMapper {
  private static final Logger LOG = LoggerFactory.getLogger(PreprocessorValueMapper.class);

  private static Deserializer<Murron.MurronMessage> murronMessageDeserializer =
      KaldbSerdes.MurronMurronMessage().deserializer();

  @FunctionalInterface
  private interface MessageTransformer {
    List<Trace.Span> toTraceSpans(byte[] record) throws Exception;
  }

  // An apiLog message is a json blob wrapped in a murron message.
  public static final MessageTransformer apiLogTransformer =
      record -> {
        final Murron.MurronMessage murronMsg = murronMessageDeserializer.deserialize("", record);
        return List.of(MurronLogFormatter.fromApiLog(murronMsg));
      };

  // An envoy log message is a json blob wrapped in a murron message.
  public static final MessageTransformer envoyLogTransformer =
      record -> {
        final Murron.MurronMessage murronMsg = murronMessageDeserializer.deserialize("", record);
        return List.of(MurronLogFormatter.fromEnvoyLog(murronMsg));
      };

  // A single trace record consists of a list of spans wrapped in a murron message.
  public static final MessageTransformer spanTransformer =
      record -> {
        Murron.MurronMessage murronMsg = murronMessageDeserializer.deserialize("", record);
        return SpanFormatter.fromMurronMessage(murronMsg).getSpansList();
      };

  // todo - add a json blob transformer, ie LogMessageWriterImpl.jsonLogMessageTransformer

  private static final Map<String, MessageTransformer> PRE_PROCESSOR_DATA_TRANSFORMER_MAP =
      ImmutableMap.of(
          "api_log", apiLogTransformer, "spans", spanTransformer, "envoy_log", envoyLogTransformer);

  /** Span key for KeyValue pair to use as the service name */
  public static String SERVICE_NAME_KEY = "service_name";

  /**
   * Helper method to extract the service name from a Span
   *
   * <p>todo - consider putting the service name into a top-level Trace.Span property
   */
  public static String getServiceName(Trace.Span span) {
    return span.getTagsList()
        .stream()
        .filter(tag -> tag.getKey().equals(SERVICE_NAME_KEY))
        .map(Trace.KeyValue::getVStr)
        .findFirst()
        .orElse(null);
  }

  /** KafkaStream ValueMapper for transforming upstream sources to target Trace.ListOfSpans */
  public static ValueMapper<byte[], Iterable<Trace.Span>> byteArrayToTraceSpans(
      String dataTransformer) {
    Preconditions.checkArgument(
        PRE_PROCESSOR_DATA_TRANSFORMER_MAP.containsKey(dataTransformer),
        "Invalid data transformer provided, must be one of %s",
        PRE_PROCESSOR_DATA_TRANSFORMER_MAP.toString());
    return messageBytes -> {
      try {
        return PRE_PROCESSOR_DATA_TRANSFORMER_MAP.get(dataTransformer).toTraceSpans(messageBytes);
      } catch (Exception e) {
        LOG.error("Error converting byte array to Trace.ListOfSpans", e);
        return Collections.emptyList();
      }
    };
  }
}
