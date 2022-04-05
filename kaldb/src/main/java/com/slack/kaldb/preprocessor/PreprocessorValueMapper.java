package com.slack.kaldb.preprocessor;

import static com.slack.kaldb.writer.LogMessageWriterImpl.toMurronMessage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.slack.kaldb.writer.ApiLogFormatter;
import com.slack.kaldb.writer.SpanFormatter;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.util.Map;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaStream value mappers for transforming upstream byte array messages into Trace.ListOfSpans.
 */
public class PreprocessorValueMapper {
  private static final Logger LOG = LoggerFactory.getLogger(PreprocessorValueMapper.class);

  @FunctionalInterface
  private interface MessageTransformer {
    Trace.ListOfSpans toTraceSpan(byte[] record) throws Exception;
  }

  // An apiLog message is a json blob wrapped in a murron message.
  public static final MessageTransformer apiLogTransformer =
      record -> {
        final Murron.MurronMessage murronMsg = toMurronMessage(record);
        Trace.Span apiSpan = ApiLogFormatter.toSpan(murronMsg);
        return Trace.ListOfSpans.newBuilder().addSpans(apiSpan).build();
      };

  // A single trace record consists of a list of spans wrapped in a murron message.
  public static final MessageTransformer spanTransformer =
      record -> {
        Murron.MurronMessage murronMsg = toMurronMessage(record);
        return SpanFormatter.fromMurronMessage(murronMsg);
      };

  // todo - add a json blob transformer, ie LogMessageWriterImpl.jsonLogMessageTransformer

  private static final Map<String, MessageTransformer> DATA_TRANSFORMER_MAP =
      ImmutableMap.of("api_log", apiLogTransformer, "spans", spanTransformer);

  /** KafkaStream ValueMapper for transforming upstream sources to target Trace.ListOfSpans */
  public static ValueMapper<byte[], Trace.ListOfSpans> byteArrayToTraceListOfSpans(
      String dataTransformer) {
    Preconditions.checkArgument(
        DATA_TRANSFORMER_MAP.containsKey(dataTransformer),
        "Invalid data transformer provided, must be one of {}",
        DATA_TRANSFORMER_MAP.toString());
    return messageBytes -> {
      try {
        return DATA_TRANSFORMER_MAP.get(dataTransformer).toTraceSpan(messageBytes);
      } catch (Exception e) {
        LOG.error("Error converting byte array to Trace.ListOfSpans", e);
        return null;
      }
    };
  }
}
