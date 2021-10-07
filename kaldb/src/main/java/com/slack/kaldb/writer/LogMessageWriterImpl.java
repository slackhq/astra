package com.slack.kaldb.writer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A LogMessageWriter ingests ConsumerRecords into a ChunkManager.
 *
 * <p>The current generic ingestion pipeline can index any json message as follows: ConsumerRecord
 * -> JSON -> LogWireMessage -> LogMessage
 *
 * <p>We ingest a JSON blob from Kafka and parse it into a LogWireMessage. LogWireMessage needs an
 * id, an index name and a json map. LogMessage in addition ensures that the index name is valid,
 * has a timestamp and the map known fields contain valid types.
 *
 * <p>While this format is generic the format has a few drawbacks compared to Spans. Spans are a
 * better meta-format for logs since they standardize the basic fields, make logs more service
 * centric instead of index centric. So, an ideal indexer ingestion pipeline would be:
 * ConsumerRecord -> ListOfSpans -> Span -> LuceneDocument.
 *
 * <p>However, at this point we can't presume that all the logs will be in span format. So, we want
 * to support both the json logs and spans in the ingestion pipeline. Since a JSON Blob is more
 * basic than a span, we will convert all the spans into LogMessage. So, the current pipeline would
 * look as follows: ConsumerRecord -> f(ConsumerRecord -> List<LogMessage>) -> LogMessage
 *
 * <p>At Slack, we also wrap our json blobs and spans in an additional MurronMessage wrapper. So,
 * the transformation function for api_log whose log messages are json blobs: ConsumerRecord ->
 * MurronMessage -> Span -> List<Span> -> LogMessage. For traces topic which contains a list of
 * spans, the transformation function would look as follows: ConsumerRecord -> MurronMessage ->
 * List<Span> -> List<LogMessage>.
 *
 * <p>In future, when all the logs are written only as spans, we can directly convert a span to a
 * Lucene Document, obviating the need for MurronMessage and LogMessage wrappers. Meanwhile, we pass
 * in a data transformation function to this class as input so we can abstract away the specific
 * details of the message format from the indexer.
 *
 * <p>In the long term, we want to index only spans since spans offer several advantages over basic
 * logs like standardization, provide a service centric log view, ability to ingest and query logs
 * and traces the same way etc..
 *
 * <p>TODO: In future, implement MessageWriter interfacce on ChunkManager.
 */
public class LogMessageWriterImpl implements MessageWriter {
  private static final Logger LOG = LoggerFactory.getLogger(LogMessageWriterImpl.class);

  // An apiLog message is a json blob wrapped in a murron message.
  public static final LogMessageTransformer apiLogTransformer =
      (ConsumerRecord<String, byte[]> record) -> {
        final Murron.MurronMessage murronMsg = toMurronMessage(record.value());
        Trace.Span apiSpan = ApiLogFormatter.toSpan(murronMsg);
        return SpanFormatter.toLogMessage(Trace.ListOfSpans.newBuilder().addSpans(apiSpan).build());
      };

  // A single trace record consists of a list of spans wrapped in a murron message.
  public static final LogMessageTransformer spanTransformer =
      (ConsumerRecord<String, byte[]> record) -> {
        Murron.MurronMessage murronMsg = toMurronMessage(record.value());
        Trace.ListOfSpans spanList = SpanFormatter.fromMurronMessage(murronMsg);
        return SpanFormatter.toLogMessage(spanList);
      };

  // A json blob with a few fields.
  public static final LogMessageTransformer jsonLogMessageTransformer =
      (ConsumerRecord<String, byte[]> record) -> {
        Optional<LogMessage> msg =
            LogMessage.fromJSON(new String(record.value(), StandardCharsets.UTF_8));
        return msg.map(List::of).orElse(Collections.emptyList());
      };

  private final IndexingChunkManager<LogMessage> chunkManager;
  private final LogMessageTransformer dataTransformer;

  public LogMessageWriterImpl(
      IndexingChunkManager<LogMessage> chunkManager, LogMessageTransformer dataTransformer) {
    this.chunkManager = chunkManager;
    this.dataTransformer = dataTransformer;
  }

  @Override
  public boolean insertRecord(ConsumerRecord<String, byte[]> record) throws IOException {
    if (record == null) return false;

    final List<LogMessage> logMessages;
    try {
      logMessages = this.dataTransformer.toLogMessage(record);
      // Ideally, we should return true when logMessages are empty. But, fail the record, since we
      // don't expect any empty records or we may have a bug in earlier code.
      if (logMessages.isEmpty()) return false;
    } catch (Exception e) {
      LOG.warn("Parsing consumer record: {} failed with an exception.", record, e);
      return false;
    }

    final int avgMsgSize = record.serializedValueSize() / logMessages.size();
    for (LogMessage logMessage : logMessages) {
      // Currently, ChunkManager.addMessage increments a failure counter to indicate an ingestion
      // error. We decided to throw the exception to a higher level since in a batch ingestion
      // the upper layers of the stack can't take any further action. If this becomes an issue
      // in future, propagate the exception upwards here or return a value.
      chunkManager.addMessage(logMessage, avgMsgSize, record.offset());
    }
    return true;
  }

  public static Murron.MurronMessage toMurronMessage(byte[] recordStr) {
    Murron.MurronMessage murronMsg = null;
    if (recordStr == null || recordStr.length == 0) return null;

    try {
      murronMsg = Murron.MurronMessage.parseFrom(recordStr);
    } catch (InvalidProtocolBufferException e) {
      LOG.info("Error parsing byte string into MurronMessage: {}", new String(recordStr), e);
    }
    return murronMsg;
  }
}
