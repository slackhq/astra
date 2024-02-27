package com.slack.kaldb.writer;

import com.slack.kaldb.chunkManager.ChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.preprocessor.KaldbSerdes;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
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

  private static final Deserializer<Murron.MurronMessage> murronMessageDeserializer =
      KaldbSerdes.MurronMurronMessage().deserializer();

  // An apiLog message is a json blob wrapped in a murron message.
  @Deprecated
  public static final LogMessageTransformer apiLogTransformer =
      (ConsumerRecord<String, byte[]> record) -> {
        final Murron.MurronMessage murronMsg =
            murronMessageDeserializer.deserialize("", record.value());
        Trace.Span apiSpan = MurronLogFormatter.fromApiLog(murronMsg);
        return SpanFormatter.toLogMessage(Trace.ListOfSpans.newBuilder().addSpans(apiSpan).build());
      };

  // A protobuf Trace.Span
  public static final LogMessageTransformer traceSpanTransformer =
      (ConsumerRecord<String, byte[]> record) -> {
        final Trace.Span span = Trace.Span.parseFrom(record.value());
        final Trace.ListOfSpans listOfSpans = Trace.ListOfSpans.newBuilder().addSpans(span).build();
        return SpanFormatter.toLogMessage(listOfSpans);
      };

  private final ChunkManager<LogMessage> chunkManager;

  public LogMessageWriterImpl(ChunkManager<LogMessage> chunkManager) {
    this.chunkManager = chunkManager;
  }

  @Override
  public boolean insertRecord(ConsumerRecord<String, byte[]> record) throws IOException {
    if (record == null) return false;

    final Trace.ListOfSpans listOfSpans;
    try {
      final Trace.Span span = Trace.Span.parseFrom(record.value());
      listOfSpans = Trace.ListOfSpans.newBuilder().addSpans(span).build();

      if (listOfSpans.getSpansCount() == 0) return false;
    } catch (Exception e) {
      LOG.warn("Parsing consumer record: {} failed with an exception.", record, e);
      return false;
    }

    final int avgMsgSize = record.serializedValueSize() / listOfSpans.getSerializedSize();
    for (Trace.Span span : listOfSpans.getSpansList()) {
      // Currently, ChunkManager.addMessage increments a failure counter to indicate an ingestion
      // error. We decided to throw the exception to a higher level since in a batch ingestion
      // the upper layers of the stack can't take any further action. If this becomes an issue
      // in future, propagate the exception upwards here or return a value.
      chunkManager.addMessage(
          span, avgMsgSize, String.valueOf(record.partition()), record.offset());
    }
    return true;
  }
}
