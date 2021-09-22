package com.slack.kaldb;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.lucene.store.Directory;
import org.openjdk.jmh.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@State(Scope.Thread)
public class IndexTraceLogBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(IndexTraceLogBenchmark.class);

  private static final String INDEX_NAME = "spans";

  private Random random;
  private final Duration commitInterval = Duration.ofMinutes(5);
  private final Duration refreshInterval = Duration.ofSeconds(30);

  BufferedReader reader = null;

  private Path tempDirectory;
  private MeterRegistry registry;
  LuceneIndexStoreImpl logStore;

  private int skipCount;
  private int indexCount;

  @Setup(Level.Iteration)
  public void createIndexer() throws Exception {

    String traceLogFile = System.getProperty("jmh.trace.log.file", "trace_logs.txt");
    reader = Files.newBufferedReader(Path.of(traceLogFile));

    random = new Random();
    registry = new SimpleMeterRegistry();
    tempDirectory =
        Files.createDirectories(
            Paths.get("jmh-output", String.valueOf(random.nextInt(Integer.MAX_VALUE))));
    logStore =
        LuceneIndexStoreImpl.makeLogStore(
            tempDirectory.toFile(), commitInterval, refreshInterval, registry);

    skipCount = 0;
    indexCount = 0;
  }

  @TearDown(Level.Iteration)
  public void tearDown() throws IOException {
    Directory directory = logStore.getIndexWriter().getDirectory();
    String[] segmentFiles = directory.listAll();
    long indexedBytes = 0;
    for (String segmentFile : segmentFiles) {
      indexedBytes += directory.fileLength(segmentFile);
    }
    LOG.info(
        "Indexed = "
            + indexCount
            + " Skipped = "
            + skipCount
            + " Index size = "
            + FileUtils.byteCountToDisplaySize(indexedBytes));
    logStore.close();
    try (Stream<Path> walk = Files.walk(tempDirectory)) {
      walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
    registry.close();
    if (reader != null) {
      reader.close();
    }
  }

  @Benchmark
  public void measureTraceLogIndexingSlingshotMode() throws IOException {
    String message;
    while ((message = reader.readLine()) != null) {

      // Mimic LogMessageWriterImpl#insertRecord kinda without the chunk rollover logic
      try {
        ConsumerRecord<String, byte[]> kafkaRecord = getKafkaRecord(message);
        LogMessage localLogMessage =
            LogMessageWriterImpl.spanTransformer.toLogMessage(kafkaRecord).get(0);
        logStore.addMessage(localLogMessage);
        indexCount++;
      } catch (Exception e) {
        skipCount++;
        LOG.error("skipping - cannot transform " + e);
      }
    }
  }

  private ConsumerRecord<String, byte[]> getKafkaRecord(String message)
      throws InvalidProtocolBufferException {
    Trace.ListOfSpans.Builder builder = Trace.ListOfSpans.newBuilder();
    JsonFormat.parser().merge(message, builder);
    Trace.ListOfSpans spans = builder.build();

    long timestamp = spans.getSpans(0).getStartTimestampMicros();

    Murron.MurronMessage testMurronMsg =
        Murron.MurronMessage.newBuilder()
            .setMessage(spans.toByteString())
            .setType(INDEX_NAME)
            .setHost("")
            .setTimestamp(timestamp)
            .build();

    return new ConsumerRecord<>(
        "testTopic",
        1,
        10,
        0L,
        TimestampType.CREATE_TIME,
        0L,
        0,
        0,
        "testKey",
        testMurronMsg.toByteString().toByteArray());
  }
}
