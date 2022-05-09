package com.slack.kaldb;

import brave.Tracer;
import brave.Tracing;
import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.logstore.search.LogIndexSearcher;
import com.slack.kaldb.logstore.search.LogIndexSearcherImpl;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import com.slack.service.murron.Murron;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Thread)
public class QueryBenchmark {
  private final Duration commitInterval = Duration.ofSeconds(5 * 60);
  private final Duration refreshInterval = Duration.ofSeconds(5 * 60);

  private Path tempDirectory;
  private MeterRegistry registry;
  LuceneIndexStoreImpl logStore;

  private static final SimpleDateFormat df = new SimpleDateFormat("yyyy-mm-ddHH:mm:ss.SSSzzz");
  private LogIndexSearcher logIndexSearcher;

  @Setup(Level.Trial)
  public void createIndexer() throws Exception {
    // active tracer required for search currently
    Tracing.newBuilder().build();

    // raises logging level of Tracer to Warning, to prevent excessive log messages
    final Logger logger = Logger.getLogger(Tracer.class.getName());
    logger.setLevel(java.util.logging.Level.WARNING);

    Random random = new Random();
    registry = new SimpleMeterRegistry();
    tempDirectory =
        Files.createDirectories(
            Paths.get("jmh-output", String.valueOf(random.nextInt(Integer.MAX_VALUE))));
    logStore =
        LuceneIndexStoreImpl.makeLogStore(
            tempDirectory.toFile(), commitInterval, refreshInterval, registry);

    String apiLogFile = System.getProperty("jmh.api.log.file", "api_logs.txt");

    // startup multi-threaded log message population
    ExecutorService executorService = Executors.newFixedThreadPool(6);
    try (BufferedReader reader = Files.newBufferedReader(Path.of(apiLogFile))) {
      String line;
      do {
        line = reader.readLine();
        if (line != null) {
          String finalLine = line;
          executorService.submit(
              () -> {
                // Work that ideally shouldn't count towards benchmark performance result
                ConsumerRecord<String, byte[]> kafkaRecord = makeConsumerRecord(finalLine);
                if (kafkaRecord == null) {
                  // makeConsumerRecord will print why we skipped
                  return;
                }
                // Mimic LogMessageWriterImpl#insertRecord kinda without the chunk rollover logic
                try {
                  LogMessage localLogMessage =
                      LogMessageWriterImpl.apiLogTransformer.toLogMessage(kafkaRecord).get(0);
                  logStore.addMessage(localLogMessage);
                } catch (Exception e) {
                  // ignored
                }
              });
        }
      } while (line != null);
    }
    executorService.shutdown();
    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

    logStore.commit();
    logStore.refresh();
    logIndexSearcher = new LogIndexSearcherImpl(logStore.getSearcherManager());
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    logStore.close();
    try (Stream<Path> walk = Files.walk(tempDirectory)) {
      walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
    registry.close();
  }

  @Benchmark
  public void measureLogSearcherSearcherOne() {
    logIndexSearcher.search("*", "", 0, Long.MAX_VALUE, 1, 60);
  }

  @Benchmark
  public void measureLogSearcherSearcherFiveHundred() {
    logIndexSearcher.search("*", "", 0, Long.MAX_VALUE, 500, 60);
  }

  public ConsumerRecord<String, byte[]> makeConsumerRecord(String line) {
    try {
      // get start of messageBody
      int messageDivision = line.indexOf("{");

      // Everything will there is metadata
      String[] splitLine = line.substring(0, messageDivision - 1).split("\\s+");
      String ts = splitLine[0] + splitLine[1] + splitLine[2] + splitLine[3];
      long timestamp = df.parse(ts).toInstant().toEpochMilli();

      String message = line.substring(messageDivision);
      Murron.MurronMessage testMurronMsg =
          Murron.MurronMessage.newBuilder()
              .setMessage(ByteString.copyFrom((message).getBytes(StandardCharsets.UTF_8)))
              .setType(splitLine[5])
              .setHost(splitLine[4])
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
    } catch (Exception e) {
      return null;
    }
  }
}
