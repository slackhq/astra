package com.slack.kaldb;

import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.openjdk.jmh.annotations.*;

@State(Scope.Thread)
public class IndexingBenchmark {

  private final Duration commitInterval = Duration.ofSeconds(5 * 60);
  private final Duration refreshInterval = Duration.ofSeconds(5 * 60);

  private Path tempDirectory;
  private MeterRegistry registry;
  LuceneIndexStoreImpl logStore;
  private Random random;

  private LogMessage logMessage;
  private Document logDocument;

  @Setup(Level.Iteration)
  public void createIndexer() throws IOException {
    random = new Random();
    Instant timestamp = Instant.now();
    registry = new SimpleMeterRegistry();
    tempDirectory =
        Files.createDirectories(
            Paths.get("jmh", String.valueOf(random.nextInt(Integer.MAX_VALUE))));
    logStore =
        LuceneIndexStoreImpl.makeLogStore(
            tempDirectory.toFile(), commitInterval, refreshInterval, registry);

    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(LogMessage.ReservedField.TIMESTAMP.fieldName, timestamp.toString());
    fieldMap.put(LogMessage.ReservedField.MESSAGE.fieldName, "Log Message");
    logMessage = new LogMessage("testindex", "INFO", "1", fieldMap);

    // inlines LogDocumentBuilderImpl#fromMessage
    logDocument = new Document();
    logDocument.add(new TextField("index", "testindex", Field.Store.NO));
    logDocument.add(new TextField("id", "1", Field.Store.NO));
    logDocument.add(new TextField("type", "INFO", Field.Store.NO));
    logDocument.add(new TextField("message", "Log Message", Field.Store.NO));

    logDocument.add(new LongPoint("_timesinceepoch", timestamp.toEpochMilli()));
    logDocument.add(new NumericDocValuesField("_timesinceepoch", timestamp.toEpochMilli()));

    String value =
        "{\"id\":\"1\",\"source\":{\"@timestamp\":\""
            + timestamp.toString()
            + "\",\"message\":\"Log Message\"},\"index\":\"testindex\",\"type\":\"INFO\"}";
    logDocument.add(new StoredField("_source", value));
  }

  @TearDown(Level.Iteration)
  public void tearDown() throws IOException {
    logStore.close();
    try (Stream<Path> walk = Files.walk(tempDirectory)) {
      walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
    registry.close();
  }

  @Benchmark
  public void measureIndexing() {
    logStore.addMessage(logMessage);
  }

  @Benchmark
  public void measureIndexingToLuceneDirectly() {
    IndexWriter indexWriter = logStore.getIndexWriter();
    try {
      indexWriter.addDocument(logDocument);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
