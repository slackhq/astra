package com.slack.kaldb.testlib;

import static com.slack.kaldb.testlib.MessageUtil.TEST_SOURCE_STRING_PROPERTY;

import com.google.common.io.Files;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LuceneIndexStoreConfig;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.kaldb.logstore.search.LogIndexSearcherImpl;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class TemporaryLogStoreAndSearcherRule implements TestRule {

  public static final long MAX_TIME = Long.MAX_VALUE;

  public static List<LogMessage> addMessages(
      LuceneIndexStoreImpl logStore, int low, int high, boolean requireCommit) {

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(low, high);
    for (LogMessage m : messages) {
      logStore.addMessage(m);
    }
    if (requireCommit) {
      logStore.commit();
      logStore.refresh();
    }
    return messages;
  }

  public static List<LogMessage> findAllMessages(
      LogIndexSearcherImpl searcher, String dataset, String query, int howMany) {
    SearchResult<LogMessage> results =
        searcher.search(
            dataset,
            query,
            0,
            MAX_TIME,
            howMany,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    return results.hits;
  }

  public final SimpleMeterRegistry metricsRegistry;
  public LuceneIndexStoreImpl logStore;
  public LogIndexSearcherImpl logSearcher;
  public final File tempFolder;

  public TemporaryLogStoreAndSearcherRule(boolean enableFullTextSearch) throws IOException {
    this(
        Duration.of(5, ChronoUnit.MINUTES),
        Duration.of(5, ChronoUnit.MINUTES),
        enableFullTextSearch,
        SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_VALUE_AND_DUPLICATE_FIELD);
  }

  public TemporaryLogStoreAndSearcherRule(
      Duration commitInterval,
      Duration refreshInterval,
      boolean enableFullTextSearch,
      SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy fieldConflictPolicy)
      throws IOException {
    this.metricsRegistry = new SimpleMeterRegistry();
    this.tempFolder = Files.createTempDir(); // TODO: don't use beta func.
    LuceneIndexStoreConfig indexStoreCfg =
        getIndexStoreConfig(commitInterval, refreshInterval, tempFolder);
    logStore =
        new LuceneIndexStoreImpl(
            indexStoreCfg,
            SchemaAwareLogDocumentBuilderImpl.build(
                fieldConflictPolicy, enableFullTextSearch, metricsRegistry),
            metricsRegistry);

    ConcurrentHashMap<String, LuceneFieldDef> schema = logStore.getSchema();

    // add schema definition for our string property
    schema.put(
        TEST_SOURCE_STRING_PROPERTY,
        new LuceneFieldDef(TEST_SOURCE_STRING_PROPERTY, FieldType.STRING.name, false, true, true));

    logSearcher = new LogIndexSearcherImpl(logStore.getSearcherManager(), schema, false);
  }

  public static LuceneIndexStoreConfig getIndexStoreConfig(
      Duration commitInterval, Duration refreshInterval, File tempFolder) throws IOException {
    return new LuceneIndexStoreConfig(
        commitInterval, refreshInterval, tempFolder.getCanonicalPath(), false);
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          base.evaluate(); // Run the test
        } finally {
          if (logStore != null) {
            logStore.close();
          }
          if (logSearcher != null) {
            logSearcher.close();
          }
          FileUtils.deleteDirectory(tempFolder);
          metricsRegistry.close();
        }
      }
    };
  }
}
