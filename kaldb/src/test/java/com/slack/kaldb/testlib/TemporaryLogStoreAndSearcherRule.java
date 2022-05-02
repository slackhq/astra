package com.slack.kaldb.testlib;

import com.google.common.io.Files;
import com.slack.kaldb.logstore.LogDocumentBuilderImpl;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LuceneIndexStoreConfig;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.logstore.search.LogIndexSearcherImpl;
import com.slack.kaldb.logstore.search.SearchResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
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
      LogIndexSearcherImpl searcher, String indexName, String query, int howMany, int bucketCount) {
    SearchResult<LogMessage> results =
        searcher.search(indexName, query, 0, MAX_TIME, howMany, bucketCount);
    return results.hits;
  }

  public final SimpleMeterRegistry metricsRegistry;
  public LuceneIndexStoreImpl logStore;
  public LogIndexSearcherImpl logSearcher;
  public final File tempFolder;

  public TemporaryLogStoreAndSearcherRule(boolean ignorePropertyExceptions) throws IOException {
    this(
        Duration.of(5, ChronoUnit.MINUTES),
        Duration.of(5, ChronoUnit.MINUTES),
        Duration.of(5, ChronoUnit.SECONDS),
        ignorePropertyExceptions);
  }

  public TemporaryLogStoreAndSearcherRule(
      Duration commitInterval,
      Duration refreshInterval,
      Duration queryTimeout,
      boolean ignorePropertyTypeExceptions)
      throws IOException {
    this.metricsRegistry = new SimpleMeterRegistry();
    this.tempFolder = Files.createTempDir(); // TODO: don't use beta func.
    LuceneIndexStoreConfig indexStoreCfg =
        getIndexStoreConfig(commitInterval, refreshInterval, tempFolder);
    logStore =
        new LuceneIndexStoreImpl(
            indexStoreCfg,
            LogDocumentBuilderImpl.build(ignorePropertyTypeExceptions),
            queryTimeout.toMillis(),
            metricsRegistry);
    logSearcher = new LogIndexSearcherImpl(logStore.getSearcherManager());
  }

  public static LuceneIndexStoreConfig getIndexStoreConfig(
      Duration commitInterval, Duration refreshInterval, File tempFolder) throws IOException {
    return new LuceneIndexStoreConfig(
        commitInterval, refreshInterval, tempFolder.getCanonicalPath(), 8, false);
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
