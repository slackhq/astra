package com.slack.astra.testlib;

import static com.slack.astra.testlib.MessageUtil.TEST_SOURCE_STRING_PROPERTY;

import com.google.common.io.Files;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LuceneIndexStoreConfig;
import com.slack.astra.logstore.LuceneIndexStoreImpl;
import com.slack.astra.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.astra.logstore.search.LogIndexSearcherImpl;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;

public class TemporaryLogStoreAndSearcherExtension implements AfterEachCallback {

  public static final long MAX_TIME = Long.MAX_VALUE;

  public static void addMessages(
      LuceneIndexStoreImpl logStore, int low, int high, boolean requireCommit) {

    for (Trace.Span m : SpanUtil.makeSpansWithTimeDifference(low, high, 1, Instant.now())) {
      logStore.addMessage(m);
    }
    if (requireCommit) {
      logStore.commit();
      logStore.refresh();
    }
  }

  public static List<LogMessage> findAllMessages(
      LogIndexSearcherImpl searcher, String dataset, String query, int howMany) {
    return findAllMessages(searcher, dataset, howMany, new QueryStringQueryBuilder(query));
  }

  public static List<LogMessage> findAllMessages(
      LogIndexSearcherImpl searcher, String dataset, int howMany, QueryBuilder queryBuilder) {
    DateHistogramAggregationBuilder dateHistogramAggregationBuilder =
        new DateHistogramAggregationBuilder("1");
    dateHistogramAggregationBuilder
        .field(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName)
        .fixedInterval(new DateHistogramInterval("1s"));

    SearchResult<LogMessage> results =
        searcher.search(
            dataset,
            howMany,
            queryBuilder,
            null,
            new AggregatorFactories.Builder().addAggregator(dateHistogramAggregationBuilder));
    return results.hits;
  }

  public final SimpleMeterRegistry metricsRegistry;
  public LuceneIndexStoreImpl logStore;
  public LogIndexSearcherImpl logSearcher;
  public final File tempFolder;

  // use this method if you do not want to include zookeeper setup or redaction in your tests
  public TemporaryLogStoreAndSearcherExtension(boolean enableFullTextSearch) throws IOException {
    this(
        Duration.of(5, ChronoUnit.MINUTES),
        Duration.of(5, ChronoUnit.MINUTES),
        enableFullTextSearch,
        SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_VALUE_AND_DUPLICATE_FIELD,
        null);
  }

  // include field redaction metadata for redaction testing
  public TemporaryLogStoreAndSearcherExtension(
      boolean enableFullTextSearch, FieldRedactionMetadataStore fieldRedactionMetadataStore)
      throws IOException {
    this(
        Duration.of(5, ChronoUnit.MINUTES),
        Duration.of(5, ChronoUnit.MINUTES),
        enableFullTextSearch,
        SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_VALUE_AND_DUPLICATE_FIELD,
        fieldRedactionMetadataStore);
  }

  public TemporaryLogStoreAndSearcherExtension(
      Duration commitInterval,
      Duration refreshInterval,
      boolean enableFullTextSearch,
      SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy fieldConflictPolicy,
      FieldRedactionMetadataStore fieldRedactionMetadataStore)
      throws IOException {
    this.metricsRegistry = new SimpleMeterRegistry();
    this.tempFolder = Files.createTempDir(); // TODO: don't use beta func.
    LuceneIndexStoreConfig indexStoreCfg =
        getIndexStoreConfig(commitInterval, refreshInterval, tempFolder);

    if (fieldRedactionMetadataStore == null) {
      RetryPolicy retryPolicy = new RetryUntilElapsed(Integer.MAX_VALUE, Integer.MAX_VALUE);
      CuratorFramework curatorFramework =
          CuratorFrameworkFactory.builder()
              .connectString("test")
              .namespace("test")
              .connectionTimeoutMs(Integer.MAX_VALUE)
              .retryPolicy(retryPolicy)
              .sessionTimeoutMs(Integer.MAX_VALUE)
              .build();
      fieldRedactionMetadataStore =
          new TestingFieldRedactionMetadataStore(
              AsyncCuratorFramework.wrap(curatorFramework), false);
    }

    logStore =
        new LuceneIndexStoreImpl(
            indexStoreCfg,
            SchemaAwareLogDocumentBuilderImpl.build(
                fieldConflictPolicy, enableFullTextSearch, metricsRegistry),
            metricsRegistry,
            fieldRedactionMetadataStore);

    ConcurrentHashMap<String, LuceneFieldDef> schema = logStore.getSchema();

    // add schema definition for our string property
    schema.put(
        TEST_SOURCE_STRING_PROPERTY,
        new LuceneFieldDef(TEST_SOURCE_STRING_PROPERTY, FieldType.STRING.name, false, true, true));

    logSearcher = new LogIndexSearcherImpl(logStore.getSearcherManager(), schema);
  }

  public static LuceneIndexStoreConfig getIndexStoreConfig(
      Duration commitInterval, Duration refreshInterval, File tempFolder) throws IOException {
    return new LuceneIndexStoreConfig(
        commitInterval, refreshInterval, tempFolder.getCanonicalPath(), false);
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    if (logStore != null) {
      logStore.close();
    }
    if (logSearcher != null) {
      logSearcher.close();
    }
    FileUtils.deleteDirectory(tempFolder);
    metricsRegistry.close();
  }

  public static class TestingFieldRedactionMetadataStore extends FieldRedactionMetadataStore {

    public TestingFieldRedactionMetadataStore(
        AsyncCuratorFramework curatorFramework, boolean shouldCache) {
      super(curatorFramework, shouldCache);
    }

    @Override
    public void createSync(FieldRedactionMetadata metadataNode) {}

    @Override
    public List<FieldRedactionMetadata> listSync() {
      return List.of();
    }

    @Override
    public void addListener(AstraMetadataStoreChangeListener<FieldRedactionMetadata> watcher) {}
  }
}
