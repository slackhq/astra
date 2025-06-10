package com.slack.astra.logstore.search;

import static com.slack.astra.logstore.LuceneIndexStoreImpl.COMMITS_TIMER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.REFRESHES_TIMER;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.astra.testlib.MessageUtil.TEST_SOURCE_LONG_PROPERTY;
import static com.slack.astra.testlib.MessageUtil.TEST_SOURCE_STRING_PROPERTY;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.testlib.MetricsUtil.getTimerCount;
import static com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension.MAX_TIME;
import static com.slack.astra.util.AggregatorFactoriesUtil.createAverageAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createExtendedStatsAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createFiltersAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createGenericDateHistogramAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createMaxAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createMinAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createSumAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createTermsAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.slack.astra.clusterManager.RedactionUpdateService;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.testlib.SpanUtil;
import com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension;
import com.slack.astra.util.QueryBuilderUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opensearch.search.aggregations.bucket.filter.InternalFilters;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalExtendedStats;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;

public class LogIndexSearcherImplTest {

  @Nested
  public class RedactionTests {
    private FieldRedactionMetadataStore fieldRedactionMetadataStore;
    private TestingServer testingServer;
    private MeterRegistry meterRegistry;
    private AsyncCuratorFramework curatorFramework;
    private AstraConfigs.RedactionUpdateServiceConfig redactionUpdateServiceConfig;
    private RedactionUpdateService redactionUpdateService;

    @BeforeEach
    public void setup() throws Exception {
      // setup ZK and redaction metadata store for field redaction testing

      testingServer = new TestingServer();
      meterRegistry = new SimpleMeterRegistry();
      AstraConfigs.MetadataStoreConfig metadataStoreConfig =
          AstraConfigs.MetadataStoreConfig.newBuilder()
              .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
              .setZookeeperConfig(
                  AstraConfigs.ZookeeperConfig.newBuilder()
                      .setZkConnectString(testingServer.getConnectString())
                      .setZkPathPrefix("test")
                      .setZkSessionTimeoutMs(Integer.MAX_VALUE)
                      .setZkConnectionTimeoutMs(Integer.MAX_VALUE)
                      .setSleepBetweenRetriesMs(1000)
                      .setZkCacheInitTimeoutMs(1000)
                      .build())
              .build();
      curatorFramework =
          CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
      fieldRedactionMetadataStore =
          new FieldRedactionMetadataStore(
              curatorFramework, metadataStoreConfig, meterRegistry, true);

      redactionUpdateServiceConfig =
          AstraConfigs.RedactionUpdateServiceConfig.newBuilder()
              .setRedactionUpdatePeriodSecs(1)
              .setRedactionUpdateInitDelaySecs(1)
              .build();
      redactionUpdateService =
          new RedactionUpdateService(fieldRedactionMetadataStore, redactionUpdateServiceConfig);
      redactionUpdateService.startAsync();
      redactionUpdateService.awaitRunning(DEFAULT_START_STOP_DURATION);
    }

    @AfterEach
    public void teardown() throws Exception {
      testingServer.close();
      fieldRedactionMetadataStore.close();
      curatorFramework.unwrap().close();
      meterRegistry.close();
      if (redactionUpdateService != null) {
        redactionUpdateService.stopAsync();
        redactionUpdateService.awaitTerminated(DEFAULT_START_STOP_DURATION);
      }
    }

    @Test
    public void testRedactionWithIncludeFilters() throws Exception {
      String redactionName = "testRedaction";
      String fieldName = "message";
      long start = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
      long end = Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli();

      // search
      TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
          new TemporaryLogStoreAndSearcherExtension(true);

      Instant time = Instant.now();
      featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(2, time.plusSeconds(100)));
      featureFlagEnabledStrictLogStore.logStore.commit();
      featureFlagEnabledStrictLogStore.logStore.refresh();

      assertThat(
              getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(2);
      assertThat(
              getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(1);

      AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
          AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
              .putIncludeFields("message", true)
              .build();

      // add redaction between log being added and searched to test that the redaction map gets
      // updated
      // a previous change passed this test when the redaction was added before the
      // DirectoryReader was created and redaction still did not work
      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata(redactionName, fieldName, start, end));

      await()
          .until(
              () ->
                  AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size() == 1);

      // redaction service is currently set to update every <redaction_update_period_secs>
      // setRedactionUpdatePeriodSecs is set to 1 second in setup()
      Thread.sleep(redactionUpdateServiceConfig.getRedactionUpdatePeriodSecs() * 1000);

      List<LogMessage> messages =
          featureFlagEnabledStrictLogStore.logSearcher.search(
                  TEST_DATASET_NAME,
                  1000,
                  QueryBuilderUtil.generateQueryBuilder(
                      "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                  SourceFieldFilter.fromProto(sourceFieldFilter),
                  createGenericDateHistogramAggregatorFactoriesBuilder())
              .hits;
      assertThat(messages).hasSize(1);
      assertThat(messages.get(0).getSource()).hasSize(1);
      assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
      assertThat(messages.get(0).getSource().get("message")).isEqualTo("REDACTED");
      featureFlagEnabledStrictLogStore.closeAll();
    }

    @Test
    public void testRedactionOutOfTimerange() throws Exception {
      String redactionName = "testRedaction";
      String fieldName = "message";
      long start = Instant.now().minus(2, ChronoUnit.DAYS).toEpochMilli();
      long end = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();

      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata(redactionName, fieldName, start, end));

      await()
          .until(
              () ->
                  AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size() == 1);
      Thread.sleep(redactionUpdateServiceConfig.getRedactionUpdatePeriodSecs() * 1000);

      // search
      TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
          new TemporaryLogStoreAndSearcherExtension(true);

      Instant time = Instant.now();
      featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(2, time.plusSeconds(100)));
      featureFlagEnabledStrictLogStore.logStore.commit();
      featureFlagEnabledStrictLogStore.logStore.refresh();

      assertThat(
              getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(2);
      assertThat(
              getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(1);

      AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
          AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
              .putIncludeFields("message", true)
              .build();

      List<LogMessage> messages =
          featureFlagEnabledStrictLogStore.logSearcher.search(
                  TEST_DATASET_NAME,
                  1000,
                  QueryBuilderUtil.generateQueryBuilder(
                      "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                  SourceFieldFilter.fromProto(sourceFieldFilter),
                  createGenericDateHistogramAggregatorFactoriesBuilder())
              .hits;
      assertThat(messages).hasSize(1);
      assertThat(messages.get(0).getSource()).hasSize(1);
      assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
      assertThat(messages.get(0).getSource().get("message"))
          .isEqualTo("The identifier in this message is Message1");
      featureFlagEnabledStrictLogStore.closeAll();
    }

    @Test
    public void testRedactionInAndOutOfTimerange() throws Exception {
      String redactionName = "testRedaction";
      String fieldName = "message";
      long start = Instant.now().minus(2, ChronoUnit.DAYS).toEpochMilli();
      long end = Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli();

      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata(redactionName, fieldName, start, end));

      await()
          .until(
              () ->
                  AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size() == 1);
      Thread.sleep(redactionUpdateServiceConfig.getRedactionUpdatePeriodSecs() * 1000);

      // search
      TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
          new TemporaryLogStoreAndSearcherExtension(true);

      Instant time = Instant.now();
      featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(2, time.minus(1, ChronoUnit.DAYS)));
      featureFlagEnabledStrictLogStore.logStore.commit();
      featureFlagEnabledStrictLogStore.logStore.refresh();

      assertThat(
              getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(2);
      assertThat(
              getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(1);

      AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
          AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
              .putIncludeFields("message", true)
              .build();

      List<LogMessage> messages =
          featureFlagEnabledStrictLogStore.logSearcher.search(
                  TEST_DATASET_NAME,
                  1000,
                  QueryBuilderUtil.generateQueryBuilder(
                      "*",
                      time.minus(2, ChronoUnit.DAYS).toEpochMilli(),
                      time.plusSeconds(10).toEpochMilli()),
                  SourceFieldFilter.fromProto(sourceFieldFilter),
                  createGenericDateHistogramAggregatorFactoriesBuilder())
              .hits;
      assertThat(messages).hasSize(2);
      assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
      assertThat(messages.get(0).getSource().get("message"))
          .isEqualTo("The identifier in this message is Message1");
      assertThat(messages.get(1).getSource().containsKey("message")).isTrue();
      assertThat(messages.get(1).getSource().get("message")).isEqualTo("REDACTED");

      featureFlagEnabledStrictLogStore.closeAll();
    }

    @Test
    public void testRedactionWithMultipleFieldsRedacted() throws Exception {

      long start = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
      long end = Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli();

      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata("testRedaction1", "message", start, end));
      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata("testRedaction2", "binaryproperty", start, end));

      await()
          .until(
              () ->
                  AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size() == 2);
      Thread.sleep(redactionUpdateServiceConfig.getRedactionUpdatePeriodSecs() * 1000);

      // search
      TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
          new TemporaryLogStoreAndSearcherExtension(true);

      Instant time = Instant.now();
      featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(2, time.plusSeconds(100)));
      featureFlagEnabledStrictLogStore.logStore.commit();
      featureFlagEnabledStrictLogStore.logStore.refresh();

      assertThat(
              getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(2);
      assertThat(
              getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(1);

      AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
          AstraSearch.SearchRequest.SourceFieldFilter.newBuilder().setIncludeAll(true).build();

      List<LogMessage> messages =
          featureFlagEnabledStrictLogStore.logSearcher.search(
                  TEST_DATASET_NAME,
                  1000,
                  QueryBuilderUtil.generateQueryBuilder(
                      "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                  SourceFieldFilter.fromProto(sourceFieldFilter),
                  createGenericDateHistogramAggregatorFactoriesBuilder())
              .hits;

      assertThat(messages).hasSize(1);
      assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
      assertThat(messages.get(0).getSource().get("message")).isEqualTo("REDACTED");
      assertThat(messages.get(0).getSource().containsKey("binaryproperty")).isTrue();
      assertThat(messages.get(0).getSource().get("binaryproperty")).isEqualTo("REDACTED");

      featureFlagEnabledStrictLogStore.closeAll();
    }

    @Test
    public void testRedactionWithIncludeFiltersWithWildcard() throws Exception {

      long start = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
      long end = Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli();

      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata("testRedaction1", "message", start, end));

      await()
          .until(
              () ->
                  AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size() == 1);
      Thread.sleep(redactionUpdateServiceConfig.getRedactionUpdatePeriodSecs() * 1000);

      TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
          new TemporaryLogStoreAndSearcherExtension(true);

      Instant time = Instant.now();
      featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(2, time.plusSeconds(100)));
      featureFlagEnabledStrictLogStore.logStore.commit();
      featureFlagEnabledStrictLogStore.logStore.refresh();

      assertThat(
              getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(2);
      assertThat(
              getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(1);

      AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
          AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
              .addIncludeWildcards("messag.*")
              .build();

      List<LogMessage> messages =
          featureFlagEnabledStrictLogStore.logSearcher.search(
                  TEST_DATASET_NAME,
                  1000,
                  QueryBuilderUtil.generateQueryBuilder(
                      "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                  SourceFieldFilter.fromProto(sourceFieldFilter),
                  createGenericDateHistogramAggregatorFactoriesBuilder())
              .hits;
      assertThat(messages).hasSize(1);
      assertThat(messages.get(0).getSource()).hasSize(1);
      assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
      assertThat(messages.get(0).getSource().get("message")).isEqualTo("REDACTED");

      featureFlagEnabledStrictLogStore.closeAll();
    }

    @Test
    public void testRedactionWithFilterAggregations() throws Exception {
      Instant time = Instant.now();
      long start = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
      long end = Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli();

      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata("testRedaction1", "message", start, end));

      await()
          .until(
              () ->
                  AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size() == 1);
      Thread.sleep(redactionUpdateServiceConfig.getRedactionUpdatePeriodSecs() * 1000);

      TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
          new TemporaryLogStoreAndSearcherExtension(true);

      featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(3, "apple baby", time.plusSeconds(2)));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(4, "car", time.plusSeconds(3)));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(5, "apple baby car", time.plusSeconds(4)));

      featureFlagEnabledStrictLogStore.logStore.commit();
      featureFlagEnabledStrictLogStore.logStore.refresh();

      SearchResult<LogMessage> scriptNull =
          featureFlagEnabledStrictLogStore.logSearcher.search(
              TEST_DATASET_NAME,
              1000,
              QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
              null,
              createFiltersAggregatorFactoriesBuilder(
                  "1",
                  List.of(),
                  Map.of(
                      "foo",
                      QueryBuilderUtil.generateQueryBuilder(
                          String.format(
                              "%s:<=%s",
                              LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                              time.plusSeconds(2).toEpochMilli()),
                          time.toEpochMilli(),
                          time.plusSeconds(2).toEpochMilli()),
                      "bar",
                      QueryBuilderUtil.generateQueryBuilder(
                          String.format(
                              "%s:>%s",
                              LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                              time.plusSeconds(2).toEpochMilli()),
                          time.plusSeconds(2).toEpochMilli(),
                          time.plusSeconds(10).toEpochMilli()))));

      assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().size())
          .isEqualTo(2);
      assertThat(
              ((InternalFilters) scriptNull.internalAggregation).getBuckets().get(0).getDocCount())
          .isEqualTo(2);
      assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().get(0).getKey())
          .isIn(List.of("foo", "bar"));
      assertThat(
              ((InternalFilters) scriptNull.internalAggregation).getBuckets().get(1).getDocCount())
          .isEqualTo(2);
      assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().get(1).getKey())
          .isIn(List.of("foo", "bar"));
      assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().get(0).getKey())
          .isNotEqualTo(
              ((InternalFilters) scriptNull.internalAggregation).getBuckets().get(1).getKey());

      featureFlagEnabledStrictLogStore.closeAll();
    }
  }

  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension strictLogStore =
      new TemporaryLogStoreAndSearcherExtension(true);

  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension strictLogStoreWithoutFts =
      new TemporaryLogStoreAndSearcherExtension(false);

  public LogIndexSearcherImplTest() throws IOException {}

  @BeforeAll
  public static void beforeClass() {
    Tracing.newBuilder().build();
  }

  private void loadTestData(Instant time) {
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(3, "apple baby", time.plusSeconds(2)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(4, "car", time.plusSeconds(3)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(5, "apple baby car", time.plusSeconds(4)));
    // when we enable multi-tenancy, we can add messages to different indices

    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
  }

  @Test
  public void testSearchWithIncludeFilters() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .putIncludeFields("message", true)
            .build();

    List<LogMessage> messages =
        featureFlagEnabledStrictLogStore.logSearcher.search(
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .hits;
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource()).hasSize(1);
    assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
    assertThat(messages.get(0).getSource().get("message"))
        .isEqualTo("The identifier in this message is Message1");
  }

  @Test
  public void testSearchWithIncludeFiltersWithWildcardAfter() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .addIncludeWildcards("messag.*")
            .build();

    List<LogMessage> messages =
        featureFlagEnabledStrictLogStore.logSearcher.search(
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .hits;
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource()).hasSize(1);
    assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
    assertThat(messages.get(0).getSource().get("message"))
        .isEqualTo("The identifier in this message is Message1");
  }

  @Test
  public void testSearchWithIncludeFiltersWithWildcardBefore() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .addIncludeWildcards(".*ssage")
            .build();

    List<LogMessage> messages =
        featureFlagEnabledStrictLogStore.logSearcher.search(
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .hits;
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource()).hasSize(1);
    assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
    assertThat(messages.get(0).getSource().get("message"))
        .isEqualTo("The identifier in this message is Message1");
  }

  @Test
  public void testSearchWithIncludeFilterAllTrue() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder().setIncludeAll(true).build();

    List<LogMessage> messages =
        featureFlagEnabledStrictLogStore.logSearcher.search(
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .hits;
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource().size()).isGreaterThan(1);
  }

  @Test
  public void testSearchWithIncludeFilterAllFalse() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder().setIncludeAll(false).build();

    List<LogMessage> messages =
        featureFlagEnabledStrictLogStore.logSearcher.search(
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .hits;
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource().size()).isEqualTo(0);
  }

  @Test
  public void testSearchWithExcludeFilters() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .putExcludeFields("message", true)
            .build();

    List<LogMessage> messages =
        featureFlagEnabledStrictLogStore.logSearcher.search(
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .hits;
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource().size()).isGreaterThan(1);
    assertThat(messages.get(0).getSource().containsKey("message")).isFalse();
  }

  @Test
  public void testSearchWithExcludeFiltersWithWildcardAfter() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .addExcludeWildcards(".*ssage")
            .build();

    List<LogMessage> messages =
        featureFlagEnabledStrictLogStore.logSearcher.search(
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .hits;
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource().size()).isGreaterThan(1);
    assertThat(messages.get(0).getSource().containsKey("message")).isFalse();
  }

  @Test
  public void testSearchWithExcludeFiltersWithWildcardBefore() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .addExcludeWildcards(".*ssage")
            .build();

    List<LogMessage> messages =
        featureFlagEnabledStrictLogStore.logSearcher.search(
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .hits;
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource().size()).isGreaterThan(1);
    assertThat(messages.get(0).getSource().containsKey("message")).isFalse();
  }

  @Test
  public void testSearchWithExcludeFilterAllTrue() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder().setExcludeAll(true).build();

    List<LogMessage> messages =
        featureFlagEnabledStrictLogStore.logSearcher.search(
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .hits;
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource().size()).isEqualTo(0);
  }

  @Test
  public void testSearchWithExcludeFilterAllFalse() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder().setExcludeAll(false).build();

    List<LogMessage> messages =
        featureFlagEnabledStrictLogStore.logSearcher.search(
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .hits;
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource().size()).isGreaterThan(0);
  }

  @Test
  public void testTimeBoundSearch() throws IOException {
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, time.plusSeconds(100)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

    // Start inclusive.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder(
                        "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);

    // Extended range still only picking one element.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder(
                        "Message1",
                        time.minusSeconds(1).toEpochMilli(),
                        time.plusSeconds(90).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);

    // Both ranges are inclusive.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder(
                        "_id:Message1 OR Message2",
                        time.toEpochMilli(),
                        time.plusSeconds(100).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);

    // Extended range to pick up both events
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder(
                        "_id:Message1 OR Message2",
                        time.minusSeconds(1).toEpochMilli(),
                        time.plusSeconds(100).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);
  }

  @Test
  @Disabled // todo - re-enable when multi-tenancy is supported - slackhq/astra/issues/223
  public void testIndexBoundSearch() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx",
                    100,
                    QueryBuilderUtil.generateQueryBuilder(
                        "test1",
                        time.minusSeconds(1).toEpochMilli(),
                        time.plusSeconds(10).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx1",
                    100,
                    QueryBuilderUtil.generateQueryBuilder(
                        "test1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx12",
                    100,
                    QueryBuilderUtil.generateQueryBuilder(
                        "test1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(0);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx1",
                    100,
                    QueryBuilderUtil.generateQueryBuilder(
                        "test", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(0);
  }

  @Test
  public void testSearchMultipleItemsAndIndices() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);
    SearchResult<LogMessage> babies =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "Message1", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(babies.hits.size()).isEqualTo(1);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(babies.internalAggregation);
    assertThat(histogram.getBuckets().size()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testAllQueryWithFullTextSearchEnabled() throws IOException {
    Instant time = Instant.now();

    Trace.KeyValue customField =
        Trace.KeyValue.newBuilder()
            .setVStr("value")
            .setKey("customField")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time, List.of(customField)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> termQuery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "customField:value", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(termQuery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> noTermStrQuery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "value", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(noTermStrQuery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> noTermNumericQuery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "Message1", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(noTermNumericQuery.hits.size()).isEqualTo(1);
  }

  @Test
  public void testAllQueryWithFullTextSearchDisabled() throws IOException {
    Instant time = Instant.now();
    Trace.KeyValue customField =
        Trace.KeyValue.newBuilder()
            .setVStr("value")
            .setKey("customField")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();
    strictLogStoreWithoutFts.logStore.addMessage(
        SpanUtil.makeSpan(1, "apple", time, List.of(customField)));
    strictLogStoreWithoutFts.logStore.commit();
    strictLogStoreWithoutFts.logStore.refresh();

    SearchResult<LogMessage> termQuery =
        strictLogStoreWithoutFts.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "customField:value", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(termQuery.hits.size()).isEqualTo(1);
  }

  @Test
  public void testExistsQuery() throws IOException {
    Instant time = Instant.now();
    Trace.KeyValue customField =
        Trace.KeyValue.newBuilder()
            .setVStr("value")
            .setKey("customField")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();
    Trace.KeyValue customField1 =
        Trace.KeyValue.newBuilder()
            .setVStr("value")
            .setKey("customField1")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time, List.of(customField)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, "apple", time, List.of(customField1)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> exists =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "_exists_:customField", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(exists.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> termQuery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "customField:value", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(termQuery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> notExists =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "_exists_:foo", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(notExists.hits.size()).isEqualTo(0);
  }

  @Test
  public void testRangeQuery() throws IOException {
    Instant time = Instant.now();

    Trace.KeyValue valTag1 =
        Trace.KeyValue.newBuilder()
            .setKey("val")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .setVInt32(1)
            .build();
    Trace.KeyValue valTag2 =
        Trace.KeyValue.newBuilder()
            .setKey("val")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .setVInt32(2)
            .build();
    Trace.KeyValue valTag3 =
        Trace.KeyValue.newBuilder()
            .setKey("val")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .setVInt32(3)
            .build();
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time, List.of(valTag1)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, "bear", time, List.of(valTag2)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(3, "car", time, List.of(valTag3)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> rangeBoundInclusive =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "val:[1 TO 3]", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(rangeBoundInclusive.hits.size()).isEqualTo(3);

    SearchResult<LogMessage> rangeBoundExclusive =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "val:{1 TO 3}", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(rangeBoundExclusive.hits.size()).isEqualTo(1);
  }

  @Test
  public void testQueryParsingFieldTypes() throws IOException {
    Instant time = Instant.now();

    Trace.KeyValue boolTag =
        Trace.KeyValue.newBuilder()
            .setVBool(true)
            .setKey("boolval")
            .setFieldType(Schema.SchemaFieldType.BOOLEAN)
            .build();

    Trace.KeyValue intTag =
        Trace.KeyValue.newBuilder()
            .setVInt32(1)
            .setKey("intval")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();

    Trace.KeyValue longTag =
        Trace.KeyValue.newBuilder()
            .setVInt64(2L)
            .setKey("longval")
            .setFieldType(Schema.SchemaFieldType.LONG)
            .build();

    Trace.KeyValue floatTag =
        Trace.KeyValue.newBuilder()
            .setVFloat32(3F)
            .setKey("floatval")
            .setFieldType(Schema.SchemaFieldType.FLOAT)
            .build();

    Trace.KeyValue doubleTag =
        Trace.KeyValue.newBuilder()
            .setVFloat64(4D)
            .setKey("doubleval")
            .setFieldType(Schema.SchemaFieldType.DOUBLE)
            .build();

    Trace.Span span =
        SpanUtil.makeSpan(1, "apple", time, List.of(boolTag, intTag, longTag, floatTag, doubleTag));
    strictLogStore.logStore.addMessage(span);
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> boolquery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "boolval:true", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(boolquery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> intquery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "intval:1", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(intquery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> longquery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "longval:2", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(longquery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> floatquery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "floatval:3", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(floatquery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> doublequery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "doubleval:4", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(doublequery.hits.size()).isEqualTo(1);
  }

  @Test
  public void testTopKQuery() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);

    SearchResult<LogMessage> apples =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            2,
            QueryBuilderUtil.generateQueryBuilder(
                "apple", time.toEpochMilli(), time.plusSeconds(100).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(apples.hits.stream().map(m -> m.getId()).collect(Collectors.toList()))
        .isEqualTo(Arrays.asList("Message5", "Message3"));
    assertThat(apples.hits.size()).isEqualTo(2);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(apples.internalAggregation);

    assertThat(histogram.getBuckets().size()).isEqualTo(3);
    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(1).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(2).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testSearchMultipleCommits() throws IOException {
    Instant time = Instant.now();

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, "apple baby", time.plusSeconds(2)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> baby =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            2,
            QueryBuilderUtil.generateQueryBuilder(
                "baby", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(baby.hits.size()).isEqualTo(1);
    assertThat(baby.hits.get(0).getId()).isEqualTo("Message2");
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

    // Add car but don't commit. So, no results for car.
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(3, "car", time.plusSeconds(3)));

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

    strictLogStore.logSearcher.search(
        TEST_DATASET_NAME,
        2,
        QueryBuilderUtil.generateQueryBuilder(
            "car", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
        null,
        createGenericDateHistogramAggregatorFactoriesBuilder());

    // Commit but no refresh. Item is still not available for search.
    strictLogStore.logStore.commit();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);

    strictLogStore.logSearcher.search(
        TEST_DATASET_NAME,
        2,
        QueryBuilderUtil.generateQueryBuilder(
            "car", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
        null,
        createGenericDateHistogramAggregatorFactoriesBuilder());

    // Car can be searched after refresh.
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);

    strictLogStore.logSearcher.search(
        TEST_DATASET_NAME,
        2,
        QueryBuilderUtil.generateQueryBuilder(
            "car", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
        null,
        createGenericDateHistogramAggregatorFactoriesBuilder());

    // Add another message to search, refresh but don't commit.
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(4, "apple baby car", time.plusSeconds(4)));
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(4);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);

    // Item shows up in search without commit.
    SearchResult<LogMessage> babies =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            2,
            QueryBuilderUtil.generateQueryBuilder(
                "baby", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(babies.hits.size()).isEqualTo(2);
    assertThat(babies.hits.stream().map(m -> m.getId()).collect(Collectors.toList()))
        .isEqualTo(Arrays.asList("Message4", "Message2"));

    // Commit now
    strictLogStore.logStore.commit();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(4);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(3);
  }

  @Test
  public void testFullIndexSearch() throws IOException {
    loadTestData(Instant.now());

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(allIndexItems.internalAggregation);
    // assertThat(histogram.getTargetBuckets()).isEqualTo(1);

    assertThat(histogram.getBuckets().size()).isEqualTo(4);
    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(1).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(2).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(3).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testAggregationWithScripting() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> scriptNull =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createAverageAggregatorFactoriesBuilder("1", TEST_SOURCE_LONG_PROPERTY, 0, null));
    assertThat(((InternalAvg) scriptNull.internalAggregation).value()).isEqualTo(3.25);

    SearchResult<LogMessage> scriptEmpty =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createAverageAggregatorFactoriesBuilder("1", TEST_SOURCE_LONG_PROPERTY, 0, ""));
    assertThat(((InternalAvg) scriptEmpty.internalAggregation).value()).isEqualTo(3.25);

    SearchResult<LogMessage> scripted =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createAverageAggregatorFactoriesBuilder(
                "1", TEST_SOURCE_LONG_PROPERTY, 0, "return 9;"));
    assertThat(((InternalAvg) scripted.internalAggregation).value()).isEqualTo(9);
  }

  @Test
  public void testFilterAggregations() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);

    SearchResult<LogMessage> scriptNull =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createFiltersAggregatorFactoriesBuilder(
                "1",
                List.of(),
                Map.of(
                    "foo",
                    QueryBuilderUtil.generateQueryBuilder(
                        String.format(
                            "%s:<=%s",
                            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                            time.plusSeconds(2).toEpochMilli()),
                        time.toEpochMilli(),
                        time.plusSeconds(2).toEpochMilli()),
                    "bar",
                    QueryBuilderUtil.generateQueryBuilder(
                        String.format(
                            "%s:>%s",
                            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                            time.plusSeconds(2).toEpochMilli()),
                        time.plusSeconds(2).toEpochMilli(),
                        time.plusSeconds(10).toEpochMilli()))));

    assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().size()).isEqualTo(2);
    assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().get(0).getDocCount())
        .isEqualTo(2);
    assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().get(0).getKey())
        .isIn(List.of("foo", "bar"));
    assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().get(1).getDocCount())
        .isEqualTo(2);
    assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().get(1).getKey())
        .isIn(List.of("foo", "bar"));
    assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().get(0).getKey())
        .isNotEqualTo(
            ((InternalFilters) scriptNull.internalAggregation).getBuckets().get(1).getKey());
  }

  @Test
  public void testFullIndexSearchForMinAgg() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createMinAggregatorFactoriesBuilder(
                "test", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "0", null));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalMin internalMin =
        (InternalMin) Objects.requireNonNull(allIndexItems.internalAggregation);

    assertThat(Double.valueOf(internalMin.getValue()).longValue()).isEqualTo(time.toEpochMilli());
  }

  @Test
  public void testFullIndexSearchForMaxAgg() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createMaxAggregatorFactoriesBuilder(
                "test", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "0", null));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalMax internalMax =
        (InternalMax) Objects.requireNonNull(allIndexItems.internalAggregation);

    // 4 seconds because of test data
    assertThat(Double.valueOf(internalMax.getValue()).longValue())
        .isEqualTo(time.plus(4, ChronoUnit.SECONDS).toEpochMilli());
  }

  @Test
  public void testFullIndexSearchForSumAgg() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createSumAggregatorFactoriesBuilder("test", TEST_SOURCE_LONG_PROPERTY, "0", null));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalSum internalSum =
        (InternalSum) Objects.requireNonNull(allIndexItems.internalAggregation);

    // 1, 3, 4, 5
    assertThat(internalSum.getValue()).isEqualTo(13);
  }

  @Test
  public void testFullIndexSearchForExtendedStatsAgg() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createExtendedStatsAggregatorFactoriesBuilder(
                "test", TEST_SOURCE_LONG_PROPERTY, "0", null, null));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalExtendedStats internalExtendedStats =
        (InternalExtendedStats) Objects.requireNonNull(allIndexItems.internalAggregation);

    // 1, 3, 4, 5
    assertThat(internalExtendedStats).isNotNull();
    assertThat(internalExtendedStats.getCount()).isEqualTo(4);
    assertThat(internalExtendedStats.getMax()).isEqualTo(5);
    assertThat(internalExtendedStats.getMin()).isEqualTo(1);
    assertThat(internalExtendedStats.getSum()).isEqualTo(13);
    assertThat(internalExtendedStats.getAvg()).isEqualTo(3.25);
    assertThat(internalExtendedStats.getSumOfSquares()).isEqualTo(51);
    assertThat(internalExtendedStats.getVariance()).isEqualTo(2.1875);
  }

  @Test
  public void testTermsAggregation() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createTermsAggregatorFactoriesBuilder(
                "1",
                List.of(),
                TEST_SOURCE_STRING_PROPERTY,
                "foo",
                10,
                0,
                Map.of("_count", "asc")));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    StringTerms stringTerms = (StringTerms) allIndexItems.internalAggregation;
    assertThat(stringTerms.getBuckets().size()).isEqualTo(4);

    List<String> bucketKeys =
        stringTerms.getBuckets().stream().map(bucket -> (String) bucket.getKey()).toList();
    assertThat(bucketKeys.contains("String-1")).isTrue();
    assertThat(bucketKeys.contains("String-3")).isTrue();
    assertThat(bucketKeys.contains("String-4")).isTrue();
    assertThat(bucketKeys.contains("String-5")).isTrue();
  }

  @Test
  public void testTermsAggregationMissingValues() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createTermsAggregatorFactoriesBuilder(
                "1", List.of(), "thisFieldDoesNotExist", "foo", 10, 0, Map.of("_count", "asc")));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    StringTerms stringTerms = (StringTerms) allIndexItems.internalAggregation;
    assertThat(stringTerms.getBuckets().size()).isEqualTo(1);
    assertThat(stringTerms.getBuckets().get(0).getKey()).isEqualTo("foo");
  }

  @Test
  public void testFullTextSearch() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);

    Trace.KeyValue customField =
        Trace.KeyValue.newBuilder()
            .setVInt32(1234)
            .setKey("field1")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time, List.of(customField)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    // Search using _all field.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("_all:apple", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);
    // Default all field search.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("Message1", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);

    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, "apple baby", time.plusSeconds(4), List.of(customField)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    // Search using _all field.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("_all:baby", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("_all:1234", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);
    // Default all field search.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("baby", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("1234", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, "baby car 1234", time.plusSeconds(4)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    // Search using _all field.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("_all:baby", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("_all:1234", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(3);
    // Default all field search.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("baby", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("1234", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(3);

    // empty string
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(3);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("app*", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);

    // Returns baby or car, 2 messages.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("baby car", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);

    // Test numbers
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("apple 1234", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(3);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("123", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(0);
  }

  @Test
  public void testDisabledFullTextSearch() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    Trace.KeyValue field1Tag =
        Trace.KeyValue.newBuilder()
            .setVInt32(1234)
            .setKey("field1")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();

    strictLogStoreWithoutFts.logStore.addMessage(
        SpanUtil.makeSpan(1, "apple", time.plusSeconds(4), List.of(field1Tag)));

    strictLogStoreWithoutFts.logStore.addMessage(
        SpanUtil.makeSpan(2, "apple baby", time.plusSeconds(4), List.of(field1Tag)));

    strictLogStoreWithoutFts.logStore.addMessage(
        SpanUtil.makeSpan(3, "baby car 1234", time.plusSeconds(4)));
    strictLogStoreWithoutFts.logStore.commit();
    strictLogStoreWithoutFts.logStore.refresh();

    assertThat(
            strictLogStoreWithoutFts
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("_all:baby", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isZero();

    assertThat(
            strictLogStoreWithoutFts
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("_all:1234", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isZero();

    assertThat(
            strictLogStoreWithoutFts
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("baby", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(0);

    // empty string
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isZero();

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("app*", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isZero();

    // Returns baby or car, 2 messages.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("baby car", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isZero();

    // Test numbers
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("apple 1234", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isZero();

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("123", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isZero();
  }

  @Test
  @Disabled // todo - re-enable when multi-tenancy is supported - slackhq/astra/issues/223
  public void testMissingIndexSearch() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME + "miss",
            1000,
            QueryBuilderUtil.generateQueryBuilder("apple", 0L, MAX_TIME),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());

    assertThat(allIndexItems.hits.size()).isEqualTo(0);

    InternalAutoDateHistogram histogram =
        (InternalAutoDateHistogram) Objects.requireNonNull(allIndexItems.internalAggregation);
    assertThat(histogram.getTargetBuckets()).isEqualTo(1);

    assertThat(histogram.getBuckets().size()).isEqualTo(4);
    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(1).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(2).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(3).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testNoResultQuery() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> elephants =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("elephant", 0L, MAX_TIME),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(elephants.hits.size()).isEqualTo(0);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(elephants.internalAggregation);
    assertThat(histogram.getBuckets().size()).isEqualTo(0);
  }

  @Test
  public void testSearchAndNoStats() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);
    SearchResult<LogMessage> results =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            100,
            QueryBuilderUtil.generateQueryBuilder(
                "_id:Message3 OR _id:Message4",
                time.toEpochMilli(),
                time.plusSeconds(10).toEpochMilli()),
            null,
            null);
    assertThat(results.hits.size()).isEqualTo(2);
    assertThat(results.internalAggregation).isNull();
  }

  @Test
  public void testSearchOnlyHistogram() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);
    SearchResult<LogMessage> babies =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            0,
            QueryBuilderUtil.generateQueryBuilder(
                "_id:Message3 OR _id:Message4",
                time.toEpochMilli(),
                time.plusSeconds(10).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(babies.hits.size()).isEqualTo(0);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(babies.internalAggregation);
    assertThat(histogram.getBuckets().size()).isEqualTo(2);

    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(1).getDocCount()).isEqualTo(1);

    assertThat(
            Long.parseLong(histogram.getBuckets().get(0).getKeyAsString()) >= time.toEpochMilli())
        .isTrue();
    assertThat(
            Long.parseLong(histogram.getBuckets().get(1).getKeyAsString())
                <= time.plusSeconds(10).toEpochMilli())
        .isTrue();
  }

  @Test
  public void testEmptyIndexName() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    "",
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("test", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder()));
  }

  @Test
  public void testNullIndexName() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    null,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("test", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder()));
  }

  @Test
  public void testSearchOrHistogramQuery() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    TEST_DATASET_NAME,
                    0,
                    QueryBuilderUtil.generateQueryBuilder(
                        "test", time.toEpochMilli(), time.plusSeconds(1).toEpochMilli()),
                    null,
                    null));
  }

  @Test
  public void testNegativeHitCount() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    TEST_DATASET_NAME,
                    -1,
                    QueryBuilderUtil.generateQueryBuilder(
                        "test", time.toEpochMilli(), time.plusSeconds(1).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder()));
  }

  @Test
  public void testQueryParseError() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    TEST_DATASET_NAME,
                    1,
                    QueryBuilderUtil.generateQueryBuilder(
                        "/", time.toEpochMilli(), time.plusSeconds(1).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder()));
  }

  @Test
  public void testConcurrentSearches() throws InterruptedException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    AtomicInteger searchFailures = new AtomicInteger(0);
    AtomicInteger statsFailures = new AtomicInteger(0);
    AtomicInteger searchExceptions = new AtomicInteger(0);
    AtomicInteger successfulRuns = new AtomicInteger(0);

    Runnable searchRun =
        () -> {
          for (int i = 0; i < 100; i++) {
            try {
              SearchResult<LogMessage> babies =
                  strictLogStore.logSearcher.search(
                      TEST_DATASET_NAME,
                      100,
                      QueryBuilderUtil.generateQueryBuilder(
                          "_id:Message3 OR _id:Message4", 0L, MAX_TIME),
                      null,
                      createGenericDateHistogramAggregatorFactoriesBuilder());
              if (babies.hits.size() != 2) {
                searchFailures.addAndGet(1);
              } else {
                successfulRuns.addAndGet(1);
              }
            } catch (Exception e) {
              searchExceptions.addAndGet(1);
            }
          }
        };

    Thread t1 = new Thread(searchRun);
    Thread t2 = new Thread(searchRun);
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    assertThat(searchExceptions.get()).isEqualTo(0);
    assertThat(statsFailures.get()).isEqualTo(0);
    assertThat(searchFailures.get()).isEqualTo(0);
    assertThat(successfulRuns.get()).isEqualTo(200);
  }

  @Test
  public void testSearchById() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);
    SearchResult<LogMessage> index =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "_id:Message1", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(index.hits.size()).isEqualTo(1);
  }
}
