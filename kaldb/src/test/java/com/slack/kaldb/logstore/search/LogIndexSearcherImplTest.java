package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.COMMITS_TIMER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.REFRESHES_TIMER;
import static com.slack.kaldb.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.kaldb.testlib.MessageUtil.makeMessageWithIndexAndTimestamp;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getTimerCount;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class LogIndexSearcherImplTest {

  @Rule
  public TemporaryLogStoreAndSearcherRule strictLogStore =
      new TemporaryLogStoreAndSearcherRule(false, true);

  @Rule
  public TemporaryLogStoreAndSearcherRule strictLogStoreWithoutFts =
      new TemporaryLogStoreAndSearcherRule(false, false);

  public LogIndexSearcherImplTest() throws IOException {}

  @BeforeClass
  public static void beforeClass() throws Exception {
    Tracing.newBuilder().build();
  }

  private void loadTestData(Instant time) {
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(1, "apple", TEST_DATASET_NAME, time));

    // todo - re-enable when multi-tenancy is supported - slackhq/kaldb/issues/223
    // strictLogStore.logStore.addMessage(
    // makeMessageWithIndexAndTimestamp(2, "baby", "new" + TEST_INDEX_NAME, time.plusSeconds(1)));
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(3, "apple baby", TEST_DATASET_NAME, time.plusSeconds(2)));
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(4, "car", TEST_DATASET_NAME, time.plusSeconds(3)));
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(
            5, "apple baby car", TEST_DATASET_NAME, time.plusSeconds(4)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
  }

  @Test
  public void testTimeBoundSearch() {
    Instant time =
        LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC)
            .atZone(ZoneOffset.UTC)
            .toInstant();
    strictLogStore.logStore.addMessage(makeMessageWithIndexAndTimestamp(1, "test1", "test", time));
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(1, "test1", "test", time.plusSeconds(100)));
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
                    "test",
                    "test1",
                    time.toEpochMilli(),
                    time.plusSeconds(10).toEpochMilli(),
                    1000,
                    1)
                .hits
                .size())
        .isEqualTo(1);

    // Extended range still only picking one element.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "test",
                    "test1",
                    time.minusSeconds(1).toEpochMilli(),
                    time.plusSeconds(90).toEpochMilli(),
                    1000,
                    1)
                .hits
                .size())
        .isEqualTo(1);

    // Both ranges are inclusive.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "test",
                    "test1",
                    time.toEpochMilli(),
                    time.plusSeconds(100).toEpochMilli(),
                    1000,
                    1)
                .hits
                .size())
        .isEqualTo(2);

    // Extended range to pick up both events
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "test",
                    "test1",
                    time.minusSeconds(1).toEpochMilli(),
                    time.plusSeconds(1000).toEpochMilli(),
                    1000,
                    1)
                .hits
                .size())
        .isEqualTo(2);
  }

  @Test
  @Ignore // todo - re-enable when multi-tenancy is supported - slackhq/kaldb/issues/223
  public void testIndexBoundSearch() {
    Instant time = Instant.ofEpochSecond(1593365471);
    strictLogStore.logStore.addMessage(makeMessageWithIndexAndTimestamp(1, "test1", "idx", time));
    strictLogStore.logStore.addMessage(makeMessageWithIndexAndTimestamp(1, "test1", "idx1", time));
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
                    "test1",
                    time.minusSeconds(1).toEpochMilli(),
                    time.plusSeconds(10).toEpochMilli(),
                    100,
                    1)
                .hits
                .size())
        .isEqualTo(1);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx1",
                    "test1",
                    time.toEpochMilli(),
                    time.plusSeconds(10).toEpochMilli(),
                    100,
                    1)
                .hits
                .size())
        .isEqualTo(1);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx12",
                    "test1",
                    time.toEpochMilli(),
                    time.plusSeconds(10).toEpochMilli(),
                    100,
                    1)
                .hits
                .size())
        .isEqualTo(0);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx1",
                    "test",
                    time.toEpochMilli(),
                    time.plusSeconds(10).toEpochMilli(),
                    100,
                    1)
                .hits
                .size())
        .isEqualTo(0);
  }

  @Test
  public void testSearchMultipleItemsAndIndices() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    SearchResult<LogMessage> babies =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "baby",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            1);
    assertThat(babies.hits.size()).isEqualTo(1);
    assertThat(babies.totalCount).isEqualTo(1);
    assertThat(babies.buckets.size()).isEqualTo(1);
    assertThat(babies.buckets.get(0).getCount()).isEqualTo(1);
  }

  @Test
  public void testTopKQuery() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> apples =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "apple",
            time.toEpochMilli(),
            time.plusSeconds(100).toEpochMilli(),
            2,
            1);
    assertThat(apples.hits.stream().map(m -> m.id).collect(Collectors.toList()))
        .isEqualTo(Arrays.asList("5", "3"));
    assertThat(apples.hits.size()).isEqualTo(2);
    assertThat(apples.totalCount).isEqualTo(3); // total count is 3, hits is 2.
    assertThat(apples.buckets.size()).isEqualTo(1);
    assertThat(apples.buckets.get(0).getCount()).isEqualTo(3);
  }

  @Test
  public void testSearchMultipleCommits() {
    Instant time = Instant.ofEpochSecond(1593365471);

    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(1, "apple", TEST_DATASET_NAME, time));
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(2, "apple baby", TEST_DATASET_NAME, time.plusSeconds(2)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> baby =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "baby",
            time.toEpochMilli(),
            time.plusSeconds(10).toEpochMilli(),
            2,
            1);
    assertThat(baby.hits.size()).isEqualTo(1);
    assertThat(baby.hits.get(0).id).isEqualTo("2");
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

    // Add car but don't commit. So, no results for car.
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(3, "car", TEST_DATASET_NAME, time.plusSeconds(3)));

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

    SearchResult<LogMessage> car =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "car",
            time.toEpochMilli(),
            time.plusSeconds(10).toEpochMilli(),
            2,
            1);
    assertThat(car.totalCount).isEqualTo(0);

    // Commit but no refresh. Item is still not available for search.
    strictLogStore.logStore.commit();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);

    SearchResult<LogMessage> carAfterCommit =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "car",
            time.toEpochMilli(),
            time.plusSeconds(10).toEpochMilli(),
            2,
            1);
    assertThat(carAfterCommit.totalCount).isEqualTo(0);

    // Car can be searched after refresh.
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);

    SearchResult<LogMessage> carAfterRefresh =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "car",
            time.toEpochMilli(),
            time.plusSeconds(10).toEpochMilli(),
            2,
            1);
    assertThat(carAfterRefresh.totalCount).isEqualTo(1);

    // Add another message to search, refresh but don't commit.
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(
            4, "apple baby car", TEST_DATASET_NAME, time.plusSeconds(4)));
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(4);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);

    // Item shows up in search without commit.
    SearchResult<LogMessage> babies =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "baby",
            time.toEpochMilli(),
            time.plusSeconds(10).toEpochMilli(),
            2,
            1);
    assertThat(babies.hits.size()).isEqualTo(2);
    assertThat(babies.totalCount).isEqualTo(2);
    assertThat(babies.hits.stream().map(m -> m.id).collect(Collectors.toList()))
        .isEqualTo(Arrays.asList("4", "2"));

    // Commit now
    strictLogStore.logStore.commit();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(4);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(3);
  }

  @Test
  public void testFullIndexSearch() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(TEST_DATASET_NAME, "", 0, MAX_TIME, 1000, 1);

    assertThat(allIndexItems.hits.size()).isEqualTo(4);
    assertThat(allIndexItems.totalCount).isEqualTo(4);
    assertThat(allIndexItems.buckets.size()).isEqualTo(1);
    assertThat(allIndexItems.buckets.get(0).getCount()).isEqualTo(4);
  }

  @Test
  public void testFullTextSearch() {
    Instant time = Instant.ofEpochSecond(1593365471);
    final LogMessage msg1 =
        makeMessageWithIndexAndTimestamp(1, "apple", TEST_DATASET_NAME, time.plusSeconds(4));
    msg1.addProperty("field1", "1234");
    strictLogStore.logStore.addMessage(msg1);
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    // Search using _all field.
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "_all:baby", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(0);
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "_all:1234", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(1);
    // Default all field search.
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "baby", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(0);
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "1234", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(1);

    final LogMessage msg2 =
        makeMessageWithIndexAndTimestamp(2, "apple baby", TEST_DATASET_NAME, time.plusSeconds(4));
    msg2.addProperty("field2", "1234");
    strictLogStore.logStore.addMessage(msg2);
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    // Search using _all field.
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "_all:baby", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(1);
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "_all:1234", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(2);
    // Default all field search.
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "baby", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(1);
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "1234", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(2);

    final LogMessage msg3 =
        makeMessageWithIndexAndTimestamp(
            3, "baby car 1234", TEST_DATASET_NAME, time.plusSeconds(4));
    strictLogStore.logStore.addMessage(msg3);
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    // Search using _all field.
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "_all:baby", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(2);
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "_all:1234", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(3);
    // Default all field search.
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "baby", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(2);
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "1234", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(3);

    // empty string
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(3);

    // Currently, returns empty since we don't parse wild card queries.
    // TODO: One we start parsing wild card queries this should return 3.
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, ".*", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(0);

    // Returns baby or car, 2 messages.
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "baby car", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(2);

    // Test numbers
    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "apple 1234", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(3);

    assertThat(
            strictLogStore
                .logSearcher
                .search(TEST_DATASET_NAME, "123", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(0);
  }

  @Test
  public void testDisabledFullTextSearch() {
    Instant time = Instant.ofEpochSecond(1593365471);
    final LogMessage msg1 =
        makeMessageWithIndexAndTimestamp(1, "apple", TEST_DATASET_NAME, time.plusSeconds(4));
    msg1.addProperty("field1", "1234");
    strictLogStoreWithoutFts.logStore.addMessage(msg1);

    final LogMessage msg2 =
        makeMessageWithIndexAndTimestamp(2, "apple baby", TEST_DATASET_NAME, time.plusSeconds(4));
    msg2.addProperty("field2", "1234");
    strictLogStoreWithoutFts.logStore.addMessage(msg2);

    final LogMessage msg3 =
        makeMessageWithIndexAndTimestamp(
            3, "baby car 1234", TEST_DATASET_NAME, time.plusSeconds(4));
    strictLogStoreWithoutFts.logStore.addMessage(msg3);
    strictLogStoreWithoutFts.logStore.commit();
    strictLogStoreWithoutFts.logStore.refresh();

    assertThat(
            strictLogStoreWithoutFts
                .logSearcher
                .search(TEST_DATASET_NAME, "_all:baby", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(0);

    assertThat(
            strictLogStoreWithoutFts
                .logSearcher
                .search(TEST_DATASET_NAME, "_all:1234", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(0);

    // Without the _all field as default.
    assertThat(
            strictLogStoreWithoutFts
                .logSearcher
                .search(TEST_DATASET_NAME, "baby", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(0);

    assertThat(
            strictLogStoreWithoutFts
                .logSearcher
                .search(TEST_DATASET_NAME, "1234", 0, MAX_TIME, 1000, 1)
                .hits
                .size())
        .isEqualTo(0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullSearchString() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    strictLogStore.logSearcher.search(TEST_DATASET_NAME + "miss", null, 0, MAX_TIME, 1000, 1);
  }

  @Test
  @Ignore // todo - re-enable when multi-tenancy is supported - slackhq/kaldb/issues/223
  public void testMissingIndexSearch() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME + "miss", "apple", 0, MAX_TIME, 1000, 1);

    assertThat(allIndexItems.hits.size()).isEqualTo(0);
    assertThat(allIndexItems.totalCount).isEqualTo(0);
    assertThat(allIndexItems.buckets.size()).isEqualTo(1);
    assertThat(allIndexItems.buckets.get(0).getCount()).isEqualTo(0);
  }

  @Test
  public void testNoResultQuery() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> elephants =
        strictLogStore.logSearcher.search(TEST_DATASET_NAME, "elephant", 0, MAX_TIME, 1000, 1);
    assertThat(elephants.hits.size()).isEqualTo(0);
    assertThat(elephants.totalCount).isEqualTo(0);
    assertThat(elephants.buckets.size()).isEqualTo(1);
    assertThat(elephants.buckets.get(0).getCount()).isEqualTo(0);
  }

  @Test
  public void testSearchAndNoStats() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    SearchResult<LogMessage> babies =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "baby",
            time.toEpochMilli(),
            time.plusSeconds(10).toEpochMilli(),
            100,
            0);
    assertThat(babies.hits.size()).isEqualTo(2);
    assertThat(babies.totalCount).isEqualTo(2);
    assertThat(babies.buckets.size()).isEqualTo(0);
  }

  @Test
  public void testSearchOnlyHistogram() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    SearchResult<LogMessage> babies =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "baby",
            time.toEpochMilli(),
            time.plusSeconds(10).toEpochMilli(),
            0,
            1);
    assertThat(babies.hits.size()).isEqualTo(0);
    assertThat(babies.totalCount).isEqualTo(2);
    assertThat(babies.buckets.size()).isEqualTo(1);
    assertThat(babies.buckets.get(0).getHigh()).isEqualTo(time.plusSeconds(10).toEpochMilli());
    assertThat(babies.buckets.get(0).getLow()).isEqualTo(time.toEpochMilli());
    assertThat(babies.buckets.get(0).getCount()).isEqualTo(2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyIndexName() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    strictLogStore.logSearcher.search("", "test", 0, MAX_TIME, 1000, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullIndexName() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    strictLogStore.logSearcher.search(null, "test", 0, MAX_TIME, 1000, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidStartTime() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    strictLogStore.logSearcher.search(TEST_DATASET_NAME, "test", -1L, MAX_TIME, 1000, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidEndTime() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    strictLogStore.logSearcher.search(TEST_DATASET_NAME, "test", 0, -1L, 1000, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTimeRange() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    strictLogStore.logSearcher.search(
        TEST_DATASET_NAME,
        "test",
        time.toEpochMilli(),
        time.minusSeconds(1).toEpochMilli(),
        1000,
        1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSearchOrHistogramQuery() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    strictLogStore.logSearcher.search(
        TEST_DATASET_NAME, "test", time.toEpochMilli(), time.plusSeconds(1).toEpochMilli(), 0, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeHitCount() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    strictLogStore.logSearcher.search(
        TEST_DATASET_NAME, "test", time.toEpochMilli(), time.plusSeconds(1).toEpochMilli(), -1, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeHistogramBuckets() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    strictLogStore.logSearcher.search(
        TEST_DATASET_NAME, "test", time.toEpochMilli(), time.plusSeconds(1).toEpochMilli(), 1, -2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueryParseError() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    strictLogStore.logSearcher.search(
        TEST_DATASET_NAME, "/", time.toEpochMilli(), time.plusSeconds(1).toEpochMilli(), 1, 1);
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
                  strictLogStore.logSearcher.search(TEST_DATASET_NAME, "baby", 0, MAX_TIME, 100, 1);
              if (babies.hits.size() != 2) {
                searchFailures.addAndGet(1);
              } else {
                successfulRuns.addAndGet(1);
              }
              if (babies.totalCount != 2) statsFailures.addAndGet(1);
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
}
