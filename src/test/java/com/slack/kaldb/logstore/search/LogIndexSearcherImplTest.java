package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.COMMITS_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.REFRESHES_COUNTER;
import static com.slack.kaldb.testlib.MessageUtil.TEST_INDEX_NAME;
import static com.slack.kaldb.testlib.MessageUtil.makeMessageWithIndexAndTimestamp;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.MAX_TIME;
import static com.slack.kaldb.testlib.TimeUtil.timeEpochMs;
import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;

public class LogIndexSearcherImplTest {

  @Rule
  public TemporaryLogStoreAndSearcherRule strictLogStore =
      new TemporaryLogStoreAndSearcherRule(false);

  public LogIndexSearcherImplTest() throws IOException {}

  private void loadTestData(LocalDateTime time) {
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(1, "apple", TEST_INDEX_NAME, time));
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(2, "baby", "new" + TEST_INDEX_NAME, time.plusSeconds(1)));
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(3, "apple baby", TEST_INDEX_NAME, time.plusSeconds(2)));
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(4, "car", TEST_INDEX_NAME, time.plusSeconds(3)));
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(
            5, "apple baby car", TEST_INDEX_NAME, time.plusSeconds(4)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
  }

  @Test
  public void testTimeBoundSearch() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    strictLogStore.logStore.addMessage(makeMessageWithIndexAndTimestamp(1, "test1", "test", time));
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(1, "test1", "test", time.plusSeconds(100)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getCount(REFRESHES_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(1);

    // Start inclusive.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "test", "test1", timeEpochMs(time), timeEpochMs(time.plusSeconds(10)), 1000, 1)
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
                    timeEpochMs(time.minusSeconds(1)),
                    timeEpochMs(time.plusSeconds(90)),
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
                    "test", "test1", timeEpochMs(time), timeEpochMs(time.plusSeconds(100)), 1000, 1)
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
                    timeEpochMs(time.minusSeconds(1)),
                    timeEpochMs(time.plusSeconds(1000)),
                    1000,
                    1)
                .hits
                .size())
        .isEqualTo(2);
  }

  @Test
  public void testIndexBoundSearch() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    strictLogStore.logStore.addMessage(makeMessageWithIndexAndTimestamp(1, "test1", "idx", time));
    strictLogStore.logStore.addMessage(makeMessageWithIndexAndTimestamp(1, "test1", "idx1", time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getCount(REFRESHES_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(1);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx",
                    "test1",
                    timeEpochMs(time.minusSeconds(1)),
                    timeEpochMs(time.plusSeconds(10)),
                    100,
                    1)
                .hits
                .size())
        .isEqualTo(1);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx1", "test1", timeEpochMs(time), timeEpochMs(time.plusSeconds(10)), 100, 1)
                .hits
                .size())
        .isEqualTo(1);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx12", "test1", timeEpochMs(time), timeEpochMs(time.plusSeconds(10)), 100, 1)
                .hits
                .size())
        .isEqualTo(0);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx1", "test", timeEpochMs(time), timeEpochMs(time.plusSeconds(10)), 100, 1)
                .hits
                .size())
        .isEqualTo(0);
  }

  @Test
  public void testSearchMultipleItemsAndIndices() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);
    SearchResult<LogMessage> babies =
        strictLogStore.logSearcher.search(
            TEST_INDEX_NAME, "baby", timeEpochMs(time), timeEpochMs(time.plusSeconds(2)), 10, 1);
    assertThat(babies.hits.size()).isEqualTo(1);
    assertThat(babies.totalCount).isEqualTo(1);
    assertThat(babies.buckets.size()).isEqualTo(1);
    assertThat(babies.buckets.get(0).getCount()).isEqualTo(1);
  }

  @Test
  public void testTopKQuery() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);

    SearchResult<LogMessage> apples =
        strictLogStore.logSearcher.search(
            TEST_INDEX_NAME, "apple", timeEpochMs(time), timeEpochMs(time.plusSeconds(100)), 2, 1);
    assertThat(apples.hits.stream().map(m -> m.id).collect(Collectors.toList()))
        .isEqualTo(Arrays.asList("5", "3"));
    assertThat(apples.hits.size()).isEqualTo(2);
    assertThat(apples.totalCount).isEqualTo(3); // total count is 3, hits is 2.
    assertThat(apples.buckets.size()).isEqualTo(1);
    assertThat(apples.buckets.get(0).getCount()).isEqualTo(3);
  }

  @Test
  public void testSearchMultipleCommits() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);

    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(1, "apple", TEST_INDEX_NAME, time));
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(2, "apple baby", TEST_INDEX_NAME, time.plusSeconds(2)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> baby =
        strictLogStore.logSearcher.search(
            TEST_INDEX_NAME, "baby", timeEpochMs(time), timeEpochMs(time.plusSeconds(10)), 2, 1);
    assertThat(baby.hits.size()).isEqualTo(1);
    assertThat(baby.hits.get(0).id).isEqualTo("2");
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getCount(REFRESHES_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getCount(COMMITS_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(1);

    // Add car but don't commit. So, no results for car.
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(3, "car", TEST_INDEX_NAME, time.plusSeconds(3)));

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getCount(REFRESHES_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getCount(COMMITS_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(1);

    SearchResult<LogMessage> car =
        strictLogStore.logSearcher.search(
            TEST_INDEX_NAME, "car", timeEpochMs(time), timeEpochMs(time.plusSeconds(10)), 2, 1);
    assertThat(car.totalCount).isEqualTo(0);

    // Commit but no refresh. Item is still not available for search.
    strictLogStore.logStore.commit();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getCount(REFRESHES_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getCount(COMMITS_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);

    SearchResult<LogMessage> carAfterCommit =
        strictLogStore.logSearcher.search(
            TEST_INDEX_NAME, "car", timeEpochMs(time), timeEpochMs(time.plusSeconds(10)), 2, 1);
    assertThat(carAfterCommit.totalCount).isEqualTo(0);

    // Car can be searched after refresh.
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getCount(REFRESHES_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getCount(COMMITS_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);

    SearchResult<LogMessage> carAfterRefresh =
        strictLogStore.logSearcher.search(
            TEST_INDEX_NAME, "car", timeEpochMs(time), timeEpochMs(time.plusSeconds(10)), 2, 1);
    assertThat(carAfterRefresh.totalCount).isEqualTo(1);

    // Add another message to search, refresh but don't commit.
    strictLogStore.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(
            4, "apple baby car", TEST_INDEX_NAME, time.plusSeconds(4)));
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(4);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getCount(REFRESHES_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(COMMITS_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);

    // Item shows up in search without commit.
    SearchResult<LogMessage> babies =
        strictLogStore.logSearcher.search(
            TEST_INDEX_NAME, "baby", timeEpochMs(time), timeEpochMs(time.plusSeconds(10)), 2, 1);
    assertThat(babies.hits.size()).isEqualTo(2);
    assertThat(babies.totalCount).isEqualTo(2);
    assertThat(babies.hits.stream().map(m -> m.id).collect(Collectors.toList()))
        .isEqualTo(Arrays.asList("4", "2"));

    // Commit now
    strictLogStore.logStore.commit();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(4);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getCount(REFRESHES_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(COMMITS_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
  }

  @Test
  public void testFullIndexSearch() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(TEST_INDEX_NAME, "", 0, MAX_TIME, 1000, 1);

    assertThat(allIndexItems.hits.size()).isEqualTo(4);
    assertThat(allIndexItems.totalCount).isEqualTo(4);
    assertThat(allIndexItems.buckets.size()).isEqualTo(1);
    assertThat(allIndexItems.buckets.get(0).getCount()).isEqualTo(4);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullSearchString() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);

    strictLogStore.logSearcher.search(TEST_INDEX_NAME + "miss", null, 0, MAX_TIME, 1000, 1);
  }

  @Test
  public void testMissingIndexSearch() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(TEST_INDEX_NAME + "miss", "apple", 0, MAX_TIME, 1000, 1);

    assertThat(allIndexItems.hits.size()).isEqualTo(0);
    assertThat(allIndexItems.totalCount).isEqualTo(0);
    assertThat(allIndexItems.buckets.size()).isEqualTo(1);
    assertThat(allIndexItems.buckets.get(0).getCount()).isEqualTo(0);
  }

  @Test
  public void testNoResultQuery() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);

    SearchResult<LogMessage> elephants =
        strictLogStore.logSearcher.search(TEST_INDEX_NAME, "elephant", 0, MAX_TIME, 1000, 1);
    assertThat(elephants.hits.size()).isEqualTo(0);
    assertThat(elephants.totalCount).isEqualTo(0);
    assertThat(elephants.buckets.size()).isEqualTo(1);
    assertThat(elephants.buckets.get(0).getCount()).isEqualTo(0);
  }

  @Test
  public void testSearchAndNoStats() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);
    SearchResult<LogMessage> babies =
        strictLogStore.logSearcher.search(
            TEST_INDEX_NAME, "baby", timeEpochMs(time), timeEpochMs(time.plusSeconds(10)), 100, 0);
    assertThat(babies.hits.size()).isEqualTo(2);
    assertThat(babies.totalCount).isEqualTo(2);
    assertThat(babies.buckets.size()).isEqualTo(0);
  }

  @Test
  public void testSearchOnlyHistogram() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);
    SearchResult<LogMessage> babies =
        strictLogStore.logSearcher.search(
            TEST_INDEX_NAME, "baby", timeEpochMs(time), timeEpochMs(time.plusSeconds(10)), 0, 1);
    assertThat(babies.hits.size()).isEqualTo(0);
    assertThat(babies.totalCount).isEqualTo(2);
    assertThat(babies.buckets.size()).isEqualTo(1);
    assertThat(babies.buckets.get(0).getHigh()).isEqualTo(timeEpochMs(time.plusSeconds(10)));
    assertThat(babies.buckets.get(0).getLow()).isEqualTo(timeEpochMs(time));
    assertThat(babies.buckets.get(0).getCount()).isEqualTo(2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyIndexName() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);
    strictLogStore.logSearcher.search("", "test", 0, MAX_TIME, 1000, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullIndexName() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);
    strictLogStore.logSearcher.search(null, "test", 0, MAX_TIME, 1000, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidStartTime() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);
    strictLogStore.logSearcher.search(TEST_INDEX_NAME, "test", -1L, MAX_TIME, 1000, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidEndTime() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);
    strictLogStore.logSearcher.search(TEST_INDEX_NAME, "test", 0, -1L, 1000, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTimeRange() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);
    strictLogStore.logSearcher.search(
        TEST_INDEX_NAME, "test", timeEpochMs(time), timeEpochMs(time.minusSeconds(1)), 1000, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSearchOrHistogramQuery() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);
    strictLogStore.logSearcher.search(
        TEST_INDEX_NAME, "test", timeEpochMs(time), timeEpochMs(time.plusSeconds(1)), 0, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeHitCount() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);
    strictLogStore.logSearcher.search(
        TEST_INDEX_NAME, "test", timeEpochMs(time), timeEpochMs(time.plusSeconds(1)), -1, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeHistogramBuckets() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);
    strictLogStore.logSearcher.search(
        TEST_INDEX_NAME, "test", timeEpochMs(time), timeEpochMs(time.plusSeconds(1)), 1, -2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueryParseError() {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
    loadTestData(time);
    strictLogStore.logSearcher.search(
        TEST_INDEX_NAME, "/", timeEpochMs(time), timeEpochMs(time.plusSeconds(1)), 1, 1);
  }

  @Test
  public void testConcurrentSearches() throws InterruptedException {
    LocalDateTime time = LocalDateTime.ofEpochSecond(1593365471, 0, ZoneOffset.UTC);
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
                  strictLogStore.logSearcher.search(TEST_INDEX_NAME, "baby", 0, MAX_TIME, 100, 1);
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
