package com.slack.kaldb.logstore.opensearch;

import static com.slack.kaldb.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.kaldb.testlib.MessageUtil.makeMessageWithIndexAndTimestamp;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class OpenSearchQueryParserTest {

  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension logStoreAndSearcherRule =
      new TemporaryLogStoreAndSearcherExtension(false);

  public OpenSearchQueryParserTest() throws IOException {
    Tracing.newBuilder().build();
  }

  @Test
  public void testRangeQueryOnTimestampField() {
    Instant time = Instant.now();
    Instant timeMinus5 = time.minus(5, ChronoUnit.MINUTES);
    logStoreAndSearcherRule.logStore.addMessage(
        makeMessageWithIndexAndTimestamp(3, "new document", TEST_DATASET_NAME, time));
    logStoreAndSearcherRule.logStore.commit();
    logStoreAndSearcherRule.logStore.refresh();
    SearchResult<LogMessage> results =
        logStoreAndSearcherRule.logSearcher.search(
            "dataset",
            "_timesinceepoch:[now-5m TO now+5m]",
            timeMinus5.toEpochMilli(),
            time.toEpochMilli(),
            500,
            null);
    assertThat(results.hits.size()).isEqualTo(1);
  }
}
