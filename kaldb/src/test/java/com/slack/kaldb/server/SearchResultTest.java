package com.slack.kaldb.server;

import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.slack.kaldb.histogram.HistogramBucket;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.SearchResultUtils;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.testlib.MessageUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Test;

public class SearchResultTest {

  @Test
  public void testSearchResultObjectConversions() throws Exception {
    Tracing.newBuilder().build();
    List<LogMessage> logMessages = new ArrayList<>();
    Random random = new Random();

    int numDocs = random.nextInt(10);
    // if we ever fail easy to repro - ideally we want to use a test framework like lucene which
    // gives us a test seed
    System.out.println("numDocs=" + numDocs);
    for (int i = 0; i < numDocs; i++) {
      LogMessage logMessage = MessageUtil.makeMessage(i);
      logMessages.add(logMessage);
    }

    List<HistogramBucket> buckets = new ArrayList<>();
    buckets.add(new HistogramBucket(1, 2));

    SearchResult<LogMessage> searchResult =
        new SearchResult<>(logMessages, 1, 1000, buckets, 10, 2, 1, 5);
    KaldbSearch.SearchResult protoSearchResult =
        SearchResultUtils.toSearchResultProto(searchResult);

    assertThat(protoSearchResult.getHitsCount()).isEqualTo(numDocs);
    assertThat(protoSearchResult.getTookMicros()).isEqualTo(1);
    assertThat(protoSearchResult.getTotalCount()).isEqualTo(1000);
    assertThat(protoSearchResult.getTotalSnapshots()).isEqualTo(10);
    assertThat(protoSearchResult.getSkippedSnapshots()).isEqualTo(2);
    assertThat(protoSearchResult.getFailedSnapshots()).isEqualTo(1);
    assertThat(protoSearchResult.getSuccessfulSnapshots()).isEqualTo(5);
    assertThat(protoSearchResult.getBucketsCount()).isEqualTo(1);

    SearchResult<LogMessage> convertedSearchResult =
        SearchResultUtils.fromSearchResultProto(protoSearchResult);

    assertThat(convertedSearchResult).isEqualTo(searchResult);
  }
}
