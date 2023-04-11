package com.slack.kaldb.server;

import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.opensearch.OpenSearchAdapter;
import com.slack.kaldb.logstore.opensearch.OpenSearchInternalAggregation;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.SearchResultUtils;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.junit.Rule;
import org.junit.Test;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;

public class SearchResultTest {

  @Rule
  public TemporaryLogStoreAndSearcherRule logStoreAndSearcherRule =
      new TemporaryLogStoreAndSearcherRule(false);

  public SearchResultTest() throws IOException {}

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
    OpenSearchAdapter openSearchAdapter = new OpenSearchAdapter(Map.of());

    Aggregator dateHistogramAggregation =
        openSearchAdapter.buildAggregatorUsingContext(
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
            logStoreAndSearcherRule.logStore.getSearcherManager().acquire(),
            null);
    InternalAggregation internalAggregation = dateHistogramAggregation.buildTopLevel();
    SearchResult<LogMessage> searchResult =
        new SearchResult<>(logMessages, 1, 1, 5, 7, 7, internalAggregation);
    KaldbSearch.SearchResult protoSearchResult =
        SearchResultUtils.toSearchResultProto(searchResult);

    assertThat(protoSearchResult.getHitsCount()).isEqualTo(numDocs);
    assertThat(protoSearchResult.getTookMicros()).isEqualTo(1);
    assertThat(protoSearchResult.getFailedNodes()).isEqualTo(1);
    assertThat(protoSearchResult.getTotalNodes()).isEqualTo(5);
    assertThat(protoSearchResult.getTotalSnapshots()).isEqualTo(7);
    assertThat(protoSearchResult.getSnapshotsWithReplicas()).isEqualTo(7);
    assertThat(protoSearchResult.getInternalAggregations().toByteArray())
        .isEqualTo(OpenSearchInternalAggregation.toByteArray(internalAggregation));

    SearchResult<LogMessage> convertedSearchResult =
        SearchResultUtils.fromSearchResultProto(protoSearchResult);

    assertThat(convertedSearchResult).isEqualTo(searchResult);
  }
}
