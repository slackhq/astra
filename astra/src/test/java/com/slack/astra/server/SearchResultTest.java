package com.slack.astra.server;

import static com.slack.astra.util.AggregatorFactoriesUtil.createGenericDateHistogramAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.opensearch.OpenSearchAdapter;
import com.slack.astra.logstore.opensearch.OpenSearchInternalAggregation;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.logstore.search.SearchResultUtils;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.testlib.MessageUtil;
import com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;

public class SearchResultTest {

  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension logStoreAndSearcherRule =
      new TemporaryLogStoreAndSearcherExtension(false);

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
        openSearchAdapter.buildAggregatorFromFactory(
            logStoreAndSearcherRule.logStore.getSearcherManager().acquire(),
            createGenericDateHistogramAggregatorFactoriesBuilder(),
            null);
    InternalAggregation internalAggregation = dateHistogramAggregation.buildTopLevel();
    SearchResult<LogMessage> searchResult =
        new SearchResult<>(logMessages, 1, 1, 5, 7, 7, internalAggregation);
    AstraSearch.SearchResult protoSearchResult =
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
