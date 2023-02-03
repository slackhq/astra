package com.slack.kaldb.server;

import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.slack.kaldb.logstore.LogMessage;
<<<<<<< bburkholder/opensearch-serialize
import com.slack.kaldb.logstore.opensearch.OpenSearchAggregationAdapter;
=======
import com.slack.kaldb.logstore.opensearch.OpensearchShim;
>>>>>>> Initial cleanup
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.SearchResultUtils;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.testlib.MessageUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Test;
<<<<<<< bburkholder/opensearch-serialize
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
=======
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.InternalAvg;
>>>>>>> Initial cleanup

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

<<<<<<< bburkholder/opensearch-serialize
    Aggregator dateHistogramAggregation =
        OpenSearchAggregationAdapter.buildAutoDateHistogramAggregator(10);
    InternalAggregation internalAggregation = dateHistogramAggregation.buildTopLevel();
    SearchResult<LogMessage> searchResult =
<<<<<<< bburkholder/opensearch-serialize
        new SearchResult<>(logMessages, 1, 1000, 1, 5, 7, 7, internalAggregation);
=======
        new SearchResult<>(logMessages, 1, 1000, buckets, 1, 5, 7, 7, null);
>>>>>>> Test aggs all the way out
=======
    InternalAggregation internalAggregation =
        new InternalAvg("avg", 10, 10, DocValueFormat.RAW, Map.of("foo", "bar"));

    SearchResult<LogMessage> searchResult =
        new SearchResult<>(logMessages, 1, 1000, 1, 5, 7, 7, internalAggregation);
>>>>>>> Initial cleanup
    KaldbSearch.SearchResult protoSearchResult =
        SearchResultUtils.toSearchResultProto(searchResult);

    assertThat(protoSearchResult.getHitsCount()).isEqualTo(numDocs);
    assertThat(protoSearchResult.getTookMicros()).isEqualTo(1);
    assertThat(protoSearchResult.getTotalCount()).isEqualTo(1000);
    assertThat(protoSearchResult.getFailedNodes()).isEqualTo(1);
    assertThat(protoSearchResult.getTotalNodes()).isEqualTo(5);
    assertThat(protoSearchResult.getTotalSnapshots()).isEqualTo(7);
    assertThat(protoSearchResult.getSnapshotsWithReplicas()).isEqualTo(7);
    assertThat(protoSearchResult.getInternalAggregations().toByteArray())
<<<<<<< bburkholder/opensearch-serialize
        .isEqualTo(OpenSearchAggregationAdapter.toByteArray(internalAggregation));
=======
        .isEqualTo(OpensearchShim.toByteArray(internalAggregation));
>>>>>>> Initial cleanup

    SearchResult<LogMessage> convertedSearchResult =
        SearchResultUtils.fromSearchResultProto(protoSearchResult);

    assertThat(convertedSearchResult).isEqualTo(searchResult);
  }
}
