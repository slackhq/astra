package com.slack.astra.graphApi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.slack.astra.server.AstraQueryServiceBase;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class GraphServiceTest {
  @Mock private AstraQueryServiceBase searcher;
  private GraphService graphService;

  @BeforeEach
  public void setup() throws IOException {
    graphService = spy(new GraphService(searcher));
  }

  @Test
  public void testGetSubgraphByTraceId_emptyResult() throws Exception {
    String traceId = "test_trace_1";

    HttpResponse response = graphService.getSubgraph(traceId);
    AggregatedHttpResponse aggregatedResponse = response.aggregate().join();

    assertEquals(HttpStatus.OK, aggregatedResponse.status());
    assertEquals("[]", aggregatedResponse.contentUtf8());
  }
}
