package com.slack.astra.zipkinApi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import brave.Span;
import brave.Tracing;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.server.AstraQueryServiceBase;
import java.io.IOException;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

public class ZipkinServiceTest {
  @Mock private AstraQueryServiceBase searcher;
  private ZipkinService zipkinService;
  private AstraSearch.SearchResult mockSearchResult;

  @BeforeEach
  public void setup() throws IOException {
    MockitoAnnotations.openMocks(this);
    zipkinService = spy(new ZipkinService(searcher));
    // Build mockSearchResult
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonNode =
        objectMapper.readTree(Resources.getResource("zipkinApi/search_result.json"));
    String jsonString = objectMapper.writeValueAsString(jsonNode);
    AstraSearch.SearchResult.Builder builder = AstraSearch.SearchResult.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    mockSearchResult = builder.build();
  }

  @Test
  public void testGetTraceByTraceId_onlyTraceIdProvided() throws Exception {

    try (MockedStatic<Tracing> mockedTracing = mockStatic(Tracing.class)) {
      brave.Tracer mockTracer = mock(brave.Tracer.class);
      Span mockSpan = mock(Span.class);

      mockedTracing.when(Tracing::currentTracer).thenReturn(mockTracer);
      when(mockTracer.currentSpan()).thenReturn(mockSpan);
      String traceId = "test_trace_1";

      when(searcher.doSearch(any())).thenReturn(mockSearchResult);

      // Act
      HttpResponse response =
          zipkinService.getTraceByTraceId(
              traceId, Optional.empty(), Optional.empty(), Optional.empty());
      AggregatedHttpResponse aggregatedResponse = response.aggregate().join();

      // Assert
      assertEquals(HttpStatus.OK, aggregatedResponse.status());
    }
  }
}
