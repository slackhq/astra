package com.slack.astra.graphApi;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Path;
import com.slack.astra.server.AstraQueryServiceBase;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")

/*
  APIs for exposing traces and their spans as subgraph dependencies.
*/
public class GraphService {
  private static final Logger LOG = LoggerFactory.getLogger(GraphService.class);
  private final AstraQueryServiceBase searcher;

  private static final ObjectMapper objectMapper =
      JsonMapper.builder()
          // sort alphabetically for easier test asserts
          .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
          // don't serialize null values or empty maps
          .serializationInclusion(JsonInclude.Include.NON_EMPTY)
          .build();

  public GraphService(AstraQueryServiceBase searcher) {
    this.searcher = searcher;
  }

  @Get
  @Path("/api/v1/trace/{traceId}/subgraph")
  public HttpResponse getSubgraph(@Param("traceId") String traceId) throws IOException {
    String output = "[]";
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }
}
