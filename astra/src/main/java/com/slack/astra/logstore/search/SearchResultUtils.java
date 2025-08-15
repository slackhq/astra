package com.slack.astra.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogWireMessage;
import com.slack.astra.logstore.opensearch.OpenSearchInternalAggregation;
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.util.JsonUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregatorFactories;

public class SearchResultUtils {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of());
  private static final NamedXContentRegistry namedXContentRegistry =
      new NamedXContentRegistry(searchModule.getNamedXContents());

  public static Map<String, Object> fromValueStruct(AstraSearch.Struct struct) {
    Map<String, Object> returnMap = new HashMap<>();
    struct.getFieldsMap().forEach((key, value) -> returnMap.put(key, fromValueProto(value)));
    return returnMap;
  }

  public static AstraSearch.Struct toStructProto(Map<String, Object> map) {
    Map<String, AstraSearch.Value> valueMap = new HashMap<>();
    map.forEach((key, value) -> valueMap.put(key, toValueProto(value)));
    return AstraSearch.Struct.newBuilder().putAllFields(valueMap).build();
  }

  public static Object fromValueProto(AstraSearch.Value value) {
    if (value.hasNullValue()) {
      return null;
    } else if (value.hasIntValue()) {
      return value.getIntValue();
    } else if (value.hasLongValue()) {
      return value.getLongValue();
    } else if (value.hasDoubleValue()) {
      return value.getDoubleValue();
    } else if (value.hasStringValue()) {
      return value.getStringValue();
    } else if (value.hasBoolValue()) {
      return value.getBoolValue();
    } else if (value.hasStructValue()) {
      return fromValueStruct(value.getStructValue());
    } else if (value.hasListValue()) {
      return value.getListValue().getValuesList().stream()
          .map(SearchResultUtils::fromValueProto)
          .collect(Collectors.toList());
    } else {
      return null;
    }
  }

  public static AstraSearch.Value toValueProto(Object object) {
    AstraSearch.Value.Builder valueBuilder = AstraSearch.Value.newBuilder();

    if (object == null) {
      valueBuilder.setNullValue(AstraSearch.NullValue.NULL_VALUE);
    } else if (object instanceof Integer) {
      valueBuilder.setIntValue((Integer) object);
    } else if (object instanceof Long) {
      valueBuilder.setLongValue((Long) object);
    } else if (object instanceof Double) {
      valueBuilder.setDoubleValue((Double) object);
    } else if (object instanceof String) {
      valueBuilder.setStringValue((String) object);
    } else if (object instanceof Boolean) {
      valueBuilder.setBoolValue((Boolean) object);
    } else if (object instanceof Map) {
      valueBuilder.setStructValue(toStructProto((Map<String, Object>) object));
    } else if (object instanceof List) {
      valueBuilder.setListValue(
          AstraSearch.ListValue.newBuilder()
              .addAllValues(
                  ((List<?>) object)
                      .stream().map(SearchResultUtils::toValueProto).collect(Collectors.toList()))
              .build());
    } else {
      throw new IllegalArgumentException();
    }

    return valueBuilder.build();
  }

  /**
   * Rewrites QueryStringQueryBuilder instances to include default searchable fields when none are
   * explicitly specified.
   *
   * <p>This method addresses a limitation where OpenSearch's index.query.default_field setting
   * cannot specify multiple default fields (e.g., both "_all" and "*"). While the setting supports
   * either "_all" OR "*" individually, it cannot handle the combination of both.
   *
   * <p>The method recursively traverses the query tree and modifies QueryStringQueryBuilder
   * instances that have neither a default_field nor explicit fields configured. For such queries,
   * it sets both "_all" and "*" as searchable fields with equal boost factors (1.0f each).
   *
   * <p>This ensures comprehensive search coverage across:
   * <ul>
   *   <li>"_all" - The meta-field that contains all document content
   *   <li>"*" - Wildcard pattern matching all regular field names
   * </ul>
   *
   * <p>The rewrite is applied to QueryStringQueryBuilder instances within:
   * <ul>
   *   <li>Standalone query_string queries
   *   <li>Nested queries within BoolQueryBuilder (must, filter, should, must_not clauses)
   * </ul>
   *
   * <p>The method preserves existing field configurations and only applies defaults when no field
   * specification exists, ensuring user preferences are respected.
   *
   * @param queryBuilder the query builder to rewrite
   * @return the rewritten query builder with default fields applied where appropriate
   */
  private static QueryBuilder rewriteQueryStringWithDefaultField(QueryBuilder queryBuilder) {
    if (queryBuilder == null) {
      return null;
    }

    if (queryBuilder instanceof QueryStringQueryBuilder) {
      QueryStringQueryBuilder queryStringQuery = (QueryStringQueryBuilder) queryBuilder;
      if (queryStringQuery.defaultField() == null && queryStringQuery.fields().isEmpty()) {
        Map<String, Float> fieldsMap = new HashMap<>();
        fieldsMap.put("_all", 1.0f);
        fieldsMap.put("*", 1.0f);
        queryStringQuery.fields(fieldsMap);
      }
      return queryStringQuery;
    } else if (queryBuilder instanceof BoolQueryBuilder) {
      BoolQueryBuilder boolQuery = (BoolQueryBuilder) queryBuilder;

      // Rewrite must clauses
      for (int i = 0; i < boolQuery.must().size(); i++) {
        QueryBuilder rewritten = rewriteQueryStringWithDefaultField(boolQuery.must().get(i));
        boolQuery.must().set(i, rewritten);
      }

      // Rewrite filter clauses
      for (int i = 0; i < boolQuery.filter().size(); i++) {
        QueryBuilder rewritten = rewriteQueryStringWithDefaultField(boolQuery.filter().get(i));
        boolQuery.filter().set(i, rewritten);
      }

      // Rewrite should clauses
      for (int i = 0; i < boolQuery.should().size(); i++) {
        QueryBuilder rewritten = rewriteQueryStringWithDefaultField(boolQuery.should().get(i));
        boolQuery.should().set(i, rewritten);
      }

      // Rewrite must_not clauses
      for (int i = 0; i < boolQuery.mustNot().size(); i++) {
        QueryBuilder rewritten = rewriteQueryStringWithDefaultField(boolQuery.mustNot().get(i));
        boolQuery.mustNot().set(i, rewritten);
      }

      return boolQuery;
    }

    return queryBuilder;
  }

  public static SearchQuery fromSearchRequest(AstraSearch.SearchRequest searchRequest) {
    QueryBuilder queryBuilder = null;

    if (!searchRequest.getQuery().isEmpty()) {
      try {
        JsonXContentParser jsonXContentParser =
            new JsonXContentParser(
                namedXContentRegistry,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                objectMapper.createParser(searchRequest.getQuery()));
        queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(jsonXContentParser);
        queryBuilder = rewriteQueryStringWithDefaultField(queryBuilder);
      } catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
    }

    AggregatorFactories.Builder aggregatorFactoriesBuilder = null;
    if (!searchRequest.getAggregationJson().isEmpty()) {
      try {
        JsonXContentParser jsonXContentParser =
            new JsonXContentParser(
                namedXContentRegistry,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                objectMapper.createParser(searchRequest.getAggregationJson()));

        jsonXContentParser.nextToken();
        aggregatorFactoriesBuilder = AggregatorFactories.parseAggregators(jsonXContentParser);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }

    return new SearchQuery(
        searchRequest.getDataset(),
        searchRequest.getStartTimeEpochMs(),
        searchRequest.getEndTimeEpochMs(),
        searchRequest.getHowMany(),
        searchRequest.getChunkIdsList(),
        queryBuilder,
        SourceFieldFilter.fromProto(searchRequest.getSourceFieldFilter()),
        aggregatorFactoriesBuilder);
  }

  public static SearchResult<LogMessage> fromSearchResultProtoOrEmpty(
      AstraSearch.SearchResult protoSearchResult) {
    try {
      return fromSearchResultProto(protoSearchResult);
    } catch (IOException e) {
      return SearchResult.empty();
    }
  }

  public static SearchResult<LogMessage> fromSearchResultProto(
      AstraSearch.SearchResult protoSearchResult) throws IOException {
    List<LogMessage> hits = new ArrayList<>(protoSearchResult.getHitsCount());

    for (ByteString bytes : protoSearchResult.getHitsList().asByteStringList()) {
      LogWireMessage hit = JsonUtil.read(bytes.toStringUtf8(), LogWireMessage.class);
      LogMessage message = LogMessage.fromWireMessage(hit);
      hits.add(message);
    }

    return new SearchResult<>(
        hits,
        protoSearchResult.getTookMicros(),
        protoSearchResult.getFailedNodes(),
        protoSearchResult.getTotalNodes(),
        protoSearchResult.getTotalSnapshots(),
        protoSearchResult.getSnapshotsWithReplicas(),
        OpenSearchInternalAggregation.fromByteArray(
            protoSearchResult.getInternalAggregations().toByteArray()));
  }

  public static FieldType fromSchemaDefinitionProto(
      AstraSearch.SchemaDefinition protoSchemaDefinition) {
    return FieldType.fromSchemaFieldType(protoSchemaDefinition.getType());
  }

  public static AstraSearch.SchemaDefinition toSchemaDefinitionProto(FieldType fieldType) {
    AstraSearch.SchemaDefinition.Builder schemaBuilder = AstraSearch.SchemaDefinition.newBuilder();
    schemaBuilder.setType(fieldType.toSchemaFieldType());
    return schemaBuilder.build();
  }

  public static Map<String, FieldType> fromSchemaResultProto(
      AstraSearch.SchemaResult protoSchemaResult) {
    Map<String, FieldType> schemaMap = new HashMap<>();
    protoSchemaResult
        .getFieldDefinitionMap()
        .forEach(
            (key, value) -> {
              schemaMap.put(key, fromSchemaDefinitionProto(value));
            });
    return schemaMap;
  }

  public static AstraSearch.SchemaResult toSchemaResultProto(Map<String, FieldType> schema) {
    AstraSearch.SchemaResult.Builder schemaBuilder = AstraSearch.SchemaResult.newBuilder();
    schema.forEach(
        (key, value) -> schemaBuilder.putFieldDefinition(key, toSchemaDefinitionProto(value)));
    return schemaBuilder.build();
  }

  public static <T> AstraSearch.SearchResult toSearchResultProto(SearchResult<T> searchResult) {
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("SearchResultUtils.toSearchResultProto");
    span.tag("tookMicros", String.valueOf(searchResult.tookMicros));
    span.tag("failedNodes", String.valueOf(searchResult.failedNodes));
    span.tag("totalNodes", String.valueOf(searchResult.totalNodes));
    span.tag("totalSnapshots", String.valueOf(searchResult.totalSnapshots));
    span.tag("snapshotsWithReplicas", String.valueOf(searchResult.snapshotsWithReplicas));
    span.tag("hits", String.valueOf(searchResult.hits.size()));

    AstraSearch.SearchResult.Builder searchResultBuilder = AstraSearch.SearchResult.newBuilder();
    searchResultBuilder.setTookMicros(searchResult.tookMicros);
    searchResultBuilder.setFailedNodes(searchResult.failedNodes);
    searchResultBuilder.setTotalNodes(searchResult.totalNodes);
    searchResultBuilder.setTotalSnapshots(searchResult.totalSnapshots);
    searchResultBuilder.setSnapshotsWithReplicas(searchResult.snapshotsWithReplicas);

    // Set hits
    ArrayList<String> protoHits = new ArrayList<>(searchResult.hits.size());
    for (T hit : searchResult.hits) {
      try {
        protoHits.add(JsonUtil.writeAsString(hit));
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    }
    searchResultBuilder.addAllHits(protoHits);

    ByteString bytes =
        ByteString.copyFrom(
            OpenSearchInternalAggregation.toByteArray(searchResult.internalAggregation));
    searchResultBuilder.setInternalAggregations(bytes);
    span.finish();
    return searchResultBuilder.build();
  }
}
