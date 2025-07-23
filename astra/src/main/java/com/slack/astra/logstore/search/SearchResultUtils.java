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
import org.opensearch.index.query.QueryBuilder;
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
            protoSearchResult.getInternalAggregations().toByteArray()),
        protoSearchResult.getHardFailedChunkIdsList(),
        protoSearchResult.getSoftFailedChunkIdsList());
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
