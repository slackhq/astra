package com.slack.astra.elasticsearchApi;

import static com.slack.astra.server.ManagerApiGrpc.MAX_TIME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.opensearch.OpenSearchAdapter;
import com.slack.astra.proto.service.AstraSearch;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.lucene.search.BooleanClause;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.SearchModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for parsing an OpenSearch NDJSON search request into a list of appropriate
 * AstraSearch.SearchRequests, that can be provided to the GRPC Search API. This class is
 * responsible for taking a raw payload string, performing any validation as appropriate, and
 * building a complete working list of queries to be performed.
 */
public class OpenSearchRequest {
  private static final ObjectMapper OM =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static final OpenSearchAdapter openSearchAdapter =
      new OpenSearchAdapter(Collections.EMPTY_MAP);
  private static final Logger log = LoggerFactory.getLogger(OpenSearchRequest.class);

  private static class DateRangeQueryBuilderVistor implements QueryBuilderVisitor {
    private Long dateRangeStart;
    private Long dateRangeEnd;

    @Override
    public void accept(QueryBuilder qb) {
      if (qb instanceof RangeQueryBuilder rangeQueryBuilder) {
        if (!rangeQueryBuilder.fieldName().equals("@timestamp")
            && !rangeQueryBuilder
                .fieldName()
                .equals(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName)) {
          return;
        }

        Object from = rangeQueryBuilder.from();
        Object to = rangeQueryBuilder.to();
        String format = rangeQueryBuilder.format();

        if (format != null && format.equals("epoch_millis")) {
          if (from instanceof Long) {
            dateRangeStart = (Long) from;
          } else if (from instanceof Integer) {
            dateRangeStart = ((Integer) from).longValue();
          } else if (from instanceof String) {
            dateRangeStart = Long.valueOf(from.toString());
          }

          if (to instanceof Long) {
            dateRangeEnd = (Long) to;
          } else if (to instanceof Integer) {
            dateRangeEnd = ((Integer) to).longValue();
          } else if (to instanceof String) {
            dateRangeEnd = Long.valueOf(to.toString());
          }
        } else {
          if (from instanceof Long) {
            dateRangeStart = (Long) from;
          } else if (from instanceof Integer) {
            dateRangeStart = ((Integer) from).longValue();
          } else if (from instanceof String) {
            dateRangeStart = Instant.parse((String) from).toEpochMilli();
          }

          if (to instanceof Long) {
            dateRangeEnd = (Long) to;
          } else if (to instanceof Integer) {
            dateRangeEnd = ((Integer) to).longValue();
          } else if (to instanceof String) {
            dateRangeEnd = Instant.parse((String) to).toEpochMilli();
          }
        }
      }
    }

    @Override
    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
      return this;
    }
  }

  public List<AstraSearch.SearchRequest> parseHttpPostBody(String postBody)
      throws JsonProcessingException {
    // the body contains an NDJSON format, with alternating rows as header/body
    // @see http://ndjson.org/
    // @see
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html#search-multi-search-api-desc

    List<AstraSearch.SearchRequest> searchRequests = new ArrayList<>();

    // List<EsSearchRequest> requests = new ArrayList<>();
    for (List<String> pair : Lists.partition(Arrays.asList(postBody.split("\n")), 2)) {
      JsonNode header = OM.readTree(pair.get(0));
      JsonNode body = OM.readTree(pair.get(1));
      String query = getQuery(body);
      DateRangeQueryBuilderVistor dateRangeQueryBuilderVistor = getDateRange(query);
      long startTimeEpochMs = 0L;
      long endTimeEpochMs = MAX_TIME;
      if (dateRangeQueryBuilderVistor != null
          && dateRangeQueryBuilderVistor.dateRangeStart != null
          && dateRangeQueryBuilderVistor.dateRangeEnd != null) {
        startTimeEpochMs = dateRangeQueryBuilderVistor.dateRangeStart;
        endTimeEpochMs = dateRangeQueryBuilderVistor.dateRangeEnd;
      }

      searchRequests.add(
          AstraSearch.SearchRequest.newBuilder()
              .setDataset(getDataset(header))
              .setHowMany(getHowMany(body))
              .setQuery(getQuery(body))
              .setSourceFieldFilter(getSourceFieldFilter(body))
              .setAggregationJson(getAggregationJson(body))
              .setStartTimeEpochMs(startTimeEpochMs)
              .setEndTimeEpochMs(endTimeEpochMs)
              .addAllSort(parseSort(body))
              .build());
    }
    return searchRequests;
  }

  private static AstraSearch.SearchRequest.SourceFieldFilter getSourceFieldFilter(JsonNode body) {
    if (body.has("_source") && body.get("_source") != null) {
      JsonNode sourceNode = body.get("_source");
      if (sourceNode.isBoolean()) {
        return AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .setIncludeAll(sourceNode.booleanValue())
            .build();

      } else if (sourceNode.isTextual()) {
        return AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .addIncludeWildcards(sourceNode.textValue())
            .build();
      } else if (sourceNode.isArray()) {
        ArrayNode includeArrayNode = (ArrayNode) sourceNode;
        HashMap<String, Boolean> includes = new HashMap<>();

        AstraSearch.SearchRequest.SourceFieldFilter.Builder fieldInclusionBuilder =
            AstraSearch.SearchRequest.SourceFieldFilter.newBuilder();

        for (JsonNode jsonNode : includeArrayNode) {
          String fieldname = jsonNode.asText();
          if (fieldname.contains("*")) {
            fieldInclusionBuilder.addIncludeWildcards(fieldname);
          } else {
            includes.put(fieldname, true);
          }
        }

        return fieldInclusionBuilder.putAllIncludeFields(includes).build();

      } else if (sourceNode.isObject()) {
        AstraSearch.SearchRequest.SourceFieldFilter.Builder sourceFieldFilterBuilder =
            AstraSearch.SearchRequest.SourceFieldFilter.newBuilder();

        if (sourceNode.has("includes")) {
          ArrayNode includeArrayNode = (ArrayNode) sourceNode.get("includes");
          HashMap<String, Boolean> includes = new HashMap<>();

          for (JsonNode jsonNode : includeArrayNode) {
            String fieldname = jsonNode.asText();
            if (fieldname.contains("*")) {
              sourceFieldFilterBuilder.addIncludeWildcards(fieldname);
            } else {
              includes.put(fieldname, true);
            }
          }

          sourceFieldFilterBuilder.putAllIncludeFields(includes);
        }

        if (sourceNode.has("excludes")) {
          ArrayNode includeArrayNode = (ArrayNode) sourceNode.get("excludes");
          HashMap<String, Boolean> excludes = new HashMap<>();

          for (JsonNode jsonNode : includeArrayNode) {
            String fieldname = jsonNode.asText();
            if (fieldname.contains("*")) {
              sourceFieldFilterBuilder.addExcludeWildcards(fieldname);
            } else {
              excludes.put(fieldname, true);
            }
          }
          sourceFieldFilterBuilder.putAllExcludeFields(excludes);
        }
        return sourceFieldFilterBuilder.build();
      }
    }
    return AstraSearch.SearchRequest.SourceFieldFilter.newBuilder().build();
  }

  private static DateRangeQueryBuilderVistor getDateRange(String queryBody) {
    try {
      openSearchAdapter.reloadSchema();
      JsonXContentParser jsonXContentParser =
          new JsonXContentParser(
              new NamedXContentRegistry(
                  new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()),
              DeprecationHandler.IGNORE_DEPRECATIONS,
              OM.createParser(queryBody));

      QueryBuilder queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(jsonXContentParser);
      DateRangeQueryBuilderVistor dateRangeQueryBuilderVistor = new DateRangeQueryBuilderVistor();
      queryBuilder.visit(dateRangeQueryBuilderVistor);
      return dateRangeQueryBuilderVistor;

    } catch (Exception e) {
      log.error("Unable to parse date/time range from query body: {}. Error: {}", queryBody, e);
      return null;
    }
  }

  private static String getQuery(JsonNode body) {
    if (!body.get("query").isNull() && !body.get("query").isEmpty()) {
      return body.get("query").toString();
    }
    return null;
  }

  private static String getDataset(JsonNode header) {
    return header.get("index").asText();
  }

  private static int getHowMany(JsonNode body) {
    return body.get("size").asInt();
  }

  private static String getAggregationJson(JsonNode body) {
    if (body.get("aggs") == null) {
      return "";
    }
    if (Iterators.size(body.get("aggs").fieldNames()) != 1) {
      throw new NotImplementedException(
          "Only exactly one top level aggregators is currently supported");
    }
    return body.get("aggs").toString();
  }

  private static List<AstraSearch.SortField> parseSort(JsonNode body) {
    List<AstraSearch.SortField> sortFields = new ArrayList<>();

    if (!body.has("sort") || body.get("sort") == null || body.get("sort").isNull()) {
      return sortFields; // No sort specified, return empty list (will use default)
    }

    JsonNode sortNode = body.get("sort");

    if (sortNode.isArray()) {
      // Array format: [{ "field1": { "order": "desc" } }, "field2", ...]
      for (JsonNode element : sortNode) {
        parseSingleSortField(element, sortFields);
      }
    } else {
      // Single value: { "field": "desc" } or "field"
      parseSingleSortField(sortNode, sortFields);
    }

    return sortFields;
  }

  private static void parseSingleSortField(
      JsonNode sortFieldNode, List<AstraSearch.SortField> sortFields) {
    if (sortFieldNode.isTextual()) {
      // String format: just field name, defaults to ascending (Elasticsearch standard)
      // Example: "severity"
      String fieldName = sortFieldNode.asText();
      sortFields.add(
          AstraSearch.SortField.newBuilder()
              // Convert Elasticsearch/Grafana @timestamp to Astra's internal _timesinceepoch
              .setFieldName(
                  fieldName.equals("@timestamp")
                      ? LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName
                      : fieldName)
              .setOrder(AstraSearch.SortOrder.ASC) // Elasticsearch default is ASC
              .build());
    } else if (sortFieldNode.isObject()) {
      // Object format: { "fieldName": "desc" } or { "fieldName": { "order": "desc" } }
      // May also include additional ES fields like unmapped_type
      // Example: { "@timestamp": { "order": "desc", "unmapped_type": "boolean" } }
      sortFieldNode
          .fieldNames()
          .forEachRemaining(
              fieldName -> {
                // fieldName = "@timestamp" (or "priority", etc.)
                JsonNode fieldValue = sortFieldNode.get(fieldName);
                AstraSearch.SortOrder order = AstraSearch.SortOrder.ASC; // Elasticsearch default
                String unmappedType = null;

                if (fieldValue.isTextual()) {
                  // Short form: { "fieldName": "desc" }
                  // fieldValue = "desc"
                  order = parseSortOrder(fieldValue.asText());
                } else if (fieldValue.isObject()) {
                  // Full form: { "fieldName": { "order": "desc", "unmapped_type": "boolean" } }
                  // fieldValue = { "order": "desc", "unmapped_type": "boolean" }
                  if (fieldValue.has("order")) {
                    order = parseSortOrder(fieldValue.get("order").asText());
                  }
                  if (fieldValue.has("unmapped_type")) {
                    unmappedType = fieldValue.get("unmapped_type").asText();
                  }
                }

                // Convert Elasticsearch/Grafana @timestamp to Astra's internal _timesinceepoch
                AstraSearch.SortField.Builder sortFieldBuilder =
                    AstraSearch.SortField.newBuilder()
                        .setFieldName(
                            fieldName.equals("@timestamp")
                                ? LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName
                                : fieldName)
                        .setOrder(order);

                if (unmappedType != null && !unmappedType.isEmpty()) {
                  sortFieldBuilder.setUnmappedType(unmappedType);
                }

                sortFields.add(sortFieldBuilder.build());
              });
    }
  }

  /** Converts order string to protobuf enum. Defaults to ASC per Elasticsearch standard. */
  private static AstraSearch.SortOrder parseSortOrder(String orderStr) {
    if (orderStr == null || orderStr.isEmpty()) {
      return AstraSearch.SortOrder.ASC; // Elasticsearch default
    }
    // "desc" or "DESC" -> DESC, everything else -> ASC
    return orderStr.equalsIgnoreCase("desc")
        ? AstraSearch.SortOrder.DESC
        : AstraSearch.SortOrder.ASC;
  }
}
