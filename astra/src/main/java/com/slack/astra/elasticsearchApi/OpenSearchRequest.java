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
import com.slack.astra.logstore.search.SearchResultUtils;
import com.slack.astra.logstore.search.aggregations.AutoDateHistogramAggBuilder;
import com.slack.astra.logstore.search.aggregations.AvgAggBuilder;
import com.slack.astra.logstore.search.aggregations.CumulativeSumAggBuilder;
import com.slack.astra.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.astra.logstore.search.aggregations.DerivativeAggBuilder;
import com.slack.astra.logstore.search.aggregations.ExtendedStatsAggBuilder;
import com.slack.astra.logstore.search.aggregations.FiltersAggBuilder;
import com.slack.astra.logstore.search.aggregations.HistogramAggBuilder;
import com.slack.astra.logstore.search.aggregations.MaxAggBuilder;
import com.slack.astra.logstore.search.aggregations.MinAggBuilder;
import com.slack.astra.logstore.search.aggregations.MovingAvgAggBuilder;
import com.slack.astra.logstore.search.aggregations.MovingFunctionAggBuilder;
import com.slack.astra.logstore.search.aggregations.PercentilesAggBuilder;
import com.slack.astra.logstore.search.aggregations.SumAggBuilder;
import com.slack.astra.logstore.search.aggregations.TermsAggBuilder;
import com.slack.astra.logstore.search.aggregations.UniqueCountAggBuilder;
import com.slack.astra.proto.service.AstraSearch;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

        if (format.equals("epoch_millis")) {
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
            dateRangeEnd= ((Integer) to).longValue();
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
              .setAggregations(getAggregations(body))
              .setQuery(getQuery(body))
              .setSourceFieldFilter(getSourceFieldFilter(body))
              .setAggregationJson(getAggregationJson(body))
              .setStartTimeEpochMs(startTimeEpochMs)
              .setEndTimeEpochMs(endTimeEpochMs)
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

  private static AstraSearch.SearchRequest.SearchAggregation getAggregations(JsonNode body) {
    if (body.get("aggs") == null) {
      return AstraSearch.SearchRequest.SearchAggregation.newBuilder().build();
    }
    if (Iterators.size(body.get("aggs").fieldNames()) != 1) {
      throw new NotImplementedException(
          "Only exactly one top level aggregators is currently supported");
    }
    return getRecursive(body.get("aggs")).get(0);
  }

  private static List<AstraSearch.SearchRequest.SearchAggregation> getRecursive(JsonNode aggs) {
    List<AstraSearch.SearchRequest.SearchAggregation> aggregations = new ArrayList<>();

    aggs.fieldNames()
        .forEachRemaining(
            aggregationName -> {
              AstraSearch.SearchRequest.SearchAggregation.Builder aggBuilder =
                  AstraSearch.SearchRequest.SearchAggregation.newBuilder();
              aggs.get(aggregationName)
                  .fieldNames()
                  .forEachRemaining(
                      aggregationObject -> {
                        if (aggregationObject.equals(AutoDateHistogramAggBuilder.TYPE)) {
                          JsonNode autoDateHistogram =
                              aggs.get(aggregationName).get(aggregationObject);
                          aggBuilder
                              .setType(AutoDateHistogramAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setValueSource(
                                  AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                      .newBuilder()
                                      .setField(getFieldName(autoDateHistogram))
                                      .setAutoDateHistogram(
                                          AstraSearch.SearchRequest.SearchAggregation
                                              .ValueSourceAggregation.AutoDateHistogramAggregation
                                              .newBuilder()
                                              .setMinInterval(
                                                  SearchResultUtils.toValueProto(
                                                      getAutoDateHistogramMinInterval(
                                                          autoDateHistogram)))
                                              .setNumBuckets(
                                                  SearchResultUtils.toValueProto(
                                                      getAutoDateHistogramNumBuckets(
                                                          autoDateHistogram)))
                                              .build())
                                      .build());
                        } else if (aggregationObject.equals(DateHistogramAggBuilder.TYPE)) {
                          JsonNode dateHistogram = aggs.get(aggregationName).get(aggregationObject);
                          if (getDateHistogramInterval(dateHistogram).equals("auto")) {
                            // if using "auto" type, default to using AutoDateHistogram as "auto" is
                            // not a valid interval for DateHistogramAggBuilder
                            aggBuilder
                                .setType(AutoDateHistogramAggBuilder.TYPE)
                                .setName(aggregationName)
                                .setValueSource(
                                    AstraSearch.SearchRequest.SearchAggregation
                                        .ValueSourceAggregation.newBuilder()
                                        .setField(getFieldName(dateHistogram))
                                        .build());
                          } else {

                            AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                    .DateHistogramAggregation.Builder
                                dateHistogramBuilder =
                                    AstraSearch.SearchRequest.SearchAggregation
                                        .ValueSourceAggregation.DateHistogramAggregation
                                        .newBuilder()
                                        .setMinDocCount(getDateHistogramMinDocCount(dateHistogram))
                                        .setInterval(getDateHistogramInterval(dateHistogram))
                                        .putAllExtendedBounds(
                                            getDateHistogramExtendedBounds(dateHistogram))
                                        .setFormat(getDateHistogramFormat(dateHistogram))
                                        .setOffset(getDateHistogramOffset(dateHistogram));

                            String zoneId = getDateHistogramZoneId(dateHistogram);
                            if (zoneId != null) {
                              dateHistogramBuilder.setZoneId(
                                  SearchResultUtils.toValueProto(zoneId));
                            }

                            aggBuilder
                                .setType(DateHistogramAggBuilder.TYPE)
                                .setName(aggregationName)
                                .setValueSource(
                                    AstraSearch.SearchRequest.SearchAggregation
                                        .ValueSourceAggregation.newBuilder()
                                        .setField(getFieldName(dateHistogram))
                                        .setDateHistogram(dateHistogramBuilder.build())
                                        .build());
                          }
                        } else if (aggregationObject.equals(FiltersAggBuilder.TYPE)) {
                          JsonNode filters = aggs.get(aggregationName).get(aggregationObject);

                          Map<String, AstraSearch.SearchRequest.SearchAggregation.FilterAggregation>
                              filtersAggregationMap = new HashMap<>();
                          for (Map.Entry<String, FilterRequest> stringFilterAggEntry :
                              getFiltersMap(filters).entrySet()) {
                            filtersAggregationMap.put(
                                stringFilterAggEntry.getKey(),
                                AstraSearch.SearchRequest.SearchAggregation.FilterAggregation
                                    .newBuilder()
                                    .setQueryString(
                                        stringFilterAggEntry.getValue().getQueryString())
                                    .setAnalyzeWildcard(
                                        stringFilterAggEntry.getValue().isAnalyzeWildcard())
                                    .build());
                          }

                          aggBuilder
                              .setType(FiltersAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setFilters(
                                  AstraSearch.SearchRequest.SearchAggregation.FiltersAggregation
                                      .newBuilder()
                                      .putAllFilters(filtersAggregationMap)
                                      .build());
                        } else if (aggregationObject.equals(HistogramAggBuilder.TYPE)) {
                          JsonNode histogram = aggs.get(aggregationName).get(aggregationObject);
                          aggBuilder
                              .setType(HistogramAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setValueSource(
                                  AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                      .newBuilder()
                                      .setField(getFieldName(histogram))
                                      .setHistogram(
                                          AstraSearch.SearchRequest.SearchAggregation
                                              .ValueSourceAggregation.HistogramAggregation
                                              .newBuilder()
                                              // Using the getters from DateHistogram
                                              .setMinDocCount(getHistogramMinDocCount(histogram))
                                              .setInterval(getHistogramInterval(histogram))
                                              .build())
                                      .build());
                        } else if (aggregationObject.equals(TermsAggBuilder.TYPE)) {
                          JsonNode terms = aggs.get(aggregationName).get(aggregationObject);
                          aggBuilder
                              .setType(TermsAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setValueSource(
                                  AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                      .newBuilder()
                                      .setField(getFieldName(terms))
                                      .setMissing(SearchResultUtils.toValueProto(getMissing(terms)))
                                      .setTerms(
                                          AstraSearch.SearchRequest.SearchAggregation
                                              .ValueSourceAggregation.TermsAggregation.newBuilder()
                                              .setSize(getSize(terms))
                                              .putAllOrder(getTermsOrder(terms))
                                              .setMinDocCount(getTermsMinDocCount(terms))
                                              .build())
                                      .build());
                        } else if (aggregationObject.equals(AvgAggBuilder.TYPE)) {
                          JsonNode avg = aggs.get(aggregationName).get(aggregationObject);
                          aggBuilder
                              .setType(AvgAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setValueSource(
                                  AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                      .newBuilder()
                                      .setField(getFieldName(avg))
                                      .setScript(SearchResultUtils.toValueProto(getScript(avg)))
                                      .setMissing(SearchResultUtils.toValueProto(getMissing(avg)))
                                      .build());
                        } else if (aggregationObject.equals(SumAggBuilder.TYPE)) {
                          JsonNode sum = aggs.get(aggregationName).get(aggregationObject);
                          aggBuilder
                              .setType(SumAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setValueSource(
                                  AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                      .newBuilder()
                                      .setField(getFieldName(sum))
                                      .setScript(SearchResultUtils.toValueProto(getScript(sum)))
                                      .setMissing(SearchResultUtils.toValueProto(getMissing(sum)))
                                      .build());
                        } else if (aggregationObject.equals(MinAggBuilder.TYPE)) {
                          JsonNode min = aggs.get(aggregationName).get(aggregationObject);
                          aggBuilder
                              .setType(MinAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setValueSource(
                                  AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                      .newBuilder()
                                      .setField(getFieldName(min))
                                      .setScript(SearchResultUtils.toValueProto(getScript(min)))
                                      .setMissing(SearchResultUtils.toValueProto(getMissing(min)))
                                      .build());
                        } else if (aggregationObject.equals(MaxAggBuilder.TYPE)) {
                          JsonNode max = aggs.get(aggregationName).get(aggregationObject);
                          aggBuilder
                              .setType(MaxAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setValueSource(
                                  AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                      .newBuilder()
                                      .setField(getFieldName(max))
                                      .setScript(SearchResultUtils.toValueProto(getScript(max)))
                                      .setMissing(SearchResultUtils.toValueProto(getMissing(max)))
                                      .build());
                        } else if (aggregationObject.equals(UniqueCountAggBuilder.TYPE)) {
                          JsonNode uniqueCount = aggs.get(aggregationName).get(aggregationObject);

                          aggBuilder
                              .setType(UniqueCountAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setValueSource(
                                  AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                      .newBuilder()
                                      .setField(getFieldName(uniqueCount))
                                      .setMissing(
                                          SearchResultUtils.toValueProto(getMissing(uniqueCount)))
                                      .setUniqueCount(
                                          AstraSearch.SearchRequest.SearchAggregation
                                              .ValueSourceAggregation.UniqueCountAggregation
                                              .newBuilder()
                                              .setPrecisionThreshold(
                                                  SearchResultUtils.toValueProto(
                                                      getPrecisionThreshold(uniqueCount)))
                                              .build())
                                      .build());
                        } else if (aggregationObject.equals(ExtendedStatsAggBuilder.TYPE)) {
                          JsonNode extendedStats = aggs.get(aggregationName).get(aggregationObject);

                          aggBuilder
                              .setType(ExtendedStatsAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setValueSource(
                                  AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                      .newBuilder()
                                      .setField(getFieldName(extendedStats))
                                      .setMissing(
                                          SearchResultUtils.toValueProto(getMissing(extendedStats)))
                                      .setScript(
                                          SearchResultUtils.toValueProto(getScript(extendedStats)))
                                      .setExtendedStats(
                                          AstraSearch.SearchRequest.SearchAggregation
                                              .ValueSourceAggregation.ExtendedStatsAggregation
                                              .newBuilder()
                                              .setSigma(
                                                  SearchResultUtils.toValueProto(
                                                      getSigma(extendedStats))))
                                      .build());
                        } else if (aggregationObject.equals(PercentilesAggBuilder.TYPE)) {
                          JsonNode percentiles = aggs.get(aggregationName).get(aggregationObject);
                          aggBuilder
                              .setType(PercentilesAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setValueSource(
                                  AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                      .newBuilder()
                                      .setField(getFieldName(percentiles))
                                      .setScript(
                                          SearchResultUtils.toValueProto(getScript(percentiles)))
                                      .setMissing(
                                          SearchResultUtils.toValueProto(getMissing(percentiles)))
                                      .setPercentiles(
                                          AstraSearch.SearchRequest.SearchAggregation
                                              .ValueSourceAggregation.PercentilesAggregation
                                              .newBuilder()
                                              .addAllPercentiles(getPercentiles(percentiles))
                                              .build())
                                      .build());
                        } else if (aggregationObject.equals(MovingAvgAggBuilder.TYPE)) {
                          JsonNode movAvg = aggs.get(aggregationName).get(aggregationObject);
                          AstraSearch.SearchRequest.SearchAggregation.PipelineAggregation
                                  .MovingAverageAggregation.Builder
                              movingAvgAggBuilder =
                                  AstraSearch.SearchRequest.SearchAggregation.PipelineAggregation
                                      .MovingAverageAggregation.newBuilder()
                                      .setModel(getMovAvgModel(movAvg))
                                      .setMinimize(getMovAvgMinimize(movAvg))
                                      .setPad(getMovAvgPad(movAvg));

                          Integer window = getWindow(movAvg);
                          if (window != null) {
                            movingAvgAggBuilder.setWindow(window);
                          }
                          Integer predict = getMovAvgPredict(movAvg);
                          if (predict != null) {
                            movingAvgAggBuilder.setPredict(predict);
                          }
                          Double alpha = getMovAvgAlpha(movAvg);
                          if (alpha != null) {
                            movingAvgAggBuilder.setAlpha(alpha);
                          }
                          Double beta = getMovAvgBeta(movAvg);
                          if (beta != null) {
                            movingAvgAggBuilder.setBeta(beta);
                          }
                          Double gamma = getMovAvgGamma(movAvg);
                          if (gamma != null) {
                            movingAvgAggBuilder.setGamma(gamma);
                          }
                          Integer period = getMovAvgPeriod(movAvg);
                          if (period != null) {
                            movingAvgAggBuilder.setPeriod(period);
                          }

                          aggBuilder
                              .setType(MovingAvgAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setPipeline(
                                  AstraSearch.SearchRequest.SearchAggregation.PipelineAggregation
                                      .newBuilder()
                                      .setBucketsPath(getBucketsPath(movAvg))
                                      .setMovingAverage(movingAvgAggBuilder.build())
                                      .build());
                        } else if (aggregationObject.equals(CumulativeSumAggBuilder.TYPE)) {
                          JsonNode cumulativeSumAgg =
                              aggs.get(aggregationName).get(aggregationObject);

                          aggBuilder
                              .setType(CumulativeSumAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setPipeline(
                                  AstraSearch.SearchRequest.SearchAggregation.PipelineAggregation
                                      .newBuilder()
                                      .setBucketsPath(getBucketsPath(cumulativeSumAgg))
                                      .setCumulativeSum(
                                          AstraSearch.SearchRequest.SearchAggregation
                                              .PipelineAggregation.CumulativeSumAggregation
                                              .newBuilder()
                                              .setFormat(
                                                  SearchResultUtils.toValueProto(
                                                      getFormat(cumulativeSumAgg)))
                                              .build())
                                      .build());
                        } else if (aggregationObject.equals(MovingFunctionAggBuilder.TYPE)) {
                          JsonNode movingFunctionAgg =
                              aggs.get(aggregationName).get(aggregationObject);

                          AstraSearch.SearchRequest.SearchAggregation.PipelineAggregation
                                  .MovingFunctionAggregation.Builder
                              movingFunctionAggBuilder =
                                  AstraSearch.SearchRequest.SearchAggregation.PipelineAggregation
                                      .MovingFunctionAggregation.newBuilder()
                                      .setWindow(getWindow(movingFunctionAgg))
                                      .setScript(getScript(movingFunctionAgg));
                          Integer shift = getMovingFunctionShift(movingFunctionAgg);
                          if (shift != null) {
                            movingFunctionAggBuilder.setShift(shift);
                          }

                          aggBuilder
                              .setType(MovingFunctionAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setPipeline(
                                  AstraSearch.SearchRequest.SearchAggregation.PipelineAggregation
                                      .newBuilder()
                                      .setBucketsPath(getBucketsPath(movingFunctionAgg))
                                      .setMovingFunction(movingFunctionAggBuilder.build())
                                      .build());
                        } else if (aggregationObject.equals(DerivativeAggBuilder.TYPE)) {
                          JsonNode derivativeAgg = aggs.get(aggregationName).get(aggregationObject);

                          aggBuilder
                              .setType(DerivativeAggBuilder.TYPE)
                              .setName(aggregationName)
                              .setPipeline(
                                  AstraSearch.SearchRequest.SearchAggregation.PipelineAggregation
                                      .newBuilder()
                                      .setBucketsPath(getBucketsPath(derivativeAgg))
                                      .setDerivative(
                                          AstraSearch.SearchRequest.SearchAggregation
                                              .PipelineAggregation.DerivativeAggregation
                                              .newBuilder()
                                              .setUnit(
                                                  SearchResultUtils.toValueProto(
                                                      getUnit(derivativeAgg)))
                                              .build())
                                      .build());
                        } else if (aggregationObject.equals("aggs")) {
                          // nested aggregations
                          aggBuilder.addAllSubAggregations(
                              getRecursive(aggs.get(aggregationName).get(aggregationObject)));
                        } else {
                          throw new NotImplementedException(
                              String.format(
                                  "Aggregation type '%s' is not yet supported", aggregationObject));
                        }
                      });
              aggregations.add(aggBuilder.build());
            });

    return aggregations;
  }

  private static String getDateHistogramInterval(JsonNode dateHistogram) {
    if (dateHistogram.has("fixed_interval")) {
      return dateHistogram.get("fixed_interval").asText();
    } else if (dateHistogram.has("interval")) {
      return dateHistogram.get("interval").asText();
    }
    return "auto";
  }

  private static String getDateHistogramZoneId(JsonNode dateHistogram) {
    if (dateHistogram.has("time_zone")) {
      return dateHistogram.get("time_zone").asText();
    }
    return null;
  }

  private static String getHistogramInterval(JsonNode dateHistogram) {
    if (dateHistogram.has("interval")) {
      return dateHistogram.get("interval").asText();
    }
    return "auto";
  }

  private static String getFieldName(JsonNode agg) {
    return agg.get("field").asText();
  }

  private static String getScript(JsonNode agg) {
    if (agg.has("script")) {
      return agg.get("script").asText();
    }
    return "";
  }

  private static String getBucketsPath(JsonNode pipelineAgg) {
    return pipelineAgg.get("buckets_path").asText();
  }

  private static Object getMissing(JsonNode agg) {
    // we can return any object here and it will correctly serialize, but Grafana only ever seems to
    // issue these as strings
    if (agg.has("missing")) {
      return agg.get("missing").asText();
    }
    return null;
  }

  private static Long getPrecisionThreshold(JsonNode uniqueCount) {
    if (uniqueCount.has("precision_threshold")) {
      return uniqueCount.get("precision_threshold").asLong();
    }
    return null;
  }

  private static Double getSigma(JsonNode extendedStats) {
    if (extendedStats.has("sigma")) {
      return extendedStats.get("sigma").asDouble();
    }
    return null;
  }

  private static Map<String, FilterRequest> getFiltersMap(JsonNode filters) {
    Map<String, FilterRequest> filtersMap = new HashMap<>();
    filters
        .get("filters")
        .fieldNames()
        .forEachRemaining(
            filterName -> {
              JsonNode filter = filters.get("filters").get(filterName).get("query_string");
              filtersMap.put(
                  filterName,
                  new FilterRequest(
                      filter.get("query").asText(), filter.get("analyze_wildcard").asBoolean()));
            });
    return filtersMap;
  }

  private static List<Double> getPercentiles(JsonNode percentiles) {
    List<Double> percentileList = new ArrayList<>();
    percentiles.get("percents").forEach(percentile -> percentileList.add(percentile.asDouble()));
    return percentileList;
  }

  private static long getDateHistogramMinDocCount(JsonNode dateHistogram) {
    // min_doc_count is provided as a string in the json payload
    return Long.parseLong(dateHistogram.get("min_doc_count").asText());
  }

  private static long getHistogramMinDocCount(JsonNode dateHistogram) {
    // min_doc_count is provided as a string in the json payload
    return Long.parseLong(dateHistogram.get("min_doc_count").asText());
  }

  private static long getTermsMinDocCount(JsonNode terms) {
    // min_doc_count is provided as a string in the json payload
    return Long.parseLong(terms.get("min_doc_count").asText());
  }

  private static Map<String, Long> getDateHistogramExtendedBounds(JsonNode dateHistogram) {
    if (dateHistogram.has("extended_bounds")
        && dateHistogram.get("extended_bounds").has("min")
        && dateHistogram.get("extended_bounds").has("max")) {
      return Map.of(
          "min", dateHistogram.get("extended_bounds").get("min").asLong(),
          "max", dateHistogram.get("extended_bounds").get("max").asLong());
    }
    return Map.of();
  }

  private static String getDateHistogramFormat(JsonNode dateHistogram) {
    return dateHistogram.get("format").asText();
  }

  private static String getDateHistogramOffset(JsonNode dateHistogram) {
    if (dateHistogram.has("offset")) {
      return dateHistogram.get("offset").asText();
    }
    return "";
  }

  private static Integer getAutoDateHistogramNumBuckets(JsonNode autoDateHistogram) {
    if (autoDateHistogram.has("buckets")) {
      return autoDateHistogram.get("buckets").asInt();
    }
    return null;
  }

  private static String getAutoDateHistogramMinInterval(JsonNode autoDateHistogram) {
    if (autoDateHistogram.has("minimum_interval")) {
      return autoDateHistogram.get("minimum_interval").asText();
    }
    return null;
  }

  private static String getFormat(JsonNode cumulateSum) {
    if (cumulateSum.has("format")) {
      return cumulateSum.get("format").asText();
    }
    return null;
  }

  private static String getMovAvgModel(JsonNode movingAverage) {
    return movingAverage.get("model").asText();
  }

  private static Integer getWindow(JsonNode agg) {
    if (agg.has("window")) {
      return agg.get("window").asInt();
    }
    return null;
  }

  private static Integer getMovingFunctionShift(JsonNode movingFunction) {
    if (movingFunction.has("shift")) {
      return movingFunction.get("shift").asInt();
    }
    return null;
  }

  private static Integer getMovAvgPredict(JsonNode movingAverage) {
    if (movingAverage.has("predict")) {
      return movingAverage.get("predict").asInt();
    }
    return null;
  }

  private static Double getMovAvgAlpha(JsonNode movingAverage) {
    if (movingAverage.has("settings") && movingAverage.get("settings").has("alpha")) {
      return movingAverage.get("settings").get("alpha").asDouble();
    }
    return null;
  }

  private static Double getMovAvgBeta(JsonNode movingAverage) {
    if (movingAverage.has("settings") && movingAverage.get("settings").has("beta")) {
      return movingAverage.get("settings").get("beta").asDouble();
    }
    return null;
  }

  private static Double getMovAvgGamma(JsonNode movingAverage) {
    if (movingAverage.has("settings") && movingAverage.get("settings").has("gamma")) {
      return movingAverage.get("settings").get("gamma").asDouble();
    }
    return null;
  }

  private static Integer getMovAvgPeriod(JsonNode movingAverage) {
    if (movingAverage.has("settings") && movingAverage.get("settings").has("period")) {
      return movingAverage.get("settings").get("period").asInt();
    }
    return null;
  }

  private static boolean getMovAvgMinimize(JsonNode movingAverage) {
    if (movingAverage.has("minimize")) {
      return movingAverage.get("minimize").asBoolean();
    }
    return false;
  }

  private static boolean getMovAvgPad(JsonNode movingAverage) {
    if (movingAverage.has("settings") && movingAverage.get("settings").has("pad")) {
      return movingAverage.get("settings").get("pad").asBoolean();
    }
    return false;
  }

  private static String getUnit(JsonNode derivative) {
    if (derivative.has("unit")) {
      return derivative.get("unit").asText();
    }
    return null;
  }

  private static int getSize(JsonNode agg) {
    return agg.get("size").asInt();
  }

  private static Map<String, String> getTermsOrder(JsonNode terms) {
    Map<String, String> orderMap = new HashMap<>();
    JsonNode order = terms.get("order");
    order
        .fieldNames()
        .forEachRemaining(fieldName -> orderMap.put(fieldName, order.get(fieldName).asText()));
    return orderMap;
  }

  private static class FilterRequest {

    private final String queryString;

    private final boolean analyzeWildcard;

    public FilterRequest(String queryString, boolean analyzeWildcard) {
      this.queryString = queryString;
      this.analyzeWildcard = analyzeWildcard;
    }

    public String getQueryString() {
      return queryString;
    }

    public boolean isAnalyzeWildcard() {
      return analyzeWildcard;
    }
  }
}
