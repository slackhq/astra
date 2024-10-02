package com.slack.astra.util;

import com.slack.astra.logstore.LogMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;

public class AggregatorFactoriesUtil {
  public static AggregatorFactories.Builder createGenericDateHistogramAggregatorFactoriesBuilder() {
    DateHistogramAggregationBuilder dateHistogramAggregationBuilder =
        new DateHistogramAggregationBuilder("1");
    dateHistogramAggregationBuilder
        .field(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName)
        .fixedInterval(new DateHistogramInterval("1s"));
    return new AggregatorFactories.Builder().addAggregator(dateHistogramAggregationBuilder);
  }

  public static AggregatorFactories.Builder createDateHistogramAggregatorFactoriesBuilder(
      String name, String field, String intervalString, int minDocCount) {
    DateHistogramAggregationBuilder dateHistogramAggregationBuilder =
        new DateHistogramAggregationBuilder(name);
    dateHistogramAggregationBuilder
        .field(field)
        .fixedInterval(new DateHistogramInterval(intervalString))
        .minDocCount(minDocCount);
    return new AggregatorFactories.Builder().addAggregator(dateHistogramAggregationBuilder);
  }

  public static AggregatorFactories.Builder createDetailedDateHistogramAggregatorFactoriesBuilder(
      String name,
      String field,
      String intervalString,
      int minDocCount,
      long startEpochMs,
      long endEpochMs) {
    DateHistogramAggregationBuilder dateHistogramAggregationBuilder =
        new DateHistogramAggregationBuilder(name);
    dateHistogramAggregationBuilder
        .field(field)
        .fixedInterval(new DateHistogramInterval(intervalString))
        .minDocCount(minDocCount)
        .extendedBounds(new LongBounds(startEpochMs, endEpochMs));
    return new AggregatorFactories.Builder().addAggregator(dateHistogramAggregationBuilder);
  }

  public static AggregatorFactories.Builder createAverageAggregatorFactoriesBuilder(
      String name, String field, Object missing, String script) {
    AvgAggregationBuilder avgAggregationBuilder = new AvgAggregationBuilder(name);
    if (missing != null) {
      avgAggregationBuilder.missing(missing);
    }

    if (script != null && !script.isEmpty()) {
      avgAggregationBuilder.script(new Script(script));
    }
    avgAggregationBuilder.field(field);

    return new AggregatorFactories.Builder().addAggregator(avgAggregationBuilder);
  }

  public static AggregatorFactories.Builder createMinAggregatorFactoriesBuilder(
      String name, String field, Object missing, String script) {
    MinAggregationBuilder minAggregationBuilder = new MinAggregationBuilder(name);
    if (missing != null) {
      minAggregationBuilder.missing(missing);
    }

    if (script != null) {
      minAggregationBuilder.script(new Script(script));
    }
    minAggregationBuilder.field(field);

    return new AggregatorFactories.Builder().addAggregator(minAggregationBuilder);
  }

  public static AggregatorFactories.Builder createMaxAggregatorFactoriesBuilder(
      String name, String field, Object missing, String script) {
    MaxAggregationBuilder maxAggregationBuilder = new MaxAggregationBuilder(name);
    if (missing != null) {
      maxAggregationBuilder.missing(missing);
    }

    if (script != null) {
      maxAggregationBuilder.script(new Script(script));
    }
    maxAggregationBuilder.field(field);

    return new AggregatorFactories.Builder().addAggregator(maxAggregationBuilder);
  }

  public static AggregatorFactories.Builder createSumAggregatorFactoriesBuilder(
      String name, String field, Object missing, String script) {
    SumAggregationBuilder sumAggregationBuilder = new SumAggregationBuilder(name);
    if (missing != null) {
      sumAggregationBuilder.missing(missing);
    }

    if (script != null) {
      sumAggregationBuilder.script(new Script(script));
    }
    sumAggregationBuilder.field(field);

    return new AggregatorFactories.Builder().addAggregator(sumAggregationBuilder);
  }

  public static AggregatorFactories.Builder createExtendedStatsAggregatorFactoriesBuilder(
      String name, String field, Object missing, String script, Double sigma) {
    ExtendedStatsAggregationBuilder extendedStatsAggregationBuilder =
        new ExtendedStatsAggregationBuilder(name);
    if (missing != null) {
      extendedStatsAggregationBuilder.missing(missing);
    }

    if (script != null) {
      extendedStatsAggregationBuilder.script(new Script(script));
    }

    if (sigma != null) {
      extendedStatsAggregationBuilder.sigma(sigma);
    }

    extendedStatsAggregationBuilder.field(field);

    return new AggregatorFactories.Builder().addAggregator(extendedStatsAggregationBuilder);
  }

  public static AggregatorFactories.Builder createTermsAggregatorFactoriesBuilder(
      String name,
      List<AggregationBuilder> aggBuilders,
      String field,
      Object missing,
      int size,
      long minDocCount,
      Map<String, String> order) {
    List<String> subAggNames = aggBuilders.stream().map(AggregationBuilder::getName).toList();

    List<BucketOrder> parsedOrder =
        order.entrySet().stream()
            .map(
                (entry) -> {
                  // todo - this potentially needs BucketOrder.compound support
                  boolean asc = !entry.getValue().equals("desc");
                  if (entry.getKey().equals("_count") || !subAggNames.contains(entry.getKey())) {
                    // we check to see if the requested key is in the sub-aggs; if not default to
                    // the count this is because when the Grafana plugin issues a request for
                    // Count agg (not Doc Count) it comes through as an agg request when the
                    // aggs are empty. This is fixed in later versions of the plugin, and will
                    // need to be ported to our fork as well.
                    return BucketOrder.count(asc);
                  } else if (entry.getKey().equals("_key") || entry.getKey().equals("_term")) {
                    // this is due to the fact that the astra plugin thinks this is ES < 6
                    // https://github.com/slackhq/slack-astra-app/blob/95b091184d5de1682c97586e271cbf2bbd7cc92a/src/datasource/QueryBuilder.ts#L55
                    return BucketOrder.key(asc);
                  } else {
                    return BucketOrder.aggregation(entry.getKey(), asc);
                  }
                })
            .toList();

    TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder(name);
    termsAggregationBuilder
        .field(field)
        .size(size)
        .minDocCount(minDocCount)
        .executionHint("map")
        .order(parsedOrder);

    if (missing != null) {
      termsAggregationBuilder.missing(missing);
    }

    for (AggregationBuilder aggBuilder : aggBuilders) {
      termsAggregationBuilder.subAggregation(aggBuilder);
    }

    return new AggregatorFactories.Builder().addAggregator(termsAggregationBuilder);
  }

  public static AggregatorFactories.Builder createFiltersAggregatorFactoriesBuilder(
      String name, List<AggregationBuilder> subAggregations, Map<String, QueryBuilder> filters) {
    List<FiltersAggregator.KeyedFilter> keyedFilterList = new ArrayList<>();
    for (Map.Entry<String, QueryBuilder> filter : filters.entrySet()) {
      keyedFilterList.add(new FiltersAggregator.KeyedFilter(filter.getKey(), filter.getValue()));
    }

    FiltersAggregationBuilder filtersAggregationBuilder =
        new FiltersAggregationBuilder(
            name, keyedFilterList.toArray(new FiltersAggregator.KeyedFilter[0]));

    for (AggregationBuilder subAggregation : subAggregations) {
      filtersAggregationBuilder.subAggregation(subAggregation);
    }

    return new AggregatorFactories.Builder().addAggregator(filtersAggregationBuilder);
  }
}
