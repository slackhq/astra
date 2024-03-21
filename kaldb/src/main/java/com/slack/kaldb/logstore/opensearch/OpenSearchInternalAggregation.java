package com.slack.kaldb.logstore.opensearch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.InternalFilters;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalExtendedStats;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.CumulativeSumPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.EwmaModel;
import org.opensearch.search.aggregations.pipeline.HoltLinearModel;
import org.opensearch.search.aggregations.pipeline.HoltWintersModel;
import org.opensearch.search.aggregations.pipeline.InternalDerivative;
import org.opensearch.search.aggregations.pipeline.InternalSimpleValue;
import org.opensearch.search.aggregations.pipeline.LinearModel;
import org.opensearch.search.aggregations.pipeline.MovAvgModel;
import org.opensearch.search.aggregations.pipeline.MovAvgPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MovFnPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.SimpleModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides helper functionality for serializing and deserializing OpenSearch InternalAggregation
 * objects. This class should not exist, as we don't want to expose InternalAggregations internal to
 * KalDB, but we still need to port these classes to our codebase.
 */
@Deprecated
public class OpenSearchInternalAggregation {
  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchInternalAggregation.class);

  private static final NamedWriteableRegistry NAMED_WRITEABLE_REGISTRY =
      new NamedWriteableRegistry(
          Arrays.asList(
              new NamedWriteableRegistry.Entry(
                  AggregationBuilder.class,
                  DateHistogramAggregationBuilder.NAME,
                  DateHistogramAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class,
                  DateHistogramAggregationBuilder.NAME,
                  InternalDateHistogram::new),
              new NamedWriteableRegistry.Entry(
                  AggregationBuilder.class,
                  AutoDateHistogramAggregationBuilder.NAME,
                  AutoDateHistogramAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class,
                  AutoDateHistogramAggregationBuilder.NAME,
                  InternalAutoDateHistogram::new),
              new NamedWriteableRegistry.Entry(
                  AggregationBuilder.class,
                  FiltersAggregationBuilder.NAME,
                  FiltersAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class, FiltersAggregationBuilder.NAME, InternalFilters::new),
              new NamedWriteableRegistry.Entry(
                  AggregationBuilder.class,
                  HistogramAggregationBuilder.NAME,
                  HistogramAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class,
                  HistogramAggregationBuilder.NAME,
                  InternalHistogram::new),
              new NamedWriteableRegistry.Entry(
                  AggregationBuilder.class,
                  TermsAggregationBuilder.NAME,
                  TermsAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class, StringTerms.NAME, StringTerms::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class, UnmappedTerms.NAME, UnmappedTerms::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class, LongTerms.NAME, LongTerms::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class, DoubleTerms.NAME, DoubleTerms::new),
              new NamedWriteableRegistry.Entry(
                  AggregationBuilder.class, AvgAggregationBuilder.NAME, AvgAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class, AvgAggregationBuilder.NAME, InternalAvg::new),
              new NamedWriteableRegistry.Entry(
                  AggregationBuilder.class, SumAggregationBuilder.NAME, SumAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class, SumAggregationBuilder.NAME, InternalSum::new),
              new NamedWriteableRegistry.Entry(
                  AggregationBuilder.class,
                  ExtendedStatsAggregationBuilder.NAME,
                  ExtendedStatsAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class,
                  ExtendedStatsAggregationBuilder.NAME,
                  InternalExtendedStats::new),
              new NamedWriteableRegistry.Entry(
                  AggregationBuilder.class,
                  CardinalityAggregationBuilder.NAME,
                  CardinalityAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class,
                  CardinalityAggregationBuilder.NAME,
                  InternalCardinality::new),
              new NamedWriteableRegistry.Entry(
                  MovAvgModel.class, SimpleModel.NAME, SimpleModel::new),
              new NamedWriteableRegistry.Entry(
                  MovAvgModel.class, LinearModel.NAME, LinearModel::new),
              new NamedWriteableRegistry.Entry(MovAvgModel.class, EwmaModel.NAME, EwmaModel::new),
              new NamedWriteableRegistry.Entry(
                  MovAvgModel.class, HoltLinearModel.NAME, HoltLinearModel::new),
              new NamedWriteableRegistry.Entry(
                  MovAvgModel.class, HoltWintersModel.NAME, HoltWintersModel::new),
              new NamedWriteableRegistry.Entry(
                  MovAvgPipelineAggregationBuilder.class,
                  MovAvgPipelineAggregationBuilder.NAME,
                  MovAvgPipelineAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  CumulativeSumPipelineAggregationBuilder.class,
                  CumulativeSumPipelineAggregationBuilder.NAME,
                  CumulativeSumPipelineAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  MovFnPipelineAggregationBuilder.class,
                  MovFnPipelineAggregationBuilder.NAME,
                  MovFnPipelineAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  DerivativePipelineAggregationBuilder.class,
                  DerivativePipelineAggregationBuilder.NAME,
                  DerivativePipelineAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class,
                  DerivativePipelineAggregationBuilder.NAME,
                  InternalDerivative::new),
              new NamedWriteableRegistry.Entry(
                  AggregationBuilder.class, MinAggregationBuilder.NAME, MinAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class, MinAggregationBuilder.NAME, InternalMin::new),
              new NamedWriteableRegistry.Entry(
                  AggregationBuilder.class, MaxAggregationBuilder.NAME, MaxAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class, MaxAggregationBuilder.NAME, InternalMax::new),
              new NamedWriteableRegistry.Entry(
                  InternalSimpleValue.class, InternalSimpleValue.NAME, InternalSimpleValue::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class, InternalSimpleValue.NAME, InternalSimpleValue::new),
              new NamedWriteableRegistry.Entry(
                  AggregationBuilder.class,
                  PercentilesAggregationBuilder.NAME,
                  PercentilesAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class,
                  InternalTDigestPercentiles.NAME,
                  InternalTDigestPercentiles::new),
              new NamedWriteableRegistry.Entry(
                  AggregationBuilder.class,
                  ValueCountAggregationBuilder.NAME,
                  ValueCountAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class,
                  ValueCountAggregationBuilder.NAME,
                  InternalValueCount::new),
              new NamedWriteableRegistry.Entry(
                  DocValueFormat.class,
                  DocValueFormat.BOOLEAN.getWriteableName(),
                  in -> DocValueFormat.BOOLEAN),
              new NamedWriteableRegistry.Entry(
                  DocValueFormat.class, DocValueFormat.DateTime.NAME, DocValueFormat.DateTime::new),
              new NamedWriteableRegistry.Entry(
                  DocValueFormat.class, DocValueFormat.Decimal.NAME, DocValueFormat.Decimal::new),
              new NamedWriteableRegistry.Entry(
                  DocValueFormat.class,
                  DocValueFormat.GEOHASH.getWriteableName(),
                  in -> DocValueFormat.GEOHASH),
              new NamedWriteableRegistry.Entry(
                  DocValueFormat.class,
                  DocValueFormat.GEOTILE.getWriteableName(),
                  in -> DocValueFormat.GEOTILE),
              new NamedWriteableRegistry.Entry(
                  DocValueFormat.class,
                  DocValueFormat.IP.getWriteableName(),
                  in -> DocValueFormat.IP),
              new NamedWriteableRegistry.Entry(
                  DocValueFormat.class,
                  DocValueFormat.RAW.getWriteableName(),
                  in -> DocValueFormat.RAW),
              new NamedWriteableRegistry.Entry(
                  DocValueFormat.class,
                  DocValueFormat.BINARY.getWriteableName(),
                  in -> DocValueFormat.BINARY),
              new NamedWriteableRegistry.Entry(
                  DocValueFormat.class,
                  DocValueFormat.UNSIGNED_LONG_SHIFTED.getWriteableName(),
                  in -> DocValueFormat.UNSIGNED_LONG_SHIFTED)));

  /** Serializes InternalAggregation to byte array for transport */
  public static byte[] toByteArray(InternalAggregation internalAggregation) {
    if (internalAggregation == null) {
      return new byte[] {};
    }

    byte[] returnBytes;
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
      try (StreamOutput streamOutput = new OutputStreamStreamOutput(byteArrayOutputStream)) {
        InternalAggregations internalAggregations =
            new InternalAggregations(List.of(internalAggregation), null);
        internalAggregations.writeTo(streamOutput);
      }
      returnBytes = byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      LOG.error("Error writing internal agg to byte array", e);
      throw new RuntimeException(e);
    }
    return returnBytes;
  }

  /** Deserializes a bytearray into an InternalAggregation */
  public static InternalAggregation fromByteArray(byte[] bytes) throws IOException {
    if (bytes.length == 0) {
      return null;
    }

    InternalAggregation internalAggregation;
    try (InputStream inputStream = new ByteArrayInputStream(bytes)) {
      try (StreamInput streamInput = new InputStreamStreamInput(inputStream)) {
        try (NamedWriteableAwareStreamInput namedWriteableAwareStreamInput =
            new NamedWriteableAwareStreamInput(streamInput, NAMED_WRITEABLE_REGISTRY)) {
          // the use of this InternalAggregations wrapper lightly follows OpenSearch
          // See OpenSearch InternalAggregationsTest.writeToAndReadFrom() for more details
          InternalAggregations internalAggregations =
              InternalAggregations.readFrom(namedWriteableAwareStreamInput);
          internalAggregation = internalAggregations.copyResults().get(0);
        }
      }
    }
    return internalAggregation;
  }
}
