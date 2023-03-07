package com.slack.kaldb.logstore.opensearch;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.aggregations.AggBuilder;
import com.slack.kaldb.logstore.search.aggregations.AggBuilderBase;
import com.slack.kaldb.logstore.search.aggregations.AvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.TermsAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.UniqueCountAggBuilder;
import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.io.stream.InputStreamStreamInput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.IndexFieldDataService;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.internal.SearchContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to allow using OpenSearch aggregations from within Kaldb. This class should
 * ultimately act as an adapter where OpenSearch code is not needed external to this class. <br>
 * TODO - implement a custom InternalAggregation and return these instead of the OpenSearch
 * InternalAggregation classes
 */
public class OpenSearchAggregationAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchAggregationAdapter.class);
  private static final NamedWriteableRegistry NAMED_WRITEABLE_REGISTRY =
      new NamedWriteableRegistry(
          Arrays.asList(
              // todo - add additional aggregations as needed
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
                  AggregationBuilder.class,
                  CardinalityAggregationBuilder.NAME,
                  CardinalityAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class,
                  CardinalityAggregationBuilder.NAME,
                  InternalCardinality::new),
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

  private final QueryShardContext queryShardContext;

  public OpenSearchAggregationAdapter(Map<String, LuceneFieldDef> chunkSchema) {
    IndexSettings indexSettings = buildIndexSettings();
    SimilarityService similarityService = new SimilarityService(indexSettings, null, emptyMap());
    MapperService mapperService = buildMapperService(indexSettings, similarityService);

    // todo - see SchemaAwareLogDocumentBuilderImpl.getDefaultLuceneFieldDefinitions
    //  this needs to be adapted to include other field types once we have support
    for (Map.Entry<String, LuceneFieldDef> entry : chunkSchema.entrySet()) {
      try {
        if (entry.getValue().fieldType == FieldType.LONG) {
          registerField(
              mapperService,
              entry.getValue().name,
              b -> b.field("type", entry.getValue().fieldType.name));
        } else if (entry.getValue().fieldType == FieldType.STRING) {
          registerField(mapperService, entry.getValue().name, b -> b.field("type", "keyword"));
        }
      } catch (Exception e) {
        LOG.error("Error parsing schema mapping for {}", entry.getValue().toString(), e);
      }
    }

    this.queryShardContext =
        buildQueryShardContext(
            KaldbBigArrays.getInstance(), indexSettings, similarityService, mapperService);
  }

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

  protected static XContentBuilder mapping(
      CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
    XContentBuilder builder =
        XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
    buildFields.accept(builder);
    return builder.endObject().endObject().endObject();
  }

  protected static XContentBuilder fieldMapping(
      String fieldName, CheckedConsumer<XContentBuilder, IOException> buildField)
      throws IOException {
    return mapping(
        b -> {
          b.startObject(fieldName);
          buildField.accept(b);
          b.endObject();
        });
  }

  /** Builds a CollectorManager for use in the Lucene aggregation step */
  public CollectorManager<Aggregator, InternalAggregation> getCollectorManager(
      AggBuilder aggBuilder, IndexSearcher indexSearcher) {
    return new CollectorManager<>() {
      @Override
      public Aggregator newCollector() throws IOException {
        Aggregator aggregator = buildAggregatorUsingContext(aggBuilder, indexSearcher);
        // preCollection must be invoked prior to using aggregations
        aggregator.preCollection();
        return aggregator;
      }

      /**
       * The collector manager required a collection of collectors for reducing, though for our
       * normal case this will likely only be a single collector
       */
      @Override
      public InternalAggregation reduce(Collection<Aggregator> collectors) throws IOException {
        List<InternalAggregation> internalAggregationList = new ArrayList<>();
        for (Aggregator collector : collectors) {
          // postCollection must be invoked prior to building the internal aggregations
          collector.postCollection();
          internalAggregationList.add(collector.buildTopLevel());
        }

        if (internalAggregationList.size() == 0) {
          return null;
        } else {
          // Using the first element on the list as the basis for the reduce method is per
          // OpenSearch recommendations: "For best efficiency, when implementing, try
          // reusing an existing instance (typically the first in the given list) to save
          // on redundant object construction."
          return internalAggregationList
              .get(0)
              .reduce(
                  internalAggregationList,
                  InternalAggregation.ReduceContext.forPartialReduction(
                      KaldbBigArrays.getInstance(), null, null));
        }
      }
    };
  }

  /**
   * Registers the field types that can be aggregated by the different aggregators. Each aggregation
   * builder must be registered with the appropriate fields, or the resulting aggregation will be
   * empty.
   */
  private static ValuesSourceRegistry buildValueSourceRegistry() {
    ValuesSourceRegistry.Builder valuesSourceRegistryBuilder = new ValuesSourceRegistry.Builder();

    // todo - add additional aggregations as needed
    DateHistogramAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    TermsAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    AvgAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    CardinalityAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    ValueCountAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);

    return valuesSourceRegistryBuilder.build();
  }

  /** Builds the minimal amount of IndexSettings required for using Aggregations */
  protected static IndexSettings buildIndexSettings() {
    Settings settings =
        Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_2_3_0)
            .build();
    return new IndexSettings(
        IndexMetadata.builder("index").settings(settings).build(), Settings.EMPTY);
  }

  /**
   * Builds a MapperService using the minimal amount of settings required for Aggregations. After
   * initializing the mapper service, individual fields will still need to be added using
   * this.registerField()
   */
  private static MapperService buildMapperService(
      IndexSettings indexSettings, SimilarityService similarityService) {
    return new MapperService(
        indexSettings,
        new IndexAnalyzers(
            singletonMap(
                "default",
                new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            emptyMap(),
            emptyMap()),
        new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
        similarityService,
        new IndicesModule(emptyList()).getMapperRegistry(),
        () -> {
          throw new UnsupportedOperationException();
        },
        () -> true,
        null);
  }

  /**
   * Minimal implementation of an OpenSearch QueryShardContext while still allowing an
   * AggregatorFactory to successfully instantiate. See AggregatorFactory.class
   */
  private static QueryShardContext buildQueryShardContext(
      BigArrays bigArrays,
      IndexSettings indexSettings,
      SimilarityService similarityService,
      MapperService mapperService) {
    final ValuesSourceRegistry valuesSourceRegistry = buildValueSourceRegistry();
    return new QueryShardContext(
        0,
        indexSettings,
        bigArrays,
        null,
        new IndexFieldDataService(
                indexSettings,
                new IndicesFieldDataCache(
                    indexSettings.getSettings(), new IndexFieldDataCache.Listener() {}),
                new NoneCircuitBreakerService(),
                mapperService)
            ::getForField,
        mapperService,
        similarityService,
        null,
        null,
        null,
        null,
        null,
        () -> Instant.now().toEpochMilli(),
        null,
        s -> false,
        null,
        valuesSourceRegistry);
  }

  /**
   * Registers a field type and name to the MapperService for use in aggregations. This informs the
   * aggregators how to access a specific field and what value type it contains. registerField(
   * mapperService, LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, b -> b.field("type",
   * "long"));
   */
  private static void registerField(
      MapperService mapperService,
      String fieldName,
      CheckedConsumer<XContentBuilder, IOException> buildField)
      throws IOException {
    XContentBuilder mapping = fieldMapping(fieldName, buildField);
    mapperService.merge(
        "_doc",
        new CompressedXContent(BytesReference.bytes(mapping)),
        MapperService.MergeReason.MAPPING_UPDATE);
  }

  /**
   * Given an aggBuilder, will use the previously initialized queryShardContext and searchContext to
   * return an OpenSearch aggregator / Lucene Collector
   */
  public Aggregator buildAggregatorUsingContext(AggBuilder builder, IndexSearcher indexSearcher)
      throws IOException {
    SearchContext searchContext =
        new KaldbSearchContext(KaldbBigArrays.getInstance(), queryShardContext, indexSearcher);

    return getAggregationBuilder(builder)
        .build(queryShardContext, null)
        .create(searchContext, null, CardinalityUpperBound.ONE);
  }

  /**
   * Given an AggBuilder, will invoke the appropriate aggregation builder method to return the
   * abstract aggregation builder. This method is expected to be invoked from within the aggregation
   * builders to compose a nested aggregation tree.
   */
  @SuppressWarnings("rawtypes")
  protected static AbstractAggregationBuilder getAggregationBuilder(AggBuilder aggBuilder)
      throws IOException {
    if (aggBuilder.getType().equals(DateHistogramAggBuilder.TYPE)) {
      return getDateHistogramAggregationBuilder((DateHistogramAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(TermsAggBuilder.TYPE)) {
      return getTermsAggregationBuilder((TermsAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(AvgAggBuilder.TYPE)) {
      return getAvgAggregationBuilder((AvgAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(UniqueCountAggBuilder.TYPE)) {
      return getUniqueCountAggregationBuilder((UniqueCountAggBuilder) aggBuilder);
    } else {
      throw new IllegalArgumentException(
          String.format("Aggregation type %s not yet supported", aggBuilder.getType()));
    }
  }

  /**
   * Given an AvgAggBuilder returns a AvgAggregationBuilder to be used in building aggregation tree
   */
  protected static AvgAggregationBuilder getAvgAggregationBuilder(AvgAggBuilder builder) {
    // todo - this is due to incorrect schema issues
    String fieldname = builder.getField();
    if (fieldname.equals("@timestamp")) {
      fieldname = LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName;
    }

    AvgAggregationBuilder avgAggregationBuilder =
        new AvgAggregationBuilder(builder.getName()).field(fieldname);

    if (builder.getMissing() != null) {
      avgAggregationBuilder.missing(builder.getMissing());
    }

    return avgAggregationBuilder;
  }

  /**
   * Given a UniqueCountAggBuilder, returns a CardinalityAggregationBuilder (aka UniqueCount) to be
   * used in building aggregation tree
   */
  protected static CardinalityAggregationBuilder getUniqueCountAggregationBuilder(
      UniqueCountAggBuilder builder) {

    CardinalityAggregationBuilder uniqueCountAggregationBuilder =
        new CardinalityAggregationBuilder(builder.getName()).field(builder.getField());

    if (builder.getPrecisionThreshold() != null) {
      uniqueCountAggregationBuilder.precisionThreshold(builder.getPrecisionThreshold());
    }

    if (builder.getMissing() != null) {
      uniqueCountAggregationBuilder.missing(builder.getMissing());
    }

    return uniqueCountAggregationBuilder;
  }

  /**
   * Given an TermsAggBuilder returns a TermsAggregationBuilder to be used in building aggregation
   * tree
   */
  protected static TermsAggregationBuilder getTermsAggregationBuilder(TermsAggBuilder builder)
      throws IOException {

    List<String> subAggNames =
        builder
            .getSubAggregations()
            .stream()
            .map(subagg -> ((AggBuilderBase) subagg).getName())
            .collect(Collectors.toList());

    List<BucketOrder> order =
        builder
            .getOrder()
            .entrySet()
            .stream()
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
                    // this is due to the fact that the kaldb plugin thinks this is ES < 6
                    // https://github.com/slackhq/slack-kaldb-app/blob/95b091184d5de1682c97586e271cbf2bbd7cc92a/src/datasource/QueryBuilder.ts#L55
                    return BucketOrder.key(asc);
                  } else {
                    return BucketOrder.aggregation(entry.getKey(), asc);
                  }
                })
            .collect(Collectors.toList());

    TermsAggregationBuilder termsAggregationBuilder =
        new TermsAggregationBuilder(builder.getName())
            .field(builder.getField())
            .executionHint("map")
            .minDocCount(builder.getMinDocCount())
            .size(builder.getSize())
            .order(order);

    if (builder.getMissing() != null) {
      termsAggregationBuilder.missing(builder.getMissing());
    }

    for (AggBuilder subAggregation : builder.getSubAggregations()) {
      termsAggregationBuilder.subAggregation(getAggregationBuilder(subAggregation));
    }

    return termsAggregationBuilder;
  }

  /**
   * Given an DateHistogramAggBuilder returns a DateHistogramAggregationBuilder to be used in
   * building aggregation tree
   */
  protected static DateHistogramAggregationBuilder getDateHistogramAggregationBuilder(
      DateHistogramAggBuilder builder) throws IOException {

    // todo - this is due to incorrect schema issues
    String fieldname = builder.getField();
    if (fieldname.equals("@timestamp")) {
      fieldname = LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName;
    }

    DateHistogramAggregationBuilder dateHistogramAggregationBuilder =
        new DateHistogramAggregationBuilder(builder.getName())
            .field(fieldname)
            .minDocCount(builder.getMinDocCount())
            .fixedInterval(new DateHistogramInterval(builder.getInterval()));

    if (builder.getOffset() != null && !builder.getOffset().isEmpty()) {
      dateHistogramAggregationBuilder.offset(builder.getOffset());
    }

    if (builder.getFormat() != null && !builder.getFormat().isEmpty()) {
      // todo - this should be used when the field type is changed to date
      // dateHistogramAggregationBuilder.format(builder.getFormat());
    }

    if (builder.getMinDocCount() == 0) {
      if (builder.getExtendedBounds() != null
          && builder.getExtendedBounds().containsKey("min")
          && builder.getExtendedBounds().containsKey("max")) {

        LongBounds longBounds =
            new LongBounds(
                builder.getExtendedBounds().get("min"), builder.getExtendedBounds().get("max"));
        dateHistogramAggregationBuilder.extendedBounds(longBounds);
      } else {
        // Minimum doc count _must_ be used with an extended bounds param
        // As per
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-histogram-aggregation.html#search-aggregations-bucket-histogram-aggregation-extended-bounds
        // "Using extended_bounds only makes sense when min_doc_count is 0 (the empty buckets will
        // never be returned if min_doc_count is greater than 0)."
        throw new IllegalArgumentException(
            "Extended bounds must be provided if using a min doc count");
      }
    }

    for (AggBuilder subAggregation : builder.getSubAggregations()) {
      dateHistogramAggregationBuilder.subAggregation(getAggregationBuilder(subAggregation));
    }

    return dateHistogramAggregationBuilder;
  }
}
