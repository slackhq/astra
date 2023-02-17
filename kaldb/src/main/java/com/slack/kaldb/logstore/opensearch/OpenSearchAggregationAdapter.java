package com.slack.kaldb.logstore.opensearch;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.aggregations.AggBuilder;
import com.slack.kaldb.logstore.search.aggregations.AvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.CollectorManager;
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
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
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
                  AggregationBuilder.class, AvgAggregationBuilder.NAME, AvgAggregationBuilder::new),
              new NamedWriteableRegistry.Entry(
                  InternalAggregation.class, AvgAggregationBuilder.NAME, InternalAvg::new),
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
  private final SearchContext searchContext;

  public OpenSearchAggregationAdapter(Map<String, LuceneFieldDef> chunkSchema) {
    IndexSettings indexSettings = buildIndexSettings();
    SimilarityService similarityService = new SimilarityService(indexSettings, null, emptyMap());
    MapperService mapperService = buildMapperService(indexSettings, similarityService);

    // todo - see SchemaAwareLogDocumentBuilderImpl.getDefaultLuceneFieldDefinitions
    //  this needs to be adapted to include other field types once we have support
    for (Map.Entry<String, LuceneFieldDef> entry : chunkSchema.entrySet()) {
      if (entry.getValue().fieldType == FieldType.LONG) {
        try {
          registerField(
              mapperService,
              entry.getValue().name,
              b -> b.field("type", entry.getValue().fieldType.name));
        } catch (Exception e) {
          LOG.error("Error parsing schema mapping for {}", entry.getValue().toString(), e);
        }
      }
    }

    this.queryShardContext =
        buildQueryShardContext(
            KaldbBigArrays.getInstance(), indexSettings, similarityService, mapperService);
    this.searchContext = new KaldbSearchContext(KaldbBigArrays.getInstance(), queryShardContext);
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

  /**
   * Deserializes a bytearray into an InternalDateHistogram. <br>
   * TODO - abstract this to allow deserialization of any internal aggregation
   */
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

  /**
   * Builds a CollectorManager for use in the Lucene aggregation step <br>
   * TODO - abstract this to allow instantiating other aggregators than just a date histogram
   */
  public CollectorManager<Aggregator, InternalAggregation> getCollectorManager(
      AggBuilder aggBuilder) {
    return new CollectorManager<>() {
      @Override
      public Aggregator newCollector() throws IOException {
        Aggregator aggregator = buildAggregatorTree(aggBuilder);
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
    AvgAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
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

  public Aggregator buildAggregatorTree(AggBuilder builder) throws IOException {
    return getAggregationBuilder(builder)
        .build(queryShardContext, null)
        .create(searchContext, null, CardinalityUpperBound.ONE);
  }

  protected static AbstractAggregationBuilder getAggregationBuilder(AggBuilder aggBuilder)
      throws IOException {
    if (aggBuilder.getType().equals(DateHistogramAggBuilder.TYPE)) {
      return getDateHistogramAggregationBuilder((DateHistogramAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(AvgAggBuilder.TYPE)) {
      return buildAvgAggregator((AvgAggBuilder) aggBuilder);
    } else {
      throw new IllegalArgumentException(
          String.format("Aggregation type %s not yet supported", aggBuilder.getType()));
    }
  }

  protected static AvgAggregationBuilder buildAvgAggregator(AvgAggBuilder builder) {
    // todo - this is due to incorrect schema issues
    String fieldname = builder.getField();
    if (fieldname.equals("@timestamp")) {
      fieldname = LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName;
    }

    return new AvgAggregationBuilder(builder.getName()).field(fieldname);
  }

  /**
   * Builds a Lucene collector that can be used in native Lucene search methods using the OpenSearch
   * aggregations implementation.
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
