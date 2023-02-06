package com.slack.kaldb.logstore.opensearch;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

import com.slack.kaldb.logstore.LogMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
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
import org.opensearch.common.util.PageCacheRecycler;
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
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregatorFactory;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.internal.SearchContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchAdapter.class);

  public static byte[] toByteArray(InternalAggregation internalAggregation) {
    if (internalAggregation == null) {
      return new byte[] {};
    }

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(byteArrayOutputStream);
    try {
      internalAggregation.writeTo(streamOutput);
    } catch (Exception e) {
      LOG.error("Error writing internal agg to byte array", e);
    }
    return byteArrayOutputStream.toByteArray();
  }

  public static InternalAutoDateHistogram fromByteArray(byte[] bytes) throws IOException {
    NamedWriteableRegistry namedWriteableRegistry =
        new NamedWriteableRegistry(
            Arrays.asList(
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
                    DocValueFormat.class,
                    DocValueFormat.DateTime.NAME,
                    DocValueFormat.DateTime::new),
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

    InputStream inputStream = new ByteArrayInputStream(bytes);
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    NamedWriteableAwareStreamInput namedWriteableAwareStreamInput =
        new NamedWriteableAwareStreamInput(streamInput, namedWriteableRegistry);

    return new InternalAutoDateHistogram(namedWriteableAwareStreamInput);
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

  public static CollectorManager<Aggregator, InternalAggregation> getCollectorManager(
      int numBuckets) {
    return new CollectorManager<>() {
      @Override
      public Aggregator newCollector() throws IOException {
        Aggregator aggregator = buildAutoDateHistogramAggregator(numBuckets);
        aggregator.preCollection();
        return aggregator;
      }

      @Override
      public InternalAggregation reduce(Collection<Aggregator> collectors) throws IOException {
        if (collectors.size() == 1) {
          collectors.stream().findFirst().get().postCollection();
          return collectors.stream().findFirst().get().buildTopLevel();
        }
        throw new IllegalArgumentException("NOT IMPLEMENTED");
      }
    };
  }

  private static ValuesSourceRegistry buildValueSourceRegistry() {
    ValuesSourceRegistry.Builder valuesSourceRegistryBuilder = new ValuesSourceRegistry.Builder();

    AutoDateHistogramAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    DateHistogramAggregatorFactory.registerAggregators(valuesSourceRegistryBuilder);
    AvgAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    ValueCountAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);

    return valuesSourceRegistryBuilder.build();
  }

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

  public static Aggregator buildAutoDateHistogramAggregator(int numBuckets) throws IOException {
    final BigArrays bigArrays =
        new BigArrays(
            PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService(), "none");
    IndexSettings indexSettings = buildIndexSettings();
    SimilarityService similarityService = new SimilarityService(indexSettings, null, emptyMap());
    MapperService mapperService = buildMapperService(indexSettings, similarityService);

    registerField(
        mapperService,
        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
        b -> b.field("type", "long"));

    QueryShardContext queryShardContext =
        buildQueryShardContext(bigArrays, indexSettings, similarityService, mapperService);
    SearchContext searchContext = new KaldbSearchContext(bigArrays, queryShardContext);

    AutoDateHistogramAggregationBuilder autoDateHistogramAggregationBuilder =
        new AutoDateHistogramAggregationBuilder("datehistogram")
            .setNumBuckets(numBuckets)
            .setMinimumIntervalExpression("minute")
            .field(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

    ValueCountAggregationBuilder valueCountAggregationBuilder =
        new ValueCountAggregationBuilder("valuecount")
            .field(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

    autoDateHistogramAggregationBuilder.subAggregation(valueCountAggregationBuilder);
    return autoDateHistogramAggregationBuilder
        .build(queryShardContext, null)
        .create(searchContext, null, CardinalityUpperBound.ONE);
  }
}
