package com.slack.kaldb.logstore;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.opensearch.Version;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchType;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.Rounding;
import org.opensearch.common.TriFunction;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.fielddata.LeafFieldData;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.DataStreamFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.script.AggregationScript;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregatorFactory;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.FieldContext;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.dfs.DfsSearchResult;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.StoredFieldsContext;
import org.opensearch.search.fetch.subphase.FetchDocValuesContext;
import org.opensearch.search.fetch.subphase.FetchFieldsContext;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.ScriptFieldsContext;
import org.opensearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ScrollContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.profile.Profilers;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.search.suggest.SuggestionSearchContext;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

public class OpensearchShim {


  protected static final XContentBuilder mapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
    XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
    buildFields.accept(builder);
    return builder.endObject().endObject().endObject();
  }

  protected static final XContentBuilder fieldMapping(String fieldName, CheckedConsumer<XContentBuilder, IOException> buildField) throws IOException {
    return mapping(b -> {
      b.startObject(fieldName);
      buildField.accept(b);
      b.endObject();
    });
  }


  public static CollectorManager<Aggregator, InternalAggregation> getCollectorManager() {

//    return new CollectorManager<?, ?>


//
//    AggregationPhase aggregationPhase = new AggregationPhase();
//    aggregationPhase.preProcess();
//
//
    return new CollectorManager<>() {
      @Override
      public Aggregator newCollector() throws IOException {
        Aggregator aggregator = OpensearchShim.test();
        aggregator.preCollection();
        return aggregator;
      }

      @Override
      public InternalAggregation reduce(Collection<Aggregator> collectors) throws IOException {
        if (collectors.size() == 1) {
          return collectors.stream().findFirst().get().buildTopLevel();
        }
        throw new IllegalArgumentException("NOT IMPLEMENTED");
//        return null;
      }
    };
//    return OpensearchShim.getCollector();
  }

  public static Collector getCollector() {
    try {
      Aggregator aggregator = OpensearchShim.test();
      aggregator.preCollection();


      return aggregator;
    } catch (Exception e) {
      return Aggregator.NO_OP_COLLECTOR;
    }
  }

  public static Aggregator test() throws IOException {


    final BigArrays bigArrays = new BigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService(), "none");


    /*
    registerAggregation(
            new AggregationSpec(AvgAggregationBuilder.NAME, AvgAggregationBuilder::new, AvgAggregationBuilder.PARSER).addResultReader(
                InternalAvg::new
            ).setAggregatorRegistrar(AvgAggregationBuilder::registerAggregators),
            builder
        );
     */


//    ValuesSourceType dateHistogramValuesSourceType = new ValuesSourceType() {
//      @Override
//      public ValuesSource getEmpty() {
//        return null;
//      }
//
//      @Override
//      public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
//        return null;
//      }
//
//      @Override
//      public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
//        return new ValuesSource.Numeric() {
//          @Override
//          public boolean isFloatingPoint() {
//            return false;
//          }
//
//          @Override
//          public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
//            NumericDocValues values = context.reader().getNumericDocValues(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
//            return DocValues.singleton(values);
//          }
//
//          @Override
//          public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
//            return null;
//          }
//
//          @Override
//          public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
//            return null;
//          }
//        };
//
//        //return null;
//      }
//
//      @Override
//      public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
//        return null;
//      }
//
//      @Override
//      public String typeName() {
//        return null;
//      }
//    };
//
//    ValuesSourceType avgValuesSourceType = new ValuesSourceType() {
//      @Override
//      public ValuesSource getEmpty() {
//        return null;
//      }
//
//      @Override
//      public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
//        return null;
//      }
//
//      @Override
//      public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
//        return new ValuesSource.Numeric() {
//          @Override
//          public boolean isFloatingPoint() {
//            return false;
//          }
//
//          @Override
//          public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
//            NumericDocValues values = context.reader().getNumericDocValues(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
//            return DocValues.singleton(values);
//          }
//
//          @Override
//          public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
//            return null;
//          }
//
//          @Override
//          public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
//            return null;
//          }
//        };
//
//        //return null;
//      }
//
//      @Override
//      public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
//        return null;
//      }
//
//      @Override
//      public String typeName() {
//        return null;
//      }
//    };

    ValuesSourceRegistry.Builder valuesSourceRegistryBuilder = new ValuesSourceRegistry.Builder();
    DateHistogramAggregatorFactory.registerAggregators(valuesSourceRegistryBuilder);


//    registerAggregation(
//        new SearchPlugin.AggregationSpec(AvgAggregationBuilder.NAME, AvgAggregationBuilder::new, AvgAggregationBuilder.PARSER).addResultReader(
//            InternalAvg::new
//        ).setAggregatorRegistrar(AvgAggregationBuilder::registerAggregators),
//        builder
//    );


//    valuesSourceRegistryBuilder.register(new ValuesSourceRegistry.RegistryKey<>(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, Long.class), new ValuesSourceType() {
//      @Override
//      public ValuesSource getEmpty() {
//        return null;
//      }
//
//      @Override
//      public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
//        return null;
//      }
//
//      @Override
//      public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
//        return null;
//      }
//
//      @Override
//      public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
//        return null;
//      }
//
//      @Override
//      public String typeName() {
//        return LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName;
////        return null;
//      }
//    }, 0L, true);

    //valuesSourceRegistryBuilder.register();


//    valuesSourceRegistryBuilder.register(
//        DateHistogramAggregationBuilder.REGISTRY_KEY, CoreValuesSourceType.DATE, new DateHistogramAggregationSupplier() {
//          @Override
//          public Aggregator build(String name, AggregatorFactories factories, Rounding rounding, Rounding.Prepared preparedRounding, BucketOrder order, boolean keyed, long minDocCount, LongBounds extendedBounds, LongBounds hardBounds, ValuesSourceConfig valuesSourceConfig, SearchContext aggregationContext, Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata) throws IOException {
//            return null;
//          }
//        }, false
//    );
//

    //valuesSourceRegistryBuilder.register();
//
//    valuesSourceRegistryBuilder.register(DateHistogramAggregationBuilder.REGISTRY_KEY, new ValuesSourceType() {
//      @Override
//      public ValuesSource getEmpty() {
//        return null;
//      }
//
//      @Override
//      public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
//        return null;
//      }
//
//      @Override
//      public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
//        return null;
//      }
//
//      @Override
//      public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
//        return null;
//      }
//
//      @Override
//      public String typeName() {
//        return "DATE";
//        //return null;
//      }
//    }, new DateHistogramAggregationSupplier() {
//      @Override
//      public Aggregator build(String name, AggregatorFactories factories, Rounding rounding, Rounding.Prepared preparedRounding, BucketOrder order, boolean keyed, long minDocCount, LongBounds extendedBounds, LongBounds hardBounds, ValuesSourceConfig valuesSourceConfig, SearchContext aggregationContext, Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata) throws IOException {
//        return null;
//      }
//    }, true);

//    valuesSourceRegistryBuilder.register(DateHistogramAggregationBuilder.REGISTRY_KEY, CoreValuesSourceType.DATE, new DateHistogramAggregationSupplier() {
//      @Override
//      public Aggregator build(String name, AggregatorFactories factories, Rounding rounding, Rounding.Prepared preparedRounding, BucketOrder order, boolean keyed, long minDocCount, LongBounds extendedBounds, LongBounds hardBounds, ValuesSourceConfig valuesSourceConfig, SearchContext aggregationContext, Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata) throws IOException {
//        return null;
//      }
//    }, true);

//    valuesSourceRegistryBuilder

    //valuesSourceRegistryBuilder.registerUsage(DateHistogramAggregationBuilder.REGISTRY_KEY.getName(), CoreValuesSourceType.DATE);

//    valuesSourceRegistryBuilder.registerUsage(DateHistogramAggregationBuilder.REGISTRY_KEY.getName(), new ValuesSourceType() {
//      @Override
//      public ValuesSource getEmpty() {
//        return null;
//      }
//
//      @Override
//      public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
//        return null;
//      }
//
//      @Override
//      public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
//        return null;
//      }
//
//      @Override
//      public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
//        return null;
//      }
//
//      @Override
//      public String typeName() {
//        return "kaldbcustom";
////        return null;
//      }
//    });

//    valuesSourceRegistryBuilder.registerUsage(AvgAggregationBuilder.REGISTRY_KEY.getName(), CoreValuesSourceType.NUMERIC);
//    valuesSourceRegistryBuilder.registerUsage(ValueCountAggregationBuilder.REGISTRY_KEY.getName(), CoreValuesSourceType.BYTES);
    ValuesSourceRegistry registry = valuesSourceRegistryBuilder.build();


    //dateHistogramAggregatorFactory.

    //DateHistogramAggregatorFactory dateHistogramAggregatorFactory1

//    registry.getUsageService();

    //todo - field context on value source
    // todo - value source aggregator facotyr
    //   DateHistogramAggregatorFactory

    // todo - I have the correct valuesourceConfig, says it's unmapped (no fild context or script)
    //   the valuesSource is a ValuesSource.Numeric

    // todo - NEEDS to have a field context


    // ValuesSourceAggregationBuilder
    // ValuesSourceConfig config = resolveConfig(queryShardContext);


//    MappedFieldType mappedFieldType = new DateFieldMapper.DateFieldType("foo");


    Settings settings = Settings.builder()
        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_2_3_0)
        .build();

    Mapper.BuilderContext builderContext = new Mapper.BuilderContext(settings, new ContentPath());
    DateFieldMapper dateFieldMapper = new DateFieldMapper.Builder(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
        DateFieldMapper.Resolution.MILLISECONDS, null, false, Version.V_2_3_0).build(builderContext);

//    DynamicKeyFieldMapper dynamicKeyFieldMapper = new DynamicKeyFieldMapper() {
//      @Override
//      public MappedFieldType keyedFieldType(String key) {
//        return null;
//      }
//
//      @Override
//      protected void parseCreateField(ParseContext context) throws IOException {
//
//      }
//
//      @Override
//      protected void mergeOptions(FieldMapper other, List<String> conflicts) {
//
//      }
//
//      @Override
//      protected String contentType() {
//        return null;
//      }
//    };

    IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("testDataSet")
        .putMapping(new MappingMetadata(new CompressedXContent(dateFieldMapper, ToXContent.EMPTY_PARAMS)))
        .settings(settings)
        .build(), Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_2_3_0)
        .build());

//    SimilarityService similarityService = new SimilarityService(
//        indexSettings,
//        null,
//        Map.of()
//    );


//    MapperRegistry
    //IndicesModule.

    //MapperRegistry.

    Map<String, Mapper.TypeParser> builtInMetadataMappers;
    // Use a LinkedHashMap for metadataMappers because iteration order matters
    builtInMetadataMappers = new LinkedHashMap<>();
    // _ignored first so that we always load it, even if only _id is requested
    builtInMetadataMappers.put(IgnoredFieldMapper.NAME, IgnoredFieldMapper.PARSER);
    // ID second so it will be the first (if no ignored fields) stored field to load
    // (so will benefit from "fields: []" early termination
    builtInMetadataMappers.put(IdFieldMapper.NAME, IdFieldMapper.PARSER);
    builtInMetadataMappers.put(RoutingFieldMapper.NAME, RoutingFieldMapper.PARSER);
    builtInMetadataMappers.put(IndexFieldMapper.NAME, IndexFieldMapper.PARSER);
    builtInMetadataMappers.put(DataStreamFieldMapper.NAME, DataStreamFieldMapper.PARSER);
    builtInMetadataMappers.put(SourceFieldMapper.NAME, SourceFieldMapper.PARSER);
    builtInMetadataMappers.put(NestedPathFieldMapper.NAME, NestedPathFieldMapper.PARSER);
    builtInMetadataMappers.put(VersionFieldMapper.NAME, VersionFieldMapper.PARSER);
    builtInMetadataMappers.put(SeqNoFieldMapper.NAME, SeqNoFieldMapper.PARSER);
    builtInMetadataMappers.put(DocCountFieldMapper.NAME, DocCountFieldMapper.PARSER);
    // _field_names must be added last so that it has a chance to see all the other mappers
    builtInMetadataMappers.put(FieldNamesFieldMapper.NAME, FieldNamesFieldMapper.PARSER);
    //return Collections.unmodifiableMap(builtInMetadataMappers);

    builtInMetadataMappers.put(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, new Mapper.TypeParser() {
      @Override
      public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
        return null;
      }
    });


//    MapperRegistry mapperRegistry = new MapperRegistry(
//        builtInMetadataMappers,
//        //Map.of(),
//        Map.of(),
//        new Function<String, Predicate<String>>() {
//          @Override
//          public Predicate<String> apply(String s) {
//            return null;
//          }
//        }
//    );

    MapperRegistry mapperRegistry = new IndicesModule(
        emptyList().stream().filter(p -> p instanceof MapperPlugin).map(p -> (MapperPlugin) p).collect(toList())
    ).getMapperRegistry();

    //mapperRegistry

    IndexAnalyzers indexAnalyzers = new IndexAnalyzers(
        singletonMap("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
        emptyMap(),
        emptyMap()
    );


    SimilarityService similarityService = new SimilarityService(indexSettings, null, emptyMap());
    MapperService mapperService = new MapperService(
        indexSettings,
        indexAnalyzers,
        new NamedXContentRegistry(
            ClusterModule.getNamedXWriteables()
        ),
        similarityService,
        mapperRegistry,
        () -> { throw new UnsupportedOperationException(); },
        () -> true,
        null
    );


    /*
    protected final XContentBuilder mapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
        buildFields.accept(builder);
        return builder.endObject().endObject().endObject();
    }
     */

    /*
        protected final XContentBuilder fieldMapping(CheckedConsumer<XContentBuilder, IOException> buildField) throws IOException {
        return mapping(b -> {
            b.startObject("field");
            buildField.accept(b);
            b.endObject();
        });
    }
     */

//    try {
//      XContentBuilder mapping = OpensearchShim.fieldMapping(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, b -> b.field("type", "long"));
      XContentBuilder mapping = OpensearchShim.fieldMapping(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, b -> b.field("type", "long")); //.field("format", "epoch_millis"));
      mapperService.merge("_doc", new CompressedXContent(BytesReference.bytes(mapping)), MapperService.MergeReason.MAPPING_UPDATE);
//    } catch (Exception e) {
//
//    }


    //mapperService.documentMapperWithAutoCreate()

//    Map<String, Object> map = Collections.unmodifiableMap(builtInMetadataMappers);
//    mapperService.merge("_doc", map, MapperService.MergeReason.MAPPING_UPDATE);
//    MetadataFieldMapper.Builder("").build(builderContext);
//    mapperService.merge("type", new CompressedXContent(new FieldNamesFieldMapper(new Explicit<>(true, true), Version.V_1_1_0, new FieldNamesFieldMapper.FieldNamesFieldType(true))));
//    mapperService.documentMapper();


    DateFieldMapper.DateFieldType dateFieldType = new DateFieldMapper.DateFieldType("_time");

    //Mapper.BuilderContext builderContext = new Mapper.BuilderContext(Settings.EMPTY, new ContentPath());
//    DateFieldMapper dateFieldMapper = new DateFieldMapper.Builder(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
//        DateFieldMapper.Resolution.MILLISECONDS, null, false, Version.V_1_0_0).build(builderContext);

//    mapperService.m

//    mapperService.merge(SINGLE_MAPPING_NAME, new CompressedXContent(dateFieldMapper, ToXContent.EMPTY_PARAMS), MapperService.MergeReason.MAPPING_UPDATE);
//    mapperService.merge("type", Map.of("string", new Object()), MapperService.MergeReason.MAPPING_UPDATE);

//    mapperService.merge(new IndexMetadata.Builder("index")
//            .
//        .build());
//


    //DateFieldMapper.DateFieldType dateFieldType = new DateFieldMapper.DateFieldType("_time");


    QueryShardContext queryShardContext = new QueryShardContext(
        0,
        indexSettings,
        bigArrays,
        null,
//        null,
        new TriFunction<MappedFieldType, String, Supplier<SearchLookup>, IndexFieldData<?>>() {
          @Override
          public IndexFieldData<?> apply(MappedFieldType mappedFieldType, String s, Supplier<SearchLookup> searchLookupSupplier) {
            MappedFieldType mappedFieldType1 = mappedFieldType;
            String index = s;
            Supplier<SearchLookup> searchLookupSupplier1 = searchLookupSupplier;

            return new SortedNumericIndexFieldData(mappedFieldType.name(), IndexNumericFieldData.NumericType.LONG);
//            return new IndexFieldData<LeafFieldData>() {
//              @Override
//              public String getFieldName() {
//                return null;
//              }
//
//              @Override
//              public ValuesSourceType getValuesSourceType() {
////                new ValuesSourceType()
//                return new SortedNumericIndexFieldData(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, IndexNumericFieldData.NumericType.LONG).getValuesSourceType();
////                return IndexNumericFieldData.NumericType.LONG.getValuesSourceType();
////                return CoreValuesSourceType.NUMERIC;
////                return null;
//              }
//
//              @Override
//              public LeafFieldData load(LeafReaderContext context) {
//                return null;
//              }
//
//              @Override
//              public LeafFieldData loadDirect(LeafReaderContext context) throws Exception {
//                return null;
//              }
//
//              @Override
//              public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
//                return null;
//              }
//
//              @Override
//              public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
//                return null;
//              }
//            };
//            return null;
          }
        },
        //null,
        mapperService,
        similarityService,
//        null,
        null,
        null,
        null,
        null,
        null,
        () -> {
          return Instant.now().toEpochMilli();
        },
        null,
        new Predicate<String>() {
          @Override
          public boolean test(String s) {
            return false;
          }
        },
        null,
//        null
        registry
    );

    //queryShardContext.fieldMapper()

    SearchContext searchContext = new SearchContext() {
      @Override
      public void setTask(SearchShardTask task) {

      }

      @Override
      public SearchShardTask getTask() {
        return null;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      protected void doClose() {

      }

      @Override
      public void preProcess(boolean rewrite) {

      }

      @Override
      public Query buildFilteredQuery(Query query) {
        return null;
      }

      @Override
      public ShardSearchContextId id() {
        return null;
      }

      @Override
      public String source() {
        return null;
      }

      @Override
      public ShardSearchRequest request() {
        return null;
      }

      @Override
      public SearchType searchType() {
        return null;
      }

      @Override
      public SearchShardTarget shardTarget() {
        return null;
      }

      @Override
      public int numberOfShards() {
        return 0;
      }

      @Override
      public float queryBoost() {
        return 0;
      }

      @Override
      public ScrollContext scrollContext() {
        return null;
      }

      @Override
      public SearchContextAggregations aggregations() {
        //new SearchContextAggregations()
        return null;
      }

      @Override
      public SearchContext aggregations(SearchContextAggregations aggregations) {
        return null;
      }

      @Override
      public void addSearchExt(SearchExtBuilder searchExtBuilder) {

      }

      @Override
      public SearchExtBuilder getSearchExt(String name) {
        return null;
      }

      @Override
      public SearchHighlightContext highlight() {
        return null;
      }

      @Override
      public void highlight(SearchHighlightContext highlight) {

      }

      @Override
      public SuggestionSearchContext suggest() {
        return null;
      }

      @Override
      public void suggest(SuggestionSearchContext suggest) {

      }

      @Override
      public List<RescoreContext> rescore() {
        return null;
      }

      @Override
      public void addRescore(RescoreContext rescore) {

      }

      @Override
      public boolean hasScriptFields() {
        return false;
      }

      @Override
      public ScriptFieldsContext scriptFields() {
        return null;
      }

      @Override
      public boolean sourceRequested() {
        return false;
      }

      @Override
      public boolean hasFetchSourceContext() {
        return false;
      }

      @Override
      public FetchSourceContext fetchSourceContext() {
        return null;
      }

      @Override
      public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        return null;
      }

      @Override
      public FetchDocValuesContext docValuesContext() {
        return null;
      }

      @Override
      public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
        return null;
      }

      @Override
      public FetchFieldsContext fetchFieldsContext() {
        return null;
      }

      @Override
      public SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext) {
        return null;
      }

      @Override
      public ContextIndexSearcher searcher() {
        return null;
      }

      @Override
      public IndexShard indexShard() {
        return null;
      }

      @Override
      public MapperService mapperService() {
        return null;
      }

      @Override
      public SimilarityService similarityService() {
        return null;
      }

      @Override
      public BigArrays bigArrays() {
        return bigArrays;
      }

      @Override
      public BitsetFilterCache bitsetFilterCache() {
        return null;
      }

      @Override
      public TimeValue timeout() {
        return null;
      }

      @Override
      public void timeout(TimeValue timeout) {

      }

      @Override
      public int terminateAfter() {
        return 0;
      }

      @Override
      public void terminateAfter(int terminateAfter) {

      }

      @Override
      public boolean lowLevelCancellation() {
        return false;
      }

      @Override
      public SearchContext minimumScore(float minimumScore) {
        return null;
      }

      @Override
      public Float minimumScore() {
        return null;
      }

      @Override
      public SearchContext sort(SortAndFormats sort) {
        return null;
      }

      @Override
      public SortAndFormats sort() {
        return null;
      }

      @Override
      public SearchContext trackScores(boolean trackScores) {
        return null;
      }

      @Override
      public boolean trackScores() {
        return false;
      }

      @Override
      public SearchContext trackTotalHitsUpTo(int trackTotalHits) {
        return null;
      }

      @Override
      public int trackTotalHitsUpTo() {
        return 0;
      }

      @Override
      public SearchContext searchAfter(FieldDoc searchAfter) {
        return null;
      }

      @Override
      public FieldDoc searchAfter() {
        return null;
      }

      @Override
      public SearchContext collapse(CollapseContext collapse) {
        return null;
      }

      @Override
      public CollapseContext collapse() {
        return null;
      }

      @Override
      public SearchContext parsedPostFilter(ParsedQuery postFilter) {
        return null;
      }

      @Override
      public ParsedQuery parsedPostFilter() {
        return null;
      }

      @Override
      public Query aliasFilter() {
        return null;
      }

      @Override
      public SearchContext parsedQuery(ParsedQuery query) {
        return null;
      }

      @Override
      public ParsedQuery parsedQuery() {
        return null;
      }

      @Override
      public Query query() {
        return null;
      }

      @Override
      public int from() {
        return 0;
      }

      @Override
      public SearchContext from(int from) {
        return null;
      }

      @Override
      public int size() {
        return 0;
      }

      @Override
      public SearchContext size(int size) {
        return null;
      }

      @Override
      public boolean hasStoredFields() {
        return false;
      }

      @Override
      public boolean hasStoredFieldsContext() {
        return false;
      }

      @Override
      public boolean storedFieldsRequested() {
        return false;
      }

      @Override
      public StoredFieldsContext storedFieldsContext() {
        return null;
      }

      @Override
      public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
        return null;
      }

      @Override
      public boolean explain() {
        return false;
      }

      @Override
      public void explain(boolean explain) {

      }

      @Override
      public List<String> groupStats() {
        return null;
      }

      @Override
      public void groupStats(List<String> groupStats) {

      }

      @Override
      public boolean version() {
        return false;
      }

      @Override
      public void version(boolean version) {

      }

      @Override
      public boolean seqNoAndPrimaryTerm() {
        return false;
      }

      @Override
      public void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {

      }

      @Override
      public int[] docIdsToLoad() {
        return new int[0];
      }

      @Override
      public int docIdsToLoadFrom() {
        return 0;
      }

      @Override
      public int docIdsToLoadSize() {
        return 0;
      }

      @Override
      public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        return null;
      }

      @Override
      public DfsSearchResult dfsResult() {
        return null;
      }

      @Override
      public QuerySearchResult queryResult() {
        return null;
      }

      @Override
      public FetchPhase fetchPhase() {
        return null;
      }

      @Override
      public FetchSearchResult fetchResult() {
        return null;
      }

      @Override
      public Profilers getProfilers() {
        return null;
      }

      @Override
      public MappedFieldType fieldType(String name) {
        return null;
      }

      @Override
      public ObjectMapper getObjectMapper(String name) {
        return null;
      }

      @Override
      public long getRelativeTimeInMillis() {
        return 0;
      }

      @Override
      public Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> queryCollectorManagers() {
        return null;
      }

      @Override
      public QueryShardContext getQueryShardContext() {
        return queryShardContext;
      }

      @Override
      public ReaderContext readerContext() {
        return null;
      }
    };


//    MappedFieldType mp = new MappedFieldType(
//        "name",
//        false,
//        false,
//        true,
//        TextSearchInfo.NONE,
//        Map.of("bar", "baz")
//    ) {
//      @Override
//      public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
//        return null;
//      }
//
//      @Override
//      public String typeName() {
//        return "foo";
////        return null;
//      }
//
//      @Override
//      public Query termQuery(Object value, QueryShardContext context) {
//        return null;
//      }
//    };


//    NumberFieldMapper.NumberFieldType numberFieldType = new NumberFieldMapper.NumberFieldType(
//        "doubleproperty",
//        NumberFieldMapper.NumberType.DOUBLE
//    );

//    FieldContext doubleFieldContext = new FieldContext("name",
//        null,
////        new SortedNumericIndexFieldData("fieldname", IndexNumericFieldData.NumericType.DATE),
//        numberFieldType);
////        null);


//    DateFieldMapper.DateFieldType timestampFieldType = new DateFieldMapper.DateFieldType(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
//    NumberFieldMapper.NumberFieldType timestampFieldType = new DateFieldMapper.Builder().build()
//
//
//        new NumberFieldMapper.NumberFieldType(
//        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
//        NumberFieldMapper.
//    );

//    FieldContext longFieldContent = new FieldContext("name",
//        null,
////        new SortedNumericIndexFieldData("fieldname", IndexNumericFieldData.NumericType.DATE),
//        timestampFieldType);
//
//    FieldContext timestampFieldContent = new FieldContext("name2", null, timestampFieldType);


//    ValuesSourceType valuesSourceType = new ValuesSourceType() {
//      @Override
//      public ValuesSource getEmpty() {
//        return null;
//      }
//
//      @Override
//      public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
//        return null;
//      }
//
//      @Override
//      public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
//        return new ValuesSource.Numeric() {
//          @Override
//          public boolean isFloatingPoint() {
//            return false;
//          }
//
//          @Override
//          public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
//            NumericDocValues values = context.reader().getNumericDocValues(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
//            return DocValues.singleton(values);
//          }
//
//          @Override
//          public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
//            return null;
//          }
//
//          @Override
//          public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
//            return null;
//          }
//        };
//
//        //return null;
//      }
//
//      @Override
//      public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
//        return null;
//      }
//
//      @Override
//      public String typeName() {
//        return null;
//      }
//    };

//    ValuesSourceConfig valuesSourceConfig = new ValuesSourceConfig(
//        valuesSourceType,
//        null,
////        timestampFieldContent,
//        true,
//        null,
//        null,
//        null,
//        ZoneId.systemDefault(),
//        DocValueFormat.RAW,
//        Instant.EPOCH::getEpochSecond
//    );

//    ValuesSourceConfig resolved = ValuesSourceConfig.resolve(
//        ,
//        ValueType.DATE,
//        "foo",
//        null,
//        null,
//        ZoneId.systemDefault(),
//        "format",
//        CoreValuesSourceType.DATE
//    );


    //longFieldContent.


    AvgAggregationBuilder avgAggregationBuilder = new AvgAggregationBuilder("foo").field("doubleproperty");
    ValueCountAggregationBuilder valueCountAggregationBuilder = new ValueCountAggregationBuilder("baz").field("doubleproperty");
//    avgAggregationBuilder.build().create()
    DateHistogramAggregationBuilder dateHistogramAggregationBuilder =
        new DateHistogramAggregationBuilder("bar")
            .fixedInterval(DateHistogramInterval.HOUR)
            .keyed(true)
            .field(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
//

//    new DateHistogramAggregator()

//    dateHistogramAggregationBuilder.subAggregation(avgAggregationBuilder);
//    dateHistogramAggregationBuilder.subAggregation(valueCountAggregationBuilder);
//    Aggregator aggregator = dateHistogramAggregationBuilder.build(queryShardContext, null)
//        .create(searchContext, null, CardinalityUpperBound.ONE);


//    DateFieldMapper dateFieldMapper = new DateFieldMapper.Builder(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
//        DateFieldMapper.Resolution.MILLISECONDS, null, false, Version.V_2_3_0).build(builderContext);


//    DateFieldMapper.DateFieldType mappedFieldType = new DateFieldMapper.DateFieldType(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

    NumberFieldMapper.NumberFieldType numberFieldType = new NumberFieldMapper.NumberFieldType(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, NumberFieldMapper.NumberType.LONG);


    SortedNumericIndexFieldData sortedNumericIndexFieldData = new SortedNumericIndexFieldData(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, IndexNumericFieldData.NumericType.DATE);

//    sortedNumericIndexFieldData.v

    ValuesSource.Numeric numeric = new ValuesSource.Numeric() {
      @Override
      public boolean isFloatingPoint() {
        return false;
      }

      @Override
      public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
        return null;
      }

      @Override
      public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
        return null;
      }

      @Override
      public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
        return null;
      }
    };


//    ValuesSourceConfig valuesSourceConfig = ValuesSourceConfig.resolveUnmapped(CoreValuesSourceType.DATE, queryShardContext);




    //NumberFieldMapper.NumberType.LONG
//    NumberFieldMapper mappedFieldType = new NumberFieldMapper.Builder(
//        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
//        NumberFieldMapper.NumberType.LONG,
//        true,
//        true
//    ).build(builderContext);

//    new SortedNumericIndexFieldData(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, IndexNumericFieldData.NumericType.LONG).getValuesSourceType()

    ValuesSourceConfig valuesSourceConfig = ValuesSourceConfig.resolve(
        queryShardContext,
        null, //ValueType.DA,
        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
        null,
        null,
        null,
//        ZoneId.systemDefault(),
        null,
        new SortedNumericIndexFieldData(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, IndexNumericFieldData.NumericType.LONG).getValuesSourceType()
//        CoreValuesSourceType.NUMERIC
//        new ValuesSourceType() {
//          @Override
//          public ValuesSource getEmpty() {
//            return null;
//          }
//
//          @Override
//          public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
//            return null;
//          }
//
//          @Override
//          public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
//            return null;
//          }
//
//          @Override
//          public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
//            return null;
//          }
//
//          @Override
//          public String typeName() {
//            return null;
//          }
//        }
        //sortedNumericIndexFieldData.getValuesSourceType()
    );

    DateHistogramAggregatorFactory factory = new DateHistogramAggregatorFactory(
        "name",
        valuesSourceConfig,
//        ValuesSourceConfig.resolveUnmapped(sortedNumericIndexFieldData.getValuesSourceType(), queryShardContext),
//        ValuesSourceConfig.resolveFieldOnly(
//            numberFieldType,
//            queryShardContext
//        ),
//        new ValuesSourceConfig(),
        BucketOrder.key(true),
        true,
        10
        , Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build(),
        null,
        null,
        queryShardContext,
        null,
        new AggregatorFactories.Builder(),
        Map.of("foo", "bar")
    );


    Aggregator aggregator = factory.create(searchContext, null, CardinalityUpperBound.ONE);

//    avgAggregationBuilder.
//
//    avgAggregationBuilder.field("doubleproperty");

    //avgAggregationBuilder.

//    AggregatorFactories.Builder subFactoriesBuilder = AggregatorFactories.builder();
//    subFactoriesBuilder.addAggregator(avgAggregationBuilder);
//
//
//
//
////    valuesSourceConfig
//
//    DateHistogramAggregatorFactory dateHistogramFactory = new DateHistogramAggregatorFactory(
//        "name",
//        valuesSourceConfig,
//        BucketOrder.key(false),
//        false,
//        10,
//        Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build(),
//        null,
//        null,
//        null,
////        queryShardContext,
//        null,
//        subFactoriesBuilder,
//        Map.of("foo", "bar")
//    );
//
////    AvgAggregationBuilder avgAggregationBuilder = new AvgAggregationBuilder("foo");
//    //AggregatorFactory avgFactory = avgAggregationBuilder.build(null, dateHistogramFactory);
//
//
//
//    //searchContext.aggregations().aggregators();
//
//    Aggregator aggregator = dateHistogramFactory.create(
//        searchContext,
//        null,
//        CardinalityUpperBound.ONE
//    );


//    aggregator.preCollection();

    return aggregator;
  }
}
