package com.slack.kaldb.logstore;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.opensearch.Version;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchType;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
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
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.IndexFieldDataService;
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
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregatorFactory;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
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
import org.opensearch.search.profile.Profilers;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.search.suggest.SuggestionSearchContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpensearchShim {

  private static final Logger LOG = LoggerFactory.getLogger(OpensearchShim.class);

  /*
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
                       DocValueFormat.class,
                       DocValueFormat.Decimal.NAME,
                       DocValueFormat.Decimal::new),
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

       ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
       StreamOutput streamOutput = new OutputStreamStreamOutput(byteArrayOutputStream);
       histogram.writeTo(streamOutput);

       InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
       StreamInput streamInput = new InputStreamStreamInput(inputStream);
       NamedWriteableAwareStreamInput namedWriteableAwareStreamInput =
           new NamedWriteableAwareStreamInput(streamInput, namedWriteableRegistry);

       // FilterStreamInput fs = new FilterStreamInput(streamInput);
       InternalAutoDateHistogram reconstructed =
           new InternalAutoDateHistogram(namedWriteableAwareStreamInput);
  */

  public static byte[] toByteArray(InternalAggregation internalAggregation) {
    if (internalAggregation == null) {
      return new byte[]{};
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

    //    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    //    StreamOutput streamOutput = new OutputStreamStreamOutput(byteArrayOutputStream);
    //    histogram.writeTo(streamOutput);

    InputStream inputStream = new ByteArrayInputStream(bytes);
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    NamedWriteableAwareStreamInput namedWriteableAwareStreamInput =
        new NamedWriteableAwareStreamInput(streamInput, namedWriteableRegistry);

    // FilterStreamInput fs = new FilterStreamInput(streamInput);
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
      int numBuckets, long from, long to) {
    return new CollectorManager<>() {
      @Override
      public Aggregator newCollector() throws IOException {
        Aggregator aggregator = OpensearchShim.test(numBuckets, from, to);
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

  public static Collector getCollector(int numBuckets, long from, long to) {
    try {
      Aggregator aggregator = OpensearchShim.test(numBuckets, from, to);
      aggregator.preCollection();

      return aggregator;
    } catch (Exception e) {
      return Aggregator.NO_OP_COLLECTOR;
    }
  }

  public static Aggregator test(int numBuckets, long from, long to) throws IOException {

    final BigArrays bigArrays =
        new BigArrays(
            PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService(), "none");

    ValuesSourceRegistry.Builder valuesSourceRegistryBuilder = new ValuesSourceRegistry.Builder();
    AutoDateHistogramAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    DateHistogramAggregatorFactory.registerAggregators(valuesSourceRegistryBuilder);
    AvgAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    ValueCountAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);

    ValuesSourceRegistry registry = valuesSourceRegistryBuilder.build();

    Settings settings =
        Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_2_3_0)
            .build();

    Mapper.BuilderContext builderContext = new Mapper.BuilderContext(settings, new ContentPath());
    DateFieldMapper dateFieldMapper =
        new DateFieldMapper.Builder(
                LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                DateFieldMapper.Resolution.MILLISECONDS,
                null,
                false,
                Version.V_2_3_0)
            .build(builderContext);

    IndexSettings indexSettings =
        new IndexSettings(
            IndexMetadata.builder("testDataSet")
                .putMapping(
                    new MappingMetadata(
                        new CompressedXContent(dateFieldMapper, ToXContent.EMPTY_PARAMS)))
                .settings(settings)
                .build(),
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_2_3_0).build());

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

    builtInMetadataMappers.put(
        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
        new Mapper.TypeParser() {
          @Override
          public Mapper.Builder<?> parse(
              String name, Map<String, Object> node, ParserContext parserContext)
              throws MapperParsingException {
            return null;
          }
        });

    MapperRegistry mapperRegistry =
        new IndicesModule(
                emptyList()
                    .stream()
                    .filter(p -> p instanceof MapperPlugin)
                    .map(p -> (MapperPlugin) p)
                    .collect(toList()))
            .getMapperRegistry();

    // mapperRegistry

    IndexAnalyzers indexAnalyzers =
        new IndexAnalyzers(
            singletonMap(
                "default",
                new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            emptyMap(),
            emptyMap());

    SimilarityService similarityService = new SimilarityService(indexSettings, null, emptyMap());
    MapperService mapperService =
        new MapperService(
            indexSettings,
            indexAnalyzers,
            new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
            similarityService,
            mapperRegistry,
            () -> {
              throw new UnsupportedOperationException();
            },
            () -> true,
            null);

    XContentBuilder mapping1 =
        OpensearchShim.fieldMapping(
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
            b -> b.field("type", "long")); // .field("format", "epoch_millis"));
    mapperService.merge(
        "_doc",
        new CompressedXContent(BytesReference.bytes(mapping1)),
        MapperService.MergeReason.MAPPING_UPDATE);

    XContentBuilder mapping2 =
        OpensearchShim.fieldMapping(
            "doubleproperty", b -> b.field("type", "double")); // .field("format", "epoch_millis"));
    mapperService.merge(
        "_doc",
        new CompressedXContent(BytesReference.bytes(mapping2)),
        MapperService.MergeReason.MAPPING_UPDATE);

    IndexFieldDataService indexFieldDataService =
        new IndexFieldDataService(
            indexSettings,
            new IndicesFieldDataCache(settings, new IndexFieldDataCache.Listener() {}),
            new NoneCircuitBreakerService(),
            mapperService);

    QueryShardContext queryShardContext =
        new QueryShardContext(
            0,
            indexSettings,
            bigArrays,
            null,
            //        null,
            indexFieldDataService::getForField,
            //        (mappedFieldType, s, searchLookupSupplier) -> {
            //          return mappedFieldType.
            //
            //          return new SortedNumericIndexFieldData(mappedFieldType.name(),
            // IndexNumericFieldData.NumericType.LONG);
            //        },
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
            registry);

    SearchContext searchContext =
        new SearchContext() {
          @Override
          public void setTask(SearchShardTask task) {}

          @Override
          public SearchShardTask getTask() {
            return null;
          }

          @Override
          public boolean isCancelled() {
            return false;
          }

          @Override
          protected void doClose() {}

          @Override
          public void preProcess(boolean rewrite) {}

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
            return null;
          }

          @Override
          public SearchContext aggregations(SearchContextAggregations aggregations) {
            return null;
          }

          @Override
          public void addSearchExt(SearchExtBuilder searchExtBuilder) {}

          @Override
          public SearchExtBuilder getSearchExt(String name) {
            return null;
          }

          @Override
          public SearchHighlightContext highlight() {
            return null;
          }

          @Override
          public void highlight(SearchHighlightContext highlight) {}

          @Override
          public SuggestionSearchContext suggest() {
            return null;
          }

          @Override
          public void suggest(SuggestionSearchContext suggest) {}

          @Override
          public List<RescoreContext> rescore() {
            return null;
          }

          @Override
          public void addRescore(RescoreContext rescore) {}

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
          public void timeout(TimeValue timeout) {}

          @Override
          public int terminateAfter() {
            return 0;
          }

          @Override
          public void terminateAfter(int terminateAfter) {}

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
          public void explain(boolean explain) {}

          @Override
          public List<String> groupStats() {
            return null;
          }

          @Override
          public void groupStats(List<String> groupStats) {}

          @Override
          public boolean version() {
            return false;
          }

          @Override
          public void version(boolean version) {}

          @Override
          public boolean seqNoAndPrimaryTerm() {
            return false;
          }

          @Override
          public void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {}

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
          public SearchContext docIdsToLoad(
              int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
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
          public Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>>
              queryCollectorManagers() {
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

    //    AvgAggregationBuilder avgAggregationBuilder = new
    // AvgAggregationBuilder("foo").field("doubleproperty");
    ValueCountAggregationBuilder valueCountAggregationBuilder =
        new ValueCountAggregationBuilder("baz")
            .field(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

    //    AutoDateHistogramAggregationBuilder

    //    DateHistogramAggregationBuilder dateHistogramAggregationBuilder = new
    // DateHistogramAggregationBuilder("bar")
    ////        .fixedInterval(DateHistogramInterval.MINUTE)
    //        .fixedInterval(DateHistogramInterval.MINUTE)
    //        .hardBounds(new LongBounds(from, to))
    //        .field(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

    AutoDateHistogramAggregationBuilder autoDateHistogramAggregationBuilder =
        new AutoDateHistogramAggregationBuilder("bar")
            .setNumBuckets(numBuckets)
            .setMinimumIntervalExpression("minute")
            .field(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

    //    dateHistogramAggregationBuilder.subAggregation(avgAggregationBuilder);
    autoDateHistogramAggregationBuilder.subAggregation(valueCountAggregationBuilder);
    Aggregator aggregator =
        autoDateHistogramAggregationBuilder
            .build(queryShardContext, null)
            .create(searchContext, null, CardinalityUpperBound.ONE);

    //    ValuesSourceConfig valuesSourceConfig = ValuesSourceConfig.resolve(
    //        queryShardContext,
    //        null,
    //        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
    //        null,
    //        null,
    //        null,
    //        null,
    //        new SortedNumericIndexFieldData(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
    // IndexNumericFieldData.NumericType.LONG).getValuesSourceType()
    //    );

    //    DateHistogramAggregatorFactory factory = new DateHistogramAggregatorFactory(
    //        "name",
    //        valuesSourceConfig,
    //        BucketOrder.key(true),
    //        true,
    //        10
    //        , Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build(),
    //        null,
    //        null,
    //        queryShardContext,
    //        null,
    //        new AggregatorFactories.Builder(),
    //        Map.of("foo", "bar")
    //    );

    //    Aggregator aggregator = factory.create(searchContext, null, CardinalityUpperBound.ONE);

    return aggregator;
  }
}
