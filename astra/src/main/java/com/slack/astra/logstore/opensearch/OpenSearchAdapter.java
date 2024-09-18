package com.slack.astra.logstore.opensearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.slack.astra.logstore.search.aggregations.AggBuilder;
import com.slack.astra.logstore.search.aggregations.AggBuilderBase;
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
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.IndexFieldDataService;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketHelpers;
import org.opensearch.search.aggregations.pipeline.CumulativeSumPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.EwmaModel;
import org.opensearch.search.aggregations.pipeline.HoltLinearModel;
import org.opensearch.search.aggregations.pipeline.HoltWintersModel;
import org.opensearch.search.aggregations.pipeline.LinearModel;
import org.opensearch.search.aggregations.pipeline.MovAvgModel;
import org.opensearch.search.aggregations.pipeline.MovAvgPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MovFnPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.aggregations.pipeline.SimpleModel;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.internal.SearchContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to allow using OpenSearch aggregations and query parsing from within Astra. This
 * class should ultimately act as an adapter where OpenSearch code is not needed external to this
 * class. <br>
 * TODO - implement a custom InternalAggregation and return these instead of the OpenSearch
 * InternalAggregation classes
 */
public class OpenSearchAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchAdapter.class);

  private static final IndexSettings indexSettings = AstraIndexSettings.getInstance();
  private static final SimilarityService similarityService = AstraSimilarityService.getInstance();

  private final MapperService mapperService;

  private final Map<String, LuceneFieldDef> chunkSchema;

  public OpenSearchAdapter(Map<String, LuceneFieldDef> chunkSchema) {
    this.mapperService = buildMapperService();
    this.chunkSchema = chunkSchema;
  }

  /**
   * Builds a Lucene query using the provided arguments, and the currently loaded schema. Uses
   * Opensearch QueryBuilder's. TODO - use the dataset param in building query
   *
   * @see <a href="https://opensearch.org/docs/latest/query-dsl/full-text/query-string/">Query
   *     parsing OpenSearch docs</a>
   * @see <a
   *     href="https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html">Query
   *     parsing ES docs</a>
   */
  public Query buildQuery(IndexSearcher indexSearcher, QueryBuilder queryBuilder)
      throws IOException {
    QueryShardContext queryShardContext =
        buildQueryShardContext(AstraBigArrays.getInstance(), indexSearcher, mapperService);

    if (queryBuilder != null) {
      try {
        return queryBuilder.rewrite(queryShardContext).toQuery(queryShardContext);
      } catch (Exception e) {
        LOG.error("Query parse exception", e);
        throw new IllegalArgumentException(e);
      }
    }
    // TODO: Should this return null? Raise an error? Just return everything?
    return new BoolQueryBuilder().rewrite(queryShardContext).toQuery(queryShardContext);
  }

  /**
   * For each defined field in the chunk schema, this will check if the field is already registered,
   * and if not attempt to register it with the mapper service.
   *
   * @see this.loadSchema()
   */
  public void reloadSchema() {
    // TreeMap here ensures the schema is sorted by natural order - to ensure multifields are
    // registered by their parent first, and then fields added second
    for (Map.Entry<String, LuceneFieldDef> entry : new TreeMap<>(chunkSchema).entrySet()) {
      String fieldMapping = getFieldMapping(entry.getValue().fieldType);
      try {
        if (fieldMapping != null) {
          tryRegisterField(
              mapperService, entry.getValue().name, b -> b.field("type", fieldMapping));
        }
      } catch (Exception e) {
        LOG.error("Error parsing schema mapping for {}", entry.getValue().toString(), e);
      }
    }
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

  protected static XContentBuilder fieldMappingWithFields(
      String parentType,
      String parentName,
      String childName,
      CheckedConsumer<XContentBuilder, IOException> buildField)
      throws IOException {

    return mapping(
        b -> {
          b.startObject(parentName);
          b.field("type", parentType);
          b.startObject("fields");
          b.startObject(childName);
          buildField.accept(b);
          b.endObject();
          b.endObject();
          b.endObject();
        });
  }

  /** Builds a CollectorManager for use in the Lucene aggregation step */
  public CollectorManager<Aggregator, InternalAggregation> getCollectorManager(
      AggBuilder aggBuilder, IndexSearcher indexSearcher, Query query) {
    return new CollectorManager<>() {
      @Override
      public Aggregator newCollector() throws IOException {
        Aggregator aggregator = buildAggregatorUsingContext(aggBuilder, indexSearcher, query);
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

        if (internalAggregationList.isEmpty()) {
          return null;
        } else {
          // Using the first element on the list as the basis for the reduce method is per
          // OpenSearch recommendations: "For best efficiency, when implementing, try
          // reusing an existing instance (typically the first in the given list) to save
          // on redundant object construction."
          return internalAggregationList
              .getFirst()
              .reduce(
                  internalAggregationList,
                  InternalAggregation.ReduceContext.forPartialReduction(
                      AstraBigArrays.getInstance(),
                      null,
                      () -> PipelineAggregator.PipelineTree.EMPTY));
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

    AutoDateHistogramAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    DateHistogramAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    HistogramAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    TermsAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    AvgAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    SumAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    MinAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    MaxAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    CardinalityAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    ExtendedStatsAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    PercentilesAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    ValueCountAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);

    // Filters are registered in a non-standard way
    valuesSourceRegistryBuilder.registerUsage(FiltersAggregationBuilder.NAME);

    return valuesSourceRegistryBuilder.build();
  }

  /**
   * Builds a MapperService using the minimal amount of settings required for Aggregations. After
   * initializing the mapper service, individual fields will still need to be added using
   * this.registerField()
   */
  private static MapperService buildMapperService() {
    return new MapperService(
        OpenSearchAdapter.indexSettings,
        AstraIndexAnalyzer.getInstance(),
        new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
        OpenSearchAdapter.similarityService,
        AstraMapperRegistry.buildNewInstance(),
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
      BigArrays bigArrays, IndexSearcher indexSearcher, MapperService mapperService) {
    final ValuesSourceRegistry valuesSourceRegistry = buildValueSourceRegistry();
    return new QueryShardContext(
        0,
        OpenSearchAdapter.indexSettings,
        bigArrays,
        null,
        new IndexFieldDataService(
                OpenSearchAdapter.indexSettings,
                new IndicesFieldDataCache(
                    OpenSearchAdapter.indexSettings.getSettings(),
                    new IndexFieldDataCache.Listener() {}),
                new NoneCircuitBreakerService(),
                mapperService)
            ::getForField,
        mapperService,
        OpenSearchAdapter.similarityService,
        ScriptServiceProvider.getInstance(),
        null,
        null,
        null,
        indexSearcher,
        () -> Instant.now().toEpochMilli(),
        null,
        s -> false,
        () -> true,
        valuesSourceRegistry);
  }

  /**
   * Performs an initial load of the schema into the mapper service. This differs from the
   * reloadSchema in that this first builds an entire mapping and then performs a single merge into
   * the mapper service, instead of individually attempting to merge each field as it is
   * encountered.
   */
  public void loadSchema() {
    ObjectNode rootNode = new ObjectNode(JsonNodeFactory.instance);

    for (Map.Entry<String, LuceneFieldDef> entry : new TreeMap<>(chunkSchema).entrySet()) {
      String fieldName = entry.getValue().name;
      if (mapperService.isMetadataField(fieldName)) {
        LOG.trace("Skipping metadata field '{}'", fieldName);
      } else {
        ObjectNode child = new ObjectNode(JsonNodeFactory.instance);
        child.put("type", getFieldMapping(entry.getValue().fieldType));
        putAtPath(rootNode, child, entry.getKey());
      }
    }

    try {
      XContentBuilder builder =
          XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
      rootNode.fields().forEachRemaining((entry) -> buildObject(builder, entry));
      builder.endObject().endObject().endObject();

      mapperService.merge(
          MapperService.SINGLE_MAPPING_NAME,
          new CompressedXContent(BytesReference.bytes(builder)),
          MapperService.MergeReason.MAPPING_UPDATE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Given a root object node and a new object node, will insert the new node into the provided
   * dot-separated path. If a deeply nested path is provided, this will initialize the entire tree
   * as needed.
   *
   * @param root ObjectNode root node
   * @param newNode ObjectNode to insert
   * @param path Dot separated path (foo.bar)
   */
  private void putAtPath(ObjectNode root, ObjectNode newNode, String path) {
    String[] fieldParts = path.split("\\.");

    ObjectNode currentRef = root;
    for (int i = 0; i < fieldParts.length; i++) {
      if (!currentRef.has(fieldParts[i])) {
        // it doesn't have the current object
        if (i == fieldParts.length - 1) {
          // last thing, add ours
          currentRef.set(fieldParts[i], newNode);
          return;
        } else {
          // add a parent holder
          ObjectNode fields = new ObjectNode(JsonNodeFactory.instance);
          fields.set("fields", new ObjectNode(JsonNodeFactory.instance));
          currentRef.set(fieldParts[i], fields);
          currentRef = (ObjectNode) currentRef.get(fieldParts[i]).get("fields");
        }

      } else {
        // it has the parent
        if (i == fieldParts.length - 1) {
          currentRef.setAll(newNode);
          return;
        } else {
          // it has the parent, does it have a fields?
          if (!currentRef.get(fieldParts[i]).has("fields")) {
            ((ObjectNode) currentRef.get(fieldParts[i]))
                .set("fields", new ObjectNode(JsonNodeFactory.instance));
          }
          currentRef = (ObjectNode) currentRef.get(fieldParts[i]).get("fields");
        }
      }
    }
  }

  /**
   * Builds the resulting XContent from the provided map entries for use in the mapper service. For
   * entries that do not have explicit field definitions (ie, foo.bar exists as a field while foo
   * does not), will default to a binary type.
   */
  private void buildObject(XContentBuilder builder, Map.Entry<String, JsonNode> entry) {
    try {
      builder.startObject(entry.getKey());

      // does it have a type field set?
      if (entry.getValue().has("type")) {
        builder.field("type", entry.getValue().get("type").asText());
      } else {
        // todo - "default" when no field exists
        builder.field("type", "binary");
      }

      // does it have fields set?
      if (entry.getValue().has("fields")) {
        builder.startObject("fields");
        entry
            .getValue()
            .get("fields")
            .fields()
            .forEachRemaining(
                (fieldEntry) -> {
                  // do this same step all-over
                  buildObject(builder, fieldEntry);
                });
        builder.endObject();
      }

      builder.endObject();
    } catch (Exception e) {
      LOG.error("Error building object", e);
    }
  }

  /**
   * Returns the corresponding OpenSearch field type as a string, given the Astra FieldType
   * definition. todo - this probably should be moved into the respective FieldType enums directly
   */
  private String getFieldMapping(FieldType fieldType) {
    if (fieldType == FieldType.TEXT) {
      return "text";
    } else if (fieldType == FieldType.STRING
        || fieldType == FieldType.KEYWORD
        || fieldType == FieldType.ID) {
      return "keyword";
    } else if (fieldType == FieldType.IP) {
      return "ip";
    } else if (fieldType == FieldType.DATE) {
      return "date";
    } else if (fieldType == FieldType.BOOLEAN) {
      return "boolean";
    } else if (fieldType == FieldType.DOUBLE) {
      return "double";
    } else if (fieldType == FieldType.FLOAT) {
      return "float";
    } else if (fieldType == FieldType.HALF_FLOAT) {
      return "half_float";
    } else if (fieldType == FieldType.INTEGER) {
      return "integer";
    } else if (fieldType == FieldType.LONG) {
      return "long";
    } else if (fieldType == FieldType.SCALED_LONG) {
      return "scaled_long";
    } else if (fieldType == FieldType.SHORT) {
      return "short";
    } else if (fieldType == FieldType.BYTE) {
      return "byte";
    } else if (fieldType == FieldType.BINARY) {
      return "binary";
    } else {
      LOG.warn("Field type '{}' is not yet currently supported", fieldType);
      return null;
    }
  }

  /**
   * Registers a field type and name to the MapperService for use in aggregations. This informs the
   * aggregators how to access a specific field and what value type it contains. registerField(
   * mapperService, LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, b -> b.field("type",
   * "long"));
   */
  private static boolean tryRegisterField(
      MapperService mapperService,
      String fieldName,
      CheckedConsumer<XContentBuilder, IOException> buildField) {
    MappedFieldType fieldType = mapperService.fieldType(fieldName);
    if (mapperService.isMetadataField(fieldName)) {
      LOG.trace("Skipping metadata field '{}'", fieldName);
      return false;
    } else if (fieldType != null) {
      LOG.trace(
          "Field '{}' already exists as typeName '{}', skipping query mapping update",
          fieldType.name(),
          fieldType.familyTypeName());
      return false;
    } else {
      try {
        XContentBuilder mapping;
        if (fieldName.contains(".")) {
          String[] fieldParts = fieldName.split("\\.");
          String parentFieldName =
              String.join(".", Arrays.copyOfRange(fieldParts, 0, fieldParts.length - 1));
          String localFieldName = fieldParts[fieldParts.length - 1];
          MappedFieldType parent = mapperService.fieldType(parentFieldName);

          if (parent == null) {
            // if we don't have a parent, register this dot separated name as its own thing
            // this will cause future registrations to fail if someone comes along with a parent
            // since you can't merge those items

            // todo - consider making a decision about the parent here
            // todo - if this happens can I declare bankruptcy and start over on the mappings?
            mapping = fieldMapping(fieldName, buildField);
          } else {
            mapping =
                fieldMappingWithFields(
                    parent.typeName(), parentFieldName, localFieldName, buildField);
          }
        } else {
          mapping = fieldMapping(fieldName, buildField);
        }
        mapperService.merge(
            "_doc",
            new CompressedXContent(BytesReference.bytes(mapping)),
            MapperService.MergeReason.MAPPING_UPDATE);
      } catch (Exception e) {
        LOG.error("Error doing map update errorMsg={}", e.getMessage());
      }
      return true;
    }
  }

  /**
   * Given an aggBuilder, will use the previously initialized queryShardContext and searchContext to
   * return an OpenSearch aggregator / Lucene Collector
   */
  public Aggregator buildAggregatorUsingContext(
      AggBuilder builder, IndexSearcher indexSearcher, Query query) throws IOException {
    QueryShardContext queryShardContext =
        buildQueryShardContext(AstraBigArrays.getInstance(), indexSearcher, mapperService);
    SearchContext searchContext =
        new AstraSearchContext(
            AstraBigArrays.getInstance(), queryShardContext, indexSearcher, query);

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
  public static AbstractAggregationBuilder getAggregationBuilder(AggBuilder aggBuilder) {
    if (aggBuilder.getType().equals(DateHistogramAggBuilder.TYPE)) {
      return getDateHistogramAggregationBuilder((DateHistogramAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(AutoDateHistogramAggBuilder.TYPE)) {
      return getAutoDateHistogramAggregationBuilder((AutoDateHistogramAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(HistogramAggBuilder.TYPE)) {
      return getHistogramAggregationBuilder((HistogramAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(FiltersAggBuilder.TYPE)) {
      return getFiltersAggregationBuilder((FiltersAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(TermsAggBuilder.TYPE)) {
      return getTermsAggregationBuilder((TermsAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(SumAggBuilder.TYPE)) {
      return getSumAggregationBuilder((SumAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(AvgAggBuilder.TYPE)) {
      return getAvgAggregationBuilder((AvgAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(MinAggBuilder.TYPE)) {
      return getMinAggregationBuilder((MinAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(MaxAggBuilder.TYPE)) {
      return getMaxAggregationBuilder((MaxAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(PercentilesAggBuilder.TYPE)) {
      return getPercentilesAggregationBuilder((PercentilesAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(UniqueCountAggBuilder.TYPE)) {
      return getUniqueCountAggregationBuilder((UniqueCountAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(ExtendedStatsAggBuilder.TYPE)) {
      return getExtendedStatsAggregationBuilder((ExtendedStatsAggBuilder) aggBuilder);
    } else {
      throw new IllegalArgumentException(
          String.format("Aggregation type %s not yet supported", aggBuilder.getType()));
    }
  }

  /**
   * Given an AggBuilder, will invoke the appropriate pipeline aggregation builder method to return
   * the abstract pipeline aggregation builder. This method is expected to be invoked from within
   * the bucket aggregation builders to compose a nested aggregation tree.@return
   */
  protected static AbstractPipelineAggregationBuilder<?> getPipelineAggregationBuilder(
      AggBuilder aggBuilder) {
    if (aggBuilder.getType().equals(MovingAvgAggBuilder.TYPE)) {
      return getMovingAverageAggregationBuilder((MovingAvgAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(CumulativeSumAggBuilder.TYPE)) {
      return getCumulativeSumAggregationBuilder((CumulativeSumAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(DerivativeAggBuilder.TYPE)) {
      return getDerivativeAggregationBuilder((DerivativeAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(MovingFunctionAggBuilder.TYPE)) {
      return getMovingFunctionAggregationBuilder((MovingFunctionAggBuilder) aggBuilder);
    } else {
      throw new IllegalArgumentException(
          String.format("PipelineAggregation type %s not yet supported", aggBuilder.getType()));
    }
  }

  /**
   * Determines if a given aggregation is of pipeline type, to allow for calling the appropriate
   * subAggregation builder step
   */
  protected static boolean isPipelineAggregation(AggBuilder aggBuilder) {
    List<String> pipelineAggregators =
        List.of(
            MovingAvgAggBuilder.TYPE,
            DerivativeAggBuilder.TYPE,
            CumulativeSumAggBuilder.TYPE,
            MovingFunctionAggBuilder.TYPE);
    return pipelineAggregators.contains(aggBuilder.getType());
  }

  /**
   * Given an SumAggBuilder returns a SumAggregationBuilder to be used in building aggregation tree
   */
  protected static SumAggregationBuilder getSumAggregationBuilder(SumAggBuilder builder) {
    SumAggregationBuilder sumAggregationBuilder =
        new SumAggregationBuilder(builder.getName()).field(builder.getField());

    if (builder.getScript() != null && !builder.getScript().isEmpty()) {
      sumAggregationBuilder.script(new Script(builder.getScript()));
    }

    if (builder.getMissing() != null) {
      sumAggregationBuilder.missing(builder.getMissing());
    }

    return sumAggregationBuilder;
  }

  /**
   * Given an AvgAggBuilder returns a AvgAggregationBuilder to be used in building aggregation tree
   */
  protected static AvgAggregationBuilder getAvgAggregationBuilder(AvgAggBuilder builder) {
    AvgAggregationBuilder avgAggregationBuilder =
        new AvgAggregationBuilder(builder.getName()).field(builder.getField());

    if (builder.getScript() != null && !builder.getScript().isEmpty()) {
      avgAggregationBuilder.script(new Script(builder.getScript()));
    }

    if (builder.getMissing() != null) {
      avgAggregationBuilder.missing(builder.getMissing());
    }

    return avgAggregationBuilder;
  }

  /**
   * Given an MinAggBuilder returns a MinAggregationBuilder to be used in building aggregation tree
   */
  protected static MinAggregationBuilder getMinAggregationBuilder(MinAggBuilder builder) {
    MinAggregationBuilder minAggregationBuilder =
        new MinAggregationBuilder(builder.getName()).field(builder.getField());

    if (builder.getScript() != null && !builder.getScript().isEmpty()) {
      minAggregationBuilder.script(new Script(builder.getScript()));
    }

    if (builder.getMissing() != null) {
      minAggregationBuilder.missing(builder.getMissing());
    }

    return minAggregationBuilder;
  }

  /**
   * Given an MaxAggBuilder returns a MaxAggregationBuilder to be used in building aggregation tree
   */
  protected static MaxAggregationBuilder getMaxAggregationBuilder(MaxAggBuilder builder) {
    MaxAggregationBuilder maxAggregationBuilder =
        new MaxAggregationBuilder(builder.getName()).field(builder.getField());

    if (builder.getScript() != null && !builder.getScript().isEmpty()) {
      maxAggregationBuilder.script(new Script(builder.getScript()));
    }

    if (builder.getMissing() != null) {
      maxAggregationBuilder.missing(builder.getMissing());
    }

    return maxAggregationBuilder;
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
   * Given a ExtendedStatsAggBuilder, returns a ExtendedStatsAggregationBuilder to be used in
   * building aggregation tree. This returns all possible extended stats and it is up to the
   * requestor to filter to the desired fields
   */
  protected static ExtendedStatsAggregationBuilder getExtendedStatsAggregationBuilder(
      ExtendedStatsAggBuilder builder) {
    ExtendedStatsAggregationBuilder extendedStatsAggregationBuilder =
        new ExtendedStatsAggregationBuilder(builder.getName()).field(builder.getField());

    if (builder.getSigma() != null) {
      extendedStatsAggregationBuilder.sigma(builder.getSigma());
    }

    if (builder.getScript() != null && !builder.getScript().isEmpty()) {
      extendedStatsAggregationBuilder.script(new Script(builder.getScript()));
    }

    if (builder.getMissing() != null) {
      extendedStatsAggregationBuilder.missing(builder.getMissing());
    }

    return extendedStatsAggregationBuilder;
  }

  /**
   * Given a PercentilesAggBuilder, returns a PercentilesAggregationBuilder to be used in building
   * aggregation tree
   */
  protected static PercentilesAggregationBuilder getPercentilesAggregationBuilder(
      PercentilesAggBuilder builder) {
    PercentilesAggregationBuilder percentilesAggregationBuilder =
        new PercentilesAggregationBuilder(builder.getName())
            .field(builder.getField())
            .percentiles(builder.getPercentilesArray());

    if (builder.getScript() != null && !builder.getScript().isEmpty()) {
      percentilesAggregationBuilder.script(new Script(builder.getScript()));
    }

    if (builder.getMissing() != null) {
      percentilesAggregationBuilder.missing(builder.getMissing());
    }

    return percentilesAggregationBuilder;
  }

  /**
   * Given a MovingAvgAggBuilder, returns a MovAvgAggPipelineAggregation to be used in building the
   * aggregation tree.
   */
  protected static MovAvgPipelineAggregationBuilder getMovingAverageAggregationBuilder(
      MovingAvgAggBuilder builder) {
    MovAvgPipelineAggregationBuilder movAvgPipelineAggregationBuilder =
        new MovAvgPipelineAggregationBuilder(builder.getName(), builder.getBucketsPath());

    if (builder.getWindow() != null) {
      movAvgPipelineAggregationBuilder.window(builder.getWindow());
    }

    if (builder.getPredict() != null) {
      movAvgPipelineAggregationBuilder.predict(builder.getPredict());
    }

    movAvgPipelineAggregationBuilder.gapPolicy(BucketHelpers.GapPolicy.SKIP);

    //noinspection IfCanBeSwitch
    if (builder.getModel().equals("simple")) {
      movAvgPipelineAggregationBuilder.model(new SimpleModel());
    } else if (builder.getModel().equals("linear")) {
      movAvgPipelineAggregationBuilder.model(new LinearModel());
    } else if (builder.getModel().equals("ewma")) {
      MovAvgModel model = new EwmaModel();
      if (builder.getAlpha() != null) {
        model = new EwmaModel(builder.getAlpha());
      }
      movAvgPipelineAggregationBuilder.model(model);
      movAvgPipelineAggregationBuilder.minimize(builder.isMinimize());
    } else if (builder.getModel().equals("holt")) {
      MovAvgModel model = new HoltLinearModel();
      if (ObjectUtils.allNotNull(builder.getAlpha(), builder.getBeta())) {
        // both are non-null, use values provided instead of default
        model = new HoltLinearModel(builder.getAlpha(), builder.getBeta());
      } else if (ObjectUtils.anyNotNull(builder.getAlpha(), builder.getBeta())) {
        throw new IllegalArgumentException(
            String.format(
                "Both alpha and beta must be provided for HoltLinearMovingAvg if not using the default values [alpha:%s, beta:%s]",
                builder.getAlpha(), builder.getBeta()));
      }
      movAvgPipelineAggregationBuilder.model(model);
      movAvgPipelineAggregationBuilder.minimize(builder.isMinimize());
    } else if (builder.getModel().equals("holt_winters")) {
      // default as listed in the HoltWintersModel.java class
      // todo - this cannot be currently configured via Grafana, but may need to be an option?
      HoltWintersModel.SeasonalityType defaultSeasonalityType =
          HoltWintersModel.SeasonalityType.ADDITIVE;
      MovAvgModel model = new HoltWintersModel();
      if (ObjectUtils.allNotNull(
          builder.getAlpha(), builder.getBeta(), builder.getGamma(), builder.getPeriod())) {
        model =
            new HoltWintersModel(
                builder.getAlpha(),
                builder.getBeta(),
                builder.getGamma(),
                builder.getPeriod(),
                defaultSeasonalityType,
                builder.isPad());
      } else if (ObjectUtils.anyNotNull()) {
        throw new IllegalArgumentException(
            String.format(
                "Alpha, beta, gamma, period, and pad must be provided for HoltWintersMovingAvg if not using the default values [alpha:%s, beta:%s, gamma:%s, period:%s, pad:%s]",
                builder.getAlpha(),
                builder.getBeta(),
                builder.getGamma(),
                builder.getPeriod(),
                builder.isPad()));
      }
      movAvgPipelineAggregationBuilder.model(model);
      movAvgPipelineAggregationBuilder.minimize(builder.isMinimize());
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Model type of '%s' is not valid moving average model, must be one of ['simple', 'linear', 'ewma', 'holt', holt_winters']",
              builder.getModel()));
    }

    return movAvgPipelineAggregationBuilder;
  }

  /**
   * Given an MovingFunctionAggBuilder returns a MovFnPipelineAggregationBuilder to be used in
   * building aggregation tree
   */
  protected static MovFnPipelineAggregationBuilder getMovingFunctionAggregationBuilder(
      MovingFunctionAggBuilder builder) {
    MovFnPipelineAggregationBuilder movFnPipelineAggregationBuilder =
        new MovFnPipelineAggregationBuilder(
            builder.getName(),
            builder.getBucketsPath(),
            new Script(builder.getScript()),
            builder.getWindow());

    if (builder.getShift() != null) {
      movFnPipelineAggregationBuilder.setShift(builder.getShift());
    }

    return movFnPipelineAggregationBuilder;
  }

  /**
   * Given an CumulativeSumAggBuilder returns a CumulativeSumPipelineAggregationBuilder to be used
   * in building aggregation tree
   */
  protected static CumulativeSumPipelineAggregationBuilder getCumulativeSumAggregationBuilder(
      CumulativeSumAggBuilder builder) {
    CumulativeSumPipelineAggregationBuilder cumulativeSumPipelineAggregationBuilder =
        new CumulativeSumPipelineAggregationBuilder(builder.getName(), builder.getBucketsPath());

    if (builder.getFormat() != null && !builder.getFormat().isEmpty()) {
      cumulativeSumPipelineAggregationBuilder.format(builder.getFormat());
    }

    return cumulativeSumPipelineAggregationBuilder;
  }

  /**
   * Given a DerivativeAggBuilder returns a DerivativePipelineAggregationBuilder to be used in
   * building aggregation tree
   */
  protected static DerivativePipelineAggregationBuilder getDerivativeAggregationBuilder(
      DerivativeAggBuilder builder) {
    DerivativePipelineAggregationBuilder derivativePipelineAggregationBuilder =
        new DerivativePipelineAggregationBuilder(builder.getName(), builder.getBucketsPath());

    if (builder.getUnit() != null && !builder.getUnit().isEmpty()) {
      derivativePipelineAggregationBuilder.unit(builder.getUnit());
    }

    return derivativePipelineAggregationBuilder;
  }

  /**
   * Given an TermsAggBuilder returns a TermsAggregationBuilder to be used in building aggregation
   * tree
   */
  protected static TermsAggregationBuilder getTermsAggregationBuilder(TermsAggBuilder builder) {
    List<String> subAggNames =
        builder.getSubAggregations().stream()
            .map(subagg -> ((AggBuilderBase) subagg).getName())
            .collect(Collectors.toList());

    List<BucketOrder> order =
        builder.getOrder().entrySet().stream()
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
      if (isPipelineAggregation(subAggregation)) {
        termsAggregationBuilder.subAggregation(getPipelineAggregationBuilder(subAggregation));
      } else {
        termsAggregationBuilder.subAggregation(getAggregationBuilder(subAggregation));
      }
    }

    return termsAggregationBuilder;
  }

  /**
   * Given an FiltersAggBuilder returns a FiltersAggregationBuilder to be used in building
   * aggregation tree. Note this is different from a filter aggregation (single) as this takes a map
   * of keyed filters, but can be invoked with a single entry in the map.
   */
  protected static FiltersAggregationBuilder getFiltersAggregationBuilder(
      FiltersAggBuilder builder) {
    List<FiltersAggregator.KeyedFilter> keyedFilterList = new ArrayList<>();
    for (Map.Entry<String, FiltersAggBuilder.FilterAgg> stringFilterAggEntry :
        builder.getFilterAggMap().entrySet()) {
      FiltersAggBuilder.FilterAgg filterAgg = stringFilterAggEntry.getValue();
      keyedFilterList.add(
          new FiltersAggregator.KeyedFilter(
              stringFilterAggEntry.getKey(),
              new QueryStringQueryBuilder(filterAgg.getQueryString())
                  .lenient(true)
                  .analyzeWildcard(filterAgg.isAnalyzeWildcard())));
    }

    FiltersAggregationBuilder filtersAggregationBuilder =
        new FiltersAggregationBuilder(
            builder.getName(), keyedFilterList.toArray(new FiltersAggregator.KeyedFilter[0]));

    for (AggBuilder subAggregation : builder.getSubAggregations()) {
      if (isPipelineAggregation(subAggregation)) {
        filtersAggregationBuilder.subAggregation(getPipelineAggregationBuilder(subAggregation));
      } else {
        filtersAggregationBuilder.subAggregation(getAggregationBuilder(subAggregation));
      }
    }

    return filtersAggregationBuilder;
  }

  /**
   * Given an AutoDateHistogramAggBuilder returns a AutoDateHistogramAggBuilder to be used in
   * building aggregation tree
   */
  protected static AutoDateHistogramAggregationBuilder getAutoDateHistogramAggregationBuilder(
      AutoDateHistogramAggBuilder builder) {
    AutoDateHistogramAggregationBuilder autoDateHistogramAggregationBuilder =
        new AutoDateHistogramAggregationBuilder(builder.getName()).field(builder.getField());

    if (builder.getMinInterval() != null && !builder.getMinInterval().isEmpty()) {
      autoDateHistogramAggregationBuilder.setMinimumIntervalExpression(builder.getMinInterval());
    }

    if (builder.getNumBuckets() != null && builder.getNumBuckets() > 0) {
      autoDateHistogramAggregationBuilder.setNumBuckets(builder.getNumBuckets());
    }

    for (AggBuilder subAggregation : builder.getSubAggregations()) {
      if (isPipelineAggregation(subAggregation)) {
        autoDateHistogramAggregationBuilder.subAggregation(
            getPipelineAggregationBuilder(subAggregation));
      } else {
        autoDateHistogramAggregationBuilder.subAggregation(getAggregationBuilder(subAggregation));
      }
    }

    return autoDateHistogramAggregationBuilder;
  }

  /**
   * Given an DateHistogramAggBuilder returns a DateHistogramAggregationBuilder to be used in
   * building aggregation tree
   */
  protected static DateHistogramAggregationBuilder getDateHistogramAggregationBuilder(
      DateHistogramAggBuilder builder) {

    DateHistogramAggregationBuilder dateHistogramAggregationBuilder =
        new DateHistogramAggregationBuilder(builder.getName())
            .field(builder.getField())
            .minDocCount(builder.getMinDocCount())
            .fixedInterval(new DateHistogramInterval(builder.getInterval()));

    if (builder.getOffset() != null && !builder.getOffset().isEmpty()) {
      dateHistogramAggregationBuilder.offset(builder.getOffset());
    }

    if (builder.getFormat() != null && !builder.getFormat().isEmpty()) {
      // todo - this should be used when the field type is changed to date
      // dateHistogramAggregationBuilder.format(builder.getFormat());
    }

    if (builder.getZoneId() != null && !builder.getZoneId().isEmpty()) {
      dateHistogramAggregationBuilder.timeZone(ZoneId.of(builder.getZoneId()));
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
      if (isPipelineAggregation(subAggregation)) {
        dateHistogramAggregationBuilder.subAggregation(
            getPipelineAggregationBuilder(subAggregation));
      } else {
        dateHistogramAggregationBuilder.subAggregation(getAggregationBuilder(subAggregation));
      }
    }

    return dateHistogramAggregationBuilder;
  }

  /**
   * Given an HistogramAggBuilder returns a HistogramAggregationBuilder to be used in building
   * aggregation tree
   */
  protected static HistogramAggregationBuilder getHistogramAggregationBuilder(
      HistogramAggBuilder builder) {

    HistogramAggregationBuilder histogramAggregationBuilder =
        new HistogramAggregationBuilder(builder.getName())
            .field(builder.getField())
            .minDocCount(builder.getMinDocCount())
            .interval(builder.getIntervalDouble());

    for (AggBuilder subAggregation : builder.getSubAggregations()) {
      if (isPipelineAggregation(subAggregation)) {
        histogramAggregationBuilder.subAggregation(getPipelineAggregationBuilder(subAggregation));
      } else {
        histogramAggregationBuilder.subAggregation(getAggregationBuilder(subAggregation));
      }
    }

    return histogramAggregationBuilder;
  }

  public MapperService getMapperService() {
    return this.mapperService;
  }
}
