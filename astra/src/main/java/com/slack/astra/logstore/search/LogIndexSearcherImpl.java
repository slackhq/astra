package com.slack.astra.logstore.search;

import static com.slack.astra.util.ArgValidationUtils.ensureNonEmptyString;
import static com.slack.astra.util.ArgValidationUtils.ensureTrue;

import brave.ScopedSpan;
import brave.Tracing;
import com.google.common.base.Stopwatch;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogMessage.SystemField;
import com.slack.astra.logstore.LogWireMessage;
import com.slack.astra.logstore.opensearch.OpenSearchAdapter;
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.util.JsonUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A wrapper around lucene that helps us search a single index containing logs.
 * TODO: Add template type to this class definition.
 */
public class LogIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(LogIndexSearcherImpl.class);

  private final SearcherManager searcherManager;

  private final OpenSearchAdapter openSearchAdapter;

  private final ReferenceManager.RefreshListener refreshListener;

  private final AstraSearcherManager astraSearcherManager;

  public LogIndexSearcherImpl(
      AstraSearcherManager astraSearcherManager,
      ConcurrentHashMap<String, LuceneFieldDef> chunkSchema) {
    this(astraSearcherManager, chunkSchema, null);
  }

  public LogIndexSearcherImpl(
      AstraSearcherManager astraSearcherManager,
      ConcurrentHashMap<String, LuceneFieldDef> chunkSchema,
      AstraConfigs.LuceneConfig luceneConfig) {
    if (luceneConfig != null) {
      this.openSearchAdapter = new OpenSearchAdapter(chunkSchema, luceneConfig);
    } else {
      this.openSearchAdapter = new OpenSearchAdapter(chunkSchema);
    }
    this.refreshListener =
        new ReferenceManager.RefreshListener() {
          @Override
          public void beforeRefresh() {
            // no-op
          }

          @Override
          public void afterRefresh(boolean didRefresh) {
            openSearchAdapter.reloadSchema();
          }
        };
    this.astraSearcherManager = astraSearcherManager;
    this.searcherManager = astraSearcherManager.getLuceneSearcherManager();
    this.searcherManager.addListener(refreshListener);
    // initialize the adapter with whatever the default schema is

    try {
      openSearchAdapter.loadSchema();
    } catch (Exception e) {
      LOG.error("Failed to load schema due to error:", e);
      this.close();

      throw new RuntimeException(e);
    }
  }

  @Override
  public SearchResult<LogMessage> search(
      String dataset,
      int howMany,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder,
      List<SearchQuery.SortSpec> sortFields) {

    ensureNonEmptyString(dataset, "dataset should be a non-empty string");
    ensureTrue(howMany >= 0, "hits requested should not be negative.");
    ensureTrue(
        howMany > 0 || aggregatorFactoriesBuilder != null,
        "Hits or aggregation should be requested.");

    ScopedSpan span = Tracing.currentTracer().startScopedSpan("LogIndexSearcherImpl.search");
    span.tag("dataset", dataset);
    span.tag("howMany", String.valueOf(howMany));

    Stopwatch elapsedTime = Stopwatch.createStarted();
    try {
      // Acquire an index searcher from searcher manager.
      // This is a useful optimization for indexes that are static.
      IndexSearcher searcher = searcherManager.acquire();

      try {
        List<LogMessage> results;
        InternalAggregation internalAggregation = null;
        Query query = openSearchAdapter.buildQuery(searcher, queryBuilder);

        // Build sort from query sort specifications
        Sort sort = buildSort(sortFields);

        if (howMany > 0) {
          CollectorManager<TopFieldCollector, TopFieldDocs> topFieldCollector =
              buildTopFieldCollector(
                  sort, howMany, aggregatorFactoriesBuilder != null ? Integer.MAX_VALUE : howMany);
          MultiCollectorManager collectorManager;

          if (aggregatorFactoriesBuilder != null) {
            collectorManager =
                new MultiCollectorManager(
                    topFieldCollector,
                    openSearchAdapter.getCollectorManager(
                        aggregatorFactoriesBuilder, searcher, query));
          } else {
            collectorManager = new MultiCollectorManager(topFieldCollector);
          }
          Object[] collector = searcher.search(query, collectorManager);

          ScoreDoc[] hits = ((TopFieldDocs) collector[0]).scoreDocs;
          results = new ArrayList<>(hits.length);
          for (ScoreDoc hit : hits) {
            results.add(buildLogMessage(searcher, hit, sourceFieldFilter));
          }
          if (aggregatorFactoriesBuilder != null) {
            internalAggregation = (InternalAggregation) collector[1];
          }
        } else {
          results = Collections.emptyList();
          internalAggregation =
              searcher.search(
                  query,
                  openSearchAdapter.getCollectorManager(
                      aggregatorFactoriesBuilder, searcher, query));
        }

        elapsedTime.stop();
        return new SearchResult<>(
            results, elapsedTime.elapsed(TimeUnit.MICROSECONDS), 0, 0, 1, 1, internalAggregation);
      } finally {
        searcherManager.release(searcher);
      }
    } catch (IllegalArgumentException e) {
      // Preserve validation error messages (e.g., TEXT field sorting errors)
      span.error(e);
      throw e;
    } catch (IOException e) {
      span.error(e);
      throw new IllegalArgumentException("Failed to acquire an index searcher.", e);
    } finally {
      span.finish();
    }
  }

  private LogMessage buildLogMessage(
      IndexSearcher searcher, ScoreDoc hit, SourceFieldFilter sourceFieldFilter) {
    String s = "";
    try {
      s = searcher.doc(hit.doc).get(SystemField.SOURCE.fieldName);
      LogWireMessage wireMessage = JsonUtil.read(s, LogWireMessage.class);
      Map<String, Object> source = wireMessage.getSource();

      if (sourceFieldFilter != null
          && sourceFieldFilter.getFilterType() == SourceFieldFilter.FilterType.INCLUDE) {
        source =
            wireMessage.getSource().keySet().stream()
                .filter(sourceFieldFilter::appliesToField)
                .collect(Collectors.toMap((key) -> key, (key) -> wireMessage.getSource().get(key)));
      } else if (sourceFieldFilter != null
          && sourceFieldFilter.getFilterType() == SourceFieldFilter.FilterType.EXCLUDE) {
        source =
            wireMessage.getSource().keySet().stream()
                .filter((key) -> !sourceFieldFilter.appliesToField(key))
                .collect(Collectors.toMap((key) -> key, (key) -> wireMessage.getSource().get(key)));
      }

      return new LogMessage(
          wireMessage.getIndex(),
          wireMessage.getType(),
          wireMessage.getId(),
          wireMessage.getTimestamp(),
          source);
    } catch (Exception e) {
      throw new IllegalStateException("Error fetching and parsing a result from index: " + s, e);
    }
  }

  /**
   * Converts a type name (from FieldType enum or unmapped_type string) to Lucene's SortField.Type
   * for sorting. This handles both fields that exist in the schema (using their FieldType.name) and
   * unmapped fields (using the unmapped_type hint from the query).
   *
   * @param esType The type name (e.g., "long", "keyword", "date", "text")
   * @return Corresponding Lucene sort field type, or LONG if type is unknown
   */
  private static SortField.Type esTypeToLuceneSortType(String esType) {
    if (esType == null || esType.isEmpty()) {
      return SortField.Type.LONG; // Default to LONG (DATE semantics)
    }

    // Normalize type name (case-insensitive, trimmed)
    String normalizedType = esType.toLowerCase().trim();

    // Map type names to Lucene sort types
    return switch (normalizedType) {
      case "text", "string", "keyword", "id" -> SortField.Type.STRING;
      case "integer", "short", "byte" -> SortField.Type.INT;
      case "long", "date", "scaled_long", "scaledlong" -> SortField.Type.LONG;
      case "double" -> SortField.Type.DOUBLE;
      case "float", "half_float" -> SortField.Type.FLOAT;
      case "boolean" -> SortField.Type.INT; // Booleans stored as 0/1
      case "ip" -> SortField.Type.STRING; // IP addresses sorted as strings
      case "binary" -> SortField.Type.STRING; // Binary fields default to string sorting
      default -> {
        LOG.warn("Unknown type '{}', defaulting to LONG", esType);
        yield SortField.Type.LONG;
      }
    };
  }

  /**
   * Sets the appropriate missing value on a SortField based on its type and sort order. Missing
   * values are always placed at the end of results (bottom), regardless of sort direction.
   *
   * @param sortField The SortField to configure
   * @param luceneType The underlying Lucene type (used for CUSTOM types like
   *     SortedNumericSortField)
   * @param isDescending Whether the sort is descending
   */
  private static void setMissingValue(
      SortField sortField, SortField.Type luceneType, boolean isDescending) {
    SortField.Type type = sortField.getType();

    // For SortedNumericSortField (type=CUSTOM), use the original luceneType
    if (type == SortField.Type.CUSTOM) {
      type = luceneType;
    }

    // For descending sorts: missing values should be "less than" all real values
    // For ascending sorts: missing values should be "greater than" all real values
    // This ensures missing values always appear at the bottom of results
    switch (type) {
      case INT:
        sortField.setMissingValue(isDescending ? Integer.MIN_VALUE : Integer.MAX_VALUE);
        break;
      case LONG:
        sortField.setMissingValue(isDescending ? Long.MIN_VALUE : Long.MAX_VALUE);
        break;
      case FLOAT:
        sortField.setMissingValue(isDescending ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY);
        break;
      case DOUBLE:
        sortField.setMissingValue(
            isDescending ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY);
        break;
      case STRING:
        // For ascending: STRING_LAST places missing after all strings (A,B,C,missing)
        // For descending: STRING_FIRST places missing before all strings, then reversal puts them
        // at end (missing,C,B,A -> C,B,A,missing)
        sortField.setMissingValue(isDescending ? SortField.STRING_FIRST : SortField.STRING_LAST);
        break;
      default:
        // Other types don't support missing values or don't need special handling
        break;
    }
  }

  /**
   * Builds a Lucene Sort object from a list of SortSpec objects. For fields not in the schema,
   * treats them as DATE fields (effectively grouping unmapped documents together).
   *
   * @param sortSpecs List of sort specifications from the query
   * @return Lucene Sort object, or default timestamp descending sort if list is empty
   */
  private Sort buildSort(List<SearchQuery.SortSpec> sortSpecs) {
    // Default to timestamp descending if no sort fields provided
    if (sortSpecs == null || sortSpecs.isEmpty()) {
      SortField defaultSort =
          new SortField(SystemField.TIME_SINCE_EPOCH.fieldName, Type.LONG, true);
      return new Sort(defaultSort);
    }

    Map<String, LuceneFieldDef> schema = openSearchAdapter.getSchema();
    SortField[] sortFields = new SortField[sortSpecs.size()];

    for (int i = 0; i < sortSpecs.size(); i++) {
      SearchQuery.SortSpec spec = sortSpecs.get(i);
      LuceneFieldDef fieldDef = schema.get(spec.fieldName);

      // Determine type name: use schema type if available, otherwise use unmappedType
      String esType = fieldDef != null ? fieldDef.fieldType.name : spec.unmappedType;

      // Reject sorting on TEXT fields (analyzed fields) - users should use .keyword instead
      if ((fieldDef != null && fieldDef.fieldType == FieldType.TEXT)
          || (fieldDef == null && "text".equalsIgnoreCase(spec.unmappedType))) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot sort on analyzed text field '%s'. Use '%s.keyword' instead for sorting.",
                spec.fieldName, spec.fieldName));
      }

      SortField.Type luceneType = esTypeToLuceneSortType(esType);

      if (fieldDef == null) {
        LOG.debug(
            "Sort field '{}' not found in schema, using unmapped_type '{}' (Lucene type: {})",
            spec.fieldName,
            spec.unmappedType != null ? spec.unmappedType : "null (defaulting to date)",
            luceneType);
      }

      // Create sort field - use SortedNumericSortField for multi-valued numeric fields
      // Only use SortedNumericSortField if field exists in schema AND is actually multi-valued
      // Don't trust unmapped_type hints from clients (e.g., Grafana sends "boolean" for all fields)
      SortField sortField;
      if (fieldDef != null && isStoredAsMultiValuedNumeric(fieldDef.fieldType.name)) {
        // Boolean and half_float are stored with SortedNumericDocValuesField (multi-valued)
        // Use SortedNumericSortField which knows how to handle multi-valued fields
        sortField = createMultiValuedSortField(spec.fieldName, luceneType, spec.isDescending);
      } else {
        // Regular single-valued fields use standard SortField
        // This includes: unmapped fields, and mapped non-multi-valued fields
        sortField = new SortField(spec.fieldName, luceneType, spec.isDescending);
      }

      // Set missing value to ensure consistent sorting of documents without this field
      setMissingValue(sortField, luceneType, spec.isDescending);

      sortFields[i] = sortField;
    }

    return new Sort(sortFields);
  }

  /**
   * Builds a top field collector for the requested amount of results, with the option to set the
   * totalHitsThreshold. If the totalHitsThreshold is set to Integer.MAX_VALUE it will force a
   * ScoreMode.COMPLETE, iterating over all documents at the expense of a longer query time. This
   * value can be set to equal howMany to allow early exiting (ScoreMode.TOP_SCORES), but should
   * only be done when all collectors are tolerant of an early exit.
   *
   * @param sort The Lucene Sort object defining the sort order
   * @param howMany Number of results to collect
   * @param totalHitsThreshold Threshold for total hits counting
   * @return CollectorManager for TopFieldCollector, or null if howMany is 0
   */
  private CollectorManager<TopFieldCollector, TopFieldDocs> buildTopFieldCollector(
      Sort sort, int howMany, int totalHitsThreshold) {
    if (howMany > 0) {
      return TopFieldCollector.createSharedManager(sort, howMany, null, totalHitsThreshold);
    } else {
      return null;
    }
  }

  @Override
  public void close() {
    try {
      searcherManager.removeListener(refreshListener);
      astraSearcherManager.close();
    } catch (IOException e) {
      LOG.error("Encountered error closing searcher manager", e);
    }
  }

  /**
   * Checks if a field type is stored using SortedNumericDocValuesField (multi-valued numeric
   * storage). These types require SortedNumericSortField instead of regular SortField.
   *
   * @param fieldType The field type name to check
   * @return true if the field type uses multi-valued numeric storage
   */
  private static boolean isStoredAsMultiValuedNumeric(String fieldType) {
    if (fieldType == null) {
      return false;
    }
    String normalized = fieldType.toLowerCase().trim();
    return normalized.equals("boolean") || normalized.equals("half_float");
  }

  /**
   * Creates a SortedNumericSortField for fields stored with SortedNumericDocValuesField. Uses MIN
   * selector for ascending sorts and MAX selector for descending sorts.
   *
   * @param fieldName The name of the field to sort by
   * @param type The Lucene sort field type
   * @param isDescending Whether to sort in descending order
   * @return SortedNumericSortField configured with appropriate selector
   */
  private static SortField createMultiValuedSortField(
      String fieldName, SortField.Type type, boolean isDescending) {
    SortedNumericSelector.Type selector =
        isDescending ? SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN;
    return new SortedNumericSortField(fieldName, type, isDescending, selector);
  }
}
