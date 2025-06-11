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
import com.slack.astra.metadata.schema.LuceneFieldDef;
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
    this.openSearchAdapter = new OpenSearchAdapter(chunkSchema);
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
      AggregatorFactories.Builder aggregatorFactoriesBuilder) {

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

        if (howMany > 0) {
          CollectorManager<TopFieldCollector, TopFieldDocs> topFieldCollector =
              buildTopFieldCollector(
                  howMany, aggregatorFactoriesBuilder != null ? Integer.MAX_VALUE : howMany);
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
   * Builds a top field collector for the requested amount of results, with the option to set the
   * totalHitsThreshold. If the totalHitsThreshold is set to Integer.MAX_VALUE it will force a
   * ScoreMode.COMPLETE, iterating over all documents at the expense of a longer query time. This
   * value can be set to equal howMany to allow early exiting (ScoreMode.TOP_SCORES), but should
   * only be done when all collectors are tolerant of an early exit.
   */
  private CollectorManager<TopFieldCollector, TopFieldDocs> buildTopFieldCollector(
      int howMany, int totalHitsThreshold) {
    if (howMany > 0) {
      SortField sortField =
          new SortField(SystemField.TIME_SINCE_EPOCH.fieldName, SortField.Type.LONG, true);
      return TopFieldCollector.createSharedManager(
          new Sort(sortField), howMany, null, totalHitsThreshold);
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
}
