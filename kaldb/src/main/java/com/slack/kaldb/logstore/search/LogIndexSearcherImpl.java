package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.util.ArgValidationUtils.ensureNonEmptyString;
import static com.slack.kaldb.util.ArgValidationUtils.ensureNonNullString;
import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

import brave.ScopedSpan;
import brave.Tracing;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogMessage.SystemField;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.logstore.opensearch.OpenSearchAdapter;
import com.slack.kaldb.logstore.search.aggregations.AggBuilder;
import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import com.slack.kaldb.util.JsonUtil;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.MMapDirectory;
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

  @VisibleForTesting
  public static SearcherManager searcherManagerFromPath(Path path) throws IOException {
    MMapDirectory directory = new MMapDirectory(path);
    return new SearcherManager(directory, null);
  }

  public LogIndexSearcherImpl(
      SearcherManager searcherManager, ConcurrentHashMap<String, LuceneFieldDef> chunkSchema) {
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
    this.searcherManager = searcherManager;
    this.searcherManager.addListener(refreshListener);

    // initialize the adapter with whatever the default schema is
    openSearchAdapter.reloadSchema();
  }

  @Override
  public SearchResult<LogMessage> search(
      String dataset,
      String queryStr,
      Long startTimeMsEpoch,
      Long endTimeMsEpoch,
      int howMany,
      AggBuilder aggBuilder) {

    ensureNonEmptyString(dataset, "dataset should be a non-empty string");
    ensureNonNullString(queryStr, "query should be a non-empty string");
    if (startTimeMsEpoch != null) {
      ensureTrue(startTimeMsEpoch >= 0, "start time should be non-negative value");
    }
    if (startTimeMsEpoch != null && endTimeMsEpoch != null) {
      ensureTrue(startTimeMsEpoch < endTimeMsEpoch, "end time should be greater than start time");
    }
    ensureTrue(howMany >= 0, "hits requested should not be negative.");
    ensureTrue(howMany > 0 || aggBuilder != null, "Hits or aggregation should be requested.");

    ScopedSpan span = Tracing.currentTracer().startScopedSpan("LogIndexSearcherImpl.search");
    span.tag("dataset", dataset);
    span.tag("queryStr", queryStr);
    span.tag("startTimeMsEpoch", String.valueOf(startTimeMsEpoch));
    span.tag("endTimeMsEpoch", String.valueOf(endTimeMsEpoch));
    span.tag("howMany", String.valueOf(howMany));

    Stopwatch elapsedTime = Stopwatch.createStarted();
    try {
      // Acquire an index searcher from searcher manager.
      // This is a useful optimization for indexes that are static.
      IndexSearcher searcher = searcherManager.acquire();

      Query query =
          openSearchAdapter.buildQuery(
              dataset, queryStr, startTimeMsEpoch, endTimeMsEpoch, searcher);
      span.tag("lucene_query", query.toString());
      try {
        List<LogMessage> results;
        InternalAggregation internalAggregation = null;

        if (howMany > 0) {
          CollectorManager<TopFieldCollector, TopFieldDocs> topFieldCollector =
              buildTopFieldCollector(howMany, aggBuilder != null ? Integer.MAX_VALUE : howMany);
          MultiCollectorManager collectorManager;
          if (aggBuilder != null) {
            collectorManager =
                new MultiCollectorManager(
                    topFieldCollector,
                    openSearchAdapter.getCollectorManager(aggBuilder, searcher, query));
          } else {
            collectorManager = new MultiCollectorManager(topFieldCollector);
          }
          Object[] collector = searcher.search(query, collectorManager);

          ScoreDoc[] hits = ((TopFieldDocs) collector[0]).scoreDocs;
          results = new ArrayList<>(hits.length);
          for (ScoreDoc hit : hits) {
            results.add(buildLogMessage(searcher, hit));
          }
          if (aggBuilder != null) {
            internalAggregation = (InternalAggregation) collector[1];
          }
        } else {
          results = Collections.emptyList();
          internalAggregation =
              searcher.search(
                  query, openSearchAdapter.getCollectorManager(aggBuilder, searcher, query));
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

  private LogMessage buildLogMessage(IndexSearcher searcher, ScoreDoc hit) {
    String s = "";
    try {
      s = searcher.doc(hit.doc).get(SystemField.SOURCE.fieldName);
      LogWireMessage wireMessage = JsonUtil.read(s, LogWireMessage.class);
      return new LogMessage(
          wireMessage.getIndex(),
          wireMessage.getType(),
          wireMessage.getId(),
          wireMessage.getTimestamp(),
          wireMessage.getSource());
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
      SortField sortField = new SortField(SystemField.TIME_SINCE_EPOCH.fieldName, Type.LONG, true);
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
      searcherManager.close();
    } catch (IOException e) {
      LOG.error("Encountered error closing searcher manager", e);
    }
  }
}
