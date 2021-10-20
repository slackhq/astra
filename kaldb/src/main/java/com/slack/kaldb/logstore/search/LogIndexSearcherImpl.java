package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.util.ArgValidationUtils.ensureNonEmptyString;
import static com.slack.kaldb.util.ArgValidationUtils.ensureNonNullString;
import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

import brave.ScopedSpan;
import brave.Tracing;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.slack.kaldb.histogram.FixedIntervalHistogramImpl;
import com.slack.kaldb.histogram.Histogram;
import com.slack.kaldb.histogram.NoOpHistogramImpl;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogMessage.ReservedField;
import com.slack.kaldb.logstore.LogMessage.SystemField;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.util.JsonUtil;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.store.NIOFSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A wrapper around lucene that helps us search a single index containing logs.
 * TODO: Add template type to this class definition.
 */
public class LogIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(LogIndexSearcherImpl.class);

  private final SearcherManager searcherManager;
  private final StandardAnalyzer analyzer;

  @VisibleForTesting
  public static SearcherManager searcherManagerFromPath(Path path) throws IOException {
    NIOFSDirectory directory = new NIOFSDirectory(path);
    return new SearcherManager(directory, null);
  }

  // todo - this is not needed once this data is on the snapshot
  public static int getNumDocs(Path path) throws IOException {
    NIOFSDirectory directory = new NIOFSDirectory(path);
    DirectoryReader directoryReader = DirectoryReader.open(directory);
    int numDocs = directoryReader.numDocs();
    directoryReader.close();
    return numDocs;
  }

  public LogIndexSearcherImpl(SearcherManager searcherManager) {
    this.searcherManager = searcherManager;
    this.analyzer = new StandardAnalyzer();
  }

  // Lucene's query parsers are not thread safe. So, create a new one for every request.
  private QueryParser buildQueryParser() {
    return new QueryParser(ReservedField.MESSAGE.fieldName, analyzer);
  }

  public SearchResult<LogMessage> search(
      String indexName,
      String queryStr,
      long startTimeMsEpoch,
      long endTimeMsEpoch,
      int howMany,
      int bucketCount) {

    ensureNonEmptyString(indexName, "indexName should be a non-empty string");
    ensureNonNullString(queryStr, "query should be a non-empty string");
    ensureTrue(startTimeMsEpoch >= 0, "start time should be non-negative value");
    ensureTrue(startTimeMsEpoch < endTimeMsEpoch, "end time should be greater than start time");
    ensureTrue(howMany >= 0, "hits requested should not be negative.");
    ensureTrue(bucketCount >= 0, "bucket count should not be negative.");
    ensureTrue(howMany > 0 || bucketCount > 0, "Hits or histogram should be requested.");

    ScopedSpan span = Tracing.currentTracer().startScopedSpan("LogIndexSearcherImpl.search");
    span.tag("indexName", indexName);
    span.tag("queryStr", queryStr);
    span.tag("startTimeMsEpoch", String.valueOf(startTimeMsEpoch));
    span.tag("endTimeMsEpoch", String.valueOf(endTimeMsEpoch));
    span.tag("howMany", String.valueOf(howMany));
    span.tag("bucketCount", String.valueOf(bucketCount));

    Stopwatch elapsedTime = Stopwatch.createStarted();
    try {
      Query query = buildQuery(indexName, queryStr, startTimeMsEpoch, endTimeMsEpoch);
      span.tag("lucene query", query.toString());

      // Acquire an index searcher from searcher manager.
      // This is a useful optimization for indexes that are static.
      IndexSearcher searcher = searcherManager.acquire();
      span.annotate("searchManager.acquire complete");
      try {
        TopFieldCollector topFieldCollector = buildTopFieldCollector(howMany);
        StatsCollector statsCollector =
            buildStatsCollector(bucketCount, startTimeMsEpoch, endTimeMsEpoch);
        Collector collectorChain = MultiCollector.wrap(topFieldCollector, statsCollector);

        searcher.search(query, collectorChain);
        span.annotate("searcher.search complete");
        List<LogMessage> results;
        if (howMany > 0) {
          ScoreDoc[] hits = topFieldCollector.topDocs().scoreDocs;
          results =
              Stream.of(hits)
                  .map(hit -> buildLogMessage(searcher, hit))
                  .collect(Collectors.toList());
        } else {
          results = Collections.emptyList();
        }

        Histogram histogram = statsCollector.histogram;

        elapsedTime.stop();
        return new SearchResult<>(
            results,
            elapsedTime.elapsed(TimeUnit.MICROSECONDS),
            histogram.count(),
            histogram.getBuckets(),
            0,
            0,
            1,
            1);
      } finally {
        searcherManager.release(searcher);
      }
    } catch (ParseException e) {
      span.error(e);
      throw new IllegalArgumentException("Unable to parse query string: " + queryStr, e);
      // TODO: Return Empty search result?
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
          wireMessage.getIndex(), wireMessage.getType(), wireMessage.id, wireMessage.source);
    } catch (IOException e) {
      throw new IllegalStateException("Error fetching and parsing a result from index: " + s, e);
    }
  }

  private TopFieldCollector buildTopFieldCollector(int howMany) {
    if (howMany > 0) {
      SortField sortField = new SortField(SystemField.TIME_SINCE_EPOCH.fieldName, Type.LONG, true);
      return TopFieldCollector.create(new Sort(sortField), howMany, howMany);
    } else {
      return null;
    }
  }

  private StatsCollector buildStatsCollector(
      int bucketCount, long startTimeMsEpoch, long endTimeMsEpoch) {
    Histogram histogram =
        bucketCount > 0
            ? new FixedIntervalHistogramImpl(startTimeMsEpoch, endTimeMsEpoch, bucketCount)
            : new NoOpHistogramImpl();
    return new StatsCollector(histogram);
  }

  private Query buildQuery(
      String indexName, String queryStr, long startTimeMsEpoch, long endTimeMsEpoch)
      throws ParseException {
    Builder queryBuilder = new Builder();
    queryBuilder.add(new TermQuery(new Term(SystemField.INDEX.fieldName, indexName)), Occur.MUST);
    queryBuilder.add(
        LongPoint.newRangeQuery(
            SystemField.TIME_SINCE_EPOCH.fieldName, startTimeMsEpoch, endTimeMsEpoch),
        Occur.MUST);
    if (queryStr.length() > 0) {
      queryBuilder.add(buildQueryParser().parse(queryStr), Occur.MUST);
    }
    return queryBuilder.build();
  }

  @Override
  public void close() {
    try {
      searcherManager.close();
    } catch (IOException e) {
      LOG.error("Encountered error closing searcher manager", e);
    }
  }
}
