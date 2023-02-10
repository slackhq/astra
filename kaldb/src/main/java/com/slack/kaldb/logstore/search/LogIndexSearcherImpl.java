package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.util.ArgValidationUtils.ensureNonEmptyString;
import static com.slack.kaldb.util.ArgValidationUtils.ensureNonNullString;
import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

import brave.ScopedSpan;
import brave.Tracing;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
=======
import com.slack.kaldb.elasticsearchApi.searchRequest.aggregations.DateHistogramAggregation;
=======
>>>>>>> Cleanup, port into logindexsearcher
import com.slack.kaldb.histogram.FixedIntervalHistogramImpl;
import com.slack.kaldb.histogram.Histogram;
import com.slack.kaldb.histogram.HistogramBucket;
import com.slack.kaldb.histogram.NoOpHistogramImpl;
>>>>>>> POC working histogram
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogMessage.SystemField;
import com.slack.kaldb.logstore.LogWireMessage;
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
import com.slack.kaldb.logstore.opensearch.OpenSearchAggregationAdapter;
<<<<<<< bburkholder/opensearch-serialize
=======
import com.slack.kaldb.logstore.OpensearchShim;
>>>>>>> Add POC for serialization
=======
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogMessage.SystemField;
import com.slack.kaldb.logstore.LogWireMessage;
<<<<<<< bburkholder/opensearch-serialize
import com.slack.kaldb.logstore.opensearch.OpensearchShim;
>>>>>>> Initial cleanup
=======
import com.slack.kaldb.logstore.opensearch.OpenSearchAdapter;
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
=======
import com.slack.kaldb.logstore.opensearch.OpenSearchAggregationAdapter;
>>>>>>> Cleanup OpenSearchAggregationAdapter
=======
import com.slack.kaldb.logstore.search.aggregations.AggBuilder;
>>>>>>> Initial aggs request POC
import com.slack.kaldb.logstore.search.queryparser.KaldbQueryParser;
import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import com.slack.kaldb.util.JsonUtil;
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
<<<<<<< bburkholder/opensearch-serialize
=======

>>>>>>> Cleanup, port into logindexsearcher
=======
>>>>>>> Add POC for serialization
=======
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
>>>>>>> Test aggs all the way out
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.MMapDirectory;
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
=======
=======
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.DeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentGenerator;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
>>>>>>> Rework results to return fixed bucket widths for now
=======
import org.opensearch.common.io.stream.InputStreamStreamInput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
>>>>>>> Add POC for serialization
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
<<<<<<< bburkholder/opensearch-serialize
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
<<<<<<< bburkholder/opensearch-serialize
>>>>>>> POC working histogram
=======
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
>>>>>>> Cleanup, port into logindexsearcher
=======
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
>>>>>>> Add POC for serialization
=======
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
>>>>>>> Test aggs all the way out
=======
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
>>>>>>> Initial aggs request POC
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

  private final ConcurrentHashMap<String, LuceneFieldDef> chunkSchema;

  @VisibleForTesting
  public static SearcherManager searcherManagerFromPath(Path path) throws IOException {
    MMapDirectory directory = new MMapDirectory(path);
    return new SearcherManager(directory, null);
  }

  public LogIndexSearcherImpl(
      SearcherManager searcherManager, ConcurrentHashMap<String, LuceneFieldDef> chunkSchema) {
    this.searcherManager = searcherManager;
    this.analyzer = new StandardAnalyzer();
    this.chunkSchema = chunkSchema;
  }

  // Lucene's query parsers are not thread safe. So, create a new one for every request.
  private QueryParser buildQueryParser() {
    return new KaldbQueryParser(SystemField.ALL.fieldName, analyzer, chunkSchema);
  }

  @Override
  public SearchResult<LogMessage> search(
      String dataset,
      String queryStr,
      long startTimeMsEpoch,
      long endTimeMsEpoch,
      int howMany,
      AggBuilder aggBuilder) {

    ensureNonEmptyString(dataset, "dataset should be a non-empty string");
    ensureNonNullString(queryStr, "query should be a non-empty string");
    ensureTrue(startTimeMsEpoch >= 0, "start time should be non-negative value");
    ensureTrue(startTimeMsEpoch < endTimeMsEpoch, "end time should be greater than start time");
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
      Query query = buildQuery(span, dataset, queryStr, startTimeMsEpoch, endTimeMsEpoch);

      // Acquire an index searcher from searcher manager.
      // This is a useful optimization for indexes that are static.
      IndexSearcher searcher = searcherManager.acquire();
      try {
        List<LogMessage> results;
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
        InternalAutoDateHistogram histogram = null;
=======
        InternalAggregation histogram = null;
>>>>>>> POC working histogram
=======
        InternalDateHistogram histogram = null;
>>>>>>> Cleanup, port into logindexsearcher
=======
        InternalAutoDateHistogram histogram = null;
>>>>>>> Switch to auto date hitogram impl
=======
        InternalDateHistogram histogram = null;
>>>>>>> Test using fixed interval to avoid bucket issue
=======
        InternalAutoDateHistogram histogram = null;
>>>>>>> Rework results to return fixed bucket widths for now
=======
        InternalDateHistogram histogram = null;
>>>>>>> Initial aggs request POC

        if (howMany > 0) {
          CollectorManager<TopFieldCollector, TopFieldDocs> topFieldCollector =
              buildTopFieldCollector(howMany, aggBuilder != null ? Integer.MAX_VALUE : howMany);
          MultiCollectorManager collectorManager;
<<<<<<< bburkholder/opensearch-serialize
          if (bucketCount > 0) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
            collectorManager =
                new MultiCollectorManager(
                    topFieldCollector,
                    OpenSearchAggregationAdapter.getCollectorManager(bucketCount));
=======
            collectorManager = new MultiCollectorManager(topFieldCollector, OpensearchShim.getCollectorManager());
>>>>>>> POC working histogram
=======
            collectorManager = new MultiCollectorManager(topFieldCollector, OpensearchShim.getCollectorManager(bucketCount));
>>>>>>> Switch to auto date hitogram impl
=======
            collectorManager = new MultiCollectorManager(topFieldCollector, OpensearchShim.getCollectorManager(bucketCount, startTimeMsEpoch, endTimeMsEpoch));
>>>>>>> Test using fixed interval to avoid bucket issue
=======
            collectorManager =
                new MultiCollectorManager(
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
                    topFieldCollector,
                    OpensearchShim.getCollectorManager(
                        bucketCount, startTimeMsEpoch, endTimeMsEpoch));
>>>>>>> Test aggs all the way out
=======
                    topFieldCollector, OpensearchShim.getCollectorManager(bucketCount));
>>>>>>> Initial cleanup
=======
                    topFieldCollector, OpenSearchAdapter.getCollectorManager(bucketCount));
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
=======
                    topFieldCollector,
                    OpenSearchAggregationAdapter.getCollectorManager(bucketCount));
>>>>>>> Cleanup OpenSearchAggregationAdapter
=======
          if (aggBuilder != null) {
            collectorManager =
                new MultiCollectorManager(
                    topFieldCollector,
                    OpenSearchAggregationAdapter.getCollectorManager(aggBuilder));
>>>>>>> Initial aggs request POC
          } else {
            collectorManager = new MultiCollectorManager(topFieldCollector);
          }
          Object[] collector = searcher.search(query, collectorManager);

          ScoreDoc[] hits = ((TopFieldDocs) collector[0]).scoreDocs;
          results = new ArrayList<>(hits.length);
          for (ScoreDoc hit : hits) {
            results.add(buildLogMessage(searcher, hit));
          }
<<<<<<< bburkholder/opensearch-serialize
          if (bucketCount > 0) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
            histogram = ((InternalAutoDateHistogram) collector[1]);
=======
          if (aggBuilder != null) {
            histogram = ((InternalDateHistogram) collector[1]);
>>>>>>> Initial aggs request POC
          }
        } else {
          results = Collections.emptyList();
          histogram =
              ((InternalDateHistogram)
                  searcher.search(
<<<<<<< bburkholder/opensearch-serialize
                      query, OpenSearchAggregationAdapter.getCollectorManager(bucketCount)));
<<<<<<< bburkholder/opensearch-serialize
=======
            histogram = ((InternalAggregation) collector[1]);
=======
            histogram = ((InternalDateHistogram) collector[1]);
>>>>>>> Cleanup, port into logindexsearcher
          }
        } else {
          results = Collections.emptyList();
          Object[] collector = searcher.search(query, new MultiCollectorManager(OpensearchShim.getCollectorManager()));
<<<<<<< bburkholder/opensearch-serialize
          histogram = ((InternalAggregation) collector[0]);
>>>>>>> POC working histogram
=======
          histogram = ((InternalDateHistogram) collector[0]);
>>>>>>> Cleanup, port into logindexsearcher
=======
            histogram = ((InternalAutoDateHistogram) collector[1]);
          }
        } else {
          results = Collections.emptyList();
          Object[] collector = searcher.search(query, new MultiCollectorManager(OpensearchShim.getCollectorManager(bucketCount)));
          histogram = ((InternalAutoDateHistogram) collector[0]);
>>>>>>> Switch to auto date hitogram impl
=======
            histogram = ((InternalDateHistogram) collector[1]);
=======
            histogram = ((InternalAutoDateHistogram) collector[1]);
>>>>>>> Rework results to return fixed bucket widths for now
          }
        } else {
          results = Collections.emptyList();
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
          Object[] collector = searcher.search(query, new MultiCollectorManager(OpensearchShim.getCollectorManager(bucketCount, startTimeMsEpoch, endTimeMsEpoch)));
<<<<<<< bburkholder/opensearch-serialize
          histogram = ((InternalDateHistogram) collector[0]);
>>>>>>> Test using fixed interval to avoid bucket issue
=======
=======
          Object[] collector =
              searcher.search(
                  query,
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
                  new MultiCollectorManager(
                      OpensearchShim.getCollectorManager(
                          bucketCount, startTimeMsEpoch, endTimeMsEpoch)));
>>>>>>> Test aggs all the way out
=======
                  new MultiCollectorManager(OpensearchShim.getCollectorManager(bucketCount)));
>>>>>>> Initial cleanup
=======
                  new MultiCollectorManager(OpenSearchAdapter.getCollectorManager(bucketCount)));
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
          histogram = ((InternalAutoDateHistogram) collector[0]);
>>>>>>> Rework results to return fixed bucket widths for now
        }

        long totalCount = results.size();
        if (histogram != null) {
          totalCount =
              histogram
                  .getBuckets()
                  .stream()
                  .collect(
                      Collectors.summarizingLong(InternalAutoDateHistogram.Bucket::getDocCount))
                  .getSum();
=======
          histogram =
              ((InternalAutoDateHistogram)
                  searcher.search(query, OpenSearchAdapter.getCollectorManager(bucketCount)));
>>>>>>> Cleanup LogSearcherImpl
=======
>>>>>>> Cleanup OpenSearchAggregationAdapter
=======
                      query, OpenSearchAggregationAdapter.getCollectorManager(aggBuilder)));
>>>>>>> Initial aggs request POC
        }

        elapsedTime.stop();
        return new SearchResult<>(
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
            results,
            elapsedTime.elapsed(TimeUnit.MICROSECONDS),
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
=======
            results,
            elapsedTime.elapsed(TimeUnit.MICROSECONDS),
<<<<<<< bburkholder/opensearch-serialize
>>>>>>> Cleanup LogSearcherImpl
            bucketCount > 0
=======
            aggBuilder != null
>>>>>>> Initial aggs request POC
                ? histogram
                    .getBuckets()
                    .stream()
                    .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                    .getSum()
                : results.size(),
<<<<<<< bburkholder/opensearch-serialize
=======
            bucketCount > 0 ? 0 : results.size(),
            List.of(),
//            histogram.getBuckets(),
>>>>>>> POC working histogram
=======
            totalCount,
            buckets,
>>>>>>> Cleanup, port into logindexsearcher
=======
>>>>>>> Cleanup LogSearcherImpl
            0,
            0,
            1,
            1,
            histogram);
<<<<<<< bburkholder/opensearch-serialize
=======
            results, elapsedTime.elapsed(TimeUnit.MICROSECONDS), totalCount, 0, 0, 1, 1, histogram);
>>>>>>> Initial cleanup
=======
>>>>>>> Cleanup LogSearcherImpl
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

  private Query buildQuery(
      ScopedSpan span, String dataset, String queryStr, long startTimeMsEpoch, long endTimeMsEpoch)
      throws ParseException {
    Builder queryBuilder = new Builder();

    // todo - we currently do not enforce searching against an dataset name, as we do not support
    //  multi-tenancy yet - see https://github.com/slackhq/kaldb/issues/223. Once index filtering
    //  is support at snapshot/query layer this should be re-enabled as appropriate.
    // queryBuilder.add(new TermQuery(new Term(SystemField.INDEX.fieldName, dataset)),
    // Occur.MUST);
    queryBuilder.add(
        LongPoint.newRangeQuery(
            SystemField.TIME_SINCE_EPOCH.fieldName, startTimeMsEpoch, endTimeMsEpoch),
        Occur.MUST);
    if (queryStr.length() > 0) {
      queryBuilder.add(buildQueryParser().parse(queryStr), Occur.MUST);
    }
    BooleanQuery query = queryBuilder.build();
    span.tag("lucene_query", query.toString());
    span.tag("lucene_query_num_clauses", Integer.toString(query.clauses().size()));
    return query;
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
