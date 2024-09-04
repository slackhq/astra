package com.slack.astra.logstore.search;

import static com.slack.astra.util.ArgValidationUtils.ensureNonEmptyString;
import static com.slack.astra.util.ArgValidationUtils.ensureNonNullString;
import static com.slack.astra.util.ArgValidationUtils.ensureTrue;

import brave.ScopedSpan;
import brave.Tracing;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogMessage.SystemField;
import com.slack.astra.logstore.LogWireMessage;
import com.slack.astra.logstore.opensearch.OpenSearchAdapter;
import com.slack.astra.logstore.search.aggregations.AggBuilder;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.astra.util.JsonUtil;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
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
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.index.query.QueryBuilder;
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

    DirectoryReader directoryReader = DirectoryReader.open(directory);

    // filter reader > sub reader wrapper > leaf reader > stored field reader,
    //   which uses the stored field visitor to perform the field level redactions
    // Heavily inspired by the DlsFlsFilterLeafReader from opensearch
    // https://github.com/opensearch-project/security/blob/4f2e689a37765786dd256a7591434815bbb950a4/src/main/java/org/opensearch/security/configuration/DlsFlsFilterLeafReader.java

    // Implements the hashing/redaction for stored fields
    class HashingStoredFieldVisitor extends StoredFieldVisitor {
      private ObjectMapper om = new ObjectMapper();
      private final StoredFieldVisitor delegate;

      public HashingStoredFieldVisitor(final StoredFieldVisitor delegate) {
        super();
        this.delegate = delegate;
      }

      @Override
      public Status needsField(FieldInfo fieldInfo) throws IOException {
        return delegate.needsField(fieldInfo);
      }

      @Override
      public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        // todo - probably need to figure out as well
        delegate.binaryField(fieldInfo, value);
      }

      @Override
      public void stringField(FieldInfo fieldInfo, String value) throws IOException {
        if (fieldInfo.name.equals("_source")) {
          Map<String, Object> source = om.readValue(value, new TypeReference<HashMap<String,Object>>() {});

          if (source.containsKey("source")) {
            Map<String, Object> innerSource = (Map<String, Object>) source.get("source");

            if (innerSource.containsKey("stringproperty")) {
              innerSource.put("stringproperty", "REDACTED");
              source.put("source", innerSource);
            }
          }

          // todo - breakpoint here
          delegate.stringField(fieldInfo, om.writeValueAsString(source));
        } else {
          delegate.stringField(fieldInfo, value);
        }
      }

      @Override
      public void intField(FieldInfo fieldInfo, int value) throws IOException {
        delegate.intField(fieldInfo, value);
      }

      @Override
      public void longField(FieldInfo fieldInfo, long value) throws IOException {
        delegate.longField(fieldInfo, value);
      }

      @Override
      public void floatField(FieldInfo fieldInfo, float value) throws IOException {
        delegate.floatField(fieldInfo, value);
      }

      @Override
      public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
        delegate.doubleField(fieldInfo, value);
      }
    }

    // implements the masked field reader
    class MaskedFieldReader extends StoredFieldsReader {

      private final StoredFieldsReader in;

      public MaskedFieldReader(StoredFieldsReader in) {
        this.in = in;
      }

      @Override
      public StoredFieldsReader clone() {
        return new MaskedFieldReader(in);
      }

      @Override
      public void checkIntegrity() throws IOException {
        in.checkIntegrity();
      }

      @Override
      public void close() throws IOException {
        in.close();
      }

      @Override
      public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        //        visitor = getDlsFlsVisitor(visitor);
        try {
          visitor = new HashingStoredFieldVisitor(visitor);
          in.document(docID, visitor);
        } finally {
          //          finishVisitor(visitor);
        }
      }
    }


    // Implements the masking leaf reader
    class MaskedLeafReader extends SequentialStoredFieldsLeafReader {
      public MaskedLeafReader(LeafReader in) {
        super(in);
      }

      @Override
      public SortedDocValues getSortedDocValues(String field) throws IOException {
        return in.getSortedDocValues(field);
      }


      @Override
      public StoredFields storedFields() throws IOException {
        return in.storedFields();
      }

      @Override
      public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        // todo
        visitor = new HashingStoredFieldVisitor(visitor);
        in.document(docID, visitor);
      }

      @Override
      protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return new MaskedFieldReader(reader);
      }

      @Override
      public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
      }
    }

    // Implements a masking subreaderwrapper
    class MaskingSubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
      @Override
      public LeafReader wrap(LeafReader reader) {
        return new MaskedLeafReader(reader);
      }
    }

    // Implements a filterdirectoryreader for masking
    class FilterMaskingReader extends FilterDirectoryReader {
      public FilterMaskingReader(DirectoryReader in) throws IOException {
        super(in, new MaskingSubReaderWrapper());
      }

      @Override
      protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new FilterMaskingReader(in);
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
      }
    }

    FilterMaskingReader reader = new FilterMaskingReader(directoryReader);
    return new SearcherManager(reader, null);
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
      AggBuilder aggBuilder,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter) {

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
    span.tag("startTimeMsEpoch", String.valueOf(startTimeMsEpoch));
    span.tag("endTimeMsEpoch", String.valueOf(endTimeMsEpoch));
    span.tag("howMany", String.valueOf(howMany));

    Stopwatch elapsedTime = Stopwatch.createStarted();
    try {
      // Acquire an index searcher from searcher manager.
      // This is a useful optimization for indexes that are static.
      IndexSearcher searcher = searcherManager.acquire();

      try {
        List<LogMessage> results;
        InternalAggregation internalAggregation = null;
        Query query =
            openSearchAdapter.buildQuery(
                dataset, queryStr, startTimeMsEpoch, endTimeMsEpoch, searcher, queryBuilder);

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
            results.add(buildLogMessage(searcher, hit, sourceFieldFilter));
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
