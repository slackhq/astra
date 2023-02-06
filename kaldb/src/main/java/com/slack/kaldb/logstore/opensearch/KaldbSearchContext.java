package com.slack.kaldb.logstore.opensearch;

import java.util.List;
import java.util.Map;
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
import org.apache.commons.lang3.NotImplementedException;
=======
>>>>>>> Initial cleanup
=======
import org.apache.commons.lang3.NotImplementedException;
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.SearchContextAggregations;
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

<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
=======
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
/**
 * Minimal implementation of an OpenSearch SearchContext while still allowing it to construct valid
 * Aggregators using the AggregatorFactory.create(SearchContext searchContext, Aggregator parent,
 * CardinalityUpperBound cardinality) method. Throws NotImplementedException() for all methods
 * except those specifically implemented, to prevent unexpected invocations without proper
 * implementation. Additional future aggregations may require more methods to be implemented, or
 * existing methods to have a higher fidelity implementation.
 */
<<<<<<< bburkholder/opensearch-serialize
=======
>>>>>>> Initial cleanup
=======
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
public class KaldbSearchContext extends SearchContext {
  private final BigArrays bigArrays;
  private final QueryShardContext queryShardContext;

  public KaldbSearchContext(BigArrays bigArrays, QueryShardContext queryShardContext) {
    this.bigArrays = bigArrays;
    this.queryShardContext = queryShardContext;
  }

  @Override
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
  public void setTask(SearchShardTask task) {
    throw new NotImplementedException();
  }

  @Override
  public SearchShardTask getTask() {
    throw new NotImplementedException();
=======
  public void setTask(SearchShardTask task) {}

  @Override
  public SearchShardTask getTask() {
    return null;
>>>>>>> Initial cleanup
=======
  public void setTask(SearchShardTask task) {
    throw new NotImplementedException();
  }

  @Override
  public SearchShardTask getTask() {
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public boolean isCancelled() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
  }

  @Override
  protected void doClose() {
    throw new NotImplementedException();
  }

  @Override
  public void preProcess(boolean rewrite) {
    throw new NotImplementedException();
  }

  @Override
  public Query buildFilteredQuery(Query query) {
    throw new NotImplementedException();
=======
    return false;
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  protected void doClose() {
    throw new NotImplementedException();
  }

  @Override
  public void preProcess(boolean rewrite) {
    throw new NotImplementedException();
  }

  @Override
  public Query buildFilteredQuery(Query query) {
<<<<<<< bburkholder/opensearch-serialize
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public ShardSearchContextId id() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public String source() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public ShardSearchRequest request() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchType searchType() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchShardTarget shardTarget() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    // only appears to be used in the AggregatorBase to construct a QueryPhaseExecutionException
    // in the QueryPhaseExecutionException the SearchShardTarget can safely be null
    // See SearchException.writeTo()
=======
>>>>>>> Initial cleanup
=======
    // only appears to be used in the AggregatorBase to construct a QueryPhaseExecutionException
    // in the QueryPhaseExecutionException the SearchShardTarget can safely be null
    // See SearchException.writeTo()
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
    return null;
  }

  @Override
  public int numberOfShards() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public float queryBoost() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public ScrollContext scrollContext() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContextAggregations aggregations() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    // todo - required for multibucket consumers
=======
>>>>>>> Initial cleanup
=======
    // todo - required for multibucket consumers
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
    return null;
  }

  @Override
  public SearchContext aggregations(SearchContextAggregations aggregations) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
  }

  @Override
  public void addSearchExt(SearchExtBuilder searchExtBuilder) {
    throw new NotImplementedException();
  }

  @Override
  public SearchExtBuilder getSearchExt(String name) {
    throw new NotImplementedException();
=======
    return null;
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public void addSearchExt(SearchExtBuilder searchExtBuilder) {
    throw new NotImplementedException();
  }

  @Override
  public SearchExtBuilder getSearchExt(String name) {
<<<<<<< bburkholder/opensearch-serialize
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchHighlightContext highlight() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
  }

  @Override
  public void highlight(SearchHighlightContext highlight) {
    throw new NotImplementedException();
  }

  @Override
  public SuggestionSearchContext suggest() {
    throw new NotImplementedException();
  }

  @Override
  public void suggest(SuggestionSearchContext suggest) {
    throw new NotImplementedException();
  }

  @Override
  public List<RescoreContext> rescore() {
    throw new NotImplementedException();
  }

  @Override
  public void addRescore(RescoreContext rescore) {
    throw new NotImplementedException();
  }

  @Override
  public boolean hasScriptFields() {
    throw new NotImplementedException();
=======
    return null;
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public void highlight(SearchHighlightContext highlight) {
    throw new NotImplementedException();
  }

  @Override
  public SuggestionSearchContext suggest() {
    throw new NotImplementedException();
  }

  @Override
  public void suggest(SuggestionSearchContext suggest) {
    throw new NotImplementedException();
  }

  @Override
  public List<RescoreContext> rescore() {
    throw new NotImplementedException();
  }

  @Override
  public void addRescore(RescoreContext rescore) {
    throw new NotImplementedException();
  }

  @Override
  public boolean hasScriptFields() {
<<<<<<< bburkholder/opensearch-serialize
    return false;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public ScriptFieldsContext scriptFields() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public boolean sourceRequested() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return false;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public boolean hasFetchSourceContext() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return false;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public FetchSourceContext fetchSourceContext() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public FetchDocValuesContext docValuesContext() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public FetchFieldsContext fetchFieldsContext() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public ContextIndexSearcher searcher() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public IndexShard indexShard() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public MapperService mapperService() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SimilarityService similarityService() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public BigArrays bigArrays() {
    return bigArrays;
  }

  @Override
  public BitsetFilterCache bitsetFilterCache() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public TimeValue timeout() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
  }

  @Override
  public void timeout(TimeValue timeout) {
    throw new NotImplementedException();
  }

  @Override
  public int terminateAfter() {
    throw new NotImplementedException();
  }

  @Override
  public void terminateAfter(int terminateAfter) {
    throw new NotImplementedException();
  }

  @Override
  public boolean lowLevelCancellation() {
    throw new NotImplementedException();
=======
    return null;
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public void timeout(TimeValue timeout) {
    throw new NotImplementedException();
  }

  @Override
  public int terminateAfter() {
    throw new NotImplementedException();
  }

  @Override
  public void terminateAfter(int terminateAfter) {
    throw new NotImplementedException();
  }

  @Override
  public boolean lowLevelCancellation() {
<<<<<<< bburkholder/opensearch-serialize
    return false;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext minimumScore(float minimumScore) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public Float minimumScore() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext sort(SortAndFormats sort) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SortAndFormats sort() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext trackScores(boolean trackScores) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public boolean trackScores() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return false;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext trackTotalHitsUpTo(int trackTotalHits) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public int trackTotalHitsUpTo() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext searchAfter(FieldDoc searchAfter) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public FieldDoc searchAfter() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext collapse(CollapseContext collapse) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public CollapseContext collapse() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext parsedPostFilter(ParsedQuery postFilter) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public ParsedQuery parsedPostFilter() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public Query aliasFilter() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext parsedQuery(ParsedQuery query) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public ParsedQuery parsedQuery() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public Query query() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public int from() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext from(int from) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public int size() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext size(int size) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public boolean hasStoredFields() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return false;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public boolean hasStoredFieldsContext() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return false;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public boolean storedFieldsRequested() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return false;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public StoredFieldsContext storedFieldsContext() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public boolean explain() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
  }

  @Override
  public void explain(boolean explain) {
    throw new NotImplementedException();
  }

  @Override
  public List<String> groupStats() {
    throw new NotImplementedException();
  }

  @Override
  public void groupStats(List<String> groupStats) {
    throw new NotImplementedException();
  }

  @Override
  public boolean version() {
    throw new NotImplementedException();
  }

  @Override
  public void version(boolean version) {
    throw new NotImplementedException();
  }

  @Override
  public boolean seqNoAndPrimaryTerm() {
    throw new NotImplementedException();
  }

  @Override
  public void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
    throw new NotImplementedException();
  }

  @Override
  public int[] docIdsToLoad() {
    throw new NotImplementedException();
=======
    return false;
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public void explain(boolean explain) {
    throw new NotImplementedException();
  }

  @Override
  public List<String> groupStats() {
    throw new NotImplementedException();
  }

  @Override
  public void groupStats(List<String> groupStats) {
    throw new NotImplementedException();
  }

  @Override
  public boolean version() {
    throw new NotImplementedException();
  }

  @Override
  public void version(boolean version) {
    throw new NotImplementedException();
  }

  @Override
  public boolean seqNoAndPrimaryTerm() {
    throw new NotImplementedException();
  }

  @Override
  public void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
    throw new NotImplementedException();
  }

  @Override
  public int[] docIdsToLoad() {
<<<<<<< bburkholder/opensearch-serialize
    return new int[0];
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public int docIdsToLoadFrom() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public int docIdsToLoadSize() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public SearchContext docIdsToLoad(
      int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public DfsSearchResult dfsResult() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public QuerySearchResult queryResult() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public FetchPhase fetchPhase() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public FetchSearchResult fetchResult() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public Profilers getProfilers() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    // Per Javadoc, return null if profiling is not enabled.
=======
>>>>>>> Initial cleanup
=======
    // Per Javadoc, return null if profiling is not enabled.
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
    return null;
  }

  @Override
  public MappedFieldType fieldType(String name) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public ObjectMapper getObjectMapper(String name) {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public long getRelativeTimeInMillis() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>>
      queryCollectorManagers() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }

  @Override
  public QueryShardContext getQueryShardContext() {
    return queryShardContext;
  }

  @Override
  public ReaderContext readerContext() {
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
=======
    throw new NotImplementedException();
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
  }
}
