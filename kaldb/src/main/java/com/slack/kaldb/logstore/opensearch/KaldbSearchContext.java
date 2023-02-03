package com.slack.kaldb.logstore.opensearch;

import java.util.List;
import java.util.Map;
<<<<<<< bburkholder/opensearch-serialize
import org.apache.commons.lang3.NotImplementedException;
=======
>>>>>>> Initial cleanup
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
/**
 * Minimal implementation of an OpenSearch SearchContext while still allowing it to construct valid
 * Aggregators using the AggregatorFactory.create(SearchContext searchContext, Aggregator parent,
 * CardinalityUpperBound cardinality) method. Throws NotImplementedException() for all methods
 * except those specifically implemented, to prevent unexpected invocations without proper
 * implementation. Additional future aggregations may require more methods to be implemented, or
 * existing methods to have a higher fidelity implementation.
 */
=======
>>>>>>> Initial cleanup
public class KaldbSearchContext extends SearchContext {
  private final BigArrays bigArrays;
  private final QueryShardContext queryShardContext;

  public KaldbSearchContext(BigArrays bigArrays, QueryShardContext queryShardContext) {
    this.bigArrays = bigArrays;
    this.queryShardContext = queryShardContext;
  }

  @Override
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
  }

  @Override
  public boolean isCancelled() {
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
  }

  @Override
  protected void doClose() {}

  @Override
  public void preProcess(boolean rewrite) {}

  @Override
  public Query buildFilteredQuery(Query query) {
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public ShardSearchContextId id() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public String source() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public ShardSearchRequest request() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchType searchType() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchShardTarget shardTarget() {
<<<<<<< bburkholder/opensearch-serialize
    // only appears to be used in the AggregatorBase to construct a QueryPhaseExecutionException
    // in the QueryPhaseExecutionException the SearchShardTarget can safely be null
    // See SearchException.writeTo()
=======
>>>>>>> Initial cleanup
    return null;
  }

  @Override
  public int numberOfShards() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
  }

  @Override
  public float queryBoost() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
  }

  @Override
  public ScrollContext scrollContext() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContextAggregations aggregations() {
<<<<<<< bburkholder/opensearch-serialize
    // todo - required for multibucket consumers
=======
>>>>>>> Initial cleanup
    return null;
  }

  @Override
  public SearchContext aggregations(SearchContextAggregations aggregations) {
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
  }

  @Override
  public void addSearchExt(SearchExtBuilder searchExtBuilder) {}

  @Override
  public SearchExtBuilder getSearchExt(String name) {
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchHighlightContext highlight() {
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
>>>>>>> Initial cleanup
  }

  @Override
  public ScriptFieldsContext scriptFields() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public boolean sourceRequested() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return false;
>>>>>>> Initial cleanup
  }

  @Override
  public boolean hasFetchSourceContext() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return false;
>>>>>>> Initial cleanup
  }

  @Override
  public FetchSourceContext fetchSourceContext() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public FetchDocValuesContext docValuesContext() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public FetchFieldsContext fetchFieldsContext() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public ContextIndexSearcher searcher() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public IndexShard indexShard() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public MapperService mapperService() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SimilarityService similarityService() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public BigArrays bigArrays() {
    return bigArrays;
  }

  @Override
  public BitsetFilterCache bitsetFilterCache() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public TimeValue timeout() {
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
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext minimumScore(float minimumScore) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public Float minimumScore() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext sort(SortAndFormats sort) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SortAndFormats sort() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext trackScores(boolean trackScores) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public boolean trackScores() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return false;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext trackTotalHitsUpTo(int trackTotalHits) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public int trackTotalHitsUpTo() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext searchAfter(FieldDoc searchAfter) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public FieldDoc searchAfter() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext collapse(CollapseContext collapse) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public CollapseContext collapse() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext parsedPostFilter(ParsedQuery postFilter) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public ParsedQuery parsedPostFilter() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public Query aliasFilter() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext parsedQuery(ParsedQuery query) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public ParsedQuery parsedQuery() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public Query query() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public int from() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext from(int from) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public int size() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext size(int size) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public boolean hasStoredFields() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return false;
>>>>>>> Initial cleanup
  }

  @Override
  public boolean hasStoredFieldsContext() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return false;
>>>>>>> Initial cleanup
  }

  @Override
  public boolean storedFieldsRequested() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return false;
>>>>>>> Initial cleanup
  }

  @Override
  public StoredFieldsContext storedFieldsContext() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public boolean explain() {
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
>>>>>>> Initial cleanup
  }

  @Override
  public int docIdsToLoadFrom() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
  }

  @Override
  public int docIdsToLoadSize() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
  }

  @Override
  public SearchContext docIdsToLoad(
      int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public DfsSearchResult dfsResult() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public QuerySearchResult queryResult() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public FetchPhase fetchPhase() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public FetchSearchResult fetchResult() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public Profilers getProfilers() {
<<<<<<< bburkholder/opensearch-serialize
    // Per Javadoc, return null if profiling is not enabled.
=======
>>>>>>> Initial cleanup
    return null;
  }

  @Override
  public MappedFieldType fieldType(String name) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public ObjectMapper getObjectMapper(String name) {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public long getRelativeTimeInMillis() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return 0;
>>>>>>> Initial cleanup
  }

  @Override
  public Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>>
      queryCollectorManagers() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }

  @Override
  public QueryShardContext getQueryShardContext() {
    return queryShardContext;
  }

  @Override
  public ReaderContext readerContext() {
<<<<<<< bburkholder/opensearch-serialize
    throw new NotImplementedException();
=======
    return null;
>>>>>>> Initial cleanup
  }
}
