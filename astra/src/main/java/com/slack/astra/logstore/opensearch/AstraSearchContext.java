package com.slack.astra.logstore.opensearch;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
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
import org.opensearch.search.aggregations.BucketCollectorProcessor;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
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

/**
 * Minimal implementation of an OpenSearch SearchContext while still allowing it to construct valid
 * Aggregators using the AggregatorFactory.create(SearchContext searchContext, Aggregator parent,
 * CardinalityUpperBound cardinality) method. Throws NotImplementedException() for all methods
 * except those specifically implemented, to prevent unexpected invocations without proper
 * implementation. Additional future aggregations may require more methods to be implemented, or
 * existing methods to have a higher fidelity implementation.
 */
public class AstraSearchContext extends SearchContext {
  private final BigArrays bigArrays;
  private final ContextIndexSearcher contextIndexSearcher;
  private BucketCollectorProcessor bucketCollectorProcessor = NO_OP_BUCKET_COLLECTOR_PROCESSOR;
  private final QueryShardContext queryShardContext;
  private final Query query;
  private SearchContextAggregations searchContextAggregations;

  public AstraSearchContext(
      BigArrays bigArrays,
      QueryShardContext queryShardContext,
      IndexSearcher indexSearcher,
      Query query)
      throws IOException {
    this.bigArrays = bigArrays;
    this.queryShardContext = queryShardContext;
    this.query = query;
    this.contextIndexSearcher =
        new ContextIndexSearcher(
            indexSearcher.getIndexReader(),
            queryShardContext.getSearchSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            false,
            indexSearcher.getExecutor(),
            this);
  }

  @Override
  public void setTask(SearchShardTask task) {
    throw new NotImplementedException();
  }

  @Override
  public SearchShardTask getTask() {
    throw new NotImplementedException();
  }

  @Override
  public boolean isCancelled() {
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
  }

  @Override
  public ShardSearchContextId id() {
    throw new NotImplementedException();
  }

  @Override
  public String source() {
    throw new NotImplementedException();
  }

  @Override
  public ShardSearchRequest request() {
    throw new NotImplementedException();
  }

  @Override
  public SearchType searchType() {
    throw new NotImplementedException();
  }

  @Override
  public SearchShardTarget shardTarget() {
    // only appears to be used in the AggregatorBase to construct a QueryPhaseExecutionException
    // in the QueryPhaseExecutionException the SearchShardTarget can safely be null
    // See SearchException.writeTo()
    return null;
  }

  @Override
  public int numberOfShards() {
    throw new NotImplementedException();
  }

  @Override
  public float queryBoost() {
    throw new NotImplementedException();
  }

  @Override
  public ScrollContext scrollContext() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContextAggregations aggregations() {
    return searchContextAggregations;
  }

  @Override
  public SearchContext aggregations(SearchContextAggregations aggregations) {
    this.searchContextAggregations = aggregations;
    return this;
  }

  @Override
  public void addSearchExt(SearchExtBuilder searchExtBuilder) {
    throw new NotImplementedException();
  }

  @Override
  public SearchExtBuilder getSearchExt(String name) {
    throw new NotImplementedException();
  }

  @Override
  public SearchHighlightContext highlight() {
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
  }

  @Override
  public ScriptFieldsContext scriptFields() {
    throw new NotImplementedException();
  }

  @Override
  public boolean sourceRequested() {
    throw new NotImplementedException();
  }

  @Override
  public boolean hasFetchSourceContext() {
    throw new NotImplementedException();
  }

  @Override
  public FetchSourceContext fetchSourceContext() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
    throw new NotImplementedException();
  }

  @Override
  public FetchDocValuesContext docValuesContext() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
    throw new NotImplementedException();
  }

  @Override
  public FetchFieldsContext fetchFieldsContext() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext) {
    throw new NotImplementedException();
  }

  @Override
  public ContextIndexSearcher searcher() {
    return contextIndexSearcher;
  }

  @Override
  public IndexShard indexShard() {
    throw new NotImplementedException();
  }

  @Override
  public MapperService mapperService() {
    throw new NotImplementedException();
  }

  @Override
  public SimilarityService similarityService() {
    throw new NotImplementedException();
  }

  @Override
  public BigArrays bigArrays() {
    return bigArrays;
  }

  @Override
  public BitsetFilterCache bitsetFilterCache() {
    throw new NotImplementedException();
  }

  @Override
  public TimeValue timeout() {
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
  }

  @Override
  public SearchContext minimumScore(float minimumScore) {
    throw new NotImplementedException();
  }

  @Override
  public Float minimumScore() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContext sort(SortAndFormats sort) {
    throw new NotImplementedException();
  }

  @Override
  public SortAndFormats sort() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContext trackScores(boolean trackScores) {
    throw new NotImplementedException();
  }

  @Override
  public boolean trackScores() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContext trackTotalHitsUpTo(int trackTotalHits) {
    throw new NotImplementedException();
  }

  @Override
  public int trackTotalHitsUpTo() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContext searchAfter(FieldDoc searchAfter) {
    throw new NotImplementedException();
  }

  @Override
  public FieldDoc searchAfter() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContext collapse(CollapseContext collapse) {
    throw new NotImplementedException();
  }

  @Override
  public CollapseContext collapse() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContext parsedPostFilter(ParsedQuery postFilter) {
    throw new NotImplementedException();
  }

  @Override
  public ParsedQuery parsedPostFilter() {
    throw new NotImplementedException();
  }

  @Override
  public Query aliasFilter() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContext parsedQuery(ParsedQuery query) {
    throw new NotImplementedException();
  }

  @Override
  public ParsedQuery parsedQuery() {
    throw new NotImplementedException();
  }

  @Override
  public Query query() {
    return this.query;
  }

  @Override
  public int from() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContext from(int from) {
    throw new NotImplementedException();
  }

  @Override
  public int size() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContext size(int size) {
    throw new NotImplementedException();
  }

  @Override
  public boolean hasStoredFields() {
    throw new NotImplementedException();
  }

  @Override
  public boolean hasStoredFieldsContext() {
    throw new NotImplementedException();
  }

  @Override
  public boolean storedFieldsRequested() {
    throw new NotImplementedException();
  }

  @Override
  public StoredFieldsContext storedFieldsContext() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
    throw new NotImplementedException();
  }

  @Override
  public boolean explain() {
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
  }

  @Override
  public int docIdsToLoadFrom() {
    throw new NotImplementedException();
  }

  @Override
  public int docIdsToLoadSize() {
    throw new NotImplementedException();
  }

  @Override
  public SearchContext docIdsToLoad(
      int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
    throw new NotImplementedException();
  }

  @Override
  public DfsSearchResult dfsResult() {
    throw new NotImplementedException();
  }

  @Override
  public QuerySearchResult queryResult() {
    throw new NotImplementedException();
  }

  @Override
  public FetchPhase fetchPhase() {
    throw new NotImplementedException();
  }

  @Override
  public FetchSearchResult fetchResult() {
    throw new NotImplementedException();
  }

  @Override
  public Profilers getProfilers() {
    // Per Javadoc, return null if profiling is not enabled.
    return null;
  }

  @Override
  public MappedFieldType fieldType(String name) {
    throw new NotImplementedException();
  }

  @Override
  public ObjectMapper getObjectMapper(String name) {
    throw new NotImplementedException();
  }

  @Override
  public long getRelativeTimeInMillis() {
    throw new NotImplementedException();
  }

  @Override
  public Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>>
      queryCollectorManagers() {
    throw new NotImplementedException();
  }

  @Override
  public QueryShardContext getQueryShardContext() {
    return queryShardContext;
  }

  @Override
  public ReaderContext readerContext() {
    throw new NotImplementedException();
  }

  @Override
  public InternalAggregation.ReduceContext partialOnShard() {
    return InternalAggregation.ReduceContext.forPartialReduction(
        AstraBigArrays.getInstance(),
        ScriptServiceProvider.getInstance(),
        () -> PipelineAggregator.PipelineTree.EMPTY);
  }

  @Override
  public void setBucketCollectorProcessor(BucketCollectorProcessor bucketCollectorProcessor) {
    this.bucketCollectorProcessor = bucketCollectorProcessor;
  }

  @Override
  public BucketCollectorProcessor bucketCollectorProcessor() {
    return bucketCollectorProcessor;
  }

  @Override
  public boolean shouldUseTimeSeriesDescSortOptimization() {
    // this is true, since we index with the timestamp in reverse order
    // see LuceneIndexStoreImpl.buildIndexWriterConfig()
    return true;
  }
}
