package com.slack.kaldb.chunk;

import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.aggregations.AggBuilder;
import java.util.Objects;

public class LogSearcherQuery {

  public String dataset;
  public String queryStr;

  public int howMany;
  public AggBuilder aggBuilder;
  public Long startTimeEpochMs;
  public Long endTimeEpochMs;

  public LogSearcherQuery(SearchQuery query, ChunkInfo chunkInfo) {
    this.dataset = query.dataset;
    this.queryStr = query.queryStr;
    this.howMany = query.howMany;
    this.aggBuilder = query.aggBuilder;
    this.startTimeEpochMs =
        getStartTime(query.startTimeEpochMs, chunkInfo.getDataStartTimeEpochMs());
    this.endTimeEpochMs = getEndTime(query.endTimeEpochMs, chunkInfo.getDataEndTimeEpochMs());
  }

  protected static Long getStartTime(long queryStartTime, long chunkStartTime) {
    Long searchStartTime = null;
    if (queryStartTime > chunkStartTime) {
      // if the query start time falls after the beginning of the chunk
      searchStartTime = queryStartTime;
    }
    return searchStartTime;
  }

  protected static Long getEndTime(long queryEndTime, long chunkEndTime) {
    Long searchEndTime = null;
    if (queryEndTime < chunkEndTime) {
      // if the query end time falls before the end of the chunk
      searchEndTime = queryEndTime;
    }
    return searchEndTime;
  }

  public boolean isCacheable() {
    // only cache aggregations
    if (howMany == 0 && aggBuilder != null) {
      // todo - consider skipping caching for queries that are looking using "until now" type
      // queries, as these are unlikely to be queried again.
      return true;
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof LogSearcherQuery that)) return false;

    if (howMany != that.howMany) return false;
    if (!Objects.equals(dataset, that.dataset)) return false;
    if (!Objects.equals(queryStr, that.queryStr)) return false;
    if (!Objects.equals(aggBuilder, that.aggBuilder)) return false;
    if (!Objects.equals(startTimeEpochMs, that.startTimeEpochMs)) return false;
    return Objects.equals(endTimeEpochMs, that.endTimeEpochMs);
  }

  @Override
  public int hashCode() {
    int result = dataset != null ? dataset.hashCode() : 0;
    result = 31 * result + (queryStr != null ? queryStr.hashCode() : 0);
    result = 31 * result + howMany;
    result = 31 * result + (aggBuilder != null ? aggBuilder.hashCode() : 0);
    result = 31 * result + (startTimeEpochMs != null ? startTimeEpochMs.hashCode() : 0);
    result = 31 * result + (endTimeEpochMs != null ? endTimeEpochMs.hashCode() : 0);
    return result;
  }
}
