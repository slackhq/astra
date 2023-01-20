package com.slack.kaldb.logstore.search;

import java.util.List;
import java.util.Map;

public class ResponseBucket {

  private List<Object> key;
  private long docCount;

  private Map<String, Object> values;

  public ResponseBucket(List<Object> key, long docCount, Map<String, Object> values) {
    this.key = key;
    this.docCount = docCount;
    this.values = values;
  }

  public List<Object> getKey() {
    return key;
  }

  public long getDocCount() {
    return docCount;
  }

  public Map<String, Object> getValues() {
    return values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ResponseBucket that = (ResponseBucket) o;

    if (docCount != that.docCount) return false;
    if (!key.equals(that.key)) return false;
    return values.equals(that.values);
  }

  @Override
  public int hashCode() {
    int result = key.hashCode();
    result = 31 * result + (int) (docCount ^ (docCount >>> 32));
    result = 31 * result + values.hashCode();
    return result;
  }
}
