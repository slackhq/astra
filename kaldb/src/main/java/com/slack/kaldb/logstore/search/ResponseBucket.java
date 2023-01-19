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
}
