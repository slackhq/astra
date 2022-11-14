package com.slack.kaldb.logstore.search;

import java.util.Map;
import org.apache.lucene.search.CollectorManager;

public class AggregationDefinition {

  public enum AggregationType {
    SUM,
    MIN,
    MAX,
    AVERAGE,
    COUNT,
    PERCENTILE
  }

  final String name;
  final AggregationType type;
  final String field;
  final Map<String, Object> parameters;

  public AggregationDefinition(String name, AggregationType type, String field) {
    this.name = name;
    this.type = type;
    this.field = field;
    this.parameters = Map.of();
  }

  public AggregationDefinition(
      String name, AggregationType type, String field, Map<String, Object> parameters) {
    this.name = name;
    this.type = type;
    this.field = field;
    this.parameters = Map.copyOf(parameters);
  }

  public CollectorManager<?, ?> getCollectorManager() {
    return new ArithmeticCollectorManager(field);
  }
}
