package com.slack.astra.logstore.opensearch;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;

public class AstraIndexAnalyzer {

  private static IndexAnalyzers indexAnalyzers;

  private AstraIndexAnalyzer() {}

  public static IndexAnalyzers getInstance() {
    if (indexAnalyzers == null) {
      indexAnalyzers =
          new IndexAnalyzers(
              singletonMap(
                  "default",
                  new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
              emptyMap(),
              emptyMap());
    }
    return indexAnalyzers;
  }
}
