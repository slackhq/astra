package com.slack.kaldb.metadata.schema;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class FieldTypeUtils {

  public static final KeywordAnalyzer KEYWORD_ANALYZER = new KeywordAnalyzer();
  public static final StandardAnalyzer STANDARD_ANALYZER = new StandardAnalyzer();
}
