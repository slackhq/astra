package com.slack.kaldb.logstore.search.queryparser;

import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.QueryParser;

public class KaldbQueryParser extends QueryParser {

  private final ConcurrentHashMap<String, LuceneFieldDef> chunkSchema;

  public KaldbQueryParser(
      String defaultField,
      Analyzer analyzer,
      ConcurrentHashMap<String, LuceneFieldDef> chunkSchema) {
    super(defaultField, analyzer);
    if (chunkSchema == null || chunkSchema.isEmpty()) {
      // When we instantiate SchemaAwareLogDocumentBuilderImpl we load a bunch of default fields. So
      // this should never be empty
      throw new RuntimeException("This should never be empty");
    }
    this.chunkSchema = chunkSchema;
  }
}
