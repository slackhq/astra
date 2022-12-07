package com.slack.kaldb.logstore.search.queryparser;

import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;

import java.util.concurrent.ConcurrentHashMap;

public class KaldbQueryParser extends QueryParser {

  private final ConcurrentHashMap<String, LuceneFieldDef> chunkSchema;

  public KaldbQueryParser(
          String defaultField,
          Analyzer analyzer,
          ConcurrentHashMap<String, LuceneFieldDef> chunkSchema) {
    super(defaultField, analyzer);
    if (chunkSchema == null || chunkSchema.isEmpty()) {
      throw new IllegalArgumentException(
          "chunkSchema should never be empty. We should always initialize the parser with default fields");
    }
    this.chunkSchema = chunkSchema;
  }

  @Override
  protected Query getWildcardQuery(String field, String termStr) throws ParseException {
    if ("*".equals(termStr)) {
      return new FieldExistsQuery(field);
    }
    return super.getWildcardQuery(field, termStr);
  }

  @Override
  public Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {
    if ("_exists_".equals(field)) {
      // https://issues.apache.org/jira/browse/LUCENE-10436
      return new FieldExistsQuery(queryText);
    }
    // TODO: KaldbQueryParser needs access to the schema object
    // Once it has access to the schema object we need to delegate these methods under the FieldType
    // enum
    // Needs to guard against case where value isn't long
    if (field.equals(LogMessage.ReservedField.DURATION_MS.fieldName)) {
      return LongPoint.newExactQuery(field, Long.parseLong(queryText));
    }
    return super.getFieldQuery(field, queryText, quoted);
  }
}
