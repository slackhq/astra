package com.slack.kaldb.logstore.search.queryparser;

import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
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
    // delegate to the field type which knows which analyzer to use for the specific field
    // for now something hacky like this without any analysis
    Term term = new Term(field, termStr);
    return new WildcardQuery(term);
  }

  @Override
  public Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {
    if ("_exists_".equals(field)) {
      // https://issues.apache.org/jira/browse/LUCENE-10436
      return new FieldExistsQuery(queryText);
    }
    if (quoted) {
      // TODO: If we were to expose phrase slop like other search engines
      queryBuilder.createPhraseQuery(field, queryText, 100);
    }
    // MUST vs SHOULD needs be configurable somewhere
    return queryBuilder.createBooleanQuery(field, queryText, BooleanClause.Occur.MUST);
  }
}
