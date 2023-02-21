package com.slack.kaldb.logstore.search.queryparser;

import static org.opensearch.common.lucene.Lucene.KEYWORD_ANALYZER;

import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;

public class KaldbQueryParser extends QueryParser {

  private static final String EXISTS_FIELD = "_exists_";

  private final ConcurrentHashMap<String, LuceneFieldDef> chunkSchema;

  public KaldbQueryParser(
      String defaultField,
      Analyzer defaultAnalyzer,
      ConcurrentHashMap<String, LuceneFieldDef> chunkSchema) {
    super(defaultField, defaultAnalyzer);
    if (chunkSchema == null || chunkSchema.isEmpty()) {
      throw new IllegalArgumentException(
          "chunkSchema should never be empty. We should always initialize the parser with default fields");
    }
    this.chunkSchema = chunkSchema;
  }

  @Override
  protected Query getWildcardQuery(String field, String termStr) throws ParseException {
    String actualField = field != null ? field : this.field;
    if (termStr.equals("*") && actualField != null) {
      // *:* case
      if ("*".equals(actualField)) {
        return newMatchAllDocsQuery();
      }
      if (chunkSchema.get(actualField) != null) {
        // field:* query
        return new FieldExistsQuery(actualField);
      } else {
        throw new ParseException(String.format("Field %s does not exist in schema", actualField));
      }
    }
    // TODO: Support any additional use-cases
    return super.getWildcardQuery(field, termStr);
  }

  @Override
  public Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {
    if (EXISTS_FIELD.equals(field)) {
      return new FieldExistsQuery(queryText);
    }

    if (quoted) {
      return getFieldQuery(field, queryText, getPhraseSlop());
    }

    if (field == null || chunkSchema.get(field) == null) {
      throw new ParseException(String.format("Field %s does not exist in schema", field));
    }

    LuceneFieldDef fieldType = chunkSchema.get(field);
    Analyzer queryAnalyzer = fieldType.fieldType.getAnalyzer(false);

    // TODO: fold this bit into the fieldType but currently super.newFieldQuery
    // has a little more logic that I don't want to fold in just yet
    if (fieldType.fieldType == FieldType.TEXT) {
      // Today we only support one text field - i.e we don't support configuring analyzer.
      // when we do, that info will be present in fieldDef that can then be passed here
      return super.newFieldQuery(this.getAnalyzer(), field, queryText, quoted);
    }
    return fieldType.fieldType.termQuery(field, queryText, queryAnalyzer);
  }

  @Override
  protected Query getFieldQuery(String field, String queryText, int slop) throws ParseException {
    if (field == null || chunkSchema.get(field) == null) {
      throw new ParseException(String.format("Field %s does not exist in schema", field));
    }
    LuceneFieldDef fieldType = chunkSchema.get(field);

    // mimics super.getFieldQuery but passes our analyzer
    // needs cleanup in the future so that we don't copy things over
    if (fieldType.fieldType == FieldType.TEXT || fieldType.fieldType == FieldType.STRING) {
      Analyzer analyzer =
          fieldType.fieldType == FieldType.TEXT ? this.getAnalyzer() : KEYWORD_ANALYZER;

      Query query = super.newFieldQuery(analyzer, field, queryText, true);
      if (query instanceof PhraseQuery) {
        query = addSlopToPhrase((PhraseQuery) query, slop);
      } else if (query instanceof MultiPhraseQuery) {
        MultiPhraseQuery mpq = (MultiPhraseQuery) query;

        if (slop != mpq.getSlop()) {
          query = new MultiPhraseQuery.Builder(mpq).setSlop(slop).build();
        }
      }
      return query;
    }
    return fieldType.fieldType.termQuery(field, queryText, fieldType.fieldType.getAnalyzer(true));
  }

  /** Rebuild a phrase query with a slop value */
  // Copied over currently
  private PhraseQuery addSlopToPhrase(PhraseQuery query, int slop) {
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.setSlop(slop);
    org.apache.lucene.index.Term[] terms = query.getTerms();
    int[] positions = query.getPositions();
    for (int i = 0; i < terms.length; ++i) {
      builder.add(terms[i], positions[i]);
    }

    return builder.build();
  }
}
