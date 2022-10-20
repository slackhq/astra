package com.slack.kaldb.logstore.query;

import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.NUMERIC_FIELD_TYPES;

import com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

public class KaldbQueryParser extends QueryParser {
  private final Map<String, SchemaAwareLogDocumentBuilderImpl.FieldDef> fieldDefMap;

  public KaldbQueryParser(
      String queryString,
      Analyzer analyzer,
      Map<String, SchemaAwareLogDocumentBuilderImpl.FieldDef> fieldDefMap) {
    super(queryString, analyzer);
    this.fieldDefMap = fieldDefMap;
  }

  @Override
  protected Query getRangeQuery(
      String field, String min, String max, boolean minInclusive, boolean maxInclusive)
      throws ParseException {
    SchemaAwareLogDocumentBuilderImpl.FieldType fieldType = fieldDefMap.get(field).fieldType;
    if (NUMERIC_FIELD_TYPES.contains(fieldDefMap.get(field).fieldType)) {
      switch (fieldType) {
        case INTEGER:
          return IntPoint.newRangeQuery(field, Integer.parseInt(min), Integer.parseInt(max));
        case FLOAT:
          return FloatPoint.newRangeQuery(field, Float.parseFloat(min), Float.parseFloat(max));
        case LONG:
          return LongPoint.newRangeQuery(field, Long.parseLong(min), Long.parseLong(max));
        case DOUBLE:
          return DoublePoint.newRangeQuery(field, Double.parseDouble(min), Double.parseDouble(max));
        default:
          throw new IllegalStateException(
              "Unknown numeric field type: " + fieldType + "for field" + " " + field);
      }
    }
    return super.getRangeQuery(field, min, max, minInclusive, maxInclusive);
  }
}
