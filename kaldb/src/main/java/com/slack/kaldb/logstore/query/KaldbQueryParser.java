package com.slack.kaldb.logstore.query;

import static com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl.NUMERIC_FIELD_TYPES;

import com.slack.kaldb.logstore.SchemaAwareLogDocumentBuilderImpl;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexOrDocValuesQuery;
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
    // TODO: Handle inclusive and exclusive range.
    // TODO: Use IndexOrDocValuesQuery here?
    if (fieldDefMap.containsKey(field)) {
      final SchemaAwareLogDocumentBuilderImpl.FieldDef fieldDef = fieldDefMap.get(field);
      final SchemaAwareLogDocumentBuilderImpl.FieldType fieldType = fieldDef.fieldType;
      if (NUMERIC_FIELD_TYPES.contains(fieldDef.fieldType)) {
        switch (fieldType) {
          case INTEGER:
            int minValue = Integer.parseInt(min);
            int maxValue = Integer.parseInt(max);
            Query pointRangeQuery =
                IntPoint.newRangeQuery(field, Integer.parseInt(min), Integer.parseInt(max));
            if (fieldDef.storeNumericDocValue) {
              Query docValuesQuery =
                  SortedNumericDocValuesField.newSlowRangeQuery(field, minValue, maxValue);
              return new IndexOrDocValuesQuery(pointRangeQuery, docValuesQuery);
            }
            return pointRangeQuery;
          case FLOAT:
            return FloatPoint.newRangeQuery(field, Float.parseFloat(min), Float.parseFloat(max));
          case LONG:
            return LongPoint.newRangeQuery(field, Long.parseLong(min), Long.parseLong(max));
          case DOUBLE:
            return DoublePoint.newRangeQuery(
                field, Double.parseDouble(min), Double.parseDouble(max));
          default:
            throw new IllegalStateException(
                "Unknown numeric field type: " + fieldType + "for field" + " " + field);
        }
      }
    }
    // TODO:  If fieldDefMap is empty, this will hide a bug
    return super.getRangeQuery(field, min, max, minInclusive, maxInclusive);
  }
}
