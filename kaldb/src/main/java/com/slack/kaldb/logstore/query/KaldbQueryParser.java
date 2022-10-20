package com.slack.kaldb.logstore.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

public class KaldbQueryParser extends QueryParser {
    public KaldbQueryParser(String queryString, Analyzer analyzer, Map<String, FieldDef> fieldMap) {
        super(queryString, analyzer);
    }

    @Override
    protected Query getRangeQuery(String field, String min, String max, boolean minInclusive, boolean maxInclusive) throws ParseException {
        if (isNumericField(field)) // context dependent
        {
            final String pointField = "_point_" + field;
            return FloatPoint.newRangeQuery(pointField,
                    Float.parseFloat(low),
                    Float.parseFloat(high));
        }

        return super.getRangeQuery(field, low, high, startInclusive, endInclusive);
    }
}