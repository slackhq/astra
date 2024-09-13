package com.slack.astra.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.astra.logstore.LogMessage;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.SearchModule;

import java.io.IOException;
import java.util.List;

public class QueryBuilderUtil {
  public static QueryBuilder generateQueryBuilder(String queryString, Long startTime, Long endTime) throws IOException {
    try {
      BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

      // only add a range filter if either start or end time is provided
      if (startTime != null || endTime != null) {
        RangeQueryBuilder rangeQueryBuilder =
            new RangeQueryBuilder(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

        if (startTime != null) {
          rangeQueryBuilder.gte(startTime);
        }

        if (endTime != null) {
          rangeQueryBuilder.lte(endTime);
        }

        boolQueryBuilder.filter(rangeQueryBuilder);
      }
      if (queryString != null
          && !queryString.isEmpty()
          && !queryString.equals("*:*")
          && !queryString.equals("*")) {
        QueryStringQueryBuilder queryStringQueryBuilder = new QueryStringQueryBuilder(queryString);
        queryStringQueryBuilder.analyzeWildcard(true);
        boolQueryBuilder.filter(queryStringQueryBuilder);
      }
      return boolQueryBuilder;
    } catch (Exception e) {
      return null;
    }
  }
}
